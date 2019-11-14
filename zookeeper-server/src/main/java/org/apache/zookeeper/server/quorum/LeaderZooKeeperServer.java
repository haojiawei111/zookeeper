/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.DataTreeBean;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import javax.management.JMException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: PrepRequestProcessor -> ProposalRequestProcessor ->
 * CommitProcessor -> Leader.ToBeAppliedRequestProcessor ->
 * FinalRequestProcessor
 */
public class LeaderZooKeeperServer extends QuorumZooKeeperServer {
    private ContainerManager containerManager;  // guarded by sync

    // 提交请求处理器
    CommitProcessor commitProcessor;

    PrepRequestProcessor prepRequestProcessor;

    /**
     * @param logFactory
     * @param self
     * @param zkDb
     * @throws IOException
     */
    LeaderZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout,
                self.clientPortListenBacklog, zkDb, self);
    }

    public Leader getLeader(){
        return self.leader;
    }

    // Leader请求处理链
    //                                                        |-> SyncRequestProcessor -> AckRequestProcessor
    //                                                        |
    // LeaderRequestProcessor -> prepRequestProcessor -> proposalProcessor -> commitProcessor -> Leader.ToBeAppliedRequestProcessor -> FinalRequestProcessor

    // PrepRequestProcessor接口客户端的请求并执行这个请求,处理结果则是生成一个事务. 事务是执行一个操作的结果, 该操作会反映到ZooKeeper的数据树上.
    // 事务信息将会以头部记录和事务记录的方式添加到Request对象中. 同时还要注意,只有改变ZooKeeper状态的操作才会产生事务,对于读操作并不会产生任何事务.
    // 因此,对于读请求的Request对象中,事务成员属性的引用值为null.
    //
    // 而之后的处理器则为ProposalRequestProcessor.该处理器会准备一个提议,并将该提议发送给跟随者. ProposalRequestProcessor将会把所有请求都转发给CommitRequestProcessor,而且对于写操作请求,还会将请求转发给SyncRequestProcessor处理器.
    //
    //SyncRequestProcessor 处理器所执行的操作与独立服务器一样,即持久化操作. 执行完之后会触发AckRequestProcessor处理器,它是一个简单请求处理器,它仅仅生成确认消息并返回给自己. 此处便是上文中提到的,在仲裁模式下,群首需要收到每个服务器的确认消息,也包括自己,而AckRequestProcessor 处理器就负责这个;
    //
    //CommitRequestProcessor 会将收到足够多的确认消息的提议进行提交. 实际上,确认消息是由Leader类处理的(Leader.processAck()方法),这个方法会将提交的请求加入到CommitRequestProcessor类中的一个队列中.这个队列会由请求处理器线程进行处理.
    //
    //FinalRequestProcessor处理器,作用与独立服务器一样,不过在它之前还有一个简单的请求处理器,这个处理器会从提议列表中删除那些待接受的提议,这个处理器的名字叫ToBeAppliedRequestProcessor,待接受请求列表包括那些已经被仲裁法定人数所确认的请求,并等待执行. 群首使用这个列表与追随者之间进行同步,并将收到确认消息的请求加入到这个列表中. 之后 ToBeAppliedRequestProcessor 处理器就会在 FinalRequestProcessor处理器执行后删除这个列表中的元素;

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(finalProcessor, getLeader());
        commitProcessor = new CommitProcessor(toBeAppliedProcessor, Long.toString(getServerId()), false,
                getZooKeeperServerListener());
        commitProcessor.start();
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(this, commitProcessor);
        proposalProcessor.initialize();
        prepRequestProcessor = new PrepRequestProcessor(this, proposalProcessor);
        prepRequestProcessor.start();
        firstProcessor = new LeaderRequestProcessor(this, prepRequestProcessor);

        setupContainerManager();
    }

    private synchronized void setupContainerManager() {
        containerManager = new ContainerManager(getZKDatabase(), prepRequestProcessor,
                Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1)),
                Integer.getInteger("znode.container.maxPerMinute", 10000)
                );
    }

    @Override
    public synchronized void startup() {
        super.startup();
        if (containerManager != null) {
            containerManager.start();
        }
    }

    @Override
    public synchronized void shutdown() {
        if (containerManager != null) {
            containerManager.stop();
        }
        super.shutdown();
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        LOG.info("Override {} to {}", GLOBAL_OUTSTANDING_LIMIT, globalOutstandingLimit);
        return globalOutstandingLimit;
    }

    // 创建会话跟踪器
    @Override
    public void createSessionTracker() {
        sessionTracker = new LeaderSessionTracker(
                this, getZKDatabase().getSessionWithTimeOuts(),
                tickTime, self.getId(), self.areLocalSessionsEnabled(), 
                getZooKeeperServerListener());
    }

    public boolean touch(long sess, int to) {
        return sessionTracker.touchSession(sess, to);
    }

    public boolean checkIfValidGlobalSession(long sess, int to) {
        if (self.areLocalSessionsEnabled() &&
            !upgradeableSessionTracker.isGlobalSession(sess)) {
            return false;
        }
        return sessionTracker.touchSession(sess, to);
    }

    /**
     * Requests coming from the learner should go directly to
     * PrepRequestProcessor
     *
     * TODO: 来自 learner 的请求应直接转到 PrepRequestProcessor
     *
     * @param request
     */
    public void submitLearnerRequest(Request request) {
        /*
         * Requests coming from the learner should have gone through
         * submitRequest() on each server which already perform some request
         * validation, so we don't need to do it again.
         *
         * Additionally, LearnerHandler should start submitting requests into
         * the leader's pipeline only when the leader's server is started, so we
         * can submit the request directly into PrepRequestProcessor.
         *
         * This is done so that requests from learners won't go through
         * LeaderRequestProcessor which perform local session upgrade.
         */
        prepRequestProcessor.processRequest(request);
    }

    @Override
    protected void registerJMX() {
        // register with JMX
        try {
            jmxDataTreeBean = new DataTreeBean(getZKDatabase().getDataTree());
            MBeanRegistry.getInstance().register(jmxDataTreeBean, jmxServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxDataTreeBean = null;
        }
    }

    public void registerJMX(LeaderBean leaderBean,
            LocalPeerBean localPeerBean)
    {
        // register with JMX
        if (self.jmxLeaderElectionBean != null) {
            try {
                MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }

        try {
            jmxServerBean = leaderBean;
            MBeanRegistry.getInstance().register(leaderBean, localPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxServerBean = null;
        }
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        try {
            MBeanRegistry.getInstance().register(handlerBean, jmxServerBean);
            return true;
        } catch (JMException e) {
            LOG.warn("Could not register connection", e);
        }
        return false;
    }

    @Override
    protected void unregisterJMX() {
        // unregister from JMX
        try {
            if (jmxDataTreeBean != null) {
                MBeanRegistry.getInstance().unregister(jmxDataTreeBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxDataTreeBean = null;
    }

    protected void unregisterJMX(Leader leader) {
        // unregister from JMX
        try {
            if (jmxServerBean != null) {
                MBeanRegistry.getInstance().unregister(jmxServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxServerBean = null;
    }

    @Override
    public String getState() {
        return "leader";
    }

    /**
     * Returns the id of the associated QuorumPeer, which will do for a unique
     * id of this server.
     */
    @Override
    public long getServerId() {
        return self.getId();
    }

    @Override
    protected void revalidateSession(ServerCnxn cnxn, long sessionId,
        int sessionTimeout) throws IOException {
        super.revalidateSession(cnxn, sessionId, sessionTimeout);
        try {
            // setowner as the leader itself, unless updated
            // via the follower handlers
            setOwner(sessionId, ServerCnxn.me);
        } catch (SessionExpiredException e) {
            // this is ok, it just means that the session revalidation failed.
        }
    }
}
