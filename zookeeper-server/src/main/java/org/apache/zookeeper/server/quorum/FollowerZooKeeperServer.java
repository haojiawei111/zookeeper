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

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jute.Record;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnHeader;

import javax.management.JMException;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -> CommitProcessor ->
 * FinalRequestProcessor
 *
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    /*
     * Pending sync requests
     * TODO:待同步请求
     */
    ConcurrentLinkedQueue<Request> pendingSyncs;

    /**
     * @param logFactory
     * @param self
     * @param zkDb
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory,QuorumPeer self,ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout,
                self.maxSessionTimeout, self.clientPortListenBacklog,
                zkDb, self);
        // 初始化pendingSyncs
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower(){
        return self.follower;
    }

    // 请求处理链 这里两条处理链
    // FollowerRequestProcessor -> CommitProcessor -> FinalRequestProcessor
    // SyncRequestProcessor -> SendAckRequestProcessor

    // 从FollowerRequestProcessor处理器开始,该处理器接收并处理客户端请求. FollowerRequestProcessor 处理器之后转发请求给CommitRequestProcessor,同时也会转发写请求到群首服务器. CommitRequestProcessor 会直接转发读请求到FinalRequestProcessor 处理器, 而且对于写请求, CommitRequestProcessor在转发给 FinalRequestProcessor处理器之前会等待提交事务.
    //
    //当群首接收到一个新的写请求操作时,直接地或通过其他追随者服务器生成一个提议(proposal),之后转发到追随者服务器. 当收到一个提议,追随者会发送这个提议道SyncRequestProcessor 处理器,SendRequestProcessor会向群首发送确认消息. 当群首收到足够确认消息来提交这个提议时,群首就会发送提交事务消息给追随者(同时也会发哦少年宫INFORM消息给观察者服务器). 当接收到提交事务消息时,追随者就通过CommitRequestProcessor 处理器进行处理.

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor,
                Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();

        syncProcessor = new SyncRequestProcessor(this,
                new SendAckRequestProcessor((Learner)getFollower()));
        syncProcessor.start();
    }

    // TODO: 待处理的事务请求
    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    /**
     * TODO:该函数将请求进行记录（放入到对应的队列中），等待处理。
     * @param hdr
     * @param txn
     */
    public void logRequest(TxnHeader hdr, Record txn) {
        // 创建请求
        Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        if ((request.zxid & 0xffffffffL) != 0) { // zxid不为0，表示本服务器已经处理过请求
            pendingTxns.add(request);
        }
        // 使用SyncRequestProcessor处理请求(其会将请求放在队列中，异步进行处理)
        syncProcessor.processRequest(request);
    }

    /**
     * When a COMMIT message is received, eventually this method is called,
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    // 该函数会提交zxid对应的请求（pendingTxns的队首元素），
    // 其首先会判断队首请求对应的zxid是否为传入的zxid，然后再进行移除和提交（放在committedRequests队列中）
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {// 没有还在等待处理的事务
            LOG.warn("Committing " + Long.toHexString(zxid)
                    + " without seeing txn");
            return;
        }
        // 队首元素的zxid
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {// 如果队首元素的zxid不等于需要提交的zxid，则退出程序
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                    + " but next pending txn 0x"
                    + Long.toHexString(firstElementZxid));
            System.exit(ExitCode.UNMATCHED_TXN_COMMIT.getValue());
        }
        // 从待处理事务请求队列中移除队首请求
        Request request = pendingTxns.remove();
        // 提交该请求
        commitProcessor.commit(request);
    }

    synchronized public void sync(){
        if(pendingSyncs.size() == 0) {// 没有需要同步的请求
            LOG.warn("Not expecting a sync.");
            return;
        }
        // 从待同步队列中移除队首请求
        Request r = pendingSyncs.remove();
        if (r instanceof LearnerSyncRequest) {
            LearnerSyncRequest lsr = (LearnerSyncRequest)r;
            lsr.fh.queuePacket(new QuorumPacket(Leader.SYNC, 0, null, null));
        }
        // 提交该请求
        commitProcessor.commit(r);
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        LOG.info("Override {} to {}", GLOBAL_OUTSTANDING_LIMIT, globalOutstandingLimit);
        return globalOutstandingLimit;
    }

    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }

    /**
     * Process a request received from external Learner through the LearnerMaster
     * These requests have already passed through validation and checks for
     * session upgrade and can be injected into the middle of the pipeline.
     *
     * @param request received from external Learner
     */
    void processObserverRequest(Request request) {
        ((FollowerRequestProcessor)firstProcessor).processRequest(request, false);
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
}
