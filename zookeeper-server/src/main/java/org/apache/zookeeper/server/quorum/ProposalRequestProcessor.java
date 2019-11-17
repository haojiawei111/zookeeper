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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: 事务投票处理器。Leader服务器事务处理流程的发起者。
 * ProposalRequestProcessor用于向Follower发送Proposal，来完成Zab算法．
 *
 * 1)对于非事务性请求，ProposalRequestProcessor会直接将请求转发到CommitProcessor处理器，不再做任何处理
 * 2)对于事务性请求，除了将请求转发到CommitProcessor外，还会根据请求类型创建对应的Proposal提议，并发送给所有的Follower服务器来发起一次集群内的事务投票。
 * 同时，ProposalRequestProcessor还会将事务请求交付给SyncRequestProcessor进行事务日志的记录。
 *
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and SyncRequestProcessor.
 * 该RequestProcessor简单地将请求转发给SyncRequestProcessor和AckRequestProcessor
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ProposalRequestProcessor.class);

    //leader角色才有这个请求处理器，这里能拿到LeaderZooKeeperServer
    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    //同步处理器
    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;//一般是CommitProcessor
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        //syncProcessor的后续是AckRequestProcess
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     * TODO：启动SyncRequestProcessor同步处理器
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */
        // 在下面的IF-THEN-ELSE块中，我们处理leader的同步。
        // 如果同步来自follower，则follower处理程序将其添加到syncHandler。
        // 否则，如果它是发出sync命令的leader的客户端，则syncHandler将不包含该处理程序。在这种情况下，我们将它添加到syncHandler，并在下一个处理器上调用processRequest。
        if (request instanceof LearnerSyncRequest){
            // Learner发过来的同步请求，让Leader处理
            // 特殊处理，不走调用链,根据lastProposed记录，processAck函数异步处理时时给对应的LearnerHandler发送Sync的消息
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
            //交给CommitProcessor处理
            nextProcessor.processRequest(request);
            //如果请求头不为空(是事务请求)
            if (request.getHdr() != null) {
                // We need to sync and get consensus on any transactions
                // 我们需要同步并就任何交易达成共识
                try {
                    //TODO: leader发出提议,集群进行投票
                    zks.getLeader().propose(request);//发起一轮投票
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                //TODO: 将事务请求交付给SyncRequestProcessor进行事务日志的记录
                //TODO: 间接调用AckRequestProcessor, 投了自己一票.
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
