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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor forwards any requests that modify the state of the system to the Leader.
 * Followerd的第一个处理器是FollowerRequestProcessor
 * 此RequestProcessor将修改系统状态的所有请求（事务请求）转发给Leader。
 */
public class FollowerRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessor.class);

    FollowerZooKeeperServer zks;

    RequestProcessor nextProcessor;

    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    boolean finished = false;

    public FollowerRequestProcessor(FollowerZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("FollowerRequestProcessor:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while (!finished) {
                // 从队列中取出请求
                Request request = queuedRequests.take();
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK,
                            'F', request, "");
                }
                // 如果是关闭的请求，退出线程
                if (request == Request.requestOfDeath) {
                    break;
                }
                // We want to queue the request to be processed before we submit
                // the request to the leader so that we are ready to receive
                // the response
                // 除了requestOfDeath请求，其他所有请求都传递给后面的处理链
                // 先交给CommitProcessor，最终投票通过后，会通过CommitProcessor的commit方法最终提交事务
                nextProcessor.processRequest(request);

                // We now ship the request to the leader. As with all
                // other quorum operations, sync also follows this code
                // path, but different from others, we need to keep track
                // of the sync operations this follower has pending, so we
                // add it to pendingSyncs.
                // 我们现在将请求发送给领导者。与所有其他仲裁操作一样，sync也遵循此代码路径，
                // 但与其他代码不同，我们需要跟踪跟随者已挂起的同步操作，因此我们将其添加到pendingSyncs。
                switch (request.type) {
                case OpCode.sync:
                    // 添加到pendingSyncs队列
                    zks.pendingSyncs.add(request);
                    // // 转发事务请求给leader
                    zks.getFollower().request(request);
                    break;
                case OpCode.create:
                case OpCode.create2:
                case OpCode.createTTL:
                case OpCode.createContainer:
                case OpCode.delete:
                case OpCode.deleteContainer:
                case OpCode.setData:
                case OpCode.reconfig:
                case OpCode.setACL:
                case OpCode.multi:
                case OpCode.check:
                    // 转发事务请求给leader
                    zks.getFollower().request(request);
                    break;
                case OpCode.createSession:
                case OpCode.closeSession:
                    // Don't forward local sessions to the leader.
                    // 不要将本地会话转发给领导者。
                    if (!request.isLocalSession()) {
                        // 创建回话和关闭会话，但不是本地会话就会发送请求给leader
                        zks.getFollower().request(request);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("FollowerRequestProcessor exited loop!");
    }

    public void processRequest(Request request) {
        processRequest(request, true);
    }

    void processRequest(Request request, boolean checkForUpgrade) {
        if (!finished) {
            if (checkForUpgrade) {
                // Before sending the request, check if the request requires a
                // global session and what we have is a local session. If so do
                // an upgrade.
                Request upgradeRequest = null;
                try {
                    upgradeRequest = zks.checkUpgradeSession(request);
                } catch (KeeperException ke) {
                    if (request.getHdr() != null) {
                        request.getHdr().setType(OpCode.error);
                        request.setTxn(new ErrorTxn(ke.code().intValue()));
                    }
                    request.setException(ke);
                    LOG.info("Error creating upgrade request", ke);
                } catch (IOException ie) {
                    LOG.error("Unexpected error in upgrade", ie);
                }
                if (upgradeRequest != null) {
                    queuedRequests.add(upgradeRequest);
                }
            }

            queuedRequests.add(request);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}
