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
import java.io.PrintWriter;
import java.nio.ByteBuffer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * Abstract base class for all ZooKeeperServers that participate in
 * a quorum.
 */
public abstract class QuorumZooKeeperServer extends ZooKeeperServer {

    public final QuorumPeer self;
    protected UpgradeableSessionTracker upgradeableSessionTracker;

    protected QuorumZooKeeperServer(FileTxnSnapLog logFactory, int tickTime,
            int minSessionTimeout, int maxSessionTimeout, int listenBacklog,
            ZKDatabase zkDb, QuorumPeer self)
    {
        super(logFactory, tickTime, minSessionTimeout, maxSessionTimeout, listenBacklog, zkDb);
        this.self = self;
    }

    @Override
    protected void startSessionTracker() {
        upgradeableSessionTracker = (UpgradeableSessionTracker) sessionTracker;
        upgradeableSessionTracker.start();
    }

    public Request checkUpgradeSession(Request request) throws IOException, KeeperException {
        // If this is a request for a local session and it is to
        // create an ephemeral node, then upgrade the session and return
        // a new session request for the leader.
        // This is called by the request processor thread (either follower
        // or observer request processor), which is unique to a learner.
        // So will not be called concurrently by two threads.
        // TODO: 如果这是对本地会话的请求，并且要创建一个临时节点，则升级该会话并返回针对领导者的新会话请求。
		// 这是由请求处理器线程（跟随者或观察者请求处理器）调用的，对于学习者而言是唯一的。所以不会被两个线程同时调用。
        if ((request.type != OpCode.create
				&& request.type != OpCode.create2
				&& request.type != OpCode.multi)
				|| !upgradeableSessionTracker.isLocalSession(request.sessionId)) {
        	// TODO: 不升级会话
            return null;
        }

        // TODO: 程序走到这里，请求一定是OpCode.create、OpCode.create2、OpCode.multi其中一种并且会话ID在会话追踪器中是本地会话
		// TODO: 下面判断
        if (OpCode.multi == request.type) {// 批量操作
            MultiTransactionRecord multiTransactionRecord = new MultiTransactionRecord();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, multiTransactionRecord);
            request.request.rewind();
            boolean containsEphemeralCreate = false;
            for (Op op : multiTransactionRecord) {
                if (op.getType() == OpCode.create || op.getType() == OpCode.create2) {
                    CreateRequest createRequest = (CreateRequest)op.toRequestRecord();
                    // 创建的节点类型
                    CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                    if (createMode.isEphemeral()) {
                    	// 创建的是临时节点
                        containsEphemeralCreate = true;
                        break;
                    }
                }
            }
            if (!containsEphemeralCreate) {
            	// TODO: 不升级会话
                return null;
            }
        } else {
            CreateRequest createRequest = new CreateRequest();
            request.request.rewind();
            ByteBufferInputStream.byteBuffer2Record(request.request, createRequest);
            request.request.rewind();
            // 节点类型
            CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
            if (!createMode.isEphemeral()) {
				// TODO: 如果不是创建临时节点，不升级会话
                return null;
            }
        }

		// TODO: 程序走到这里，请求一定是OpCode.create、OpCode.create2、OpCode.multi其中一种并且包含创建临时节点的操作并且会话ID在会话追踪器中是本地会话

        // Uh oh.  We need to upgrade before we can proceed.
        // 哦哦我们需要先进行升级，然后才能继续。
        if (!self.isLocalSessionsUpgradingEnabled()) {
            throw new KeeperException.EphemeralOnLocalSessionException();
        }
        // TODO: 升级会话
        return makeUpgradeRequest(request.sessionId);
    }

    private Request makeUpgradeRequest(long sessionId) {
        // Make sure to atomically check local session status, upgrade
        // session, and make the session creation request.  This is to
        // avoid another thread upgrading the session in parallel.
        // 确保自动检查本地会话状态，升级会话并发出会话创建请求。这是为了避免另一个线程并行升级会话。
        synchronized (upgradeableSessionTracker) {
            if (upgradeableSessionTracker.isLocalSession(sessionId)) {
            	// 升级会话
                int timeout = upgradeableSessionTracker.upgradeSession(sessionId);
                ByteBuffer to = ByteBuffer.allocate(4);
                to.putInt(timeout);
                // TODO: 返回创建会话的请求
                return new Request(null, sessionId, 0, OpCode.createSession, to, null);
            }
        }
        return null;
    }

    /**
     * Implements the SessionUpgrader interface,
     * 实现SessionUpgrader接口，
     *
     * @param sessionId
     */
    public void upgrade(long sessionId) {
        Request request = makeUpgradeRequest(sessionId);
        if (request != null) {
            LOG.info("Upgrading session 0x" + Long.toHexString(sessionId));
            // This must be a global request
            submitRequest(request);
        }
    }
    // 对于这些类型的请求，我们需要将isLocalSession设置为tree，以便请求处理器可以正确处理它们。
    @Override
    protected void setLocalSessionFlag(Request si) {
        // We need to set isLocalSession to tree for these type of request
        // so that the request processor can process them correctly.
        switch (si.type) {
        case OpCode.createSession:
            if (self.areLocalSessionsEnabled()) {
                // All new sessions local by default.
                // 默认情况下所有新会话都是本地
                si.setLocalSession(true);
            }
            break;
        case OpCode.closeSession:
            String reqType = "global";
            if (upgradeableSessionTracker.isLocalSession(si.sessionId)) {
                si.setLocalSession(true);
                reqType = "local";
            }
            LOG.info("Submitting " + reqType + " closeSession request"
                    + " for session 0x" + Long.toHexString(si.sessionId));
            break;
        default:
            break;
        }
    }

    @Override
    public void dumpConf(PrintWriter pwriter) {
        super.dumpConf(pwriter);

        pwriter.print("initLimit=");
        pwriter.println(self.getInitLimit());
        pwriter.print("syncLimit=");
        pwriter.println(self.getSyncLimit());
        pwriter.print("electionAlg=");
        pwriter.println(self.getElectionType());
        pwriter.print("electionPort=");
        pwriter.println(self.getElectionAddress().getPort());
        pwriter.print("quorumPort=");
        pwriter.println(self.getQuorumAddress().getPort());
        pwriter.print("peerType=");
        pwriter.println(self.getLearnerType().ordinal());
        pwriter.println("membership: ");
        pwriter.print(new String(self.getQuorumVerifier().toString().getBytes()));
    }

    // 设置服务状态
    @Override
    protected void setState(State state) {
        this.state = state;
    }
}
