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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;

import javax.net.ssl.SSLSocket;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication.
 * 这个类是ZK合奏中三个主要演员中的两个的超类：追随者和观察者。追随者和观察者共享大量代码，这些代码被移入Peer以避免重复。
 * <p>
 * 主要是发现leader，连接leader，向leader注册自己，与leader进行数据同步
 */
public class Learner {

    // PacketInFlight类，这个类是记录Leader发出提议，但是还没有通过过半验证时候记录的数据格式
    // 类名代表"还在处理的包"
    static class PacketInFlight {
        TxnHeader hdr;
        Record rec;
    }

    //当前集群对象
    QuorumPeer self;
    //当前learner状态的zk服务器
    LearnerZooKeeperServer zk;
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;

    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    // 这里是和leader建立的连接
    protected InputArchive leaderIs;//输入
    protected OutputArchive leaderOs;  //输出
    /** the protocol version of the leader */
    //当前协议版本
    protected int leaderProtocolVersion = 0x01;
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    /**
     * Time to wait after connection attempt with the Leader or LearnerMaster before this
     * Learner tries to connect again.
     */
    //连接leader是否允许延迟
    private static final int leaderConnectDelayDuringRetryMs = Integer.getInteger("zookeeper.leaderConnectDelayDuringRetryMs", 100);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("leaderConnectDelayDuringRetryMs: {}", leaderConnectDelayDuringRetryMs);
        LOG.info("TCP NoDelay set to: {}", nodelay);
    }
    //client连接到learner时，learner要向leader提出REVALIDATE请求，在收到回复之前，记录在一个map中，表示尚未处理完的验证
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations = new ConcurrentHashMap<Long, ServerCnxn>();
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     * 验证客户端的会话
     * 集群版client重连时调用，learner验证会话是否有效，并激活，需要发送请求给Leader
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout)
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);//写入clientId
        dos.writeInt(timeout);
        dos.close();
        //发送REVALIDATE命令
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos.toByteArray(), null);
        //需要验证，还未返回结果，记录在map中
        pendingRevalidations.put(clientId, cnxn);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        writePacket(qp, true);
    }     
    
    /**
     * write a packet to the leader
     * TODO: 写一个packet包给leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        // 这里写出用了同步块
        synchronized (leaderOs) {
            if (pp != null) {
                // 向leader转发一个请求
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     * 从leader那里读取一个packet包
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet");
        }
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     * TODO: 向leader发送请求包
     *
     * 所有请求都被LEADER处理，learner接收到请求会转发给Leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        // 反序列化
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos.toByteArray(), request.authInfo);
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     * TODO: 返回我们认为是领导者的节点的地址。这是Follower查找Leader的逻辑，和Observer不一样
     *
     * 通过currentVote的sid遍历所有集群机器，看哪个sid一样，就是那台机器
     */
    protected QuorumServer findLeader() {
        QuorumServer leaderServer = null;
        // Find the leader by id
        // 通过id找到Leader
        Vote current = self.getCurrentVote();
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) {//集群中某个机器的sid和当前投票的sid一样
                // Ensure we have the leader's correct IP address before attempting to connect.
                // 在尝试连接之前，请确保我们拥有领导者的正确IP地址。
                s.recreateSocketAddresses();
                leaderServer = s;
                break;
            }
        }
        if (leaderServer == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return leaderServer;
    }
   
    /**
     * Overridable helper method to return the System.nanoTime().
     * This method behaves identical to System.nanoTime().
     */
    protected long nanoTime() {
        return System.nanoTime();
    }

    /**
     * Overridable helper method to simply call sock.connect(). This can be
     * overriden in tests to fake connection success/failure for connectToLeader. 
     */
    protected void sockConnect(Socket sock, InetSocketAddress addr, int timeout) 
    throws IOException {
        sock.connect(addr, timeout);
    }

    /**
     * Establish a connection with the LearnerMaster found by findLearnerMaster.
     * Followers only connect to Leaders, Observers can connect to any active LearnerMaster.
     * Retries until either initLimit time has elapsed or 5 tries have happened.
     *
     * 与findLearnerMaster找到的LearnerMaster建立连接。
     * Followers只能连接到Leaders，Observers可以连接到任何活跃的LearnerMaster。
     * 重试，直到initLimit时间过去或发生了5次尝试。
     *
     * @param addr - the address of the Peer to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * <li>if there is an authentication failure while connecting to leader</li>
     * @throws ConnectException
     * @throws InterruptedException
     */
    protected void connectToLeader(InetSocketAddress addr, String hostname)
            throws IOException, InterruptedException, X509Exception {
        this.sock = createSocket();

        int initLimitTime = self.tickTime * self.initLimit;
        int remainingInitLimitTime;
        long startNanoTime = nanoTime();

        for (int tries = 0; tries < 5; tries++) {
            try {
                // recalculate the init limit time because retries sleep for 1000 milliseconds
                // 重新计算init限制时间，因为重试休眠1000毫秒
                remainingInitLimitTime = initLimitTime - (int)((nanoTime() - startNanoTime) / 1000000);
                if (remainingInitLimitTime <= 0) {
                    LOG.error("initLimit exceeded on retries.");
                    throw new IOException("initLimit exceeded on retries.");
                }

                sockConnect(sock, addr, Math.min(self.tickTime * self.syncLimit, remainingInitLimitTime));
                if (self.isSslQuorum())  {
                    ((SSLSocket) sock).startHandshake();
                }
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                remainingInitLimitTime = initLimitTime - (int)((nanoTime() - startNanoTime) / 1000000);

                if (remainingInitLimitTime <= 1000) {
                    LOG.error("Unexpected exception, initLimit exceeded. tries=" + tries +
                             ", remaining init limit=" + remainingInitLimitTime +
                             ", connecting to " + addr,e);
                    throw e;
                } else if (tries >= 4) {
                    LOG.error("Unexpected exception, retries exceeded. tries=" + tries +
                             ", remaining init limit=" + remainingInitLimitTime +
                             ", connecting to " + addr,e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries=" + tries +
                            ", remaining init limit=" + remainingInitLimitTime +
                            ", connecting to " + addr,e);
                    this.sock = createSocket();
                }
            }
            Thread.sleep(leaderConnectDelayDuringRetryMs);
        }

        self.authLearner.authenticate(sock, hostname);

        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);
    }

    private Socket createSocket() throws X509Exception, IOException {
        Socket sock;
        if (self.isSslQuorum()) {
            sock = self.getX509Util().createSSLSocket();
        } else {
            sock = new Socket();
        }
        sock.setSoTimeout(self.tickTime * self.initLimit);
        return sock;
    }

    /**
     * Once connected to the leader or learner master, perform the handshake
     * protocol to establish a following / observing connection.
     *
     * 连上leader后，进行握手,参数代表一个following或者observing连接的注册
     * 注册时会发把自己基本信息发送给leader，称为learnerInfo
     *
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException{ // 注册当前Follower
        /*
         * Send follower info, including last zxid and sid
         */
    	long lastLoggedZxid = self.getLastLoggedZxid();
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType);  //设置类型为Leader.FOLLOWERINFO或者Leader.OBSERVERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));
        
        /*
         * Add sid to payload
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000, self.getQuorumVerifier().getVersion());
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");//把learner当前信息发送给leader
        qp.setData(bsid.toByteArray());
        
        //发送LearnerInfo包
        writePacket(qp, true);
        //接收leader的回复(新版本是一个LEADERINFO的消息，包含leader的状态),这里会阻塞
        readPacket(qp);
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());
		if (qp.getType() == Leader.LEADERINFO) {//新版本的leader
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
            // 我们连接到1.0服务器，所以接受新epoch并读取下一个packet数据包
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt();
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	if (newEpoch > self.getAcceptedEpoch()) {
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		self.setAcceptedEpoch(newEpoch);
        	} else if (newEpoch == self.getAcceptedEpoch()) {
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                // 因为我们已经开始了一个与领导者相同的时代，我们无法获得收益，但我们仍然需要将我们的lastZxid发送给领导者，
                // 以便我们可以与它同步，如果它确实承担了时代的领导。
                // -1表示此回复不应计为新纪元的确认
                wrappedEpochBytes.putInt(-1);
        	} else {
        	    // Leaders的代小于当前跟随着的代
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	// 返回ACKEPOCH消息
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);//8.接受完了leader状态之后，要发送ACK消息
        	writePacket(ackNewEpoch, true);
            return ZxidUtils.makeZxid(newEpoch, 0);
        } else {
            //老版本的leader(用于兼容)
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    } 

//    1.DIFF 和 SNAP 两种数据同步的方式的区别是什么
//    打个比方,LEADER的样子是A，LEARNER的样式是B
//    DIFF：代表LEADER后续会告诉LEARNER：不断接收PROPOSAL和COMMIT，一步步提交让LEARNER从B变成B1，到B2。。。最后到A的样子.
//    SNAP：代表LEADER直接告诉LEARNER:我长得样子是A，你copy一下变成我的样子就好了
//    2.是否发送了DIFF ,SNAP或者TRUNC，learner接收到就一下子数据同步完成了
//            不是
//    DIFF：后面会跟着一堆INFORM和COMMIT让learner一步步的同步
//    TRUNC：回滚到某个zxid后，后面也会跟着一堆INFORM和COMMIT
//    SNAP：后面会发送自己db的序列化内容
    /**
     * Finally, synchronize our history with the Leader (if Follower)
     * or the LearnerMaster (if Observer).
     *
     * TODO: 启动时learner先和leader进行数据同步
     *
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    protected void syncWithLeader(long newLeaderZxid) throws Exception{
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null);
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
        
        QuorumVerifier newLeaderQV = null;
        
        // In the DIFF case we don't need to do a snapshot because the transactions will sync on top of any existing snapshot
        //        // For SNAP and TRUNC the snapshot is needed to save that history
        // 在DIFF情况下，我们不需要执行快照，因为事务将在任何现有快照之上同步
        // 对于SNAP和TRUNC，需要快照来保存该历史记录
        boolean snapshotNeeded = true;
        // 这个如果是true就是 SNAP 类型发同步
        boolean syncSnapshot = false;
        readPacket(qp);
        Deque<Long> packetsCommitted = new ArrayDeque<>();
        Deque<PacketInFlight> packetsNotCommitted = new ArrayDeque<>();//收到proposal但是还未commit的包
        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {//接收diff，表示以diff方式与leader的数据同
                LOG.info("Getting a diff from the leader 0x{}", Long.toHexString(qp.getZxid()));
                snapshotNeeded = false;
            }
            else if (qp.getType() == Leader.SNAP) {//表示snap方式与leader同步,从leader复制一份镜像数据到本地内存
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.getZxid()));
                // The leader is going to dump the database
                // db is clear as part of deserializeSnapshot()
                zk.getZKDatabase().deserializeSnapshot(leaderIs);//从leader复制一份镜像数据到本地内存
                // ZOOKEEPER-2819: overwrite config node content extracted
                // from leader snapshot with local config, to avoid potential
                // inconsistency of config node content during rolling restart.
                if (!QuorumPeerConfig.isReconfigEnabled()) {
                    LOG.debug("Reset config node content from local config after deserialization of snapshot.");
                    zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
                }
                String signature = leaderIs.readString("signature");
                if (!signature.equals("BenWasHere")) {
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

                // immediately persist the latest snapshot when there is txn log gap
                syncSnapshot = true;
            } else if (qp.getType() == Leader.TRUNC) {//触发回滚，回滚到leader的lastzxid
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid());
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(ExitCode.QUORUM_PACKET_ERROR.getValue());
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.getZxid());

            }
            else {
                LOG.error("Got unexpected packet from leader: {}, exiting ... ",
                          LearnerHandler.packetToString(qp));
                System.exit(ExitCode.QUORUM_PACKET_ERROR.getValue());

            }
            zk.getZKDatabase().initConfigInZKDatabase(self.getQuorumVerifier());
            zk.createSessionTracker();            
            
            long lastQueued = 0;

            // in Zab V1.0 (ZK 3.4+) we might take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot on the UPDATE message, since Zab V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean isPreZAB1_0 = true;
            //If we are not going to take the snapshot be sure the transactions are not applied in memory
            // but written out to the transaction log
            boolean writeToTxnLog = !snapshotNeeded;
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) {//启动时数据同步,不断读取leader的数据，直到收到UPTODATE表示同步完成
                readPacket(qp);
                switch(qp.getType()) {
                case Leader.PROPOSAL://接收到提议
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr);
                    if (pif.hdr.getZxid() != lastQueued + 1) {
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid();
                    
                    if (pif.hdr.getType() == OpCode.reconfig){                
                        SetDataTxn setDataTxn = (SetDataTxn) pif.rec;       
                       QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
                       self.setLastSeenQuorumVerifier(qv, true);                               
                    }
                    
                    packetsNotCommitted.add(pif);//因为还没有commit，记录在notCommit的队列里面
                    break;
                case Leader.COMMIT://接收到COMMIT
                case Leader.COMMITANDACTIVATE:
                    pif = packetsNotCommitted.peekFirst();
                    if (pif.hdr.getZxid() == qp.getZxid() && qp.getType() == Leader.COMMITANDACTIVATE) {
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn) pif.rec).getData()));
                        boolean majorChange = self.processReconfig(qv, ByteBuffer.wrap(qp.getData()).getLong(),
                                qp.getZxid(), true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    }
                    if (!writeToTxnLog) {
                        if (pif.hdr.getZxid() != qp.getZxid()) {
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);//直接处理事务
                            packetsNotCommitted.remove();//从未COMMIT记录中删除对应记录
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.INFORM://observer才会拿到INFORM消息，来同步
                case Leader.INFORMANDACTIVATE:
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();

                    if (qp.getType() == Leader.INFORMANDACTIVATE) {
                        ByteBuffer buffer = ByteBuffer.wrap(qp.getData());
                        long suggestedLeaderId = buffer.getLong();
                        byte[] remainingdata = new byte[buffer.remaining()];
                        buffer.get(remainingdata);
                        packet.rec = SerializeUtils.deserializeTxn(remainingdata, packet.hdr);
                        QuorumVerifier qv = self.configFromString(new String(((SetDataTxn)packet.rec).getData()));
                        boolean majorChange =
                                self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
                        if (majorChange) {
                            throw new Exception("changes proposed in reconfig");
                        }
                    } else {
                        packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                        // Log warning message if txn comes out-of-order
                        if (packet.hdr.getZxid() != lastQueued + 1) {
                            LOG.warn("Got zxid 0x"
                                    + Long.toHexString(packet.hdr.getZxid())
                                    + " expected 0x"
                                    + Long.toHexString(lastQueued + 1));
                        }
                        lastQueued = packet.hdr.getZxid();
                    }
                    if (!writeToTxnLog) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }

                    break;                
                case Leader.UPTODATE://过半机器完成了leader验证，自己也完成了数据同步,可以跳出循环(表示过半机器已完成同步，可以对外工作)
                    LOG.info("Learner received UPTODATE message");                                      
                    if (newLeaderQV!=null) {
                       boolean majorChange =
                           self.processReconfig(newLeaderQV, null, null, true);
                       if (majorChange) {
                           throw new Exception("changes proposed in reconfig");
                       }
                    }
                    if (isPreZAB1_0) {
                        zk.takeSnapshot(syncSnapshot);
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.setZooKeeperServer(zk);
                    self.adminServer.setZooKeeperServer(zk);
                    break outerLoop;
                case Leader.NEWLEADER: // Getting NEWLEADER here instead of in discovery (leader告诉learner同步的相关请求已经发完了)
                    // means this is Zab 1.0
                   LOG.info("Learner received NEWLEADER message");
                   if (qp.getData()!=null && qp.getData().length > 1) {
                       try {                       
                           QuorumVerifier qv = self.configFromString(new String(qp.getData()));
                           self.setLastSeenQuorumVerifier(qv, true);
                           newLeaderQV = qv;
                       } catch (Exception e) {
                           e.printStackTrace();
                       }
                   }

                    //设置快照和当前epoch
                   if (snapshotNeeded) {
                       zk.takeSnapshot(syncSnapshot);
                   }
                   
                    self.setCurrentEpoch(newEpoch);
                    writeToTxnLog = true; //Anything after this needs to go to the transaction log, not applied directly in memory
                    // 此后的任何内容都需要转到事务日志，而不是直接在内存中应用
                    isPreZAB1_0 = false;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true);//最后再发一个ack
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        zk.startup();//启动服务器
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 在此更新选举投票，以确保整体的所有成员向启动的新服务器报告相同的投票，并将领导者选举通知发送到整体。
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        //更新选举投票，解决某个bug用的
        self.updateElectionVote(newEpoch);

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) {//如果是follower
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid); //进行commit
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.setTxn(p.rec);
                request.setHdr(p.hdr);
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }

    /**
     * {@see validateSession(ServerCnxn cnxn, long clientId, int timeout)}
     * 接收到了leader返回的REVALIDATE信息，进行验证处理
     *
     * @param qp
     * @throws IOException
     */
    protected void revalidate(QuorumPacket qp) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations.remove(sessionId);//验证完成，从map中移除
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            //完成session的初始化
            zk.finishSessionInit(cnxn, valid);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }

    //TODO: learner接收leader的ping命令时，返回LearnerSessionTracker的快照
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Map<Long, Integer> touchTable = zk.getTouchSnapshot();
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        self.setZooKeeperServer(null);
        self.closeAllConnections();
        self.adminServer.setZooKeeperServer(null);
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }

    boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }
}
