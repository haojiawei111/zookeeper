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

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Leader选举小结
 * 1.server启动时默认选举自己，并向整个集群广播
 * 2.收到消息时，通过3层判断：逻辑时钟，zxid，server id大小判断是否同意对方，如果同意，则修改自己的选票，并向集群广播
 * 3.QuorumCnxManager负责IO处理，每2个server建立一个连接，只允许id大的server连id小的server，每个server启动单独的读写线程处理，使用阻塞IO
 * 4.默认超过半数机器同意时，则选举成功，修改自身状态为LEADING或FOLLOWING
 * 5.Obserer机器不参与选举
 *
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */

// 其是标准的fast paxos算法的实现，基于TCP协议进行选举
// FastLeaderElection实现了Election接口，其需要实现接口中定义的lookForLeader方法和shutdown方法，其是标准的Fast Paxos算法的实现，各服务器之间基于TCP协议进行选举。
// FastLeaderElection有三个较为重要的内部类，分别为Notification、ToSend、Messenger
    //其维护了服务器之间的连接（用于发送消息）、发送消息队列、接收消息队列、推选者的一些信息（zxid、id）、是否停止选举流程标识等。
public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
     // 完成Leader选举之后需要等待时长
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */
    // 两个连续通知检查之间的最大时长
    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL =
            "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL =
            "zookeeper.fastleader.maxNotificationInterval";

    static {
        // "zookeeper.fastleader.minNotificationInterval"
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL,
                minNotificationInterval);
        LOG.info("{}={}", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        // "zookeeper.fastleader.maxNotificationInterval"
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL,
                maxNotificationInterval);
        LOG.info("{}={}", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */
    // 管理服务器之间的连接
    // zookeeper选举网络结构
    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */
    //Notification表示收到的选举投票信息（其他服务器发来的选举投票信息），其包含了被选举者的id、zxid、选举周期等信息，
    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader提议的领导者
         */
        // 被推选的leader的id
        long leader;

        /*
         * zxid of the proposed leader
         */
        // 被推选的leader的事务id
        long zxid;

        /*
         * Epoch
         */
        // 推选者的选举周期
        long electionEpoch;

        /*
         * current state of sender
         */
        // 推选者的状态
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        // 推选者的id
        long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        // 被推选者的选举周期
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    // ToSend表示发送给其他服务器的选举投票信息，也包含了被选举者的id、zxid、选举周期等信息。
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
                long leader,
                long zxid,
                long electionEpoch,
                ServerState state,
                long sid,
                long peerEpoch,
                byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        //被推举的leader的id
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        // 被推举的leader的最大事务id
        long zxid;

        /*
         * Epoch
         */
        // 推举者的选举周期
        long electionEpoch;

        /*
         * Current state;
         */
        // 推举者的状态
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        // 推举者的id
        long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         * 用于发送QuorumVerifier（配置信息）
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch
         */
        // 被推举的leader的选举周期
        long peerEpoch;
    }

    // 选票发送队列，用于保存待发送的选票
    LinkedBlockingQueue<ToSend> sendqueue;
    // 选票接收队列，用于保存接收到的外部投票
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */
    //Messenger包含了WorkerReceiver和WorkerSender两个内部类
    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
         /*选票接收器线程，不断从QuorumCnxManager获取其他服务器发来的选举消息，
        并将其转换成一个选票，保存到recvqueque，如果当前状态不为looking，即已
        经选出leader，将leader信息发回*/
        class WorkerReceiver extends ZooKeeperThread  {
            // 是否终止
            volatile boolean stop;
            // 服务器之间的连接
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                // 响应
                Message response;
                while (!stop) {// 不终止
                    // Sleeps on receive
                    try {
                        // 从recvQueue中取出一个选举投票消息（从其他服务器发送过来）
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        // 无投票，跳过
                        if(response == null) continue;

                        // The current protocol and two previous generations all send at least 28 bytes
                        // 当前协议和前两代都发送至少28个字节
                        if (response.buffer.capacity() < 28) {
                            LOG.error("Got a short response得到了简短的回复: " + response.buffer.capacity());
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        // 这是ZK-107之前的后向兼容模式它是我们没有发送对等时期的协议版本，对于对等时期和版本，消息变为40字节
                        boolean backCompatibility28 = (response.buffer.capacity() == 28);

                        // this is the backwardCompatibility mode for no version information
                        // 这是没有版本信息的向后兼容模式
                        boolean backCompatibility40 = (response.buffer.capacity() == 40);
                        // 简单理解就是复位（Reset） 但不清除数据（position=0, limit=capacity）
                        response.buffer.clear();

                        // Instantiate Notification and set its attributes实例化通知并设置其属性
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        if (!backCompatibility28) {
                            rpeerepoch = response.buffer.getLong();
                            if (!backCompatibility40) {
                                /*
                                 * Version added in 3.4.6
                                 */

                                version = response.buffer.getInt();
                            } else {
                                LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                            }
                        } else {
                            LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                            rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                        }

                        QuorumVerifier rqv = null;

                        // check if we have a version that includes config. If so extract config info from message.
                        if (version > 0x1) {
                            int configLength = response.buffer.getInt();
                            byte b[] = new byte[configLength];

                            response.buffer.get(b);

                            synchronized(self) {
                                try {
                                    rqv = self.configFromString(new String(b));
                                    QuorumVerifier curQV = self.getQuorumVerifier();
                                    if (rqv.getVersion() > curQV.getVersion()) {
                                        LOG.info("{} Received version: {} my version: {}", self.getId(),
                                                Long.toHexString(rqv.getVersion()),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                        if (self.getPeerState() == ServerState.LOOKING) {
                                            LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                            self.processReconfig(rqv, null, null, false);
                                            if (!rqv.equals(curQV)) {
                                                LOG.info("restarting leader election");
                                                self.shuttingDownLE = true;
                                                self.getElectionAlg().shutdown();

                                                break;
                                            }
                                        } else {
                                            LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                               } catch (ConfigException e) {
                                   LOG.error("Something went wrong while processing config received from {}", response.sid);
                               }
                            }
                        } else {
                            LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                        }

                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if(!validVoter(response.sid)) {// 当前的投票者集合不包含服务器
                            // 获取自己的投票
                            Vote current = self.getCurrentVote();
                            // 构造ToSend消息
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());
                            // 放入sendqueue队列，等待发送
                            sendqueue.offer(notmsg);
                        } else {// 包含服务器，表示接收到该服务器的选票消息
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            // State of peer that sent this message发送此消息的对等状态
                            // 推选者的状态
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {// 读取状态
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }
                            // 获取leader的id
                            n.leader = rleader;
                            // 获取zxid
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            // 设置服务器的id
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if(LOG.isInfoEnabled()){
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             * 如果此服务器正在查找，则发送提议的领导者
                             */

                            if(self.getPeerState() == QuorumPeer.ServerState.LOOKING){ // 本服务器为LOOKING状态
                                // 将消息放入recvqueue中
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if((ackstate == QuorumPeer.ServerState.LOOKING)// 推选者服务器为LOOKING状态
                                        && (n.electionEpoch < logicalclock.get())){// 选举周期小于逻辑时钟
                                    // 创建新的投票
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    // 构造新的发送消息（本服务器自己的投票）
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    // 将发送消息放置于队列，等待发送
                                    sendqueue.offer(notmsg);
                                }
                            } else {// 推选服务器状态不为LOOKING
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                // 获取当前投票
                                Vote current = self.getCurrentVote();
                                if(ackstate == QuorumPeer.ServerState.LOOKING){ // 为LOOKING状态
                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }
                                    if(LOG.isDebugEnabled()){
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                                self.getId(),
                                                response.sid,
                                                Long.toHexString(current.getZxid()),
                                                current.getId(),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    // 构造ToSend消息
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    // 将发送消息放置于队列，等待发送
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        // WorkerSender也实现了Runnable接口，为选票发送器，其会不断地从sendqueue中获取待发送的选票，并将其传递到底层QuorumCnxManager中，
        // 其过程是将FastLeaderElection的ToSend转化为QuorumCnxManager的Message。
        /*选票发送器线程，发送选票。负责把选票转化为消息，放入QuorumCnxManager的发送队列，如果是投给自己的，直接放入接收队列recvqueue*/
        class WorkerSender extends ZooKeeperThread {
            // 是否终止
            volatile boolean stop;
            // 服务器之间的连接
            QuorumCnxManager manager;
            // 构造器
            WorkerSender(QuorumCnxManager manager){
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {// 不终止
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);// 从sendqueue中取出ToSend消息
                        // 若为空，则跳过
                        if(m == null) continue;
                        // 不为空，则进行处理
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                 // 构建消息
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                                                    m.leader,
                                                    m.zxid,
                                                    m.electionEpoch,
                                                    m.peerEpoch,
                                                    m.configData);
                // 发送消息
                manager.toSend(m.sid, requestBuffer);

            }
        }
        // 选票发送器
        WorkerSender ws;
        // 选票接收器
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * 创建WorkerSender和WorkerReceiver两个线程
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {
            // 创建WorkerSender
            this.ws = new WorkerSender(manager);
            // 新创建线程
            this.wsThread = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);// 设置为守护线程

            // 创建WorkerReceiver
            this.wr = new WorkerReceiver(manager);
            // 创建线程
            this.wrThread = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);// 设置为守护线程
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        // 会启动WorkerSender和WorkerReceiver
        void start(){
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt(){
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }
    // 投票者
    QuorumPeer self;
    //选票发送器和接收器线程
    Messenger messenger;
    // 逻辑时钟
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    // 推选的leader的id
    long proposedLeader;
    // 推选的leader的zxid
    long proposedZxid;
    // 推选的leader的选举周期
    long proposedEpoch;

    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock.get();
    }

    // buildMsg方法将选举信息封装至ByteBuffer中再进行发送。
    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch,
            byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager){
        this.stop = false;
        this.manager = manager;
        // 初始化其他信息
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        // 赋值，对Leader和投票者的ID进行初始化操作
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        // 初始化发送队列
        sendqueue = new LinkedBlockingQueue<ToSend>();
        // 初始化接收队列
        recvqueue = new LinkedBlockingQueue<Notification>();
        // 创建Messenger，不会启动接收器和发送器线程
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager(){
        return manager;
    }
    // 是否停止选举
    volatile boolean stop;
    public void shutdown(){
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     * 在我们的投票发生变化时向所有同行发送通知
     */
    // 其会遍历所有的参与者投票集合，然后将自己的选票信息发送至上述所有的投票者集合，其并非同步发送，而是将ToSend消息放置于sendqueue中，之后由WorkerSender进行发送。
    private void sendNotifications() {
        for (long sid : self.getCurrentAndNextConfigVoters()) {// 遍历投票参与者集合
            QuorumVerifier qv = self.getQuorumVerifier();
            // 构造发送消息
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch, qv.toString().getBytes());
            if(LOG.isDebugEnabled()){
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x"  +
                      Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get())  +
                      " (n.round), " + sid + " (recipient), " + self.getId() +
                      " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 将发送消息放置于队列
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, " +
                        "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                self.getPeerState(), n.sid, n.state, n.leader, Long.toHexString(n.electionEpoch),
                Long.toHexString(n.peerEpoch), Long.toHexString(n.zxid), Long.toHexString(n.version),
                (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));
    }


    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param id    Server identifier
     * @param zxid  Last zxid observed by the issuer of this vote
     */
    // 该函数将接收的投票与自身投票进行PK，查看是否消息中包含的服务器id是否更优，其按照epoch、zxid、id的优先级进行PK
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if(self.getQuorumVerifier().getWeight(newId) == 0){ // 使用计票器判断当前服务器的权重是否为0
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        // 1. 判断消息里的epoch是不是比当前的大，如果大则消息中id对应的服务器就是leader
        // 2. 如果epoch相等则判断zxid，如果消息里的zxid大，则消息中id对应的服务器就是leader
        // 3. 如果前面两个都相等那就比较服务器id，如果大，则其就是leader
        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self
                        .getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    // 该函数检查是否已经完成了Leader的选举，此时Leader的状态应该是LEADING状态。
    protected boolean checkLeader(
            Map<Long, Vote> votes,
            long leader,
            long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()){// 自己不为leader
            if(votes.get(leader) == null) predicate = false;// 还未选出leader
            else if(votes.get(leader).getState() != ServerState.LEADING) predicate = false;// 选出的leader还未给出ack信号，其他服务器还不知道leader
        } else if(logicalclock.get() != electionEpoch) {// 逻辑时钟不等于选举周期
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch){
        if(LOG.isDebugEnabled()){
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote(){
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT){
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        }
        else{
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *  即是获取选谁，id就是myid里指定的那个数字，所以说一定要唯一
     * @return long
     */
    private long getInitId(){
        if(self.getQuorumVerifier().getVotingMembers().containsKey(self.getId()))
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
        	try {
        		return self.getCurrentEpoch();
        	} catch(IOException e) {
        		RuntimeException re = new RuntimeException(e.getMessage());
        		re.setStackTrace(e.getStackTrace());
        		throw re;
        	}
        else return Long.MIN_VALUE;
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     * 根据给定的proposedLeader更新对等状态。如果它成为领导者，还要更新* leadingVoteSet。
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        ServerState ss = (proposedLeader == self.getId()) ?
                ServerState.LEADING: learningState();
        self.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     * 开始新一轮领导人选举。
     * 每当我们的QuorumPeer 将其状态更改为LOOKING时，都会调用此方法，并且会向所有其他对等方发送通知。
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
           self.start_fle = Time.currentElapsedTime();
        }
        try {
            //用于选票归档
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = minNotificationInterval;

            synchronized(this){
                // TODO：逻辑时钟加一
                //logicalclock代表该当前机器的逻辑时钟（初始为0），每次进行一次leader选举，就会加一
                logicalclock.incrementAndGet();
                //初始化选票，投给自己，
                // getInitLastLoggedZxid得到的是该服务器已经处理的事务的最大zxid
                // getPeerEpoch得到的是该服务器的选举轮次
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            //初始化选票后发给所有服务器
            sendNotifications();

            SyncedLearnerTracker voteSet;

            /*
             * Loop in which we exchange notifications until we find a leader
             * 在我们找到领导者之前我们交换通知的循环
             */

            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)){
                /*
                 * 从 recvqueue获得来自所有机器的投票
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                Notification n = recvqueue.poll(notTimeout,
                        TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                //如果没有获得投票
                if(n == null){
                    //如果与其他服务器的连接仍然保持，重新发送投票
                    if(manager.haveDelivered()){
                        sendNotifications();
                    } else {
                        //连接失效，重新建立连接。选举刚开始的时候就是这样建立连接的
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    //修改超时参数...
                    int tmpTimeOut = notTimeout*2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    //否则处理选票
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    switch (n.state) {
                        // 状态
                    case LOOKING:
                        if (getInitLastLoggedZxid() == -1) {
                            LOG.debug("Ignoring notification as our zxid is -1。忽略通知，因为我们的zxid是-1");
                            break;
                        }
                        if (n.zxid == -1) {
                            LOG.debug("Ignoring notification from member with -1 zxid" + n.sid);
                            break;
                        }
                        // If notification > current, replace and send messages out
                        // 外部选票的逻辑时钟大于当前机器的逻辑时钟
                        if (n.electionEpoch > logicalclock.get()) {
                            //更新当前机器的logicalclock（意味着当前机器逻辑时钟与集群一致了）并且清空接收的选票
                            logicalclock.set(n.electionEpoch);
                            recvset.clear();
                            //选票PK，外部投票更新，则变更自己的投票。
                            if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                //变更选票
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                //不变更选票
                                updateProposal(getInitId(),
                                        getInitLastLoggedZxid(),
                                        getPeerEpoch());
                            }
                            //敲黑板*******************不管变不变更选票，logicalclock都变了，
                            //**********它会影响选票的electionEpoch，所以再将内部投票发送出去
                            sendNotifications();
                        } else if (n.electionEpoch < logicalclock.get()) {
                            // 小于当前机器的逻辑时钟，直接丢弃
                            if(LOG.isDebugEnabled()){
                                LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                        + Long.toHexString(n.electionEpoch)
                                        + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                            }
                            break;
                         //等于当前机器的逻辑时钟，直接PK,外部选票PK赢了则更新内部投票并将内部投票发送出去
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                proposedLeader, proposedZxid, proposedEpoch)) {
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            sendNotifications();
                        }

                        if(LOG.isDebugEnabled()){
                            LOG.debug("Adding vote: from=" + n.sid +
                                    ", proposed leader=" + n.leader +
                                    ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                    ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                        }
                        //将收到的选票归档，<sid, 选票>
                        //敲黑板*******************，recvset是HashMap<Long, Vote>，这里的key是投票发出方的服务器的id，
                        //**************也就是说一个服务器只会在recvset保留一条记录，这就是为什么一台服务器可以多次发出投票
                        // don't care about the version if it's in LOOKING state
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                        voteSet = getVoteTracker(
                                recvset, new Vote(proposedLeader, proposedZxid,
                                        logicalclock.get(), proposedEpoch));

                        //统计投票，决定是否终止投票,终止的条件是集群中过半的服务器认可了当前的内部投票，否则回到上面
                        //的while循环，继续循环
                        if (voteSet.hasAllQuorums()) {

                            // Verify if there is any change in the proposed leader
                            while((n = recvqueue.poll(finalizeWait,
                                    TimeUnit.MILLISECONDS)) != null){
                                if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        proposedLeader, proposedZxid, proposedEpoch)){
                                    recvqueue.put(n);
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                            //没有收到更优的投票
                            if (n == null) {
                                //设置状态，如果leader是自己，状态为Leading
                                //如果leader是其他节点，状态可能为observing或者following
                                setPeerState(proposedLeader, voteSet);
                                Vote endVote = new Vote(proposedLeader,
                                        proposedZxid, logicalclock.get(), 
                                        proposedEpoch);
                                //清空接收队列recvqueue，返回投票选举结果
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        break;
                    case OBSERVING:
                        LOG.debug("Notification from observer 来自观察员的通知: " + n.sid);
                        break;
                     //这两种情况发生在集群中本来就已经存在一个leader了，可能本台机器刚刚启动进入leader选举
                     //或刚因为网络延迟与leader断开然后重新进入选举
                    case FOLLOWING:
                    case LEADING:
                        //除了做出过半判断，同时还要检查leader是否给自己发送过投票信息，从投票信息中确认该
                        //leader是不是LEADING状态(防止出现时间差)。

                        /*
                         * Consider all notifications from the same epoch together.
                         * 同时考虑来自同一时期的所有通知。
                         */
                        //如果当前leader是真的有效，那一定有过半的机器（followers和leader）都会发来指向leade的选票

                        /* 同一轮投票选出leader，那么判断是不是半数以上的服务器都选举同一个leader，
                         *如果是设置角色并退出选举 */
                        if(n.electionEpoch == logicalclock.get()){
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                            voteSet = getVoteTracker(recvset, new Vote(n.version, 
                                      n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            if (voteSet.hasAllQuorums() && 
                                    checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                setPeerState(n.leader, voteSet);
                                Vote endVote = new Vote(n.leader, 
                                        n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }

                        /*
                         * Before joining an established ensemble, verify that
                         * a majority are following the same leader.
                         */
                        /* 非同一轮次，例如宕机很久的机器重新启动/某个节点延迟很大变为looking，需要收集过半选票。*/

                        /*例如本机服务器是刚启动的，则我的logicalclock一定为1，假设这时候集群已经选好了leader了，
                         *这时候本机一定收不到带looking状态的选票，所以无法更新自己的logicalclock，因此我们将这种
                         *的选票不在放入recvset，而是放入outofelection，当收到过半选票时，更新本机的logicalclock
                         *与集群同步，大家都在同一逻辑时钟并设置好对应角色退出选举*/
                        outofelection.put(n.sid, new Vote(n.version, n.leader,
                                n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                        voteSet = getVoteTracker(outofelection, new Vote(n.version, 
                                n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

                        if (voteSet.hasAllQuorums() &&
                                checkLeader(outofelection, n.leader, n.electionEpoch)) {
                            synchronized(this){
                                logicalclock.set(n.electionEpoch);
                                setPeerState(n.leader, voteSet);
                            }
                            Vote endVote = new Vote(n.leader, n.zxid, 
                                    n.electionEpoch, n.peerEpoch);
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;
                    default:
                        LOG.warn("通知状态未被记录 Notification state unrecoginized: " + n.state
                              + " (n.state), " + n.sid + " (n.sid)");
                        break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("从sid {}忽略非集群成员sid {}的通知 Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("从非仲裁成员sid {}忽略sid {}的通知 Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}",
                    manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     * 检查给定的sid是否在当前或下一个投票视图中表示
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }
}
