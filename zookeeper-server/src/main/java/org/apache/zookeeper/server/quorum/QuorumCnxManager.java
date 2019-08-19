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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import static org.apache.zookeeper.common.NetUtils.formatInetAddr;

/**
 * 每台服务器在启动的过程中，会启动一个QuorumPeerManager，负责各台服务器之间的底层Leader选举过程中的网络通信。
 *
 * 内部类
 *   SendWorker类作为网络IO的发送者，从发送队列取出，发给对应sid的机器
 *   Message类定义了消息结构，包含sid以及消息体ByteBuffer
 *   RecvWorker类作为网络IO的接受者
 *   Listener类作为electionPort端口的监听器，等待其他机器的连接
 * 属性
 *   recvQueue作为接受队列
 *   queueSendMap表示每个sid对应的发送的发送队列
 * 函数
 *   连接相关
 *   sender，recv生产消费相关
 *   其他
 *
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 *
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties.
 *
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 *
 */

public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;

    /*
     * Negative counter for observer server ids.
     */

    private AtomicLong observerCounter = new AtomicLong(-1);

    /*
     * Protocol identifier used among peers
     * 对等体之间使用的协议标识
     */
    public static final long PROTOCOL_VERSION = -65536L;

    /*
     * Max buffer size to be read from the network.
     */
    static public final int maxBuffer = 2048;

    /*
     * Connection time out value in milliseconds
     */

    private int cnxTO = 5000;

    final QuorumPeer self;

    /*
     * Local IP address
     */
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections
            .synchronizedSet(new HashSet<Long>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    /*
     * Counter to count connection processing threads.
     */
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    /*
     * Mapping from Peer to Thread number
     * 对每一个远程节点都会定义一个SendWorker
     * 发送器集合，按SID分组,每个SendWorker消息发送器对应一台远程zookeeper服务器，负责从对应的发送队列取出消息发送
     */
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    // 每个远程节点都会定义一个消息发型队列
    // 消息发送队列按SID分组，分别为集群中每台机器分配一个单独队列
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    // 每个远程节点最后发送的消息  为每个SID保留最近发送过的一个消息
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;
    /*
     * Reception queue
     * 本节点的消息接收队列
     * 消息接收队列只有一个
     */
    public final ArrayBlockingQueue<Message> recvQueue;
    /*
     * Object to synchronize access to recvQueue
     */
    private final Object recvQLock = new Object();

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    private AtomicInteger threadCnt = new AtomicInteger(0);

    /*
     * Socket options for TCP keepalive
     * 保持长连接
     */
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");

    // 这个类定义了server之间传输的消息结构
    static public class Message {
        // sid为消息来源方的sid，buffer即消息体
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    /*
     * This class parses the initial identification sent out by peers with their
     * sid & hostname.
     * 该类用sid和hostname解析对等体发出的初始标识。
     */
    static public class InitialMessage {
        public Long sid;
        public InetSocketAddress electionAddr;

        InitialMessage(Long sid, InetSocketAddress address) {
            this.sid = sid;
            this.electionAddr = address;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {
            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }
        }

        static public InitialMessage parse(Long protocolVersion, DataInputStream din)
            throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION) {
                throw new InitialMessageException(
                        "Got unrecognized protocol version %s", protocolVersion);
            }

            // 读取serverID
            sid = din.readLong();
            // 读取remaining的长度
            int remaining = din.readInt();
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException(
                        "Unreasonable buffer length: %s", remaining);
            }

            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException(
                        "Read only %s bytes out of %s sent by server %s",
                        num_read, remaining, sid);
            }
            // 解析出连接的地址
            String addr = new String(b);
            String[] host_port;
            try {
                // 端口 而且是两个端口
                host_port = ConfigUtils.getHostAndPort(addr);
            } catch (ConfigException e) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            if (host_port.length != 2) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            int port;
            try {
                // 拿到ip+端口，端口号为选举端口
                port = Integer.parseInt(host_port[1]);
            } catch (NumberFormatException e) {
                throw new InitialMessageException("Bad port number: %s", host_port[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new InitialMessageException("No port number in: %s", addr);
            }
            // host_port[0] ip地址
            // host_port[1] 端口号
            return new InitialMessage(sid, new InetSocketAddress(host_port[0], port));
        }
    }

    public QuorumCnxManager(QuorumPeer self,
                            final long mySid,
                            Map<Long,QuorumPeer.QuorumServer> view,
                            QuorumAuthServer authServer,
                            QuorumAuthLearner authLearner,
                            int socketTimeout,
                            boolean listenOnAllIPs,
                            int quorumCnxnThreadsSize,
                            boolean quorumSaslAuthEnabled) {
        // RECV_CAPACITY = 100
        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();

        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();

        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if(cnxToValue != null){
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;
        // 本节点的serverId
        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        // 配置文件中配置的服务器集合视图
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;

        initializeAuth(mySid, authServer, authLearner, quorumCnxnThreadsSize, quorumSaslAuthEnabled);

        // Starts listener thread that waits for connection requests
        listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

    private void initializeAuth(final long mySid,
                                final QuorumAuthServer authServer,
                                final QuorumAuthLearner authLearner,
                                final int quorumCnxnThreadsSize,
                                final boolean quorumSaslAuthEnabled) {
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;
        if (!this.quorumSaslAuthEnabled) {
            LOG.debug("Not initializing connection executor as quorum sasl auth is disabled");
            return;
        }

        // init connection executors
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup()
                : Thread.currentThread().getThreadGroup();
        ThreadFactory daemonThFactory = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r, "QuorumConnectionThread-"
                        + "[myid=" + mySid + "]-"
                        + threadIndex.getAndIncrement());
                return t;
            }
        };
        // 创建线程池
        this.connectionExecutor = new ThreadPoolExecutor(3,
                quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }


    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        LOG.debug("Opening channel to server " + sid);
        Socket sock = new Socket();
        setSockOpts(sock);
        sock.connect(self.getVotingView().get(sid).electionAddr, cnxTO);
        initiateConnection(sock, sid);
    }

    /**
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    public void initiateConnection(final Socket sock, final Long sid) {
        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error("Exception while connecting, id: {}, addr: {}, closing learner connection",
                    new Object[] { sid, sock.getRemoteSocketAddress() }, e);
            closeSocket(sock);
            return;
        }
    }

    /**
     * Server will initiate the connection request to its peer server
     * asynchronously via separate connection thread.
     */
    public void initiateConnectionAsync(final Socket sock, final Long sid) {
        if(!inprogressConnections.add(sid)){
            // simply return as there is a connection request to
            // server 'sid' already in progress.
            LOG.debug("Connection request to server id: {} is already in progress, so skipping this request",
                    sid);
            closeSocket(sock);
            return;
        }
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReqThread(sock, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            // Imp: Safer side catching all type of exceptions and remove 'sid'
            // from inprogress connections. This is to avoid blocking further
            // connection requests from this 'sid' in case of errors.
            inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            closeSocket(sock);
        }
    }

    /**
     * Thread to send connection request to peer server.
     * 线程向对等服务器发送连接请求。
     */
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final Socket sock;
        final Long sid;
        QuorumConnectionReqThread(final Socket sock, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.sock = sock;
            this.sid = sid;
        }

        @Override
        public void run() {
            try{
                initiateConnection(sock, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }
    }

    private boolean startConnection(Socket sock, Long sid)
            throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        try {
            // Use BufferedOutputStream to reduce the number of IP packets. This is
            // important for x-DC scenarios.
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

            // Sending id and challenge

            // represents protocol version (in other words - message type)
            dout.writeLong(PROTOCOL_VERSION);
            dout.writeLong(self.getId());
            String addr = formatInetAddr(self.getElectionAddress());
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();

            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // authenticate learner
        QuorumPeer.QuorumServer qps = self.getVotingView().get(sid);
        if (qps != null) {
            // TODO - investigate why reconfig makes qps null.
            authLearner.authenticate(sock, qps.hostname);
        }

        // If lost the challenge, then drop the new connection
        if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the " +
                     "connection: (" + sid + ", " + self.getId() + ")");
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if(vsw != null)
                vsw.finish();

            senderWorkerMap.put(sid, sw);
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(
                        SEND_CAPACITY));

            sw.start();
            rw.start();

            return true;

        }
        return false;
    }


    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     *
     */
    public void receiveConnection(final Socket sock) {
        // 根据socket创建输入流，然后进入handleConnection
        DataInputStream din = null;
        try {
            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));

            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * Server receives a connection request and handles it asynchronously via
     * separate thread.
     */
    public void receiveConnectionAsync(final Socket sock) {
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * Thread to receive connection request from peer server.
     * 用于从对等服务器接收连接请求的线程。
     */
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {
        private final Socket sock;
        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }
    }

    /**
     * handleConnection处理网络连接请求，通过输入流获取initMessage,然后根据sid判断是否通过连接申请。
     * 如果不通过则自己建立与远处节点连接，通过则初始化SendWorker和RecvWorker等线程
     *
     * 为了防止两台服务器有重复链接，zookeeper定义了规则，
     * 只能sid大的去连接sid小的。如果sid小的连接了sid大的，在连接处理程序中会断掉这条连接，然后重新发起连接。
     *
     * @param sock
     * @param din
     * @throws IOException
     */
    private void handleConnection(Socket sock, DataInputStream din) throws IOException {
        Long sid = null, protocolVersion = null;
        InetSocketAddress electionAddr = null;

        try {
            // 读取-协议版本
            protocolVersion = din.readLong();
            if (protocolVersion >= 0) { // this is a server id and not a protocol version
                // 这是服务器ID而不是协议版本
                sid = protocolVersion;
            } else {
                try {
                    // 获取serverID以及IP+选举端口
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    electionAddr = init.electionAddr;
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error(ex.toString());
                    closeSocket(sock);
                    return;
                }
            }

            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * Choose identifier at random. We need a value to identify the connection.
                 * 随机选择标识符。我们需要一个值来识别连接。
                 */
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer 将任意标识符设置为观察者: " + sid);
            }
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge: {}", e);
            closeSocket(sock);
            return;
        }

        // do authenticating learner 认证learner
        authServer.authenticate(sock, din);
        //If wins the challenge, then close the new connection.
        //如果远处节点sid比自己的sid小，挑战成功，则关闭socket，自己申请与远程节点连接
        if (sid < self.getId()) {
            // 如果 连接的serverId < 本机的serverId
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            //先关闭sendworker
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server 创建与服务器的新连接: {}", sid);
            closeSocket(sock);
            //自己重新申请连接
            if (electionAddr != null) {
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }

        } else { // Otherwise start worker threads to receive data.
            // 挑战失败使用连接启动worker线程，准备接收和发送选举消息
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                vsw.finish();
            }

            senderWorkerMap.put(sid, sw);
            // SEND_CAPACITY = 1
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));
            //这一点我有个疑问，可以看出上面会先用queueSendMap.containsKey(sid)判断原有sid
            //是不是已经有对应的消息发送队列，如果没有创建新的，意思是如果有就用旧的消息发送队
            //列，我的疑问就是这时旧的消息发送队列可能会包含上轮选举的旧消息，为什么这里不对它清
            //空呢，不清空会把旧的选票信息发给对应的sid服务器，虽然对选举结果没啥影响，但感觉清空
            //队列效率更高
            sw.start();
            rw.start();
        }
    }

    /**
     * Processes invoke this message to queue a message to send. Currently,
     * only leader election uses it.
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        //如果接受者是自己，直接放置到接收队列
        if (this.mySid == sid) {
             b.position(0);
             addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else {
            //否则发送到对应的发送队列上
             /*
              * Start a new connection if doesn't have one already.
              */
             ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(
                SEND_CAPACITY);
             ArrayBlockingQueue<ByteBuffer> oldq = queueSendMap.putIfAbsent(sid, bq);
             if (oldq != null) {
                 addToSendQueue(oldq, b);
             } else {
                 addToSendQueue(bq, b);
             }
            //连接申请
             connectOne(sid);

        }
    }

    /**
     * Try to establish a connection to server with id sid using its electionAddr.
     *
     *  @param sid  server id
     *  @return boolean success indication
     */
    synchronized private boolean connectOne(long sid, InetSocketAddress electionAddr){
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return true;
        }

        Socket sock = null;
        try {
            LOG.debug("Opening channel to server " + sid);
            if (self.isSslQuorum()) {
                 SSLSocket sslSock = self.getX509Util().createSSLSocket();
                 setSockOpts(sslSock);
                 sslSock.connect(electionAddr, cnxTO);
                 sslSock.startHandshake();
                 sock = sslSock;
                 LOG.info("SSL handshake complete with {} - {} - {}", sslSock.getRemoteSocketAddress(), sslSock.getSession().getProtocol(), sslSock.getSession().getCipherSuite());
             } else {
                 sock = new Socket();
                 setSockOpts(sock);
                 sock.connect(electionAddr, cnxTO);

             }
             LOG.debug("Connected to server " + sid);
            // Sends connection request asynchronously if the quorum
            // sasl authentication is enabled. This is required because
            // sasl server authentication process may take few seconds to
            // finish, this may delay next peer connection requests.
            if (quorumSaslAuthEnabled) {
                initiateConnectionAsync(sock, sid);
            } else {
                initiateConnection(sock, sid);
            }
            return true;
        } catch (UnresolvedAddressException e) {
            // Sun doesn't include the address that causes this
            // exception to be thrown, also UAE cannot be wrapped cleanly
            // so we log the exception in order to capture this critical
            // detail.
            LOG.warn("Cannot open channel to " + sid
                    + " at election address " + electionAddr, e);
            closeSocket(sock);
            throw e;
        } catch (X509Exception e) {
            LOG.warn("Cannot open secure channel to " + sid
                    + " at election address " + electionAddr, e);
            closeSocket(sock);
            return false;
        } catch (IOException e) {
            LOG.warn("Cannot open channel to " + sid
                            + " at election address " + electionAddr,
                    e);
            closeSocket(sock);
            return false;
        }
    }

    /**
     * Try to establish a connection to server with id sid.
     *
     *  @param sid  server id
     */
    synchronized void connectOne(long sid){
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return;
        }
        synchronized (self.QV_LOCK) {
            boolean knownId = false;
            // Resolve hostname for the remote server before attempting to
            // connect in case the underlying ip address has changed.
            self.recreateSocketAddresses(sid);
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr))
                    return;
            }
            if (lastSeenQV != null && lastProposedView.containsKey(sid)
                    && (!knownId || (lastProposedView.get(sid).electionAddr !=
                    lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                if (connectOne(sid, lastProposedView.get(sid).electionAddr))
                    return;
            }
            if (!knownId) {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
        }
    }


    /**
     * Try to establish a connection with each server if one doesn't exist.
     * 尝试与每个服务器建立连接（如果不存在）。
     */

    public void connectAll(){
        long sid;
        for(Enumeration<Long> en = queueSendMap.keys();
            en.hasMoreElements();){
            sid = en.nextElement();
            connectOne(sid);
        }
    }


    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

        // Wait for the listener to terminate.
        try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();

        // clear data structures used for auth
        if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }

    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish();
        }
    }

    /**
     * Helper method to set socket options.
     *
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(tcpKeepAlive);
        sock.setSoTimeout(self.tickTime * self.syncLimit);
    }

    /**
     * Helper method to close a socket.
     *
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return number of connection processing threads.
     */
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    /**
     * Reset the value of connection processing threads count to zero.
     */
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    /**
     * 继承ZooKeeperThread，主要监听electionPort，不断的接收外部连接
     * Listener主要监听本机配置的electionPort，不断的接收外部连接
     *
     * 为能互相投票，zookeeper集群的所有机器都需要两两建立起网络连接。QuorumCnxManager启动时，会创建一个ServerSocket来监听Leader选举的通信端口（默认端口是3888）。
     * Listener会首先被启动
     *
     * Thread to listen on some port
     */
    public class Listener extends ZooKeeperThread {

        volatile ServerSocket ss = null;

        public Listener() {
            // During startup of thread, thread name will be overridden to
            // specific election address
            super("ListenerThread");
        }

        /**
         * Lisener的run方式就是基于java io网络通信方式的监听
         *
         * Sleeps on accept().
         */
        @Override
        public void run() {
            int numRetries = 0;
            InetSocketAddress addr;
            Socket client = null;
            Exception exitException = null;
            // numRetries 重试次数不能超过3次
            while((!shutdown) && (numRetries < 3)){
                try {
                    if (self.shouldUsePortUnification()) {
                        LOG.info("Creating TLS-enabled quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), true);
                    } else if (self.isSslQuorum()) {
                        LOG.info("Creating TLS-only quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), false);
                    } else {
                        ss = new ServerSocket();
                    }

                    ss.setReuseAddress(true);
                    if (self.getQuorumListenOnAllIPs()) {
                        int port = self.getElectionAddress().getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                        // Resolve hostname for this server in case the
                        // underlying ip address has changed.
                        self.recreateSocketAddresses(self.getId());
                        addr = self.getElectionAddress();
                    }
                    // 选举端口  配置文件中serverXXX中的第二个端口是选举端口
                    LOG.info("我的选举绑定端口 My election bind port: " + addr.toString());
                    setName(addr.toString());
                    ss.bind(addr);
                    while (!shutdown) {
                        try {
                            //不断接受连接
                            client = ss.accept();
                            setSockOpts(client);
                            LOG.info("Received connection request "
                                     + formatInetAddr((InetSocketAddress)client.getRemoteSocketAddress()));
                            // Receive and handle the connection request
                            // asynchronously if the quorum sasl authentication is
                            // enabled. This is required because sasl server
                            // authentication process may take few seconds to finish,
                            // this may delay next peer connection requests.
                            /*接收到其他服务器TCP连接请求时交由receiveConnection处理*/
                            //验证方式
                            if (quorumSaslAuthEnabled) {
                                receiveConnectionAsync(client);
                            } else {
                                //非验证方式
                                receiveConnection(client);
                            }
                            numRetries = 0;
                        } catch (SocketTimeoutException e) {
                            LOG.warn("The socket is listening for the election accepted "
                                     + "and it timed out unexpectedly, but will retry."
                                     + "see ZOOKEEPER-2836");
                        }
                    }
                } catch (IOException e) {
                    if (shutdown) {
                        break;
                    }
                    LOG.error("Exception while listening", e);
                    exitException = e;
                    numRetries++;
                    try {
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                            "Ignoring exception", ie);
                    }
                    closeSocket(client);
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread, "
                        + "I won't be able to participate in leader "
                        + "election any longer: "
                        + formatInetAddr(self.getElectionAddress()));
                if (exitException instanceof BindException) {
                    // After leaving listener thread, the host cannot join the
                    // quorum anymore, this is a severe error that we cannot
                    // recover from, so we need to exit
                    System.exit(ExitCode.UNABLE_TO_BIND_QUORUM_PORT.getValue());
                }
            } else if (ss != null) {
                // Clean up for shutdown.
                try {
                    ss.close();
                } catch (IOException ie) {
                    // Don't log an error for shutdown.
                    LOG.debug("Error closing server socket", ie);
                }
            }
        }

        /**
         * Halts this listener thread.
         */
        void halt(){
            try{
                LOG.debug("Trying to close listener: " + ss);
                if(ss != null) {
                    LOG.debug("Closing listener: "
                              + QuorumCnxManager.this.mySid);
                    ss.close();
                }
            } catch (IOException e){
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * “发送者”，继承ZooKeeperThread，线程不断地从发送队列取出，发给对应sid的机器
     *
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     * 线程发送消息。实例等待队列，并在有可用消息时立即发送消息。如果连接断开，则打开一个新的。
     */
    class SendWorker extends ZooKeeperThread {
        Long sid; //目标机器sid，不是当前机器sid
        Socket sock;
        RecvWorker recvWorker; //该sid对应的RecvWorker
        volatile boolean running = true;
        DataOutputStream dout;

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         * 此线程的一个实例接收通过队列发送的消息，并将它们发送到服务器sid。
         *
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                // Socket输出流
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         *
         * @return RecvWorker
         */
        synchronized RecvWorker getRecvWorker(){
            return recvWorker;
        }

        synchronized boolean finish() {
            LOG.debug("Calling finish for " + sid);

            if(!running){
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }

            running = false;
            closeSocket(sock);

            this.interrupt();
            if (recvWorker != null) {
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid=" + sid);

            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }

        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        // run方法
        // 在SendWorker中，一旦Zookeeper发现针对当前服务器的消息发送队列为空，
        // 那么此时需要从lastMessageSent中取出一个最近发送过的消息来进行再次发送，
        // 这是为了解决接收方在消息接收前或者接收到消息后服务器挂了，导致消息尚未被正确处理。
        // 同时，Zookeeper能够保证接收方在处理消息时，会对重复消息进行正确的处理。
        @Override
        public void run() {
            threadCnt.incrementAndGet();//线程数+1
            try {
                /**
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);//找到sid对应需要send的队列
                if (bq == null || isSendQueueEmpty(bq)) {
                   ByteBuffer b = lastMessageSent.get(sid);//如果没有什么发的，就把上一次发的再发一遍(重发能够正确处理)
                   if (b != null) {
                       LOG.debug("Attempting to send lastMessage to sid=" + sid);
                       send(b);
                   }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }

            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap
                                .get(sid);
                        if (bq != null) {
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);//从发送队列里面取出消息
                        } else {//队列没有记录在map中
                            LOG.error("No queue of incoming messages for " +
                                      "server " + sid);
                            break;
                        }

                        if(b != null){
                            lastMessageSent.put(sid, b);//更新最后一次发送的
                            send(b);//发送
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid
                         + " my id = " + QuorumCnxManager.this.mySid
                         + " error = " + e);
            }
            this.finish();
            LOG.warn("Send worker leaving thread " + " id " + sid + " my id = " + self.getId());
        }
    }

    /**
     * “接受者”，类似SendWorker，继承ZooKeeperThread，线程不断地从网络IO中读取数据，放入接收队列
     *
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     * 用于接收消息的线程。实例等待套接字读取。如果通道中断，则将其自身从接收器池中移除。
     */
    class RecvWorker extends ZooKeeperThread {
        Long sid;//来源方sid
        Socket sock;
        volatile boolean running = true;
        // Socket的输入流
        final DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if(!running){
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    int length = din.readInt();//获取长度
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    final byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    // 解析出byteBuffer并添加到队列中去
                    addToRecvQueue(new Message(ByteBuffer.wrap(msgArray), sid));
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = "
                         + QuorumCnxManager.this.mySid + ", error = " , e);
            } finally {
                LOG.warn("Interrupting SendWorker");
                sw.finish();
                closeSocket(sock);
            }
        }
    }

    /**
     * Inserts an element in the specified queue. If the Queue is full, this
     * method removes an element from the head of the Queue and then inserts
     * the element at the tail. It can happen that the an element is removed
     * by another thread in
     * method before this method attempts to remove an element from the queue.
     * This will cause {@link ArrayBlockingQueue#remove() remove} to throw an
     * exception, which is safe to ignore.
     *
     * Unlike {@link #addToRecvQueue(Message) addToRecvQueue} this method does
     * not need to be synchronized since there is only one thread that inserts
     * an element in the queue and another thread that reads from the queue.
     *
     * @param queue
     *          Reference to the Queue
     * @param buffer
     *          Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          ByteBuffer buffer) {
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
          long timeout, TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts
     * the element at the tail of the queue.
     *
     * This method is synchronized to achieve fairness between two threads that
     * are trying to insert an element in the queue. Each thread checks if the
     * queue is full, then removes the element at the head of the queue, and
     * then inserts an element at the tail. This three-step process is done to
     * prevent a thread from blocking while inserting an element in the queue.
     * If we do not synchronize the call to this method, then a thread can grab
     * a slot in the queue created by the second thread. This can cause the call
     * to insert by the second thread to fail.
     * Note that synchronizing this method does not block another thread
     * from polling the queue since that synchronization is provided by the
     * queue itself.
     *
     * @param msg
     *          Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(Message msg) {
        synchronized(recvQLock) {
            if (recvQueue.remainingCapacity() == 0) {
                try {
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                    // element could be removed by poll()
                     LOG.debug("Trying to remove from an empty " +
                         "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                recvQueue.add(msg);
            } catch (IllegalStateException ie) {
                // This should never happen
                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(long timeout, TimeUnit unit)
       throws InterruptedException {
       return recvQueue.poll(timeout, unit);
    }

    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }
}
