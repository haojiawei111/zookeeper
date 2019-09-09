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

package org.apache.zookeeper.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.apache.zookeeper.server.command.NopCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 */
//NIOServerCnxn继承了ServerCnxn抽象类，使用NIO来处理与客户端之间的通信，使用单线程处理
public class NIOServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    // ServerCnxn工厂
    private final NIOServerCnxnFactory factory;
    // 针对面向流的连接套接字的可选择通道
    private final SocketChannel sock;

    private final SelectorThread selectorThread;
    // 表示 SelectableChannel 在 Selector 中注册的标记
    private final SelectionKey sk;
    // 初始化标志
    private boolean initialized;
    // 分配四个字节缓冲区
    private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);
    // 赋值incomingBuffer
    private ByteBuffer incomingBuffer = lenBuffer;
    // 缓冲队列
    private final Queue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<ByteBuffer>();
    // 会话超时时间
    private int sessionTimeout;

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     */
    // 会话ID
    private long sessionId;

    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
                         SelectionKey sk, NIOServerCnxnFactory factory,
                         SelectorThread selectorThread) throws IOException {
        super(zk);
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        this.selectorThread = selectorThread;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not block */
        // 设置linger为false，以便在socket关闭时不会阻塞
        sock.socket().setSoLinger(false, -1);
        // 获取IP地址
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        // 认证信息中添加IP地址
        addAuthInfo(new Id("ip", addr.getHostAddress()));
        this.sessionTimeout = factory.sessionlessCnxnTimeout;
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in
            * a tight while loop
            */
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.configureBlocking(true);
                   sock.write(bb);
               }
               packetSent();
           }
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }

    /**
     * sendBuffer pushes a byte buffer onto the outgoing buffer queue for asynchronous writes.
     * sendBuffer将字节缓冲区推送到传出缓冲区队列以进行异步发送。
     */
    public void sendBuffer(ByteBuffer... buffers) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                      + " is valid: " + sk.isValid());
        }
        synchronized (outgoingBuffers) {
            for (ByteBuffer buffer : buffers) {
                outgoingBuffers.add(buffer);
            }
            outgoingBuffers.add(packetSentinel);
        }
        requestInterestOpsUpdate();
    }

    /**
     * 有两种情况会调用此方法:
     * 1.根据lengthBuffer的值为incomingBuffer分配空间后,此时尚未将数据从socketChannel读取至incomingBuffer中
     * 2.已经将数据从socketChannel中读取至incomingBuffer,且读取完毕
     * <p>
     * Read the request payload (everything following the length prefix) 读取请求有效负载（长度前缀后面的所有内容）
     */
    private void readPayload() throws IOException, InterruptedException, ClientCnxnLimitException {
        if (incomingBuffer.remaining() != 0) { // have we read length bytes?
            //对应情况1,此时刚为incomingBuffer分配空间,incomingBuffer为空,进行一次数据读取
            //(1)若将incomingBuffer读满,则直接进行处理;
            //(2)若未将incomingBuffer读满,则说明此次发送的数据不能构成一个完整的请求,则等待下一次数据到达后调用doIo()时再次将数据
            //从socketChannel读取至incomingBuffer
            int rc = sock.read(incomingBuffer); // sock is non-blocking, so ok
            if (rc < 0) {
                ServerMetrics.getMetrics().CONNECTION_DROP_COUNT.add(1);
                throw new EndOfStreamException(
                        "Unable to read additional data from client sessionid 0x"
                        + Long.toHexString(sessionId)
                        + ", likely client has closed socket");
            }
        }

        if (incomingBuffer.remaining() == 0) { // have we read length bytes?
            //不管是情况1还是情况2,此时incomingBuffer已读满,其中内容必是一个request,处理该request
            incomingBuffer.flip();
            //更新统计值
            packetReceived(4 + incomingBuffer.remaining());
            if (!initialized) {
                //处理连接请求
                readConnectRequest();
            } else {
                //处理普通请求
                readRequest();
            }
            //请求处理结束,重置lenBuffer和incomingBuffer
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * This boolean tracks whether the connection is ready for selection or
     * not. A connection is marked as not ready for selection while it is
     * processing an IO request. The flag is used to gatekeep pushing interest
     * op updates onto the selector.
     * 此布尔值跟踪连接是否已准备好进行选择。
     * 在处理IO请求时，连接被标记为未准备好进行选择。该标志用于将兴趣操作更新推送到选择器上。
     */
    private final AtomicBoolean selectable = new AtomicBoolean(true);

    public boolean isSelectable() {
        return sk.isValid() && selectable.get();
    }

    public void disableSelectable() {
        selectable.set(false);
    }

    public void enableSelectable() {
        selectable.set(true);
    }

    private void requestInterestOpsUpdate() {
        if (isSelectable()) {
            selectorThread.addInterestOpsUpdateRequest(sk);
        }
    }

    /**
     * 当{@link #sock}可写时调用该方法
     *
     * @param  k {@link #sock}关联的SelectionKey
     * @throws IOException
     * @throws CloseRequestException
     */
    void handleWrite(SelectionKey k) throws IOException, CloseRequestException {
        if (outgoingBuffers.isEmpty()) {
            return;
        }

        /*
         * 使用其执行高效的socket I/O,
         * <p>
         * This is going to reset the buffer position to 0 and the
         * limit to the size of the buffer, so that we can fill it
         * with data from the non-direct buffers that we need to
         * send.
         */
        /*
         * 尝试获取直接内存
         */
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        if (directBuffer == null) {
            //不使用直接内存
            ByteBuffer[] bufferList = new ByteBuffer[outgoingBuffers.size()];
            // Use gathered write call. This updates the positions of the
            // byte buffers to reflect the bytes that were written out.
            sock.write(outgoingBuffers.toArray(bufferList));

            // Remove the buffers that we have sent 删除我们发送的缓冲区
            ByteBuffer bb;
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb == packetSentinel) {
                    packetSent();
                }
                if (bb.remaining() > 0) {
                    break;
                }
                outgoingBuffers.remove();
            }
         } else {
            //使用直接内存
            directBuffer.clear();

            for (ByteBuffer b : outgoingBuffers) {
                if (directBuffer.remaining() < b.remaining()) {
                    /*
                     * When we call put later, if the directBuffer is to
                     * small to hold everything, nothing will be copied,
                     * so we've got to slice the buffer if it's too big.
                     */
                    /*
                     * 若directBuffer的剩余可写空间不足以容纳b的所有数据,则修改b的limit为directBuffer的剩余可写空间.
                     * 这样下面的复制代码刚好将directBuffer的可写空间写满
                     */
                    b = (ByteBuffer) b.slice().limit(
                            directBuffer.remaining());
                }
                /*
                 * put()会修改b和directBuffer的position值,但是我们不能修改b的position值,
                 * 因为下文需要position的值将已发送的数据移出outgoingBuffers,因此在复制结束后重置position值.
                 *
                 */

                /*
                 * put() is going to modify the positions of both
                 * buffers, put we don't want to change the position of
                 * the source buffers (we'll do that after the send, if
                 * needed), so we save and reset the position after the
                 * copy
                 */
                int p = b.position();
                //将b中的数据复制到directBuffer中
                directBuffer.put(b);
                b.position(p);
                if (directBuffer.remaining() == 0) {
                    break;
                }
            }
            /*
             * Do the flip: limit becomes position, position gets set to
             * 0. This sets us up for the write.
             */
            directBuffer.flip();
            //返回发送的字节数,下文据此移除已发送的数据
            int sent = sock.write(directBuffer);

            ByteBuffer bb;

            // Remove the buffers that we have sent
            // 将已发送的buffers从outgoingBuffers中移除
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb == packetSentinel) {
                    packetSent();
                }
                if (sent < bb.remaining()) {
                    /*
                     * We only partially sent this buffer, so we update
                     * the position and exit the loop.
                     */
                    // 只发送了此Buffer的部分数据,因此修改position的值并退出循环
                    bb.position(bb.position() + sent);
                    break;
                }
                /* We've sent the whole buffer, so drop the buffer */
                //该buffer的数据已经全部发送,将buffer从outgoingBuffers中移除
                sent -= bb.remaining();
                outgoingBuffers.remove();
            }
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    /**
     * Handles read/write IO on connection.
     * 处理连接的读/写IO。
     * 当SocketChannel上有数据可读时,worker thread调用NIOServerCnxn.doIO()进行读操作
     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x"
                         + Long.toHexString(sessionId));

                return;
            }
            if (k.isReadable()) {
                // 处理读操作的流程
                // 1.最开始incomingBuffer就是lenBuffer,容量为4.第一次读取4个字节,即此次请求报文的长度
                // 2.根据请求报文的长度分配incomingBuffer的大小
                // 3.将读到的字节存放在incomingBuffer中,直至读满
                //         (由于第2步中为incomingBuffer分配的长度刚好是报文的长度,此时incomingBuffer中刚好时一个报文)
                // 4.处理报文
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    ServerMetrics.getMetrics().CONNECTION_DROP_COUNT.add(1);
                    throw new EndOfStreamException(
                            "Unable to read additional data from client sessionid 0x"
                            + Long.toHexString(sessionId)
                            + ", likely client has closed socket");
                }
                /*
                只有incomingBuffer.remaining() == 0,才会进行下一步的处理,否则一直读取数据直到incomingBuffer读满,此时有两种可能:
                1.incomingBuffer就是lenBuffer,此时incomingBuffer的内容是此次请求报文的长度.
                根据lenBuffer为incomingBuffer分配空间后调用readPayload().
                在readPayload()中会立马进行一次数据读取,(1)若可以将incomingBuffer读满,则incomingBuffer中就是一个完整的请求,处理该请求;
                (2)若不能将incomingBuffer读满,说明出现了拆包问题,此时不能构造一个完整的请求,只能等待客户端继续发送数据,等到下次socketChannel可读时,继续将数据读取到incomingBuffer中
                2.incomingBuffer不是lenBuffer,说明上次读取时出现了拆包问题,incomingBuffer中只有一个请求的部分数据.
                而这次读取的数据加上上次读取的数据凑成了一个完整的请求,调用readPayload()
                */

                // return limit - position;返回limit和position之间相对位置差
                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    if (incomingBuffer == lenBuffer) { // start of next request 开始下一个请求
                        //解析上文中读取的报文总长度,同时为"incomingBuffer"分配len的空间供读取全部报文
                        incomingBuffer.flip();
                        //为incomeingBuffer分配空间时还包括了判断是否是"4字命令"的逻辑
                        isPayload = readLength(k);
                        incomingBuffer.clear();
                    } else {
                        // continuation
                        //2.incomingBuffer不是lenBuffer,此时incomingBuffer的内容是payload
                        isPayload = true;
                    }

                    if (isPayload) { // not the case for 4letterword
                        //处理报文
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            if (k.isWritable()) {
                handleWrite(k);

                if (!initialized && !getReadInterest() && !getWriteInterest()) {
                    throw new CloseRequestException("responded to info probe");
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x"
                     + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            // expecting close to log session closure
            close();
        } catch (ClientCnxnLimitException e) {
            // Common case exception, print at debug level
            ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Exception causing close of session 0x"
                          + Long.toHexString(sessionId) + ": " + e.getMessage());
            }
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x"
                     + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }

    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }

    // returns whether we are interested in writing, which is determined
    // by whether we have any pending buffers on the output queue or not
    private boolean getWriteInterest() {
        return !outgoingBuffers.isEmpty();
    }

    // returns whether we are interested in taking new requests, which is
    // determined by whether we are currently throttled or not
    private boolean getReadInterest() {
        return !throttled.get();
    }

    private final AtomicBoolean throttled = new AtomicBoolean(false);

    // Throttle acceptance of new requests. If this entailed a state change,
    // register an interest op update request with the selector.
    //
    // Don't support wait disable receive in NIO, ignore the parameter
    public void disableRecv(boolean waitDisableRecv) {
        if (throttled.compareAndSet(false, true)) {
            requestInterestOpsUpdate();
        }
    }

    // Disable throttling and resume acceptance of new requests. If this
    // entailed a state change, register an interest op update request with
    // the selector.
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            requestInterestOpsUpdate();
        }
    }

    private void readConnectRequest() throws IOException, InterruptedException, ClientCnxnLimitException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    // 该类用来将给客户端的响应进行分块，其核心方法是checkFlush方法
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        // 是否准备好发送另一块
        // 当需要强制发送时，sb缓冲中只要有内容就会同步发送，或者是当sb的大小超过2048（块）时就需要发送，其会调用NIOServerCnxn的sendBufferSync方法，该之后会进行分析，然后再清空sb缓冲。
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) { // 当强制发送并且sb大小大于0，或者sb大小大于2048即发送缓存
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            // 关闭之前需要强制性发送缓存
            checkFlush(true);
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }
    /** Return if four letter word found and responded to, otw false 如果发现并回复了四个字母的单词，则返回，otw false **/
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
    throws IOException
    {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        // 我们利用有限的长度来寻找cmds。它们都是4字节，适合int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);
        packetReceived(4);

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open.
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd +
                    " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }
            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /** Reads the first 4 bytes of lenBuffer, which could be true length or
     *  four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer 读取长度，现在获取缓冲区
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);
        return true;
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Used by "dump" 4-letter command to list all connection in
     * cnxnExpiryMap
     */
    @Override
    public String toString() {
        return "ip: " + sock.socket().getRemoteSocketAddress() +
               " sessionId: 0x" + Long.toHexString(sessionId);
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close() {
        setStale();
        if (!factory.removeCnxn(this)) {
            return;
        }

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }

        closeSock();
    }

    /**
     * Close resources associated with the sock of this cnxn.
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.debug("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                        " which had sessionid 0x" + Long.toHexString(sessionId) :
                        " (no session established for client)"));
        closeSock(sock);
    }

    /**
     * Close resources associated with a sock.
     */
    public static void closeSock(SocketChannel sock) {
        if (sock.isOpen() == false) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }

    private final static ByteBuffer packetSentinel = ByteBuffer.allocate(0);

    /**
     * Serializes a ZooKeeper response and enqueues it for sending.
     *
     * Serializes client response parts and enqueues them into outgoing queue.
     *
     * If both cache key and last modified zxid are provided, the serialized
     * response is caсhed under the provided key, the last modified zxid is
     * stored along with the value. A cache entry is invalidated if the
     * provided last modified zxid is more recent than the stored one.
     *
     * Attention: this function is not thread safe, due to caching not being
     * thread safe.
     *
     * @param h reply header
     * @param r reply payload, can be null
     * @param tag Jute serialization tag, can be null
     * @param cacheKey key for caching the serialized payload. a null value
     *     prvents caching
     * @param stat stat information for the the reply payload, used
     *     for cache invalidation. a value of 0 prevents caching.
     */
    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag,
                             String cacheKey, Stat stat) {
        try {
            sendBuffer(serialize(h, r, tag, cacheKey, stat));
            decrOutstandingAndCheckThrottle(h);
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);//xid为-1表示为通知
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();//包装为WatcherEvent来提供网络传输

        sendResponse(h, e, "notification", null, null);//给client发送请求,通知WatchedEvent的发生
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        factory.touchCnxn(this);
    }

    @Override
    public int getInterestOps() {
        if (!isSelectable()) {
            return 0;
        }
        int interestOps = 0;
        if (getReadInterest()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (getWriteInterest()) {
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    public InetAddress getSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return sock.socket().getInetAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

}
