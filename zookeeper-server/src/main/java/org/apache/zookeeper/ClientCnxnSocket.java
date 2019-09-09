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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ClientCnxnSocket does the lower level communication with a socket
 * implementation.
 * 
 * This code has been moved out of ClientCnxn so that a Netty implementation can
 * be provided as an alternative to the NIO socket code.
 * 
 */
abstract class ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnSocket.class);
    //是否初始化
    protected boolean initialized;

    /**
     * This buffer is only used to read the length of the incoming message.
     * 仅仅用来读取 incoming message的长度
     */
    protected final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(4);

    /**
     * After the length is read, a new incomingBuffer is allocated in
     * readLength() to receive the full message.
     */
    protected ByteBuffer incomingBuffer = lenBuffer;
    protected final AtomicLong sentCount = new AtomicLong(0L);//send次数
    protected final AtomicLong recvCount = new AtomicLong(0L);//接收次数
    protected long lastHeard;//上次接收时间
    protected long lastSend;//上次发送时间
    protected long now;//当前时间
    protected ClientCnxn.SendThread sendThread;//客户端通信的发送线程

    protected LinkedBlockingDeque<Packet> outgoingQueue;
    protected ZKClientConfig clientConfig;
    private int packetLen = ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;

    /**
     * The sessionId is only available here for Log and Exception messages.
     * Otherwise the socket doesn't need to know it.
     * 仅仅用来辅助log和Exception记录用的
     */
    protected long sessionId;

    //设置outgoingQueue、sendThread以及sessionId
    void introduce(ClientCnxn.SendThread sendThread, long sessionId,
                   LinkedBlockingDeque<Packet> outgoingQueue) {
        this.sendThread = sendThread;
        this.sessionId = sessionId;
        this.outgoingQueue = outgoingQueue;
    }

    //更新now时间
    void updateNow() {
        now = Time.currentElapsedTime();
    }

    //获取接收的闲置时间
    int getIdleRecv() {
        return (int) (now - lastHeard);
    }

    //获取发送的闲置时间
    int getIdleSend() {
        return (int) (now - lastSend);
    }

    //发送次数
    long getSentCount() {
        return sentCount.get();
    }

    //接收次数
    long getRecvCount() {
        return recvCount.get();
    }

    //更新最后一次监听的时间
    void updateLastHeard() {
        this.lastHeard = now;
    }

    //更新最后一次发送的时间
    void updateLastSend() {
        this.lastSend = now;
    }

    //同时更新最后一次监听和发送的时间
    void updateLastSendAndHeard() {
        this.lastSend = now;
        this.lastHeard = now;
    }

    //读取incoming message的length
    void readLength() throws IOException {
        int len = incomingBuffer.getInt();
        if (len < 0 || len >= packetLen) {
            throw new IOException("Packet len " + len + " is out of range!");
        }
        //分配对应长度的空间
        incomingBuffer = ByteBuffer.allocate(len);
    }

    //读取connect的response
    void readConnectResult() throws IOException {
        if (LOG.isTraceEnabled()) {
            StringBuilder buf = new StringBuilder("0x[");
            for (byte b : incomingBuffer.array()) {
                buf.append(Integer.toHexString(b) + ",");
            }
            buf.append("]");
            LOG.trace("readConnectResult " + incomingBuffer.remaining() + " "
                    + buf.toString());
        }
        ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
        BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
        ConnectResponse conRsp = new ConnectResponse();
        conRsp.deserialize(bbia, "connect");

        // read "is read-only" flag
        boolean isRO = false;
        try {
            isRO = bbia.readBool("readOnly");//反序列化,看是否是只读的
        } catch (IOException e) {
            // this is ok -- just a packet from an old server which
            // doesn't contain readOnly field
            LOG.warn("Connected to an old server; r-o mode will be unavailable");
        }

        this.sessionId = conRsp.getSessionId();
        //sendThread完成connect时一些参数验证以及zk state更新以及事件处理
        sendThread.onConnected(conRsp.getTimeOut(), this.sessionId,
                conRsp.getPasswd(), isRO);
    }

    abstract boolean isConnected();

    abstract void connect(InetSocketAddress addr) throws IOException;

    /**
     * Returns the address to which the socket is connected.
     */
    abstract SocketAddress getRemoteSocketAddress();

    /**
     * Returns the address to which the socket is bound.
     */
    abstract SocketAddress getLocalSocketAddress();

    /**
     * Clean up resources for a fresh new socket.
     * It's called before reconnect or close.
     */
    abstract void cleanup();

    /**
     * new packets are added to outgoingQueue.
     */
    abstract void packetAdded();

    /**
     * connState is marked CLOSED and notify ClientCnxnSocket to react.
     */
    abstract void onClosing();

    /**
     * Sasl completes. Allows non-priming packgets to be sent.
     * Note that this method will only be called if Sasl starts and completes.
     */
    abstract void saslCompleted();

    /**
     * being called after ClientCnxn finish PrimeConnection
     */
    abstract void connectionPrimed();

    /**
     * Do transportation work:
     * - read packets into incomingBuffer.
     * - write outgoing queue packets.
     * - update relevant timestamp.
     *
     * @param waitTimeOut timeout in blocking wait. Unit in MilliSecond.
     * @param pendingQueue These are the packets that have been sent and
     *                     are waiting for a response.
     * @param cnxn
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doTransport(int waitTimeOut, Queue<Packet> pendingQueue,
            ClientCnxn cnxn)
            throws IOException, InterruptedException;

    /**
     * Close the socket.
     */
    abstract void testableCloseSocket() throws IOException;

    /**
     * Close this client.
     */
    abstract void close();

    /**
     * Send Sasl packets directly.
     * The Sasl process will send the first (requestHeader == null) packet,
     * and then block the doTransport write,
     * finally unblock it when finished.
     */
    abstract void sendPacket(Packet p) throws IOException;

    protected void initProperties() throws IOException {
        try {
            packetLen = clientConfig.getInt(ZKConfig.JUTE_MAXBUFFER,
                    ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT);
            LOG.info("{} value is {} Bytes", ZKConfig.JUTE_MAXBUFFER,
                    packetLen);
        } catch (NumberFormatException e) {
            String msg = MessageFormat.format(
                    "Configured value {0} for property {1} can not be parsed to int",
                    clientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER),
                    ZKConfig.JUTE_MAXBUFFER);
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

}
