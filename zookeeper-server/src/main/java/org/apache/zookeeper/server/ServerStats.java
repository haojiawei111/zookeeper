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

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.metric.AvgMinMaxCounter;
import org.apache.zookeeper.server.quorum.BufferStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic Server Statistics基本服务器统计
 *
 * jmx中如何用这些参数的
 */
public class ServerStats {
    private static final Logger LOG = LoggerFactory.getLogger(ServerStats.class);

    private final AtomicLong packetsSent = new AtomicLong();//发送包个数
    private final AtomicLong packetsReceived = new AtomicLong();//接收个数

    private final AvgMinMaxCounter requestLatency = new AvgMinMaxCounter("request_latency");

    private final AtomicLong fsyncThresholdExceedCount = new AtomicLong(0);

    private final BufferStats clientResponseStats = new BufferStats();
    //Provider对象，提供部分统计数据
    private final Provider provider;
    private final long startTime = Time.currentElapsedTime();

    // 内部类Provider完成服务器状态，队列数量，client存活数量等信息
    // 属性，函数完成延迟，发送，接收量等信息
    public static interface Provider {
        // getOutstandingRequests，getLastProcessedZxid，getNumAliveConnections，getDataDirSize，getLogDirSize就一种默认实现，在ZooKeeperServer类中
        public long getOutstandingRequests();//获取队列中还没有被处理的请求数量
        public long getLastProcessedZxid();//获取最后一个处理的zxid
        public String getState();//获取服务器状态
        public int getNumAliveConnections();//获取存活的客户端连接总数
        public long getDataDirSize();
        public long getLogDirSize();
    }
    
    public ServerStats(Provider provider) {
        this.provider = provider;
    }

    // getters
    public long getMinLatency() {
        return requestLatency.getMin();
    }

    public double getAvgLatency() {
        return requestLatency.getAvg();
    }

    public long getMaxLatency() {
        return requestLatency.getMax();
    }

    public long getOutstandingRequests() {
        return provider.getOutstandingRequests();
    }
    
    public long getLastProcessedZxid(){
        return provider.getLastProcessedZxid();
    }

    public long getDataDirSize() {
        return provider.getDataDirSize();
    }

    public long getLogDirSize() {
        return provider.getLogDirSize();
    }
    
    public long getPacketsReceived() {
        return packetsReceived.get();
    }

    public long getPacketsSent() {
        return packetsSent.get();
    }

    public String getServerState() {
        return provider.getState();
    }
    
    /** The number of client connections alive to this server */
    public int getNumAliveClientConnections() {
    	return provider.getNumAliveConnections();
    }

    public long getUptime() {
        return Time.currentElapsedTime() - startTime;
    }

    public boolean isProviderNull() {
        return provider == null;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/"
                + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        sb.append("Connections: " + getNumAliveClientConnections() + "\n");

        if (provider != null) {
            sb.append("Outstanding: " + getOutstandingRequests() + "\n");
            sb.append("Zxid: 0x"+ Long.toHexString(getLastProcessedZxid())+ "\n");
        }
        sb.append("Mode: " + getServerState() + "\n");
        return sb.toString();
    }

    /**
     * Update request statistic. This should only be called from a request
     * that originated from that machine.
     * 更新延迟统计数据
     */
    public void updateLatency(Request request, long currentTime) {
        long latency = currentTime - request.createTime;
        if (latency < 0) {
            return;
        }
        requestLatency.addDataPoint(latency);
        if (request.getHdr() != null) {
            // Only quorum request should have header
            ServerMetrics.getMetrics().UPDATE_LATENCY.add(latency);
        } else {
            // All read request should goes here
            ServerMetrics.getMetrics().READ_LATENCY.add(latency);
        }
    }

    public void resetLatency() {
        requestLatency.reset();
    }

    public void resetMaxLatency() {
        requestLatency.resetMax();
    }

    public void incrementPacketsReceived() {
        packetsReceived.incrementAndGet();
    }

    public void incrementPacketsSent() {
        packetsSent.incrementAndGet();
    }

    public void resetRequestCounters(){
        packetsReceived.set(0);
        packetsSent.set(0);
    }

    public long getFsyncThresholdExceedCount() {
        return fsyncThresholdExceedCount.get();
    }

    public void incrementFsyncThresholdExceedCount() {
        fsyncThresholdExceedCount.incrementAndGet();
    }

    public void resetFsyncThresholdExceedCount() {
        fsyncThresholdExceedCount.set(0);
    }

    public void reset() {
        resetLatency();
        resetRequestCounters();
        clientResponseStats.reset();
        ServerMetrics.getMetrics().resetAll();
    }

    public void updateClientResponseSize(int size) {
        clientResponseStats.setLastBufferSize(size);
    }

    public BufferStats getClientResponseStats() {
        return clientResponseStats;
    }
}
