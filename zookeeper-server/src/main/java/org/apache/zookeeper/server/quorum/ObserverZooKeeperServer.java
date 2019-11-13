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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * A ZooKeeperServer for the Observer node type. Not much is different, but
 * we anticipate specializing the request processors in the future. 
 *
 */
public class ObserverZooKeeperServer extends LearnerZooKeeperServer {
    private static final Logger LOG =
        LoggerFactory.getLogger(ObserverZooKeeperServer.class);        
    
    /**
     * Enable since request processor for writing txnlog to disk and
     * take periodic snapshot. Default is ON.
     * 由于请求处理器将txnlog写入磁盘并进行定期快照，因此启用。默认为开。
     */
    // 同步处理器是否可用
    private boolean syncRequestProcessorEnabled = this.self.getSyncEnabled();
    
    /*
     * Pending sync requests
     */
    // 待同步请求队列
    ConcurrentLinkedQueue<Request> pendingSyncs =  new ConcurrentLinkedQueue<Request>();

    ObserverZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        LOG.info("syncEnabled =" + syncRequestProcessorEnabled);
    }
    
    public Observer getObserver() {
        return self.observer;
    }
    
    @Override
    public Learner getLearner() {
        return self.observer;
    }       
    
    /**
     * Unlike a Follower, which sees a full request only during the PROPOSAL
     * phase, Observers get all the data required with the INFORM packet. 
     * This method commits a request that has been unpacked by from an INFORM
     * received from the Leader.
     * Follower仅在PROPOSAL阶段看到完整的请求，与众不同的是，观察者使用INFORM数据包获取所需的所有数据。
     * 此方法提交从Leader收到的INFORM提取的请求。
     *      
     * @param request
     */
    public void commitRequest(Request request) {     
        if (syncRequestProcessorEnabled) {// 同步处理器可用
            // Write to txnlog and take periodic snapshot
            // 使用同步处理器处理请求
            syncProcessor.processRequest(request);
        }
        // 提交请求
        commitProcessor.commit(request);        
    }
    
    /**
     * Set up the request processors for an Observer:
     * firstProcesor->commitProcessor->finalProcessor
     */
    // 请求处理链 这里两条处理链
    // ObserverRequestProcessor -> CommitProcessor -> FinalRequestProcessor
    // SyncRequestProcessor(如果syncRequestProcessorEnabled为true)
    @Override
    protected void setupRequestProcessors() {      
        // We might consider changing the processor behaviour of 
        // Observers to, for example, remove the disk sync requirements.
        // Currently, they behave almost exactly the same as followers.
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor,
                Long.toString(getServerId()), true,
                getZooKeeperServerListener());
        commitProcessor.start();
        firstProcessor = new ObserverRequestProcessor(this, commitProcessor);
        ((ObserverRequestProcessor) firstProcessor).start();

        /*
         * Observer should write to disk, so that the it won't request
         * too old txn from the leader which may lead to getting an entire
         * snapshot.
         *
         * However, this may degrade performance as it has to write to disk
         * and do periodic snapshot which may double the memory requirements
         *
         * 观察者应该写入磁盘，这样它就不会向领导者请求太旧的txn，这可能导致获得整个快照。
         *
         * 但是，这可能会降低性能，因为它必须写入磁盘并执行定期快照，这可能会使内存需求翻倍
         */
        if (syncRequestProcessorEnabled) {
            syncProcessor = new SyncRequestProcessor(this, null);
            syncProcessor.start();
        }
    }

    /*
     * Process a sync request
     */
    synchronized public void sync(){
        if(pendingSyncs.size() ==0){// 没有未完成的同步请求
            LOG.warn("Not expecting a sync.");
            return;
        }
        // 移除队首元素
        Request r = pendingSyncs.remove();
        // 提交请求
        commitProcessor.commit(r);
    }
    
    @Override
    public String getState() {
        return "observer";
    }

    @Override
    public synchronized void shutdown() {
        if (!canShutdown()) {
            LOG.debug("ZooKeeper server is not running, so not proceeding to shutdown!");
            return;
        }
        super.shutdown();
        if (syncRequestProcessorEnabled && syncProcessor != null) {
            syncProcessor.shutdown();
        }
    }
}
