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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.WorkerService;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up. Instead of just waiting for the committed requests,
 * we process the uncommitted requests that belong to other sessions.
 * 此RequestProcessor将传入的已提交请求与本地提交的请求进行匹配。
 * 诀窍是，本地提交的更改系统状态的请求将作为传入的已提交请求返回，因此我们需要将它们匹配。
 * 我们不是仅仅等待已提交的请求，而是处理属于其他会话的未提交请求。
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 * CommitProcessor是多线程的。线程之间的通信通过队列，原子和处理器上同步的wait / notifyAll来处理。
 * CommitProcessor充当网关，允许请求继续处理管道的其余部分。
 * 它将允许许多读取请求，但只有一个写入请求同时处于飞行状态，从而确保以事务ID顺序处理写入请求。
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   -  1个提交处理器主线程，它监视请求队列并根据其sessionId将请求分配给工作线程，以便特定会话的读写请求始终分配给同一个线程（因此保证按顺序运行）。
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *   -  0-N工作线程，它在请求上运行请求处理器管道的其余部分。如果配置了0个工作线程，则主提交处理器线程直接运行管道。
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 * 典型（默认）线程计数是：在32核机器上，1个提交处理器线程和32个工作线程。
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *  多线程约束：
 *    - 必须按顺序处理每个会话的请求。
 *    - 必须以zxid顺序处理写请求
 *    - 必须确保在一个会话中写入之间没有竞争条件，这将触发由另一个会话中的读取请求设置的监视
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 * 当前实现通过简单地允许不与写请求并行处理读取请求来解决第三约束。
 */
// 这个processor主要负责将已经完成本机submit的request和已经在集群中达成commit（即收到过半follower的proposal ack）的request匹配，
// 并将匹配后的request交给nextProcessor（对于leader来说是ToBeAppliedRequestProcessor）处理。
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS =
        "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT =
        "zookeeper.commitProcessor.shutdownTimeout";

    /**
     * Incoming requests.
     * TODO: 已经发出提议等待收到过半服务器ack的请求队列
     */
    protected LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /**
     * The number of read requests currently held in all session queues
     * 当前在所有会话队列中保留的读取请求数
     */
    private AtomicInteger numReadQueuedRequests = new AtomicInteger(0);

    /**
     * The number of quorum requests currently held in all session queued
     * 所有会话中当前保留的仲裁请求数排队
     */
    private AtomicInteger numWriteQueuedRequests = new AtomicInteger(0);

    /**
     * Requests that have been committed.
     * TODO: 已经收到过半服务器ack的请求队列，以为着该请求可以被提交了
     */
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Requests that we are holding until commit comes in. Keys represent
     * session ids, each value is a linked list of the session's requests.
     * 在提交之前我们持有的请求。Keys表示会话ID，每个value是会话请求的列表。
     */
    protected final Map<Long, Deque<Request>> pendingRequests = new HashMap<>(10000);

    /** The number of requests currently being processed 当前正在处理的请求数*/
    protected final AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    RequestProcessor nextProcessor;

    /** For testing purposes, we use a separated stopping condition for the outer loop.
     * 出于测试目的，我们对外环使用单独的停止条件。*/
    protected volatile boolean stoppedMainLoop = true;
    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;
    private Object emptyPoolSync = new Object();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     * 此标志指示我们是否需要等待响应从领导者返回，或者我们只是让同步操作像读取一样流过。
     * 如果CommitProcessor位于Leader管道中，则该标志将为false。
     * TODO: matchSyncs 在leader端是false，learner端是true，因为learner端sync请求需要等待leader回复，而leader端本身则不需要
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
                           boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    // 是否有正在处理的请求
    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;
            case OpCode.sync:
                return matchSyncs;
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    @Override
    public void run() {
        try {
            /*
             * In each iteration of the following loop we process at most
             * requestsToProcess requests of queuedRequests. We have to limit
             * the number of request we poll from queuedRequests, since it is
             * possible to endlessly poll read requests from queuedRequests, and
             * that will lead to a starvation of non-local committed requests.
             * 在以下循环的每次迭代中，我们最多处理queueedRequests的requestsToProcess请求。
             * 我们必须限制从queuedRequests轮询的请求的数量，因为有可能无限轮询来自queuedRequests的读取请求，这将导致非本地提交请求的匮乏。
             */
            int requestsToProcess = 0;
            // committedRequests为空commitIsWaiting就是false
            boolean commitIsWaiting = false;
			do {
                /*
                 * Since requests are placed in the queue before being sent to
                 * the leader, if commitIsWaiting = true, the commit belongs to
                 * the first update operation in the queuedRequests or to a
                 * request from a client on another server (i.e., the order of
                 * the following two lines is important!).
                 * 由于请求是在发送给领导者之前放入队列中，
                 * TODO: 因此如果commitIsWaiting = true，则提交属于queuedRequests中的第一个更新操作，或者属于另一台服务器上的客户端的请求（即，以下两行的顺序）很重要！）。
                 */
                commitIsWaiting = !committedRequests.isEmpty();// committedRequests为空commitIsWaiting就是false
                requestsToProcess =  queuedRequests.size();
                // Avoid sync if we have something to do
                // 如果有事情要做，请避免同步
                if (requestsToProcess == 0 && !commitIsWaiting){
                    // TODO: queuedRequests队列没数据并且committedRequests队列也为空是时候会进来
                    // Waiting for requests to process
                    // 等待请求处理
                    synchronized (this) {
                        while (!stopped && requestsToProcess == 0 && !commitIsWaiting) { //在这里等待commitRequest
                            wait();
                            commitIsWaiting = !committedRequests.isEmpty();
                            requestsToProcess = queuedRequests.size();
                        }
                    }
                }

                ServerMetrics.getMetrics().READS_QUEUED_IN_COMMIT_PROCESSOR.add(numReadQueuedRequests.get());
                ServerMetrics.getMetrics().WRITES_QUEUED_IN_COMMIT_PROCESSOR.add(numWriteQueuedRequests.get());
                ServerMetrics.getMetrics().COMMITS_QUEUED_IN_COMMIT_PROCESSOR.add(committedRequests.size());

                long time = Time.currentElapsedTime();

                /*
                 * Processing up to requestsToProcess requests from the incoming
                 * queue (queuedRequests), possibly less if a committed request
                 * is present along with a pending local write. After the loop,
                 * we process one committed request if commitIsWaiting.
                 * 处理来自传入队列的最多requestToProcess请求（queueedRequests），如果存在已提交的请求和未决的本地写入，则可能更少。
                 * 循环后，如果commitIsWaiting，我们将处理一个提交的请求。
                 */
                Request request = null;
                while (!stopped && requestsToProcess > 0 && (request = queuedRequests.poll()) != null) {
                    // 处理queuedRequests中的数据
                    requestsToProcess--;
                    if (needCommit(request) || pendingRequests.containsKey(request.sessionId)) {
                        // TODO:这里此请求是事务请求或是此请求对应的客户端有积压的事务请求
                        // Add request to pending
                        // 将请求添加到待处理
                        pendingRequests
                                .computeIfAbsent(request.sessionId, sid -> new ArrayDeque<>())
                                .add(request);
                        ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(pendingRequests.get(request.sessionId).size());
                    } else {
                        // 处理读请求
                        numReadQueuedRequests.decrementAndGet();
                        sendToNextProcessor(request);
                    }
                    /*
                     * Stop feeding the pool if there is a local pending update
                     * and a committed request that is ready. Once we have a
                     * pending request with a waiting committed request, we know
                     * we can process the committed one. This is because commits
                     * for local requests arrive in the order they appeared in
                     * the queue, so if we have a pending request and a
                     * committed request, the committed request must be for that
                     * pending write or for a write originating at a different
                     * server.
                     * 如果存在本地暂挂更新和已准备好的提交请求，请停止向池中添加数据。
                     * 一旦我们有了一个待处理的请求和一个等待的提交请求，我们就知道可以处理提交的请求了。
                     * 这是因为对本地请求的提交以它们在队列中出现的顺序到达，因此，如果我们有一个挂起的请求和一个提交的请求，
                     * 则提交的请求必须是针对该挂起的写或源自其他服务器的写的。
                     */
                    if (!pendingRequests.isEmpty() && !committedRequests.isEmpty()){
                        /*
                         * We set commitIsWaiting so that we won't check
                         * committedRequests again.
                         */
                        commitIsWaiting = true;
                        break;
                    }
                }

                // Handle a single committed request处理单个提交的请求
                if (commitIsWaiting && !stopped){
                    // commitIsWaiting为true说明committedRequests里面有数据
                    // TODO: 等待空池
                    waitForEmptyPool();

                    if (stopped){
                        return;
                    }

                    // Process committed head
                    if ((request = committedRequests.poll()) == null) {
                        throw new IOException("Error: committed head is null");
                    }

                    /*
                     * Check if request is pending, if so, update it with the committed info
                     * 检查请求是否未决，如果是，请使用已提交的信息对其进行更新
                     */
                    Deque<Request> sessionQueue = pendingRequests.get(request.sessionId);
                    ServerMetrics.getMetrics().PENDING_SESSION_QUEUE_SIZE.add(pendingRequests.size());
                    if (sessionQueue != null) {
                        ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(sessionQueue.size());
                        // If session queue != null, then it is also not empty.
                        Request topPending = sessionQueue.poll();
                        if (request.cxid != topPending.cxid) {
                            /*
                             * TL;DR - we should not encounter this scenario often under normal load.
                             * We pass the commit to the next processor and put the pending back with a warning.
                             *
                             * Generally, we can get commit requests that are not at the queue head after
                             * a session moved (see ZOOKEEPER-2684). Let's denote the previous server of the session
                             * with A, and the server that the session moved to with B (keep in mind that it is
                             * possible that the session already moved from B to a new server C, and maybe C=A).
                             * 1. If request.cxid < topPending.cxid : this means that the session requested this update
                             * from A, then moved to B (i.e., which is us), and now B receives the commit
                             * for the update after the session already performed several operations in B
                             * (and therefore its cxid is higher than that old request).
                             * 2. If request.cxid > topPending.cxid : this means that the session requested an updated
                             * from B with cxid that is bigger than the one we know therefore in this case we
                             * are A, and we lost the connection to the session. Given that we are waiting for a commit
                             * for that update, it means that we already sent the request to the leader and it will
                             * be committed at some point (in this case the order of cxid won't follow zxid, since zxid
                             * is an increasing order). It is not safe for us to delete the session's queue at this
                             * point, since it is possible that the session has newer requests in it after it moved
                             * back to us. We just leave the queue as it is, and once the commit arrives (for the old
                             * request), the finalRequestProcessor will see a closed cnxn handle, and just won't send a
                             * response.
                             * Also note that we don't have a local session, therefore we treat the request
                             * like any other commit for a remote request, i.e., we perform the update without sending
                             * a response.
                             */
                            LOG.warn("Got request " + request +
                                    " but we are expecting request " + topPending);
                            sessionQueue.addFirst(topPending);
                        } else {
                            /*
                             * Generally, we want to send to the next processor our version of the request,
                             * since it contains the session information that is needed for post update processing.
                             * In more details, when a request is in the local queue, there is (or could be) a client
                             * attached to this server waiting for a response, and there is other bookkeeping of
                             * requests that are outstanding and have originated from this server
                             * (e.g., for setting the max outstanding requests) - we need to update this info when an
                             * outstanding request completes. Note that in the other case (above), the operation
                             * originated from a different server and there is no local bookkeeping or a local client
                             * session that needs to be notified.
                             */
                            topPending.setHdr(request.getHdr());
                            topPending.setTxn(request.getTxn());
                            topPending.zxid = request.zxid;
                            topPending.commitRecvTime = request.commitRecvTime;
                            request = topPending;

                            // Only decrement if we take a request off the queue.
                            numWriteQueuedRequests.decrementAndGet();
                        }
                    }

                    sendToNextProcessor(request);
                    waitForEmptyPool();

                    /*
                     * Process following reads if any, remove session queue if
                     * empty.
                     */
                    if (sessionQueue != null) {
                        int readsAfterWrite = 0;
                        while (!stopped && !sessionQueue.isEmpty() && !needCommit(sessionQueue.peek())) {
                            numReadQueuedRequests.decrementAndGet();
                            sendToNextProcessor(sessionQueue.poll());
                            readsAfterWrite++;
                        }
                        ServerMetrics.getMetrics().READS_AFTER_WRITE_IN_SESSION_QUEUE.add(readsAfterWrite);

                        // Remove empty queues
                        if (sessionQueue.isEmpty()) {
                            pendingRequests.remove(request.sessionId);
                        }
                    }
                }

                ServerMetrics.getMetrics().COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - time);
                endOfIteration();
            } while (!stoppedMainLoop);
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    //for test only
    protected void endOfIteration() {

    }

    protected void waitForEmptyPool() throws InterruptedException {
        // 当前正在处理的请求数
        int numRequestsInProcess = numRequestsProcessing.get();
        if (numRequestsInProcess != 0) {
            ServerMetrics.getMetrics().CONCURRENT_REQUEST_PROCESSING_IN_COMMIT_PROCESSOR.add(
                    numRequestsInProcess);
        }

        long startWaitTime = Time.currentElapsedTime();
        synchronized(emptyPoolSync) {
            while ((!stopped) && isProcessingRequest()) {
                emptyPoolSync.wait();
            }
        }
        ServerMetrics.getMetrics().TIME_WAITING_EMPTY_POOL_IN_COMMIT_PROCESSOR_READ.add(
                Time.currentElapsedTime() - startWaitTime);
    }

    @Override
    public void start() {
        // 拿到系统cpu核心数
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(
            ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(
            ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with "
                 + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                 + " worker threads.");
        if (workerPool == null) {
            // TODO: 业务线程池
            workerPool = new WorkerService(
                "CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        stoppedMainLoop = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     * 安排最终请求处理；如果未使用工作线程池，则此线程直接进行处理。
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     * CommitWorkRequest是一个小包装类，允许使用WorkerService运行下游处理
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor, unable to continue." +
                        "下游处理器抛出异常，无法继续。");
                CommitProcessor.this.halt();
            }
        }

        /**
         * 主要做的事：
         * 更新Metric
         * 调用下一个请求处理器
         *
         * @throws RequestProcessorException
         */
        public void doWork() throws RequestProcessorException {
            try {
                if (needCommit(request)) {
                    // 事务请求，需要commit
                    if (request.commitProcQueueStartTime != -1 && request.commitRecvTime != -1) {
                        // Locally issued writes. 本地发布的写入。
                        long currentTime = Time.currentElapsedTime();
                        ServerMetrics.getMetrics().WRITE_COMMITPROC_TIME.add(currentTime - request.commitProcQueueStartTime);
                        ServerMetrics.getMetrics().LOCAL_WRITE_COMMITTED_TIME.add(currentTime - request.commitRecvTime);
                    } else if (request.commitRecvTime != -1) {
                        // Writes issued by other servers. 由其他服务器发出的写入。
                        ServerMetrics.getMetrics().SERVER_WRITE_COMMITTED_TIME.add(
                                Time.currentElapsedTime() - request.commitRecvTime);
                    }
                } else {
                    // 非事务请求，不需要commit的请求
                    if (request.commitProcQueueStartTime != -1) {
                        ServerMetrics.getMetrics().READ_COMMITPROC_TIME.add(
                                Time.currentElapsedTime() -
                                        request.commitProcQueueStartTime);
                    }
                }

                long timeBeforeFinalProc = Time.currentElapsedTime();
                // 调用下一个请求处理器
                nextProcessor.processRequest(request);
                if (needCommit(request)) {
                    ServerMetrics.getMetrics().WRITE_FINAL_PROC_TIME.add(
                            Time.currentElapsedTime() - timeBeforeFinalProc);
                } else {
                    ServerMetrics.getMetrics().READ_FINAL_PROC_TIME.add(
                            Time.currentElapsedTime() - timeBeforeFinalProc);
                }

            } finally {

                if (numRequestsProcessing.decrementAndGet() == 0){
                    wakeupOnEmpty();
                }
            }
        }
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    synchronized private void wakeup() {
        notifyAll();
    }

    private void wakeupOnEmpty() {
        synchronized(emptyPoolSync){
            emptyPoolSync.notifyAll();
        }
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }
        request.commitRecvTime = Time.currentElapsedTime();
        ServerMetrics.getMetrics().COMMITS_QUEUED.add(1);
        committedRequests.add(request);
        wakeup();
    }

    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        //TODO: 记录这个请求进入队列的时间
        request.commitProcQueueStartTime = Time.currentElapsedTime();
        queuedRequests.add(request);
        // If the request will block, add it to the queue of blocking requests
        // 如果请求将阻止，请将其添加到阻止请求队列中
        if (needCommit(request)) {
            // 事务请求
            numWriteQueuedRequests.incrementAndGet();
        } else {
            // 非事务请求
            numReadQueuedRequests.incrementAndGet();
        }
        wakeup();
    }

    private void halt() {
        stoppedMainLoop = true;
        stopped = true;
        wakeupOnEmpty();
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
