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

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncRequestProcessor用于将请求持久化到磁盘中．
 *
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 * 此RequestProcessor将请求记录到磁盘。
 * 它批量处理有效执行io的请求。在将日志同步到磁盘之前，请求不会传递到下一个RequestProcessor。
 *
 * SyncRequestProcessor is used in 3 different cases
 * SyncRequestProcessor用于3种不同的情况
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 *             将请求同步到磁盘并将其转​​发到AckRequestProcessor，后者将ack发送回自身。
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 *             将请求同步到磁盘并将请求转发给SendAckRequestProcessor，SendAckRequestProcessor将数据包发送给leader。
 *             SendAckRequestProcessor是可刷新的，允许我们强制推送数据包到领导者。
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 *             将已提交的请求同步到磁盘（作为INFORM数据包接收）。
 *             它永远不会将ack发送回领导者，因此nextProcessor将为null。这改变了观察者txnlog的语义，因为它只包含已提交的txns。
 */
// 继承ZooKeeperCriticalThread，是一个关键重要线程
// SyncRequestProcessor，该处理器将事务请求存入磁盘，其将请求批量的存入磁盘以提高效率，请求在写入磁盘之前是不会被转发到下个处理器的。
// SyncRequestProcessor也继承了Thread类并实现了RequestProcessor接口，表示其可以作为线程使用。
// SyncRequestProcessor维护了ZooKeeperServer实例，其用于获取ZooKeeper的数据库和其他信息；
// 维护了一个处理请求的队列，其用于存放请求；
// 维护了一个处理快照的线程，用于处理快照；
// 同时还维护了一个等待被刷新到磁盘的请求队列。
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);

    private static final int FLUSH_SIZE = 1000;

    private static final Request REQUEST_OF_DEATH = Request.requestOfDeath;

    /** The number of log entries to log before starting a snapshot启动快照之前要记录的日志条目数 */
     // 最少是2个
    private static int snapCount = ZooKeeperServer.getSnapCount();

    // 请求队列
    private final BlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();
    // 信号量
    private final Semaphore snapThreadMutex = new Semaphore(1);
    // Zookeeper服务器
    private final ZooKeeperServer zks;
    // 下一个请求处理链的处理类
    private final RequestProcessor nextProcessor;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     * 已写入并等待刷新到磁盘的事务。
     * 基本上这是SyncItems的列表，其回调将在flush返回成功后被调用。
     */
     // 等待被刷新到磁盘的请求队列
    private final Queue<Request> toFlush = new ArrayDeque<>(FLUSH_SIZE);

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
             // 写日志数量初始化为0
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 我们这样做是为了确保并非集合中的所有服务器都同时拍摄快照
            int randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2, snapCount);
            while (true) {
                // 从请求队列中拿到上个处理器下发的请求
                Request si = queuedRequests.poll();
                if (si == null) {
                    // 说明没有请求，刷新磁盘数据
                    flush();
                    // TODO: 从请求队列中取出一个请求，若队列为空会阻塞
                    si = queuedRequests.take();
                }

                if (si == REQUEST_OF_DEATH) {
                    // 执行了关闭操作
                    break;
                }

                // track the number of records written to the log
                // TODO: 跟踪写入日志的记录数
                if (zks.getZKDatabase().append(si)) {// TODO: 将请求添加至事务日志文件，只有事务性请求才会返回true
                    // 写入一条日志，logCount加1
                    logCount++;
                    if (logCount > randRoll) { // 满足roll the log的条件
                        randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2, snapCount);
                        // roll the log 滚动快照文件
                        zks.getZKDatabase().rollLog();
                        // take a snapshot
                        // 尝试获取1个permits，如果获取不到则返回false，如果获取到则启动Snapshot Thread线程，
                        // 此方法通常与if语句结合使用，具有无阻塞的特点
                        // 获取不到又可能是上一个Snapshot Thread没有结束
                        if (!snapThreadMutex.tryAcquire()) { // 正在进行快照
                            LOG.warn("Too busy to snap, skipping");
                        } else {
                            // 创建线程来处理快照
                            new ZooKeeperThread("Snapshot Thread") {
                                public void run() {
                                    try {
                                        zks.takeSnapshot();
                                    } catch (Exception e) {
                                        LOG.warn("Unexpected exception", e);
                                    } finally {
                                      snapThreadMutex.release();
                                    }
                                }
                            }.start();
                        }
                        logCount = 0;
                    }
                } else if (toFlush.isEmpty()) { // 等待被刷新到磁盘的请求队列为空
                    // optimization for read heavy workloads
                    // iff this is a read, and there are no pending
                    // flushes (writes), then just pass this to the next
                    // processor
                    // 查看此时toFlush是否为空，如果为空，说明近段时间读多写少，直接响应
                    // 如果不是事务性日志并且toFlush中没有数据
                    if (nextProcessor != null) {
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {// 处理器是Flushable的
                            // 刷新到磁盘
                            ((Flushable)nextProcessor).flush();
                        }
                    }
                    continue;// 跳过后续处理
                }

                // TODO: 请求添加到toFlush中
                toFlush.add(si);
                if (toFlush.size() == FLUSH_SIZE) {
                    // toFlush的大小等于FLUSH_SIZE（1000）时才flush
                    flush();
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    // 主要是刷出toFlush队列中的Request
    private void flush() throws IOException, RequestProcessorException {
      if (this.toFlush.isEmpty()) {
          return;
      }
      // commit ZKDatabase
        // TODO: 这里不是提交事务日志（这名字起的真误导人），这是在flush事务日志文件
      zks.getZKDatabase().commit();

      if (this.nextProcessor == null) {
          // 如果处理链没有下一个处理程序，就直接清除toFlush中的内容
        this.toFlush.clear();
      } else {
          while (!this.toFlush.isEmpty()) {
              // TODO: 从toFlush队列移除并取出全部的Request执行下一个处理程序
              final Request i = this.toFlush.remove();
              this.nextProcessor.processRequest(i);
          }
          if (this.nextProcessor instanceof Flushable) {
              //如果下一个请求处理程序是Flushable,执行下一个处理器的flush操作
              ((Flushable)this.nextProcessor).flush();
          }
      }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        // 将REQUEST_OF_DEATH请求添加到队列中
        queuedRequests.add(REQUEST_OF_DEATH);
        try {
            this.join();// 等待子线程处理结束
            this.flush();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
            Thread.currentThread().interrupt(); // 中断线程
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }
    // 上一个处理器调用的入口函数
    public void processRequest(final Request request) {
        Objects.requireNonNull(request, "Request cannot be null");
        queuedRequests.add(request);
    }

    /**
     * used by tests to check for changing snapcounts
     * 测试用于检查更改快照计数
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

}
