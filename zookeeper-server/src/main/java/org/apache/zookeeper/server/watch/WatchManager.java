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

package org.apache.zookeeper.server.watch;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 * 服务端的WatchManager，管理Watcher。
 *
 * <p>
 * client端注册watcher
 *     client端watcher的注册，管理
 *     client端watcher在网络请求中的体现
 *     client端接收server回复时，watcher的注册
 * server端处理watcher
 *     收到client请求时进行ServerCnxn的注册
 *     触发事件时，通过WatchManager找到相应Watcher(ServerCnxn),进而通知对该事件感兴趣的client
 * client端回调watcher
 *     client端接收server的通知，调用queueEvent函数放在waitingEvents队列中
 *     ClientCnxn.EventThread#run调用ClientCnxn.EventThread#processEvent，消费waitingEvents，回调watcher.process()
 *
 * <p>
 *
 *
 */
//WatcherManager类用于管理watchers和相应的触发器。watchTable表示从节点路径到watcher集合的映射，而watch2Paths则表示从watcher到所有节点路径集合的映射。
public class WatchManager implements IWatchManager {
    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);
    // watcher表 节点路径到watcher的映射
    private final Map<String, Set<Watcher>> watchTable = new HashMap<String, Set<Watcher>>();
    // watcher到节点路径的映射
    private final Map<Watcher, Set<String>> watch2Paths = new HashMap<Watcher, Set<String>>();

    /**
     * size方法是同步的，因此在多线程环境下是安全的，
     * 其主要作用是获取watchTable的大小，即遍历watchTable的值集合。
     * @return
     */
    @Override
    public synchronized int size(){
        int result = 0;
        for(Set<Watcher> watches : watchTable.values()) { // 遍历watchTable所有的值集合(HashSet<Watcher>集合)
            // 每个集合大小累加
            result += watches.size();
        }
        return result;
    }

    boolean isDeadWatcher(Watcher watcher) {
        return watcher instanceof ServerCnxn && ((ServerCnxn) watcher).isStale();
    }

    /**
     * addWatch是同步方法，线程安全
     *
     * 添加一个watch，把path，watch放入两个表中
     *
     * @param path znode path
     * @param watcher watcher object reference
     *
     * @return
     */
    @Override
    public synchronized boolean addWatch(String path, Watcher watcher) {
        if (isDeadWatcher(watcher)) {
            LOG.debug("Ignoring addWatch with closed cnxn忽略已经关闭的cnxn的addWatch");
            return false;
        }
        // 根据路径获取对应的所有watcher
        Set<Watcher> list = watchTable.get(path);
        if (list == null) {
            // don't waste memory if there are few watches on a node
            // rehash when the 4th entry is added, doubling size thereafter
            // seems like a good compromise
            list = new HashSet<Watcher>(4);
            watchTable.put(path, list);
        }
        // 将watcher直接添加至watcher集合
        list.add(watcher);

        // 通过watcher获取对应的所有路径
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null) {
            // cnxns typically have many watches, so use default cap here
            paths = new HashSet<String>();
            watch2Paths.put(watcher, paths);
        }
        // 将路径添加至paths集合
        return paths.add(path);
    }

    //removeWatcher用作从watch2Paths和watchTable中中移除该watcher
    @Override
    public synchronized void removeWatcher(Watcher watcher) {
        // 从wach2Paths中移除watcher，并返回watcher对应的path集合
        Set<String> paths = watch2Paths.remove(watcher);
        if (paths == null) {// 集合为空，直接返回
            return;
        }
        for (String p : paths) {// 遍历路径集合
            // 从watcher表中根据路径取出相应的watcher集合
            Set<Watcher> list = watchTable.get(p);
            if (list != null) {
                list.remove(watcher);
                if (list.isEmpty()) {// 移除后list为空，则从watch表中移出
                    watchTable.remove(p);
                }
            }
        }
    }

    /**
     * triggerWatch方法用于根据path，EventType找到需要触发的watch集合，并过滤掉抑制触发的watch集合
     * 然后触发这些watcher
     *
     * @param path znode path
     * @param type the watch event type
     *
     * @return
     */
    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type) {
        return triggerWatch(path, type, null);
    }

    /**
     * 该方法主要用于触发watch事件，并对事件进行处理。
     * 从指定的watcher集合supress 中筛选出要触发的watcher，将剩下的watcher执行对应的回调
     * @param path znode path
     * @param type the watch event type
     * @param supress
     * @return
     */
    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type, WatcherOrBitSet supress) {
        // 根据事件类型、连接状态、节点路径创建WatchedEvent
        WatchedEvent e = new WatchedEvent(type,KeeperState.SyncConnected, path);
        Set<Watcher> watchers;
        synchronized (this) {
            // 从watcher表中移除path，并返回其对应的watcher集合
            watchers = watchTable.remove(path); // 这里可以看出事件只要发生就会移除watch，所以需要发生回调之后反复注册
            if (watchers == null || watchers.isEmpty()) { // watcher集合为空
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG,
                            ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                            "No watchers for " + path);
                }
                return null;//如果watchTable没有path这条记录，返回空
            }
            for (Watcher w : watchers) {// 遍历watcher集合
                Set<String> paths = watch2Paths.get(w);// 根据watcher从watcher表中取出路径集合
                if (paths != null) {// 路径集合不为空
                    // 则移除路径
                    paths.remove(path);//在watch2Paths中删掉[watcher,path]这种记录
                }
            }
        }

        for (Watcher w : watchers) {// 遍历watcher集合
            if (supress != null && supress.contains(w)) { //从supress中过滤掉部分watcher(类似抑制触发)
                continue;
            }
            // 进行处理 这里调用的是ServerCnxn#process，默认实现 NIOServerCnxn#process
            // server触发watch，其实就是给相应的client发送回复，然后client端自己处理
            w.process(e);//没有被抑制的watcher进行回调
        }

        // 更新各自的Metric
        switch (type) {
        case NodeCreated:
            ServerMetrics.getMetrics().NODE_CREATED_WATCHER.add(watchers.size());
            break;

        case NodeDeleted:
            ServerMetrics.getMetrics().NODE_DELETED_WATCHER.add(watchers.size());
            break;

        case NodeDataChanged:
            ServerMetrics.getMetrics().NODE_CHANGED_WATCHER.add(watchers.size());
            break;

        case NodeChildrenChanged:
            ServerMetrics.getMetrics().NODE_CHILDREN_WATCHER.add(watchers.size());
            break;
        default:
            // Other types not logged.
            break;
        }

        return new WatcherOrBitSet(watchers);//返回所有触发的watcher
    }

    //dumpWatches用作将watchTable或watch2Paths写入磁盘。
    @Override
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        if (byPath) {// 控制写入watchTable或watch2Paths
            for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {// 遍历每个键值对
                pwriter.println(e.getKey());// 写入键
                for (Watcher w : e.getValue()) { // 遍历值(HashSet<Watcher>)
                    pwriter.print("\t0x");
                    pwriter.print(Long.toHexString(((ServerCnxn)w).getSessionId()));
                    pwriter.print("\n");
                }
            }
        } else {
            for (Entry<Watcher, Set<String>> e : watch2Paths.entrySet()) { // 遍历每个键值对
                // 遍历每个键值对
                pwriter.print("0x");
                pwriter.println(Long.toHexString(((ServerCnxn)e.getKey()).getSessionId()));
                for (String path : e.getValue()) {// 遍历值(HashSet<String>)
                    pwriter.print("\t");
                    pwriter.println(path);
                }
            }
        }
    }

    // 这个路径下是否包含这个watcher
    @Override
    public synchronized boolean containsWatcher(String path, Watcher watcher) {
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null || !paths.contains(path)) {
            return false;
        }
        return true;
    }

    // 移除某个路径下的watcher
    @Override
    public synchronized boolean removeWatcher(String path, Watcher watcher) {
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null || !paths.remove(path)) {
            return false;
        }

        Set<Watcher> list = watchTable.get(path);
        if (list == null || !list.remove(watcher)) {
            return false;
        }

        if (list.isEmpty()) {
            watchTable.remove(path);
        }

        return true;
    }

    @Override
    public synchronized WatchesReport getWatches() {
        Map<Long, Set<String>> id2paths = new HashMap<Long, Set<String>>();
        for (Entry<Watcher, Set<String>> e: watch2Paths.entrySet()) {
            // 通过Watcher拿到会话ID
            Long id = ((ServerCnxn) e.getKey()).getSessionId();
            Set<String> paths = new HashSet<String>(e.getValue());
            id2paths.put(id, paths);
        }
        return new WatchesReport(id2paths);
    }

    @Override
    public synchronized WatchesPathReport getWatchesByPath() {
        Map<String, Set<Long>> path2ids = new HashMap<String, Set<Long>>();
        for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {
            Set<Long> ids = new HashSet<Long>(e.getValue().size());
            path2ids.put(e.getKey(), ids);
            for (Watcher watcher : e.getValue()) {
                ids.add(((ServerCnxn) watcher).getSessionId());
            }
        }
        return new WatchesPathReport(path2ids);
    }

    @Override
    public synchronized WatchesSummary getWatchesSummary() {
        int totalWatches = 0;
        for (Set<String> paths : watch2Paths.values()) {
            totalWatches += paths.size();
        }
        return new WatchesSummary (watch2Paths.size(), watchTable.size(),
                                   totalWatches);
    }

    // 这个类只存储了状态，关闭时不需要做什么
    @Override
    public void shutdown() { /* do nothing */ }


    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(watch2Paths.size()).append(" connections watching ")
                .append(watchTable.size()).append(" paths\n");

        int total = 0;
        for (Set<String> paths : watch2Paths.values()) {
            total += paths.size();
        }
        sb.append("Total watches:").append(total);

        return sb.toString();
    }
}
