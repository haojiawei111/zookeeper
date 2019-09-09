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

import org.apache.jute.Record;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoWatcherException;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckWatchesRequest;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetAllChildrenNumberRequest;
import org.apache.zookeeper.proto.GetAllChildrenNumberResponse;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetEphemeralsRequest;
import org.apache.zookeeper.proto.GetEphemeralsResponse;
import org.apache.zookeeper.proto.ReconfigRequest;
import org.apache.zookeeper.proto.RemoveWatchesRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.EphemeralType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the main class of ZooKeeper client library. To use a ZooKeeper
 * service, an application must first instantiate an object of ZooKeeper class.
 * All the iterations will be done by calling the methods of ZooKeeper class.
 * The methods of this class are thread-safe unless otherwise noted.
 * <p>
 * Once a connection to a server is established, a session ID is assigned to the
 * client. The client will send heart beats to the server periodically to keep
 * the session valid.
 * <p>
 * The application can call ZooKeeper APIs through a client as long as the
 * session ID of the client remains valid.
 * <p>
 * If for some reason, the client fails to send heart beats to the server for a
 * prolonged period of time (exceeding the sessionTimeout value, for instance),
 * the server will expire the session, and the session ID will become invalid.
 * The client object will no longer be usable. To make ZooKeeper API calls, the
 * application must create a new client object.
 * <p>
 * If the ZooKeeper server the client currently connects to fails or otherwise
 * does not respond, the client will automatically try to connect to another
 * server before its session ID expires. If successful, the application can
 * continue to use the client.
 * <p>
 * The ZooKeeper API methods are either synchronous or asynchronous. Synchronous
 * methods blocks until the server has responded. Asynchronous methods just queue
 * the request for sending and return immediately. They take a callback object that
 * will be executed either on successful execution of the request or on error with
 * an appropriate return code (rc) indicating the error.
 * <p>
 * Some successful ZooKeeper API calls can leave watches on the "data nodes" in
 * the ZooKeeper server. Other successful ZooKeeper API calls can trigger those
 * watches. Once a watch is triggered, an event will be delivered to the client
 * which left the watch at the first place. Each watch can be triggered only
 * once. Thus, up to one event will be delivered to a client for every watch it
 * leaves.
 * <p>
 * A client needs an object of a class implementing Watcher interface for
 * processing the events delivered to the client.
 *
 * When a client drops the current connection and re-connects to a server, all the
 * existing watches are considered as being triggered but the undelivered events
 * are lost. To emulate this, the client will generate a special event to tell
 * the event handler a connection has been dropped. This special event has
 * EventType None and KeeperState Disconnected.
 *
 */
/*
 * We suppress the "try" warning here because the close() method's signature
 * allows it to throw InterruptedException which is strongly advised against
 * by AutoCloseable (see: http://docs.oracle.com/javase/7/docs/api/java/lang/AutoCloseable.html#close()).
 * close() will never throw an InterruptedException but the exception remains in the
 * signature for backwards compatibility purposes.
*/
// ZooKeeper的内部类框架图如下图所示
//           ZKWatchManager
//           States
//                                  |-- ExistsWatchRegistration
//                                  |
//           WatchRegistration  ------- DataWatchRegistration
//                                  |
//                                  |-- ChildWatchRegistration　
// 　
//  ZKWatchManager，Zookeeper的Watcher管理者，其源码在之前已经分析过，不再累赘。
//
//　WatchRegistration，抽象类，用作watch注册。
//
//　ExistsWatchRegistration，存在性watch注册。
//
//　DataWatchRegistration，数据watch注册。
//
//　ChildWatchRegistration，子节点注册。
//
//　States，枚举类型，表示服务器的状态

@SuppressWarnings("try")
@InterfaceAudience.Public
public class ZooKeeper implements AutoCloseable {

    /**
     * @deprecated Use {@link ZKClientConfig#ZOOKEEPER_CLIENT_CNXN_SOCKET}
     *             instead.
     */
    // 客户端Socket
    @Deprecated
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
    // Setting this to "true" will enable encrypted client-server communication.

    /**
     * @deprecated Use {@link ZKClientConfig#SECURE_CLIENT}
     *             instead.
     */
    @Deprecated
    public static final String SECURE_CLIENT = "zookeeper.client.secure";
    // 客户端，用来管理客户端与服务端的连接
    // ZooKeeper类维护一个ClientCnxn类，用来管理客户端与服务端的连接。
    protected final ClientCnxn cnxn;
    private static final Logger LOG;
    static {
        //Keep these two lines together to keep the initialization order explicit将这两行保持在一起以保持初始化顺序显式
        // 初始化
        LOG = LoggerFactory.getLogger(ZooKeeper.class);
        Environment.logEnv("Client environment:", LOG);
    }

    protected final HostProvider hostProvider;

    /**
     * This function allows a client to update the connection string by providing 
     * a new comma separated list of host:port pairs, each corresponding to a 
     * ZooKeeper server. 
     * <p>
     * The function invokes a <a href="https://issues.apache.org/jira/browse/ZOOKEEPER-1355">
     * probabilistic load-balancing algorithm</a> which may cause the client to disconnect from 
     * its current host with the goal to achieve expected uniform number of connections per server 
     * in the new list. In case the current host to which the client is connected is not in the new
     * list this call will always cause the connection to be dropped. Otherwise, the decision
     * is based on whether the number of servers has increased or decreased and by how much.
     * For example, if the previous connection string contained 3 hosts and now the list contains
     * these 3 hosts and 2 more hosts, 40% of clients connected to each of the 3 hosts will
     * move to one of the new hosts in order to balance the load. The algorithm will disconnect 
     * from the current host with probability 0.4 and in this case cause the client to connect 
     * to one of the 2 new hosts, chosen at random.
     * <p>
     * If the connection is dropped, the client moves to a special mode "reconfigMode" where he chooses
     * a new server to connect to using the probabilistic algorithm. After finding a server,
     * or exhausting all servers in the new list after trying all of them and failing to connect,
     * the client moves back to the normal mode of operation where it will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed (or the session is expired by the server).

     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *            If the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).     
     *
     * @throws IOException in cases of network failure     
     */
    public void updateServerList(String connectString) throws IOException {
        ConnectStringParser connectStringParser = new ConnectStringParser(connectString);
        Collection<InetSocketAddress> serverAddresses = connectStringParser.getServerAddresses();

        ClientCnxnSocket clientCnxnSocket = cnxn.sendThread.getClientCnxnSocket();
        InetSocketAddress currentHost = (InetSocketAddress) clientCnxnSocket.getRemoteSocketAddress();

        boolean reconfigMode = hostProvider.updateServerList(serverAddresses, currentHost);

        // cause disconnection - this will cause next to be called
        // which will in turn call nextReconfigMode
        if (reconfigMode) clientCnxnSocket.testableCloseSocket();
    }

    public ZooKeeperSaslClient getSaslClient() {
        return cnxn.zooKeeperSaslClient;
    }

    protected final ZKWatchManager watchManager;

    private final ZKClientConfig clientConfig;

    public ZKClientConfig getClientConfig() {
        return clientConfig;
    }

    protected List<String> getDataWatches() {
        synchronized(watchManager.dataWatches) {
            List<String> rc = new ArrayList<String>(watchManager.dataWatches.keySet());
            return rc;
        }
    }
    protected List<String> getExistWatches() {
        synchronized(watchManager.existWatches) {
            List<String> rc =  new ArrayList<String>(watchManager.existWatches.keySet());
            return rc;
        }
    }
    protected List<String> getChildWatches() {
        synchronized(watchManager.childWatches) {
            List<String> rc = new ArrayList<String>(watchManager.childWatches.keySet());
            return rc;
        }
    }

    /**
     * Manage watchers & handle events generated by the ClientCnxn object.
     *
     * We are implementing this as a nested class of ZooKeeper so that
     * the public methods will not be exposed as part of the ZooKeeper client
     * API.
     *
     * KWatchManager，Zookeeper的内部类，继承ClientWatchManager。
     *
     */
    static class ZKWatchManager implements ClientWatchManager {
        //针对内容的watch   path -> set<Watcher>
        private final Map<String, Set<Watcher>> dataWatches = new HashMap<String, Set<Watcher>>();
        //针对exist API相关的watch   path -> set<Watcher>
        private final Map<String, Set<Watcher>> existWatches = new HashMap<String, Set<Watcher>>();
        //针对getChildren API相关的watch   path -> set<Watcher>
        private final Map<String, Set<Watcher>> childWatches = new HashMap<String, Set<Watcher>>();
        //client传递的,默认的watcher实现
        private boolean disableAutoWatchReset;

        ZKWatchManager(boolean disableAutoWatchReset) {
            this.disableAutoWatchReset = disableAutoWatchReset;
        }

        protected volatile Watcher defaultWatcher;

        final private void addTo(Set<Watcher> from, Set<Watcher> to) {
            if (from != null) {
                to.addAll(from);
            }
        }

        public Map<EventType, Set<Watcher>> removeWatcher(String clientPath,
                Watcher watcher, WatcherType watcherType, boolean local, int rc)
                throws KeeperException {
            // Validate the provided znode path contains the given watcher of
            // watcherType
            containsWatcher(clientPath, watcher, watcherType);

            Map<EventType, Set<Watcher>> removedWatchers = new HashMap<EventType, Set<Watcher>>();
            HashSet<Watcher> childWatchersToRem = new HashSet<Watcher>();
            removedWatchers
                    .put(EventType.ChildWatchRemoved, childWatchersToRem);
            HashSet<Watcher> dataWatchersToRem = new HashSet<Watcher>();
            removedWatchers.put(EventType.DataWatchRemoved, dataWatchersToRem);
            boolean removedWatcher = false;
            switch (watcherType) {
            case Children: {
                synchronized (childWatches) {
                    removedWatcher = removeWatches(childWatches, watcher,
                            clientPath, local, rc, childWatchersToRem);
                }
                break;
            }
            case Data: {
                synchronized (dataWatches) {
                    removedWatcher = removeWatches(dataWatches, watcher,
                            clientPath, local, rc, dataWatchersToRem);
                }

                synchronized (existWatches) {
                    boolean removedDataWatcher = removeWatches(existWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
                break;
            }
            case Any: {
                synchronized (childWatches) {
                    removedWatcher = removeWatches(childWatches, watcher,
                            clientPath, local, rc, childWatchersToRem);
                }

                synchronized (dataWatches) {
                    boolean removedDataWatcher = removeWatches(dataWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
                synchronized (existWatches) {
                    boolean removedDataWatcher = removeWatches(existWatches,
                            watcher, clientPath, local, rc, dataWatchersToRem);
                    removedWatcher |= removedDataWatcher;
                }
            }
            }
            // Watcher function doesn't exists for the specified params
            if (!removedWatcher) {
                throw new KeeperException.NoWatcherException(clientPath);
            }
            return removedWatchers;
        }

        /**
         * @param path
         * @param watcherObj
         * @param pathVsWatchers
         * @return 判断path对应的watcherObj是否在pathVsWatchers中存在，存在则返回true，否则返回false
         */
        private boolean contains(String path, Watcher watcherObj,
                Map<String, Set<Watcher>> pathVsWatchers) {
            boolean watcherExists = true;
            if (pathVsWatchers == null || pathVsWatchers.size() == 0) {
                watcherExists = false;
            } else {
                Set<Watcher> watchers = pathVsWatchers.get(path);
                if (watchers == null) {
                    watcherExists = false;
                } else if (watcherObj == null) {
                    watcherExists = watchers.size() > 0;
                } else {
                    watcherExists = watchers.contains(watcherObj);
                }
            }
            return watcherExists;
        }

        /**
         * Validate the provided znode path contains the given watcher and watcherType
         *
         * 验证提供的znode路径包含给定的watcher和watcherType
         * 
         * @param path
         *            - client path
         * @param watcher
         *            - watcher object reference
         * @param watcherType
         *            - type of the watcher
         * @throws NoWatcherException
        */
        void containsWatcher(String path, Watcher watcher,
                WatcherType watcherType) throws NoWatcherException{
            boolean containsWatcher = false;
            switch (watcherType) {
            case Children: {
                synchronized (childWatches) {
                    containsWatcher = contains(path, watcher, childWatches);
                }
                break;
            }
            case Data: {
                synchronized (dataWatches) {
                    containsWatcher = contains(path, watcher, dataWatches);
                }

                synchronized (existWatches) {
                    boolean contains_temp = contains(path, watcher,
                            existWatches);
                    containsWatcher |= contains_temp;
                }
                break;
            }
            case Any: {
                synchronized (childWatches) {
                    containsWatcher = contains(path, watcher, childWatches);
                }

                synchronized (dataWatches) {
                    boolean contains_temp = contains(path, watcher, dataWatches);
                    containsWatcher |= contains_temp;
                }
                synchronized (existWatches) {
                    boolean contains_temp = contains(path, watcher,
                            existWatches);
                    containsWatcher |= contains_temp;
                }
            }
            }
            // Watcher function doesn't exists for the specified params 指定的参数不存在Watcher函数
            if (!containsWatcher) {
                throw new KeeperException.NoWatcherException(path);
            }
        }

        protected boolean removeWatches(Map<String, Set<Watcher>> pathVsWatcher,
                Watcher watcher, String path, boolean local, int rc,
                Set<Watcher> removedWatchers) throws KeeperException {
            if (!local && rc != Code.OK.intValue()) {
                throw KeeperException
                        .create(KeeperException.Code.get(rc), path);
            }
            boolean success = false;
            // When local flag is true, remove watchers for the given path
            // irrespective of rc. Otherwise shouldn't remove watchers locally
            // when sees failure from server.
            if (rc == Code.OK.intValue() || (local && rc != Code.OK.intValue())) {
                // Remove all the watchers for the given path
                if (watcher == null) {
                    Set<Watcher> pathWatchers = pathVsWatcher.remove(path);
                    if (pathWatchers != null) {
                        // found path watchers
                        removedWatchers.addAll(pathWatchers);
                        success = true;
                    }
                } else {
                    Set<Watcher> watchers = pathVsWatcher.get(path);
                    if (watchers != null) {
                        if (watchers.remove(watcher)) {
                            // found path watcher
                            removedWatchers.add(watcher);
                            // cleanup <path vs watchlist>
                            if (watchers.size() <= 0) {
                                pathVsWatcher.remove(path);
                            }
                            success = true;
                        }
                    }
                }
            }
            return success;
        }
        
        /* (non-Javadoc)
         * @see org.apache.zookeeper.ClientWatchManager#materialize(Event.KeeperState, 
         *                                                        Event.EventType, java.lang.String)
         */
        // 核心方法--该方法表示事件发生时，返回需要被通知的Watcher集合，可能为空集合。
        @Override
        public Set<Watcher> materialize(Watcher.Event.KeeperState state,
                                        Watcher.Event.EventType type,
                                        String clientPath)
        {
            Set<Watcher> result = new HashSet<Watcher>();

            switch (type) {
            case None:  //eventType是null
                // 则所有dataWatches,existWatches,childWatches都需要被通知
                // 当客户端删除当前连接并重新连接到服务器时，所有现有监视都被视为已触发，但未传递的事件将丢失。
                // 为了模拟这一点，客户端将生成一个特殊事件来告诉事件处理程序已删除连接。
                // 此特殊事件具有EventType None和KeeperState Disconnected。
                result.add(defaultWatcher);//添加默认watcher
                boolean clear = disableAutoWatchReset && state != Watcher.Event.KeeperState.SyncConnected;//获取clear标记
                synchronized(dataWatches) {
                    for(Set<Watcher> ws: dataWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        dataWatches.clear();
                    }
                }

                synchronized(existWatches) {
                    for(Set<Watcher> ws: existWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        existWatches.clear();
                    }
                }

                synchronized(childWatches) {
                    for(Set<Watcher> ws: childWatches.values()) {
                        result.addAll(ws);
                    }
                    if (clear) {
                        childWatches.clear();
                    }
                }

                return result;
            case NodeDataChanged:// 节点数据变化
            case NodeCreated: // 创建节点
                //如果节点内容变化或者创建
                synchronized (dataWatches) {
                    addTo(dataWatches.remove(clientPath), result);//从dataWatches中移除，并且添加到result中
                }
                synchronized (existWatches) {
                    addTo(existWatches.remove(clientPath), result);//从existWatches中移除，并且添加到result中
                }
                break;
            case NodeChildrenChanged:// 节点子节点变化
                synchronized (childWatches) {
                    addTo(childWatches.remove(clientPath), result);
                }
                break;
            case NodeDeleted: // 删除节点
                synchronized (dataWatches) {
                    addTo(dataWatches.remove(clientPath), result);
                }
                // XXX This shouldn't be needed, but just in case
                synchronized (existWatches) {
                    Set<Watcher> list = existWatches.remove(clientPath);
                    if (list != null) {
                        addTo(list, result);
                        LOG.warn("We are triggering an exists watch for delete! Shouldn't happen!");
                    }
                }
                synchronized (childWatches) {
                    addTo(childWatches.remove(clientPath), result);
                }
                break;
            default://默认处理
                String msg = "Unhandled watch event type " + type
                    + " with state " + state + " on path " + clientPath;
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
            //返回结果
            return result;
        }
    }

    /**
     * Register a watcher for a particular path.为特定路径注册观察者。
     * 抽象类，用作watch注册
     * client中管理watch注册的类
     */
    public abstract class WatchRegistration {
        // 注册的watcher
        private Watcher watcher;
        // 监听的znode path
        private String clientPath;
        public WatchRegistration(Watcher watcher, String clientPath)
        {
            this.watcher = watcher;
            this.clientPath = clientPath;
        }

        // 根据response的resultCode来获取所有注册的path以及对应的watcher集合
        abstract protected Map<String, Set<Watcher>> getWatches(int rc);

        /**
         * Register the watcher with the set of watches on path.
         * @param rc the result code of the operation that attempted to
         * add the watch on the path.
         */
        // 注册
        public void register(int rc) {
            if (shouldAddWatch(rc)) {// 应该添加监听
                // 获取路径到Watchers集合的键值对，工厂模式
                Map<String, Set<Watcher>> watches = getWatches(rc);
                synchronized(watches) {
                    // 通过路径获取watcher集合
                    Set<Watcher> watchers = watches.get(clientPath);
                    if (watchers == null) {
                        watchers = new HashSet<Watcher>();
                        // 将路径和watchers集合存入
                        watches.put(clientPath, watchers);
                    }
                    // 添加至watchers集合
                    watchers.add(watcher);
                }
            }
        }
        /**
         * Determine whether the watch should be added based on return code.
         * 确定是否应根据返回代码添加watch。
         * @param rc the result code of the operation that attempted to add the
         * watch on the node
         * @return true if the watch should be added, otw false
         */
        // 根据resultCode判断是否需要添加，判断rc是否为0,如果是0就可以注册，这是默认的策略，子类有可能会覆盖此方法
        protected boolean shouldAddWatch(int rc) {
            return rc == 0;
        }
    }

    /** Handle the special case of exists watches - they add a watcher
     * even in the case where NONODE result code is returned.
     */
    // 存在性watch注册
    class ExistsWatchRegistration extends WatchRegistration {
        public ExistsWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            // 根据rc是否为0确定返回dataWatches或existsWatches
            return rc == 0 ?  watchManager.dataWatches : watchManager.existWatches;
        }

        @Override
        protected boolean shouldAddWatch(int rc) {
            // 判断rc是否为0或者rc是否等于NONODE的值
            return rc == 0 || rc == KeeperException.Code.NONODE.intValue();
        }
    }

    // 数据watch注册
    class DataWatchRegistration extends WatchRegistration {
        public DataWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            // 直接返回dataWatches
            return watchManager.dataWatches;
        }
    }

    // 子节点注册
    class ChildWatchRegistration extends WatchRegistration {
        public ChildWatchRegistration(Watcher watcher, String clientPath) {
            super(watcher, clientPath);
        }

        @Override
        protected Map<String, Set<Watcher>> getWatches(int rc) {
            // 直接返回childWatches
            return watchManager.childWatches;
        }
    }

    // 表示服务器的状态
    @InterfaceAudience.Public
    public enum States {
        // 代表服务器的状态
        CONNECTING,
        ASSOCIATING,
        CONNECTED,
        CONNECTEDREADONLY,
        CLOSED,
        AUTH_FAILED,
        NOT_CONNECTED;
        // 判断服务器是否存活          不为关闭状态并且未认证失败
        public boolean isAlive() {
            return this != CLOSED && this != AUTH_FAILED;
        }

        /**
         * Returns whether we are connected to a server (which
         * could possibly be read-only, if this client is allowed
         * to go to read-only mode)
         * */
        // 判断客户端是否连接至服务端       已连接或者只读连接
        public boolean isConnected() {
            return this == CONNECTED || this == CONNECTEDREADONLY;
        }
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
        throws IOException
    {
        this(connectString, sessionTimeout, watcher, false);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param conf
     *            (added in 3.5.2) passing this conf object gives each client the flexibility of
     *            configuring properties differently compared to other instances
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, false, conf);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * For backward compatibility, there is another version
     * {@link #ZooKeeper(String, int, Watcher, boolean)} which uses
     * default {@link StaticHostProvider}
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @param aHostProvider
     *            use this as HostProvider to enable custom behaviour.
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, HostProvider aHostProvider)
            throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                aHostProvider, null);
    }


    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * For backward compatibility, there is another version
     * {@link #ZooKeeper(String, int, Watcher, boolean)} which uses default
     * {@link StaticHostProvider}
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @param aHostProvider
     *            use this as HostProvider to enable custom behaviour.
     * @param clientConfig
     *            (added in 3.5.2) passing this conf object gives each client the flexibility of
     *            configuring properties differently compared to other instances
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, HostProvider aHostProvider,
            ZKClientConfig clientConfig) throws IOException {
        LOG.info("Initiating client connection, connectString=" + connectString
                + " sessionTimeout=" + sessionTimeout + " watcher=" + watcher);

        if (clientConfig == null) {
            clientConfig = new ZKClientConfig();
        }
        this.clientConfig = clientConfig;
        // ZKWatchManager
        watchManager = defaultWatchManager();
        // 初始化默认Watcher
        watchManager.defaultWatcher = watcher;//设置会话默认Watcher
        // 对传入的connectString进行解析
        // connectString 类似于127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002未指定根空间的字符串
        // 或者是127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a指定根空间的字符串,根为/app/a
        ConnectStringParser connectStringParser = new ConnectStringParser(
                connectString);
        // 根据服务器地址列表生成HostProvider
        hostProvider = aHostProvider;
        // 生成客户端管理
        cnxn = createConnection(connectStringParser.getChrootPath(),
                hostProvider, sessionTimeout, this, watchManager,
                getClientCnxnSocket(), canBeReadOnly);
        // 启动
        cnxn.start();
    }

    // @VisibleForTesting
    protected ClientCnxn createConnection(String chrootPath,
            HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
            ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
            boolean canBeReadOnly) throws IOException {
        return new ClientCnxn(chrootPath, hostProvider, sessionTimeout, this,
                watchManager, clientCnxnSocket, canBeReadOnly);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     *
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                createDefaultHostProvider(connectString));
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed.
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *            the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @param conf
     *            (added in 3.5.2) passing this conf object gives each client the flexibility of
     *            configuring properties differently compared to other instances
     * @throws IOException
     *             in cases of network failure
     * @throws IllegalArgumentException
     *             if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly, ZKClientConfig conf) throws IOException {
        this(connectString, sessionTimeout, watcher, canBeReadOnly,
                // 根据connectString 服务器地址列表生成HostProvider
                createDefaultHostProvider(connectString), conf);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed (or the session is expired by the server).
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * Use {@link #getSessionId} and {@link #getSessionPasswd} on an established
     * client connection, these values must be passed as sessionId and
     * sessionPasswd respectively if reconnecting. Otherwise, if not
     * reconnecting, use the other constructor which does not require these
     * parameters.
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *            If the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param sessionId
     *            specific session id to use if reconnecting
     * @param sessionPasswd
     *            password for this session
     *
     * @throws IOException in cases of network failure
     * @throws IllegalArgumentException if an invalid chroot path is specified
     * @throws IllegalArgumentException for an invalid list of ZooKeeper hosts
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd)
        throws IOException
    {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, false);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed (or the session is expired by the server).
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * Use {@link #getSessionId} and {@link #getSessionPasswd} on an established
     * client connection, these values must be passed as sessionId and
     * sessionPasswd respectively if reconnecting. Otherwise, if not
     * reconnecting, use the other constructor which does not require these
     * parameters.
     * <p>
     * For backward compatibility, there is another version
     * {@link #ZooKeeper(String, int, Watcher, long, byte[], boolean)} which uses
     * default {@link StaticHostProvider}
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *            If the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param sessionId
     *            specific session id to use if reconnecting
     * @param sessionPasswd
     *            password for this session
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @param aHostProvider
     *            use this as HostProvider to enable custom behaviour.
     * @throws IOException in cases of network failure
     * @throws IllegalArgumentException if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly,
            HostProvider aHostProvider) throws IOException {
    	this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd,
    			canBeReadOnly, aHostProvider, null);
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed (or the session is expired by the server).
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * Use {@link #getSessionId} and {@link #getSessionPasswd} on an established
     * client connection, these values must be passed as sessionId and
     * sessionPasswd respectively if reconnecting. Otherwise, if not
     * reconnecting, use the other constructor which does not require these
     * parameters.
     * <p>
     * For backward compatibility, there is another version
     * {@link #ZooKeeper(String, int, Watcher, long, byte[], boolean)} which uses
     * default {@link StaticHostProvider}
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *            If the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param sessionId
     *            specific session id to use if reconnecting
     * @param sessionPasswd
     *            password for this session
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @param aHostProvider
     *            use this as HostProvider to enable custom behaviour.使用它作为HostProvider来启用自定义行为。
     * @param clientConfig
     *            (added in 3.5.2) passing this conf object gives each client the flexibility of
     *            configuring properties differently compared to other instances
     * @throws IOException in cases of network failure
     * @throws IllegalArgumentException if an invalid chroot path is specified
     *
     * @since 3.5.5
     */

    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
    		long sessionId, byte[] sessionPasswd, boolean canBeReadOnly,
    		HostProvider aHostProvider, ZKClientConfig clientConfig) throws IOException {
    	LOG.info("Initiating client connection, connectString=" + connectString
    			+ " sessionTimeout=" + sessionTimeout
    			+ " watcher=" + watcher
    			+ " sessionId=" + Long.toHexString(sessionId)
    			+ " sessionPasswd="
    			+ (sessionPasswd == null ? "<null>" : "<hidden>"));

        if (clientConfig == null) {
            clientConfig = new ZKClientConfig();
        }
        this.clientConfig = clientConfig;
        // 初始化默认Watcher
        watchManager = defaultWatchManager();
        // 这个默认的watcher就是创建客户端的时候传入的
        watchManager.defaultWatcher = watcher;
        // 对传入的connectString进行解析
        // connectString 类似于127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002未指定根空间的字符串
        // 或者是127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a指定根空间的字符串,根为/app/a
        ConnectStringParser connectStringParser = new ConnectStringParser(
                connectString);
        hostProvider = aHostProvider;

        // 生成客户端管理
        cnxn = new ClientCnxn(connectStringParser.getChrootPath(),
                hostProvider, sessionTimeout, this, watchManager,
                getClientCnxnSocket(), sessionId, sessionPasswd, canBeReadOnly);

        // 此型构造函数和之前构造函数的区别在于本构造函数提供了sessionId和sessionPwd，
        // 这表明用户已经之前已经连接过服务端，所以能够获取到sessionId，其流程与之前的构造函数类似。
        // 设置客户端的seenRwServerBefore字段为true(因为用户提供了sessionId，表示肯定已经连接过)
        cnxn.seenRwServerBefore = true; // since user has provided sessionId
        // 启动
        cnxn.start();
    }

    /**
     * To create a ZooKeeper client object, the application needs to pass a
     * connection string containing a comma separated list of host:port pairs,
     * each corresponding to a ZooKeeper server.
     * <p>
     * Session establishment is asynchronous. This constructor will initiate
     * connection to the server and return immediately - potentially (usually)
     * before the session is fully established. The watcher argument specifies
     * the watcher that will be notified of any changes in state. This
     * notification can come at any point before or after the constructor call
     * has returned.
     * <p>
     * The instantiated ZooKeeper client object will pick an arbitrary server
     * from the connectString and attempt to connect to it. If establishment of
     * the connection fails, another server in the connect string will be tried
     * (the order is non-deterministic, as we random shuffle the list), until a
     * connection is established. The client will continue attempts until the
     * session is explicitly closed (or the session is expired by the server).
     * <p>
     * Added in 3.2.0: An optional "chroot" suffix may also be appended to the
     * connection string. This will run the client commands while interpreting
     * all paths relative to this root (similar to the unix chroot command).
     * <p>
     * Use {@link #getSessionId} and {@link #getSessionPasswd} on an established
     * client connection, these values must be passed as sessionId and
     * sessionPasswd respectively if reconnecting. Otherwise, if not
     * reconnecting, use the other constructor which does not require these
     * parameters.
     * <p>
     * This constructor uses a StaticHostProvider; there is another one
     * to enable custom behaviour.
     *
     * @param connectString
     *            comma separated host:port pairs, each corresponding to a zk
     *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *            If the optional chroot suffix is used the example would look
     *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *            where the client would be rooted at "/app/a" and all paths
     *            would be relative to this root - ie getting/setting/etc...
     *            "/foo/bar" would result in operations being run on
     *            "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout
     *            session timeout in milliseconds
     * @param watcher
     *            a watcher object which will be notified of state changes, may
     *            also be notified for node events
     * @param sessionId
     *            specific session id to use if reconnecting
     * @param sessionPasswd
     *            password for this session
     * @param canBeReadOnly
     *            (added in 3.4) whether the created client is allowed to go to
     *            read-only mode in case of partitioning. Read-only mode
     *            basically means that if the client can't find any majority
     *            servers but there's partitioned server it could reach, it
     *            connects to one in read-only mode, i.e. read requests are
     *            allowed while write requests are not. It continues seeking for
     *            majority in the background.
     * @throws IOException in cases of network failure
     * @throws IllegalArgumentException if an invalid chroot path is specified
     */
    public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher,
            long sessionId, byte[] sessionPasswd, boolean canBeReadOnly)
            throws IOException {
        this(connectString, sessionTimeout, watcher, sessionId, sessionPasswd,
                canBeReadOnly, createDefaultHostProvider(connectString));
    }

    // default hostprovider
    private static HostProvider createDefaultHostProvider(String connectString) {
        return new StaticHostProvider(
                new ConnectStringParser(connectString).getServerAddresses());
    }

    // VisibleForTesting
    public Testable getTestable() {
        return new ZooKeeperTestable(cnxn);
    }

    /* Useful for testing watch handling behavior */
    protected ZKWatchManager defaultWatchManager() {
        // ZKClientConfig.DISABLE_AUTO_WATCH_RESET = "zookeeper.disableAutoWatchReset"  默认是false
        return new ZKWatchManager(getClientConfig().getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET));
    }

    /**
     * The session id for this ZooKeeper client instance. The value returned is
     * not valid until the client connects to a server and may change after a
     * re-connect.
     *
     * This method is NOT thread safe
     *
     * @return current session id
     */
    public long getSessionId() {
        return cnxn.getSessionId();
    }

    /**
     * The session password for this ZooKeeper client instance. The value
     * returned is not valid until the client connects to a server and may
     * change after a re-connect.
     *
     * This method is NOT thread safe
     *
     * @return current session password
     */
    public byte[] getSessionPasswd() {
        return cnxn.getSessionPasswd();
    }

    /**
     * The negotiated session timeout for this ZooKeeper client instance. The
     * value returned is not valid until the client connects to a server and
     * may change after a re-connect.
     *
     * This method is NOT thread safe
     *
     * @return current session timeout
     */
    public int getSessionTimeout() {
        return cnxn.getSessionTimeout();
    }

    /**
     * Add the specified scheme:auth information to this connection.
     *
     * This method is NOT thread safe
     *
     * @param scheme
     * @param auth
     */
    public void addAuthInfo(String scheme, byte auth[]) {
        cnxn.addAuthInfo(scheme, auth);
    }

    /**
     * Specify the default watcher for the connection (overrides the one
     * specified during construction).
     * 注册默认watcher
     * client构造Zookeeper对象时传递，会记录在ZooKeeper.ZKWatchManager#defaultWatcher中
     *
     * @param watcher
     */
    public synchronized void register(Watcher watcher) {
        watchManager.defaultWatcher = watcher;
    }

    /**
     * Close this client object. Once the client is closed, its session becomes
     * invalid. All the ephemeral nodes in the ZooKeeper server associated with
     * the session will be removed. The watches left on those nodes (and on
     * their parents) will be triggered.
     * <p>
     * Added in 3.5.3: <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">try-with-resources</a>
     * may be used instead of calling close directly.
     * </p>
     * <p>
     * This method does not wait for all internal threads to exit.
     * Use the {@link #close(int) } method to wait for all resources to be released
     * </p>
     *
     * @throws InterruptedException
     */
    public synchronized void close() throws InterruptedException {
        if (!cnxn.getState().isAlive()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Close called on already closed client");
            }
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing session: 0x" + Long.toHexString(getSessionId()));
        }

        try {
            cnxn.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring unexpected exception during close", e);
            }
        }

        LOG.info("Session: 0x" + Long.toHexString(getSessionId()) + " closed");
    }

    /**
     * Close this client object as the {@link #close() } method.
     * This method will wait for internal resources to be released.
     *
     * @param waitForShutdownTimeoutMs timeout (in milliseconds) to wait for resources to be released.
     * Use zero or a negative value to skip the wait
     * @throws InterruptedException
     * @return true if waitForShutdownTimeout is greater than zero and all of the resources have been released
     *
     * @since 3.5.4
     */
    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException {
        close();
        return testableWaitForShutdown(waitForShutdownTimeoutMs);
    }

    /**
     * Prepend the chroot to the client path (if present). The expectation of
     * this function is that the client path has been validated before this
     * function is called
     * @param clientPath path to the node
     * @return server view of the path (chroot prepended to client path)
     */
    private String prependChroot(String clientPath) {
        if (cnxn.chrootPath != null) {
            // handle clientPath = "/"
            if (clientPath.length() == 1) {
                return cnxn.chrootPath;
            }
            return cnxn.chrootPath + clientPath;
        } else {
            return clientPath;
        }
    }

    /**
     * Create a node with the given path. The node data will be the given data,
     * and node acl will be the given acl.
     * <p>
     * The flags argument specifies whether the created node will be ephemeral
     * or not.
     * <p>
     * An ephemeral node will be removed by the ZooKeeper automatically when the
     * session associated with the creation of the node expires.
     * <p>
     * The flags argument can also specify to create a sequential node. The
     * actual path name of a sequential node will be the given path plus a
     * suffix "i" where i is the current sequential number of the node. The sequence
     * number is always fixed length of 10 digits, 0 padded. Once
     * such a node is created, the sequential number will be incremented by one.
     * <p>
     * If a node with the same actual path already exists in the ZooKeeper, a
     * KeeperException with error code KeeperException.NodeExists will be
     * thrown. Note that since a different actual path is used for each
     * invocation of creating sequential node with the same path argument, the
     * call will never throw "file exists" KeeperException.
     * <p>
     * If the parent node does not exist in the ZooKeeper, a KeeperException
     * with error code KeeperException.NoNode will be thrown.
     * <p>
     * An ephemeral node cannot have children. If the parent node of the given
     * path is ephemeral, a KeeperException with error code
     * KeeperException.NoChildrenForEphemerals will be thrown.
     * <p>
     * This operation, if successful, will trigger all the watches left on the
     * node of the given path by exists and getData API calls, and the watches
     * left on the parent node by getChildren API calls.
     * <p>
     * If a node is created successfully, the ZooKeeper server will trigger the
     * watches on the path left by exists calls, and the watches on the parent
     * of the node by getChildren calls.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperExecption to be thrown.
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     * @return the actual path of the created node
     * @throws KeeperException if the server returns a non-zero error code
     * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty/+*
     * @throws InterruptedException if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String create(final String path, byte data[], List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        // 验证路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, -1);
        validateACL(acl);
        // 添加根空间
        final String serverPath = prependChroot(clientPath);

        // 生成新请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);
        // 生成创建节点请求
        CreateRequest request = new CreateRequest();
        // 生成创建节点响应
        CreateResponse response = new CreateResponse();
        request.setData(data);
        request.setFlags(createMode.toFlag());
        request.setPath(serverPath);
        request.setAcl(acl);
        // 提交请求
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            // 请求的响应的错误码不为0，则抛出异常
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (cnxn.chrootPath == null) { // 根空间为空
            // 则返回响应中的路径
            return response.getPath();
        } else {
            // 除去根空间后返回
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }

    /**
     * Create a node with the given path and returns the Stat of that node. The
     * node data will be the given data and node acl will be the given acl.
     * <p>
     * The flags argument specifies whether the created node will be ephemeral
     * or not.
     * <p>
     * An ephemeral node will be removed by the ZooKeeper automatically when the
     * session associated with the creation of the node expires.
     * <p>
     * The flags argument can also specify to create a sequential node. The
     * actual path name of a sequential node will be the given path plus a
     * suffix "i" where i is the current sequential number of the node. The sequence
     * number is always fixed length of 10 digits, 0 padded. Once
     * such a node is created, the sequential number will be incremented by one.
     * <p>
     * If a node with the same actual path already exists in the ZooKeeper, a
     * KeeperException with error code KeeperException.NodeExists will be
     * thrown. Note that since a different actual path is used for each
     * invocation of creating sequential node with the same path argument, the
     * call will never throw "file exists" KeeperException.
     * <p>
     * If the parent node does not exist in the ZooKeeper, a KeeperException
     * with error code KeeperException.NoNode will be thrown.
     * <p>
     * An ephemeral node cannot have children. If the parent node of the given
     * path is ephemeral, a KeeperException with error code
     * KeeperException.NoChildrenForEphemerals will be thrown.
     * <p>
     * This operation, if successful, will trigger all the watches left on the
     * node of the given path by exists and getData API calls, and the watches
     * left on the parent node by getChildren API calls.
     * <p>
     * If a node is created successfully, the ZooKeeper server will trigger the
     * watches on the path left by exists calls, and the watches on the parent
     * of the node by getChildren calls.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperExecption to be thrown.
     *
     * @param path
     *                the path for the node
     * @param data
     *                the initial data for the node
     * @param acl
     *                the acl for the node
     * @param createMode
     *                specifying whether the node to be created is ephemeral
     *                and/or sequential
     * @param stat
     *                The output Stat object.
     * @return the actual path of the created node
     * @throws KeeperException if the server returns a non-zero error code
     * @throws KeeperException.InvalidACLException if the ACL is invalid, null, or empty
     * @throws InterruptedException if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Stat stat)
            throws KeeperException, InterruptedException {
        return create(path, data, acl, createMode, stat, -1);
    }

    /**
     * same as {@link #create(String, byte[], List, CreateMode, Stat)} but
     * allows for specifying a TTL when mode is {@link CreateMode#PERSISTENT_WITH_TTL}
     * or {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}. If the znode has not been modified
     * within the given TTL, it will be deleted once it has no children. The TTL unit is
     * milliseconds and must be greater than 0 and less than or equal to
     * {@link EphemeralType#maxValue()} for {@link EphemeralType#TTL}.
     */
    public String create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Stat stat, long ttl)
            throws KeeperException, InterruptedException {
        final String clientPath = path;
        // 验证路径是否合法
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, ttl);
        validateACL(acl);
        // 添加根空间
        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        Create2Response response = new Create2Response();
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);
        ReplyHeader r = cnxn.submitRequest(h, record, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        if (cnxn.chrootPath == null) {
            return response.getPath();
        } else {
            return response.getPath().substring(cnxn.chrootPath.length());
        }
    }

    private void setCreateHeader(CreateMode createMode, RequestHeader h) {
        if (createMode.isTTL()) {
            h.setType(ZooDefs.OpCode.createTTL);
        } else {
            h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create2);
        }
    }

    private Record makeCreateRecord(CreateMode createMode, String serverPath, byte[] data, List<ACL> acl, long ttl) {
        Record record;
        if (createMode.isTTL()) {
            CreateTTLRequest request = new CreateTTLRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            request.setTtl(ttl);
            record = request;
        } else {
            CreateRequest request = new CreateRequest();
            request.setData(data);
            request.setFlags(createMode.toFlag());
            request.setPath(serverPath);
            request.setAcl(acl);
            record = request;
        }
        return record;
    }

    /**
     * The asynchronous version of create.
     * 该create函数是异步的，其大致步骤与同步版的create函数相同，只是最后其会将请求打包成packet，然后放入队列等待提交。
     *
     * @see #create(String, byte[], List, CreateMode)
     */
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, StringCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, -1);
        // 添加根空间
        final String serverPath = prependChroot(clientPath);
        // 新生请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(createMode.isContainer() ? ZooDefs.OpCode.createContainer : ZooDefs.OpCode.create);
        // 新生创建节点请求
        CreateRequest request = new CreateRequest();
        // 新生创建节点响应
        CreateResponse response = new CreateResponse();
        ReplyHeader r = new ReplyHeader();
        // 设置请求的数据
        request.setData(data);
        // 设置请求对应的Flag
        request.setFlags(createMode.toFlag());
        // 设置服务器路径
        request.setPath(serverPath);
        // 设置请求的ACL列表
        request.setAcl(acl);
        // 封装成packet放入队列，等待提交
        cnxn.queuePacket(h, r, request, response, cb, clientPath,
                serverPath, ctx, null);
    }

    /**
     * The asynchronous version of create.
     *
     * @see #create(String, byte[], List, CreateMode, Stat)
     */
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Create2Callback cb, Object ctx)
    {
        create(path, data, acl, createMode, cb, ctx, -1);
    }

    /**
     * The asynchronous version of create with ttl.
     *
     * @see #create(String, byte[], List, CreateMode, Stat, long)
     */
    public void create(final String path, byte data[], List<ACL> acl,
            CreateMode createMode, Create2Callback cb, Object ctx, long ttl)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath, createMode.isSequential());
        EphemeralType.validateTTL(createMode, ttl);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        setCreateHeader(createMode, h);
        ReplyHeader r = new ReplyHeader();
        Create2Response response = new Create2Response();
        Record record = makeCreateRecord(createMode, serverPath, data, acl, ttl);
        cnxn.queuePacket(h, r, record, response, cb, clientPath,
                serverPath, ctx, null);
    }

    /**
     * 该函数是同步的
     *
     * Delete the node with the given path. The call will succeed if such a node
     * exists, and the given version matches the node's version (if the given
     * version is -1, it matches any node's versions).
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if the nodes does not exist.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given version does not match the node's version.
     * <p>
     * A KeeperException with error code KeeperException.NotEmpty will be thrown
     * if the node has children.
     * <p>
     * This operation, if successful, will trigger all the watches on the node
     * of the given path left by exists API calls, and the watches on the parent
     * node left by getChildren API calls.
     *
     * @param path
     *                the path of the node to be deleted.
     * @param version
     *                the expected node version.
     * @throws InterruptedException IF the server transaction is interrupted
     * @throws KeeperException If the server signals an error with a non-zero
     *   return code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public void delete(final String path, int version)
        throws InterruptedException, KeeperException
    {
        final String clientPath = path;
        // 验证路径的合法性
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case
        // specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {// 判断是否是"/"，即zookeeper的根目录，根目录无法删除
            // a bit of a hack, but delete(/) will never succeed and ensures
            // that the same semantics are maintained
            serverPath = clientPath;
        } else { // 添加根空间
            serverPath = prependChroot(clientPath);
        }

        // 创建请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(ZooDefs.OpCode.delete);
        // 新生删除请求
        DeleteRequest request = new DeleteRequest();
        // 设置路径
        request.setPath(serverPath);
        // 设置版本号
        request.setVersion(version);
        // 创建响应头
        ReplyHeader r = cnxn.submitRequest(h, request, null, null);
        if (r.getErr() != 0) { // 判断返回码
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
    }

    /**
     * 该函数用于执行多个操作或者不执行，其首先会验证每个操作的合法性，
     * 然后将每个操作添加根空间后加入到事务列表中，之后会调用multiInternal函数
     * Executes multiple ZooKeeper operations or none of them.
     * <p>
     * On success, a list of results is returned.
     * On failure, an exception is raised which contains partial results and
     * error details, see {@link KeeperException#getResults}
     * <p>
     * Note: The maximum allowable size of all of the data arrays in all of
     * the setData operations in this single request is typically 1 MB
     * (1,048,576 bytes). This limit is specified on the server via
     * <a href="http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#Unsafe+Options">jute.maxbuffer</a>.
     * Requests larger than this will cause a KeeperException to be
     * thrown.
     *
     * @param ops An iterable that contains the operations to be done.
     * These should be created using the factory methods on {@link Op}.
     * @return A list of results, one for each input Op, the order of
     * which exactly matches the order of the <code>ops</code> input
     * operations.
     * @throws InterruptedException If the operation was interrupted.
     * The operation may or may not have succeeded, but will not have
     * partially succeeded if this exception is thrown.
     * @throws KeeperException If the operation could not be completed
     * due to some error in doing one of the specified ops.
     * @throws IllegalArgumentException if an invalid path is specified
     *
     * @since 3.4.0
     */
    public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
        // 验证每个操作是否合法
        for (Op op : ops) {
            op.validate();
        }
        // 调用multiInternal后返回
        return multiInternal(generateMultiTransaction(ops));
    }

    /**
     * The asynchronous version of multi.
     *
     * @see #multi(Iterable)
     */
    public void multi(Iterable<Op> ops, MultiCallback cb, Object ctx) {
        List<OpResult> results = validatePath(ops);
        if (results.size() > 0) {
            cb.processResult(KeeperException.Code.BADARGUMENTS.intValue(),
                    null, ctx, results);
            return;
        }
        multiInternal(generateMultiTransaction(ops), cb, ctx);
    }

    private List<OpResult> validatePath(Iterable<Op> ops) {
        List<OpResult> results = new ArrayList<OpResult>();
        boolean error = false;
        for (Op op : ops) {
            try {
                op.validate();
            } catch (IllegalArgumentException iae) {
                LOG.error("IllegalArgumentException: " + iae.getMessage());
                ErrorResult err = new ErrorResult(
                        KeeperException.Code.BADARGUMENTS.intValue());
                results.add(err);
                error = true;
                continue;
            } catch (KeeperException ke) {
                LOG.error("KeeperException: " + ke.getMessage());
                ErrorResult err = new ErrorResult(ke.code().intValue());
                results.add(err);
                error = true;
                continue;
            }
            ErrorResult err = new ErrorResult(
                    KeeperException.Code.RUNTIMEINCONSISTENCY.intValue());
            results.add(err);
        }
        if (false == error) {
            results.clear();
        }
        return results;
    }

    private MultiTransactionRecord generateMultiTransaction(Iterable<Op> ops) {
        // reconstructing transaction with the chroot prefix
        // 新生事务列表
        List<Op> transaction = new ArrayList<Op>();
        for (Op op : ops) {
            transaction.add(withRootPrefix(op));
        }
        // 调用MultiTransactionRecord后返回
        return new MultiTransactionRecord(transaction);
    }

    private Op withRootPrefix(Op op) {
        if (null != op.getPath()) {
            final String serverPath = prependChroot(op.getPath());
            if (!op.getPath().equals(serverPath)) {
                return op.withChroot(serverPath);
            }
        }
        return op;
    }

    protected void multiInternal(MultiTransactionRecord request, MultiCallback cb, Object ctx) {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.multi);
        MultiResponse response = new MultiResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb, null, null, ctx, null);
    }

    /**
     * multiInternal函数会提交多个操作并且等待响应结果集，然后判断结果集中是否有异常，
     * 若有异常则抛出异常，否则返回响应结果集。
     * @param request
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected List<OpResult> multiInternal(MultiTransactionRecord request)
        throws InterruptedException, KeeperException {
        // 创建请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(ZooDefs.OpCode.multi);
        // 新生多重响应
        MultiResponse response = new MultiResponse();
        // 创建响应头
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) { // 判断返回码是否为0
            throw KeeperException.create(KeeperException.Code.get(r.getErr()));
        }
        // 获取响应的结果集
        List<OpResult> results = response.getResultList();
        
        ErrorResult fatalError = null;
        for (OpResult result : results) { // 遍历结果集
            if (result instanceof ErrorResult && ((ErrorResult)result).getErr() != KeeperException.Code.OK.intValue()) {
                //判断结果集中是否出现了异常
                fatalError = (ErrorResult) result;
                break;
            }
        }

        if (fatalError != null) { // 出现了异常
            KeeperException ex = KeeperException.create(KeeperException.Code.get(fatalError.getErr()));
            ex.setMultiResults(results);
            throw ex;
        }

        return results;
    }

    /**
     * A Transaction is a thin wrapper on the {@link #multi} method
     * which provides a builder object that can be used to construct
     * and commit an atomic set of operations.
     *
     * @since 3.4.0
     *
     * @return a Transaction builder object
     */
    public Transaction transaction() {
        return new Transaction(this);
    }

    /**
     * The asynchronous version of delete.
     *
     * @see #delete(String, int)
     */
    public void delete(final String path, int version, VoidCallback cb,
            Object ctx)
    {
        final String clientPath = path;
        // 验证路径是否合法
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case
        // specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {// 判断是否是"/"，即zookeeper的根目录，根目录无法删除
            // a bit of a hack, but delete(/) will never succeed and ensures
            // that the same semantics are maintained
            serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        // 创建请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(ZooDefs.OpCode.delete);
        // 新生删除请求
        DeleteRequest request = new DeleteRequest();
        // 设置路径
        request.setPath(serverPath);
        // 设置版本号
        request.setVersion(version);
        // 封装成packet放入队列，等待提交
        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null);
    }

    /**
     * 该函数是同步的，用于判断指定路径的节点是否存在，值得注意的是，其会对指定路径的结点进行注册监听。
     *
     * Return the stat of the node of the given path. Return null if no such a
     * node exists.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that creates/delete the node or sets
     * the data on the node.
     *
     * @param path the node path
     * @param watcher explicit watcher
     * @return the stat of the node of the given path; return null if no such a
     *         node exists.
     * @throws KeeperException If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat exists(final String path, Watcher watcher)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        // 验证路径是否合法
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {// 创建存在性观察者注册
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }
        // 添加根空间
        final String serverPath = prependChroot(clientPath);

        // 创建请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(ZooDefs.OpCode.exists);
        // 新生节点存在请求
        ExistsRequest request = new ExistsRequest();
        // 设置路径
        request.setPath(serverPath);
        // 设置Watcher
        request.setWatch(watcher != null);
        // 创建设置数据响应
        SetDataResponse response = new SetDataResponse();
        // 提交请求
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) { // 判断返回码
            if (r.getErr() == KeeperException.Code.NONODE.intValue()) {
                return null;
            }
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        // 返回结果的状态
        return response.getStat().getCzxid() == -1 ? null : response.getStat();
    }

    /**
     * Return the stat of the node of the given path. Return null if no such a
     * node exists.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that creates/delete the node or sets
     * the data on the node.
     *
     * @param path
     *                the node path
     * @param watch
     *                whether need to watch this node
     * @return the stat of the node of the given path; return null if no such a
     *         node exists.
     * @throws KeeperException If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public Stat exists(String path, boolean watch) throws KeeperException,
        InterruptedException
    {
        return exists(path, watch ? watchManager.defaultWatcher : null);
    }

    /**
     * 该函数是异步的，与同步的流程相似
     * The asynchronous version of exists.
     *
     * @see #exists(String, Watcher)
     */
    public void exists(final String path, Watcher watcher,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        // 验证路径是否合法
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {// 生成存在性注册
            wcb = new ExistsWatchRegistration(watcher, clientPath);
        }
        // 添加根空间
        final String serverPath = prependChroot(clientPath);

        // 创建请求头
        RequestHeader h = new RequestHeader();
        // 设置请求头类型
        h.setType(ZooDefs.OpCode.exists);
        // 新生节点存在请求
        ExistsRequest request = new ExistsRequest();
        // 设置路径
        request.setPath(serverPath);
        // 设置Watcher
        request.setWatch(watcher != null);
        // 创建设置数据响应
        SetDataResponse response = new SetDataResponse();
        // 将请求封装成packet，放入队列，等待执行
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    /**
     * The asynchronous version of exists.
     *
     * @see #exists(String, boolean)
     */
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is
     * thrown), a watch will be left on the node with the given path. The watch
     * will be triggered by a successful operation that sets data on the node, or
     * deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path the given path
     * @param watcher explicit watcher
     * @param stat the stat of the node
     * @return the data of the node
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException
     {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is
     * thrown), a watch will be left on the node with the given path. The watch
     * will be triggered by a successful operation that sets data on the node, or
     * deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path the given path
     * @param watch whether need to watch this node
     * @param stat the stat of the node
     * @return the data of the node
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getData(path, watch ? watchManager.defaultWatcher : null, stat);
    }

    /**
     * The asynchronous version of getData.
     * getData的异步版本
     *
     * @see #getData(String, Watcher, Stat)
     */
    public void getData(final String path, Watcher watcher,
            DataCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {//如果有watcher，就注册
            wcb = new DataWatchRegistration(watcher, clientPath);//生成一个DataWatchRegistration，即Data的watch的注册
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();//生成请求头
        h.setType(ZooDefs.OpCode.getData);//设置请求类型为getData

        GetDataRequest request = new GetDataRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);//设置标志位,是否有函数watch

        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);//client端提交请求
    }

    /**
     * The asynchronous version of getData.
     *
     * @see #getData(String, boolean, Stat)
     */
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        getData(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    /**
     * Return the last committed configuration (as known to the server to which the client is connected)
     * and the stat of the configuration.
     * <p>
     * If the watch is non-null and the call is successful (no exception is
     * thrown), a watch will be left on the configuration node (ZooDefs.CONFIG_NODE). The watch
     * will be triggered by a successful reconfig operation
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if the configuration node doesn't exists.
     *
     * @param watcher explicit watcher
     * @param stat the stat of the configuration node ZooDefs.CONFIG_NODE
     * @return configuration data stored in ZooDefs.CONFIG_NODE
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getConfig(Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException
     {
        final String configZnode = ZooDefs.CONFIG_NODE;
 
        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                   configZnode);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getData();
    }

    /**
     * The asynchronous version of getConfig.
     *
     * @see #getConfig(Watcher, Stat)
     */
    public void getConfig(Watcher watcher,
            DataCallback cb, Object ctx)
    {
        final String configZnode = ZooDefs.CONFIG_NODE;
        
        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new DataWatchRegistration(watcher, configZnode);
        }

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getData);
        GetDataRequest request = new GetDataRequest();
        request.setPath(configZnode);
        request.setWatch(watcher != null);
        GetDataResponse response = new GetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
               configZnode, configZnode, ctx, wcb);
    }

    
    /**
     * Return the last committed configuration (as known to the server to which the client is connected)
     * and the stat of the configuration.
     * <p>
     * If the watch is true and the call is successful (no exception is
     * thrown), a watch will be left on the configuration node (ZooDefs.CONFIG_NODE). The watch
     * will be triggered by a successful reconfig operation
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param watch whether need to watch this node
     * @param stat the stat of the configuration node ZooDefs.CONFIG_NODE
     * @return configuration data stored in ZooDefs.CONFIG_NODE
     * @throws KeeperException If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getConfig(boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getConfig(watch ? watchManager.defaultWatcher : null, stat);
    }
 
    /**
     * The Asynchronous version of getConfig. 
     * 
     * @see #getData(String, boolean, Stat)
     */
    public void getConfig(boolean watch, DataCallback cb, Object ctx) {
        getConfig(watch ? watchManager.defaultWatcher : null, cb, ctx);
    }
   
    /**
     * Set the data for the node of the given path if such a node exists and the
     * given version matches the version of the node (if the given version is
     * -1, it matches any node's versions). Return the stat of the node.
     * <p>
     * This operation, if successful, will trigger all the watches on the node
     * of the given path left by getData calls.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given version does not match the node's version.
     * <p>
     * The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
     * Arrays larger than this will cause a KeeperException to be thrown.
     *
     * @param path
     *                the path of the node
     * @param data
     *                the data to set
     * @param version
     *                the expected matching version
     * @return the state of the node
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat setData(final String path, byte data[], int version)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getStat();
    }

    /**
     * The asynchronous version of setData.
     *
     * @see #setData(String, byte[], int)
     */
    public void setData(final String path, byte data[], int version,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setData);
        SetDataRequest request = new SetDataRequest();
        request.setPath(serverPath);
        request.setData(data);
        request.setVersion(version);
        SetDataResponse response = new SetDataResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    /**
     * Return the ACL and stat of the node of the given path.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     *                the given path for the node
     * @param stat
     *                the stat of the node will be copied to this parameter if
     *                not null.
     * @return the ACL array of the given node.
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<ACL> getACL(final String path, Stat stat)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getAcl();
    }

    /**
     * The asynchronous version of getACL.
     *
     * @see #getACL(String, Stat)
     */
    public void getACL(final String path, Stat stat, ACLCallback cb,
            Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getACL);
        GetACLRequest request = new GetACLRequest();
        request.setPath(serverPath);
        GetACLResponse response = new GetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    /**
     * Set the ACL for the node of the given path if such a node exists and the
     * given aclVersion matches the acl version of the node. Return the stat of the
     * node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     * <p>
     * A KeeperException with error code KeeperException.BadVersion will be
     * thrown if the given aclVersion does not match the node's aclVersion.
     *
     * @param path the given path for the node
     * @param acl the given acl for the node
     * @param aclVersion the given acl version of the node
     * @return the stat of the node.
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws org.apache.zookeeper.KeeperException.InvalidACLException If the acl is invalide.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat setACL(final String path, List<ACL> acl, int aclVersion)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);
        validateACL(acl);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        request.setAcl(acl);
        request.setVersion(aclVersion);
        SetACLResponse response = new SetACLResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getStat();
    }

    /**
     * The asynchronous version of setACL.
     *
     * @see #setACL(String, List, int)
     */
    public void setACL(final String path, List<ACL> acl, int version,
            StatCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.setACL);
        SetACLRequest request = new SetACLRequest();
        request.setPath(serverPath);
        request.setAcl(acl);
        request.setVersion(version);
        SetACLResponse response = new SetACLResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    /**
     * Return the list of the children of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     * @param watcher explicit watcher
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<String> getChildren(final String path, Watcher watcher)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getChildren();
    }

    /**
     * Return the list of the children of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @param path
     * @param watch
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     */
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null);
    }

    /**
     * The asynchronous version of getChildren.
     *
     * @see #getChildren(String, Watcher)
     */
    public void getChildren(final String path, Watcher watcher,
            ChildrenCallback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren);
        GetChildrenRequest request = new GetChildrenRequest();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildrenResponse response = new GetChildrenResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    /**
     * The asynchronous version of getChildren.
     *
     * @see #getChildren(String, boolean)
     */
    public void getChildren(String path, boolean watch, ChildrenCallback cb,
            Object ctx)
    {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    /**
     * For the given znode path return the stat and children list.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @since 3.3.0
     * 
     * @param path
     * @param watcher explicit watcher
     * @param stat stat of the znode designated by path
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public List<String> getChildren(final String path, Watcher watcher,
            Stat stat)
        throws KeeperException, InterruptedException
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        ReplyHeader r = cnxn.submitRequest(h, request, response, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        if (stat != null) {
            DataTree.copyStat(response.getStat(), stat);
        }
        return response.getChildren();
    }

    /**
     * For the given znode path return the stat and children list.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown),
     * a watch will be left on the node with the given path. The watch will be
     * triggered by a successful operation that deletes the node of the given
     * path or creates/delete a child under the node.
     * <p>
     * The list of children returned is not sorted and no guarantee is provided
     * as to its natural or lexical order.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown
     * if no node with the given path exists.
     *
     * @since 3.3.0
     * 
     * @param path
     * @param watch
     * @param stat stat of the znode designated by path
     * @return an unordered array of children of the node with the given path
     * @throws InterruptedException If the server transaction is interrupted.
     * @throws KeeperException If the server signals an error with a non-zero
     *  error code.
     */
    public List<String> getChildren(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        return getChildren(path, watch ? watchManager.defaultWatcher : null,
                stat);
    }

    /**
     * The asynchronous version of getChildren.
     *
     * @since 3.3.0
     * 
     * @see #getChildren(String, Watcher, Stat)
     */
    public void getChildren(final String path, Watcher watcher,
            Children2Callback cb, Object ctx)
    {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        WatchRegistration wcb = null;
        if (watcher != null) {
            wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getChildren2);
        GetChildren2Request request = new GetChildren2Request();
        request.setPath(serverPath);
        request.setWatch(watcher != null);
        GetChildren2Response response = new GetChildren2Response();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, wcb);
    }

    /**
     * The asynchronous version of getChildren.
     *
     * @since 3.3.0
     * 
     * @see #getChildren(String, boolean, Stat)
     */
    public void getChildren(String path, boolean watch, Children2Callback cb,
            Object ctx)
    {
        getChildren(path, watch ? watchManager.defaultWatcher : null, cb, ctx);
    }

    /**
     * Synchronously gets all numbers of children nodes under a specific path
     *
     * @since 3.6.0
     * @param path
     * @return Children nodes count under path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public int getAllChildrenNumber(final String path)
            throws KeeperException, InterruptedException {

        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getAllChildrenNumber);
        GetAllChildrenNumberRequest request = new GetAllChildrenNumberRequest(serverPath);
        GetAllChildrenNumberResponse response = new GetAllChildrenNumberResponse();

        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
        return response.getTotalNumber();
    }

    /**
     * Asynchronously gets all numbers of children nodes under a specific path
     *
     * @since 3.6.0
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getAllChildrenNumber(final String path, AsyncCallback.AllChildrenNumberCallback cb, Object ctx) {

        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getAllChildrenNumber);
        GetAllChildrenNumberRequest request = new GetAllChildrenNumberRequest(serverPath);
        GetAllChildrenNumberResponse response = new GetAllChildrenNumberResponse();

        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }


    /**
     * Synchronously gets all the ephemeral nodes  created by this session.
     *
     * @since 3.6.0
     *
     */
    public List<String> getEphemerals()
        throws KeeperException, InterruptedException {
        return getEphemerals("/");
    }

    /**
     * Synchronously gets all the ephemeral nodes matching prefixPath
     * created by this session.  If prefixPath is "/" then it returns all
     * ephemerals
     *
     * @since 3.6.0
     *
     */
    public List<String> getEphemerals(String prefixPath)
        throws KeeperException, InterruptedException {
        PathUtils.validatePath(prefixPath);
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getEphemerals);
        GetEphemeralsRequest request = new GetEphemeralsRequest(prefixPath);
        GetEphemeralsResponse response = new GetEphemeralsResponse();
        ReplyHeader r = cnxn.submitRequest(h, request, response, null);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()));
        }
        return response.getEphemerals();
    }

    /**
     * Asynchronously gets all the ephemeral nodes matching prefixPath
     * created by this session.  If prefixPath is "/" then it returns all
     * ephemerals
     *
     * @since 3.6.0
     *
     */
    public void getEphemerals(String prefixPath, AsyncCallback.EphemeralsCallback cb, Object ctx) {
        PathUtils.validatePath(prefixPath);
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.getEphemerals);
        GetEphemeralsRequest request = new GetEphemeralsRequest(prefixPath);
        GetEphemeralsResponse response = new GetEphemeralsResponse();
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
            null, null, ctx, null);
    }

    /**
     * Asynchronously gets all the ephemeral nodes created by this session.
     * ephemerals
     *
     * @since 3.6.0
     *
     */
    public void getEphemerals(AsyncCallback.EphemeralsCallback cb, Object ctx) {
        getEphemerals("/", cb, ctx);
    }

    /**
     * Asynchronous sync. Flushes channel between process and leader.
     * @param path
     * @param cb a handler for the callback
     * @param ctx context to be provided to the callback
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public void sync(final String path, VoidCallback cb, Object ctx){
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath = prependChroot(clientPath);

        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.sync);
        SyncRequest request = new SyncRequest();
        SyncResponse response = new SyncResponse();
        request.setPath(serverPath);
        cnxn.queuePacket(h, new ReplyHeader(), request, response, cb,
                clientPath, serverPath, ctx, null);
    }

    /**
     * For the given znode path, removes the specified watcher of given
     * watcherType.
     *
     * <p>
     * Watcher shouldn't be null. A successful call guarantees that, the
     * removed watcher won't be triggered.
     * </p>
     *
     * @param path
     *            - the path of the node
     * @param watcher
     *            - a concrete watcher
     * @param watcherType
     *            - the type of watcher to be removed
     * @param local
     *            - whether the watcher can be removed locally when there is no
     *            server connection
     * @throws InterruptedException
     *             if the server transaction is interrupted.
     * @throws KeeperException.NoWatcherException
     *             if no watcher exists that match the specified parameters
     * @throws KeeperException
     *             if the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException
     *             if any of the following is true:
     *             <ul>
     *             <li> {@code path} is invalid
     *             <li> {@code watcher} is null
     *             </ul>
     *
     * @since 3.5.0
     */
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local)
            throws InterruptedException, KeeperException {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local);
    }

    /**
     * The asynchronous version of removeWatches.
     *
     * @see #removeWatches
     */
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        validateWatcher(watcher);
        removeWatches(ZooDefs.OpCode.checkWatches, path, watcher,
                watcherType, local, cb, ctx);
    }

    /**
     * For the given znode path, removes all the registered watchers of given
     * watcherType.
     *
     * <p>
     * A successful call guarantees that, the removed watchers won't be
     * triggered.
     * </p>
     *
     * @param path
     *            - the path of the node
     * @param watcherType
     *            - the type of watcher to be removed
     * @param local
     *            - whether watches can be removed locally when there is no
     *            server connection
     * @throws InterruptedException
     *             if the server transaction is interrupted.
     * @throws KeeperException.NoWatcherException
     *             if no watcher exists that match the specified parameters
     * @throws KeeperException
     *             if the server signals an error with a non-zero error code.
     * @throws IllegalArgumentException
     *             if an invalid {@code path} is specified
     *
     * @since 3.5.0
     */
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local) throws InterruptedException, KeeperException {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null, watcherType,
                local);
    }

    /**
     * The asynchronous version of removeAllWatches.
     *
     * @see #removeAllWatches
     */
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local, VoidCallback cb, Object ctx) {

        removeWatches(ZooDefs.OpCode.removeWatches, path, null,
                watcherType, local, cb, ctx);
    }

    private void validateWatcher(Watcher watcher) {
        if (watcher == null) {
            throw new IllegalArgumentException(
                    "Invalid Watcher, shouldn't be null!");
        }
    }

    private void removeWatches(int opCode, String path, Watcher watcher,
            WatcherType watcherType, boolean local)
            throws InterruptedException, KeeperException {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        ReplyHeader r = cnxn.submitRequest(h, request, null, null, wcb);
        if (r.getErr() != 0) {
            throw KeeperException.create(KeeperException.Code.get(r.getErr()),
                    clientPath);
        }
    }

    private void removeWatches(int opCode, String path, Watcher watcher,
            WatcherType watcherType, boolean local, VoidCallback cb, Object ctx) {
        PathUtils.validatePath(path);
        final String clientPath = path;
        final String serverPath = prependChroot(clientPath);
        WatchDeregistration wcb = new WatchDeregistration(clientPath, watcher,
                watcherType, local, watchManager);

        RequestHeader h = new RequestHeader();
        h.setType(opCode);
        Record request = getRemoveWatchesRequest(opCode, watcherType,
                serverPath);

        cnxn.queuePacket(h, new ReplyHeader(), request, null, cb, clientPath,
                serverPath, ctx, null, wcb);
    }

    private Record getRemoveWatchesRequest(int opCode, WatcherType watcherType,
            final String serverPath) {
        Record request = null;
        switch (opCode) {
        case ZooDefs.OpCode.checkWatches:
            CheckWatchesRequest chkReq = new CheckWatchesRequest();
            chkReq.setPath(serverPath);
            chkReq.setType(watcherType.getIntValue());
            request = chkReq;
            break;
        case ZooDefs.OpCode.removeWatches:
            RemoveWatchesRequest rmReq = new RemoveWatchesRequest();
            rmReq.setPath(serverPath);
            rmReq.setType(watcherType.getIntValue());
            request = rmReq;
            break;
        default:
            LOG.warn("unknown type " + opCode);
            break;
        }
        return request;
    }

    public States getState() {
        return cnxn.getState();
    }

    /**
     * String representation of this ZooKeeper client. Suitable for things
     * like logging.
     * 
     * Do NOT count on the format of this string, it may change without
     * warning.
     * 
     * @since 3.3.0
     */
    @Override
    public String toString() {
        States state = getState();
        return ("State:" + state.toString()
                + (state.isConnected() ?
                        " Timeout:" + getSessionTimeout() + " " :
                        " ")
                + cnxn);
    }

    /*
     * Methods to aid in testing follow.
     * 
     * THESE METHODS ARE EXPECTED TO BE USED FOR TESTING ONLY!!!
     */

    /**
     * Wait up to wait milliseconds for the underlying threads to shutdown.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     * 
     * @since 3.3.0
     * 
     * @param wait max wait in milliseconds
     * @return true iff all threads are shutdown, otw false
     */
    protected boolean testableWaitForShutdown(int wait)
        throws InterruptedException
    {
        cnxn.sendThread.join(wait);
        if (cnxn.sendThread.isAlive()) return false;
        cnxn.eventThread.join(wait);
        if (cnxn.eventThread.isAlive()) return false;
        return true;
    }

    /**
     * Returns the address to which the socket is connected. Useful for testing
     * against an ensemble - test client may need to know which server
     * to shutdown if interested in verifying that the code handles
     * disconnection/reconnection correctly.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     *
     * @since 3.3.0
     * 
     * @return ip address of the remote side of the connection or null if
     *         not connected
     */
    protected SocketAddress testableRemoteSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getRemoteSocketAddress();
    }

    /** 
     * Returns the local address to which the socket is bound.
     * THIS METHOD IS EXPECTED TO BE USED FOR TESTING ONLY!!!
     * 返回套接字绑定的本地地址。
     * *此方法预计仅用于测试!!!
     *
     * @since 3.3.0
     * 
     * @return ip address of the remote side of the connection or null if
     *         not connected
     */
    protected SocketAddress testableLocalSocketAddress() {
        return cnxn.sendThread.getClientCnxnSocket().getLocalSocketAddress();
    }

    // 该函数会利用反射创建ClientCnxnSocketNIO实例
    private ClientCnxnSocket getClientCnxnSocket() throws IOException {
        // 查看是否在系统属性中进行了设置   zookeeper.clientCnxnSocket
        String clientCnxnSocketName = getClientConfig().getProperty(
                ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        if (clientCnxnSocketName == null) {
            clientCnxnSocketName = ClientCnxnSocketNIO.class.getName(); // 若未进行设置，取得ClientCnxnSocketNIO的类名
        }
        try {
            // 反射拿到构造器，构造器要传入ZKClientConfig对象
            Constructor<?> clientCxnConstructor = Class.forName(clientCnxnSocketName).getDeclaredConstructor(ZKClientConfig.class);
            ClientCnxnSocket clientCxnSocket = (ClientCnxnSocket) clientCxnConstructor.newInstance(getClientConfig());
            return clientCxnSocket;
        } catch (Exception e) {
            IOException ioe = new IOException("Couldn't instantiate "
                    + clientCnxnSocketName);
            ioe.initCause(e);
            throw ioe;
        }
    }

    /**
     * Validates the provided ACL list for null, empty or null value in it.
     * 
     * @param acl
     *            ACL list
     * @throws InvalidACLException
     *             if ACL list is not valid
     */
    private void validateACL(List<ACL> acl) throws KeeperException.InvalidACLException {
        if (acl == null || acl.isEmpty() || acl.contains(null)) {
            throw new KeeperException.InvalidACLException();
        }
    }
}
