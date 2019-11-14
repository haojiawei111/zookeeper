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

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ReconfigRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.ServerAuthenticationProvider;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 * 该请求处理器通常处于RequestProcessor更改的开始。
 * 它设置与更改系统状态的请求关联的任何事务。
 * 它依靠ZooKeeperServer来更新outstandingRequests，因此它可以考虑生成事务时要应用的队列中的事务。
 *
 * 请求预处理器。在Zookeeper中，那些会改变服务器状态的请求称为事务请求（创建节点、更新数据、删除节点、创建会话等），
 * PrepRequestProcessor能够识别出当前客户端请求是否是事务请求。对于事务请求，PrepRequestProcessor处理器会对其进行一系列预处理，
 * 如创建请求事务头、事务体、会话检查、ACL检查和版本检查等。
 *
 */
    // PrepRequestProcessor继承了Thread类并实现了RequestProcessor接口，表示其可以作为线程使用。
    //类的核心属性有submittedRequests和nextProcessor，前者表示已经提交的请求，而后者表示提交的下个处理器
public class PrepRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);
    // 是否跳过ACL,需查看系统配置
    static boolean skipACL;

    static {
        // 是否跳过ACL,需查看系统配置
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be used otherwise
     */
    // 仅用作测试使用
    private static  boolean failCreate = false;

    // TODO: 请求队列
    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();

    // 下个处理器
    private final RequestProcessor nextProcessor;

    // Zookeeper服务器
    ZooKeeperServer zks;

    //构造函数首先会调用父类Thread的构造函数，然后利用构造函数参数给nextProcessor和zks赋值
    public PrepRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        // 调用父类Thread构造函数
        super("ProcessThread(sid:" + zks.getServerId() + " cport:" + zks.getClientPort() + "):", zks.getZooKeeperServerListener());
        // 类属性赋值
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }

    // run函数是对Thread类run函数的重写，其核心逻辑相对简单
    // TODO:不断从队列中取出request进行处理，其会调用pRequest函数 run:线程方法，消费请求队列，调用pRequest
    @Override
    public void run() {
        try {
            while (true) {
                // 更新Metrics监控
                ServerMetrics.getMetrics().PREP_PROCESSOR_QUEUE_SIZE.add(submittedRequests.size());
                // TODO: 从队列中取出一个请求
                Request request = submittedRequests.take();
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {// 请求类型为PING 心跳请求
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) { // 是否可追踪
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                if (Request.requestOfDeath == request) {// 在关闭处理器之后，会添加requestOfDeath，表示关闭后不再处理请求
                    // 关闭处理器
                    break;
                }
                long prepStartTime = Time.currentElapsedTime();
                // 调用pRequest函数 处理请求
                pRequest(request);
                ServerMetrics.getMetrics().PREP_PROCESS_TIME.add(Time.currentElapsedTime() - prepStartTime);
            }
        } catch (RequestProcessorException e) {// 请求处理异常
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    private ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path);
            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Set<String> children;
                    synchronized(n) {
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat, children.size(),
                            zks.getZKDatabase().aclForNode(n));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    private ChangeRecord getOutstandingChange(String path) {
        synchronized (zks.outstandingChanges) {
            return zks.outstandingChangesForPath.get(path);
        }
    }

    protected void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
            ServerMetrics.getMetrics().OUTSTANDING_CHANGES_QUEUED.add(1);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     * 获取多操作中每个操作的当前挂起的更改记录。
     *
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     * 这在MultiOp错误代码路径中用于在失败的多操作的事件中回滚。
     *
     * @param multiRequest
     * @return a map that contains previously existed records that probably need to be
     *         rolled back in any failure.
     */
    // TODO: 会遍历多重操作
    // 针对每个操作，通过其路径获取对应的Record，然后添加至pendingChangeRecords，然后对其父节点进行相应操作，之后返回，其中会调用getOutstandingChange函数
    private Map<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        Map<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();

        for (Op op : multiRequest) {
            String path = op.getPath();
            // 获取path对应的ChangeRecord
            ChangeRecord cr = getOutstandingChange(path);
            // only previously existing records need to be rolled back.
            // 只需要回滚以前存在的记录。
            if (cr != null) {
                pendingChangeRecords.put(path, cr);
            }

            /*
             * ZOOKEEPER-1624 - We need to store for parent's ChangeRecord
             * of the parent node of a request. So that if this is a
             * sequential node creation request, rollbackPendingChanges()
             * can restore previous parent's ChangeRecord correctly.
             *
             * Otherwise, sequential node name generation will be incorrect
             * for a subsequent request.
             */
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1 || path.indexOf('\0') != -1) {
                continue;
            }
            // 提取节点的父节点路径
            String parentPath = path.substring(0, lastSlash);
            // 获取父节点的Record
            ChangeRecord parentCr = getOutstandingChange(parentPath);
            if (parentCr != null) {
                pendingChangeRecords.put(parentPath, parentCr);
            }
        }

        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, Map<String, ChangeRecord>pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            Iterator<ChangeRecord> iter = zks.outstandingChanges.descendingIterator();
            while (iter.hasNext()) {
                ChangeRecord c = iter.next();
                if (c.zxid == zxid) {
                    iter.remove();
                    // Remove all outstanding changes for paths of this multi.
                    // Previous records will be added back later.
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

            // we don't need to roll back any records because there is nothing left.
            if (zks.outstandingChanges.isEmpty()) {
                return;
            }

            long firstZxid = zks.outstandingChanges.peek().zxid;

            for (ChangeRecord c : pendingChangeRecords.values()) {
                // Don't apply any prior change records less than firstZxid.
                // Note that previous outstanding requests might have been removed
                // once they are completed.
                if (c.zxid < firstZxid) {
                    continue;
                }

                // add previously existing records back.
                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    /**
     * Grant or deny authorization to an operation on a node as a function of:
     * @param zks :     the ZooKeeper server
     * @param cnxn :    the server connection
     * @param acl :     set of ACLs for the node
     * @param perm :    the permission that the client is requesting
     * @param ids :     the credentials supplied by the client
     * @param path :    the ZNode path
     * @param setAcls : for set ACL operations, the list of ACLs being set. Otherwise null.
     */
    static void checkACL(ZooKeeperServer zks, ServerCnxn cnxn, List<ACL> acl, int perm, List<Id> ids,
                         String path, List<ACL> setAcls) throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Permission requested: {} ", perm);
            LOG.debug("ACLs for node: {}", acl);
            LOG.debug("Client credentials: {}", ids);
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                ServerAuthenticationProvider ap = ProviderRegistry.getServerProvider(id
                        .getScheme());
                if (ap != null) {
                    for (Id authId : ids) {
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(new ServerAuthenticationProvider.ServerObjs(zks, cnxn),
                                new ServerAuthenticationProvider.MatchValues(path, authId.getId(), id.getId(), perm, setAcls))) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    /**
     * Performs basic validation of a path for a create request.
     * Throws if the path is not valid and returns the parent path.
     * @throws BadArgumentsException
     */
    private String validatePathForCreate(String path, long sessionId)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
            LOG.info("Invalid path %s with session 0x%s",
                    path, Long.toHexString(sessionId));
            throw new KeeperException.BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    /**
     * TODO:pRequest2Txn函数调用addChangeRecord会添加记录到zks.outstandingChanges，何时删除
     * PrepRequestProcessor#rollbackPendingChanges 或者
     * FinalRequestProcessor#processRequest
     *
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type
     * @param zxid
     * @param request
     * @param record
     */
    // pRequest2Txn会根据不同的请求类型进行不同的验证，如对创建节点而言，其会进行会话验证，ACL列表验证，节点路径验证及判断创建节点的类型（顺序节点、临时节点等）而进行不同操作，
    // 同时还会使父节点的子节点数目加1
    // 之后会再调用addChangeRecord函数将ChangeRecord添加至ZooKeeperServer的outstandingChanges和outstandingChangesForPath中。
    protected void pRequest2Txn(int type, long zxid, Request request, Record record, boolean deserialize)
        throws KeeperException, IOException, RequestProcessorException {
        //生成事务头，完成acl，path验证，进行count，version对应修改，将改动记录在zks.outstandingChanges 等
        request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid, Time.currentWallTime(), type));


        switch (type) { // 确定类型
            case OpCode.create:// 创建节点操作
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer: {
                // 检查
                pRequest2TxnCreate(type, request, record, deserialize);
                break;
            }

            case OpCode.deleteContainer: {
                String path = new String(request.request.array());
                String parentPath = getParentPathAndValidate(path);
                ChangeRecord nodeRecord = getRecordForPath(path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                if (EphemeralType.get(nodeRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL) {
                    throw new KeeperException.BadVersionException(path);
                }
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                request.setTxn(new DeleteTxn(path));
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            }

            case OpCode.delete:
                // 检查会话，检查会话持有者是否为该owner
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 向下转化为DeleteRequest
                DeleteRequest deleteRequest = (DeleteRequest)record;
                if(deserialize)// 反序列化，将ByteBuffer转化为Record
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                // 获取节点路径
                String path = deleteRequest.getPath();
                // 提取节点的父节点路径
                String parentPath = getParentPathAndValidate(path);
                // 获取父节点的Record
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                // 检查ACL列表
                checkACL(zks, request.cnxn, parentRecord.acl, ZooDefs.Perms.DELETE, request.authInfo, path, null);
                // 获取节点的Record
                ChangeRecord nodeRecord = getRecordForPath(path);
                // 检查版本
                checkAndIncVersion(nodeRecord.stat.getVersion(), deleteRequest.getVersion(), path);
                if (nodeRecord.childCount > 0) { // 该结点有子节点，抛出异常
                    throw new KeeperException.NotEmptyException(path);
                }
                // 新生删除事务
                request.setTxn(new DeleteTxn(path));
                // 拷贝父节点Record
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                // 父节点的孩子节点数目减1
                parentRecord.childCount--;
                // 将parentRecord添加至outstandingChanges和outstandingChangesForPath中
                addChangeRecord(parentRecord);
                // 将新生成的ChangeRecord(包含了StatPersisted信息)添加至outstandingChanges和outstandingChangesForPath中
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;

            case OpCode.setData: // 设置数据请求
                // 检查会话，检查会话持有者是否为该owner
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 向下转换
                SetDataRequest setDataRequest = (SetDataRequest)record;
                if(deserialize)// 反序列化，将ByteBuffer转化为Record
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                // 获取节点路径
                path = setDataRequest.getPath();
                validatePath(path, request.sessionId);
                // 获取节点的Record
                nodeRecord = getRecordForPath(path);
                // 检查ACL列表
                checkACL(zks, request.cnxn, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo, path, null);
                // 检查版本号
                int newVersion = checkAndIncVersion(nodeRecord.stat.getVersion(), setDataRequest.getVersion(), path);
                // 新生设置数据事务
                request.setTxn(new SetDataTxn(path, setDataRequest.getData(), newVersion));
                // 拷贝
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                // 设置版本号
                nodeRecord.stat.setVersion(newVersion);
                // 将nodeRecord添加至outstandingChanges和outstandingChangesForPath中
                addChangeRecord(nodeRecord);
                break;

            case OpCode.reconfig:
                if (!QuorumPeerConfig.isReconfigEnabled()) {
                    LOG.error("Reconfig operation requested but reconfig feature is disabled.");
                    throw new KeeperException.ReconfigDisabledException();
                }

                if (skipACL) {
                    LOG.warn("skipACL is set, reconfig operation will skip ACL checks!");
                }

                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                LeaderZooKeeperServer lzks;
                try {
                    lzks = (LeaderZooKeeperServer)zks;
                } catch (ClassCastException e) {
                    // standalone mode - reconfiguration currently not supported
                    throw new KeeperException.UnimplementedException();
                }
                QuorumVerifier lastSeenQV = lzks.self.getLastSeenQuorumVerifier();
                // check that there's no reconfig in progress
                if (lastSeenQV.getVersion()!=lzks.self.getQuorumVerifier().getVersion()) {
                       throw new KeeperException.ReconfigInProgress();
                }
                ReconfigRequest reconfigRequest = (ReconfigRequest)record;
                long configId = reconfigRequest.getCurConfigId();

                if (configId != -1 && configId!=lzks.self.getLastSeenQuorumVerifier().getVersion()){
                   String msg = "Reconfiguration from version " + configId + " failed -- last seen version is " +
                           lzks.self.getLastSeenQuorumVerifier().getVersion();
                   throw new KeeperException.BadVersionException(msg);
                }

                String newMembers = reconfigRequest.getNewMembers();

                if (newMembers != null) { //non-incremental membership change
                   LOG.info("Non-incremental reconfig");

                   // Input may be delimited by either commas or newlines so convert to common newline separated format
                   newMembers = newMembers.replaceAll(",", "\n");

                   try{
                       Properties props = new Properties();
                       props.load(new StringReader(newMembers));
                       request.qv = QuorumPeerConfig.parseDynamicConfig(props, lzks.self.getElectionType(), true, false);
                       request.qv.setVersion(request.getHdr().getZxid());
                   } catch (IOException | ConfigException e) {
                       throw new KeeperException.BadArgumentsException(e.getMessage());
                   }
                } else { //incremental change - must be a majority quorum system
                   LOG.info("Incremental reconfig");

                   List<String> joiningServers = null;
                   String joiningServersString = reconfigRequest.getJoiningServers();
                   if (joiningServersString != null)
                   {
                       joiningServers = StringUtils.split(joiningServersString,",");
                   }

                   List<String> leavingServers = null;
                   String leavingServersString = reconfigRequest.getLeavingServers();
                   if (leavingServersString != null)
                   {
                       leavingServers = StringUtils.split(leavingServersString, ",");
                   }

                   if (!(lastSeenQV instanceof QuorumMaj)) {
                           String msg = "Incremental reconfiguration requested but last configuration seen has a non-majority quorum system";
                           LOG.warn(msg);
                           throw new KeeperException.BadArgumentsException(msg);
                   }
                   Map<Long, QuorumServer> nextServers = new HashMap<Long, QuorumServer>(lastSeenQV.getAllMembers());
                   try {
                       if (leavingServers != null) {
                           for (String leaving: leavingServers){
                               long sid = Long.parseLong(leaving);
                               nextServers.remove(sid);
                           }
                       }
                       if (joiningServers != null) {
                           for (String joiner: joiningServers){
                        	   // joiner should have the following format: server.x = server_spec;client_spec
                        	   String[] parts = StringUtils.split(joiner, "=").toArray(new String[0]);
                               if (parts.length != 2) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string");
                               }
                               // extract server id x from first part of joiner: server.x
                               Long sid = Long.parseLong(parts[0].substring(parts[0].lastIndexOf('.') + 1));
                               QuorumServer qs = new QuorumServer(sid, parts[1]);
                               if (qs.clientAddr == null || qs.electionAddr == null || qs.addr == null) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string - each server should have 3 ports specified");
                               }

                               // check duplication of addresses and ports
                               for (QuorumServer nqs: nextServers.values()) {
                                   if (qs.id == nqs.id) {
                                       continue;
                                   }
                                   qs.checkAddressDuplicate(nqs);
                               }

                               nextServers.remove(qs.id);
                               nextServers.put(qs.id, qs);
                           }
                       }
                   } catch (ConfigException e){
                       throw new KeeperException.BadArgumentsException("Reconfiguration failed");
                   }
                   request.qv = new QuorumMaj(nextServers);
                   request.qv.setVersion(request.getHdr().getZxid());
                }
                if (QuorumPeerConfig.isStandaloneEnabled() && request.qv.getVotingMembers().size() < 2) {
                   String msg = "Reconfig failed - new configuration must include at least 2 followers";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                } else if (request.qv.getVotingMembers().size() < 1) {
                   String msg = "Reconfig failed - new configuration must include at least 1 follower";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                }

                if (!lzks.getLeader().isQuorumSynced(request.qv)) {
                   String msg2 = "Reconfig failed - there must be a connected and synced quorum in new configuration";
                   LOG.warn(msg2);
                   throw new KeeperException.NewConfigNoQuorum();
                }

                nodeRecord = getRecordForPath(ZooDefs.CONFIG_NODE);
                checkACL(zks, request.cnxn, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo, null, null);
                request.setTxn(new SetDataTxn(ZooDefs.CONFIG_NODE, request.qv.toString().getBytes(), -1));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(-1);
                addChangeRecord(nodeRecord);
                break;

            case OpCode.setACL:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                path = setAclRequest.getPath();
                validatePath(path, request.sessionId);
                List<ACL> listACL = fixupACL(path, request.authInfo, setAclRequest.getAcl());
                nodeRecord = getRecordForPath(path);
                checkACL(zks, request.cnxn, nodeRecord.acl, ZooDefs.Perms.ADMIN, request.authInfo, path, listACL);
                newVersion = checkAndIncVersion(nodeRecord.stat.getAversion(), setAclRequest.getVersion(), path);
                request.setTxn(new SetACLTxn(path, listACL, newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setAversion(newVersion);
                addChangeRecord(nodeRecord);
                break;

            case OpCode.createSession:// 创建会话请求
                // 将request缓冲区rewind
                request.request.rewind();
                // 获取缓冲区大小
                int to = request.request.getInt();
                // 创建会话事务
                request.setTxn(new CreateSessionTxn(to));
                // 再次将request缓冲区rewind
                request.request.rewind();
                // only add the global session tracker but not to ZKDb
                // 添加session
                zks.sessionTracker.trackSession(request.sessionId, to);
                // 设置会话的owner
                zks.setOwner(request.sessionId, request.getOwner());
                break;

            case OpCode.closeSession:// 关闭会话请求
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                long startTime =  Time.currentElapsedTime();
                // 获取sessionId对应的临时节点的路径列表
                Set<String> es = zks.getZKDatabase()
                        .getEphemerals(request.sessionId);
                // outstandingChanges理解成zk serve的事务变更队列,事务还没有完成，尚未同步到内存数据库中的一个队列，表示变更记录
                synchronized (zks.outstandingChanges) {
                    //遍历 zk serve的事务变更队列,这些事务处理尚未完成，没有应用到内存数据库中
                    for (ChangeRecord c : zks.outstandingChanges) {
                        //如果当前变更记录没有状态信息(删除时才会出现，参照上面处理delete时的ChangeRecord构造参数)
                        if (c.stat == null) { // 若临时节点属于该会话
                            // Doing a delete
                            // 则从es中移除其路径
                            es.remove(c.path);
                            // 若临时节点属于该会话
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            //添加记录，最终要将添加或者修改的record再删除掉
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {//添加节点变更事务,将es中所有路径的临时节点都删掉
                        // 完成该会话相关的临时节点收集后，Zookeeper会逐个将这些临时节点转换成"节点删除"请求，并放入事务变更队列outstandingChanges中。
                        // FinalRequestProcessor会触发内存数据库，删除该会话对应的所有临时节点
                        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path2Delete, null, 0, null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }
                ServerMetrics.getMetrics().CLOSE_SESSION_PREP_TIME.add(Time.currentElapsedTime() - startTime);
                break;

            case OpCode.check:// 检查请求
                // 检查会话，检查会话持有者是否为该owner
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                // 向下转化
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                if(deserialize)// 反序列化，将ByteBuffer转化为Record
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                // 获取节点路径
                path = checkVersionRequest.getPath();
                validatePath(path, request.sessionId);
                // 获取节点的Record
                nodeRecord = getRecordForPath(path);
                // 检查ACL列表
                checkACL(zks, request.cnxn, nodeRecord.acl, ZooDefs.Perms.READ, request.authInfo, path, null);
                // 新生请求的事务
                request.setTxn(new CheckVersionTxn(path, checkAndIncVersion(nodeRecord.stat.getVersion(),
                        checkVersionRequest.getVersion(), path)));
                break;
            default:
                LOG.warn("unknown type " + type);
                break;
        }
    }

    private void pRequest2TxnCreate(int type, Request request, Record record, boolean deserialize) throws IOException, KeeperException {
        if (deserialize) {
            ByteBufferInputStream.byteBuffer2Record(request.request, record);
        }

        int flags;
        String path;
        List<ACL> acl;
        byte[] data;
        long ttl;
        if (type == OpCode.createTTL) {
            CreateTTLRequest createTtlRequest = (CreateTTLRequest)record;
            flags = createTtlRequest.getFlags();
            path = createTtlRequest.getPath();
            acl = createTtlRequest.getAcl();
            data = createTtlRequest.getData();
            ttl = createTtlRequest.getTtl();
        } else {
            CreateRequest createRequest = (CreateRequest)record;
            flags = createRequest.getFlags();
            path = createRequest.getPath();
            acl = createRequest.getAcl();
            data = createRequest.getData();
            ttl = -1;
        }
        //根据标志位判断是哪一种createMode
        CreateMode createMode = CreateMode.fromFlag(flags);
        validateCreateRequest(path, createMode, request, ttl);
        String parentPath = validatePathForCreate(path, request.sessionId);

        List<ACL> listACL = fixupACL(path, request.authInfo, acl);
        ChangeRecord parentRecord = getRecordForPath(parentPath);

        // 检查ACL列表
        checkACL(zks, request.cnxn, parentRecord.acl, ZooDefs.Perms.CREATE, request.authInfo, path, listACL);

        // 获取父节点的Record的子节点版本号
        int parentCVersion = parentRecord.stat.getCversion();
        if (createMode.isSequential()) {
            path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
        }
        validatePath(path, request.sessionId);
        try {
            if (getRecordForPath(path) != null) {
                throw new KeeperException.NodeExistsException(path);
            }
        } catch (KeeperException.NoNodeException e) {
            // ignore this one
        }
        boolean ephemeralParent = EphemeralType.get(parentRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL;
        if (ephemeralParent) {
            throw new KeeperException.NoChildrenForEphemeralsException(path);
        }
        int newCversion = parentRecord.stat.getCversion()+1;
        if (type == OpCode.createContainer) {
            request.setTxn(new CreateContainerTxn(path, data, listACL, newCversion));
        } else if (type == OpCode.createTTL) {
            request.setTxn(new CreateTTLTxn(path, data, listACL, newCversion, ttl));
        } else {
            request.setTxn(new CreateTxn(path, data, listACL, createMode.isEphemeral(),
                    newCversion));
        }
        StatPersisted s = new StatPersisted();
        if (createMode.isEphemeral()) {
            s.setEphemeralOwner(request.sessionId);
        }
        parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
        parentRecord.childCount++;
        parentRecord.stat.setCversion(newCversion);
        addChangeRecord(parentRecord);
        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, s, 0, listACL));
    }

    private void validatePath(String path, long sessionId) throws BadArgumentsException {
        try {
            PathUtils.validatePath(path);
        } catch(IllegalArgumentException ie) {
            LOG.info("Invalid path {} with session 0x{}, reason: {}",
                    path, Long.toHexString(sessionId), ie.getMessage());
            throw new BadArgumentsException(path);
        }
    }

    private String getParentPathAndValidate(String path)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1
                || zks.getZKDatabase().isSpecialPath(path)) {
            throw new BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    private static int checkAndIncVersion(int currentVersion, int expectedVersion, String path)
            throws KeeperException.BadVersionException {
        if (expectedVersion != -1 && expectedVersion != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }
        return currentVersion + 1;
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    // pRequest:处理请求，根据request.type区分是否是事务请求,事务的话调用pRequest2Txn.最后让下一个处理器接着处理
    protected void pRequest(Request request) throws RequestProcessorException {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        // TODO: 将请求的hdr和txn设置为null
        request.setHdr(null);
        request.setTxn(null);

        try {
            //根据request.type区分是否是事务请求
            switch (request.type)  {// 确定请求类型
            case OpCode.createContainer:// 创建节点请求
            case OpCode.create:
            case OpCode.create2:
                // 创建节点请求
                CreateRequest create2Request = new CreateRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, create2Request, true);
                break;
            case OpCode.createTTL:
                CreateTTLRequest createTtlRequest = new CreateTTLRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, createTtlRequest, true);
                break;
            case OpCode.deleteContainer:// 删除节点请求
            case OpCode.delete:
                // 新生删除节点请求
                DeleteRequest deleteRequest = new DeleteRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                break;
            case OpCode.setData:// 设置数据请求
                // 新生设置数据请求
                SetDataRequest setDataRequest = new SetDataRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                break;
            case OpCode.reconfig:
                ReconfigRequest reconfigRequest = new ReconfigRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, reconfigRequest);
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, reconfigRequest, true);
                break;
            case OpCode.setACL:
                SetACLRequest setAclRequest = new SetACLRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                break;
            case OpCode.check:
                CheckVersionRequest checkRequest = new CheckVersionRequest();
                // 如果是事务请求，调用pRequest2Txn
                pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                break;
            case OpCode.multi:// 批量请求
                // 批量请求
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                try {
                    // 将ByteBuffer转化为Record
                    ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                } catch(IOException e) {
                    // 出现异常则重新生成Txn头
                    request.setHdr(new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                            Time.currentWallTime(), OpCode.multi));
                    throw e;
                }
                // TODO：对请求预处理后放入这个list中
                List<Txn> txns = new ArrayList<Txn>();
                // Each op in a multi-op must have the same zxid!
                // TODO: 多操作中的每个操作必须具有相同的zxid！
                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                //Store off current pending change records in case we need to rollback
                // 存储当前挂起的更改记录，以防我们需要回滚
                Map<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                for(Op op: multiRequest) {// 遍历请求

                    Record subrequest = op.toRequestRecord();
                    int type;
                    Record txn;

                    /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                    if (ke != null) { // 发生了异常
                        type = OpCode.error;
                        txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    }

                    /* Prep the request and convert to a Txn */
                    else {// 未发生异常
                        try {
                            // 将Request转化为Txn
                            pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                            type = request.getHdr().getType();
                            txn = request.getTxn();

                        } catch (KeeperException e) {// 转化发生异常
                            ke = e;
                            type = OpCode.error;
                            txn = new ErrorTxn(e.code().intValue());

                            if (e.code().intValue() > Code.APIERROR.intValue()) {
                                LOG.info("Got user-level KeeperException when processing {} aborting" +
                                        " remaining multi ops. Error Path:{} Error:{}",
                                        request.toString(), e.getPath(), e.getMessage());
                            }
                            // 设置异常
                            request.setException(e);

                            /* Rollback change records from failed multi-op */
                            // TODO: 从多重操作中回滚更改记录
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                    //FIXME: I don't want to have to serialize it here and then
                    //       immediately deserialize in next processor. But I'm
                    //       not sure how else to get the txn stored into our list.
                    // 序列化
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                        txn.serialize(boa, "request");
                        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
                        txns.add(new Txn(type, bb.array()));
                    }
                }

                // TODO: 给请求头赋值
                request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                        Time.currentWallTime(), request.type));
                // TODO: 设置请求的Txn
                request.setTxn(new MultiTxn(txns));

                break;

            //create/close session don't require request record
                // create/close session不要求记录
            case OpCode.createSession:// 创建会话请求
            case OpCode.closeSession: // 关闭会话请求
                if (!request.isLocalSession()) {
                    // 如果是事务请求，调用pRequest2Txn
                    pRequest2Txn(request.type, zks.getNextZxid(), request,
                                 null, true);
                }
                break;

            //All the rest don't need to create a Txn - just verify session
                // 所有以下请求只需验证会话即可
            case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getAllChildrenNumber:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
            case OpCode.checkWatches:
            case OpCode.removeWatches:
            case OpCode.getEphemerals:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                break;
            default:
                LOG.warn("unknown type " + request.type);
                break;
            }
        } catch (KeeperException e) { // 发生KeeperException异常
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(e.code().intValue()));
            }

            if (e.code().intValue() > Code.APIERROR.intValue()) {
                LOG.info("Got user-level KeeperException when processing {} Error Path:{} Error:{}",
                        request.toString(), e.getPath(), e.getMessage());
            }
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(Code.MARSHALLINGERROR.intValue()));
            }
        }
        // 给请求的zxid赋值
        request.zxid = zks.getZxid();
        ServerMetrics.getMetrics().PREP_PROCESSOR_QUEUE_TIME.add(Time.currentElapsedTime() - request.prepQueueStartTime);
        // 给请求的zxid赋值
        nextProcessor.processRequest(request);
    }

    private List<ACL> removeDuplicates(final List<ACL> acls) {
        if (acls == null || acls.isEmpty()) {
          return Collections.emptyList();
        }

        // This would be done better with a Set but ACL hashcode/equals do not
        // allow for null values
        final ArrayList<ACL> retval = new ArrayList<>(acls.size());
        for (final ACL acl : acls) {
            if (!retval.contains(acl)) {
              retval.add(acl);
            }
        }
        return retval;
    }

    private void validateCreateRequest(String path, CreateMode createMode, Request request, long ttl)
            throws KeeperException {
        if (createMode.isTTL() && !EphemeralType.extendedEphemeralTypesEnabled()) {
            throw new KeeperException.UnimplementedException();
        }
        try {
            EphemeralType.validateTTL(createMode, ttl);
        } catch (IllegalArgumentException e) {
            throw new BadArgumentsException(path);
        }
        if (createMode.isEphemeral()) {
            // Exception is set when local session failed to upgrade
            // so we just need to report the error
            if (request.getException() != null) {
                throw request.getException();
            }
            zks.sessionTracker.checkGlobalSession(request.sessionId,
                    request.getOwner());
        } else {
            zks.sessionTracker.checkSession(request.sessionId,
                    request.getOwner());
        }
    }

    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acls list of ACLs being assigned to the node (create or setACL operation)
     * @return verified and expanded ACLs
     *  @throws KeeperException.InvalidACLException
     */
    private List<ACL> fixupACL(String path, List<Id> authInfo, List<ACL> acls)
        throws KeeperException.InvalidACLException {
        // check for well formed ACLs
        // This resolves https://issues.apache.org/jira/browse/ZOOKEEPER-1877
        List<ACL> uniqacls = removeDuplicates(acls);
        if (uniqacls == null || uniqacls.size() == 0) {
            throw new KeeperException.InvalidACLException(path);
        }
        List<ACL> rv = new ArrayList<>();
        for (ACL a: uniqacls) {
            LOG.debug("Processing ACL: {}", a);
            if (a == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            Id id = a.getId();
            if (id == null || id.getScheme() == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                rv.add(a);
            } else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    ServerAuthenticationProvider ap =
                        ProviderRegistry.getServerProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                            + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        rv.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    throw new KeeperException.InvalidACLException(path);
                }
            } else {
                ServerAuthenticationProvider ap = ProviderRegistry.getServerProvider(id.getScheme());
                if (ap == null || !ap.isValid(id.getId())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                rv.add(a);
            }
        }
        return rv;
    }

    // processRequest:生产者，将请求放入队列
    // 将request加到submittedRequests中
    public void processRequest(Request request) {
        //放入请求队列并更新request的prepQueueStartTime时间
        request.prepQueueStartTime =  Time.currentElapsedTime();
        submittedRequests.add(request);
        ServerMetrics.getMetrics().PREP_PROCESSOR_QUEUED.add(1);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        // 向请求队列中放入关闭的请求
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
