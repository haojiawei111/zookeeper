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

package org.apache.zookeeper.server.persistence;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class
 * above the implementations
 * of txnlog and snapshot
 * classes
 * 这是txnlog和snapshot 类的实现之上的封装类
 */
public class FileTxnSnapLog {
    //the directory containing the
    //the transaction logs
    final File dataDir;//事务日志目录
    //the directory containing the
    //the snapshot directory
    final File snapDir;//快照目录
    TxnLog txnLog;
    SnapShot snapLog;
    private final boolean autoCreateDB;
    public final static int VERSION = 2;//版本号
    public final static String version = "version-";

    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE =
            "zookeeper.datadir.autocreate";

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT = "true";

    static final String ZOOKEEPER_DB_AUTOCREATE = "zookeeper.db.autocreate";

    private static final String ZOOKEEPER_DB_AUTOCREATE_DEFAULT = "true";

    /**
     * This listener helps
     * the external apis calling
     * restore to gather information
     * while the data is being
     * restored.
     * 此监听器帮助外部apis调用恢复以在数据被恢复时收集信息。
     */
    //根据日志恢复dataTree，session时的回调函数
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }

    /**
     * the constructor which takes the datadir and
     * snapdir.
     * @param dataDir the transaction directory
     * @param snapDir the snapshot directory
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);

        // 默认生成一个version-2的文件夹
        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);

        // by default create snap/log dirs, but otherwise complain instead
        // See ZOOKEEPER-1161 for more details
        boolean enableAutocreate = Boolean.valueOf(
                System.getProperty(ZOOKEEPER_DATADIR_AUTOCREATE,
                        ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT));

        if (!this.dataDir.exists()) {
            if (!enableAutocreate) {
                throw new DatadirException("Missing data directory 缺少数据目录"
                        + this.dataDir
                        + ", automatic data directory creation is disabled 自动数据目录创建已禁用 ("
                        + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.请手动创建此目录");
            }

            if (!this.dataDir.mkdirs()) {
                throw new DatadirException("Unable to create data directory 无法创建数据目录"
                        + this.dataDir);
            }
        }
        if (!this.dataDir.canWrite()) {
            throw new DatadirException("Cannot write to data directory 无法写入数据目录" + this.dataDir);
        }

        if (!this.snapDir.exists()) {
            // by default create this directory, but otherwise complain instead
            // See ZOOKEEPER-1161 for more details
            if (!enableAutocreate) {
                throw new DatadirException("Missing snap directory 缺少快照数据目录"
                        + this.snapDir
                        + ", automatic data directory creation is disabled 自动数据目录创建已禁用("
                        + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.请手动创建此目录");
            }

            if (!this.snapDir.mkdirs()) {
                throw new DatadirException("Unable to create snap directory 无法创建快照数据目录"
                        + this.snapDir);
            }
        }
        if (!this.snapDir.canWrite()) {
            throw new DatadirException("Cannot write to snap directory 无法写入快照数据目录" + this.snapDir);
        }

        // check content of transaction log and snapshot dirs if they are two different directories
        // 如果它们是两个不同的目录，则检查事务日志和快照目录的内容
        // 事务日志目录不能包含快照文件，快照目录不能包含事务日志文件
        // See ZOOKEEPER-2967 for more details
        if(!this.dataDir.getPath().equals(this.snapDir.getPath())){
            checkLogDir();
            checkSnapDir();
        }

        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);

        autoCreateDB = Boolean.parseBoolean(System.getProperty(ZOOKEEPER_DB_AUTOCREATE,
                ZOOKEEPER_DB_AUTOCREATE_DEFAULT));
    }

    private void checkLogDir() throws LogDirContentCheckException {
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isSnapshotFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct." +
                    "日志目录包含快照文件。检查dataLogDir和dataDir配置是否正确。");
        }
    }

    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Util.isLogFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct." +
                    "快照目录包含日志文件。检查dataLogDir和dataDir配置是否正确。");
        }
    }


    // 设置txnLog的服务状态
    public void setServerStats(ServerStats serverStats) {
        txnLog.setServerStats(serverStats);
    }

    /**
     * get the datadir used by this filetxn
     * snap log
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }

    /**
     * get the snap dir used by this
     * filetxn snap log
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }

    /**
     * TODO: 根据snapshot和txnlog，恢复datatree以及sessions，并配置回调函数
     * this function restores the server
     * database after reading from the
     * snapshots and transaction logs
     *
     * @param dt the datatree to be restored
     * @param sessions the sessions to be restored
     * @param listener the playback listener to run on the
     * database restoration
     * @return the highest zxid restored
     * @throws IOException
     */
    public long restore(DataTree dt, Map<Long, Integer> sessions,PlayBackListener listener) throws IOException {
        //取最近的快照 反序列化dt和sessions
        long deserializeResult = snapLog.deserialize(dt, sessions);
        // 事务日志对象
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        boolean trustEmptyDB;
        File initFile = new File(dataDir.getParent(), "initialize");
        if (Files.deleteIfExists(initFile.toPath())) {
            // 如果存在删除这个文件
            // 找到初始化文件，空数据库不会阻止投票参与
            LOG.info("Initialize file found, an empty database will not block voting participation");
            trustEmptyDB = true;
        } else {
            trustEmptyDB = autoCreateDB;
        }
        if (-1L == deserializeResult) {
            /* this means that we couldn't find any snapshot, so we need to
             * initialize an empty database (reported in ZOOKEEPER-2325) */
            if (txnLog.getLastLoggedZxid() != -1) {
                throw new IOException(
                        "No snapshot found, but there are log entries. " +
                        "Something is broken!");
            }

            if (trustEmptyDB) {
                /* TODO: (br33d) we should either put a ConcurrentHashMap on restore()
                 *       or use Map on save() */
                save(dt, (ConcurrentHashMap<Long, Integer>)sessions, false);

                /* return a zxid of 0, since we know the database is empty */
                return 0L;
            } else {
                /* return a zxid of -1, since we are possibly missing data */
                LOG.warn("Unexpected empty data tree, setting zxid to -1");
                dt.lastProcessedZxid = -1L;
                return -1L;
            }
        }
        return fastForwardFromEdits(dt, sessions, listener);
    }

    /**
     * This function will fast forward the server database to have the latest
     * transactions in it.  This is the same as restore, but only reads from
     * the transaction logs and not restores from a snapshot.
     *
     * 此功能将快速转发服务器数据库以在其中包含最新的事务。
     * 这与还原相同，但仅从事务日志读取，而不从快照还原。
     *
     * @param dt the datatree to write transactions to.
     * @param sessions the sessions to be restored.
     * @param listener the playback listener to run on the
     * database transactions.
     * @return the highest zxid restored.
     * @throws IOException
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions,
                                     PlayBackListener listener) throws IOException {

        // 依据dt最后的事务ID从txnLog中创建事务日志的迭代器，这里读dt.lastProcessedZxid+1号的事务日志
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid+1);
        // 目前快照中最大的zxid
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {
                // iterator points to
                // the first valid txn when initialized
                hdr = itr.getHeader();
                if (hdr == null) {
                    //empty logs 空日志
                    return dt.lastProcessedZxid;
                }
                //如果出现了事务迭代器的zxid比当前最大的zxid小的话，抛异常(就是snapshot是落后于txn的zxid的)
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(highestZxid) > {}(next log) for type {}",
                            highestZxid, hdr.getZxid(), hdr.getType());
                } else {
                    highestZxid = hdr.getZxid();
                }
                try {
                    // 回放事务日志 在dt上处理事务
                    processTransaction(hdr,dt,sessions, itr.getTxn());
                } catch(KeeperException.NoNodeException e) {
                   throw new IOException("Failed to process transaction type: " +
                         hdr.getType() + " error: " + e.getMessage(), e);
                }
                // 执行回调函数
                // 每当有一个事务被应用到内存数据库中，ZooKeeper同时会回调PlayBackListener监听器，
                // TODO：将这一事务操作记录转换成Proposal，保存到ZKDatabase.committedLog中，以便Follower进行快速同步。
                listener.onTxnLoaded(hdr, itr.getTxn());
                if (!itr.next())
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        //返回最高的zxid
        return highestZxid;
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid starting zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid) throws IOException {
        return readTxnLog(zxid, true);
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid starting zxid
     * @param fastForward true if the iterator should be fast forwarded to point
     *        to the txn of a given zxid, else the iterator will point to the
     *        starting txn of a txnlog that may contain txn of a given zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid, boolean fastForward)
            throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.read(zxid, fastForward);
    }
    
    /**
     * process the transaction on the datatree
     *
     * 处理数据树上的事务
     * 主要是回放事务 恢复使用
     *
     * @param hdr the hdr of the transaction
     * @param dt the datatree to apply transaction to
     * @param sessions the sessions to be restored
     * @param txn the transaction to be applied
     */
    /*
    根据事务日志来处理事务，更新dataTree，
    主要就是sessions这个map的维护，再调用dt.processTxn(hdr, txn);
     */
    public void processTransaction(TxnHeader hdr,DataTree dt, Map<Long, Integer> sessions, Record txn)
        throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
        case OpCode.createSession:
            //如果是创建session，添加到sessions这个map里面去
            sessions.put(hdr.getClientId(),
                    ((CreateSessionTxn) txn).getTimeOut());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- create session in log: 0x"
                                + Long.toHexString(hdr.getClientId())
                                + " with timeout: "
                                + ((CreateSessionTxn) txn).getTimeOut());
            }
            // give dataTree a chance to sync its lastProcessedZxid
            // 让dataTree有机会同步它的lastProcessedZxid
            rc = dt.processTxn(hdr, txn);
            break;
        case OpCode.closeSession:
            //如果是关闭session，从sessions这个map里面删除
            sessions.remove(hdr.getClientId());
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG,ZooTrace.SESSION_TRACE_MASK,
                        "playLog --- close session in log: 0x"
                                + Long.toHexString(hdr.getClientId()));
            }
            //用dataTree处理事务
            rc = dt.processTxn(hdr, txn);
            break;
        default:
            //用dataTree处理事务
            rc = dt.processTxn(hdr, txn);
        }

        /**
         * Snapshots are lazily created. So when a snapshot is in progress,
         * there is a chance for later transactions to make into the
         * snapshot. Then when the snapshot is restored, NONODE/NODEEXISTS
         * errors could occur. It should be safe to ignore these.
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug(
                    "Ignoring processTxn failure hdr: {}, error: {}, path: {}",
                    hdr.getType(), rc.err, rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * todo:save函数中，用的是lastZxid，也就是一个snap文件的命名是它记录的最后一个zxid，不是第一个
     *
     * save the datatree and the sessions into a snapshot
     * @param dataTree the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the session timeouts to be
     * serialized onto disk
     * @param syncSnap sync the snapshot immediately after write
     * @throws IOException
     */
    //将sessions和datatree保存至snapshot文件，是序列化的过程
    public void save(DataTree dataTree,
                     ConcurrentHashMap<Long, Integer> sessionsWithTimeouts,
                     boolean syncSnap)
        throws IOException {
        long lastZxid = dataTree.lastProcessedZxid;
        //按照lastZxid来命名，也就是当前最大的zxid
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid),
                snapshotFile);
        try {
            snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile, syncSnap);
        } catch (IOException e) {
            if (snapshotFile.length() == 0) {
                /* This may be caused by a full disk. In such a case, the server
                 * will get stuck in a loop where it tries to write a snapshot
                 * out to disk, and ends up creating an empty file instead.
                 * Doing so will eventually result in valid snapshots being
                 * removed during cleanup. */
                if (snapshotFile.delete()) {
                    LOG.info("Deleted empty snapshot file: " +
                             snapshotFile.getAbsolutePath());
                } else {
                    LOG.warn("Could not delete empty snapshot file: " +
                             snapshotFile.getAbsolutePath());
                }
            } else {
                /* Something else went wrong when writing the snapshot out to
                 * disk. If this snapshot file is invalid, when restarting,
                 * ZooKeeper will skip it, and find the last known good snapshot
                 * instead. */
            }
            throw e;
        }
    }

    /**
     * truncate the transaction logs the zxid
     * specified
     * 截断指定的zxid 后面的所有事务日志
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog 关闭现有的txnLog和snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);//用FileTxnLog删掉指定zxid后的所有事务日志
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }

    /**
     * the most recent snapshot in the snapshot
     * directory
     * snapshot 目录中的最新快照
     * @return the file that contains the most
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }

    /**
     * the n most recent snapshots  n个最近的快照
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * get the snapshot logs which may contain transactions newer than the given zxid.
     * This includes logs with starting zxid greater than given zxid, as well as the
     * newest transaction log with starting zxid less than given zxid.  The latter log
     * file may contain transactions beyond given zxid.
     * 获取可能包含比给定zxid更新的事务的快照日志。
     * 这包括起始zxid大于给定zxid的日志，以及起始zxid小于给定zxid的最新事务日志。后一个log 文件可能包含超出给定zxid的事务。
     * @param zxid the zxid that contains logs greater than
     * zxid
     * @return
     */
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * append the request to the transaction logs
     *
     * 将请求附加到事务日志
     *
     * @param si the request to be appended
     * returns true iff something appended, otw false
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.getHdr(), si.getTxn());
    }

    /**
     * commit the transaction of logs
     *
     * 提交日志事务
     *
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     *
     * @return elapsed sync time of transaction log commit in milliseconds
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLog.getTxnLogSyncElapsedTime();
    }

    /**
     * roll the transaction logs
     *
     * 滚动事务日志
     *
     * @throws IOException
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }

    /**
     * close the transaction log files
     * 关闭事务日志文件和快照
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }


    /******************************************定义异常类*****************************************************************/
    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {
        public DatadirException(String msg) {
            super(msg);
        }
        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }
    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {
        public LogDirContentCheckException(String msg) {
            super(msg);
        }
    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {
        public SnapDirContentCheckException(String msg) {
            super(msg);
        }
    }
}
