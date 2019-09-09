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

package org.apache.zookeeper.client;

import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.ConfigUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.common.StringUtils.split;

/**
 *
 *  ConnectStringParser
 *     解析服务器地址
 *     解析chrootPath

 *
 * A parser for ZooKeeper Client connect strings.
 * 
 * This class is not meant to be seen or used outside of ZooKeeper itself.
 * 
 * The chrootPath member should be replaced by a Path object in issue
 * ZOOKEEPER-849.
 * 
 * @see org.apache.zookeeper.ZooKeeper
 */
public final class ConnectStringParser {
    //默认端口
    private static final int DEFAULT_PORT = 2181;

    //客户端命名空间客
    // 户端可以通过在connectString中添加后缀的方式来设置Chroot，如下所示：
    //  192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181/apps/X
    //这个client的chrootPath就是/apps/X
    //将这样一个connectString传入客户端的ConnectStringParser后就能够解析出Chroot并保存在chrootPath属性中。
    private final String chrootPath;

    //地址列表
    private final ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    /**
     * Parse host and port by spliting client connectString
     * with support for IPv6 literals
     * @throws IllegalArgumentException
     *             for an invalid chroot path.
     */
    public ConnectStringParser(String connectString) {
        // parse out chroot, if any
        int off = connectString.indexOf('/');
        if (off >= 0) {
            String chrootPath = connectString.substring(off);
            // ignore "/" chroot spec, same as null
            if (chrootPath.length() == 1) {
                this.chrootPath = null;
            } else {
                PathUtils.validatePath(chrootPath);
                this.chrootPath = chrootPath;
            }
            connectString = connectString.substring(0, off);
        } else {
            this.chrootPath = null;
        }

        List<String> hostsList = split(connectString,",");
        for (String host : hostsList) {
            int port = DEFAULT_PORT;
            try {
                String[] hostAndPort = ConfigUtils.getHostAndPort(host);//端口
                host = hostAndPort[0];
                if (hostAndPort.length == 2) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } catch (ConfigException e) {
                e.printStackTrace();
            }
		    
            serverAddresses.add(InetSocketAddress.createUnresolved(host, port));
        }
    }

    public String getChrootPath() {
        return chrootPath;
    }

    public ArrayList<InetSocketAddress> getServerAddresses() {
        return serverAddresses;
    }
}
