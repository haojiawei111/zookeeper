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

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.server.ZooKeeperServer.State;

/**
 * ZooKeeperCriticalThread:表明了哪些入口的异常可以算作是严重的异常，能够让server关闭的，并且handleException方法完成对ZooKeeperServerListener的调用
 * ZooKeeperServerListener:表示接收到异常时，通知zk状态变更为ERROR
 *
 * ZooKeeper server shutdown handler which will be used to handle ERROR or
 * SHUTDOWN server state transitions, which in turn releases the associated
 * shutdown latch.
 * ZooKeeper服务器关闭处理程序，它将用于处理ERROR或 SHUTDOWN服务器状态转换，从而释放相关的 shutdown锁存器。
 *
 * 启动时，zkServer有一个计时器为1，当遇到严重异常时，计时器-1变为0，就调用ZooKeeperServerMain#shutdown
 *
 */
class ZooKeeperServerShutdownHandler {
    private final CountDownLatch shutdownLatch;

    ZooKeeperServerShutdownHandler(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
    }

    /**
     * This will be invoked when the server transition to a new server state.
     * 关闭处理器handle，
     * 把一个计时器-1
     * @param state new server state
     */
    void handle(State state) {
        if (state == State.ERROR || state == State.SHUTDOWN) {
            shutdownLatch.countDown();
        }
    }
}
