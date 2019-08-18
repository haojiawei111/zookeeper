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

/**
 * RequestProcessors are chained together to process transactions. Requests are
 * always processed in order. The standalone server, follower, and leader all
 * have slightly different RequestProcessors chained together.
 * 
 * Requests always move forward through the chain of RequestProcessors. Requests
 * are passed to a RequestProcessor through processRequest(). Generally method
 * will always be invoked by a single thread.
 * 
 * When shutdown is called, the request RequestProcessor should also shutdown
 * any RequestProcessors that it is connected to.
 */
// 对于请求处理链而言，所有请求处理器的父接口为RequestProcessor　　

//　　PrepRequestProcessor，通常是请求处理链的第一个处理器。这个可以是单独线程运行
//
//　　CommitProcessor，将到来的请求与本地提交的请求进行匹配，这是因为改变系统状态的本地请求的返回结果是到来的请求。这个可以是单独线程运行
//
//　　FollowerRequestProcessor，将修改了系统状态的请求转发给Leader。这个可以是单独线程运行
//
//　　ObserverRequestProcessor，同FollowerRequestProcessor一样，将修改了系统状态的请求转发给Leader。这个可以是单独线程运行
//
//　　SyncRequestProcessor，发送Sync请求的处理器。这个可以是单独线程运行
//
//　　ReadOnlyRequestProcessor，是ReadOnlyZooKeeperServer请求处理链的第一个处理器，将只读请求传递给下个处理器，抛弃改变状态的请求。这个可以是单独线程运行
//
//    AckRequestProcessor，将前一阶段的请求作为ACK转发给Leader。
//
//　　FinalRequestProcessor，通常是请求处理链的最后一个处理器。
//
//　　ProposalRequestProcessor，将请求转发给AckRequestProcessor和SyncRequestProcessor。
//
//　　SendAckRequestProcessor，发送ACK请求的处理器。
//
//　　ToBeAppliedRequestProcessor，维护toBeApplied列表，下个处理器必须是FinalRequestProcessor并且FinalRequestProcessor必须同步处理请求。
//
//　　UnimplementedRequestProcessor，用于管理未知请求。
//
//    DelayRequestProcessor 允许阻止ZooKeeperServer上的请求处理器队列。 这用于模拟任意长度延迟或在请求处理中产生延迟，这对于给定特征来说是最不方便的，以便进行测试。
//
//    LeaderRequestProcessor 负责执行本地会话升级。只有直接提交给 Leader 的请求才能通过此处理器。

public interface RequestProcessor {
    // 请求处理异常类
    @SuppressWarnings("serial")
    public static class RequestProcessorException extends Exception {
        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    //通过processRequest方法可以将请求传递到下个处理器，通常是单线程的
    void processRequest(Request request) throws RequestProcessorException;
    //而shutdown表示关闭处理器，其意味着该处理器要关闭和其他处理器的连接
    void shutdown();
}
