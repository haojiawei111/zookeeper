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

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.zookeeper.Login;
import org.apache.zookeeper.util.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSaslServer {
    public static final String LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.serverconfig";
    public static final String DEFAULT_LOGIN_CONTEXT_NAME = "Server";

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSaslServer.class);
    private SaslServer saslServer;

    ZooKeeperSaslServer(final Login login) {
        saslServer = createSaslServer(login);
    }

    private SaslServer createSaslServer(final Login login) {
        synchronized (login) {
            Subject subject = login.getSubject();
            return SecurityUtils.createSaslServer(subject, "zookeeper",
                    "zk-sasl-md5", login.callbackHandler, LOG);
        }
    }

    /**
     * 评估响应数据并产生挑战。
     *
     * 如果在身份验证过程中从客户端收到响应，则调用此方法以准备适当的下一个质询以提交给客户端。
     * 如果身份验证成功并且没有更多挑战数据要发送到客户端，则挑战为null。
     * 如果必须通过向客户端发送质询来继续进行身份验证，或者如果身份验证成功但客户端需要处理质询数据，则此字段为非null。
     * 在每次调用{@codevaluateResponse（）}之后，应调用{@code isComplete（）}，以确定是否需要客户端进一步的响应。
     *
     * @param response
     * @return
     * @throws SaslException
     */
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        return saslServer.evaluateResponse(response);
    }

    /**
     * 确定认证交换是否已完成。
     * 通常在每次调用{@code validateResponse（）}之后调用此方法，以确定身份验证是成功完成还是应该继续进行。
     *
     * @return
     */
    public boolean isComplete() {
        return saslServer.isComplete();
    }

    public String getAuthorizationID() {
        return saslServer.getAuthorizationID();
    }

}




