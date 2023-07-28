/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.sessiondict.client;

import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.Ping;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;

public interface ISessionDictClient extends IConnectable {
    static SessionDictClientBuilder newBuilder() {
        return new SessionDictClientBuilder();
    }

    /**
     * Register an IMessagePipeline for one session
     *
     * @param clientInfo
     * @return
     */
    IRPCClient.IMessageStream<Quit, Ping> reg(ClientInfo clientInfo);

    CompletableFuture<KillReply> kill(long reqId, String tenantId, String userId, String clientId, ClientInfo killer);

    void stop();
}
