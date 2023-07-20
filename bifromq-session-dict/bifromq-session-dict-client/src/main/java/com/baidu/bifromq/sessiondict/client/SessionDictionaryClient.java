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

import static com.baidu.bifromq.sessiondict.WCHKeyUtil.toWCHKey;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.sessiondict.PipelineUtil;
import com.baidu.bifromq.sessiondict.rpc.proto.KillReply;
import com.baidu.bifromq.sessiondict.rpc.proto.KillRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.Ping;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictionaryServiceGrpc;
import com.baidu.bifromq.type.ClientInfo;
import io.reactivex.rxjava3.core.Observable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SessionDictionaryClient implements ISessionDictionaryClient {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final IRPCClient rpcClient;

    SessionDictionaryClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public IRPCClient.IMessageStream<Quit, Ping> reg(ClientInfo clientInfo) {
        assert MQTT_TYPE_VALUE.equalsIgnoreCase(clientInfo.getType());
        Map<String, String> metadata = new HashMap<>();
        metadata.put(PipelineUtil.CLIENT_INFO, PipelineUtil.encode(clientInfo));
        return rpcClient.createMessageStream(clientInfo.getTenantId(),
            null,
            toWCHKey(clientInfo),
            metadata,
            SessionDictionaryServiceGrpc.getJoinMethod());
    }

    @Override
    public CompletableFuture<KillReply> kill(long reqId,
                                             String userId,
                                             String clientId,
                                             ClientInfo killer) {
        return rpcClient.invoke(killer.getTenantId(), null, KillRequest.newBuilder()
            .setReqId(reqId)
            .setUserId(userId)
            .setClientId(clientId)
            .setKiller(killer)
            .build(), SessionDictionaryServiceGrpc.getKillMethod());
    }

    @Override
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping session dict client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Session dict client stopped");
        }
    }
}
