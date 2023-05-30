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

package com.baidu.bifromq.retain.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.core.Observable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IRetainServiceClient {
    static RetainServiceClientBuilder.InProcRetainServiceClientBuilder inProcClientBuilder() {
        return new RetainServiceClientBuilder.InProcRetainServiceClientBuilder();
    }

    static RetainServiceClientBuilder.NonSSLRetainServiceClientBuilder nonSSLClientBuilder() {
        return new RetainServiceClientBuilder.NonSSLRetainServiceClientBuilder();
    }

    static RetainServiceClientBuilder.SSLRetainServiceClientBuilder sslClientBuilder() {
        return new RetainServiceClientBuilder.SSLRetainServiceClientBuilder();
    }

    interface IClientPipeline {
        CompletableFuture<RetainReply> retain(long reqId, String topic, QoS qos, ByteBuffer payload, int expirySeconds);

        void close();
    }

    Observable<IRPCClient.ConnState> connState();

    IClientPipeline open(ClientInfo clientInfo);

    CompletableFuture<MatchReply> match(long reqId, String trafficId, String topicFilter,
                                        int limit, ClientInfo clientInfo);

    void stop();
}
