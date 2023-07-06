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

package com.baidu.bifromq.dist.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.core.Observable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IDistClient {
    static DistClientBuilder.InProcDistClientBuilder inProcClientBuilder() {
        return new DistClientBuilder.InProcDistClientBuilder();
    }

    static DistClientBuilder.NonSSLDistClientBuilder nonSSLClientBuilder() {
        return new DistClientBuilder.NonSSLDistClientBuilder();
    }

    static DistClientBuilder.SSLDistClientBuilder sslClientBuilder() {
        return new DistClientBuilder.SSLDistClientBuilder();
    }

    Observable<IRPCClient.ConnState> connState();

    CompletableFuture<DistResult> pub(long reqId, String topic, QoS qos, ByteBuffer payload, int expirySeconds,
                                      ClientInfo publisher);

    CompletableFuture<SubResult> sub(long reqId, String tenantId, String topicFilter, QoS qos, String inboxId,
                                     String delivererKey, int subBrokerId);

    CompletableFuture<UnsubResult> unsub(long reqId, String tenantId, String topicFilter, String inboxId,
                                         String delivererKey, int subBrokerId);

    CompletableFuture<ClearResult> clear(long reqId, String tenantId, String inboxId, String delivererKey,
                                         int subBrokerId);

    void stop();
}
