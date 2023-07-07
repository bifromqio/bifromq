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

    /**
     * publish a message at best effort, there are many edge cases that could lead to publish failure, so the normal
     * completion of the returned future doesn't mean the message has been delivered successfully. There are various
     * events reported during the process.
     *
     * @param reqId         the caller supplied request id for event tracing
     * @param topic         the message topic
     * @param qos           the message original qos
     * @param payload       the message body
     * @param expirySeconds the expiry of the message
     * @param publisher     the publisher of the message which must have non-null tenantId and type field
     * @return a future for tracking the publishing process asynchronously
     */
    CompletableFuture<Void> pub(long reqId, String topic, QoS qos, ByteBuffer payload, int expirySeconds,
                                ClientInfo publisher);

    /**
     * Add a topic subscription
     *
     * @param reqId        the caller supplied request id for event tracing
     * @param tenantId     the id of caller tenant
     * @param topicFilter  the topic filter to apply
     * @param qos          the qos associated with the topic filter
     * @param inboxId      the id of the receiving inbox which is hosted by corresponding sub broker
     * @param delivererKey the key of the deliverer via which the message will be sent to the inbox
     * @param subBrokerId  the hosting subbroker of the inbox
     * @return correspond to Mqtt Sub QoS
     */
    CompletableFuture<Integer> sub(long reqId, String tenantId, String topicFilter, QoS qos, String inboxId,
                                   String delivererKey, int subBrokerId);

    /**
     * Remove a topic subscription
     *
     * @param reqId        the caller supplied request id for event tracing
     * @param tenantId     the id of caller tenant
     * @param topicFilter  the topic filter to remove
     * @param inboxId      the id of the receiving inbox which is hosted by corresponding sub broker
     * @param delivererKey the key of the deliverer via which the message will be sent to the inbox
     * @param subBrokerId  the hosting subbroker of the inbox
     * @return true for remove successfully, false for subscription not found
     */
    CompletableFuture<Boolean> unsub(long reqId, String tenantId, String topicFilter, String inboxId,
                                     String delivererKey, int subBrokerId);

    CompletableFuture<Void> clear(long reqId, String tenantId, String inboxId, String delivererKey,
                                         int subBrokerId);

    void stop();
}
