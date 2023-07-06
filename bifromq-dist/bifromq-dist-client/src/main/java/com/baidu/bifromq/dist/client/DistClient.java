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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.dist.client.scheduler.ClientCall;
import com.baidu.bifromq.dist.client.scheduler.DistServerCallScheduler;
import com.baidu.bifromq.dist.rpc.proto.ClearRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.core.Observable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistClient implements IDistClient {
    private final DistServerCallScheduler reqScheduler;
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DistClient(@NonNull IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        reqScheduler = new DistServerCallScheduler(rpcClient);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<DistResult> dist(long reqId, String topic, QoS qos, ByteBuffer payload,
                                              int expirySeconds, ClientInfo sender) {
        long now = HLC.INST.getPhysical();
        long expiry = expirySeconds == Integer.MAX_VALUE ? Long.MAX_VALUE :
            now + TimeUnit.MILLISECONDS.convert(expirySeconds, TimeUnit.SECONDS);
        return reqScheduler.schedule(new ClientCall(sender, topic, Message.newBuilder()
            .setMessageId(reqId)
            .setPubQoS(qos)
            .setPayload(unsafeWrap(payload))
            .setTimestamp(now)
            .setExpireTimestamp(expiry)
            .build()));
    }

    @Override
    public CompletableFuture<SubResult> sub(long reqId, String topicFilter, QoS qos, String inboxId,
                                            String delivererKey, int subBrokerId, ClientInfo client) {
        SubRequest request = SubRequest.newBuilder()
            .setReqId(reqId)
            .setTopicFilter(topicFilter)
            .setSubQoS(qos)
            .setInboxId(inboxId)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .setClient(client)
            .build();
        log.trace("Handling sub request:\n{}", request);
        return rpcClient.invoke(client.getTenantId(), null, request, DistServiceGrpc.getSubMethod())
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("Sub request failed: reqId={}, tenantId={}", request.getReqId(),
                        client.getTenantId(), e);
                    return SubResult.error(e);
                }
                log.trace("Finish handling sub request:\n{}, reply:\n{}", request, v);
                switch (v.getResult()) {
                    case OK_QoS0:
                        return SubResult.QoS0;
                    case OK_QoS1:
                        return SubResult.QoS1;
                    case OK_QoS2:
                        return SubResult.QoS2;
                    case Failure:
                    default:
                        return SubResult.InternalError;
                }
            });

    }

    @Override
    public CompletableFuture<UnsubResult> unsub(long reqId, String topicFilter, String inbox, String delivererKey,
                                                int subBrokerId, ClientInfo client) {
        UnsubRequest request = UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTopicFilter(topicFilter)
            .setInboxId(inbox)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .setClient(client)
            .build();
        log.trace("Handling unsub request:\n{}", request);
        return rpcClient.invoke(client.getTenantId(), null, request, DistServiceGrpc.getUnsubMethod())
            .handle((v, e) -> {
                log.trace("Finish handling unsub request:\n{}", request);
                if (e != null) {
                    log.debug("unsub request error: reqId={}, tenantId={}",
                        request.getReqId(), client.getTenantId(), e);
                    return UnsubResult.error(e);
                }
                switch (v.getResult()) {
                    case OK:
                        return UnsubResult.OK;
                    case ERROR:
                    default:
                        return UnsubResult.InternalError;
                }
            });
    }

    @Override
    public CompletableFuture<ClearResult> clear(long reqId, String inboxId, String delivererKey,
                                                int subBrokerId, ClientInfo client) {
        log.trace("Requesting clear: reqId={}", reqId);
        ClearRequest request = ClearRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .setClient(client)
            .build();
        return rpcClient.invoke(client.getTenantId(), null, request, DistServiceGrpc.getClearMethod())
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("clear request error: reqId={}", reqId, e);
                    return ClearResult.error(e);
                } else {
                    log.trace("Got clear reply: request={}, reply={}", request, v);
                }
                switch (v.getResult()) {
                    case OK:
                        return ClearResult.OK;
                    case ERROR:
                    default:
                        return ClearResult.INTERNAL_ERROR;
                }
            });
    }

    @Override
    public void stop() {
        // close tenant logger and drain logs before closing the dist client
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping dist client");
            log.debug("Closing request scheduler");
            reqScheduler.close();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Dist client stopped");
        }
    }
}
