/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.retain.server;

import static com.baidu.bifromq.baserpc.server.UnaryResponse.response;
import static com.baidu.bifromq.deliverer.DeliveryCallResult.OK;
import static com.baidu.bifromq.metrics.TenantMetric.MqttRetainMatchedBytes;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.basescheduler.exception.BatcherUnavailableException;
import com.baidu.bifromq.deliverer.DeliveryCall;
import com.baidu.bifromq.deliverer.DeliveryCallResult;
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.deliverer.TopicMessagePackHolder;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.retain.rpc.proto.ExpireAllReply;
import com.baidu.bifromq.retain.rpc.proto.ExpireAllRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceGrpc;
import com.baidu.bifromq.retain.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.IRetainCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.MatchRetainedRequest;
import com.baidu.bifromq.retain.store.gc.IRetainStoreGCProcessor;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainService extends RetainServiceGrpc.RetainServiceImplBase {
    private final IRetainStoreGCProcessor gcProcessor;
    private final IMessageDeliverer messageDeliverer;
    private final IMatchCallScheduler matchCallScheduler;
    private final IRetainCallScheduler retainCallScheduler;
    private final IRetainCallScheduler deleteCallScheduler;

    RetainService(IRetainStoreGCProcessor gcProcessor,
                  IMessageDeliverer messageDeliverer,
                  IMatchCallScheduler matchCallScheduler,
                  IRetainCallScheduler retainCallScheduler,
                  IRetainCallScheduler deleteCallScheduler) {
        this.gcProcessor = gcProcessor;
        this.messageDeliverer = messageDeliverer;
        this.matchCallScheduler = matchCallScheduler;
        this.retainCallScheduler = retainCallScheduler;
        this.deleteCallScheduler = deleteCallScheduler;
    }

    @Override
    public void retain(RetainRequest request, StreamObserver<RetainReply> responseObserver) {
        log.trace("Handling retain request:\n{}", request);
        response((tenantId, metadata) -> {
            CompletionStage<RetainReply> completionStage;
            if (request.getMessage().getPayload().isEmpty()) {
                completionStage = deleteCallScheduler.schedule(request);
            } else {
                completionStage = retainCallScheduler.schedule(request);
            }
            return completionStage.exceptionally(e -> {
                if (e instanceof BatcherUnavailableException || e.getCause() instanceof BatcherUnavailableException) {
                    return RetainReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(RetainReply.Result.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return RetainReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(RetainReply.Result.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Retain failed", e);
                return RetainReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(RetainReply.Result.ERROR)
                    .build();

            });
        }, responseObserver);
    }

    @Override
    public void match(MatchRequest request, StreamObserver<MatchReply> responseObserver) {
        log.trace("Handling match request:\n{}", request);
        response((tenantId, metadata) -> matchCallScheduler
            .schedule(new MatchRetainedRequest(request.getTenantId(),
                request.getMatchInfo().getMatcher().getMqttTopicFilter(),
                request.getLimit()))
            .thenCompose(matchCallResult -> {
                if (Objects.requireNonNull(matchCallResult.result()) == MatchReply.Result.OK) {
                    MatchInfo matchInfo = request.getMatchInfo();
                    AtomicInteger matchedBytes = new AtomicInteger();
                    List<CompletableFuture<DeliveryCallResult>> deliveryResults = matchCallResult.retainMessages()
                        .stream()
                        .map(retainedMsg -> {
                            matchedBytes.addAndGet(
                                retainedMsg.getTopic().length() + retainedMsg.getMessage().getPayload().size());
                            TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder()
                                .setTopic(retainedMsg.getTopic())
                                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                                    .addMessage(retainedMsg.getMessage())
                                    .setPublisher(retainedMsg.getPublisher())
                                    .build())
                                .build();
                            return messageDeliverer.schedule(new DeliveryCall(request.getTenantId(), matchInfo,
                                request.getBrokerId(), request.getDelivererKey(),
                                TopicMessagePackHolder.hold(topicMessagePack)));
                        }).toList();
                    ITenantMeter.get(request.getTenantId()).recordSummary(MqttRetainMatchedBytes, matchedBytes.get());
                    return CompletableFuture.allOf(deliveryResults.toArray(CompletableFuture[]::new))
                        .thenApply(v -> deliveryResults.stream().map(CompletableFuture::join))
                        .thenApply(resultList -> {
                            if (resultList.allMatch(r -> r == OK)) {
                                return MatchReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setResult(MatchReply.Result.OK)
                                    .build();
                            } else {
                                return MatchReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setResult(MatchReply.Result.ERROR)
                                    .build();
                            }
                        });
                }
                return CompletableFuture.completedFuture(MatchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(matchCallResult.result())
                    .build());
            })
            .exceptionally(e -> {
                if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                    return MatchReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(MatchReply.Result.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Match failed", e);
                return MatchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(MatchReply.Result.ERROR)
                    .build();
            }), responseObserver);
    }

    @Override
    public void expireAll(ExpireAllRequest request, StreamObserver<ExpireAllReply> responseObserver) {
        response(tenantId -> gcProcessor.gc(request.getReqId(), request.getTenantId(),
                request.hasExpirySeconds() ? request.getExpirySeconds() : null,
                HLC.INST.getPhysical())
            .thenApply(result -> {
                switch (result) {
                    case OK -> {
                        return ExpireAllReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(ExpireAllReply.Result.OK)
                            .build();
                    }
                    case TRY_LATER -> {
                        return ExpireAllReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(ExpireAllReply.Result.TRY_LATER)
                            .build();
                    }
                    default -> {
                        return ExpireAllReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(ExpireAllReply.Result.ERROR)
                            .build();
                    }
                }
            }), responseObserver);
    }

    public void close() {
        log.debug("Stop match call scheduler");
        matchCallScheduler.close();
        log.debug("Stop retain call scheduler");
        retainCallScheduler.close();
        log.debug("Stop delete call scheduler");
        deleteCallScheduler.close();
        log.debug("Stop message deliverer");
        messageDeliverer.close();
    }
}
