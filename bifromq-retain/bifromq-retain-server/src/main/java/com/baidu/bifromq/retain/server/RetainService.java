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

import static com.baidu.bifromq.baserpc.UnaryResponse.response;

import com.baidu.bifromq.deliverer.DeliveryRequest;
import com.baidu.bifromq.deliverer.IMessageDeliverer;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceGrpc;
import com.baidu.bifromq.retain.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.IRetainCallScheduler;
import com.baidu.bifromq.retain.server.scheduler.MatchCall;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainService extends RetainServiceGrpc.RetainServiceImplBase {
    private final IMessageDeliverer messageDeliverer;
    private final IMatchCallScheduler matchCallScheduler;
    private final IRetainCallScheduler retainCallScheduler;

    RetainService(IMessageDeliverer messageDeliverer,
                  IMatchCallScheduler matchCallScheduler,
                  IRetainCallScheduler retainCallScheduler) {
        this.messageDeliverer = messageDeliverer;
        this.matchCallScheduler = matchCallScheduler;
        this.retainCallScheduler = retainCallScheduler;
    }

    @Override
    public void retain(RetainRequest request, StreamObserver<RetainReply> responseObserver) {
        log.trace("Handling retain request:\n{}", request);
        response((tenantId, metadata) -> retainCallScheduler.schedule(request)
            .exceptionally(e -> {
                log.error("Retain failed", e);
                return RetainReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(RetainReply.Result.ERROR)
                    .build();

            }), responseObserver);
    }

    @Override
    public void match(MatchRequest request, StreamObserver<MatchReply> responseObserver) {
        log.trace("Handling match request:\n{}", request);
        response((tenantId, metadata) -> matchCallScheduler
            .schedule(new MatchCall(request.getMatchInfo().getTenantId(), request.getMatchInfo().getTopicFilter(),
                request.getLimit()))
            .thenCompose(matchCallResult -> {
                if (matchCallResult.result() == MatchReply.Result.OK) {
                    MatchInfo matchInfo = request.getMatchInfo();
                    List<CompletableFuture<DeliveryResult>> deliveryResults = matchCallResult.retainMessages()
                        .stream()
                        .map(retainedMsg -> {
                            TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder()
                                .setTopic(retainedMsg.getTopic())
                                .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                                    .addMessage(retainedMsg.getMessage())
                                    .setPublisher(retainedMsg.getPublisher())
                                    .build())
                                .build();
                            return messageDeliverer.schedule(new DeliveryRequest(matchInfo,
                                request.getBrokerId(), request.getDelivererKey(), topicMessagePack));
                        }).toList();
                    return CompletableFuture.allOf(deliveryResults.toArray(CompletableFuture[]::new))
                        .thenApply(v -> deliveryResults.stream().map(CompletableFuture::join))
                        .thenApply(resultList -> {
                            if (resultList.allMatch(r -> r == DeliveryResult.OK)) {
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
                    .setResult(MatchReply.Result.ERROR)
                    .build());
            })
            .exceptionally(e -> {
                log.error("Match failed", e);
                return MatchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(MatchReply.Result.ERROR)
                    .build();
            }), responseObserver);
    }
}
