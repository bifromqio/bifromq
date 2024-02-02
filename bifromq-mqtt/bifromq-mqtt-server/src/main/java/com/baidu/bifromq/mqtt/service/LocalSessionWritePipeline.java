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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WritePack;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteResult;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class LocalSessionWritePipeline extends ResponsePipeline<WriteRequest, WriteReply> {
    private final Map<String, IMQTTTransientSession> sessionMap;

    public LocalSessionWritePipeline(Map<String, IMQTTTransientSession> sessionMap,
                                     StreamObserver<WriteReply> responseObserver) {
        super(responseObserver);
        this.sessionMap = sessionMap;
    }

    @Override
    protected CompletableFuture<WriteReply> handleRequest(String tenantId, WriteRequest request) {
        log.trace("Handle inbox write request: \n{}", request);
        Map<SubInfo, List<TopicMessagePack>> pubPack = new HashMap<>();
        for (WritePack writePack : request.getPackList()) {
            List<SubInfo> subInfos = writePack.getSubscriberList();
            for (SubInfo subInfo : subInfos) {
                pubPack.computeIfAbsent(subInfo, k -> new LinkedList<>()).add(writePack.getMessagePack());
            }
        }
        List<CompletableFuture<WriteResult>> resultFutures = pubPack.entrySet().stream().map(entry -> {
            SubInfo subInfo = entry.getKey();
            List<TopicMessagePack> msgPackList = entry.getValue();
            IMQTTTransientSession session = sessionMap.get(subInfo.getInboxId());
            if (session == null) {
                return CompletableFuture.completedFuture(WriteResult.newBuilder()
                    .setSubInfo(subInfo)
                    .setResult(WriteResult.Result.NO_INBOX)
                    .build());
            } else {
                return session.publish(subInfo, msgPackList)
                    .thenApply(result -> WriteResult.newBuilder().setSubInfo(subInfo).setResult(result).build());
            }
        }).toList();
        return CompletableFuture.allOf(resultFutures.toArray(CompletableFuture[]::new))
            .thenApply(v -> resultFutures.stream().map(CompletableFuture::join).toList())
            .thenApply(results -> WriteReply.newBuilder()
                .setReqId(request.getReqId())
                .addAllResult(results)
                .build());
    }
}
