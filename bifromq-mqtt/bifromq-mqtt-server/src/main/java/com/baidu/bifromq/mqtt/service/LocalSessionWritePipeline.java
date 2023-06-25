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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.DeliveryPack;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteResult;
import com.baidu.bifromq.mqtt.session.v3.IMQTT3TransientSession;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class LocalSessionWritePipeline extends ResponsePipeline<WriteRequest, WriteReply> {
    private final Map<String, IMQTT3TransientSession> sessionMap;

    public LocalSessionWritePipeline(Map<String, IMQTT3TransientSession> sessionMap,
                                     StreamObserver<WriteReply> responseObserver) {
        super(responseObserver);
        this.sessionMap = sessionMap;
    }

    @Override
    protected CompletableFuture<WriteReply> handleRequest(String trafficId, WriteRequest request) {
        log.trace("Handle inbox write request: \n{}", request);
        WriteReply.Builder replyBuilder = WriteReply.newBuilder().setReqId(request.getReqId());
        Set<SubInfo> ok = new HashSet<>();
        Set<SubInfo> noInbox = new HashSet<>();
        for (DeliveryPack deliveryPack : request.getDeliveryPackList()) {
            TopicMessagePack topicMsgPack = deliveryPack.getMessagePack();
            List<SubInfo> subInfos = deliveryPack.getSubscriberList();
            Map<IMQTT3TransientSession, SubInfo> inboxes = new HashMap<>();
            for (SubInfo subInfo : subInfos) {
                IMQTT3TransientSession session = sessionMap.get(subInfo.getInboxId());
                if (!noInbox.contains(subInfo) && session != null) {
                    ok.add(subInfo);
                    inboxes.put(session, subInfo);
                } else {
                    noInbox.add(subInfo);
                }
            }
            inboxes.forEach((session, subInfo) -> session.publish(subInfo, topicMsgPack));
        }
        ok.forEach(subInfo -> replyBuilder.addResult(WriteResult.newBuilder()
            .setSubInfo(subInfo)
            .setResult(WriteResult.Result.OK)
            .build()));
        noInbox.forEach(subInfo -> replyBuilder.addResult(WriteResult.newBuilder()
            .setSubInfo(subInfo)
            .setResult(WriteResult.Result.NO_INBOX)
            .build()));
        return CompletableFuture.completedFuture(replyBuilder.build());
    }
}
