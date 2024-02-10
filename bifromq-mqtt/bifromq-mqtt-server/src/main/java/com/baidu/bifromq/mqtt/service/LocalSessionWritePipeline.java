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

import static java.util.Collections.singletonList;

import com.baidu.bifromq.baserpc.ResponsePipeline;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WritePack;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteResult;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        WriteReply.Builder replyBuilder = WriteReply.newBuilder().setReqId(request.getReqId());
        Set<SubInfo> ok = new HashSet<>();
        Set<SubInfo> noSub = new HashSet<>();
        for (WritePack writePack : request.getPackList()) {
            TopicMessagePack topicMsgPack = writePack.getMessagePack();
            List<SubInfo> subInfos = writePack.getSubscriberList();
            for (SubInfo subInfo : subInfos) {
                if (!noSub.contains(subInfo)) {
                    IMQTTTransientSession session = sessionMap.get(subInfo.getInboxId());
                    if (session != null) {
                        boolean success = session.publish(subInfo, singletonList(topicMsgPack));
                        if (success) {
                            ok.add(subInfo);
                        } else {
                            noSub.add(subInfo);
                        }
                    } else {
                        // no session found for the subscription
                        noSub.add(subInfo);
                    }
                }
            }
        }
        ok.forEach(subInfo -> replyBuilder.addResult(WriteResult.newBuilder()
            .setSubInfo(subInfo)
            .setResult(WriteResult.Result.OK)
            .build()));
        noSub.forEach(subInfo -> replyBuilder.addResult(WriteResult.newBuilder()
            .setSubInfo(subInfo)
            .setResult(WriteResult.Result.NO_INBOX)
            .build()));
        return CompletableFuture.completedFuture(replyBuilder.build());
    }
}
