/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.server.handler;

import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basescheduler.exception.BackPressureException;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import com.baidu.bifromq.dist.server.scheduler.IUnmatchCallScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.UnmatchError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Unmatched;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnmatchReqHandler implements IDistServiceReqHandler<UnmatchRequest, UnmatchReply> {
    private final IEventCollector eventCollector;
    private final IUnmatchCallScheduler unmatchCallScheduler;

    public UnmatchReqHandler(IEventCollector eventCollector, IUnmatchCallScheduler unmatchCallScheduler) {
        this.eventCollector = eventCollector;
        this.unmatchCallScheduler = unmatchCallScheduler;
    }

    @Override
    public CompletableFuture<UnmatchReply> handle(UnmatchRequest request) {
        return unmatchCallScheduler.schedule(request)
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("Failed to exec UnsubRequest, tenantId={}, req={}", request.getTenantId(), request, e);
                    eventCollector.report(getLocal(UnmatchError.class)
                        .reqId(request.getReqId())
                        .tenantId(request.getTenantId())
                        .topicFilter(request.getTopicFilter())
                        .receiverId(request.getReceiverId())
                        .subBrokerId(request.getBrokerId())
                        .delivererKey(request.getDelivererKey())
                        .reason(e.getMessage()));
                    if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                        return UnmatchReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(UnmatchReply.Result.BACK_PRESSURE_REJECTED)
                            .build();
                    }
                    return UnmatchReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(UnmatchReply.Result.ERROR)
                        .build();
                } else {
                    if (v.getResult() == UnmatchReply.Result.OK) {
                        eventCollector.report(getLocal(Unmatched.class)
                            .reqId(request.getReqId())
                            .tenantId(request.getTenantId())
                            .topicFilter(request.getTopicFilter())
                            .receiverId(request.getReceiverId())
                            .subBrokerId(request.getBrokerId())
                            .delivererKey(request.getDelivererKey()));
                    } else {
                        eventCollector.report(getLocal(UnmatchError.class)
                            .reqId(request.getReqId())
                            .tenantId(request.getTenantId())
                            .topicFilter(request.getTopicFilter())
                            .receiverId(request.getReceiverId())
                            .subBrokerId(request.getBrokerId())
                            .delivererKey(request.getDelivererKey())
                            .reason("Internal Error"));
                    }
                }
                return v;
            });
    }

    @Override
    public void close() {
        unmatchCallScheduler.close();
    }
}
