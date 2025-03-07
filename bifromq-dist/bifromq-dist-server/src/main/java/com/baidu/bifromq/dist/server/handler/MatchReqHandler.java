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
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.baidu.bifromq.dist.server.scheduler.IMatchCallScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.MatchError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Matched;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MatchReqHandler implements IDistServiceReqHandler<MatchRequest, MatchReply> {
    private final IEventCollector eventCollector;
    private final IMatchCallScheduler matchCallScheduler;

    public MatchReqHandler(IEventCollector eventCollector, IMatchCallScheduler matchCallScheduler) {
        this.eventCollector = eventCollector;
        this.matchCallScheduler = matchCallScheduler;
    }

    @Override
    public CompletableFuture<MatchReply> handle(MatchRequest request) {
        return matchCallScheduler.schedule(request)
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("Failed to exec SubRequest, tenantId={}, req={}", request.getTenantId(), request, e);
                    eventCollector.report(getLocal(MatchError.class)
                        .reqId(request.getReqId())
                        .tenantId(request.getTenantId())
                        .topicFilter(request.getTopicFilter())
                        .receiverId(request.getReceiverId())
                        .subBrokerId(request.getBrokerId())
                        .delivererKey(request.getDelivererKey())
                        .reason(e.getMessage()));
                    if (e instanceof BackPressureException || e.getCause() instanceof BackPressureException) {
                        return MatchReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setResult(MatchReply.Result.BACK_PRESSURE_REJECTED)
                            .build();
                    }
                    return MatchReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(MatchReply.Result.ERROR)
                        .build();
                } else {
                    switch (v.getResult()) {
                        case OK -> eventCollector.report(getLocal(Matched.class)
                            .reqId(request.getReqId())
                            .tenantId(request.getTenantId())
                            .topicFilter(request.getTopicFilter())
                            .receiverId(request.getReceiverId())
                            .subBrokerId(request.getBrokerId())
                            .delivererKey(request.getDelivererKey()));
                        case EXCEED_LIMIT -> eventCollector.report(getLocal(MatchError.class)
                            .reqId(request.getReqId())
                            .tenantId(request.getTenantId())
                            .topicFilter(request.getTopicFilter())
                            .receiverId(request.getReceiverId())
                            .subBrokerId(request.getBrokerId())
                            .delivererKey(request.getDelivererKey())
                            .reason(v.getResult().name()));
                        default -> eventCollector.report(getLocal(MatchError.class)
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
        matchCallScheduler.close();
    }
}
