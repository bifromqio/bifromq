/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.worker;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.plugin.subbroker.ISubBrokerManager;
import com.baidu.bifromq.type.MatchInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SubscriptionCleaner implements ISubscriptionCleaner {
    private final ISubBrokerManager subBrokerManager;
    private final IDistClient distClient;

    SubscriptionCleaner(ISubBrokerManager subBrokerManager, IDistClient distClient) {
        this.subBrokerManager = subBrokerManager;
        this.distClient = distClient;
    }

    @Override
    public CompletableFuture<Void> sweep(int subBrokerId, CheckRequest request) {
        long reqId = System.nanoTime();
        return subBrokerManager.get(subBrokerId)
            .check(request)
            .thenCompose(checkReply -> {
                assert checkReply.getCodeCount() == request.getMatchInfoCount();
                List<CompletableFuture<UnmatchResult>> futures = new ArrayList<>();
                for (int i = 0; i < request.getMatchInfoCount(); i++) {
                    MatchInfo matchInfo = request.getMatchInfo(i);
                    CheckReply.Code code = checkReply.getCode(i);
                    switch (code) {
                        case NO_SUB -> {
                            log.debug("No sub found: tenantId={}, topicFilter={}, receiverId={}, subBrokerId={}",
                                request.getTenantId(), matchInfo.getTopicFilter(),
                                matchInfo.getReceiverId(), subBrokerId);
                            futures.add(distClient.removeTopicMatch(reqId, request.getTenantId(),
                                matchInfo.getTopicFilter(), matchInfo.getReceiverId(),
                                request.getDelivererKey(), subBrokerId, matchInfo.getIncarnation()));
                        }
                        case NO_RECEIVER -> {
                            log.debug(
                                "No receiverInfo found: tenantId={}, topicFilter={}, receiverId={}, subBrokerId={}",
                                request.getTenantId(), matchInfo.getTopicFilter(),
                                matchInfo.getReceiverId(), subBrokerId);
                            futures.add(distClient.removeTopicMatch(reqId, request.getTenantId(),
                                matchInfo.getTopicFilter(), matchInfo.getReceiverId(),
                                request.getDelivererKey(), subBrokerId, matchInfo.getIncarnation()));
                        }
                        default -> {
                            // do nothing
                        }
                    }
                }
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            });
    }
}
