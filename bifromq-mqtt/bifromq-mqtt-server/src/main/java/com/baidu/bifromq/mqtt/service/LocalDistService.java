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

package com.baidu.bifromq.mqtt.service;

import static com.baidu.bifromq.metrics.TenantMetric.MqttTransientFanOutBytes;
import static com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil.toDelivererKey;
import static com.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientFanOutBytesPerSeconds;
import static java.util.Collections.singletonList;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.plugin.subbroker.DeliveryPack;
import com.baidu.bifromq.plugin.subbroker.DeliveryPackage;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryResults;
import com.baidu.bifromq.type.MatchInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.baidu.bifromq.util.SizeUtil;
import com.baidu.bifromq.util.TopicUtil;
import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalDistService implements ILocalDistService {
    private final String serverId;
    private final IDistClient distClient;
    private final ILocalTopicRouter localTopicRouter;
    private final IResourceThrottler resourceThrottler;
    private final ILocalSessionRegistry sessionRegistry;


    public LocalDistService(String serverId,
                            ILocalSessionRegistry sessionRegistry,
                            ILocalTopicRouter localTopicRouter,
                            IDistClient distClient,
                            IResourceThrottler resourceThrottler) {
        this.serverId = serverId;
        this.sessionRegistry = sessionRegistry;
        this.localTopicRouter = localTopicRouter;
        this.distClient = distClient;
        this.resourceThrottler = resourceThrottler;
    }

    @Override
    public CompletableFuture<MatchResult> match(long reqId, String topicFilter, IMQTTTransientSession session) {
        String tenantId = session.clientInfo().getTenantId();
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.match(reqId,
                tenantId,
                topicFilter,
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(ILocalDistService.globalize(session.channelId()), serverId), 0);
        } else {
            return localTopicRouter.addTopicRoute(reqId, tenantId, topicFilter, session.channelId());
        }
    }

    @Override
    public CompletableFuture<UnmatchResult> unmatch(long reqId, String topicFilter, IMQTTTransientSession session) {
        String tenantId = session.clientInfo().getTenantId();
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.unmatch(reqId,
                tenantId,
                topicFilter,
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(ILocalDistService.globalize(session.channelId()), serverId), 0);
        } else {
            return localTopicRouter.removeTopicRoute(reqId, tenantId, topicFilter, session.channelId());
        }
    }

    @Override
    public CompletableFuture<DeliveryReply> dist(DeliveryRequest request) {
        DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder();
        for (Map.Entry<String, DeliveryPackage> entry : request.getPackageMap().entrySet()) {
            String tenantId = entry.getKey();
            DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
            ITenantMeter tenantMeter = ITenantMeter.get(tenantId);
            boolean isFanOutThrottled = !resourceThrottler.hasResource(tenantId, TotalTransientFanOutBytesPerSeconds);
            Set<MatchInfo> ok = new HashSet<>();
            Set<MatchInfo> skip = new HashSet<>();
            Set<MatchInfo> noSub = new HashSet<>();
            long totalFanOutBytes = 0L;
            for (DeliveryPack writePack : entry.getValue().getPackList()) {
                TopicMessagePack topicMsgPack = writePack.getMessagePack();
                long msgPackSize = SizeUtil.estSizeOf(topicMsgPack);
                int fanoutScale = 1;
                boolean hasFanOutDone = false;
                for (MatchInfo matchInfo : writePack.getMatchInfoList()) {
                    if (!noSub.contains(matchInfo) && !skip.contains(matchInfo)) {
                        if (ILocalDistService.isGlobal(matchInfo.getReceiverId())) {
                            IMQTTSession session =
                                sessionRegistry.get(ILocalDistService.parseReceiverId(matchInfo.getReceiverId()));
                            if (session instanceof IMQTTTransientSession) {
                                boolean success =
                                    ((IMQTTTransientSession) session).publish(matchInfo, singletonList(topicMsgPack));
                                if (success) {
                                    ok.add(matchInfo);
                                } else {
                                    noSub.add(matchInfo);
                                }
                            } else {
                                // no session found for shared subscription
                                noSub.add(matchInfo);
                            }
                        } else {
                            if (isFanOutThrottled && hasFanOutDone) {
                                continue;
                            }
                            Optional<CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes>> routesFutureOpt =
                                localTopicRouter.getTopicRoutes(tenantId, matchInfo);
                            if (routesFutureOpt.isEmpty()) {
                                noSub.add(matchInfo);
                                continue;
                            }
                            CompletableFuture<? extends ILocalTopicRouter.ILocalRoutes> routesFuture =
                                routesFutureOpt.get();
                            if (!routesFuture.isDone() || routesFuture.isCompletedExceptionally()) {
                                // skip the matchInfo if the route is not ready
                                skip.add(matchInfo);
                                continue;
                            }
                            try {
                                ILocalTopicRouter.ILocalRoutes localRoutes = routesFuture.join();
                                if (!localRoutes.localReceiverId().equals(matchInfo.getReceiverId())) {
                                    noSub.add(matchInfo);
                                    continue;
                                }
                                boolean published = false;
                                if (!isFanOutThrottled) {
                                    fanoutScale *= localRoutes.routeList().size();
                                    for (String sessionId : localRoutes.routeList()) {
                                        // at least one session should publish the message
                                        IMQTTSession session = sessionRegistry.get(sessionId);
                                        if (session instanceof IMQTTTransientSession) {
                                            if (((IMQTTTransientSession) session)
                                                .publish(matchInfo, singletonList(topicMsgPack))) {
                                                published = true;
                                            }
                                        }
                                    }
                                } else {
                                    // send to one subscriber for each topic to make sure matchinfo not lost
                                    for (String sessionId : localRoutes.routeList()) {
                                        // at least one session should publish the message
                                        IMQTTSession session = sessionRegistry.get(sessionId);
                                        if (session instanceof IMQTTTransientSession) {
                                            if (((IMQTTTransientSession) session)
                                                .publish(matchInfo, singletonList(topicMsgPack))) {
                                                published = true;
                                                hasFanOutDone = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (published) {
                                    ok.add(matchInfo);
                                } else {
                                    noSub.add(matchInfo);
                                }
                            } catch (Throwable e) {
                                log.error("Unexpected error during local dist", e);
                                skip.add(matchInfo);
                            }
                        }
                    }
                }
                totalFanOutBytes += msgPackSize * Math.max(fanoutScale, 1);
            }
            tenantMeter.recordSummary(MqttTransientFanOutBytes, totalFanOutBytes);
            // don't include duplicated matchInfo in the result
            // treat skip as ok
            Sets.difference(Sets.union(ok, skip), noSub)
                .forEach(matchInfo -> resultsBuilder.addResult(DeliveryResult.newBuilder()
                    .setMatchInfo(matchInfo)
                    .setCode(DeliveryResult.Code.OK)
                    .build()));
            noSub.forEach(matchInfo -> resultsBuilder.addResult(DeliveryResult.newBuilder()
                .setMatchInfo(matchInfo)
                .setCode(DeliveryResult.Code.NO_SUB)
                .build()));
            replyBuilder.putResult(tenantId, resultsBuilder.build());
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }
}
