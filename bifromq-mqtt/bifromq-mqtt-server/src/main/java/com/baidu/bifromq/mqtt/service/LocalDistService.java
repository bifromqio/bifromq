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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.MQTT_DELIVERERS_PER_SERVER;
import static java.util.Collections.singletonList;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.metrics.ITenantMeter;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LocalDistService implements ILocalDistService {
    static final int TOPIC_FILTER_BUCKET_NUM = MQTT_DELIVERERS_PER_SERVER.get();

    private record TopicFilter(String tenantId, String topicFilter, int bucketId) {
    }

    private static class LocalRoutes {
        private final String localizedReceiverId;
        public final Map<String, IMQTTTransientSession> routeList = new ConcurrentHashMap<>();

        private LocalRoutes(int bucketId) {
            this.localizedReceiverId = ILocalDistService.localize(bucketId + "_" + System.nanoTime());
        }

        public String localizedReceiverId() {
            return localizedReceiverId;
        }

        public static int parseBucketId(String localizedReceiverId) {
            String receiverId = ILocalDistService.parseReceiverId(localizedReceiverId);
            return Integer.parseInt(receiverId.substring(0, receiverId.indexOf('_')));
        }
    }

    private final IDistClient distClient;
    private final String serverId;

    private final ConcurrentMap<String, IMQTTTransientSession> sessionMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<TopicFilter, CompletableFuture<LocalRoutes>> routeMap = new ConcurrentHashMap<>();

    public LocalDistService(String serverId, IDistClient distClient) {
        this.serverId = serverId;
        this.distClient = distClient;
    }

    private static class AddRouteException extends RuntimeException {
        final MatchResult matchResult;

        private AddRouteException(MatchResult matchResult) {
            this.matchResult = matchResult;
        }
    }

    @Override
    public CompletableFuture<MatchResult> match(long reqId, String topicFilter, IMQTTTransientSession session) {
        sessionMap.put(session.channelId(), session);
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.match(reqId,
                session.clientInfo().getTenantId(),
                topicFilter,
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(ILocalDistService.globalize(session.channelId()), serverId), 0);
        } else {
            int bucketId = topicFilterBucketId(session.channelId());
            CompletableFuture<LocalRoutes> toReturn =
                routeMap.compute(new TopicFilter(session.clientInfo().getTenantId(), topicFilter, bucketId), (k, v) -> {
                    if (v == null || v.isCompletedExceptionally()) {
                        LocalRoutes localRoutes = new LocalRoutes(k.bucketId);
                        return distClient.match(reqId,
                                k.tenantId,
                                k.topicFilter,
                                localRoutes.localizedReceiverId(),
                                toDelivererKey(localRoutes.localizedReceiverId(), serverId), 0)
                            .thenApply(matchResult -> {
                                if (matchResult == MatchResult.OK) {
                                    localRoutes.routeList.put(session.channelId(), session);
                                    return localRoutes;
                                }
                                throw new AddRouteException(matchResult);
                            });
                    } else {
                        CompletableFuture<LocalRoutes> updated = new CompletableFuture<>();
                        v.whenComplete((routeList, e) -> {
                            if (e != null) {
                                updated.completeExceptionally(e);
                            } else {
                                routeList.routeList.put(session.channelId(), session);
                                updated.complete(routeList);
                            }
                        });
                        return updated;
                    }
                });
            return toReturn
                .handle((routeList, e) -> {
                    if (e != null) {
                        routeMap.remove(new TopicFilter(session.clientInfo().getTenantId(), topicFilter, bucketId),
                            toReturn);
                        if (e instanceof AddRouteException) {
                            return ((AddRouteException) e).matchResult;
                        }
                        return MatchResult.ERROR;
                    } else {
                        return MatchResult.OK;
                    }
                });
        }
    }

    private static class RemoveRouteException extends RuntimeException {
        final UnmatchResult unmatchResult;

        private RemoveRouteException(UnmatchResult unmatchResult) {
            this.unmatchResult = unmatchResult;
        }
    }

    @Override
    public CompletableFuture<UnmatchResult> unmatch(long reqId, String topicFilter,
                                                    IMQTTTransientSession session) {
        sessionMap.remove(session.channelId(), session);
        if (TopicUtil.isSharedSubscription(topicFilter)) {
            return distClient.unmatch(reqId,
                session.clientInfo().getTenantId(),
                topicFilter,
                ILocalDistService.globalize(session.channelId()),
                toDelivererKey(ILocalDistService.globalize(session.channelId()), serverId), 0);
        } else {
            int bucketId = topicFilterBucketId(session.channelId());
            CompletableFuture<LocalRoutes> toReturn =
                routeMap.compute(new TopicFilter(session.clientInfo().getTenantId(), topicFilter, bucketId), (k, v) -> {
                    if (v != null) {
                        CompletableFuture<LocalRoutes> updated = new CompletableFuture<>();
                        v.whenComplete((localRoutes, e) -> {
                            if (e != null) {
                                updated.completeExceptionally(e);
                            } else {
                                localRoutes.routeList.remove(session.channelId(), session);
                                if (localRoutes.routeList.isEmpty()) {
                                    distClient.unmatch(reqId,
                                            k.tenantId,
                                            k.topicFilter,
                                            localRoutes.localizedReceiverId(),
                                            toDelivererKey(localRoutes.localizedReceiverId(), serverId), 0)
                                        .whenComplete((unmatchResult, t) -> {
                                            if (t != null) {
                                                updated.completeExceptionally(t);
                                            } else {
                                                // we use exception to return the dist unmatch call result
                                                updated.completeExceptionally(new RemoveRouteException(unmatchResult));
                                            }
                                        });
                                } else {
                                    updated.complete(localRoutes);
                                }
                            }
                        });
                        return updated;
                    }
                    return null;
                });
            if (toReturn == null) {
                // no route found
                return CompletableFuture.completedFuture(UnmatchResult.OK);
            }
            return toReturn
                .handle((r, e) -> {
                    if (e != null) {
                        routeMap.remove(new TopicFilter(session.clientInfo().getTenantId(), topicFilter,
                                bucketId),
                            toReturn);
                        if (e instanceof RemoveRouteException) {
                            // we use exception to return the unmatch result
                            return ((RemoveRouteException) e).unmatchResult;
                        }
                        // if any exception occurs, we treat it as an error
                        return UnmatchResult.ERROR;
                    } else {
                        return UnmatchResult.OK;
                    }
                });
        }
    }

    @Override
    public CompletableFuture<DeliveryReply> dist(DeliveryRequest request) {
        DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder();
        Set<MatchInfo> ok = new HashSet<>();
        Set<MatchInfo> skip = new HashSet<>();
        Set<MatchInfo> noSub = new HashSet<>();
        DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
        for (Map.Entry<String, DeliveryPackage> entry : request.getPackageMap().entrySet()) {
            String tenantId = entry.getKey();
            ITenantMeter tenantMeter = ITenantMeter.get(tenantId);
            for (DeliveryPack writePack : entry.getValue().getPackList()) {
                TopicMessagePack topicMsgPack = writePack.getMessagePack();
                int msgPackSize = SizeUtil.estSizeOf(topicMsgPack);
                int fanout = 1;
                for (MatchInfo matchInfo : writePack.getMatchInfoList()) {
                    if (!noSub.contains(matchInfo) && !skip.contains(matchInfo)) {
                        if (ILocalDistService.isGlobal(matchInfo.getReceiverId())) {
                            IMQTTTransientSession session =
                                sessionMap.get(ILocalDistService.parseReceiverId(matchInfo.getReceiverId()));
                            if (session != null) {
                                boolean success = session.publish(matchInfo, singletonList(topicMsgPack));
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
                            int bucketId = LocalRoutes.parseBucketId(matchInfo.getReceiverId());
                            CompletableFuture<LocalRoutes> routesFuture =
                                routeMap.get(new TopicFilter(tenantId, matchInfo.getTopicFilter(),
                                    bucketId));
                            if (routesFuture == null) {
                                noSub.add(matchInfo);
                                continue;
                            }
                            if (!routesFuture.isDone() || routesFuture.isCompletedExceptionally()) {
                                skip.add(matchInfo);
                            }
                            try {
                                LocalRoutes localRoutes = routesFuture.join();
                                if (!localRoutes.localizedReceiverId().equals(matchInfo.getReceiverId())) {
                                    noSub.add(matchInfo);
                                    continue;
                                }
                                boolean published = false;
                                fanout *= localRoutes.routeList.size();
                                for (IMQTTTransientSession session : localRoutes.routeList.values()) {
                                    // at least one session should publish the message
                                    if (session.publish(matchInfo, singletonList(topicMsgPack))) {
                                        published = true;
                                    }
                                }
                                if (published) {
                                    ok.add(matchInfo);
                                } else {
                                    noSub.add(matchInfo);
                                }
                            } catch (Throwable e) {
                                skip.add(matchInfo);
                            }
                        }
                    }
                }
                tenantMeter.recordSummary(MqttTransientFanOutBytes, msgPackSize * Math.max(fanout, 1));
            }
            ok.forEach(matchInfo -> resultsBuilder.addResult(DeliveryResult.newBuilder()
                .setMatchInfo(matchInfo)
                .setCode(DeliveryResult.Code.OK)
                .build()));
            skip.forEach(matchInfo -> resultsBuilder.addResult(DeliveryResult.newBuilder()
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

    private int topicFilterBucketId(String key) {
        int bucketId = key.hashCode() % TOPIC_FILTER_BUCKET_NUM;
        if (bucketId < 0) {
            bucketId =
                (bucketId + Runtime.getRuntime().availableProcessors()) % TOPIC_FILTER_BUCKET_NUM;
        }
        return bucketId;
    }

}
