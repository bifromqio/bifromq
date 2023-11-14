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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_TOPIC_MATCH_EXPIRY;
import static com.google.common.hash.Hashing.murmur3_128;

import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.worker.scheduler.DeliveryRequest;
import com.baidu.bifromq.dist.worker.scheduler.IDeliveryScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverError;
import com.baidu.bifromq.plugin.eventcollector.distservice.DeliverNoInbox;
import com.baidu.bifromq.plugin.eventcollector.distservice.Delivered;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

@Slf4j
class FanoutExecutorGroup {
    private record OrderedSharedMatchingKey(String tenantId, String escapedTopicFilter) {
    }

    // OuterCacheKey: OrderedSharedMatchingKey(<tenantId>, <escapedTopicFilter>)
    // InnerCacheKey: ClientInfo(<tenantId>, <type>, <metadata>)
    private final LoadingCache<OrderedSharedMatchingKey, Cache<ClientInfo, NormalMatching>> orderedSharedMatching;
    private final IEventCollector eventCollector;
    private final IDistClient distClient;
    private final IDeliveryScheduler scheduler;
    private final ExecutorService[] sendExecutors;

    FanoutExecutorGroup(IDeliveryScheduler scheduler,
                        IEventCollector eventCollector,
                        IDistClient distClient,
                        int groupSize) {
        int expirySec = DIST_TOPIC_MATCH_EXPIRY.get();
        this.eventCollector = eventCollector;
        this.distClient = distClient;
        this.scheduler = scheduler;
        orderedSharedMatching = Caffeine.newBuilder()
            .expireAfterAccess(expirySec * 2L, TimeUnit.SECONDS)
            .scheduler(Scheduler.systemScheduler())
            .removalListener((RemovalListener<OrderedSharedMatchingKey, Cache<ClientInfo, NormalMatching>>)
                (key, value, cause) -> {
                    if (value != null) {
                        value.invalidateAll();
                    }
                })
            .build(k -> Caffeine.newBuilder()
                .expireAfterAccess(expirySec, TimeUnit.SECONDS)
                .build());
        sendExecutors = new ExecutorService[groupSize];
        for (int i = 0; i < groupSize; i++) {
            sendExecutors[i] = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new MpscBlockingConsumerArrayQueue<>(2000),
                    EnvProvider.INSTANCE.newThreadFactory("send-executor-" + i)), "send-executor-" + i);
        }
    }

    public void shutdown() {
        for (ExecutorService executorService : sendExecutors) {
            executorService.shutdown();
        }
        orderedSharedMatching.invalidateAll();
    }

    public void submit(List<Matching> matchedRoutes, TopicMessagePack msgPack) {
        if (matchedRoutes.size() == 1) {
            send(matchedRoutes.get(0), msgPack);
        } else {
            matchedRoutes.parallelStream().forEach(matching -> send(matching, msgPack));
        }
    }

    public void invalidate(ScopedTopic scopedTopic) {
        orderedSharedMatching.invalidate(new OrderedSharedMatchingKey(scopedTopic.tenantId, escape(scopedTopic.topic)));
    }

    private void send(Matching matching, TopicMessagePack msgPack) {
        switch (matching.type()) {
            case Normal -> send((NormalMatching) matching, msgPack);
            case Group -> {
                GroupMatching groupMatching = (GroupMatching) matching;
                if (!groupMatching.ordered) {
                    // pick one route randomly
                    send(groupMatching.inboxList.get(
                        ThreadLocalRandom.current().nextInt(groupMatching.inboxList.size())), msgPack);
                } else {
                    // ordered shared subscription
                    Map<NormalMatching, TopicMessagePack.Builder> orderedRoutes = new HashMap<>();
                    for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                        ClientInfo sender = publisherPack.getPublisher();
                        NormalMatching matchedInbox = orderedSharedMatching
                            .get(new OrderedSharedMatchingKey(groupMatching.tenantId, groupMatching.escapedTopicFilter))
                            .get(sender, k -> {
                                RendezvousHash<ClientInfo, NormalMatching> hash =
                                    new RendezvousHash<>(murmur3_128(),
                                        (from, into) -> into.putInt(from.hashCode()),
                                        (from, into) -> into.putBytes(from.scopedInboxId.getBytes()),
                                        Comparator.comparing(a -> a.scopedInboxId));
                                groupMatching.inboxList.forEach(hash::add);
                                return hash.get(k);
                            });
                        // ordered share sub
                        orderedRoutes.computeIfAbsent(matchedInbox, k -> TopicMessagePack.newBuilder())
                            .setTopic(msgPack.getTopic())
                            .addMessage(publisherPack);
                    }
                    orderedRoutes.forEach((route, msgPackBuilder) -> send(route, msgPackBuilder.build()));
                }
            }
        }
    }

    private void send(NormalMatching route, TopicMessagePack msgPack) {
        int idx = route.hashCode() % sendExecutors.length;
        if (idx < 0) {
            idx += sendExecutors.length;
        }
        sendExecutors[idx].submit(() -> sendDirectly(route, msgPack));
    }

    private void sendDirectly(NormalMatching matched, TopicMessagePack msgPack) {
        int subBrokerId = matched.subBrokerId;
        String delivererKey = matched.delivererKey;
        SubInfo sub = matched.subInfo;
        DeliveryRequest request = new DeliveryRequest(sub, subBrokerId, delivererKey, msgPack);
        scheduler.schedule(request).whenComplete((result, e) -> {
            if (e != null) {
                log.debug("Failed to deliver", e);
                eventCollector.report(getLocal(DeliverError.class)
                    .brokerId(subBrokerId)
                    .delivererKey(delivererKey)
                    .subInfo(sub)
                    .messages(msgPack));
            } else {
                switch (result) {
                    case OK -> eventCollector.report(getLocal(Delivered.class)
                        .brokerId(subBrokerId)
                        .delivererKey(delivererKey)
                        .subInfo(sub)
                        .messages(msgPack));
                    case NO_INBOX -> {
                        // unsub as side effect
                        SubInfo subInfo = matched.subInfo;
                        distClient.unmatch(System.nanoTime(),
                            subInfo.getTenantId(),
                            subInfo.getTopicFilter(),
                            subInfo.getInboxId(),
                            delivererKey,
                            subBrokerId);
                        eventCollector.report(getLocal(DeliverNoInbox.class)
                            .brokerId(subBrokerId)
                            .delivererKey(delivererKey)
                            .subInfo(sub)
                            .messages(msgPack));
                    }
                    case FAILED -> eventCollector.report(getLocal(DeliverError.class)
                        .brokerId(subBrokerId)
                        .delivererKey(delivererKey)
                        .subInfo(sub)
                        .messages(msgPack));
                }
            }
        });
    }
}
