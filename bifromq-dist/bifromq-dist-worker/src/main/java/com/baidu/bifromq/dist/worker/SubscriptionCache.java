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

import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.compare;
import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordTopicFilterPrefix;
import static com.baidu.bifromq.dist.entity.EntityUtil.parseMatchRecord;
import static com.baidu.bifromq.dist.util.TopicUtil.escape;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_MAX_CACHED_SUBS_PER_TENANT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_TOPIC_MATCH_EXPIRY;
import static com.google.common.hash.Hashing.murmur3_128;
import static java.util.Collections.singleton;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.util.TopicFilterMatcher;
import com.baidu.bifromq.dist.util.TopicTrie;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.TopicMessage;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SubscriptionCache {
    // OuterCacheKey: OrderedSharedMatchingKey(<tenantId>, <escapedTopicFilter>, <tenantVer>)
    // InnerCacheKey: ClientInfo(<tenantId>, <type>, <metadata>)
    private final LoadingCache<OrderedSharedMatchingKey, Cache<ClientInfo, NormalMatching>> orderedSharedMatching;
    private final LoadingCache<String, AsyncLoadingCache<ScopedTopic, MatchResult>> tenantCache;
    private final LoadingCache<String, AtomicLong> tenantVerCache;
    private final ThreadLocal<IKVRangeReader> threadLocalReader;
    private final ILoadEstimator loadEstimator;
    private final Timer externalMatchTimer;
    private final Timer internalMatchTimer;

    SubscriptionCache(KVRangeId id, Supplier<IKVRangeReader> rangeReaderProvider, Executor matchExecutor,
                      ILoadEstimator loadEstimator) {
        int expirySec = DIST_TOPIC_MATCH_EXPIRY.get();
        threadLocalReader = ThreadLocal.withInitial(rangeReaderProvider);
        this.loadEstimator = loadEstimator;
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
        tenantCache = Caffeine.newBuilder()
            .expireAfterAccess(expirySec * 3L, TimeUnit.SECONDS)
            .scheduler(Scheduler.systemScheduler())
            .executor(MoreExecutors.directExecutor())
            .removalListener((RemovalListener<String, AsyncLoadingCache<ScopedTopic, MatchResult>>)
                (key, value, cause) -> {
                    if (value != null) {
                        value.synchronous().invalidateAll();
                    }
                })
            .build(k -> Caffeine.newBuilder()
                .scheduler(Scheduler.systemScheduler())
                .maximumWeight(DIST_MAX_CACHED_SUBS_PER_TENANT.get())
                .weigher(new Weigher<ScopedTopic, MatchResult>() {
                    @Override
                    public @NonNegative int weigh(ScopedTopic key, MatchResult value) {
                        return value.routes.size();
                    }
                })
                .expireAfterAccess(expirySec, TimeUnit.SECONDS)
                .refreshAfterWrite(expirySec, TimeUnit.SECONDS)
                .executor(matchExecutor)
                .buildAsync(new CacheLoader<>() {
                    @Override
                    public @Nullable MatchResult load(ScopedTopic key) {
                        return match(key.tenantId, singleton(key.topic), key.matchRecordRange).get(key);
                    }

                    @Override
                    public Map<ScopedTopic, MatchResult> loadAll(Set<? extends ScopedTopic> keys) {
                        Map<String, Set<String>> topicsByTenantId = new HashMap<>();
                        Map<String, Range> matchRecordRangeByTenantId = new HashMap<>();
                        for (ScopedTopic st : keys) {
                            topicsByTenantId.computeIfAbsent(st.tenantId, k -> new HashSet<>()).add(st.topic);
                            matchRecordRangeByTenantId.computeIfAbsent(st.tenantId, k -> st.matchRecordRange);
                        }
                        return topicsByTenantId
                            .entrySet()
                            .stream()
                            .flatMap(entry -> match(entry.getKey(), entry.getValue(),
                                matchRecordRangeByTenantId.get(entry.getKey())).entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    }

                    @Override
                    public @Nullable MatchResult reload(ScopedTopic key, MatchResult oldValue) {
                        long tenantVer = tenantVerCache.get(key.tenantId).get();
                        if (oldValue.tenantVer >= tenantVer) {
                            return oldValue;
                        }
                        return match(key.tenantId, singleton(key.topic), key.matchRecordRange, tenantVer).get(
                            key);
                    }
                }));
        tenantVerCache = Caffeine.newBuilder()
            .expireAfterAccess(expirySec * 2L, TimeUnit.SECONDS)
            .scheduler(Scheduler.systemScheduler())
            .build(k -> new AtomicLong(System.nanoTime()));

        Tags tag = Tags.of("id", KVRangeIdUtil.toString(id));
        externalMatchTimer = Timer.builder("dist.match.external")
            .tags(tag)
            .register(Metrics.globalRegistry);
        internalMatchTimer = Timer.builder("dist.match.internal")
            .tags(tag)
            .register(Metrics.globalRegistry);
    }

    public CompletableFuture<Map<NormalMatching, Set<ClientInfo>>> get(ScopedTopic topic, Set<ClientInfo> senders) {
        Timer.Sample sample = Timer.start();
        return tenantCache.get(topic.tenantId).get(topic)
            .thenApply(matchResult -> {
                sample.stop(externalMatchTimer);
                Map<NormalMatching, Set<ClientInfo>> routesMap = new HashMap<>();
                for (Matching matching : matchResult.routes) {
                    NormalMatching matchedInbox;
                    if (matching instanceof NormalMatching) {
                        matchedInbox = (NormalMatching) matching;
                        // track load
                        loadEstimator.track(matchedInbox.key, LoadUnits.KEY_CACHE_HIT);
                        routesMap.put(matchedInbox, senders);
                    } else {
                        GroupMatching groupMatching = (GroupMatching) matching;
                        // track load
                        loadEstimator.track(groupMatching.key, LoadUnits.KEY_CACHE_HIT);
                        if (groupMatching.ordered) {
                            for (ClientInfo sender : senders) {
                                matchedInbox = orderedSharedMatching
                                    .get(new OrderedSharedMatchingKey(groupMatching.tenantId,
                                        groupMatching.escapedTopicFilter,
                                        matchResult.tenantVer))
                                    .get(sender, k -> {
                                        RendezvousHash<ClientInfo, NormalMatching> hash =
                                            new RendezvousHash<>(murmur3_128(),
                                                (from, into) -> into.putInt(from.hashCode()),
                                                (from, into) -> into.putBytes(from.scopedInboxId.getBytes()),
                                                Comparator.comparing(a -> a.scopedInboxId));
                                        groupMatching.inboxList.forEach(hash::add);
                                        return hash.get(k);
                                    });
                                routesMap.computeIfAbsent(matchedInbox, k -> new HashSet<>()).add(sender);
                            }
                        } else {
                            matchedInbox = groupMatching.inboxList.get(ThreadLocalRandom.current()
                                .nextInt(groupMatching.inboxList.size()));
                            routesMap.put(matchedInbox, senders);
                        }
                    }
                }
                return routesMap;
            });
    }

    // TODO: explore the possibility to invalidate all keys which matching a given wildcard topic filter
    public void touch(String tenantId) {
        tenantVerCache.get(tenantId).updateAndGet(v -> Math.max(v, System.nanoTime()));
    }

    public void invalidate(ScopedTopic topic) {
        AsyncLoadingCache<ScopedTopic, MatchResult> routeCache = tenantCache.getIfPresent(topic.tenantId);
        if (routeCache != null) {
            routeCache.synchronous().invalidate(topic);
        }
        orderedSharedMatching.invalidate(new OrderedSharedMatchingKey(topic.tenantId, escape(topic.topic),
            tenantVerCache.get(topic.tenantId).get()));
    }

    public void close() {
        tenantCache.invalidateAll();
        orderedSharedMatching.invalidateAll();
        tenantVerCache.invalidateAll();
        Metrics.globalRegistry.remove(externalMatchTimer);
        Metrics.globalRegistry.remove(internalMatchTimer);
    }

    private Map<ScopedTopic, MatchResult> match(String tenantId, Set<String> topics, Range matchRecordRange) {
        long tenantVer = tenantVerCache.get(tenantId).get();
        return match(tenantId, topics, matchRecordRange, tenantVer);
    }

    private Map<ScopedTopic, MatchResult> match(String tenantId,
                                                Set<String> topics,
                                                Range matchRecordRange,
                                                long tenantVer) {
        Timer.Sample sample = Timer.start();
        Map<ScopedTopic, MatchResult> routes = Maps.newHashMap();
        topics.forEach(topic -> routes.put(ScopedTopic.builder()
            .tenantId(tenantId)
            .topic(topic)
            .range(matchRecordRange)
            .build(), new MatchResult(tenantVer)));
        IKVRangeReader rangeReader = threadLocalReader.get();
        rangeReader.refresh();

        TopicTrie topicTrie = new TopicTrie();
        topics.forEach(topic -> topicTrie.add(topic, singleton(TopicMessage.getDefaultInstance())));
        // key: topicFilter, value: set of matched topics
        Map<String, Set<String>> matched = Maps.newHashMap();
        TopicFilterMatcher matcher = new TopicFilterMatcher(topicTrie);
        int probe = 0;
        IKVIterator itr = rangeReader.kvReader().iterator();

        // track seek
        loadEstimator.track(matchRecordRange.getStartKey(), LoadUnits.KEY_ITR_SEEK);
        itr.seek(matchRecordRange.getStartKey());
        while (itr.isValid() && compare(itr.key(), matchRecordRange.getEndKey()) < 0) {
            // track itr.key()
            loadEstimator.track(itr.key(), LoadUnits.KEY_ITR_GET);
            Matching matching = parseMatchRecord(itr.key(), itr.value());
            // key: topic
            Set<String> matchedTopics = matched.get(matching.escapedTopicFilter);
            if (matchedTopics == null) {
                Optional<Map<String, Iterable<TopicMessage>>> clientMsgs = matcher.match(matching.escapedTopicFilter);
                if (clientMsgs.isPresent()) {
                    matchedTopics = clientMsgs.get().keySet();
                    matched.put(matching.escapedTopicFilter, matchedTopics);
                    itr.next();
                    probe = 0;
                } else {
                    Optional<String> higherFilter = matcher.higher(matching.escapedTopicFilter);
                    if (higherFilter.isPresent()) {
                        // next() is much cheaper than seek(), we probe following 20 entries
                        if (probe++ < 20) {
                            // probe next
                            itr.next();
                        } else {
                            // seek to match next topic filter
                            ByteString nextMatch = matchRecordTopicFilterPrefix(tenantId, higherFilter.get());
                            loadEstimator.track(nextMatch, LoadUnits.KEY_ITR_SEEK);
                            itr.seek(nextMatch);
                        }
                        continue;
                    } else {
                        break; // no more topic filter to match, stop here
                    }
                }
            } else {
                itr.next();
            }
            matchedTopics.forEach(t -> routes.get(ScopedTopic.builder()
                    .tenantId(tenantId)
                    .topic(t)
                    .range(matchRecordRange)
                    .build())
                .routes.add(matching));
        }
        sample.stop(internalMatchTimer);
        return routes;
    }

    @AllArgsConstructor
    private static class MatchResult {
        final List<Matching> routes = new ArrayList<>();
        final long tenantVer;
    }

    private record OrderedSharedMatchingKey(String tenantId, String escapedTopicFilter, long tenantVer) {
    }
}
