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

package com.baidu.bifromq.dist.worker.cache;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.worker.TopicIndex;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.github.benmanes.caffeine.cache.Weigher;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.checkerframework.checker.index.qual.NonNegative;

class TenantRouteCache implements ITenantRouteCache {
    @EqualsAndHashCode
    @AllArgsConstructor
    private static class RouteCacheKey {
        final String topic;
        final Boundary matchRecordBoundary;
        @EqualsAndHashCode.Exclude
        final boolean refreshExpiry;
    }

    private final String tenantId;
    private final ITenantRouteMatcher matcher;
    private final Cache<RouteCacheKey, Set<Matching>> routesCache;
    private final TopicIndex<RouteCacheKey> index;
    private final StampedLock stampedLock = new StampedLock();

    TenantRouteCache(String tenantId, ITenantRouteMatcher matcher, Duration expiryAfterAccess) {
        this(tenantId, matcher, expiryAfterAccess, Ticker.systemTicker());
    }

    TenantRouteCache(String tenantId, ITenantRouteMatcher matcher, Duration expiryAfterAccess, Ticker ticker) {
        this.tenantId = tenantId;
        this.matcher = matcher;
        index = new TopicIndex<>();
        routesCache = Caffeine.newBuilder()
            .ticker(ticker)
            .scheduler(Scheduler.systemScheduler())
            .maximumWeight(DistMaxCachedRoutesPerTenant.INSTANCE.get())
            .weigher(new Weigher<RouteCacheKey, Set<Matching>>() {
                @Override
                public @NonNegative int weigh(RouteCacheKey key, Set<Matching> value) {
                    return value.size();
                }
            })
            .expireAfter(new Expiry<RouteCacheKey, Set<Matching>>() {
                @Override
                public long expireAfterCreate(RouteCacheKey key, Set<Matching> matchings, long currentTime) {
                    return expiryAfterAccess.toNanos();
                }

                @Override
                public long expireAfterUpdate(RouteCacheKey key, Set<Matching> matchings, long currentTime,
                                              @NonNegative long currentDuration) {
                    return expiryAfterAccess.toNanos();
                }

                @Override
                public long expireAfterRead(RouteCacheKey key, Set<Matching> matchings, long currentTime,
                                            @NonNegative long currentDuration) {
                    if (key.refreshExpiry) {
                        return expiryAfterAccess.toNanos();
                    }
                    return currentDuration;
                }
            })
            .evictionListener((key, value, cause) -> {
                if (key != null) {
                    index.remove(key.topic, key);
                }
            })
            .build();
        ITenantMeter.gauging(tenantId, TenantMetric.MqttRouteCacheSize, routesCache::estimatedSize);
    }

    @Override
    public void addAllMatch(Map<String, Set<Matching>> newMatches) {
        long stamp = stampedLock.writeLock();
        try {
            for (Map.Entry<String, Set<Matching>> entry : newMatches.entrySet()) {
                String topicFilter = entry.getKey();
                for (RouteCacheKey cacheKey : index.match(topicFilter)) {
                    for (Matching matching : entry.getValue()) {
                        // indexed key will not refresh expiry
                        Set<Matching> cachedMatching = routesCache.getIfPresent(cacheKey);
                        if (cachedMatching != null) {
                            switch (matching.type()) {
                                case Normal -> cachedMatching.add(matching);
                                case Group -> {
                                    GroupMatching newGroupMatching = (GroupMatching) matching;
                                    boolean found = false;
                                    for (Matching m : cachedMatching) {
                                        if (m.type() == Matching.Type.Group) {
                                            GroupMatching cachedGroupMatching = (GroupMatching) m;
                                            if (cachedGroupMatching.originalTopicFilter()
                                                .equals(newGroupMatching.originalTopicFilter())) {
                                                cachedGroupMatching.addAll(newGroupMatching.receiverIds);
                                                found = true;
                                                break;
                                            }
                                        }
                                    }
                                    if (!found) {
                                        cachedMatching.add(matching);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeAllMatch(Map<String, Set<Matching>> obsoleteMatches) {
        long stamp = stampedLock.writeLock();
        try {
            for (Map.Entry<String, Set<Matching>> entry : obsoleteMatches.entrySet()) {
                String topicFilter = entry.getKey();
                for (RouteCacheKey cacheKey : index.match(topicFilter)) {
                    for (Matching matching : entry.getValue()) {
                        // indexed key will not refresh expiry
                        Set<Matching> cachedMatching = routesCache.getIfPresent(cacheKey);
                        if (cachedMatching != null) {
                            // remove matching from cache
                            switch (matching.type()) {
                                case Normal -> cachedMatching.remove(matching);
                                case Group -> {
                                    GroupMatching obsoleteGroupMatching = (GroupMatching) matching;
                                    cachedMatching.removeIf(m -> {
                                        if (m.type() == Matching.Type.Group) {
                                            GroupMatching cachedGroupMatching = ((GroupMatching) m);
                                            if (cachedGroupMatching.originalTopicFilter()
                                                .equals(obsoleteGroupMatching.originalTopicFilter())) {
                                                cachedGroupMatching.removeAll(obsoleteGroupMatching.receiverIds);
                                                return cachedGroupMatching.receiverIds.isEmpty();
                                            }
                                        }
                                        return false;
                                    });
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public Set<Matching> getIfPresent(String topic, Boundary matchRecordRange) {
        return routesCache.getIfPresent(new RouteCacheKey(topic, matchRecordRange, true));
    }

    @Override
    public Set<Matching> get(String topic, Boundary matchRecordRange) {
        Set<Matching> cachedMatchings = getIfPresent(topic, matchRecordRange);
        if (cachedMatchings != null) {
            return cachedMatchings;
        }
        long stamp = stampedLock.readLock();
        try {
            ITenantMeter.get(tenantId).recordCount(TenantMetric.MqttRouteCacheMissCount);
            Set<Matching> matchings = matcher.match(topic, matchRecordRange);
            routesCache.put(new RouteCacheKey(topic, matchRecordRange, true), matchings);
            index.add(topic, new RouteCacheKey(topic, matchRecordRange, false));
            return matchings;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public void destroy() {
        ITenantMeter.stopGauging(tenantId, TenantMetric.MqttRouteCacheSize);
    }
}
