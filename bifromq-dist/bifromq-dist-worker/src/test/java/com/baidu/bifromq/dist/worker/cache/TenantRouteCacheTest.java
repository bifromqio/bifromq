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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.util.TopicUtil.unescape;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.entity.GroupMatching;
import com.baidu.bifromq.dist.entity.Matching;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.worker.MeterTest;
import com.baidu.bifromq.metrics.TenantMetric;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TenantRouteCacheTest extends MeterTest {

    private TenantRouteCache cache;
    private Ticker mockTicker;
    private ITenantRouteMatcher mockMatcher;
    private String tenantId;
    private Duration expiryDuration;

    @BeforeMethod
    public void setup() {
        super.setup();
        tenantId = "tenant1";
        mockTicker = mock(Ticker.class);
        mockMatcher = mock(ITenantRouteMatcher.class);
        expiryDuration = Duration.ofMinutes(1);
        cache = new TenantRouteCache(tenantId, mockMatcher, expiryDuration);
    }

    @AfterMethod
    public void tearDown() {
        cache.destroy();
        super.tearDown();
    }

    @Test
    public void get() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Set.of(normalMatching));

        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));
        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertNotNull(cachedMatchings);
        assertTrue(cachedMatchings.contains(normalMatching));
    }

    @Test
    public void addAllMatch() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Sets.newHashSet(normalMatching));

        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertTrue(cachedMatchings.contains(normalMatching));

        Matching normalMatching1 = mock(Matching.class);
        when(normalMatching1.type()).thenReturn(Matching.Type.Normal);

        Matching normalMatching2 = mock(Matching.class);
        when(normalMatching2.type()).thenReturn(Matching.Type.Normal);

        Map<String, Set<Matching>> newMatches = new HashMap<>();
        newMatches.put("#", Set.of(normalMatching1, normalMatching2));
        cache.addAllMatch(newMatches);

        cachedMatchings = cache.getIfPresent(topic, FULL_BOUNDARY);
        assertEquals(cachedMatchings.size(), 3);
    }

    @Test
    public void addMatchToEmptyCache() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);

        Map<String, Set<Matching>> newMatches = new HashMap<>();
        newMatches.put("#", Set.of(normalMatching));
        cache.addAllMatch(newMatches);

        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));

        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Collections.emptySet());
        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertTrue(cachedMatchings.isEmpty());
    }

    @Test
    public void removeAllMatch() {
        String topicFilter = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);

        Matching normalMatching1 = mock(Matching.class);
        when(normalMatching1.type()).thenReturn(Matching.Type.Normal);

        when(mockMatcher.match(eq(topicFilter), eq(FULL_BOUNDARY))).thenReturn(
            Sets.newHashSet(normalMatching, normalMatching1));

        Set<Matching> cachedMatchings = cache.get(topicFilter, FULL_BOUNDARY);
        assertTrue(cachedMatchings.contains(normalMatching));
        assertTrue(cachedMatchings.contains(normalMatching1));

        Map<String, Set<Matching>> obsoleteMatches = new HashMap<>();
        obsoleteMatches.put("#", Set.of(normalMatching, normalMatching1));
        cache.removeAllMatch(obsoleteMatches);

        cachedMatchings = cache.getIfPresent(topicFilter, FULL_BOUNDARY);
        assertTrue(cachedMatchings.isEmpty());
    }

    @Test
    public void removeMatchFromEmptyCache() {
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);

        Map<String, Set<Matching>> obsoleteMatches = new HashMap<>();
        obsoleteMatches.put("#", Set.of(normalMatching));
        cache.removeAllMatch(obsoleteMatches);

        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Collections.emptySet());
        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertTrue(cachedMatchings.isEmpty());
    }

    @Test
    public void groupMatchingAdd() {
        String topic = "home/sensor/temperature";
        String tenantId = "tenant1";
        String topicFilter = "$oshare/group1/home/sensor/temperature";

        ByteString key = EntityUtil.toGroupMatchRecordKey(tenantId, topicFilter);
        GroupMatchRecord matchRecord = GroupMatchRecord.newBuilder()
            .addQReceiverId(toQInboxId(1, "inbox1", "deliverer1"))
            .build();

        GroupMatching groupMatching = (GroupMatching) EntityUtil.parseMatchRecord(key, matchRecord.toByteString());

        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Sets.newHashSet(groupMatching));
        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertNotNull(cachedMatchings);
        assertTrue(cachedMatchings.contains(groupMatching));

        Map<String, Set<Matching>> newMatches = new HashMap<>();
        matchRecord = GroupMatchRecord.newBuilder()
            .addQReceiverId(toQInboxId(1, "inbox2", "deliverer1"))
            .build();
        groupMatching = (GroupMatching) EntityUtil.parseMatchRecord(key, matchRecord.toByteString());

        newMatches.put(unescape(groupMatching.escapedTopicFilter), Set.of(groupMatching));
        cache.addAllMatch(newMatches);

        cachedMatchings = cache.getIfPresent(topic, FULL_BOUNDARY);
        GroupMatching cachedGroupMatching = (GroupMatching) cachedMatchings.iterator().next();
        assertEquals(cachedGroupMatching.receiverIds.size(), 2);
    }

    @Test
    public void groupMatchingRemove() {
        String topic = "home/sensor/temperature";
        String tenantId = "tenant1";
        String topicFilter = "$oshare/group1/home/sensor/temperature";

        ByteString key = EntityUtil.toGroupMatchRecordKey(tenantId, topicFilter);
        GroupMatchRecord matchRecord = GroupMatchRecord.newBuilder()
            .addQReceiverId(toQInboxId(1, "inbox1", "deliverer1"))
            .build();

        GroupMatching groupMatching = (GroupMatching) EntityUtil.parseMatchRecord(key, matchRecord.toByteString());

        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Sets.newHashSet(groupMatching));
        Set<Matching> cachedMatchings = cache.get(topic, FULL_BOUNDARY);
        assertNotNull(cachedMatchings);
        assertTrue(cachedMatchings.contains(groupMatching));

        Map<String, Set<Matching>> delMatches = new HashMap<>();
        matchRecord = GroupMatchRecord.newBuilder()
            .addQReceiverId(toQInboxId(1, "inbox1", "deliverer1"))
            .build();
        groupMatching = (GroupMatching) EntityUtil.parseMatchRecord(key, matchRecord.toByteString());

        delMatches.put(unescape(groupMatching.escapedTopicFilter), Set.of(groupMatching));
        cache.removeAllMatch(delMatches);

        cachedMatchings = cache.getIfPresent(topic, FULL_BOUNDARY);
        assertTrue(cachedMatchings.isEmpty());
    }

    @Test
    public void refreshExpiryOnGet() {
        when(mockTicker.read()).thenReturn(0L);
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, Duration.ofSeconds(5), mockTicker);
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Set.of(normalMatching));
        assertNotNull(cache.get(topic, FULL_BOUNDARY));

        when(mockTicker.read()).thenReturn(Duration.ofSeconds(4L).toNanos());
        assertNotNull(cache.getIfPresent(topic, FULL_BOUNDARY));

        when(mockTicker.read()).thenReturn(Duration.ofSeconds(8L).toNanos());
        assertNotNull(cache.getIfPresent(topic, FULL_BOUNDARY));

        when(mockTicker.read()).thenReturn(Duration.ofSeconds(20L).toNanos());
        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));

        verify(mockMatcher, times(1)).match(anyString(), any());
        cache.destroy();
    }

    @Test
    public void noRefreshExpiryOnAdd() {
        when(mockTicker.read()).thenReturn(0L);
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, Duration.ofSeconds(5), mockTicker);
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Sets.newHashSet(normalMatching));
        assertNotNull(cache.get(topic, FULL_BOUNDARY));
        when(mockTicker.read()).thenReturn(Duration.ofSeconds(4).toNanos());

        Matching normalMatching1 = mock(Matching.class);
        when(normalMatching1.type()).thenReturn(Matching.Type.Normal);
        Map<String, Set<Matching>> newMatches = new HashMap<>();
        newMatches.put("#", Set.of(normalMatching1));
        cache.addAllMatch(newMatches);

        when(mockTicker.read()).thenReturn(Duration.ofSeconds(6).toNanos());
        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));
        cache.destroy();
    }

    @Test
    public void noRefreshExpiryOnRemove() {
        when(mockTicker.read()).thenReturn(0L);
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, Duration.ofSeconds(5), mockTicker);
        String topic = "home/sensor/temperature";
        Matching normalMatching = mock(Matching.class);
        when(normalMatching.type()).thenReturn(Matching.Type.Normal);
        when(mockMatcher.match(eq(topic), eq(FULL_BOUNDARY))).thenReturn(Sets.newHashSet(normalMatching));
        assertNotNull(cache.get(topic, FULL_BOUNDARY));
        when(mockTicker.read()).thenReturn(Duration.ofSeconds(4).toNanos());

        Map<String, Set<Matching>> obsoleteMatches = new HashMap<>();
        obsoleteMatches.put("#", Set.of(normalMatching));
        cache.removeAllMatch(obsoleteMatches);

        when(mockTicker.read()).thenReturn(Duration.ofSeconds(6).toNanos());
        assertNull(cache.getIfPresent(topic, FULL_BOUNDARY));
        cache.destroy();
    }

    @Test
    void testDestroy() {
        TenantRouteCache cache = new TenantRouteCache(tenantId, mockMatcher, expiryDuration);
        assertGauge(tenantId, TenantMetric.MqttRouteCacheSize);
        cache.destroy();
        assertNoGauge(tenantId, TenantMetric.MqttRouteCacheSize);
    }
}