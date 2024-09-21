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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.dist.entity.Matching;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionCacheTest {

    private SubscriptionCache cache;
    private ITenantRouteCacheFactory tenantRouteCacheFactoryMock;
    private ITenantRouteCache tenantRouteCacheMock;
    private Supplier<IKVCloseableReader> readerSupplierMock;
    private KVRangeId kvRangeIdMock;
    private Executor matchExecutor;
    private Ticker tickerMock;

    @BeforeMethod
    public void setUp() {
        tenantRouteCacheFactoryMock = mock(ITenantRouteCacheFactory.class);
        tenantRouteCacheMock = mock(ITenantRouteCache.class);
        readerSupplierMock = mock(Supplier.class);
        kvRangeIdMock = mock(KVRangeId.class);
        matchExecutor = Executors.newSingleThreadExecutor();
        tickerMock = mock(Ticker.class);

        when(tenantRouteCacheFactoryMock.create(anyString())).thenReturn(tenantRouteCacheMock);
        when(tenantRouteCacheFactoryMock.expiry()).thenReturn(Duration.ofMinutes(10));

        cache = new SubscriptionCache(kvRangeIdMock, tenantRouteCacheFactoryMock, matchExecutor, tickerMock);
        cache.reset(FULL_BOUNDARY);
    }

    @Test
    public void getCacheHit() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getIfPresent(eq(topic), any(Boundary.class))).thenReturn(mockMatchings);

        CompletableFuture<Set<Matching>> result = cache.get(tenantId, topic);
        assertNotNull(result);
        assertTrue(result.isDone());
        assertEquals(mockMatchings, result.join());
    }

    @Test
    public void getCacheMiss() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getIfPresent(eq(topic), any(Boundary.class))).thenReturn(null);
        when(tenantRouteCacheMock.get(eq(topic), any(Boundary.class))).thenReturn(mockMatchings);

        CompletableFuture<Set<Matching>> result = cache.get(tenantId, topic);
        assertNotNull(result);
        assertFalse(result.isDone());

        Set<Matching> resultSet = result.join();
        assertEquals(mockMatchings, resultSet);
        verify(tenantRouteCacheMock).get(eq(topic), any(Boundary.class));
    }

    @Test
    public void addAllMatch() {
        String tenantId = "tenant1";
        Map<String, Set<Matching>> matchings = new HashMap<>();
        Map<String, Map<String, Set<Matching>>> matchesByTenant = new HashMap<>();
        matchesByTenant.put(tenantId, matchings);

        when(tenantRouteCacheFactoryMock.create(tenantId)).thenReturn(tenantRouteCacheMock);
        cache.addAllMatch(matchesByTenant);

        verify(tenantRouteCacheMock, never()).addAllMatch(matchings);
    }

    @Test
    public void removeAllMatch() {
        String tenantId = "tenant1";
        Map<String, Set<Matching>> matchings = new HashMap<>();
        Map<String, Map<String, Set<Matching>>> matchesByTenant = new HashMap<>();
        matchesByTenant.put(tenantId, matchings);

        when(tenantRouteCacheFactoryMock.create(tenantId)).thenReturn(tenantRouteCacheMock);
        cache.removeAllMatch(matchesByTenant);

        verify(tenantRouteCacheMock, never()).removeAllMatch(matchings);
    }

    @Test
    public void cacheExpiry() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getIfPresent(eq(topic), any(Boundary.class))).thenReturn(mockMatchings);

        cache.get(tenantId, topic);

        long expiryNanos = Duration.ofMinutes(20).toNanos();
        when(tickerMock.read()).thenReturn(0L).thenReturn(expiryNanos);

        CompletableFuture<Set<Matching>> result = cache.get(tenantId, topic);
        assertNotNull(result);
        assertTrue(result.isDone());
        verify(tenantRouteCacheMock, times(2)).getIfPresent(eq(topic), any(Boundary.class));
    }

    @Test
    public void resetBoundary() {
        String tenantId = "tenant1";
        String topic = "home/sensor/temperature";

        Set<Matching> mockMatchings = new HashSet<>();
        when(tenantRouteCacheMock.getIfPresent(eq(topic), any(Boundary.class))).thenReturn(mockMatchings);
        cache.get(tenantId, topic);

        Boundary boundary = Boundary.newBuilder().setStartKey(ByteString.copyFromUtf8("start"))
            .setEndKey(ByteString.copyFromUtf8("end")).build();
        cache.reset(boundary);

        CompletableFuture<Set<Matching>> result = cache.get("tenant1", "home/sensor/temperature");
        assertNotNull(result);
        verify(tenantRouteCacheMock, times(2)).getIfPresent(eq(topic), any(Boundary.class));
    }

    @Test
    public void close() {
        cache.close();
        verify(tenantRouteCacheFactoryMock).close();
    }
}