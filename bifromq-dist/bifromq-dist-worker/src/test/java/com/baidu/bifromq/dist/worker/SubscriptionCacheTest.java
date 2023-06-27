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

import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DIST_TOPIC_MATCH_EXPIRY;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.entity.EntityUtil;
import com.baidu.bifromq.dist.entity.NormalMatching;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.MatchRecord;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mock;

public class SubscriptionCacheTest {
    private KVRangeId id = KVRangeIdUtil.generate();
    @Mock
    private IKVRangeReader rangeReader;
    @Mock
    private IKVReader kvReader;
    @Mock
    private IKVIterator kvIterator;
    @Mock
    private Supplier<IKVRangeReader> rangeReaderProvider;

    @Mock
    private ILoadEstimator loadTracker;
    private ExecutorService matchExecutor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        id = KVRangeIdUtil.generate();
        System.setProperty(DIST_TOPIC_MATCH_EXPIRY.propKey, "1");
        when(kvReader.iterator()).thenReturn(kvIterator);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReaderProvider.get()).thenReturn(rangeReader);
        matchExecutor = MoreExecutors.newDirectExecutorService();
    }

    @AfterMethod
    public void teardown() throws Exception {
        MoreExecutors.shutdownAndAwaitTermination(matchExecutor, 5, TimeUnit.SECONDS);
        closeable.close();
    }

    @Test
    public void cacheHit() {
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId("testTraffic")
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);
        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(false);

        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);
//        assertTrue(routes.get(scopedTopic).isEmpty());

        // cacheHit
        routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);
//        assertTrue(routes.get(scopedTopic).isEmpty());

        verify(rangeReader).refresh();
    }

    @SneakyThrows
    @Test
    public void cacheRefreshAndShortcut() {
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId("testTraffic")
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();

        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(false);
        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        Thread.sleep(500);
        cache.get(scopedTopic, singleton(sender)).join();
        Thread.sleep(600);
        routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);

        verify(kvIterator, times(1)).seek(any());
        verify(rangeReader, times(1)).refresh();
    }

    @SneakyThrows
    @Test
    public void cacheExpiredAndMatch() {
        String trafficId = "testTraffic";
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId(trafficId)
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        when(kvIterator.isValid()).thenReturn(false);

        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        Thread.sleep(1100);
        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(false);

        routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);
        verify(kvIterator, times(2)).seek(any());
        verify(kvIterator, times(2)).seek(scopedTopic.matchRecordRange.getStartKey());
    }

    @SneakyThrows
    @Test(priority = 0)
    public void cacheTouchAndMatch() {
        String trafficId = "testTraffic";
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId(trafficId)
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        when(kvIterator.isValid()).thenReturn(false);
        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        Thread.sleep(500);
        cache.get(scopedTopic, singleton(sender)).join();
        cache.touch(trafficId);
        Thread.sleep(600);

        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(false);

        routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);
        verify(kvIterator, times(2)).seek(any());
        verify(kvIterator, times(2)).seek(scopedTopic.matchRecordRange.getStartKey());
    }

    @SneakyThrows
    @Test
    public void cacheInvalidateAndMatch() {
        String trafficId = "testTraffic";
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId(trafficId)
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        when(kvIterator.isValid()).thenReturn(false);

        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();

        Thread.sleep(500);
        cache.get(scopedTopic, singleton(sender)).join();
        cache.invalidate(scopedTopic); // invalidate
        Thread.sleep(600);

        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(false);

        routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 0);
        verify(kvIterator, times(2)).seek(any());
        verify(kvIterator, times(2)).seek(scopedTopic.matchRecordRange.getStartKey());
    }

    @Test
    public void groupMatch() {
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId("testTraffic")
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(true, false);
        String qInboxId = EntityUtil.toQualifiedInboxId(0, "inbox1", "inboxGroupKey");
        String sharedTopicFilter = "$oshare/group/" + scopedTopic.topic;
        when(kvIterator.key())
            .thenReturn(EntityUtil.matchRecordKey(scopedTopic.trafficId, sharedTopicFilter, qInboxId));
        when(kvIterator.value())
            .thenReturn(MatchRecord.newBuilder()
                .setGroup(GroupMatchRecord.newBuilder().putEntry(qInboxId, QoS.AT_MOST_ONCE).build())
                .build().toByteString());

        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        assertEquals(routes.size(), 1);
        for (Map.Entry<NormalMatching, Set<ClientInfo>> entry : routes.entrySet()) {
            NormalMatching matching = entry.getKey();
            assertEquals(matching.trafficId, scopedTopic.trafficId);
            assertEquals(matching.originalTopicFilter(), sharedTopicFilter);
            assertEquals(matching.brokerId, 0);
            assertEquals(matching.subInfo.getInboxId(), "inbox1");
            assertEquals(matching.subInfo.getSubQoS(), QoS.AT_MOST_ONCE);
            assertEquals(matching.inboxGroupKey, "inboxGroupKey");
            assertTrue(entry.getValue().contains(sender));
        }
    }

    @SneakyThrows
    @Test
    public void groupMatchRefresh() {
        String trafficId = "testTraffic";
        ScopedTopic scopedTopic = ScopedTopic.builder()
            .trafficId(trafficId)
            .topic("/test/user")
            .range(FULL_RANGE)
            .build();
        ClientInfo sender = ClientInfo.newBuilder().setTrafficId("testTraffic").setUserId("testSender").build();
        SubscriptionCache cache = new SubscriptionCache(id, rangeReaderProvider, matchExecutor, loadTracker);

        doNothing().when(kvIterator).seek(scopedTopic.matchRecordRange.getStartKey());
        when(kvIterator.isValid()).thenReturn(true, false, true, false);
        String qInboxId1 = EntityUtil.toQualifiedInboxId(0, "inbox1", "inboxGroupKey");
        String qInboxId2 = EntityUtil.toQualifiedInboxId(0, "inbox2", "inboxGroupKey");
        String sharedTopicFilter = "$oshare/group/" + scopedTopic.topic;
        when(kvIterator.key())
            .thenReturn(EntityUtil.matchRecordKey(scopedTopic.trafficId, sharedTopicFilter, qInboxId1));
        when(kvIterator.value())
            .thenReturn(
                MatchRecord.newBuilder()
                    .setGroup(GroupMatchRecord.newBuilder().putEntry(qInboxId1, QoS.AT_MOST_ONCE).build())
                    .build().toByteString(),
                MatchRecord.newBuilder()
                    .setGroup(GroupMatchRecord.newBuilder().putEntry(qInboxId2, QoS.AT_LEAST_ONCE).build())
                    .build().toByteString());

        Map<NormalMatching, Set<ClientInfo>> routes = cache.get(scopedTopic, singleton(sender)).join();
        cache.touch(trafficId);
        Thread.sleep(1100);
        routes = cache.get(scopedTopic, singleton(sender)).join();

        assertEquals(routes.size(), 1);
        for (Map.Entry<NormalMatching, Set<ClientInfo>> entry : routes.entrySet()) {
            NormalMatching matching = entry.getKey();
            assertEquals(matching.trafficId, scopedTopic.trafficId);
            assertEquals(matching.originalTopicFilter(), sharedTopicFilter);
            assertEquals(matching.brokerId, 0);
            assertEquals(matching.subInfo.getInboxId(), "inbox2");
            assertEquals(matching.subInfo.getSubQoS(), QoS.AT_LEAST_ONCE);
            assertEquals(matching.inboxGroupKey, "inboxGroupKey");
            assertTrue(entry.getValue().contains(sender));
        }
    }
}
