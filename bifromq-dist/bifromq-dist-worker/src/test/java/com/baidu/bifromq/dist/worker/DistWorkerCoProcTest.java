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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static com.baidu.bifromq.util.BSUtil.toByteString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.dist.rpc.proto.BatchDistRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistPack;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.GCReply;
import com.baidu.bifromq.dist.rpc.proto.GCRequest;
import com.baidu.bifromq.dist.rpc.proto.MatchRoute;
import com.baidu.bifromq.dist.rpc.proto.RouteGroup;
import com.baidu.bifromq.dist.rpc.proto.TenantOption;
import com.baidu.bifromq.dist.worker.cache.ISubscriptionCache;
import com.baidu.bifromq.dist.worker.schema.KVSchemaUtil;
import com.baidu.bifromq.dist.worker.schema.Matching;
import com.baidu.bifromq.plugin.subbroker.CheckRequest;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistWorkerCoProcTest {

    private ISubscriptionCache routeCache;
    private ITenantsState tenantsState;
    private IDeliverExecutorGroup deliverExecutorGroup;
    private ISubscriptionCleaner subscriptionChecker;
    private Supplier<IKVCloseableReader> readerProvider;
    private IKVCloseableReader reader;
    private IKVWriter writer;
    private IKVIterator iterator;
    private KVRangeId rangeId;
    private DistWorkerCoProc distWorkerCoProc;

    @BeforeMethod
    public void setUp() {
        routeCache = mock(ISubscriptionCache.class);
        tenantsState = mock(ITenantsState.class);
        deliverExecutorGroup = mock(IDeliverExecutorGroup.class);
        subscriptionChecker = mock(ISubscriptionCleaner.class);
        readerProvider = mock(Supplier.class);
        reader = mock(IKVCloseableReader.class);
        iterator = mock(IKVIterator.class);
        writer = mock(IKVWriter.class);
        rangeId = KVRangeId.newBuilder().setId(1).setEpoch(1).build();
        when(readerProvider.get()).thenReturn(reader);
        when(reader.boundary()).thenReturn(FULL_BOUNDARY);
        when(reader.iterator()).thenReturn(iterator);
        when(iterator.isValid()).thenReturn(false);
        distWorkerCoProc = new DistWorkerCoProc(
            rangeId, readerProvider, routeCache, tenantsState, deliverExecutorGroup, subscriptionChecker);
    }

    @Test
    public void testMutateBatchMatch() {
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(BatchMatchRequest.newBuilder()
                    .setReqId(123)
                    .putRequests("tenant1", BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(10).build())
                        .addRoute(MatchRoute.newBuilder()
                            .setTopicFilter("topicFilter1")
                            .setBrokerId(1)
                            .setReceiverId("inbox1")
                            .setDelivererKey("deliverer1")
                            .setIncarnation(1L)
                            .build())
                        .build())
                    .putRequests("tenant2", BatchMatchRequest.TenantBatch.newBuilder()
                        .setOption(TenantOption.newBuilder().setMaxReceiversPerSharedSubGroup(5).build())
                        .addRoute(MatchRoute.newBuilder()
                            .setTopicFilter("topicFilter2")
                            .setBrokerId(1)
                            .setReceiverId("inbox2")
                            .setDelivererKey("deliverer2")
                            .setIncarnation(1L)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.exist(any(ByteString.class))).thenReturn(false);

        // Simulate mutation
        Supplier<RWCoProcOutput> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        RWCoProcOutput result = resultSupplier.get();

        verify(writer, times(2)).put(any(), eq(toByteString(1L)));
        // Verify that matches are added to the cache
        verify(routeCache, times(1)).refresh(any());

        // Verify that tenant state is updated for both tenants
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant1"), eq(1));
        verify(tenantsState, times(1)).incNormalRoutes(eq("tenant2"), eq(1));

        // Check the result output
        BatchMatchReply reply = result.getDistService().getBatchMatch();
        assertEquals(reply.getReqId(), 123);
        assertEquals(reply.getResultsOrThrow("tenant1").getCode(0), BatchMatchReply.TenantBatch.Code.OK);
        assertEquals(reply.getResultsOrThrow("tenant2").getCode(0), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testMutateBatchUnmatch() {
        long incarnation = 1;
        RWCoProcInput rwCoProcInput = RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchUnmatch(BatchUnmatchRequest.newBuilder()
                    .setReqId(456)
                    .putRequests("tenant1", BatchUnmatchRequest.TenantBatch.newBuilder()
                        .addRoute(MatchRoute.newBuilder()
                            .setTopicFilter("topicFilter1")
                            .setBrokerId(1)
                            .setReceiverId("inbox1")
                            .setDelivererKey("deliverer1")
                            .setIncarnation(incarnation)
                            .build())
                        .build())
                    .build())
                .build())
            .build();

        // Simulate match exists in the reader
        when(reader.get(any(ByteString.class))).thenReturn(Optional.of(toByteString(1L)));

        // Simulate mutation
        Supplier<RWCoProcOutput> resultSupplier = distWorkerCoProc.mutate(rwCoProcInput, reader, writer);
        RWCoProcOutput result = resultSupplier.get();

        // Verify that matches are removed from the cache
        verify(routeCache, times(1)).refresh(argThat(m -> m.containsKey("tenant1")
            && m.get("tenant1").contains("topicFilter1")));

        // Verify that tenant state is updated
        verify(tenantsState, times(1)).decNormalRoutes(eq("tenant1"), eq(1));

        // Check the result output
        BatchUnmatchReply reply = result.getDistService().getBatchUnmatch();
        assertEquals(reply.getReqId(), 456);
        assertEquals(reply.getResultsOrThrow("tenant1").getCode(0), BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @Test
    public void testQueryBatchDist() {
        ROCoProcInput roCoProcInput = ROCoProcInput.newBuilder()
            .setDistService(DistServiceROCoProcInput.newBuilder()
                .setBatchDist(BatchDistRequest.newBuilder()
                    .setReqId(789)
                    .addDistPack(DistPack.newBuilder()
                        .setTenantId("tenant1")
                        .addMsgPack(TopicMessagePack.newBuilder().setTopic("topic1").build())
                        .build())
                    .build())
                .build())
            .build();

        // Simulate routes in cache
        CompletableFuture<Set<Matching>> futureRoutes = CompletableFuture.completedFuture(
            Set.of(createMatching("tenant1", MatchRoute.newBuilder()
                .setTopicFilter("topic1")
                .setBrokerId(1)
                .setReceiverId("inbox1")
                .setDelivererKey("deliverer1")
                .setIncarnation(1L)
                .build())));
        when(routeCache.get(eq("tenant1"), eq("topic1"))).thenReturn(futureRoutes);

        // Simulate query
        CompletableFuture<ROCoProcOutput> resultFuture = distWorkerCoProc.query(roCoProcInput, reader);
        ROCoProcOutput result = resultFuture.join();

        // Verify the submission to executor group
        verify(deliverExecutorGroup, times(1)).submit(eq("tenant1"), anySet(), any(TopicMessagePack.class));

        // Check the result output
        BatchDistReply reply = result.getDistService().getBatchDist();
        assertEquals(789, reply.getReqId());
    }

    @Test
    public void testGC() {
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String topic1 = "topic1";
        String topic2 = "topic2";
        String receiverUrl1 = toReceiverUrl(1, "inbox1", "deliverer1");
        String receiverUrl2 = toReceiverUrl(2, "inbox2", "deliverer2");

        ByteString normalMatchKey1 =
            KVSchemaUtil.toNormalRouteKey(tenant1, topic1, toReceiverUrl(1, "inbox1", "deliverer1"));
        ByteString normalMatchKey2 =
            KVSchemaUtil.toNormalRouteKey(tenant2, topic2, toReceiverUrl(2, "inbox2", "deliverer2"));

        String sharedTopic = "$share/group/topic3";
        ByteString groupMatchKey = KVSchemaUtil.toGroupRouteKey(tenant1, sharedTopic);
        RouteGroup groupMembers = RouteGroup.newBuilder()
            .putMembers(receiverUrl1, 1L)
            .putMembers(receiverUrl2, 1L)
            .build();

        when(iterator.isValid()).thenReturn(true, true, true, false);
        when(iterator.key()).thenReturn(normalMatchKey1, groupMatchKey, normalMatchKey2);
        when(iterator.value()).thenReturn(
            toByteString(1L),
            groupMembers.toByteString(),
            toByteString(1L)
        );

        when(routeCache.isCached(eq(tenant1), eq(topic1))).thenReturn(false);
        when(routeCache.isCached(eq(tenant1), eq(KVSchemaUtil.parseRouteDetail(groupMatchKey).topicFilter())))
            .thenReturn(false);
        when(routeCache.isCached(eq(tenant2), eq(topic2))).thenReturn(false);

        when(subscriptionChecker.sweep(anyInt(), any(CheckRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        ROCoProcInput roCoProcInput = ROCoProcInput.newBuilder()
            .setDistService(DistServiceROCoProcInput.newBuilder()
                .setGc(GCRequest.newBuilder().setReqId(999).build())
                .build())
            .build();

        CompletableFuture<ROCoProcOutput> resultFuture = distWorkerCoProc.query(roCoProcInput, reader);
        ROCoProcOutput result = resultFuture.join();

        GCReply reply = result.getDistService().getGc();
        assertEquals(reply.getReqId(), 999);

        verify(subscriptionChecker, times(1)).sweep(
            eq(1),
            argThat(req -> req.getTenantId().equals(tenant1) && req.getMatchInfoCount() == 2)
        );
        verify(subscriptionChecker, times(1)).sweep(
            eq(2),
            argThat(req -> req.getTenantId().equals(tenant2) && req.getMatchInfoCount() == 1
                && req.getMatchInfoList().get(0).getTopicFilter().equals(topic2))
        );
        verify(subscriptionChecker, times(1)).sweep(
            eq(2),
            argThat(req -> req.getTenantId().equals(tenant1) && req.getMatchInfoCount() == 1
                && req.getMatchInfoList().get(0).getTopicFilter().equals(sharedTopic))
        );
    }

    @Test
    public void testReset() {
        Boundary boundary = Boundary.newBuilder()
            .setStartKey(ByteString.copyFromUtf8("start"))
            .setEndKey(ByteString.copyFromUtf8("end"))
            .build();
        when(reader.boundary()).thenReturn(boundary);
        distWorkerCoProc.reset(boundary);

        // Verify that tenant state and route cache are reset
        verify(tenantsState, times(1)).reset();
        verify(routeCache, times(1)).reset(eq(boundary));
    }

    @Test
    public void testClose() {
        distWorkerCoProc.close();

        // Verify that tenant state, route cache, and deliver executor group are closed
        verify(tenantsState, times(1)).close();
        verify(routeCache, times(1)).close();
        verify(deliverExecutorGroup, times(1)).shutdown();
    }

    private Matching createMatching(String tenantId, MatchRoute route) {
        // Sample data for creating a Matching object

        // Construct a ByteString for normal match record key
        ByteString normalRouteKey =
            KVSchemaUtil.toNormalRouteKey(tenantId, route.getTopicFilter(), toReceiverUrl(route));

        // Construct the match record value (for example, an empty value for a normal match)
        ByteString matchRecordValue = toByteString(1L);

        // Use EntityUtil to parse the key and value into a Matching object
        return KVSchemaUtil.buildMatchRoute(normalRouteKey, matchRecordValue);
    }
}