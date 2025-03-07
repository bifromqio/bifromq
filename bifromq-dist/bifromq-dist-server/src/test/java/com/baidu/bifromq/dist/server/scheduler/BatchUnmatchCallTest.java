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

package com.baidu.bifromq.dist.server.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchUnmatchCallTest {

    private KVRangeId rangeId;
    private IBaseKVStoreClient storeClient;
    private BatchUnmatchCall batchUnmatchCall;

    @BeforeMethod
    void setUp() {
        rangeId = KVRangeId.newBuilder().setId(1).build();
        storeClient = mock(IBaseKVStoreClient.class);
        batchUnmatchCall = new BatchUnmatchCall(rangeId, storeClient, Duration.ofMinutes(1));
    }

    @Test
    void testMakeBatch() {
        UnmatchRequest request1 = UnmatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant1")
            .setTopicFilter("filter1")
            .setReceiverId("receiver1")
            .setBrokerId(1)
            .setDelivererKey("key1")
            .setIncarnation(1L)
            .build();

        UnmatchRequest request2 = UnmatchRequest.newBuilder()
            .setReqId(2)
            .setTenantId("tenant2")
            .setTopicFilter("filter2")
            .setReceiverId("receiver2")
            .setBrokerId(2)
            .setDelivererKey("key2")
            .setIncarnation(1L)
            .build();

        // contain duplicate request
        Iterator<UnmatchRequest> iterator = List.of(request1, request1, request2).iterator();

        RWCoProcInput input = batchUnmatchCall.makeBatch(iterator);

        BatchUnmatchRequest batchRequest = input.getDistService().getBatchUnmatch();
        assertEquals(batchRequest.getRequestsCount(), 2);
    }

    private void testHandleOutput(BatchUnmatchReply.TenantBatch.Code batchResult,
                                  UnmatchReply.Result expectedUnmatchResult) {
        ICallTask<UnmatchRequest, UnmatchReply, MutationCallBatcherKey> callTask = mock(ICallTask.class);
        UnmatchRequest request = UnmatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant1")
            .setTopicFilter("filter1")
            .setReceiverId("receiver1")
            .setBrokerId(1)
            .setDelivererKey("key1")
            .setIncarnation(1L)
            .build();
        when(callTask.call()).thenReturn(request);
        CompletableFuture<UnmatchReply> resultPromise = new CompletableFuture<>();
        when(callTask.resultPromise()).thenReturn(resultPromise);

        Queue<ICallTask<UnmatchRequest, UnmatchReply, MutationCallBatcherKey>> batchedTasks = new LinkedList<>();
        batchedTasks.add(callTask);

        BatchUnmatchReply batchUnmatchReply = BatchUnmatchReply.newBuilder()
            .setReqId(1)
            .putResults(request.getTenantId(), BatchUnmatchReply.TenantBatch.newBuilder().addCode(batchResult).build())
            .build();
        RWCoProcOutput output = RWCoProcOutput.newBuilder()
            .setDistService(DistServiceRWCoProcOutput.newBuilder()
                .setBatchUnmatch(batchUnmatchReply)
                .build())
            .build();

        batchUnmatchCall.handleOutput(batchedTasks, output);

        verify(callTask).resultPromise();
        UnmatchReply reply = resultPromise.join();
        assertEquals(expectedUnmatchResult, reply.getResult());
        assertEquals(reply.getReqId(), 1);
    }

    @Test
    void testHandleOutput() {
        testHandleOutput(BatchUnmatchReply.TenantBatch.Code.OK, UnmatchReply.Result.OK);
        testHandleOutput(BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED, UnmatchReply.Result.NOT_EXISTED);
        testHandleOutput(BatchUnmatchReply.TenantBatch.Code.ERROR, UnmatchReply.Result.ERROR);
    }

    @Test
    void testHandleException() {
        ICallTask<UnmatchRequest, UnmatchReply, MutationCallBatcherKey> callTask = mock(ICallTask.class);

        Throwable exception = new RuntimeException("Test exception");
        CompletableFuture<UnmatchReply> resultPromise = new CompletableFuture<>();
        when(callTask.resultPromise()).thenReturn(resultPromise);

        batchUnmatchCall.handleException(callTask, exception);

        verify(callTask).resultPromise();
        assertTrue(resultPromise.isCompletedExceptionally());
    }
}
