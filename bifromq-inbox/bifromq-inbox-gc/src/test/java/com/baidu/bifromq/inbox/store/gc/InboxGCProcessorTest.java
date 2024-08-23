/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.store.gc;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.type.ClientInfo;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InboxGCProcessorTest {

    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IBaseKVStoreClient storeClient;
    private InboxStoreGCProcessor inboxGCProc;
    private final String localStoreId = "testLocalStoreId";
    private final KVRangeSetting localRangeSetting = new KVRangeSetting("cluster", localStoreId,
        KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).build());

    private final String remoteStoreId = "testRemoteStoreId";
    private final KVRangeSetting remoteRangeSetting = new KVRangeSetting("cluster", remoteStoreId,
        KVRangeDescriptor.newBuilder().setId(KVRangeIdUtil.generate()).build());
    private final String tenantId = "testTenantId";
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    public void testNoRangeForTenant() {
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(any())).thenReturn(Collections.emptyList());
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), tenantId, 10, HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.OK);
        verify(storeClient).findByBoundary(argThat(boundary ->
            boundary.getStartKey().equals(tenantPrefix(tenantId))
                && boundary.getEndKey().equals(upperBound(tenantPrefix(tenantId)))));
    }

    @Test
    public void testNoLocalRange() {
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient, localStoreId);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), tenantId, 10, HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.OK);
        verify(storeClient).findByBoundary(argThat(boundary ->
            boundary.getStartKey().equals(tenantPrefix(tenantId))
                && boundary.getEndKey().equals(upperBound(tenantPrefix(tenantId)))));
    }

    @Test
    public void testStoreQueryException() {
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));

        when(storeClient.query(anyString(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), null, 10, HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }

    @Test
    public void testStoreQueryFailed() {
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));

        when(storeClient.query(anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.InternalError)
                .build()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), null, 10, HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }

    @Test
    public void testGCScanFailed() {
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));
        when(storeClient.query(anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder()
                            .setCode(GCReply.Code.ERROR)
                            .build())
                        .build())
                    .build())
                .build()));
        IInboxStoreGCProcessor.Result result =
            inboxGCProc.gc(System.nanoTime(), null, 10, HLC.INST.getPhysical()).join();
        assertEquals(result, IInboxStoreGCProcessor.Result.ERROR);
    }

    @Test
    public void detachAfterScan() {
        GCReply.GCCandidate gcCandidate1 = GCReply.GCCandidate.newBuilder()
            .setInboxId("inboxId1")
            .setIncarnation(1)
            .setVersion(2)
            .setExpirySeconds(3)
            .setClient(ClientInfo.newBuilder().build())
            .build();
        GCReply.GCCandidate gcCandidate2 = GCReply.GCCandidate.newBuilder()
            .setInboxId("inboxId2")
            .setIncarnation(1)
            .setVersion(2)
            .setExpirySeconds(3)
            .setClient(ClientInfo.newBuilder().build())
            .build();
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));
        when(storeClient.query(anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder()
                            .setCode(GCReply.Code.OK)
                            .addCandidate(gcCandidate1)
                            .addCandidate(gcCandidate2)
                            .build())
                        .build())
                    .build())
                .build()));
        when(inboxClient.detach(any())).thenReturn(new CompletableFuture<>());
        inboxGCProc.gc(System.nanoTime(), null, null, HLC.INST.getPhysical());
        ArgumentCaptor<DetachRequest> detachRequestCaptor = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient, times(2)).detach(detachRequestCaptor.capture());
        List<DetachRequest> detachRequestList = detachRequestCaptor.getAllValues();

        assertEquals(detachRequestList.size(), 2);
        assertEquals(detachRequestList.get(0).getInboxId(), gcCandidate1.getInboxId());
        assertEquals(detachRequestList.get(0).getIncarnation(), gcCandidate1.getIncarnation());
        assertEquals(detachRequestList.get(0).getVersion(), gcCandidate1.getVersion());
        assertEquals(detachRequestList.get(0).getExpirySeconds(), gcCandidate1.getExpirySeconds());
        assertEquals(detachRequestList.get(0).getClient(), gcCandidate1.getClient());
        assertFalse(detachRequestList.get(0).getDiscardLWT());

        // verify the second detach request
        assertEquals(detachRequestList.get(1).getInboxId(), gcCandidate2.getInboxId());
        assertEquals(detachRequestList.get(1).getIncarnation(), gcCandidate2.getIncarnation());
        assertEquals(detachRequestList.get(1).getVersion(), gcCandidate2.getVersion());
        assertEquals(detachRequestList.get(1).getExpirySeconds(), gcCandidate2.getExpirySeconds());
        assertEquals(detachRequestList.get(1).getClient(), gcCandidate2.getClient());
        assertFalse(detachRequestList.get(1).getDiscardLWT());
    }

    @Test
    public void expirySecondsOverride() {
        GCReply.GCCandidate gcCandidate1 = GCReply.GCCandidate.newBuilder()
            .setInboxId("inboxId1")
            .setIncarnation(1)
            .setVersion(2)
            .setExpirySeconds(3)
            .setClient(ClientInfo.newBuilder().build())
            .build();
        inboxGCProc = new InboxStoreGCProcessor(inboxClient, storeClient);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(remoteRangeSetting));
        when(storeClient.query(anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder()
                            .setCode(GCReply.Code.OK)
                            .addCandidate(gcCandidate1)
                            .build())
                        .build())
                    .build())
                .build()));
        when(inboxClient.detach(any())).thenReturn(new CompletableFuture<>());
        inboxGCProc.gc(System.nanoTime(), null, 3, HLC.INST.getPhysical());
        ArgumentCaptor<DetachRequest> detachRequestCaptor = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient).detach(detachRequestCaptor.capture());
        List<DetachRequest> detachRequestList = detachRequestCaptor.getAllValues();

        assertEquals(detachRequestList.size(), 1);
        assertEquals(detachRequestList.get(0).getInboxId(), gcCandidate1.getInboxId());
        assertEquals(detachRequestList.get(0).getIncarnation(), gcCandidate1.getIncarnation());
        assertEquals(detachRequestList.get(0).getVersion(), gcCandidate1.getVersion());
        assertEquals(detachRequestList.get(0).getExpirySeconds(), 0);
        assertEquals(detachRequestList.get(0).getClient(), gcCandidate1.getClient());
        assertFalse(detachRequestList.get(0).getDiscardLWT());
    }
}

