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

import static com.baidu.bifromq.inbox.util.KeyUtil.inboxKeyPrefix;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InboxGCProcessorTest {

    @Mock
    private IInboxClient inboxClient;
    @Mock
    private IBaseKVStoreClient storeClient;
    private InboxGCProcessor inboxGCProc;
    private final KVRangeId rangeId = KVRangeIdUtil.generate();
    private final String tenantId = "testTenantId";
    private final ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        inboxGCProc = new InboxGCProcessor(inboxClient, storeClient);
        when(storeClient.findById(rangeId)).thenReturn(Optional.of(
            new KVRangeSetting("cluster", "leader", KVRangeDescriptor.newBuilder().build())
        ));
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }


    @Test
    public void testScanFailed() {
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        IInboxGCProcessor.Result result =
            inboxGCProc.gcRange(rangeId, tenantId, 10, HLC.INST.getPhysical(), 100).join();
        assertEquals(result, IInboxGCProcessor.Result.ERROR);
    }

    @Test
    public void testScanNoRange() {
        when(storeClient.findById(rangeId)).thenReturn(Optional.empty());
        IInboxGCProcessor.Result result =
            inboxGCProc.gcRange(rangeId, tenantId, 10, HLC.INST.getPhysical(), 100).join();
        assertEquals(result, IInboxGCProcessor.Result.ERROR);
    }

    @Test
    public void testScanHasNext() {
        long now = HLC.INST.getPhysical();
        ByteString cursor = ByteString.copyFromUtf8("Cursor");
        KVRangeROReply reply1 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                    .setGc(GCReply.newBuilder()
                        .setCode(GCReply.Code.OK)
                        .addInbox(GCReply.Inbox.newBuilder()
                            .setInboxId("inbox1")
                            .setClient(clientInfo)
                            .build())
                        .addInbox(GCReply.Inbox.newBuilder()
                            .setInboxId("inbox2")
                            .setClient(clientInfo)
                            .build())
                        .addInbox(GCReply.Inbox.newBuilder()
                            .setInboxId("inbox3")
                            .setClient(clientInfo)
                            .build())
                        .setCursor(cursor)
                        .build())
                    .build())
                .build())
            .build();
        KVRangeROReply reply2 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                    .setGc(GCReply.newBuilder().setCode(GCReply.Code.OK).build())
                    .build())
                .build())
            .build();
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply1), CompletableFuture.completedFuture(reply2));
        when(inboxClient.detach(any())).thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder()
            .setCode(DetachReply.Code.OK)
            .build()));
        IInboxGCProcessor.Result result = inboxGCProc.gcRange(rangeId, tenantId, 10, now, 2).join();
        assertEquals(result, IInboxGCProcessor.Result.OK);
        ArgumentCaptor<KVRangeRORequest> queryCap = ArgumentCaptor.forClass(KVRangeRORequest.class);
        verify(storeClient, times(2)).query(anyString(), queryCap.capture());
        List<KVRangeRORequest> queryRequests = queryCap.getAllValues();
        Assert.assertFalse(queryRequests.get(0).getRoCoProc().getInboxService().getGc().hasCursor());
        assertEquals(queryRequests.get(1).getRoCoProc().getInboxService().getGc().getCursor(), cursor);

        ArgumentCaptor<DetachRequest> detachCap = ArgumentCaptor.forClass(DetachRequest.class);
        verify(inboxClient, times(3)).detach(detachCap.capture());
        List<DetachRequest> detachRequests = detachCap.getAllValues();
        for (DetachRequest detachRequest : detachRequests) {
            assertEquals(detachRequest.getExpirySeconds(), 0);
            assertEquals(detachRequest.getNow(), now);
            assertEquals(detachRequest.getClient(), clientInfo);
        }
    }

    @Test
    public void testScanHasNext2() {
        KVRangeROReply reply1 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                    .setGc(GCReply.newBuilder()
                        .setCode(GCReply.Code.OK)
                        .setCursor(inboxKeyPrefix(tenantId, "inbox1", 0)).build())
                    .build())
                .build()
            )
            .build();
        KVRangeROReply reply2 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                    .setGc(GCReply.newBuilder().setCode(GCReply.Code.OK).build())
                    .build())
                .build()
            )
            .build();
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply1), CompletableFuture.completedFuture(reply2));
        when(inboxClient.detach(any())).thenReturn(CompletableFuture.completedFuture(DetachReply.newBuilder()
            .setCode(DetachReply.Code.OK)
            .build()));

        inboxGCProc.gcRange(rangeId, tenantId, 10, HLC.INST.getPhysical(), 2);
        verify(storeClient, times(2)).query(anyString(), any());
        verify(inboxClient, times(0)).detach(any());
    }
}

