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

package com.baidu.bifromq.inbox.store.gc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InboxGCProcTest {

    @Mock
    private IBaseKVStoreClient storeClient;
    @Mock
    private Function<ByteString, CompletableFuture<Void>> gcer;
    private InboxGCProc inboxGCProc;
    private KVRangeId rangeId = KVRangeIdUtil.generate();
    private String tenantId = "testTenantId";
    private AutoCloseable closeable;

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        inboxGCProc = new InboxGCProc(storeClient, MoreExecutors.directExecutor()) {
            @Override
            protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
                return gcer.apply(scopedInboxId);
            }
        };
        when(storeClient.findById(rangeId)).thenReturn(Optional.of(
            new KVRangeSetting("cluster", "leader", KVRangeDescriptor.newBuilder().build())
        ));
    }

    @Test
    public void testScanFailed() {
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException()));
        inboxGCProc.gcRange(rangeId, tenantId, 10, 100);
        verify(gcer, times(0)).apply(any(ByteString.class));
    }

    @Test
    public void testScanNoRange() {
        when(storeClient.findById(rangeId)).thenReturn(Optional.empty());
        inboxGCProc.gcRange(rangeId, tenantId, 10, 100);
        verify(gcer, times(0)).apply(any(ByteString.class));
    }

    @Test
    public void testScanHasNext() {
        KVRangeROReply reply1 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(
                ROCoProcOutput.newBuilder()
                    .setInboxService(
                        InboxServiceROCoProcOutput.newBuilder()
                            .setGc(
                                GCReply.newBuilder()
                                    .addScopedInboxId(KeyUtil.scopedInboxId(tenantId, "inbox1"))
                                    .addScopedInboxId(KeyUtil.scopedInboxId(tenantId, "inbox2"))
                                    .setNextScopedInboxId(KeyUtil.scopedInboxId(tenantId, "inbox3"))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        KVRangeROReply reply2 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(
                ROCoProcOutput.newBuilder()
                    .setInboxService(
                        InboxServiceROCoProcOutput.newBuilder()
                            .setGc(
                                GCReply.newBuilder()
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply1), CompletableFuture.completedFuture(reply2));
        when(gcer.apply(any(ByteString.class))).thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<KVRangeRORequest> argCap = ArgumentCaptor.forClass(KVRangeRORequest.class);
        inboxGCProc.gcRange(rangeId, tenantId, 10, 2);
        verify(storeClient, times(2)).query(anyString(), argCap.capture());
        verify(gcer, times(2)).apply(any(ByteString.class));
        Assert.assertFalse(argCap.getAllValues().get(0).getRoCoProc().getInboxService().getGc().hasScopedInboxId());
        Assert.assertEquals(argCap.getAllValues().get(1).getRoCoProc().getInboxService().getGc().getScopedInboxId(),
            KeyUtil.scopedInboxId(tenantId, "inbox3"));
    }

    @Test
    public void testScanHasNext2() {
        KVRangeROReply reply1 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(
                ROCoProcOutput.newBuilder()
                    .setInboxService(
                        InboxServiceROCoProcOutput.newBuilder()
                            .setGc(
                                GCReply.newBuilder()
                                    .setNextScopedInboxId(KeyUtil.scopedInboxId(tenantId, "inbox1"))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        KVRangeROReply reply2 = KVRangeROReply.newBuilder()
            .setCode(ReplyCode.Ok)
            .setRoCoProcResult(
                ROCoProcOutput.newBuilder()
                    .setInboxService(
                        InboxServiceROCoProcOutput.newBuilder()
                            .setGc(
                                GCReply.newBuilder()
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        when(storeClient.query(anyString(), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply1), CompletableFuture.completedFuture(reply2));
        when(gcer.apply(any(ByteString.class))).thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<KVRangeRORequest> argCap = ArgumentCaptor.forClass(KVRangeRORequest.class);
        inboxGCProc.gcRange(rangeId, tenantId, 10, 2);
        verify(storeClient, times(2)).query(anyString(), argCap.capture());
        verify(gcer, times(0)).apply(any(ByteString.class));
        Assert.assertFalse(argCap.getAllValues().get(0).getRoCoProc().getInboxService().getGc().hasScopedInboxId());
        Assert.assertEquals(argCap.getAllValues().get(1).getRoCoProc().getInboxService().getGc().getScopedInboxId(),
            KeyUtil.scopedInboxId(tenantId, "inbox1"));
    }

}

