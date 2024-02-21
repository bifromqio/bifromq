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

package com.baidu.bifromq.retain.store.gc;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetainStoreGCProcessorTest {
    @Mock
    private IBaseKVStoreClient storeClient;
    private RetainStoreGCProcessor gcProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void testGCNonExistTenant() {
        String tenantId = "tenantId";
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        when(storeClient.findByKey(any())).thenReturn(Optional.empty());
        IRetainStoreGCProcessor.Result result =
            gcProcessor.gc(System.nanoTime(), tenantId, null, HLC.INST.getPhysical()).join();
        assertEquals(result, IRetainStoreGCProcessor.Result.ERROR);
        verify(storeClient).findByKey(eq(tenantNS(tenantId)));
    }

    @Test
    public void testGCTenantWithNullExpirySeconds() {
        String tenantId = "tenantId";
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .build();
        KVRangeSetting setting = new KVRangeSetting("clueter", "store", rangeDescriptor);
        when(storeClient.findByKey(any())).thenReturn(Optional.of(setting));
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, tenantId, null, now);
        verify(storeClient).findByKey(eq(tenantNS(tenantId)));
        verify(storeClient).execute(eq(setting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return gcRequest.getTenantId().equals(tenantId)
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && !gcRequest.hasExpirySeconds();
        }));
    }

    @Test
    public void testGCTenantWithExpirySeconds() {
        String tenantId = "tenantId";
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        int expirySeconds = 10;
        gcProcessor = new RetainStoreGCProcessor(storeClient, null);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .build();
        KVRangeSetting setting = new KVRangeSetting("clueter", "store", rangeDescriptor);
        when(storeClient.findByKey(any())).thenReturn(Optional.of(setting));
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, tenantId, expirySeconds, now);
        verify(storeClient).findByKey(eq(tenantNS(tenantId)));
        verify(storeClient).execute(eq(setting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return gcRequest.getTenantId().equals(tenantId)
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && gcRequest.getExpirySeconds() == expirySeconds;
        }));
    }

    @Test
    public void testGCWithLocalStoreAndExpirySeconds() {
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        int expirySeconds = 10;
        String localStoreId = "localStore";
        gcProcessor = new RetainStoreGCProcessor(storeClient, localStoreId);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .build();
        KVRangeSetting localSetting = new KVRangeSetting("cluster", localStoreId, rangeDescriptor);
        KVRangeSetting remoteSetting = new KVRangeSetting("cluster", "remoteStore", rangeDescriptor);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(localSetting, remoteSetting));
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, null, expirySeconds, now);
        verify(storeClient).execute(eq(localSetting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return !gcRequest.hasTenantId()
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && gcRequest.getExpirySeconds() == expirySeconds;
        }));
    }

    @Test
    public void testGCWithLocalStoreSpecified() {
        long reqId = System.nanoTime();
        long now = HLC.INST.getPhysical();
        String localStoreId = "localStore";
        gcProcessor = new RetainStoreGCProcessor(storeClient, localStoreId);
        KVRangeDescriptor rangeDescriptor = KVRangeDescriptor.newBuilder()
            .setId(KVRangeIdUtil.generate())
            .setVer(1)
            .build();
        KVRangeSetting localSetting = new KVRangeSetting("cluster", localStoreId, rangeDescriptor);
        KVRangeSetting remoteSetting = new KVRangeSetting("cluster", "remoteStore", rangeDescriptor);
        when(storeClient.findByBoundary(FULL_BOUNDARY)).thenReturn(List.of(localSetting, remoteSetting));
        when(storeClient.execute(anyString(), any())).thenReturn(new CompletableFuture<>());
        gcProcessor.gc(reqId, null, null, now);
        verify(storeClient).execute(eq(localSetting.leader), argThat(req -> {
            if (req.getReqId() != reqId
                || req.getVer() != rangeDescriptor.getVer()
                || !req.getKvRangeId().equals(rangeDescriptor.getId())) {
                return false;
            }
            GCRequest gcRequest = req.getRwCoProc().getRetainService().getGc();
            return !gcRequest.hasTenantId()
                && gcRequest.getReqId() == reqId
                && gcRequest.getNow() == now
                && !gcRequest.hasExpirySeconds();
        }));
    }
}
