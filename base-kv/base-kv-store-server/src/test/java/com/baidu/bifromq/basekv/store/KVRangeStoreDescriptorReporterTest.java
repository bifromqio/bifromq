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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.store.CRDTUtil.storeDescriptorMapCRDTURI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IMVReg;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.IORMap.ORMapKey;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation.ORMapRemove;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation.ORMapUpdate;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreDescriptorReporterTest extends MockableTest {

    @Mock
    private ICRDTService crdtService;
    @Mock
    private IORMap storeDescriptorMap;
    @Mock
    private IMVReg localMVReg;
    @Mock
    private IMVReg remoteMVReg;

    private KVRangeStoreDescriptorReporter storeDescriptorReporter;
    private Subject<Long> inflationSubject = PublishSubject.create();
    private String localStoreId = "localStoreId";
    private String remoteStoreId = "remoteStoreId";
    private KVRangeStoreDescriptor storeDescriptor;
    private KVRangeStoreDescriptor remoteStoreDescriptor;

    @Override
    protected void doSetup(Method method) {
        String uri = storeDescriptorMapCRDTURI("testCluster");
        when(crdtService.get(uri)).thenReturn(Optional.of(storeDescriptorMap));
        storeDescriptorReporter = new KVRangeStoreDescriptorReporter("testCluster", crdtService, 200L);
        storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .setHlc(HLC.INST.get())
            .build();
        remoteStoreDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(remoteStoreId)
            .setHlc(HLC.INST.get())
            .build();
        when(storeDescriptorMap.inflation()).thenReturn(inflationSubject);
        storeDescriptorReporter.start();
    }

    @Override
    protected void doTeardown(Method method) {
        when(storeDescriptorMap.execute(any(ORMapOperation.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(crdtService.stopHosting(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        storeDescriptorReporter.stop();
    }

    @Test
    public void testReportWithStoreDescriptorNotExist() {
        when(storeDescriptorMap.getMVReg(ByteString.copyFromUtf8(localStoreId))).thenReturn(localMVReg);
        when(localMVReg.read()).thenReturn(Collections.emptyIterator());
        storeDescriptorReporter.report(storeDescriptor);
        verify(storeDescriptorMap, times(1)).execute(any(ORMapUpdate.class));

    }

    @Test
    public void testReportWithStoreDescriptorExist() {
        when(storeDescriptorMap.getMVReg(ByteString.copyFromUtf8(localStoreId))).thenReturn(localMVReg);
        when(localMVReg.read()).thenReturn(Lists.<ByteString>newArrayList(storeDescriptor.toByteString()).iterator());
        storeDescriptorReporter.report(storeDescriptor);
        verify(storeDescriptorMap, times(0)).execute(any(ORMapUpdate.class));
    }

    @Test
    public void testRemoveRemoteDescriptorNotAlive() throws InterruptedException {
        when(storeDescriptorMap.getMVReg(ByteString.copyFromUtf8(localStoreId))).thenReturn(localMVReg);
        when(storeDescriptorMap.getMVReg(ByteString.copyFromUtf8(remoteStoreId))).thenReturn(remoteMVReg);
        when(localMVReg.read())
            .thenReturn(Lists.<ByteString>newArrayList(storeDescriptor.toByteString()).iterator())
            .thenReturn(Lists.<ByteString>newArrayList(storeDescriptor.toByteString()).iterator());
        when(remoteMVReg.read()).thenReturn(
            Lists.<ByteString>newArrayList(remoteStoreDescriptor.toByteString()).iterator());
        ORMapKey localKey = new ORMapKey() {
            @Override
            public ByteString key() {
                return ByteString.copyFromUtf8(localStoreId);
            }

            @Override
            public CausalCRDTType valueType() {
                return CausalCRDTType.mvreg;
            }
        };
        ORMapKey remoteKey = new ORMapKey() {
            @Override
            public ByteString key() {
                return ByteString.copyFromUtf8(remoteStoreId);
            }

            @Override
            public CausalCRDTType valueType() {
                return CausalCRDTType.mvreg;
            }
        };
        when(storeDescriptorMap.keys()).thenReturn(Sets.newHashSet(localKey, remoteKey).iterator());
        storeDescriptorReporter.report(storeDescriptor);
        Thread.sleep(400);
        inflationSubject.onNext(System.currentTimeMillis());
        verify(storeDescriptorMap, times(1)).execute(any(ORMapRemove.class));
    }

}
