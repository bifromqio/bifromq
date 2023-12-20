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

package com.baidu.bifromq.basekv.store;

import static com.baidu.bifromq.basekv.store.CRDTUtil.getDescriptorFromCRDT;
import static com.baidu.bifromq.basekv.store.CRDTUtil.storeDescriptorMapCRDTURI;

import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVRangeStoreDescriptorReporter implements IKVRangeStoreDescriptorReporter {
    private static final long WAIT_CLEANUP_SPREAD_MILLIS = 1000L;

    private final CompositeDisposable disposables = new CompositeDisposable();
    private final ICRDTService crdtService;
    private final String clusterId;
    private final IORMap storeDescriptorMap;
    private final long deadStoreCleanupInMillis;

    private KVRangeStoreDescriptor localStoreDescriptor;

    public KVRangeStoreDescriptorReporter(@NonNull String clusterId,
                                          @NonNull ICRDTService crdtService,
                                          @NonNull Long deadStoreCleanupInMillis) {
        this.clusterId = clusterId;
        this.crdtService = crdtService;
        this.deadStoreCleanupInMillis = deadStoreCleanupInMillis;
        String uri = storeDescriptorMapCRDTURI(clusterId);
        crdtService.host(uri);
        Optional<IORMap> crdtOpt = crdtService.get(uri);
        assert crdtOpt.isPresent();
        storeDescriptorMap = crdtOpt.get();
    }

    @Override
    public void start() {
        disposables.add(storeDescriptorMap.inflation().subscribe(t -> checkAndHealStoreDescriptorList()));
    }

    @Override
    public void stop() {
        disposables.dispose();
        if (localStoreDescriptor != null) {
            removeStoreDescriptor(localStoreDescriptor.getId()).join();
            try {
                // ICausalCRDT could supply a way to wait localJoin spread to remote replicas
                Thread.sleep(WAIT_CLEANUP_SPREAD_MILLIS);
            } catch (InterruptedException e) {
                log.warn("Interrupted when waiting localStoreDescriptor cleanup spread", e);
            }
        }
        crdtService.stopHosting(storeDescriptorMapCRDTURI(clusterId)).join();
    }

    @Override
    public void report(KVRangeStoreDescriptor storeDescriptor) {
        localStoreDescriptor = storeDescriptor.toBuilder().build();
        log.trace("Report store descriptor: clusterId={}\n{}", clusterId, localStoreDescriptor);
        ByteString ormapKey = ByteString.copyFromUtf8(localStoreDescriptor.getId());
        Optional<KVRangeStoreDescriptor> settingsOnCRDT = getDescriptorFromCRDT(storeDescriptorMap, ormapKey);
        if (settingsOnCRDT.isEmpty() || !settingsOnCRDT.get().equals(localStoreDescriptor)) {
            storeDescriptorMap.execute(ORMapOperation
                .update(ormapKey)
                .with(MVRegOperation.write(localStoreDescriptor.toByteString())));
        }
    }

    private void checkAndHealStoreDescriptorList() {
        if (localStoreDescriptor == null) {
            return;
        }
        Map<ByteString, KVRangeStoreDescriptor> storedDescriptors = new HashMap<>();
        storeDescriptorMap.keys().forEachRemaining(ormapKey ->
            getDescriptorFromCRDT(storeDescriptorMap, ormapKey.key())
                .ifPresent(descriptor -> storedDescriptors.put(ormapKey.key(), descriptor)));
        ByteString ormapKey = ByteString.copyFromUtf8(localStoreDescriptor.getId());
        // check if some store is not alive anymore
        storedDescriptors.remove(ormapKey);
        for (KVRangeStoreDescriptor storeDescriptor : storedDescriptors.values()) {
            long elapsed = HLC.INST.getPhysical() - HLC.INST.getPhysical(storeDescriptor.getHlc());
            if (elapsed > deadStoreCleanupInMillis) {
                log.debug("store[{}] is not alive, remove its storeDescriptor", storeDescriptor.getId());
                removeStoreDescriptor(storeDescriptor.getId());
            }
        }
    }

    private CompletableFuture<Void> removeStoreDescriptor(String storeId) {
        return storeDescriptorMap.execute(ORMapOperation
            .remove(ByteString.copyFromUtf8(storeId))
            .of(CausalCRDTType.mvreg)
        );
    }

}
