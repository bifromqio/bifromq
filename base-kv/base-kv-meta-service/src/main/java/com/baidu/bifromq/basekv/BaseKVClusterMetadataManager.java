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

package com.baidu.bifromq.basekv;

import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IMVReg;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.proto.DescriptorKey;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.LoadRules;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BaseKVClusterMetadataManager implements IBaseKVClusterMetadataManager {
    private static final LoadRules DEFAULT_LOAD_RULES = LoadRules.newBuilder().setLoadRules("{}").build();

    private record StoreDescriptorAndReplicas(Map<DescriptorKey, KVRangeStoreDescriptor> descriptorMap,
                                              Set<ByteString> replicaIds) {
    }

    private static final ByteString ORMapKey_LoadRules = ByteString.copyFromUtf8("LoadRules");
    private static final ByteString ORMapKey_Landscape = ByteString.copyFromUtf8("Landscape");
    private final IMVReg loadRulesMVReg;
    private final IORMap landscapeORMap;
    private final Subject<LoadRules> loadRulesSubject = BehaviorSubject.createDefault(DEFAULT_LOAD_RULES);
    private final BehaviorSubject<Map<DescriptorKey, KVRangeStoreDescriptor>> landscapeSubject =
        BehaviorSubject.createDefault(Collections.emptyMap());
    private final CompositeDisposable disposable = new CompositeDisposable();

    public BaseKVClusterMetadataManager(IORMap basekvClusterDescriptor,
                                        Observable<Set<Replica>> aliveReplicas) {
        this.loadRulesMVReg = basekvClusterDescriptor.getMVReg(ORMapKey_LoadRules);
        this.landscapeORMap = basekvClusterDescriptor.getORMap(ORMapKey_Landscape);
        disposable.add(landscapeORMap.inflation()
            .map(this::buildLandscape)
            .subscribe(landscapeSubject::onNext));
        disposable.add(loadRulesMVReg.inflation()
            .mapOptional(this::buildLoadRules)
            .subscribe(loadRulesSubject::onNext));
        disposable.add(Observable.combineLatest(landscapeSubject, aliveReplicas
                    .map(replicas -> replicas.stream().map(Replica::getId).collect(Collectors.toSet())),
                (StoreDescriptorAndReplicas::new))
            .subscribe(this::checkAndHealStoreDescriptorList));
    }

    @Override
    public Observable<String> loadRules() {
        return loadRulesSubject.map(LoadRules::getLoadRules);
    }

    @Override
    public CompletableFuture<Void> setLoadRules(String loadRules) {
        Optional<LoadRules> rulesOnCRDT = buildLoadRules(System.currentTimeMillis());
        if (rulesOnCRDT.isEmpty() || !rulesOnCRDT.get().getLoadRules().equals(loadRules)) {
            return loadRulesMVReg.execute(MVRegOperation.write(LoadRules.newBuilder()
                .setLoadRules(loadRules)
                .setHlc(HLC.INST.get())
                .build().toByteString()));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Observable<Map<String, KVRangeStoreDescriptor>> landscape() {
        return landscapeSubject
            .map(descriptorMap -> {
                Map<String, KVRangeStoreDescriptor> descriptorMapByStoreId = new HashMap<>();
                descriptorMap.forEach((key, value) -> descriptorMapByStoreId.compute(key.getStoreId(), (k, v) -> {
                    if (v == null) {
                        return value;
                    }
                    return v.getHlc() > value.getHlc() ? v : value;
                }));
                return descriptorMapByStoreId;
            });
    }

    @Override
    public Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId) {
        return Optional.ofNullable(landscapeSubject.getValue().get(toDescriptorKey(storeId)));
    }

    @Override
    public CompletableFuture<Void> report(KVRangeStoreDescriptor descriptor) {
        DescriptorKey descriptorKey = toDescriptorKey(descriptor.getId());
        Optional<KVRangeStoreDescriptor> descriptorOnCRDT =
            buildLandscape(landscapeORMap.getMVReg(descriptorKey.toByteString()));
        if (descriptorOnCRDT.isEmpty() || !descriptorOnCRDT.get().equals(descriptor)) {
            return landscapeORMap.execute(ORMapOperation
                .update(descriptorKey.toByteString())
                .with(MVRegOperation.write(descriptor.toByteString())));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopReport(String storeId) {
        return removeDescriptorFromCRDT(toDescriptorKey(storeId));
    }

    public void stop() {
        disposable.dispose();
        loadRulesSubject.onComplete();
        landscapeSubject.onComplete();
    }

    private Optional<LoadRules> buildLoadRules(long ts) {
        List<LoadRules> l = Lists.newArrayList(Iterators.filter(Iterators.transform(loadRulesMVReg.read(), b -> {
            try {
                return LoadRules.parseFrom(b);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse LoadRules", e);
                return null;
            }
        }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private Map<DescriptorKey, KVRangeStoreDescriptor> buildLandscape(long ts) {
        Map<DescriptorKey, KVRangeStoreDescriptor> landscape = new HashMap<>();
        landscapeORMap.keys().forEachRemaining(ormapKey ->
            buildLandscape(landscapeORMap.getMVReg(ormapKey.key()))
                .ifPresent(descriptor -> landscape.put(parseDescriptorKey(ormapKey.key()), descriptor)));
        return landscape;
    }

    private Optional<KVRangeStoreDescriptor> buildLandscape(IMVReg mvReg) {
        List<KVRangeStoreDescriptor> l = Lists.newArrayList(Iterators.filter(Iterators.transform(mvReg.read(), b -> {
            try {
                return KVRangeStoreDescriptor.parseFrom(b);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse KVRangeStoreDescriptor", e);
                return null;
            }
        }), Objects::nonNull));
        l.sort((a, b) -> Long.compareUnsigned(b.getHlc(), a.getHlc()));
        return Optional.ofNullable(l.isEmpty() ? null : l.get(0));
    }

    private void checkAndHealStoreDescriptorList(StoreDescriptorAndReplicas storeDescriptorAndReplicas) {
        Map<DescriptorKey, KVRangeStoreDescriptor> storedDescriptors = storeDescriptorAndReplicas.descriptorMap;
        Set<ByteString> aliveReplicas = storeDescriptorAndReplicas.replicaIds;
        for (DescriptorKey descriptorKey : storedDescriptors.keySet()) {
            if (!aliveReplicas.contains(descriptorKey.getReplicaId())) {
                log.debug("store[{}] is not alive, remove its storeDescriptor", descriptorKey.getStoreId());
                removeDescriptorFromCRDT(descriptorKey);
            }
        }
    }

    private CompletableFuture<Void> removeDescriptorFromCRDT(DescriptorKey key) {
        return landscapeORMap.execute(ORMapOperation.remove(key.toByteString()).of(CausalCRDTType.mvreg));
    }

    private DescriptorKey toDescriptorKey(String storeId) {
        return DescriptorKey.newBuilder()
            .setStoreId(storeId)
            .setReplicaId(landscapeORMap.id().getId())
            .build();
    }

    private DescriptorKey parseDescriptorKey(ByteString key) {
        try {
            return DescriptorKey.parseFrom(key);
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse DescriptorKey", e);
            return null;
        }
    }
}
