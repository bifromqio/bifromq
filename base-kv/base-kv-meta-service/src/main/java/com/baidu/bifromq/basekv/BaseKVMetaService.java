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

import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BaseKVMetaService implements IBaseKVMetaService {
    private static final String BaseKVDescriptorCRDTURI = CRDTURI.toURI(CausalCRDTType.ormap, "base-kv-descriptor");
    private final ICRDTService crdtService;
    private final BehaviorSubject<Set<String>> clusterIdsSubject =
        BehaviorSubject.createDefault(Collections.emptySet());
    private final Map<String, BaseKVClusterMetadataManager> metadataManagers = new ConcurrentHashMap<>();
    private final IORMap basekvDescriptor;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Duration proposalTimeout;

    BaseKVMetaService(ICRDTService crdtService) {
        this(crdtService, Duration.ofSeconds(5));
    }

    BaseKVMetaService(ICRDTService crdtService, Duration proposalTimeout) {
        this.crdtService = crdtService;
        this.proposalTimeout = proposalTimeout;
        basekvDescriptor = crdtService.host(BaseKVDescriptorCRDTURI);
        disposables.add(basekvDescriptor.inflation().subscribe(ts -> {
            Set<String> clusterIds = new HashSet<>();
            basekvDescriptor.keys().forEachRemaining(ormapKey -> clusterIds.add(ormapKey.key().toStringUtf8()));
            clusterIdsSubject.onNext(clusterIds);
        }));
    }

    @Override
    public Observable<Set<String>> clusterIds() {
        return clusterIdsSubject.distinctUntilChanged();
    }

    @Override
    public IBaseKVClusterMetadataManager metadataManager(String clusterId) {
        return metadataManagers.computeIfAbsent(clusterId,
            k -> new BaseKVClusterMetadataManager(basekvDescriptor.getORMap(ByteString.copyFromUtf8(clusterId)),
                crdtService.aliveReplicas(BaseKVDescriptorCRDTURI), proposalTimeout));
    }

    @Override
    public void stop() {
        disposables.dispose();
        metadataManagers.values().forEach(BaseKVClusterMetadataManager::stop);
        crdtService.stopHosting(BaseKVDescriptorCRDTURI).join();
    }
}
