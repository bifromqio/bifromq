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

package com.baidu.bifromq.basecrdt.core.api;

import com.baidu.bifromq.basecrdt.core.internal.InMemCRDTEngine;
import com.baidu.bifromq.basecrdt.proto.Replacement;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ICRDTEngine {
    static ICRDTEngine newInstance(CRDTEngineOptions crdtEngineOptions) {
        return new InMemCRDTEngine(crdtEngineOptions);
    }

    long id();

    Iterator<Replica> hosting();

    /**
     * Host a CRDT replica under specified URI if not host yet
     *
     * @param crdtURI
     * @return
     */
    Replica host(String crdtURI);

    /**
     * Host a CRDT replica using specified replicaId or return existing hosted replica. It's caller's duty to ensure the
     * replicaId provided is unique within the cluster
     *
     * @param crdtURI
     * @param replicaId
     * @return
     */
    Replica host(String crdtURI, ByteString replicaId);

    CompletableFuture<Void> stopHosting(String crdtURI);

    <O extends ICRDTOperation, C extends ICausalCRDT<O>> Optional<C> get(String crdtURI);

    Optional<Map<ByteString, NavigableMap<Long, Long>>> latticeEvents(String crdtURI);

    Optional<Map<ByteString, NavigableMap<Long, Long>>> historyEvents(String crdtURI);

    CompletableFuture<Void> join(String crdtURI, Iterable<Replacement> delta);

    CompletableFuture<Optional<Iterable<Replacement>>> delta(String crdtURI,
                                                             Map<ByteString, NavigableMap<Long, Long>>
                                                                 coveredLatticeEvents,
                                                             Map<ByteString, NavigableMap<Long, Long>>
                                                                 coveredHistoryEvents,
                                                             int maxEvents);

    void start();

    void stop();
}
