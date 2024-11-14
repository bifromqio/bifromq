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

package com.baidu.bifromq.basecrdt.service;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.core.api.ICRDTOperation;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * The CRDT service with decentralized membership management based on base-cluster.
 */
public interface ICRDTService {

    /**
     * Construct a new instance.
     *
     * @param options the service options
     * @return the CRDT service object
     */
    static ICRDTService newInstance(@NonNull CRDTServiceOptions options) {
        return new CRDTService(options);
    }

    /**
     * The id of the underlying CRDT store.
     *
     * @return the id of the store
     */
    String id();

    /**
     * The id of the local agent host.
     *
     * @return the id of the local agent host
     */
    ByteString agentHostId();

    /**
     * host a CRDT replica of given uri.
     *
     * @param uri the uri of the CRDT
     * @return the hosted CRDT replica object
     */
    <O extends ICRDTOperation, C extends ICausalCRDT<O>> C host(String uri);

    /**
     * Stop hosting the CRDT replica of given uri.
     *
     * @param uri the uri of the CRDT
     * @return a future of the operation
     */
    CompletableFuture<Void> stopHosting(String uri);

    /**
     * Get the membership observable of hosted CRDT.
     *
     * @param uri the uri of the CRDT
     * @return the observable of alive replicas
     */
    Observable<Set<Replica>> aliveReplicas(String uri);

    /**
     * Get the alive CRDTs within the crdt services.
     *
     * @return the set of uri of alive CRDTs
     */
    Observable<Set<String>> aliveCRDTs();

    boolean isStarted();

    /**
     * Start the store by providing agentHost.
     */
    void start(IAgentHost agentHost);

    /**
     * Stop the store.
     */
    void stop();
}
