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

package com.baidu.bifromq.basecrdt.store;

import com.baidu.bifromq.basecrdt.core.api.ICRDTOperation;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public interface ICRDTStore {
    /**
     * Construct a new instance
     *
     * @param options
     * @return
     */
    static ICRDTStore newInstance(@NonNull CRDTStoreOptions options) {
        return new CRDTStore(options);
    }

    /**
     * The global unique id of the store.
     * <br>
     * NOTE. To ensure the uniqueness in distributed deployment, make sure the time is synchronized with each
     * other(clock skew below 1S), and start the stores in serial with sufficient delay(at least 1 second).
     *
     * @return
     */
    long id();

    /**
     * Host a CRDT replica if not host yet. The replica will be assigned a internal generated globally unique id.
     *
     * @param crdtURI the name of the CRDT
     * @return the logical address
     */
    Replica host(String crdtURI);

    /**
     * Host a CRDT replica using the specified replicaId. It's caller's duty to ensure the uniqueness of the provided id
     * within the external managed cluster.
     *
     * @param crdtURI
     * @param replicaId
     * @return
     */
    Replica host(String crdtURI, ByteString replicaId);

    /**
     * Stop hosting the replica for given uri
     *
     * @param uri
     */
    CompletableFuture<Void> stopHosting(String uri);

    /**
     * Returns an iterator to iterate all CRDT replicas currently hosting
     *
     * @return the iterator from CRDTName to a set of logical replica addresses
     */
    Iterator<Replica> hosting();

    /**
     * Get the live replica
     *
     * @param uri the id of the hosted replica
     * @return the replica object of the CRDT
     */
    <O extends ICRDTOperation, T extends ICausalCRDT<O>> Optional<T> get(String uri);

    /**
     * Join a cluster from specified local address. Some remote members will be selected to be the neighbors with which
     * the local replica keeps synchronizing.
     *
     * @param localAddr the local addr
     * @param cluster   the list of member address
     */
    void join(String uri, ByteString localAddr, Set<ByteString> cluster);

    /**
     * Currently bind address of hosted replica in join cluster
     *
     * @return
     */
    Optional<ByteString> localAddr(String uri);

    /**
     * Currently joined cluster of given hosted replica
     *
     * @return
     */
    Optional<Set<ByteString>> cluster(String uri);

    /**
     * If the peer addr is the neighbor of the local replica in current cluster landscape, trigger a full sync with it
     *
     * @param uri
     * @param peerAddr
     */
    void sync(String uri, ByteString peerAddr);

    /**
     * An observable of messages originated from current store
     *
     * @return
     */
    Observable<CRDTStoreMessage> storeMessages();

    /**
     * Start the store by providing observable of incoming store messages
     * <br>
     * NOTE: the messages with toStoreId set to '0' is used for broadcast, and will be accepted by every CRDTStore
     *
     * @param replicaMessages the observable to receive messages from others
     */
    void start(Observable<CRDTStoreMessage> replicaMessages);

    /**
     * Stop the store
     */
    void stop();
}
