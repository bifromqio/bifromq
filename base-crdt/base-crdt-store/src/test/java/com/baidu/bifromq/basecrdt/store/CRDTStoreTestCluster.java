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


import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTStoreTestCluster {
    @AllArgsConstructor
    static class CRDTStoreMeta {
        final CRDTStoreOptions options;
        final double packetLossPercent;
        final long packetDelayTime;
        final boolean packetRandom;
    }

    private final Map<String, CRDTStoreMeta> storeOptionsMap = Maps.newConcurrentMap();
    private final Map<String, Long> storeIdMap = Maps.newConcurrentMap();
    private final Map<Long, ICRDTStore> storeMap = Maps.newConcurrentMap();
    private final Map<Long, Subject<CRDTStoreMessage>> storeReceiverMap = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();

    public List<String> stores() {
        return Lists.newArrayList(storeIdMap.keySet());
    }

    public String newStore(String storeId, CRDTStoreMeta meta) {
        storeOptionsMap.computeIfAbsent(storeId, k -> {
            loadStore(storeId, meta);
            return meta;
        });
        return storeId;
    }

    public void stopStore(String storeId) {
        checkStore(storeId);
        storeMap.remove(storeIdMap.remove(storeId)).stop();
        storeReceiverMap.remove(storeId);
    }

    public Replica host(String storeId, String uri) {
        checkStore(storeId);
        Replica replica = getStore(storeId).host(uri);
        NavigableSet<ByteString> members = Sets.newTreeSet(ByteString.unsignedLexicographicalComparator());
        members.add(replica.getId());
        getStore(storeId).join(uri, replica.getId(), members);
        return replica;
    }

    public void join(String storeId, String uri, ByteString localAddr, ByteString... memberAddrs) {
        checkStore(storeId);
        Set<ByteString> membership = Sets.newHashSet();
        for (ByteString memberAddr : memberAddrs) {
            membership.add(memberAddr);
        }
        getStore(storeId).join(uri, localAddr, membership);
    }

    public void sync(String storeId, String uri, ByteString peerAddr) {
        checkStore(storeId);
        getStore(storeId).sync(uri, peerAddr);
    }

    public ICRDTStore getStore(String storeId) {
        checkStore(storeId);
        return storeMap.get(storeIdMap.get(storeId));
    }

    private long loadStore(String storeKey, CRDTStoreMeta meta) {
        Subject<CRDTStoreMessage> receiverSubject = PublishSubject.<CRDTStoreMessage>create().toSerialized();
        Observable<CRDTStoreMessage> receiver = receiverSubject;
        if (meta.packetLossPercent > 0) {
            receiver = receiverSubject
                .filter(t -> ThreadLocalRandom.current().nextDouble() > meta.packetLossPercent);
        }
        if (meta.packetDelayTime > 0) {
            receiver = receiverSubject.flatMap(t -> Observable.just(t) // reorder
                .delay(meta.packetDelayTime, TimeUnit.MILLISECONDS));
        }
        if (meta.packetRandom) {
            receiver = receiverSubject.flatMap(t -> Observable.just(t) // reorder
                .delay(ThreadLocalRandom.current().nextInt(2000), TimeUnit.MILLISECONDS));
        }
        ICRDTStore store = ICRDTStore.newInstance(meta.options);
        store.start(receiver);
        storeIdMap.put(storeKey, store.id());
        storeMap.put(store.id(), store);
        storeReceiverMap.put(store.id(), receiverSubject);
        disposables.add(store.storeMessages()
            .observeOn(Schedulers.io())
            .subscribe(msg -> {
                long storeId = hostStoreId(msg.getReceiver());
                if (storeReceiverMap.containsKey(storeId)) {
//                        log.trace("Forward message {} to target store[{}]", msg, storeId);
//                        Log.info(log, "Store[{}] forward {} message[size:{}] to target store[{}]:\n{}",
//                                store.id(), msg.getMsgTypeCase(), msg.getSerializedSize(), storeId,
//                                (Log.Stringify) () -> {
//                                    try {
//                                        switch (msg.getMsgTypeCase()) {
//                                            case DELTA:
//                                                DeltaMessage.Builder builder = msg.getDelta().toBuilder();
//                                                for (int i = 0; i < builder.getReplacementCount(); i++) {
//                                                    Replacement replacement = builder.getReplacement(i);
//                                                    if (replacement.getDots(0).hasLattice()) {
//                                                        builder.setReplacement(i, replacement.toBuilder()
//                                                                .setDots(0, replacement.getDots(0).toBuilder()
//                                                                        .setLattice(StateLattice.getDefaultInstance())
//                                                                        .build())
//                                                                .build());
//                                                    }
//                                                }
//                                                return JsonFormat.printer()
//                                                        .print(msg.toBuilder().setDelta(builder.build()).build());
//                                            case ACK:
//                                            default:
//                                                return JsonFormat.printer().print(msg);
//                                        }
//                                    } catch (Exception e) {
//                                        return msg.toString();
//                                    }
//                                });
                    storeReceiverMap.get(storeId).onNext(msg);
                } else {
                    log.debug("Drop message {} from store[{}]", msg, hostStoreId(msg.getSender()));
                }
            }));
        return store.id();
    }

    public void shutdown() {
        disposables.dispose();
        storeIdMap.keySet().forEach(this::stopStore);
    }

    public long hostStoreId(ByteString replicaAddr) {
        return toLong(replicaAddr.substring(0, Long.BYTES));
    }

    public long toLong(ByteString b) {
        assert b.size() == Long.BYTES;
        ByteBuffer buffer = b.asReadOnlyByteBuffer();
        return buffer.getLong();
    }

    private void checkStore(String storeId) {
        Preconditions.checkArgument(storeIdMap.containsKey(storeId));
    }
}
