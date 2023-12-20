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

import static com.baidu.bifromq.basecrdt.util.Formatter.toStringifiable;

import com.baidu.bifromq.basecrdt.ReplicaLogger;
import com.baidu.bifromq.basecrdt.core.api.ICRDTEngine;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Replacement;
import com.baidu.bifromq.basecrdt.store.compressor.Compressor;
import com.baidu.bifromq.basecrdt.store.proto.AckMessage;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.baidu.bifromq.basecrdt.store.proto.DeltaMessage;
import com.baidu.bifromq.basecrdt.store.proto.EventIndex;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Manage a hosted replica's anti-entropy process with its neighbors
 */
final class AntiEntropyManager {

    private class MetricManager {
        private final Counter sendDeltaNum;
        private final Counter sendDeltaBytes;
        private final Counter receiveDeltaNum;
        private final Counter receiveDeltaBytes;
        private final Counter sendAckNum;
        private final Counter sendAckBytes;
        private final Counter receiveAckNum;
        private final Counter receiveAckBytes;
        private final Set<Meter> meters = new HashSet<>();

        MetricManager(Tags tags) {
            meters.add(Gauge.builder("basecrdt.neighbor", antiEntropyMap, Map::size)
                .tags(tags)
                .register(Metrics.globalRegistry)
            );
            sendDeltaNum = Metrics.counter("basecrdt.send.delta.count", tags);
            sendDeltaBytes = Metrics.counter("basecrdt.send.delta.bytes", tags);
            receiveDeltaNum = Metrics.counter("basecrdt.receive.delta.count", tags);
            receiveDeltaBytes = Metrics.counter("basecrdt.receive.delta.bytes", tags);
            sendAckNum = Metrics.counter("basecrdt.send.ack.count", tags);
            sendAckBytes = Metrics.counter("basecrdt.send.ack.bytes", tags);
            receiveAckNum = Metrics.counter("basecrdt.receive.ack.count", tags);
            receiveAckBytes = Metrics.counter("basecrdt.receive.ack.bytes", tags);
        }

        void close() {
            meters.forEach(meter -> Metrics.globalRegistry.removeByPreFilterId(meter.getId()));
            Metrics.globalRegistry.removeByPreFilterId(sendAckNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendAckBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveAckNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveAckBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendDeltaNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendDeltaBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveDeltaNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveDeltaBytes.getId());
        }
    }

    private final Logger log;
    private final ICausalCRDT<?> replica;
    private final ByteString localAddr;
    private final ICRDTEngine engine;
    private final Subject<CRDTStoreMessage> messageSource;
    private final ScheduledExecutorService executor;
    private final Map<ByteString, AntiEntropy> antiEntropyMap; // peerAddr to anti entropy
    private final Set<ByteString> lostNeighbors = new HashSet<>();
    private final Subject<ByteString> neighborLost = PublishSubject.<ByteString>create().toSerialized();
    private final NavigableSet<ByteString> cluster = Sets.newTreeSet(ByteString.unsignedLexicographicalComparator());
    private final Cache<ByteString, Long> ackCache;
    private final int maxEventsInDelta;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final Compressor compressor;
    private final MetricManager metricManager;

    public AntiEntropyManager(ICausalCRDT<?> replica,
                              ByteString localAddr, // address from which local replica could be reached with in cluster
                              ICRDTEngine engine,
                              ScheduledExecutorService executor,
                              Subject<CRDTStoreMessage> msgSource,
                              int maxEventsInDelta,
                              Compressor compressor) {
        this.log = new ReplicaLogger(replica.id(), AntiEntropyManager.class);
        this.replica = replica;
        this.localAddr = localAddr;
        this.engine = engine;
        this.executor = executor;
        messageSource = msgSource;
        antiEntropyMap = Maps.newConcurrentMap();
        this.maxEventsInDelta = maxEventsInDelta;
        this.compressor = compressor;
        ackCache = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofSeconds(30))
            .build();
        disposable.add(neighborLost.subscribe(lostNeighbourAddr -> {
            this.lostNeighbors.add(lostNeighbourAddr);
            log.debug("Add lostNeighbor[{}]", lostNeighbourAddr);
            if (antiEntropyMap.containsKey(lostNeighbourAddr)) {
                resetNeighbors();
            }
        }));
        this.metricManager = new MetricManager(Tags.of("store.id", Long.toUnsignedString(engine.id()))
            .and("replica.uri", replica.id().getUri())
            .and("replica.id", BaseEncoding.base64().encode(replica.id().getId().toByteArray())));
    }

    ByteString getLocalAddr() {
        return localAddr;
    }

    synchronized void setCluster(Set<ByteString> cluster) {
        this.cluster.clear();
        this.cluster.addAll(cluster);
        this.lostNeighbors.clear();
        resetNeighbors();
    }

    synchronized Set<ByteString> cluster() {
        return Sets.newHashSet(this.cluster);
    }

    void resetIfNeighbor(ByteString peerAddr) {
        antiEntropyMap.computeIfPresent(peerAddr, (k, v) -> {
            v.cancel();
            return new AntiEntropy(replica, localAddr, peerAddr, engine, executor,
                messageSource, maxEventsInDelta, neighborLost, metricManager.sendDeltaNum,
                metricManager.sendDeltaBytes, compressor);
        });
    }

    synchronized void resetNeighbors() {
        Set<ByteString> neighborAddrs = PartialMesh.neighbors(this.cluster, lostNeighbors, localAddr);
        Set<ByteString> removeAddrs = Sets.newHashSet(Sets.difference(antiEntropyMap.keySet(), neighborAddrs));
        for (ByteString neighborAddr : neighborAddrs) {
            antiEntropyMap.computeIfAbsent(neighborAddr, k -> {
                log.trace("Add new neighbor[{}]", toStringifiable(neighborAddr));
                return new AntiEntropy(replica, localAddr, neighborAddr, engine,
                    executor, messageSource, maxEventsInDelta, neighborLost,
                    metricManager.sendDeltaNum, metricManager.sendDeltaBytes, compressor);
            });
        }
        for (ByteString removeAddr : removeAddrs) {
            log.trace("Remove neighbor[{}]", toStringifiable(removeAddr));
            antiEntropyMap.remove(removeAddr).cancel();
        }

        if (neighborAddrs.isEmpty()) {
            executor.schedule(this::probe, 60, TimeUnit.SECONDS);
        }
    }

    synchronized void probe() {
        if (antiEntropyMap.isEmpty() && !lostNeighbors.isEmpty()) {
            log.trace("No neighbors, try probing lostNeighbors");
            lostNeighbors.forEach(addr -> {
                CRDTStoreMessage probeMessage = CRDTStoreMessage.newBuilder()
                    .setUri(replica.id().getUri())
                    .setSender(localAddr)
                    .setReceiver(addr)
                    .setPayload(MessagePayloadUtil.compressToPayload(compressor,
                        DeltaMessage.newBuilder().build()))
                    .build();
                metricManager.sendDeltaNum.increment();
                metricManager.sendDeltaBytes.increment(probeMessage.getSerializedSize());
                messageSource.onNext(probeMessage);
            });
            executor.schedule(this::probe, 60, TimeUnit.SECONDS);
        }
    }

    void join(DeltaMessage delta, ByteString sender) {
        log.trace("Join delta[{}] from addr[{}]:\n{}", delta.getSeqNo(), toStringifiable(sender), delta);
        metricManager.receiveDeltaNum.increment(1D);
        metricManager.receiveDeltaBytes.increment(delta.getSerializedSize());
        String uri = replica.id().getUri();
        Long ackSeqNo = ackCache.getIfPresent(sender);
        if (ackSeqNo != null && ackSeqNo >= delta.getSeqNo()) {
            // don't trigger excessive inflation on duplicated deltas
            Optional<Map<ByteString, NavigableMap<Long, Long>>> latticeEvents = engine.latticeEvents(uri);
            Optional<Map<ByteString, NavigableMap<Long, Long>>> historyEvents = engine.historyEvents(uri);
            if (latticeEvents.isPresent() && historyEvents.isPresent()) {
                CRDTStoreMessage msg = CRDTStoreMessage.newBuilder()
                    .setUri(uri)
                    .setReceiver(sender)
                    .setSender(localAddr)
                    .setPayload(MessagePayloadUtil.compressToPayload(compressor,
                        AckMessage.newBuilder()
                            .setSeqNo(delta.getSeqNo())
                            .addAllLatticeEvents(serialize(latticeEvents.get()))
                            .addAllHistoryEvents(serialize(historyEvents.get()))
                            .build()))
                    .build();
                metricManager.sendAckNum.increment(1D);
                metricManager.sendAckBytes.increment(msg.getSerializedSize());
                messageSource.onNext(msg);
            }
            ackCache.invalidate(sender);
        } else if (delta.getReplacementList().isEmpty()) {
            Optional<Map<ByteString, NavigableMap<Long, Long>>> latticeEvents = engine.latticeEvents(uri);
            Optional<Map<ByteString, NavigableMap<Long, Long>>> historyEvents = engine.historyEvents(uri);
            if (latticeEvents.isPresent() && historyEvents.isPresent()) {
                log.trace("Reply probe ack[{}] to addr[{}]", delta.getSeqNo(), toStringifiable(sender));
                CRDTStoreMessage msg = CRDTStoreMessage.newBuilder()
                    .setUri(uri)
                    .setReceiver(sender)
                    .setSender(localAddr)
                    .setPayload(MessagePayloadUtil.compressToPayload(compressor,
                        AckMessage.newBuilder()
                            .setSeqNo(delta.getSeqNo())
                            .addAllLatticeEvents(serialize(latticeEvents.get()))
                            .addAllHistoryEvents(serialize(historyEvents.get()))
                            .build()))
                    .build();
                ackCache.put(sender, delta.getSeqNo());
                metricManager.sendAckNum.increment(1D);
                metricManager.sendAckBytes.increment(msg.getSerializedSize());
                messageSource.onNext(msg);
            }
            // receive delta message from a new sender, add it to neighbors
            if (delta.getSeqNo() == 1 && !antiEntropyMap.containsKey(sender)) {
                log.debug("Add sender[{}] to neighbors", toStringifiable(sender));
                antiEntropyMap.computeIfAbsent(sender, k ->
                    new AntiEntropy(replica, localAddr, sender, engine,
                        executor, messageSource, maxEventsInDelta, neighborLost,
                        metricManager.sendDeltaNum, metricManager.sendDeltaBytes,
                        compressor));
            }
        } else if (ackSeqNo == null || ackSeqNo < delta.getSeqNo()) {
            engine.join(uri, delta.getReplacementList())
                .whenComplete((v, e) -> {
                    if (e == null) {
                        Optional<Map<ByteString, NavigableMap<Long, Long>>> latticeEvents =
                            engine.latticeEvents(uri);
                        Optional<Map<ByteString, NavigableMap<Long, Long>>> historyEvents =
                            engine.historyEvents(uri);
                        if (latticeEvents.isPresent() && historyEvents.isPresent()) {
                            log.trace("Reply ack[{}] to addr[{}]", delta.getSeqNo(), toStringifiable(sender));
                            CRDTStoreMessage msg = CRDTStoreMessage.newBuilder()
                                .setUri(uri)
                                .setReceiver(sender)
                                .setSender(localAddr)
                                .setPayload(MessagePayloadUtil.compressToPayload(compressor,
                                    AckMessage.newBuilder()
                                        .setSeqNo(delta.getSeqNo())
                                        .addAllLatticeEvents(serialize(latticeEvents.get()))
                                        .addAllHistoryEvents(serialize(historyEvents.get()))
                                        .build()))
                                .build();
                            ackCache.put(sender, delta.getSeqNo());
                            metricManager.sendAckNum.increment(1D);
                            metricManager.sendAckBytes.increment(msg.getSerializedSize());
                            messageSource.onNext(msg);
                            AntiEntropy a = antiEntropyMap.get(sender);
                            if (a != null) {
                                // trigger anti-entropy if necessary
                                a.handleDelta(delta);
                            }
                        }
                    }
                });
        }
    }

    void receive(AckMessage ack, ByteString sender) {
        metricManager.receiveAckNum.increment(1D);
        metricManager.receiveAckBytes.increment(ack.getSerializedSize());
        AntiEntropy a = antiEntropyMap.get(sender);
        if (a != null) {
            log.trace("Receive ack[{}] from addr[{}]:\n{}", ack.getSeqNo(), toStringifiable(sender), ack);
            a.handleAck(ack);
        } else if (lostNeighbors.contains(sender)) {
            synchronized (this) {
                log.trace("Receive probe ack from lost neighbor[{}]", toStringifiable(sender));
                lostNeighbors.remove(sender);
                if (antiEntropyMap.isEmpty()) {
                    log.trace("Add [{}] to neighbors", toStringifiable(sender));
                    antiEntropyMap.computeIfAbsent(sender, k ->
                        new AntiEntropy(replica, localAddr, sender, engine,
                            executor, messageSource, maxEventsInDelta, neighborLost,
                            metricManager.sendDeltaNum, metricManager.sendDeltaBytes, compressor));
                }
            }
        } else {
            log.debug("Ignore ack[{}] from addr[{}]:\n{}", ack.getSeqNo(), toStringifiable(sender), ack);
        }
    }

    void stop() {
        log.debug("Stop anti-entropy manager");
        antiEntropyMap.values().forEach(AntiEntropy::cancel);
        disposable.dispose();
        this.metricManager.close();
    }

    private List<EventIndex> serialize(Map<ByteString, NavigableMap<Long, Long>> latticeEvents) {
        List<EventIndex> histories = new ArrayList<>(latticeEvents.size());
        latticeEvents.forEach((k, v) -> {
            EventIndex.Builder builder = EventIndex.newBuilder().setReplicaId(k);
            // do not use builder.putAllRanges() to avoid NPE
            v.forEach(builder::putRanges);
            histories.add(builder.build());
        });
        return histories;
    }

    private Set<ByteString> contributors(List<Replacement> replacements) {
        return replacements.stream().map(r -> r.getDots(0).getReplicaId()).collect(Collectors.toSet());
    }
}
