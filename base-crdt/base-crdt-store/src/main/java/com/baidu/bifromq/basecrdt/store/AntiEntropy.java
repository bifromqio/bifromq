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

import static com.baidu.bifromq.basecrdt.core.util.LatticeIndexUtil.remember;
import static com.baidu.bifromq.basecrdt.store.MessagePayloadUtil.compressToPayload;
import static com.baidu.bifromq.basecrdt.util.Formatter.toStringifiable;

import com.baidu.bifromq.basecrdt.ReplicaLogger;
import com.baidu.bifromq.basecrdt.core.api.ICRDTEngine;
import com.baidu.bifromq.basecrdt.core.api.ICausalCRDT;
import com.baidu.bifromq.basecrdt.proto.Dot;
import com.baidu.bifromq.basecrdt.proto.Replacement;
import com.baidu.bifromq.basecrdt.store.compressor.Compressor;
import com.baidu.bifromq.basecrdt.store.proto.AckMessage;
import com.baidu.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.baidu.bifromq.basecrdt.store.proto.DeltaMessage;
import com.baidu.bifromq.basecrdt.store.proto.EventIndex;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

final class AntiEntropy {
    private final Logger log;
    private final ICausalCRDT<?> replica;
    private final ByteString localAddr;
    private final ByteString neighborAddr;
    private final ICRDTEngine engine;
    private final ScheduledExecutorService executor;
    private final Subject<CRDTStoreMessage> deltaSubject;
    private final Observer<ByteString> neighborLost;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final AtomicLong seqNo = new AtomicLong(0);
    private final AtomicLong ackSeqNo = new AtomicLong(0);
    private final AtomicInteger resendCount = new AtomicInteger();
    private final ConcurrentMap<ByteString, NavigableMap<Long, Long>> neighborLatticeEvents;
    private final ConcurrentMap<ByteString, NavigableMap<Long, Long>> neighborHistoryEvents;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final int maxEventsInDelta;
    private final Compressor compressor;
    private final Counter deltaMsgCounter;
    private final Counter deltaMsgBytesCounter;
    private volatile ScheduledFuture<?> resendTask = null;
    private volatile CRDTStoreMessage recentSent = null;

    AntiEntropy(ICausalCRDT<?> replica,
                ByteString localAddr,
                ByteString neighborAddr,
                ICRDTEngine engine,
                ScheduledExecutorService executor,
                Subject<CRDTStoreMessage> deltaSubject,
                int maxEventsInDelta,
                Observer<ByteString> neighborLost,
                Counter deltaMsgCounter,
                Counter deltaMsgBytesCounter,
                Compressor compressor) {
        this.log = new ReplicaLogger(replica.id(), AntiEntropy.class);
        this.replica = replica;
        this.localAddr = localAddr;
        this.neighborAddr = neighborAddr;
        this.engine = engine;
        this.executor = executor;
        this.deltaSubject = deltaSubject;
        this.maxEventsInDelta = maxEventsInDelta;
        this.neighborLost = neighborLost;
        this.compressor = compressor;
        this.deltaMsgCounter = deltaMsgCounter;
        this.deltaMsgBytesCounter = deltaMsgBytesCounter;
        this.neighborLatticeEvents = Maps.newConcurrentMap();
        this.neighborHistoryEvents = Maps.newConcurrentMap();
        disposable.add(replica.inflation().subscribe(d -> this.start()));
        start();
    }

    void start() {
        if (canceled.get()) {
            return;
        }
        if (running.compareAndSet(false, true)) {
            log.debug("Start anti-entropy to addr[{}]", toStringifiable(neighborAddr));
            resendCount.set(0);
            ackSeqNo.set(seqNo.get());
            resendTask = null;
            recentSent = null;
            executor.execute(this::tick);
        }
    }

    void handleDelta(DeltaMessage delta) {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            // merge events in delta in to currently known history of the neighbor
            for (Replacement replacement : delta.getReplacementList()) {
                for (Dot dot : replacement.getDotsList()) {
                    if (dot.hasLattice()) {
                        remember(neighborLatticeEvents, dot.getReplicaId(), dot.getVer());
                    } else {
                        remember(neighborHistoryEvents, dot.getReplicaId(), dot.getVer());
                    }
                }
            }
            start();
        }
    }

    void handleAck(AckMessage ack) {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            if (ackSeqNo.get() < ack.getSeqNo() && ack.getSeqNo() <= seqNo.get()) {
                log.trace("Accept ack[{}] from addr[{}]:\n{}", ack.getSeqNo(), toStringifiable(neighborAddr), ack);
                // replace by events in ack
                neighborLatticeEvents.clear();
                neighborHistoryEvents.clear();
                for (EventIndex idx : ack.getLatticeEventsList()) {
                    Map<Long, Long> ranges = neighborLatticeEvents
                        .computeIfAbsent(idx.getReplicaId(), k -> Maps.newTreeMap());
                    ranges.putAll(idx.getRangesMap());
                }
                for (EventIndex idx : ack.getHistoryEventsList()) {
                    Map<Long, Long> ranges = neighborHistoryEvents
                        .computeIfAbsent(idx.getReplicaId(), k -> Maps.newTreeMap());
                    ranges.putAll(idx.getRangesMap());
                }
                ackSeqNo.set(ack.getSeqNo());
                if (resendTask != null && !resendTask.isDone()) {
                    resendTask.cancel(false);
                    resendTask = null;
                }
                executor.schedule(this::tick, 0, TimeUnit.MILLISECONDS);
            } else {
                log.trace("Ignore ack[{}] from addr[{}]:\n{}", ack.getSeqNo(), toStringifiable(neighborAddr), ack);
            }
        }
    }

    void cancel() {
        if (canceled.compareAndSet(false, true)) {
            log.debug("Canceled anti-entropy to addr[{}] ", toStringifiable(neighborAddr));
            disposable.dispose();
            canceled.set(true);
        }
    }

    private void tick() {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            assert seqNo.get() >= ackSeqNo.get();
            if (seqNo.get() > ackSeqNo.get()) {
                // the recent delta has not been ack'ed
                if (resendCount.get() > 10) {
                    neighborLost.onNext(this.neighborAddr);
                    // restart the task
                    running.set(false);
                    start();
                    return;
                }
                resendCount.incrementAndGet();
                log.trace("Resend last delta to addr[{}] from addr[{}]:\n{}",
                    toStringifiable(neighborAddr), toStringifiable(localAddr), recentSent);
                deltaMsgCounter.increment(1D);
                deltaMsgBytesCounter.increment(recentSent.getSerializedSize());
                deltaSubject.onNext(recentSent);
                resendTask = executor.schedule(this::tick, delay(), TimeUnit.MILLISECONDS);
            } else {
                if (neighborLatticeEvents.isEmpty() && seqNo.get() == 0) {
                    // empty delta for getting complete OAH from neighbor
                    recentSent = CRDTStoreMessage.newBuilder()
                        .setUri(replica.id().getUri())
                        .setSender(localAddr)
                        .setReceiver(neighborAddr)
                        .setPayload(compressToPayload(compressor,
                            DeltaMessage.newBuilder().setSeqNo(seqNo.incrementAndGet()).build()))
                        .build();
                    resendCount.set(0);
                    log.trace("Send empty delta to addr[{}] from addr[{}]:\n{}",
                        toStringifiable(neighborAddr), toStringifiable(localAddr), recentSent);
                    deltaMsgCounter.increment(1D);
                    deltaMsgBytesCounter.increment(recentSent.getSerializedSize());
                    deltaSubject.onNext(recentSent);
                    resendTask = executor.schedule(this::tick, delay(), TimeUnit.MILLISECONDS);
                } else {
                    engine.delta(replica.id().getUri(), neighborLatticeEvents, neighborHistoryEvents, maxEventsInDelta)
                        .whenComplete((delta, e) -> {
                            if (e != null) {
                                log.error("Failed to calculate delta for neighbor[{}]", toStringifiable(neighborAddr),
                                    e);
                                return;
                            }
                            if (delta.isPresent()) {
                                recentSent = CRDTStoreMessage.newBuilder()
                                    .setUri(replica.id().getUri())
                                    .setSender(localAddr)
                                    .setReceiver(neighborAddr)
                                    .setPayload(compressToPayload(compressor,
                                        DeltaMessage.newBuilder()
                                            .setSeqNo(seqNo.incrementAndGet())
                                            .addAllReplacement(delta.get())
                                            .build()))
                                    .build();
                                resendCount.set(0);
                                log.trace("Send delta to addr[{}] from addr[{}]:\n{}",
                                    toStringifiable(neighborAddr), toStringifiable(localAddr), recentSent);
                                deltaMsgCounter.increment(1D);
                                deltaMsgBytesCounter.increment(recentSent.getSerializedSize());
                                deltaSubject.onNext(recentSent);
                                resendTask = executor.schedule(this::tick, delay(), TimeUnit.MILLISECONDS);
                            } else {
                                running.set(false);
                                // there are new inflation happened
                                tryTick();
                            }
                        });
                }
            }
        }
    }

    private void tryTick() {
        engine.delta(replica.id().getUri(), neighborLatticeEvents, neighborHistoryEvents, maxEventsInDelta)
            .whenComplete((delta, e) -> {
                if (delta.isPresent()) {
                    start();
                }
            });
    }

    private long delay() {
        return ThreadLocalRandom.current().nextLong(500, 2000) * (resendCount.get() + 1);
    }
}
