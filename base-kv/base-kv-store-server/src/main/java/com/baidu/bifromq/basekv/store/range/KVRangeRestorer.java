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

package com.baidu.bifromq.basekv.store.range;

import com.baidu.bifromq.basekv.proto.KVPair;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataReply;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataRequest;
import com.baidu.bifromq.basekv.proto.SnapshotSyncRequest;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.logger.SiftLogger;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

class KVRangeRestorer {
    private final Logger log;
    private final IKVRange range;
    private final ISnapshotEnsurer snapshotEnsurer;
    private final IKVRangeMessenger messenger;
    private final IKVRangeMetricManager metricManager;
    private final Executor executor;
    private final int idleTimeSec;
    private final AtomicReference<RestoreSession> currentSession = new AtomicReference<>();

    KVRangeRestorer(KVRangeSnapshot startSnapshot,
                    IKVRange range,
                    ISnapshotEnsurer snapshotEnsurer,
                    IKVRangeMessenger messenger,
                    IKVRangeMetricManager metricManager,
                    Executor executor,
                    int idleTimeSec,
                    String... tags) {
        this.range = range;
        this.snapshotEnsurer = snapshotEnsurer;
        this.messenger = messenger;
        this.metricManager = metricManager;
        this.executor = executor;
        this.idleTimeSec = idleTimeSec;
        this.log = SiftLogger.getLogger(KVRangeRestorer.class, tags);
        RestoreSession initialSession = new RestoreSession(startSnapshot);
        initialSession.doneFuture.complete(null);
        currentSession.set(initialSession);
    }

    public CompletableFuture<Void> awaitDone() {
        return currentSession.get().doneFuture.exceptionally(ex -> null);
    }

    public CompletableFuture<Void> restoreFrom(String leader, KVRangeSnapshot rangeSnapshot) {
        RestoreSession session = new RestoreSession(rangeSnapshot);
        RestoreSession prevSession = currentSession.getAndSet(session);
        if (!prevSession.snapshot.equals(rangeSnapshot) && !prevSession.doneFuture.isDone()) {
            // cancel previous restore session
            log.debug("Cancel previous restore session: rangeId={}\n{}",
                KVRangeIdUtil.toString(range.id()), prevSession.snapshot.toByteString());
            prevSession.doneFuture.cancel(true);
        }
        CompletableFuture<Void> onDone = session.doneFuture;
        snapshotEnsurer.ensure(rangeSnapshot.getId(), rangeSnapshot.getVer(), rangeSnapshot.getBoundary())
            .whenCompleteAsync((v, e) -> {
                if (onDone.isCancelled()) {
                    return;
                }
                if (e != null) {
                    log.error("Ensured snapshot's compatibility error: rangeId={}",
                        KVRangeIdUtil.toString(range.id()), e);
                    onDone.completeExceptionally(e);
                    return;
                }
                log.debug("Setup restore session: rangeId={}", KVRangeIdUtil.toString(range.id()));
                try {
                    String sessionId = UUID.randomUUID().toString();
                    IKVReseter restorer = range.toReseter(rangeSnapshot);
                    DisposableObserver<KVRangeMessage> observer = messenger.receive()
                        .filter(m -> m.hasSaveSnapshotDataRequest() &&
                            m.getSaveSnapshotDataRequest().getSessionId().equals(sessionId))
                        .timeout(idleTimeSec, TimeUnit.SECONDS)
                        .subscribeWith(new DisposableObserver<KVRangeMessage>() {
                            @Override
                            public void onNext(@NonNull KVRangeMessage m) {
                                SaveSnapshotDataRequest request = m.getSaveSnapshotDataRequest();
                                try {
                                    switch (request.getFlag()) {
                                        case More, End -> {
                                            int bytes = 0;
                                            for (KVPair kv : request.getKvList()) {
                                                bytes += kv.getKey().size();
                                                bytes += kv.getValue().size();
                                                restorer.put(kv.getKey(), kv.getValue());
                                            }
                                            metricManager.reportRestore(bytes);
                                            log.debug(
                                                "Saved {} bytes snapshot data, send reply to {}: rangeId={}, sessionId={}",
                                                bytes, m.getHostStoreId(), KVRangeIdUtil.toString(range.id()),
                                                sessionId);
                                            if (request.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                                                if (!onDone.isCancelled()) {
                                                    restorer.done();
                                                    dispose();
                                                    onDone.complete(null);
                                                    log.debug("Snapshot restored: rangeId={}, sessionId={}",
                                                        KVRangeIdUtil.toString(range.id()), sessionId);
                                                } else {
                                                    restorer.abort();
                                                    dispose();
                                                    log.debug("Snapshot restore canceled: rangeId={}, sessionId={}",
                                                        KVRangeIdUtil.toString(range.id()), sessionId);
                                                }
                                            }
                                            messenger.send(KVRangeMessage.newBuilder()
                                                .setRangeId(range.id())
                                                .setHostStoreId(m.getHostStoreId())
                                                .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                                    .setReqId(request.getReqId())
                                                    .setSessionId(request.getSessionId())
                                                    .setResult(SaveSnapshotDataReply.Result.OK)
                                                    .build())
                                                .build());
                                        }
                                        case Error -> throw new KVRangeStoreException("Snapshot dump failed");
                                    }
                                } catch (Throwable t) {
                                    log.error("Snapshot restored failed: rangeId={}, sessionId={}",
                                        KVRangeIdUtil.toString(range.id()), sessionId, t);
                                    onError(t);
                                    messenger.send(KVRangeMessage.newBuilder()
                                        .setRangeId(range.id())
                                        .setHostStoreId(m.getHostStoreId())
                                        .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                            .setReqId(request.getReqId())
                                            .setSessionId(request.getSessionId())
                                            .setResult(SaveSnapshotDataReply.Result.Error)
                                            .build())
                                        .build());
                                }
                            }

                            @Override
                            public void onError(@NonNull Throwable e) {
                                restorer.abort();
                                onDone.completeExceptionally(e);
                                dispose();
                            }

                            @Override
                            public void onComplete() {

                            }
                        });
                    onDone.whenComplete((_v, _e) -> {
                        if (onDone.isCancelled()) {
                            observer.dispose();
                        }
                    });
                    log.debug("Send snapshot sync request to {} {}: rangeId={}", leader,
                        !onDone.isDone(), KVRangeIdUtil.toString(range.id()));
                    if (!onDone.isDone()) {
                        messenger.send(KVRangeMessage.newBuilder()
                            .setRangeId(range.id())
                            .setHostStoreId(leader)
                            .setSnapshotSyncRequest(SnapshotSyncRequest.newBuilder()
                                .setSessionId(sessionId)
                                .setSnapshot(rangeSnapshot)
                                .build())
                            .build());
                    }
                } catch (Throwable t) {
                    log.error("Unexpected error", t);
                }
            }, executor);
        return onDone;
    }

    private static class RestoreSession {
        final KVRangeSnapshot snapshot;
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

        private RestoreSession(KVRangeSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
