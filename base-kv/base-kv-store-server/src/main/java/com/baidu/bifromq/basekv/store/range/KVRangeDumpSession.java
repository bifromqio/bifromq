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

package com.baidu.bifromq.basekv.store.range;

import com.baidu.bifromq.basekv.proto.KVPair;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataReply;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataRequest;
import com.baidu.bifromq.basekv.proto.SnapshotSyncRequest;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.util.AsyncRunner;
import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.rxjava3.disposables.Disposable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KVRangeDumpSession {
    interface DumpBytesRecorder {
        void record(int bytes);
    }

    private final String peerStoreId;
    private final SnapshotSyncRequest request;
    private final IKVRangeMessenger messenger;
    private final AsyncRunner runner;
    private final AtomicInteger reqId = new AtomicInteger();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final Duration maxIdleDuration;
    private final CompletableFuture<Void> doneSignal = new CompletableFuture<>();
    private final DumpBytesRecorder recorder;
    private RateLimiter rateLimiter;
    private IKVIterator snapshotItr;
    private volatile KVRangeMessage currentRequest;
    private volatile long lastReplyTS;

    KVRangeDumpSession(String peerStoreId,
                       SnapshotSyncRequest request,
                       IKVRangeState accessor,
                       IKVRangeMessenger messenger,
                       Executor executor,
                       Duration maxIdleDuration,
                       long bandwidth,
                       DumpBytesRecorder recorder) {
        this.peerStoreId = peerStoreId;
        this.request = request;
        this.messenger = messenger;
        this.runner = new AsyncRunner(executor);
        this.maxIdleDuration = maxIdleDuration;
        this.recorder = recorder;
        rateLimiter = RateLimiter.create(bandwidth);
        if (!request.getSnapshot().hasCheckpointId()) {
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(peerStoreId)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(request.getSessionId())
                    .setFlag(SaveSnapshotDataRequest.Flag.End)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(null));
        } else if (!accessor.hasCheckpoint(request.getSnapshot())) {
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(peerStoreId)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(request.getSessionId())
                    .setFlag(SaveSnapshotDataRequest.Flag.Error)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(null));
        } else {
            snapshotItr = accessor.open(request.getSnapshot());
            snapshotItr.seekToFirst();
            Disposable disposable = messenger.receive()
                .mapOptional(m -> {
                    if (m.hasSaveSnapshotDataReply()) {
                        SaveSnapshotDataReply reply = m.getSaveSnapshotDataReply();
                        if (reply.getSessionId().equals(request.getSessionId())) {
                            return Optional.of(reply);
                        }
                    }
                    return Optional.empty();
                })
                .subscribe(this::handleReply);
            doneSignal.whenComplete((v, e) -> {
                try {
                    snapshotItr.close();
                } catch (Exception ex) {
                    log.error("Unable to close snapshot iterator", e);
                }
                disposable.dispose();
            });
            nextSaveRequest();
        }
    }

    String checkpointId() {
        return request.getSnapshot().getCheckpointId();
    }

    void tick() {
        long elapseNanos = Duration.ofNanos(System.nanoTime() - lastReplyTS).toNanos();
        if (maxIdleDuration.toNanos() < elapseNanos) {
            cancel();
        } else if (maxIdleDuration.toNanos() / 2 < elapseNanos) {
            runner.add(() -> {
                if (maxIdleDuration.toNanos() / 2 < Duration.ofNanos(System.nanoTime() - lastReplyTS).toNanos()) {
                    messenger.send(currentRequest);
                }
            });
        }
    }

    void cancel() {
        if (canceled.compareAndSet(false, true)) {
            runner.add(() -> doneSignal.complete(null));
        }
    }

    CompletionStage<Void> awaitDone() {
        return doneSignal;
    }

    private void handleReply(SaveSnapshotDataReply reply) {
        SaveSnapshotDataRequest req = currentRequest.getSaveSnapshotDataRequest();
        lastReplyTS = System.nanoTime();
        if (req != null && req.getReqId() == reply.getReqId()) {
            currentRequest = null;
            switch (reply.getResult()) {
                case OK:
                    switch (req.getFlag()) {
                        case More:
                            nextSaveRequest();
                            break;
                        case End:
                            runner.add(() -> doneSignal.complete(null));
                            break;
                    }
                    break;
                case NoSessionFound:
                case Error:
                    runner.add(() -> doneSignal.complete(null));
                    break;
            }
        }
    }

    private void nextSaveRequest() {
        runner.add(() -> {
            SaveSnapshotDataRequest.Builder reqBuilder = SaveSnapshotDataRequest.newBuilder()
                .setSessionId(request.getSessionId())
                .setReqId(reqId.getAndIncrement());
            while (true) {
                if (!canceled.get()) {
                    try {
                        if (snapshotItr.isValid()) {
                            KVPair kvPair = KVPair.newBuilder()
                                .setKey(snapshotItr.key())
                                .setValue(snapshotItr.value())
                                .build();
                            reqBuilder.addKv(kvPair);
                            int bytes = snapshotItr.key().size() + snapshotItr.value().size();
                            snapshotItr.next();
                            if (!rateLimiter.tryAcquire(bytes)) {
                                if (snapshotItr.isValid()) {
                                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.More);
                                } else {
                                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.End);
                                }
                                break;
                            }
                            recorder.record(bytes);
                        } else {
                            // current iterator finished
                            reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.End);
                            break;
                        }
                    } catch (Throwable e) {
                        log.error("Dump error for session: " + request.getSessionId(), e);
                        reqBuilder.clearKv();
                        reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
                        break;
                    }
                } else {
                    reqBuilder.clearKv();
                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
                    break;
                }
            }
            currentRequest = KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(peerStoreId)
                .setSaveSnapshotDataRequest(reqBuilder.build())
                .build();
            lastReplyTS = System.nanoTime();
            messenger.send(currentRequest);
        });
    }
}
