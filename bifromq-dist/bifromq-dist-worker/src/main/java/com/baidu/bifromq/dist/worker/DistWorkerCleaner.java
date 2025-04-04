/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.client.exception.BadRequestException;
import com.baidu.bifromq.basekv.client.exception.BadVersionException;
import com.baidu.bifromq.basekv.client.exception.InternalErrorException;
import com.baidu.bifromq.basekv.client.exception.TryLaterException;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.GCReply;
import com.baidu.bifromq.dist.rpc.proto.GCRequest;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistWorkerCleaner {
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final IBaseKVStoreClient distWorkerClient;
    private final Duration cleanInterval;
    private final ScheduledExecutorService jobScheduler;
    private volatile ScheduledFuture<?> cleanerFuture;

    DistWorkerCleaner(IBaseKVStoreClient distWorkerClient,
                      Duration cleanInterval,
                      ScheduledExecutorService jobScheduler) {
        this.distWorkerClient = distWorkerClient;
        this.cleanInterval = cleanInterval;
        this.jobScheduler = jobScheduler;
    }

    void start(String storeId) {
        if (started.compareAndSet(false, true)) {
            doStart(storeId);
        }
    }

    CompletableFuture<Void> stop() {
        if (started.compareAndSet(true, false)) {
            cleanerFuture.cancel(true);
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            jobScheduler.execute(() -> onDone.complete(null));
            return onDone;
        }
        return CompletableFuture.completedFuture(null);
    }

    private void doStart(String storeId) {
        if (!started.get()) {
            return;
        }
        cleanerFuture = jobScheduler.schedule(() -> {
            doGC(storeId).thenRun(() -> doStart(storeId));
        }, cleanInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> doGC(String storeId) {
        Collection<KVRangeSetting> rangeSettingList =
            findByBoundary(FULL_BOUNDARY, distWorkerClient.latestEffectiveRouter());
        rangeSettingList.removeIf(rangeSetting -> !rangeSetting.leader.equals(storeId));
        long reqId = HLC.INST.getPhysical();
        List<CompletableFuture<GCReply>> replyFutures = rangeSettingList.stream()
            .map(rangeSetting -> doGC(reqId, rangeSetting))
            .toList();
        return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
            .exceptionally(e -> {
                log.debug("[DistWorker] gc failed: {}", e.getMessage());
                return null;
            });
    }

    private CompletableFuture<GCReply> doGC(long reqId, KVRangeSetting rangeSetting) {
        log.debug("[DistWorker] gc: rangeId={}", KVRangeIdUtil.toString(rangeSetting.id));
        return distWorkerClient.query(rangeSetting.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id)
                .setVer(rangeSetting.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setDistService(DistServiceROCoProcInput.newBuilder()
                        .setGc(GCRequest.newBuilder()
                            .setReqId(reqId)
                            .build())
                        .build())
                    .build())
                .build())
            .handle((v, e) -> {
                switch (v.getCode()) {
                    case Ok -> {
                        return v.getRoCoProcResult().getDistService().getGc();
                    }
                    case TryLater -> throw new TryLaterException();
                    case BadVersion -> throw new BadVersionException();
                    case BadRequest -> throw new BadRequestException();
                    default -> throw new InternalErrorException();
                }
            });
    }
}
