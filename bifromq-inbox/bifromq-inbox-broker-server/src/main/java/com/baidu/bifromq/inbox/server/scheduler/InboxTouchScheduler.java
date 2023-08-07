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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_TOUCH;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;

@Slf4j
public class InboxTouchScheduler extends InboxUpdateScheduler<InboxTouchScheduler.Touch, Void> {
    private final int maxInboxesPerTouch;

    public InboxTouchScheduler(IBaseKVStoreClient kvStoreClient) {
        super(kvStoreClient, "inbox_server_touch");
        maxInboxesPerTouch = INBOX_MAX_INBOXES_PER_TOUCH.get();
    }

    @Override
    protected ByteString rangeKey(Touch request) {
        return ByteString.copyFromUtf8(request.scopedInboxIdUtf8);
    }

    @Override
    protected BatchCallBuilder<Touch, Void> newBuilder(String name, int maxInflights, KVRangeSetting rangeSetting) {
        return new BatchTouchBuilder(name, maxInflights, rangeSetting, kvStoreClient);
    }

    private class BatchTouchBuilder extends BatchCallBuilder<Touch, Void> {
        private class BatchTouch implements IBatchCall<Touch, Void> {
            // key: scopedInboxIdUtf8
            private final Map<String, Boolean> inboxTouches = new NonBlockingHashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<Touch, CompletableFuture<Void>> onInboxTouched = new NonBlockingHashMap<>();

            @Override
            public boolean isEmpty() {
                return inboxTouches.isEmpty();
            }

            @Override
            public CompletableFuture<Void> add(Touch request) {
                inboxTouches.compute(request.scopedInboxIdUtf8, (k, v) -> {
                    if (v == null) {
                        return request.keep;
                    }
                    return v && request.keep;
                });
                return onInboxTouched.computeIfAbsent(request, k -> new CompletableFuture<>());
            }

            @Override
            public void reset() {
                inboxTouches.clear();
                onInboxTouched.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                batchInboxCount.record(inboxTouches.size());
                Timer.Sample start = Timer.start();
                return kvStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setTouch(TouchRequest.newBuilder()
                                    .putAllScopedInboxId(inboxTouches)
                                    .build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        start.stop(batchTouchTimer);
                        switch (reply.getCode()) {
                            case Ok:
                                try {
                                    return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getTouch();
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Unable to parse rw co-proc output", e);
                                    throw new RuntimeException(e);
                                }
                            default:
                                log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                                throw new RuntimeException();
                        }
                    })
                    .handle((v, e) -> {
                        for (Touch request : onInboxTouched.keySet()) {
                            if (e != null) {
                                onInboxTouched.get(request).completeExceptionally(e);
                            } else {
                                onInboxTouched.get(request).complete(null);
                            }
                        }
                        return null;
                    });
            }

            @Override
            public boolean isEnough() {
                return inboxTouches.size() > maxInboxesPerTouch;
            }
        }

        private final IBaseKVStoreClient kvStoreClient;
        private final KVRangeSetting range;

        private final DistributionSummary batchInboxCount;
        private final Timer batchTouchTimer;

        BatchTouchBuilder(String name, int maxInflights,
                          KVRangeSetting range, IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.range = range;
            this.kvStoreClient = kvStoreClient;
            Tags tags = Tags.of("rangeId", KVRangeIdUtil.toShortString(range.id));
            batchInboxCount = DistributionSummary.builder("inbox.server.touch.inboxes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchTouchTimer = Timer.builder("inbox.server.touch.latency")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchTouch newBatch() {
            return new BatchTouch();
        }

        @Override
        public void close() {
            Metrics.globalRegistry.remove(batchInboxCount);
            Metrics.globalRegistry.remove(batchTouchTimer);
        }
    }

    public static class Touch {
        final String scopedInboxIdUtf8;
        final boolean keep;

        public Touch(DeleteInboxRequest req) {
            scopedInboxIdUtf8 = scopedInboxId(req.getClientInfo().getTenantId(), req.getInboxId()).toStringUtf8();
            keep = false;
        }

        public Touch(ByteString scopedInboxId) {
            scopedInboxIdUtf8 = scopedInboxId.toStringUtf8();
            keep = true;
        }
    }
}
