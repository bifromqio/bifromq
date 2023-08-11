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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.TouchRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxTouchScheduler extends InboxMutateScheduler<IInboxTouchScheduler.Touch, Void>
    implements IInboxTouchScheduler {
    public InboxTouchScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_touch");
    }

    @Override
    protected Batcher<Touch, Void, KVRangeSetting> newBatcher(String name,
                                                              long tolerableLatencyNanos,
                                                              long burstLatencyNanos,
                                                              KVRangeSetting range) {
        return new InboxTouchBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(Touch request) {
        return ByteString.copyFromUtf8(request.scopedInboxIdUtf8);
    }

    private static class InboxTouchBatcher extends Batcher<Touch, Void, KVRangeSetting> {
        private class InboxTouchBatch implements IBatchCall<Touch, Void> {
            // key: scopedInboxIdUtf8
            private final Map<String, Boolean> inboxTouches = new HashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<Touch, CompletableFuture<Void>> onInboxTouched = new HashMap<>();

            @Override
            public void reset() {
                inboxTouches.clear();
                onInboxTouched.clear();
            }

            @Override
            public void add(CallTask<Touch, Void> callTask) {
                Touch request = callTask.call;
                inboxTouches.compute(request.scopedInboxIdUtf8, (k, v) -> {
                    if (v == null) {
                        return request.keep;
                    }
                    return v && request.keep;
                });
                onInboxTouched.put(request, callTask.callResult);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return inboxStoreClient.execute(range.leader,
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
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getTouch();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                        throw new RuntimeException();
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
        }

        private final IBaseKVStoreClient inboxStoreClient;
        private final KVRangeSetting range;

        InboxTouchBatcher(String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          KVRangeSetting range,
                          IBaseKVStoreClient inboxStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
        }

        @Override
        protected IBatchCall<Touch, Void> newBatch() {
            return new InboxTouchBatch();
        }
    }
}
