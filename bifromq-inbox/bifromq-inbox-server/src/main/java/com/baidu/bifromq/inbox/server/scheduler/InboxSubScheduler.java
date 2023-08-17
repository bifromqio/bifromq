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
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.AddSubReply;
import com.baidu.bifromq.inbox.rpc.proto.AddSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchAddSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxSubScheduler extends InboxMutateScheduler<AddSubRequest, AddSubReply>
    implements IInboxSubScheduler {
    public InboxSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_sub");
    }

    @Override
    protected Batcher<AddSubRequest, AddSubReply, KVRangeSetting> newBatcher(String name,
                                                                             long tolerableLatencyNanos,
                                                                             long burstLatencyNanos,
                                                                             KVRangeSetting range) {
        return new InboxSubBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(AddSubRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxSubBatcher extends Batcher<AddSubRequest, AddSubReply, KVRangeSetting> {
        private class InboxBatchSub implements IBatchCall<AddSubRequest, AddSubReply> {
            private final Queue<CallTask<AddSubRequest, AddSubReply>> batchTasks = new ArrayDeque<>();
            private BatchAddSubRequest.Builder reqBuilder = BatchAddSubRequest.newBuilder();

            @Override
            public void add(CallTask<AddSubRequest, AddSubReply> task) {
                batchTasks.add(task);
                AddSubRequest request = task.call;
                reqBuilder.putTopicFilters(
                    scopedTopicFilter(request.getClientInfo().getTenantId(), request.getInboxId(),
                        request.getTopicFilter()).toStringUtf8(), request.getSubQoS());
            }

            @Override
            public void reset() {
                reqBuilder = BatchAddSubRequest.newBuilder();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                reqBuilder.setReqId(reqId);
                return inboxStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setAddTopicFilter(reqBuilder.build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult())
                                    .getAddTopicFilter();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        throw new RuntimeException(reply.getCode().name());
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<AddSubRequest, AddSubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(AddSubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(AddSubReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<AddSubRequest, AddSubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(AddSubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(AddSubReply.Result.forNumber(v
                                        .getResultsMap()
                                        .get(scopedTopicFilter(task.call.getClientInfo().getTenantId(),
                                            task.call.getInboxId(), task.call.getTopicFilter()).toStringUtf8())
                                        .getNumber()))
                                    .build());
                            }
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient inboxStoreClient;
        private final KVRangeSetting range;

        InboxSubBatcher(String name,
                        long tolerableLatencyNanos,
                        long burstLatencyNanos,
                        KVRangeSetting range,
                        IBaseKVStoreClient inboxStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
        }

        @Override
        protected IBatchCall<AddSubRequest, AddSubReply> newBatch() {
            return new InboxBatchSub();
        }
    }
}
