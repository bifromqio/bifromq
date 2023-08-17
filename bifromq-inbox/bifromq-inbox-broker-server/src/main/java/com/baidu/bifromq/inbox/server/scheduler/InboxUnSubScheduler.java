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
import com.baidu.bifromq.inbox.rpc.proto.RemoveSubReply;
import com.baidu.bifromq.inbox.rpc.proto.RemoveSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchRemoveSubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxUnSubScheduler extends InboxMutateScheduler<RemoveSubRequest, RemoveSubReply>
    implements IInboxUnsubScheduler {
    public InboxUnSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_unsub");
    }

    @Override
    protected Batcher<RemoveSubRequest, RemoveSubReply, KVRangeSetting> newBatcher(String name,
                                                                                   long tolerableLatencyNanos,
                                                                                   long burstLatencyNanos,
                                                                                   KVRangeSetting range) {
        return new InboxUnSubBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(RemoveSubRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxUnSubBatcher extends Batcher<RemoveSubRequest, RemoveSubReply, KVRangeSetting> {
        private class InboxBatchUnSub implements IBatchCall<RemoveSubRequest, RemoveSubReply> {
            private final Queue<CallTask<RemoveSubRequest, RemoveSubReply>> batchTasks = new ArrayDeque<>();
            private BatchRemoveSubRequest.Builder reqBuilder = BatchRemoveSubRequest.newBuilder();

            @Override
            public void add(CallTask<RemoveSubRequest, RemoveSubReply> task) {
                batchTasks.add(task);
                RemoveSubRequest request = task.call;
                reqBuilder.addTopicFilters(
                    scopedTopicFilter(request.getClientInfo().getTenantId(), request.getInboxId(),
                        request.getTopicFilter()));
            }

            @Override
            public void reset() {
                reqBuilder = BatchRemoveSubRequest.newBuilder();
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
                                .setRemoveTopicFilter(reqBuilder.setReqId(reqId).build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult())
                                    .getRemoveTopicFilter();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        throw new RuntimeException(reply.getCode().name());
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<RemoveSubRequest, RemoveSubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(RemoveSubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(RemoveSubReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<RemoveSubRequest, RemoveSubReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(RemoveSubReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(RemoveSubReply.Result.forNumber(v
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

        InboxUnSubBatcher(String name,
                          long tolerableLatencyNanos,
                          long burstLatencyNanos,
                          KVRangeSetting range,
                          IBaseKVStoreClient inboxStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
        }

        @Override
        protected IBatchCall<RemoveSubRequest, RemoveSubReply> newBatch() {
            return new InboxBatchUnSub();
        }
    }
}
