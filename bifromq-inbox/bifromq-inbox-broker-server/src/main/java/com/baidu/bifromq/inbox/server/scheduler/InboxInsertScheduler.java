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

import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_BYTES_PER_INSERT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_INSERT;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.rpc.proto.SendResult.Result;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.storage.proto.UpdateRequest;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxInsertScheduler extends InboxUpdateScheduler<MessagePack, SendResult.Result> {
    private final IBaseKVStoreClient kvStoreClient;

    private final int maxInboxPerBatch;

    private final int maxSizePerBatch;

    public InboxInsertScheduler(IBaseKVStoreClient kvStoreClient) {
        super(kvStoreClient, "inbox_server_insert");
        this.kvStoreClient = kvStoreClient;
        maxInboxPerBatch = INBOX_MAX_INBOXES_PER_INSERT.get();
        maxSizePerBatch = INBOX_MAX_BYTES_PER_INSERT.get();
    }

    @Override
    protected ByteString rangeKey(MessagePack request) {
        return KeyUtil.scopedInboxId(request.getSubInfo().getTrafficId(), request.getSubInfo().getInboxId());
    }

    @Override
    protected BatchCallBuilder<MessagePack, Result> newBuilder(String name, int maxInflights,
                                                               KVRangeSetting rangeSetting) {
        return new BatchInsertBuilder(name, maxInflights, rangeSetting, kvStoreClient);
    }

    private final class BatchInsertBuilder extends BatchCallBuilder<MessagePack, Result> {
        private class BatchInsert implements IBatchCall<MessagePack, Result> {
            private final AtomicInteger msgSize = new AtomicInteger();

            // key: scopedInboxIdUtf8
            private final Queue<InsertTask> inboxInserts = new ConcurrentLinkedQueue<>();

            @Override
            public boolean isEmpty() {
                return inboxInserts.isEmpty();
            }

            @Override
            public CompletableFuture<SendResult.Result> add(MessagePack request) {
                InsertTask task = new InsertTask(request);
                inboxInserts.add(task);
                msgSize.addAndGet(request.getSerializedSize());
                return task.onDone;
            }

            @Override
            public void reset() {
                msgSize.set(0);
                inboxInserts.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                UpdateRequest updateRequest = UpdateRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setInsert(InboxInsertRequest.newBuilder()
                        .addAllSubMsgPack(inboxInserts.stream()
                            .map(t -> t.request)
                            .collect(Collectors.toList()))
                        .build())
                    .build();
                batchInboxCount.record(inboxInserts.size());
                batchMsgCount.record(msgSize.get());
                Timer.Sample start = Timer.start();
                return kvStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(updateRequest.getReqId())
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setRequest(updateRequest)
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        start.stop(batchInsertTimer);
                        switch (reply.getCode()) {
                            case Ok:
                                try {
                                    return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult())
                                        .getReply().getInsert();
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
                        Map<SubInfo, SendResult.Result> insertResults = new HashMap<>();
                        for (InboxInsertResult result : v.getResultsList()) {
                            switch (result.getResult()) {
                                case OK:
                                    insertResults.put(result.getSubInfo(), Result.OK);
                                    break;
                                case NO_INBOX:
                                    insertResults.put(result.getSubInfo(), Result.NO_INBOX);
                                    break;
                                default:
                                    insertResults.put(result.getSubInfo(), Result.ERROR);
                            }
                        }
                        while (!inboxInserts.isEmpty()) {
                            InsertTask task = inboxInserts.poll();
                            if (e != null) {
                                task.onDone.complete(Result.ERROR);
                            } else {
                                assert insertResults.containsKey(task.request.getSubInfo());
                                task.onDone.complete(insertResults.get(task.request.getSubInfo()));
                            }
                        }
                        return null;
                    });
            }

            @Override
            public boolean isEnough() {
                return inboxInserts.size() > maxInboxPerBatch || msgSize.get() > maxSizePerBatch;
            }
        }

        private final IBaseKVStoreClient kvStoreClient;

        private final KVRangeSetting range;

        private final DistributionSummary batchInboxCount;

        private final DistributionSummary batchMsgCount;

        private final Timer batchInsertTimer;

        BatchInsertBuilder(String name, int maxInflights,
                                   KVRangeSetting range, IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.range = range;
            this.kvStoreClient = kvStoreClient;
            Tags tags = Tags.of("rangeId", KVRangeIdUtil.toShortString(range.id));
            batchInboxCount = DistributionSummary.builder("inbox.server.insert.inboxes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchMsgCount = DistributionSummary.builder("inbox.server.insert.msgs")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchInsertTimer = Timer.builder("inbox.server.insert.latency")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchInsert newBatch() {
            return new BatchInsert();
        }

        @Override
        public void close() {
            Metrics.globalRegistry.remove(batchInboxCount);
            Metrics.globalRegistry.remove(batchMsgCount);
            Metrics.globalRegistry.remove(batchInsertTimer);
        }
    }

    @AllArgsConstructor
    public static final class InsertTask {
        final MessagePack request;
        final CompletableFuture<SendResult.Result> onDone = new CompletableFuture<>();
    }
}
