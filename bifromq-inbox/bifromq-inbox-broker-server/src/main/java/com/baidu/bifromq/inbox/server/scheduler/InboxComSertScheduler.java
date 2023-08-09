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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_BYTES_PER_INSERT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_COMMIT;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_INSERT;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.InboxComSertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import org.jctools.maps.NonBlockingHashMap;

public class InboxComSertScheduler extends
    InboxUpdateScheduler<InboxComSertScheduler.ComSertCall, InboxComSertScheduler.ComSertResult> {
    private final int maxInboxPerBatch;
    private final int maxSizePerBatch;
    private final int maxInboxesPerCommit;

    public InboxComSertScheduler(IBaseKVStoreClient kvStoreClient) {
        super(kvStoreClient, "inbox_server_comsert");
        maxInboxPerBatch = INBOX_MAX_INBOXES_PER_INSERT.get();
        maxSizePerBatch = INBOX_MAX_BYTES_PER_INSERT.get();
        maxInboxesPerCommit = INBOX_MAX_INBOXES_PER_COMMIT.get();
    }

    @Override
    protected BatchCallBuilder<ComSertCall, ComSertResult> newBuilder(String name,
                                                                      int maxInflights,
                                                                      KVRangeSetting rangeSetting) {
        return new BatchInsertOrCommitBuilder(name, maxInflights, rangeSetting, kvStoreClient);
    }

    @Override
    protected ByteString rangeKey(ComSertCall request) {
        switch (request.type()) {
            case INSERT -> {
                return KeyUtil.scopedInboxId(((InsertCall) request).messagePack.getSubInfo().getTenantId(),
                    ((InsertCall) request).messagePack.getSubInfo().getInboxId());
            }
            default -> {
                return scopedInboxId(((CommitCall) request).request.getClientInfo().getTenantId(),
                    ((CommitCall) request).request.getInboxId());
            }
        }
    }

    private final class BatchInsertOrCommitBuilder
        extends BatchCallBuilder<ComSertCall, ComSertResult> {
        private final IBaseKVStoreClient kvStoreClient;
        private final KVRangeSetting range;
        private final DistributionSummary batchInboxCount;
        private final DistributionSummary batchMsgCount;

        private class BatchInsertOrCommit implements IBatchCall<ComSertCall, ComSertResult> {
            private final AtomicInteger inboxCount = new AtomicInteger();
            private final AtomicInteger msgSize = new AtomicInteger();
            private final Queue<InsertTask> inboxInserts = new ConcurrentLinkedQueue<>();
            // key: scopedInboxIdUtf8, value: [qos0, qos1, qos2]
            private final Map<String, Long[]> inboxCommits = new NonBlockingHashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<CommitRequest, CompletableFuture<ComSertResult>> onInboxCommitted =
                new NonBlockingHashMap<>();

            @Override
            public boolean isEmpty() {
                return inboxInserts.isEmpty() && inboxCommits.isEmpty();
            }

            @Override
            public boolean isEnough() {
                return (inboxInserts.size() > maxInboxPerBatch ||
                    msgSize.get() > maxSizePerBatch) ||
                    inboxCount.get() > maxInboxesPerCommit;
            }

            @Override
            public CompletableFuture<ComSertResult> add(ComSertCall request) {
                switch (request.type()) {
                    case INSERT -> {
                        MessagePack msgPack = ((InsertCall) request).messagePack;
                        InsertTask task = new InsertTask(msgPack);
                        inboxInserts.add(task);
                        msgSize.addAndGet(msgPack.getSerializedSize());
                        return task.onDone;
                    }
                    default -> {
                        CommitRequest commitRequest = ((CommitCall) request).request;
                        ClientInfo clientInfo = commitRequest.getClientInfo();
                        String scopedInboxIdUtf8 = scopedInboxId(clientInfo.getTenantId(),
                            commitRequest.getInboxId()).toStringUtf8();
                        Long[] upToSeqs = inboxCommits.computeIfAbsent(scopedInboxIdUtf8, k -> {
                            inboxCount.incrementAndGet();
                            return new Long[3];
                        });
                        QoS qos = commitRequest.getQos();
                        upToSeqs[qos.ordinal()] = upToSeqs[qos.ordinal()] == null ?
                            commitRequest.getUpToSeq() : Math.max(upToSeqs[qos.ordinal()], commitRequest.getUpToSeq());
                        return onInboxCommitted.computeIfAbsent(commitRequest, k -> new CompletableFuture<>());
                    }
                }
            }

            @Override
            public void reset() {
                msgSize.set(0);
                inboxInserts.clear();
                inboxCount.set(0);
                inboxCommits.clear();
                onInboxCommitted.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                batchInboxCount.record(inboxInserts.size());
                batchMsgCount.record(msgSize.get());
                InboxComSertRequest.Builder reqBuilder = InboxComSertRequest.newBuilder();
                inboxInserts.forEach(insertTask -> reqBuilder.addInsert(insertTask.request));
                inboxCommits.forEach((k, v) -> {
                    InboxCommit.Builder cb = InboxCommit.newBuilder();
                    if (v[0] != null) {
                        cb.setQos0UpToSeq(v[0]);
                    }
                    if (v[1] != null) {
                        cb.setQos1UpToSeq(v[1]);
                    }
                    if (v[2] != null) {
                        cb.setQos2UpToSeq(v[2]);
                    }
                    reqBuilder.putCommit(k, cb.build());
                });
                return kvStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setInsertAndCommit(reqBuilder.build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException("Unable to parse rw co-proc output", e);
                            }
                        }
                        throw new RuntimeException(
                            String.format("Failed to exec rw co-proc[code=%s]", reply.getCode()));
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            while (!inboxInserts.isEmpty()) {
                                InsertTask task = inboxInserts.poll();
                                task.onDone.completeExceptionally(e);
                            }
                            for (CommitRequest request : onInboxCommitted.keySet()) {
                                onInboxCommitted.get(request)
                                    .complete(new CommitResult(CommitReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(CommitReply.Result.ERROR)
                                        .build()));
                            }
                        } else {
                            Map<SubInfo, SendResult.Result> insertResults = new HashMap<>();
                            for (InboxInsertResult result : v.getInsertAndCommit().getInsertResultsList()) {
                                if (result.getResult() == InboxInsertResult.Result.NO_INBOX) {
                                    insertResults.put(result.getSubInfo(), SendResult.Result.NO_INBOX);
                                } else {
                                    insertResults.put(result.getSubInfo(), SendResult.Result.OK);
                                }
                            }
                            while (!inboxInserts.isEmpty()) {
                                InsertTask task = inboxInserts.poll();
                                task.onDone.complete(new InsertResult(insertResults.get(task.request.getSubInfo())));
                            }
                            for (CommitRequest request : onInboxCommitted.keySet()) {
                                onInboxCommitted.get(request)
                                    .complete(new CommitResult(CommitReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(v.getInsertAndCommit()
                                            .getCommitResultsMap()
                                            .get(scopedInboxId(
                                                request.getClientInfo().getTenantId(),
                                                request.getInboxId()).toStringUtf8()) ?
                                            CommitReply.Result.OK : CommitReply.Result.ERROR)
                                        .build()));
                            }
                        }
                        return null;
                    });
            }
        }

        BatchInsertOrCommitBuilder(String name,
                                   int maxInflights,
                                   KVRangeSetting range,
                                   IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.kvStoreClient = kvStoreClient;
            this.range = range;
            Tags tags = Tags.of("rangeId", KVRangeIdUtil.toShortString(range.id));
            batchInboxCount = DistributionSummary.builder("inbox.server.insert.inboxes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchMsgCount = DistributionSummary.builder("inbox.server.insert.msgs")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public IBatchCall<ComSertCall, ComSertResult> newBatch() {
            return new BatchInsertOrCommit();
        }

        @Override
        public void close() {
            Metrics.globalRegistry.remove(batchInboxCount);
            Metrics.globalRegistry.remove(batchMsgCount);
        }
    }

    @AllArgsConstructor
    public static final class InsertTask {
        final MessagePack request;
        final CompletableFuture<ComSertResult> onDone = new CompletableFuture<>();
    }

    public enum ComSertCallType {
        INSERT, COMMIT
    }

    public abstract static class ComSertCall {
        abstract ComSertCallType type();
    }

    public static class InsertCall extends ComSertCall {
        public final MessagePack messagePack;

        public InsertCall(MessagePack messagePack) {
            this.messagePack = messagePack;
        }

        @Override
        final ComSertCallType type() {
            return ComSertCallType.INSERT;
        }
    }

    public static class CommitCall extends ComSertCall {
        public final CommitRequest request;

        public CommitCall(CommitRequest request) {
            this.request = request;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.COMMIT;
        }
    }

    public abstract static class ComSertResult {
        abstract ComSertCallType type();
    }

    public static class InsertResult extends ComSertResult {
        public final SendResult.Result result;

        public InsertResult(SendResult.Result result) {
            this.result = result;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.INSERT;
        }
    }

    public static class CommitResult extends ComSertResult {
        public final CommitReply result;

        public CommitResult(CommitReply result) {
            this.result = result;
        }

        @Override
        ComSertCallType type() {
            return ComSertCallType.COMMIT;
        }
    }
}
