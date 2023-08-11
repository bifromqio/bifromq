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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.InboxComSertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class InboxComSertScheduler
    extends InboxMutateScheduler<IInboxComSertScheduler.ComSertCall, IInboxComSertScheduler.ComSertResult>
    implements IInboxComSertScheduler {
    public InboxComSertScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_comsert");
    }

    @Override
    protected Batcher<ComSertCall, ComSertResult, KVRangeSetting> newBatcher(String name,
                                                                             long tolerableLatencyNanos,
                                                                             long burstLatencyNanos,
                                                                             KVRangeSetting range) {
        return new InboxComSertBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(ComSertCall request) {
        if (request.type() == ComSertCallType.INSERT) {
            return scopedInboxId(
                ((InsertCall) request).messagePack.getSubInfo().getTenantId(),
                ((InsertCall) request).messagePack.getSubInfo().getInboxId());
        }
        assert request.type() == ComSertCallType.COMMIT;
        return scopedInboxId(
            ((CommitCall) request).request.getClientInfo().getTenantId(),
            ((CommitCall) request).request.getInboxId());
    }

    private static class InboxComSertBatcher
        extends Batcher<IInboxComSertScheduler.ComSertCall, IInboxComSertScheduler.ComSertResult, KVRangeSetting> {
        private class InboxComSertBatch implements IBatchCall<ComSertCall, ComSertResult> {
            private final Queue<CallTask<IInboxComSertScheduler.ComSertCall, IInboxComSertScheduler.ComSertResult>>
                inboxInserts = new ArrayDeque<>();
            // key: scopedInboxIdUtf8, value: [qos0, qos1, qos2]
            private final Map<String, Long[]> inboxCommits = new HashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<CommitRequest, CompletableFuture<IInboxComSertScheduler.ComSertResult>> onInboxCommitted =
                new HashMap<>();

            @Override
            public void reset() {
                inboxInserts.clear();
                inboxCommits.clear();
                onInboxCommitted.clear();
            }

            @Override
            public void add(CallTask<ComSertCall, ComSertResult> callTask) {
                ComSertCall request = callTask.call;
                switch (request.type()) {
                    case INSERT -> inboxInserts.add(callTask);
                    case COMMIT -> {
                        CommitRequest commitRequest = ((CommitCall) request).request;
                        ClientInfo clientInfo = commitRequest.getClientInfo();
                        String scopedInboxIdUtf8 = scopedInboxId(clientInfo.getTenantId(),
                            commitRequest.getInboxId()).toStringUtf8();
                        Long[] upToSeqs = inboxCommits.computeIfAbsent(scopedInboxIdUtf8, k -> new Long[3]);
                        QoS qos = commitRequest.getQos();
                        upToSeqs[qos.ordinal()] = upToSeqs[qos.ordinal()] == null ?
                            commitRequest.getUpToSeq() : Math.max(upToSeqs[qos.ordinal()], commitRequest.getUpToSeq());
                        onInboxCommitted.put(commitRequest, callTask.callResult);
                    }
                }
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                InboxComSertRequest.Builder reqBuilder = InboxComSertRequest.newBuilder();
                inboxInserts.forEach(insertTask -> reqBuilder.addInsert(((InsertCall) insertTask.call).messagePack));
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
                return inboxStoreClient.execute(range.leader,
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
                                CallTask<ComSertCall, ComSertResult> task = inboxInserts.poll();
                                task.callResult.completeExceptionally(e);
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
                                CallTask<ComSertCall, ComSertResult> task = inboxInserts.poll();
                                task.callResult.complete(new InsertResult(
                                    insertResults.get(((InsertCall) task.call).messagePack.getSubInfo())));
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

        private final IBaseKVStoreClient inboxStoreClient;
        private final KVRangeSetting range;

        private InboxComSertBatcher(String name,
                                    long tolerableLatencyNanos,
                                    long burstLatencyNanos,
                                    KVRangeSetting range,
                                    IBaseKVStoreClient inboxStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.inboxStoreClient = inboxStoreClient;
            this.range = range;
        }

        @Override
        protected IBatchCall<ComSertCall, ComSertResult> newBatch() {
            return new InboxComSertBatch();
        }
    }
}
