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
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CommitReply;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCommitScheduler extends InboxMutateScheduler<CommitRequest, CommitReply>
    implements IInboxCommitScheduler {
    public InboxCommitScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_commit");
    }

    @Override
    protected Batcher<CommitRequest, CommitReply, KVRangeSetting> newBatcher(String name,
                                                                             long tolerableLatencyNanos,
                                                                             long burstLatencyNanos,
                                                                             KVRangeSetting range) {
        return new InboxCommitBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(CommitRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxCommitBatcher extends Batcher<CommitRequest, CommitReply, KVRangeSetting> {
        private class InboxBatchCommit implements IBatchCall<CommitRequest, CommitReply> {
            private final Queue<CallTask<CommitRequest, CommitReply>> batchTasks = new ArrayDeque<>();
            // key: scopedInboxIdUtf8, value: [qos0, qos1, qos2]
            private Map<String, Long[]> inboxCommits = new HashMap<>(128);

            @Override
            public void reset() {
                inboxCommits = new HashMap<>(128);
            }

            @Override
            public void add(CallTask<CommitRequest, CommitReply> callTask) {
                ClientInfo clientInfo = callTask.call.getClientInfo();
                String scopedInboxIdUtf8 = scopedInboxId(clientInfo.getTenantId(),
                    callTask.call.getInboxId()).toStringUtf8();
                Long[] upToSeqs = inboxCommits.computeIfAbsent(scopedInboxIdUtf8, k -> new Long[3]);
                QoS qos = callTask.call.getQos();
                upToSeqs[qos.ordinal()] = upToSeqs[qos.ordinal()] == null ?
                    callTask.call.getUpToSeq() : Math.max(upToSeqs[qos.ordinal()], callTask.call.getUpToSeq());
                batchTasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                InboxCommitRequest.Builder reqBuilder = InboxCommitRequest.newBuilder();
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
                    reqBuilder.putInboxCommit(k, cb.build());
                });
                return inboxStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setCommit(reqBuilder.build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        switch (reply.getCode()) {
                            case Ok:
                                try {
                                    return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Unable to parse rw co-proc output", e);
                                    throw new RuntimeException(e);
                                }
                            default:
                                throw new RuntimeException(reply.getCode().name());
                        }
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<CommitRequest, CommitReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(CommitReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(CommitReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<CommitRequest, CommitReply> task;
                            while ((task = batchTasks.poll()) != null) {
                                task.callResult.complete(CommitReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(v.getCommit()
                                        .getResultMap()
                                        .get(scopedInboxId(
                                            task.call.getClientInfo().getTenantId(),
                                            task.call.getInboxId()).toStringUtf8()) ?
                                        CommitReply.Result.OK : CommitReply.Result.ERROR)
                                    .build());
                            }
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient inboxStoreClient;
        private final KVRangeSetting range;

        InboxCommitBatcher(String name,
                           long tolerableLatencyNanos,
                           long burstLatencyNanos,
                           KVRangeSetting range,
                           IBaseKVStoreClient inboxStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
        }

        @Override
        protected IBatchCall<CommitRequest, CommitReply> newBatch() {
            return new InboxBatchCommit();
        }
    }
}
