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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_COMMIT;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;

@Slf4j
public class InboxCommitScheduler extends InboxUpdateScheduler<CommitRequest, CommitReply>
    implements IInboxCommitScheduler {
    private final int maxInboxesPerCommit;

    public InboxCommitScheduler(IBaseKVStoreClient kvStoreClient) {
        super(kvStoreClient, "inbox_server_commit");
        maxInboxesPerCommit = INBOX_MAX_INBOXES_PER_COMMIT.get();
    }

    @Override
    protected ByteString rangeKey(CommitRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    @Override
    protected BatchCallBuilder<CommitRequest, CommitReply> newBuilder(String name, int maxInflights,
                                                                      KVRangeSetting rangeSetting) {
        return new BatchCommitBuilder(name, maxInflights, rangeSetting, kvStoreClient);
    }

    private class BatchCommitBuilder extends BatchCallBuilder<CommitRequest, CommitReply> {
        private class BatchCommit implements IBatchCall<CommitRequest, CommitReply> {
            private final AtomicInteger inboxCount = new AtomicInteger();

            // key: scopedInboxIdUtf8, value: [qos0, qos1, qos2]
            private final Map<String, Long[]> inboxCommits = new NonBlockingHashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<CommitRequest, CompletableFuture<CommitReply>> onInboxCommitted =
                new NonBlockingHashMap<>();

            @Override
            public boolean isEmpty() {
                return inboxCommits.isEmpty();
            }

            @Override
            public CompletableFuture<CommitReply> add(CommitRequest request) {
                ClientInfo clientInfo = request.getClientInfo();
                String scopedInboxIdUtf8 = scopedInboxId(clientInfo.getTenantId(),
                    request.getInboxId()).toStringUtf8();
                Long[] upToSeqs = inboxCommits.computeIfAbsent(scopedInboxIdUtf8, k -> {
                    inboxCount.incrementAndGet();
                    return new Long[3];
                });
                QoS qos = request.getQos();
                upToSeqs[qos.ordinal()] = upToSeqs[qos.ordinal()] == null ?
                    request.getUpToSeq() : Math.max(upToSeqs[qos.ordinal()], request.getUpToSeq());
                return onInboxCommitted.computeIfAbsent(request, k -> new CompletableFuture<>());
            }

            @Override
            public void reset() {
                inboxCount.set(0);
                inboxCommits.clear();
                onInboxCommitted.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                batchInboxCount.record(inboxCount.get());
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
                Timer.Sample start = Timer.start();
                return kvStoreClient.execute(range.leader,
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
                        start.stop(batchCommitTimer);
                        switch (reply.getCode()) {
                            case Ok:
                                try {
                                    return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
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
                        if (e != null) {
                            for (CommitRequest request : onInboxCommitted.keySet()) {
                                onInboxCommitted.get(request)
                                    .complete(CommitReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(CommitReply.Result.ERROR)
                                        .build());
                            }
                        } else {
                            for (CommitRequest request : onInboxCommitted.keySet()) {
                                onInboxCommitted.get(request)
                                    .complete(CommitReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setResult(v.getCommit()
                                            .getResultMap()
                                            .get(scopedInboxId(
                                                request.getClientInfo().getTenantId(),
                                                request.getInboxId()).toStringUtf8()) ?
                                            CommitReply.Result.OK : CommitReply.Result.ERROR)
                                        .build());
                            }
                        }
                        return null;
                    });

            }

            @Override
            public boolean isEnough() {
                return inboxCount.get() > maxInboxesPerCommit;
            }
        }

        private final IBaseKVStoreClient kvStoreClient;
        private final KVRangeSetting range;
        private final DistributionSummary batchInboxCount;
        private final Timer batchCommitTimer;

        BatchCommitBuilder(String name, int maxInflights,
                           KVRangeSetting range, IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.range = range;
            this.kvStoreClient = kvStoreClient;
            Tags tags = Tags.of("rangeId", KVRangeIdUtil.toShortString(range.id));
            batchInboxCount = DistributionSummary.builder("inbox.server.commit.inboxes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchCommitTimer = Timer.builder("inbox.server.commit.latency")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchCommit newBatch() {
            return new BatchCommit();
        }

        @Override
        public void close() {
            Metrics.globalRegistry.remove(batchInboxCount);
            Metrics.globalRegistry.remove(batchCommitTimer);
        }
    }
}
