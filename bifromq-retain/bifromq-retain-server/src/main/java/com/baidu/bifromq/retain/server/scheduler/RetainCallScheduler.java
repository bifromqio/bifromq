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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainMessage;
import com.baidu.bifromq.retain.rpc.proto.RetainMessagePack;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResultPack;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcOutput;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainCallScheduler extends BatchCallScheduler<RetainRequest, RetainReply, KVRangeSetting>
    implements IRetainCallScheduler {
    private final IBaseKVStoreClient retainStoreClient;

    public RetainCallScheduler(IBaseKVStoreClient retainStoreClient) {
        super("retain_server_retain_batcher", Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.retainStoreClient = retainStoreClient;
    }

    @Override
    protected Batcher<RetainRequest, RetainReply, KVRangeSetting> newBatcher(String name, long tolerableLatencyNanos,
                                                                             long burstLatencyNanos,
                                                                             KVRangeSetting range) {
        return new RetainCallBatcher(range, name, tolerableLatencyNanos, burstLatencyNanos, retainStoreClient);
    }

    @Override
    protected Optional<KVRangeSetting> find(RetainRequest request) {
        return retainStoreClient.findByKey(tenantNS(request.getPublisher().getTenantId()));
    }

    private static class RetainCallBatcher extends Batcher<RetainRequest, RetainReply, KVRangeSetting> {
        private class BatchRetainCall implements IBatchCall<RetainRequest, RetainReply> {
            private final Queue<CallTask<RetainRequest, RetainReply>> batchedTasks = new ArrayDeque<>();
            private Map<String, RetainMessagePack.Builder> retainMsgPackBuilders = new HashMap<>(128);

            @Override
            public void add(CallTask<RetainRequest, RetainReply> task) {
                RetainRequest request = task.call;
                batchedTasks.add(task);
                retainMsgPackBuilders.computeIfAbsent(request.getPublisher().getTenantId(),
                        k -> RetainMessagePack.newBuilder())
                    .putTopicMessages(request.getTopic(), RetainMessage.newBuilder()
                        .setMessage(request.getMessage())
                        .setPublisher(request.getPublisher())
                        .build());
            }

            @Override
            public void reset() {
                retainMsgPackBuilders = new HashMap<>(128);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                BatchRetainRequest.Builder reqBuilder = BatchRetainRequest.newBuilder().setReqId(reqId);
                retainMsgPackBuilders.forEach((tenantId, retainMsgPackBuilder) ->
                    reqBuilder.putRetainMessagePack(tenantId, retainMsgPackBuilder.build()));
                return retainStoreClient.execute(batcherKey.leader, KVRangeRWRequest.newBuilder()
                        .setReqId(reqId)
                        .setVer(batcherKey.ver)
                        .setKvRangeId(batcherKey.id)
                        .setRwCoProc(RetainServiceRWCoProcInput.newBuilder()
                            .setBatchRetain(reqBuilder.build())
                            .build().toByteString())
                        .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return RetainServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getBatchRetain()
                                    .getResultsMap();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                        throw new RuntimeException();
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<RetainRequest, RetainReply> task;
                            while ((task = batchedTasks.poll()) != null) {
                                task.callResult.complete(RetainReply.newBuilder()
                                    .setReqId(task.call.getReqId())
                                    .setResult(RetainReply.Result.ERROR)
                                    .build());
                            }
                        } else {
                            CallTask<RetainRequest, RetainReply> task;
                            while ((task = batchedTasks.poll()) != null) {
                                RetainReply.Builder replyBuilder = RetainReply.newBuilder()
                                    .setReqId(task.call.getReqId());
                                RetainResult result =
                                    v.getOrDefault(task.call.getPublisher().getTenantId(),
                                            RetainResultPack.getDefaultInstance())
                                        .getResultsOrDefault(task.call.getTopic(), RetainResult.ERROR);
                                switch (result) {
                                    case RETAINED -> replyBuilder.setResult(RetainReply.Result.RETAINED);
                                    case CLEARED -> replyBuilder.setResult(RetainReply.Result.CLEARED);
                                    case ERROR -> replyBuilder.setResult(RetainReply.Result.ERROR);
                                }
                                task.callResult.complete(replyBuilder.build());
                            }
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient retainStoreClient;

        protected RetainCallBatcher(KVRangeSetting range,
                                    String name,
                                    long tolerableLatencyNanos,
                                    long burstLatencyNanos,
                                    IBaseKVStoreClient retainStoreClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.retainStoreClient = retainStoreClient;
        }


        @Override
        protected IBatchCall<RetainRequest, RetainReply> newBatch() {
            return new BatchRetainCall();
        }
    }
}
