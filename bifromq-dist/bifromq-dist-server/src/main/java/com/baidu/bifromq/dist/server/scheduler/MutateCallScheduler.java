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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MutateCallScheduler<Req, Resp> extends BatchCallScheduler<Req, Resp, KVRangeSetting> {
    protected final IBaseKVStoreClient distWorkerClient;

    public MutateCallScheduler(String name, IBaseKVStoreClient distWorkerClient) {
        super(name, Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected final Optional<KVRangeSetting> find(Req subCall) {
        return distWorkerClient.findByKey(rangeKey(subCall));
    }

    protected abstract ByteString rangeKey(Req call);

    protected abstract static class MutateCallBatcher<Req, Resp> extends Batcher<Req, Resp, KVRangeSetting> {
        private final IBaseKVStoreClient.IExecutionPipeline executionPipeline;

        protected MutateCallBatcher(String name,
                                    long tolerableLatencyNanos,
                                    long burstLatencyNanos,
                                    KVRangeSetting range,
                                    IBaseKVStoreClient distWorkerClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.executionPipeline = distWorkerClient.createExecutionPipeline(range.leader);
        }

        protected CompletableFuture<DistServiceRWCoProcOutput> mutate(long reqId, DistServiceRWCoProcInput input) {
            return executionPipeline.execute(KVRangeRWRequest.newBuilder()
                    .setReqId(reqId)
                    .setVer(batcherKey.ver)
                    .setKvRangeId(batcherKey.id)
                    .setRwCoProc(input.toByteString())
                    .build())
                .thenApply(reply -> {
                    if (reply.getCode() == ReplyCode.Ok) {
                        try {
                            return DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
                        } catch (InvalidProtocolBufferException e) {
                            log.error("Unable to parse rw co-proc output", e);
                            throw new RuntimeException(e);
                        }
                    }
                    log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                    throw new RuntimeException();
                });
        }

        @Override
        public void close() {
            super.close();
            executionPipeline.close();
        }

    }
}