/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.client.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IMutationPipeline;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BatchMutationCall<ReqT, RespT> implements IBatchCall<ReqT, RespT, MutationCallBatcherKey> {
    private final KVRangeId rangeId;
    private final LoadingCache<String, IMutationPipeline> storePipelines;
    private final Deque<MutationCallTaskBatch<ReqT, RespT>> batchCallTasks = new ArrayDeque<>();

    protected BatchMutationCall(KVRangeId rangeId, IBaseKVStoreClient storeClient, Duration pipelineExpiryTime) {
        this.rangeId = rangeId;
        storePipelines = Caffeine.newBuilder()
            .evictionListener((RemovalListener<String, IMutationPipeline>) (key, value, cause) -> {
                if (value != null) {
                    value.close();
                }
            })
            .expireAfterAccess(pipelineExpiryTime)
            .build(storeClient::createMutationPipeline);
    }

    @Override
    public final void add(ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask) {
        MutationCallTaskBatch<ReqT, RespT> lastBatchCallTask;
        MutationCallBatcherKey batcherKey = callTask.batcherKey();
        assert callTask.batcherKey().id.equals(rangeId);
        if ((lastBatchCallTask = batchCallTasks.peekLast()) != null) {
            if (lastBatchCallTask.storeId.equals(batcherKey.leaderStoreId) && lastBatchCallTask.ver == batcherKey.ver) {
                if (!lastBatchCallTask.isBatchable(callTask)) {
                    lastBatchCallTask = newBatch(batcherKey.leaderStoreId, batcherKey.ver);
                    batchCallTasks.add(lastBatchCallTask);
                }
                lastBatchCallTask.add(callTask);
            } else {
                lastBatchCallTask = newBatch(batcherKey.leaderStoreId, batcherKey.ver);
                lastBatchCallTask.add(callTask);
                batchCallTasks.add(lastBatchCallTask);
            }
        } else {
            lastBatchCallTask = newBatch(batcherKey.leaderStoreId, batcherKey.ver);
            lastBatchCallTask.add(callTask);
            batchCallTasks.add(lastBatchCallTask);
        }
    }

    protected MutationCallTaskBatch<ReqT, RespT> newBatch(String storeId, long ver) {
        return new MutationCallTaskBatch<>(storeId, ver);
    }

    protected abstract RWCoProcInput makeBatch(Iterator<ReqT> reqIterator);

    protected abstract void handleOutput(Queue<ICallTask<ReqT, RespT, MutationCallBatcherKey>> batchedTasks,
                                         RWCoProcOutput output);

    protected abstract void handleException(ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask, Throwable e);

    @Override
    public void reset() {

    }

    @Override
    public CompletableFuture<Void> execute() {
        MutationCallTaskBatch<ReqT, RespT> batchCallTask = batchCallTasks.poll();
        if (batchCallTask == null) {
            return CompletableFuture.completedFuture(null);
        }
        return fireBatchCall(batchCallTask);
    }

    @Override
    public void destroy() {
        storePipelines.asMap().forEach((k, v) -> v.close());
        storePipelines.invalidateAll();
    }

    private CompletableFuture<Void> fireBatchCall() {
        MutationCallTaskBatch<ReqT, RespT> batchCallTask = batchCallTasks.poll();
        if (batchCallTask == null) {
            return CompletableFuture.completedFuture(null);
        }
        return fireBatchCall(batchCallTask);
    }

    private CompletableFuture<Void> fireBatchCall(MutationCallTaskBatch<ReqT, RespT> batchCallTask) {
        RWCoProcInput input = makeBatch(batchCallTask.batchedTasks.stream().map(ICallTask::call).iterator());
        long reqId = System.nanoTime();
        return storePipelines.get(batchCallTask.storeId)
            .execute(KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(batchCallTask.ver)
                .setKvRangeId(rangeId)
                .setRwCoProc(input)
                .build())
            .thenApply(reply -> {
                if (reply.getCode() == ReplyCode.Ok) {
                    return reply.getRwCoProcResult();
                }
                log.debug("Failed to exec rw co-proc[code={}]", reply.getCode());
                throw new RuntimeException(String.format("Failed to exec rw co-proc[code=%s]", reply.getCode()));
            })
            .handle((v, e) -> {
                if (e != null) {
                    ICallTask<ReqT, RespT, MutationCallBatcherKey> callTask;
                    while ((callTask = batchCallTask.batchedTasks.poll()) != null) {
                        handleException(callTask, e);
                    }
                } else {
                    handleOutput(batchCallTask.batchedTasks, v);
                }
                return null;
            })
            .thenCompose(v -> fireBatchCall());
    }

    protected static class MutationCallTaskBatch<CallT, CallResultT> {
        private final String storeId;
        private final long ver;
        private final LinkedList<ICallTask<CallT, CallResultT, MutationCallBatcherKey>> batchedTasks =
            new LinkedList<>();

        protected MutationCallTaskBatch(String storeId, long ver) {
            this.storeId = storeId;
            this.ver = ver;
        }

        protected void add(ICallTask<CallT, CallResultT, MutationCallBatcherKey> callTask) {
            this.batchedTasks.add(callTask);
        }

        protected boolean isBatchable(ICallTask<CallT, CallResultT, MutationCallBatcherKey> callTask) {
            return true;
        }
    }
}
