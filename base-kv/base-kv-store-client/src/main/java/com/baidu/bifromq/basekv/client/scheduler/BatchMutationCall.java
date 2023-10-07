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

package com.baidu.bifromq.basekv.client.scheduler;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IMutationPipeline;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
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
public abstract class BatchMutationCall<Req, Resp> implements IBatchCall<Req, Resp, MutationCallBatcherKey> {
    private final KVRangeId rangeId;
    private final LoadingCache<String, IMutationPipeline> storePipelines;
    private final Deque<BatchCallTask<Req, Resp>> batchCallTasks = new ArrayDeque<>();

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
    public void add(CallTask<Req, Resp, MutationCallBatcherKey> callTask) {
        BatchCallTask<Req, Resp> lastBatchCallTask;
        MutationCallBatcherKey batcherKey = callTask.batcherKey;
        assert callTask.batcherKey.id.equals(rangeId);
        if ((lastBatchCallTask = batchCallTasks.peekLast()) != null) {
            if (lastBatchCallTask.storeId.equals(batcherKey.leaderStoreId) && lastBatchCallTask.ver == batcherKey.ver) {
                lastBatchCallTask.batchedTasks.add(callTask);
            } else {
                lastBatchCallTask = new BatchCallTask<>(batcherKey.leaderStoreId, batcherKey.ver);
                lastBatchCallTask.batchedTasks.add(callTask);
                batchCallTasks.add(lastBatchCallTask);
            }
        } else {
            lastBatchCallTask = new BatchCallTask<>(batcherKey.leaderStoreId, batcherKey.ver);
            lastBatchCallTask.batchedTasks.add(callTask);
            batchCallTasks.add(lastBatchCallTask);
        }
    }

    protected abstract RWCoProcInput makeBatch(Iterator<Req> reqIterator);

    protected abstract void handleOutput(Queue<CallTask<Req, Resp, MutationCallBatcherKey>> batchedTasks,
                                         RWCoProcOutput output);

    protected abstract void handleException(CallTask<Req, Resp, MutationCallBatcherKey> callTask, Throwable e);

    @Override
    public void reset() {

    }

    @Override
    public CompletableFuture<Void> execute() {
        BatchCallTask<Req, Resp> batchCallTask = batchCallTasks.poll();
        if (batchCallTask == null) {
            return CompletableFuture.completedFuture(null);
        }
        return fireBatchCall(batchCallTask);
    }

    @Override
    public void destroy() {
        storePipelines.invalidateAll();
    }

    private CompletableFuture<Void> fireBatchCall() {
        BatchCallTask<Req, Resp> batchCallTask = batchCallTasks.poll();
        if (batchCallTask == null) {
            return CompletableFuture.completedFuture(null);
        }
        return fireBatchCall(batchCallTask);
    }

    private CompletableFuture<Void> fireBatchCall(BatchCallTask<Req, Resp> batchCallTask) {
        RWCoProcInput input = makeBatch(batchCallTask.batchedTasks.stream()
            .map(call -> call.call).iterator());
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
                throw new RuntimeException();
            })
            .handle((v, e) -> {
                if (e != null) {
                    CallTask<Req, Resp, MutationCallBatcherKey> callTask;
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

    private static class BatchCallTask<Req, Resp> {
        final String storeId;
        final long ver;
        final LinkedList<CallTask<Req, Resp, MutationCallBatcherKey>> batchedTasks = new LinkedList<>();

        private BatchCallTask(String storeId, long ver) {
            this.storeId = storeId;
            this.ver = ver;
        }
    }
}
