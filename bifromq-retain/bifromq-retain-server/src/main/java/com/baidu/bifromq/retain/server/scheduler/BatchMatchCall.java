/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.retain.rpc.proto.MatchReply.Result.ERROR;
import static com.baidu.bifromq.retain.rpc.proto.MatchReply.Result.OK;
import static com.baidu.bifromq.retain.server.scheduler.BatchMatchCallHelper.parallelMatch;
import static com.baidu.bifromq.retain.server.scheduler.BatchMatchCallHelper.serialMatch;
import static com.baidu.bifromq.retain.server.scheduler.MatchCallRangeRouter.rangeLookup;
import static com.baidu.bifromq.util.TopicUtil.isWildcardTopicFilter;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchError;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchMatchCall implements IBatchCall<MatchCall, MatchCallResult, MatchCallBatcherKey> {
    private final MatchCallBatcherKey batcherKey;
    private final IBaseKVStoreClient retainStoreClient;
    private final ISettingProvider settingProvider;
    private final Queue<ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey>> tasks = new ArrayDeque<>(128);
    private Set<String> nonWildcardTopicFilters = new HashSet<>(128);
    private Set<String> wildcardTopicFilters = new HashSet<>(128);


    BatchMatchCall(MatchCallBatcherKey batcherKey, IBaseKVStoreClient retainStoreClient,
                   ISettingProvider settingProvider) {
        this.batcherKey = batcherKey;
        this.retainStoreClient = retainStoreClient;
        this.settingProvider = settingProvider;
    }

    @Override
    public void add(ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey> task) {
        tasks.add(task);
        if (isWildcardTopicFilter(task.call().topicFilter())) {
            wildcardTopicFilters.add(task.call().topicFilter());
        } else {
            nonWildcardTopicFilters.add(task.call().topicFilter());
        }
    }

    @Override
    public void reset() {
        nonWildcardTopicFilters = new HashSet<>(128);
        wildcardTopicFilters = new HashSet<>(128);
    }

    @Override
    public CompletableFuture<Void> execute() {
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        NavigableMap<Boundary, KVRangeSetting> effectiveRouter = retainStoreClient.latestEffectiveRouter();

        Map<KVRangeSetting, Set<String>> parallelMatches =
            rangeLookup(batcherKey.tenantId(), nonWildcardTopicFilters, effectiveRouter);
        CompletableFuture<Map<String, MatchResult>> parallelMatchFutures =
            parallelMatch(reqId, now, parallelMatches, this::match);
        CompletableFuture<Map<String, MatchResult>> wildcardMatchFuture;
        if (wildcardTopicFilters.isEmpty()) {
            wildcardMatchFuture = CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            int limit = settingProvider.provide(Setting.RetainMessageMatchLimit, batcherKey.tenantId());
            Map<KVRangeSetting, Set<String>> serialMatches =
                rangeLookup(batcherKey.tenantId(), wildcardTopicFilters, effectiveRouter);
            wildcardMatchFuture = serialMatch(reqId, now, serialMatches, limit, this::match);
        }

        return CompletableFuture.allOf(parallelMatchFutures, wildcardMatchFuture)
            .handle((v, e) -> {
                ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey> task;
                if (e != null) {
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise()
                            .complete(new MatchCallResult(MatchReply.Result.ERROR, Collections.emptyList()));
                    }
                } else {
                    // aggregate result from each reply
                    Map<String, MatchResult> aggregatedResults = new HashMap<>();
                    aggregatedResults.putAll(parallelMatchFutures.join());
                    aggregatedResults.putAll(wildcardMatchFuture.join());
                    while ((task = tasks.poll()) != null) {
                        MatchResult matchResult = aggregatedResults.get(task.call().topicFilter());
                        if (matchResult.hasOk()) {
                            task.resultPromise()
                                .complete(new MatchCallResult(OK, matchResult.getOk().getMessagesList()));
                        } else {
                            task.resultPromise().complete(new MatchCallResult(ERROR, Collections.emptyList()));
                        }
                    }
                }
                return null;
            });
    }

    private CompletableFuture<Map<String, MatchResult>> match(long reqId, long now, Map<String, Integer> topicFilters,
                                                              KVRangeSetting rangeSetting) {
        BatchMatchRequest request = BatchMatchRequest.newBuilder().putMatchParams(batcherKey.tenantId(),
            MatchParam.newBuilder().putAllTopicFilters(topicFilters).setNow(now).build()).setReqId(reqId).build();
        return queryCoProc(request, rangeSetting).thenApply(
            reply -> reply.getResultPackMap().get(batcherKey.tenantId()).getResultsMap()).exceptionally(
            e -> topicFilters.keySet().stream().collect(Collectors.toMap(k -> k,
                v -> MatchResult.newBuilder().setError(MatchError.getDefaultInstance()).build())));
    }

    private CompletableFuture<BatchMatchReply> queryCoProc(BatchMatchRequest request, KVRangeSetting rangeSetting) {
        return retainStoreClient.query(rangeSetting.randomReplica(),
                KVRangeRORequest.newBuilder().setReqId(request.getReqId()).setKvRangeId(rangeSetting.id)
                    .setVer(rangeSetting.ver).setRoCoProc(ROCoProcInput.newBuilder()
                        .setRetainService(RetainServiceROCoProcInput.newBuilder().setBatchMatch(request).build()).build())
                    .build())
            .thenApply(v -> {
                if (v.getCode() == ReplyCode.Ok) {
                    BatchMatchReply batchMatchReply = v.getRoCoProcResult().getRetainService().getBatchMatch();
                    assert batchMatchReply.getReqId() == request.getReqId();
                    return batchMatchReply;
                }
                log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                throw new RuntimeException("Failed to exec rw co-proc");
            });
    }
}
