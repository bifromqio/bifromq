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
import static com.baidu.bifromq.retain.server.scheduler.MatchCallRangeRouter.rangeLookup;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.MatchParam;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.type.TopicMessage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchMatchCall implements IBatchCall<MatchCall, MatchCallResult, MatchCallBatcherKey> {
    private final MatchCallBatcherKey batcherKey;
    private final IBaseKVStoreClient retainStoreClient;
    private final ISettingProvider settingProvider;
    private final Queue<ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey>> tasks = new ArrayDeque<>(128);
    private Set<String> topicFilters = new HashSet<>(128);


    public BatchMatchCall(MatchCallBatcherKey batcherKey,
                          IBaseKVStoreClient retainStoreClient,
                          ISettingProvider settingProvider) {
        this.batcherKey = batcherKey;
        this.retainStoreClient = retainStoreClient;
        this.settingProvider = settingProvider;
    }

    @Override
    public void add(ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey> task) {
        tasks.add(task);
        topicFilters.add(task.call().topicFilter());
    }

    @Override
    public void reset() {
        topicFilters = new HashSet<>(128);
    }

    @Override
    public CompletableFuture<Void> execute() {
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        Map<KVRangeSetting, Set<String>> topicFiltersByRange =
            rangeLookup(batcherKey.tenantId(), topicFilters, retainStoreClient.latestEffectiveRouter());
        List<CompletableFuture<BatchMatchReply>> futures = new ArrayList<>(topicFiltersByRange.size());
        int limit = settingProvider.provide(Setting.RetainMessageMatchLimit, batcherKey.tenantId());
        for (KVRangeSetting rangeSetting : topicFiltersByRange.keySet()) {
            Set<String> topicFilters = topicFiltersByRange.get(rangeSetting);
            futures.add(queryCoProc(BatchMatchRequest.newBuilder()
                .putMatchParams(batcherKey.tenantId(), MatchParam.newBuilder()
                    .putAllTopicFilters(topicFilters.stream().collect(Collectors.toMap(k -> k, v -> limit)))
                    .setNow(now)
                    .build())
                .setReqId(reqId)
                .build(), rangeSetting));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
            .handle((v, e) -> {
                ICallTask<MatchCall, MatchCallResult, MatchCallBatcherKey> task;
                if (e != null) {
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise()
                            .complete(new MatchCallResult(MatchReply.Result.ERROR, Collections.emptyList()));
                    }
                } else {
                    // aggregate result from each reply
                    Map<String, List<MatchResult>> aggregatedResults = new HashMap<>();
                    for (CompletableFuture<BatchMatchReply> future : futures) {
                        BatchMatchReply reply = future.join();
                        Map<String, MatchResult> matchResults =
                            reply.getResultPackMap().get(batcherKey.tenantId()).getResultsMap();
                        matchResults.forEach((topic, matchResult) ->
                            aggregatedResults.computeIfAbsent(topic, k -> new ArrayList<>()).add(matchResult));
                    }
                    while ((task = tasks.poll()) != null) {
                        List<MatchResult> matchResults = aggregatedResults.get(task.call().topicFilter());
                        List<TopicMessage> messages = new LinkedList<>();
                        int i = 0;
                        boolean anySucceed = false;
                        out:
                        for (MatchResult matchResult : matchResults) {
                            if (matchResult.hasOk()) {
                                for (TopicMessage message : matchResult.getOk().getMessagesList()) {
                                    if (i++ < limit) {
                                        messages.add(message);
                                    } else {
                                        break out;
                                    }
                                }
                                anySucceed = true;
                            }
                        }
                        if (anySucceed) {
                            task.resultPromise().complete(new MatchCallResult(OK, messages));
                        } else {
                            task.resultPromise().complete(new MatchCallResult(ERROR, Collections.emptyList()));
                        }
                    }
                }
                return null;
            });
    }

    private CompletableFuture<BatchMatchReply> queryCoProc(BatchMatchRequest request, KVRangeSetting rangeSetting) {
        return retainStoreClient.query(rangeSetting.randomReplica(), KVRangeRORequest.newBuilder()
                .setReqId(request.getReqId())
                .setKvRangeId(rangeSetting.id)
                .setVer(rangeSetting.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setRetainService(RetainServiceROCoProcInput.newBuilder()
                        .setBatchMatch(request)
                        .build())
                    .build())
                .build())
            .thenApply(v -> {
                if (v.getCode() == ReplyCode.Ok) {
                    BatchMatchReply batchMatchReply = v.getRoCoProcResult()
                        .getRetainService()
                        .getBatchMatch();
                    assert batchMatchReply.getReqId() == request.getReqId();
                    return batchMatchReply;
                }
                log.warn("Failed to exec ro co-proc[code={}]", v.getCode());
                throw new RuntimeException("Failed to exec rw co-proc");
            });
    }
}
