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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.toQInboxId;
import static com.baidu.bifromq.dist.entity.EntityUtil.toScopedTopicFilter;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.ICallTask;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.MatchReply;
import com.baidu.bifromq.dist.rpc.proto.MatchRequest;
import com.baidu.bifromq.dist.rpc.proto.TenantOption;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class BatchMatchCall extends BatchMutationCall<MatchRequest, MatchReply> {
    private final ISettingProvider settingProvider;

    BatchMatchCall(KVRangeId rangeId, IBaseKVStoreClient distWorkerClient, Duration pipelineExpiryTime,
                   ISettingProvider settingProvider) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
        this.settingProvider = settingProvider;
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<MatchRequest> reqIterator) {
        BatchMatchRequest.Builder reqBuilder = BatchMatchRequest.newBuilder();
        Map<String, TenantOption> tenantOptionMap = new HashMap<>();
        while (reqIterator.hasNext()) {
            MatchRequest matchReq = reqIterator.next();
            String qInboxId =
                toQInboxId(matchReq.getBrokerId(), matchReq.getReceiverId(), matchReq.getDelivererKey());
            String scopedTopicFilter =
                toScopedTopicFilter(matchReq.getTenantId(), qInboxId, matchReq.getTopicFilter());
            reqBuilder.putScopedTopicFilter(scopedTopicFilter, matchReq.getIncarnation());
            tenantOptionMap.computeIfAbsent(matchReq.getTenantId(), k -> TenantOption.newBuilder()
                .setMaxReceiversPerSharedSubGroup(
                    settingProvider.provide(Setting.MaxSharedGroupMembers, matchReq.getTenantId()))
                .build());
        }
        reqBuilder.putAllOptions(tenantOptionMap);
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(reqBuilder
                    .setReqId(reqId)
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleException(ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask, Throwable e) {
        callTask.resultPromise().completeExceptionally(e);
    }

    @Override
    protected void handleOutput(Queue<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask;
        while ((callTask = batchedTasks.poll()) != null) {
            BatchMatchReply reply = output.getDistService().getBatchMatch();
            MatchRequest subCall = callTask.call();
            String qInboxId = toQInboxId(subCall.getBrokerId(), subCall.getReceiverId(), subCall.getDelivererKey());
            String scopedTopicFilter = toScopedTopicFilter(subCall.getTenantId(), qInboxId, subCall.getTopicFilter());
            BatchMatchReply.Result result = reply.getResultsOrDefault(scopedTopicFilter, BatchMatchReply.Result.ERROR);
            MatchReply.Result matchResult = switch (result) {
                case OK:
                    yield MatchReply.Result.OK;
                case EXCEED_LIMIT:
                    yield MatchReply.Result.EXCEED_LIMIT;
                case ERROR:
                default:
                    yield MatchReply.Result.ERROR;
            };
            callTask.resultPromise().complete(MatchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(matchResult)
                .build());
        }
    }
}
