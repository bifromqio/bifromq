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
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineExpireTimeSeconds;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineOverflowDropOldest;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineQueueSize;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.BatchMutationCall;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public class BatchCreateCall extends BatchMutationCall<CreateInboxRequest, List<String>> {
    private final ISettingProvider settingProvider;

    protected BatchCreateCall(KVRangeId rangeId,
                              IBaseKVStoreClient distWorkerClient,
                              ISettingProvider settingProvider,
                              Duration pipelineExpiryTime) {
        super(rangeId, distWorkerClient, pipelineExpiryTime);
        this.settingProvider = settingProvider;
    }

    @Override
    protected RWCoProcInput makeBatch(Iterator<CreateInboxRequest> createInboxRequestIterator) {
        BatchCreateRequest.Builder reqBuilder = BatchCreateRequest.newBuilder();
        createInboxRequestIterator.forEachRemaining(request -> {
            ClientInfo client = request.getClientInfo();
            String tenantId = client.getTenantId();
            String scopedInboxIdUtf8 = scopedInboxId(tenantId, request.getInboxId()).toStringUtf8();
            reqBuilder.putInboxes(scopedInboxIdUtf8, CreateParams.newBuilder()
                .setExpireSeconds(settingProvider.provide(OfflineExpireTimeSeconds, tenantId))
                .setLimit(settingProvider.provide(OfflineQueueSize, tenantId))
                .setDropOldest(settingProvider.provide(OfflineOverflowDropOldest, tenantId))
                .setClient(client)
                .build());
        });
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                .setReqId(reqId)
                .setBatchCreate(reqBuilder.build())
                .build())
            .build();
    }

    @Override
    protected void handleOutput(Queue<CallTask<CreateInboxRequest, List<String>, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        CallTask<CreateInboxRequest, List<String>, MutationCallBatcherKey> callTask;
        while ((callTask = batchedTasks.poll()) != null) {
            String scopedInboxId = scopedInboxId(callTask.call.getClientInfo().getTenantId(),
                callTask.call.getInboxId()).toStringUtf8();
            callTask.callResult.complete(output.getInboxService().getBatchCreate()
                .getSubsOrDefault(scopedInboxId, TopicFilterList.getDefaultInstance())
                .getTopicFiltersList());
        }
    }

    @Override
    protected void handleException(CallTask<CreateInboxRequest, List<String>, MutationCallBatcherKey> callTask,
                                   Throwable e) {
        callTask.callResult.completeExceptionally(e);
    }
}
