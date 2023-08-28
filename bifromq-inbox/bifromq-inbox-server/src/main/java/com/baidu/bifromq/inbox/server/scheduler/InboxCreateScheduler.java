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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCreateScheduler extends InboxMutateScheduler<CreateInboxRequest, List<String>>
    implements IInboxCreateScheduler {
    private final ISettingProvider settingProvider;

    public InboxCreateScheduler(IBaseKVStoreClient inboxStoreClient, ISettingProvider settingProvider) {
        super(inboxStoreClient, "inbox_server_create");
        this.settingProvider = settingProvider;
    }

    @Override
    protected Batcher<CreateInboxRequest, List<String>, KVRangeSetting> newBatcher(String name,
                                                                                   long tolerableLatencyNanos,
                                                                                   long burstLatencyNanos,
                                                                                   KVRangeSetting range) {
        return new InboxCreateBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient,
            settingProvider);
    }

    @Override
    protected ByteString rangeKey(CreateInboxRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxCreateBatcher extends InboxMutateBatcher<CreateInboxRequest, List<String>> {
        private class InboxCreateBatch implements IBatchCall<CreateInboxRequest, List<String>> {
            private final Queue<CallTask<CreateInboxRequest, List<String>>> batchedTasks =
                new ArrayDeque<>();
            private BatchCreateRequest.Builder reqBuilder = BatchCreateRequest.newBuilder();

            @Override
            public void reset() {
                reqBuilder = BatchCreateRequest.newBuilder();
            }

            @Override
            public void add(CallTask<CreateInboxRequest, List<String>> callTask) {
                CreateInboxRequest request = callTask.call;
                ClientInfo client = request.getClientInfo();
                String tenantId = client.getTenantId();
                String scopedInboxIdUtf8 = scopedInboxId(tenantId, request.getInboxId()).toStringUtf8();
                reqBuilder.putInboxes(scopedInboxIdUtf8, CreateParams.newBuilder()
                    .setExpireSeconds(settingProvider.provide(OfflineExpireTimeSeconds, tenantId))
                    .setLimit(settingProvider.provide(OfflineQueueSize, tenantId))
                    .setDropOldest(settingProvider.provide(OfflineOverflowDropOldest, tenantId))
                    .setClient(client)
                    .build());
                batchedTasks.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                return mutate(InboxServiceRWCoProcInput.newBuilder()
                    .setReqId(reqId)
                    .setBatchCreate(reqBuilder.build())
                    .build())
                    .thenApply(InboxServiceRWCoProcOutput::getBatchCreate)
                    .handle((v, e) -> {
                        if (e != null) {
                            CallTask<CreateInboxRequest, List<String>> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                callTask.callResult.completeExceptionally(e);
                            }
                        } else {
                            CallTask<CreateInboxRequest, List<String>> callTask;
                            while ((callTask = batchedTasks.poll()) != null) {
                                String scopedInboxId = scopedInboxId(callTask.call.getClientInfo().getTenantId(),
                                    callTask.call.getInboxId()).toStringUtf8();
                                callTask.callResult.complete(
                                    v.getSubsOrDefault(scopedInboxId, TopicFilterList.getDefaultInstance())
                                        .getTopicFiltersList());
                            }
                        }
                        return null;
                    });
            }
        }

        private final ISettingProvider settingProvider;

        private InboxCreateBatcher(String name,
                                   long expectLatencyNanos,
                                   long maxTolerableLatencyNanos,
                                   KVRangeSetting range,
                                   IBaseKVStoreClient inboxStoreClient,
                                   ISettingProvider settingProvider) {
            super(name, expectLatencyNanos, maxTolerableLatencyNanos, range, inboxStoreClient);
            this.settingProvider = settingProvider;
        }

        @Override
        protected IBatchCall<CreateInboxRequest, List<String>> newBatch() {
            return new InboxCreateBatch();
        }
    }
}
