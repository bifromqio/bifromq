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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCreateScheduler extends MutationCallScheduler<CreateInboxRequest, List<String>>
    implements IInboxCreateScheduler {
    private final ISettingProvider settingProvider;

    public InboxCreateScheduler(IBaseKVStoreClient inboxStoreClient, ISettingProvider settingProvider) {
        super("inbox_server_create", inboxStoreClient, Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
        this.settingProvider = settingProvider;
    }

    @Override
    protected Batcher<CreateInboxRequest, List<String>, MutationCallBatcherKey> newBatcher(String name,
                                                                                           long tolerableLatencyNanos,
                                                                                           long burstLatencyNanos,
                                                                                           MutationCallBatcherKey range) {
        return new InboxCreateBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, storeClient,
            settingProvider);
    }

    @Override
    protected ByteString rangeKey(CreateInboxRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxCreateBatcher extends MutationCallBatcher<CreateInboxRequest, List<String>> {
        private final ISettingProvider settingProvider;

        private InboxCreateBatcher(String name,
                                   long expectLatencyNanos,
                                   long maxTolerableLatencyNanos,
                                   MutationCallBatcherKey range,
                                   IBaseKVStoreClient inboxStoreClient,
                                   ISettingProvider settingProvider) {
            super(name, expectLatencyNanos, maxTolerableLatencyNanos, range, inboxStoreClient);
            this.settingProvider = settingProvider;
        }

        @Override
        protected IBatchCall<CreateInboxRequest, List<String>, MutationCallBatcherKey> newBatch() {
            return new BatchCreateCall(batcherKey.id, storeClient, settingProvider, Duration.ofMinutes(5));
        }
    }
}
