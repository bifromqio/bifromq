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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcher;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.retain.rpc.proto.RetainRequest;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainCallScheduler extends MutationCallScheduler<RetainRequest, RetainReply>
    implements IRetainCallScheduler {
    private final ISettingProvider settingProvider;
    private final IBaseKVStoreClient retainStoreClient;

    public RetainCallScheduler(ISettingProvider settingProvider,
                               IBaseKVStoreClient retainStoreClient) {
        super("retain_server_retain_batcher", retainStoreClient,
            Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(DATA_PLANE_BURST_LATENCY_MS.get()));
        this.settingProvider = settingProvider;
        this.retainStoreClient = retainStoreClient;
    }

    @Override
    protected Batcher<RetainRequest, RetainReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                     long tolerableLatencyNanos,
                                                                                     long burstLatencyNanos,
                                                                                     MutationCallBatcherKey batchKey) {
        return new RetainCallBatcher(batchKey, name, tolerableLatencyNanos,
            burstLatencyNanos, settingProvider, retainStoreClient);
    }

    @Override
    protected ByteString rangeKey(RetainRequest request) {
        return tenantNS(request.getPublisher().getTenantId());
    }

    private static class RetainCallBatcher extends MutationCallBatcher<RetainRequest, RetainReply> {
        private final ISettingProvider settingProvider;

        protected RetainCallBatcher(MutationCallBatcherKey batchKey,
                                    String name,
                                    long tolerableLatencyNanos,
                                    long burstLatencyNanos,
                                    ISettingProvider settingProvider,
                                    IBaseKVStoreClient retainStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, batchKey, retainStoreClient);
            this.settingProvider = settingProvider;
        }

        @Override
        protected IBatchCall<RetainRequest, RetainReply, MutationCallBatcherKey> newBatch() {
            return new BatchRetainCall(batcherKey.id, settingProvider, storeClient, Duration.ofMinutes(5));
        }
    }
}
