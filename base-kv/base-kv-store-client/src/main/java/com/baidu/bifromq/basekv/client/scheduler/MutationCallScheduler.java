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

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MutationCallScheduler<ReqT, RespT>
    extends BatchCallScheduler<ReqT, RespT, MutationCallBatcherKey> {
    protected final IBaseKVStoreClient storeClient;

    public MutationCallScheduler(String name,
                                 IBaseKVStoreClient storeClient,
                                 Duration tolerableLatency,
                                 Duration burstLatency) {
        super(name, tolerableLatency, burstLatency);
        this.storeClient = storeClient;
    }

    public MutationCallScheduler(String name,
                                 IBaseKVStoreClient storeClient,
                                 Duration tolerableLatency,
                                 Duration burstLatency,
                                 Duration batcherExpiry) {
        super(name, tolerableLatency, burstLatency, batcherExpiry);
        this.storeClient = storeClient;
    }

    @Override
    protected final Optional<MutationCallBatcherKey> find(ReqT subCall) {
        Optional<KVRangeSetting> rangeSetting = storeClient.findByKey(rangeKey(subCall));
        return rangeSetting.map(setting -> new MutationCallBatcherKey(setting.id, setting.leader, setting.ver));
    }

    protected abstract ByteString rangeKey(ReqT call);
}