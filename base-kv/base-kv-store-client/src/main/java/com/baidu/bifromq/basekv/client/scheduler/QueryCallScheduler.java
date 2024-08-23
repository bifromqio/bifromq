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

import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;

public abstract class QueryCallScheduler<ReqT, RespT> extends BatchCallScheduler<ReqT, RespT, QueryCallBatcherKey> {
    protected final IBaseKVStoreClient storeClient;

    public QueryCallScheduler(String name,
                              IBaseKVStoreClient storeClient,
                              Duration tolerableLatency,
                              Duration burstLatency) {
        super(name, tolerableLatency, burstLatency);
        this.storeClient = storeClient;
    }

    public QueryCallScheduler(String name,
                              IBaseKVStoreClient storeClient,
                              Duration tolerableLatency,
                              Duration burstLatency,
                              Duration batcherExpiry) {
        super(name, tolerableLatency, burstLatency, batcherExpiry);
        this.storeClient = storeClient;
    }


    protected String selectStore(KVRangeSetting setting, ReqT request) {
        return setting.leader;
    }

    protected abstract int selectQueue(ReqT request);

    protected abstract ByteString rangeKey(ReqT request);

    @Override
    protected final Optional<QueryCallBatcherKey> find(ReqT req) {
        return storeClient.findByKey(rangeKey(req)).map(
            range -> new QueryCallBatcherKey(range.id, selectStore(range, req), selectQueue(req), range.ver));
    }


}
