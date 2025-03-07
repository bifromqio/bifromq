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

import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static com.baidu.bifromq.util.TopicUtil.isNormalTopicFilter;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import com.baidu.bifromq.basekv.client.scheduler.MutationCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.dist.rpc.proto.UnmatchReply;
import com.baidu.bifromq.dist.rpc.proto.UnmatchRequest;
import com.baidu.bifromq.sysprops.props.ControlPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.ControlPlaneTolerableLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnmatchCallScheduler extends MutationCallScheduler<UnmatchRequest, UnmatchReply>
    implements IUnmatchCallScheduler {

    public UnmatchCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_unsub_batcher",
            distWorkerClient,
            Duration.ofMillis(ControlPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(ControlPlaneBurstLatencyMillis.INSTANCE.get()));
    }

    @Override
    protected Batcher<UnmatchRequest, UnmatchReply, MutationCallBatcherKey> newBatcher(String name,
                                                                                       long tolerableLatencyNanos,
                                                                                       long burstLatencyNanos,
                                                                                       MutationCallBatcherKey range) {
        return new UnmatchCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, storeClient);
    }

    protected ByteString rangeKey(UnmatchRequest call) {
        if (isNormalTopicFilter(call.getTopicFilter())) {
            return toNormalRouteKey(call.getTenantId(), call.getTopicFilter(),
                toReceiverUrl(call.getBrokerId(), call.getReceiverId(), call.getDelivererKey()));
        } else {
            return toGroupRouteKey(call.getTenantId(), call.getTopicFilter());
        }
    }
}
