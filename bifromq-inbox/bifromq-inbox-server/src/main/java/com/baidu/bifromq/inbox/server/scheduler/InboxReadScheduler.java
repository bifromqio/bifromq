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

package com.baidu.bifromq.inbox.server.scheduler;

import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.DATA_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.scheduler.QueryCallScheduler;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public abstract class InboxReadScheduler<Req, Resp> extends QueryCallScheduler<Req, Resp> {
    protected final int queuesPerRange;

    public InboxReadScheduler(int queuesPerRange, IBaseKVStoreClient inboxStoreClient, String name) {
        super(name, inboxStoreClient, Duration.ofMillis(DATA_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofSeconds(DATA_PLANE_BURST_LATENCY_MS.get()));
        Preconditions.checkArgument(queuesPerRange > 0, "Queues per range must be positive");
        this.queuesPerRange = queuesPerRange;
    }

    @Override
    protected int selectQueue(Req request) {
        return ThreadLocalRandom.current().nextInt(0, queuesPerRange);
    }
}
