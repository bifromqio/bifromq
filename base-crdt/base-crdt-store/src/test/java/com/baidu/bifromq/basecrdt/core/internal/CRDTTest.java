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

package com.baidu.bifromq.basecrdt.core.internal;

import com.baidu.bifromq.basecrdt.proto.Replacement;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;

public abstract class CRDTTest {
    protected ScheduledExecutorService executor;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() {
        MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
    }

    protected IReplicaStateLattice newStateLattice(ByteString ownerReplicaId, long historyDurationInMS) {
        return new InMemReplicaStateLattice(ownerReplicaId,
            Duration.ofMillis(historyDurationInMS),
            Duration.ofMillis(200));
    }

    protected void sync(CausalCRDTInflater left, CausalCRDTInflater right) {
        CompletableFuture<Optional<Iterable<Replacement>>> deltaProto =
            left.delta(right.latticeEvents(), right.historyEvents(), 1024);
        if (deltaProto.join().isPresent()) {
            right.join(deltaProto.join().get()).join();
        }
        deltaProto = right.delta(left.latticeEvents(), left.historyEvents(), 1024);
        if (deltaProto.join().isPresent()) {
            left.join(deltaProto.join().get()).join();
        }
    }
}
