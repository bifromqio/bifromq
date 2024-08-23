/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basekv.balance.impl;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.balance.StoreBalancer;
import com.baidu.bifromq.basekv.balance.command.BootstrapCommand;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.utils.DescriptorUtil;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The balancer is used to bootstrap KVRange to ensure the key space is complete.
 */
public class RangeBootstrapBalancer extends StoreBalancer {
    private record BootstrapTrigger(KVRangeId id, Boundary boundary, long triggerTime) {

    }

    private final Supplier<Long> millisSource;
    private final long suspicionDurationMillis;
    private AtomicReference<BootstrapTrigger> bootstrapTrigger = new AtomicReference<>();

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public RangeBootstrapBalancer(String clusterId,
                                  String localStoreId) {
        this(clusterId, localStoreId, Duration.ofSeconds(15), HLC.INST::getPhysical);
    }

    /**
     * Constructor of the balancer with 15 seconds of suspicion duration.
     *
     * @param clusterId         the id of the BaseKV cluster which the store belongs to
     * @param localStoreId      the id of the store which the balancer is responsible for
     * @param suspicionDuration the duration of the replica being suspected unreachable
     */
    public RangeBootstrapBalancer(String clusterId, String localStoreId, Duration suspicionDuration) {
        this(clusterId, localStoreId, suspicionDuration, HLC.INST::getPhysical);
    }

    /**
     * Constructor of balancer.
     *
     * @param clusterId         the id of the BaseKV cluster which the store belongs to
     * @param localStoreId      the id of the store which the balancer is responsible for
     * @param suspicionDuration the duration of the replica being suspected unreachable
     * @param millisSource      the time source in milliseconds precision
     */
    RangeBootstrapBalancer(String clusterId,
                           String localStoreId,
                           Duration suspicionDuration,
                           Supplier<Long> millisSource) {
        super(clusterId, localStoreId);
        this.millisSource = millisSource;
        this.suspicionDurationMillis = suspicionDuration.toMillis();
    }


    @Override
    public void update(Set<KVRangeStoreDescriptor> storeDescriptors) {
        Optional<DescriptorUtil.EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(storeDescriptors);
        if (effectiveEpoch.isEmpty()) {
            if (bootstrapTrigger.get() == null) {
                KVRangeId rangeId = KVRangeIdUtil.generate();
                log.debug("No epoch found, schedule bootstrap command to create first full boundary range: {}",
                    KVRangeIdUtil.toString(rangeId));
                bootstrapTrigger.set(new BootstrapTrigger(rangeId, FULL_BOUNDARY, randomSuspicionTimeout()));
            }
        }
    }

    @Override
    public Optional<BootstrapCommand> balance() {
        BootstrapTrigger current = bootstrapTrigger.get();
        if (current != null && millisSource.get() > current.triggerTime) {
            bootstrapTrigger.set(null);
            return Optional.of(BootstrapCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(current.id)
                .boundary(current.boundary)
                .build());
        }
        return Optional.empty();
    }

    private long randomSuspicionTimeout() {
        return millisSource.get()
            + ThreadLocalRandom.current().nextLong(suspicionDurationMillis, suspicionDurationMillis * 2);
    }
}
