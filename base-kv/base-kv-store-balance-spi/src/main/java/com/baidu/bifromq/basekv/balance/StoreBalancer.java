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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.logger.SiftLogger;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;

/**
 * The base class for implementing store balancer.
 */
public abstract class StoreBalancer {
    protected final Logger log;
    protected final String clusterId;
    protected final String localStoreId;

    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public StoreBalancer(String clusterId, String localStoreId) {
        this.log = SiftLogger.getLogger("balancer.logger", "clusterId", clusterId, "storeId", localStoreId);
        this.clusterId = clusterId;
        this.localStoreId = localStoreId;
    }

    public abstract void update(Set<KVRangeStoreDescriptor> storeDescriptors);

    public abstract Optional<BalanceCommand> balance();

}
