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

package com.baidu.bifromq.baserpc.loadbalancer;

import io.grpc.MethodDescriptor;
import java.util.Optional;

/**
 * The interface for choosing server for load balancing.
 */
public interface IServerSelector {
    /**
     * If the server exists.
     *
     * @param tenantId         the tenant id
     * @param serverId         the server id
     * @param methodDescriptor the method descriptor
     * @return true if can be selected
     */
    boolean exists(String tenantId, String serverId, MethodDescriptor<?, ?> methodDescriptor);

    /**
     * If the server can be selected via balancing strategy.
     *
     * @param tenantId         the tenant id
     * @param serverId         the server id
     * @param methodDescriptor the method descriptor
     * @return true if can be selected
     */
    boolean isBalancable(String tenantId, String serverId, MethodDescriptor<?, ?> methodDescriptor);

    /**
     * Select server based on hash key.
     *
     * @param tenantId         the tenant id
     * @param key              the key
     * @param methodDescriptor the method descriptor
     * @return the server id
     */
    Optional<String> hashing(String tenantId, String key, MethodDescriptor<?, ?> methodDescriptor);

    /**
     * Select server based on round-robin strategy.
     *
     * @param tenantId         the tenant id
     * @param methodDescriptor the method descriptor
     * @return the server id
     */
    Optional<String> roundRobin(String tenantId, MethodDescriptor<?, ?> methodDescriptor);

    /**
     * Select server randomly.
     *
     * @param tenantId         the tenant id
     * @param methodDescriptor the method descriptor
     * @return the server id
     */
    Optional<String> random(String tenantId, MethodDescriptor<?, ?> methodDescriptor);
}
