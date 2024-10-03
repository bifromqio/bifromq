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

package com.baidu.bifromq.baserpc.loadbalancer;

import static java.util.Collections.emptyMap;

import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.baserpc.RPCContext;
import io.grpc.LoadBalancer;
import io.grpc.MethodDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TrafficDirectiveAwarePicker extends LoadBalancer.SubchannelPicker implements IServerSelector {
    private final BluePrint bluePrint;

    private final AtomicReference<ITrafficRouter> currentMatcher =
        new AtomicReference<>(new TrafficRouter(emptyMap(), emptyMap(), emptyMap()));

    @Builder
    public TrafficDirectiveAwarePicker(BluePrint bluePrint) {
        this.bluePrint = bluePrint;
    }

    public void refresh(Map<String, Map<String, Integer>> trafficDirective,
                        Map<TrafficDirectiveLoadBalancer.ServerKey, List<LoadBalancer.Subchannel>> subchannelMap,
                        Map<String, Set<String>> serverGroupTags) {
        currentMatcher.set(new TrafficRouter(trafficDirective, subchannelMap, serverGroupTags));
    }

    @Override
    public LoadBalancer.PickResult pickSubchannel(LoadBalancer.PickSubchannelArgs pickSubchannelArgs) {
        boolean collectSelection = Boolean.parseBoolean(pickSubchannelArgs.getHeaders()
            .get(Constants.COLLECT_SELECTION_METADATA_META_KEY));
        if (collectSelection && RPCContext.SELECTED_SERVER_ID_CTX_KEY.get() == null) {
            return LoadBalancer.PickResult.withDrop(Constants.TRANSIENT_FAILURE);
        }
        pickSubchannelArgs.getHeaders().remove(Constants.COLLECT_SELECTION_METADATA_META_KEY,
            Boolean.toString(collectSelection));
        String tenantId = pickSubchannelArgs.getHeaders().get(Constants.TENANT_ID_META_KEY);
        MethodDescriptor<?, ?> methodDescriptor = pickSubchannelArgs.getMethodDescriptor();
        ITrafficRouter matcher = currentMatcher.get();
        // if this is a direct lb request
        String designatedServerId = pickSubchannelArgs.getHeaders().get(Constants.DESIRED_SERVER_META_KEY);
        if (designatedServerId != null) {
            assert bluePrint.semantic(methodDescriptor.getFullMethodName()).mode() == BluePrint.BalanceMode.DDBalanced;
            if (matcher.exists(designatedServerId)) {
                log.trace("Direct pick sub-channel by serverId:{}", designatedServerId);
                if (collectSelection) {
                    RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().setServerId(designatedServerId);
                }
                // remove DESIRED_SERVER_META_KEY from header
                pickSubchannelArgs.getHeaders().remove(Constants.DESIRED_SERVER_META_KEY, designatedServerId);
                Optional<LoadBalancer.Subchannel> selectedSubChannel = matcher.getSubchannel(designatedServerId);
                return selectedSubChannel.map(LoadBalancer.PickResult::withSubchannel)
                    .orElseGet(() -> LoadBalancer.PickResult.withDrop(Constants.SERVER_UNREACHABLE));
            }
            return LoadBalancer.PickResult.withDrop(Constants.SERVER_NOT_FOUND);
        } else {
            IServerGroupRouter selector = matcher.get(tenantId);
            Optional<String> selection = switch (bluePrint.semantic(methodDescriptor.getFullMethodName()).mode()) {
                case WCHBalanced -> {
                    // weighted-consistent-hashing mode
                    String hashKey = pickSubchannelArgs.getHeaders().get(Constants.WCH_KEY_META_KEY);
                    yield selector.hashing(hashKey);
                }
                case WRBalanced -> selector.random();
                default -> selector.roundRobin();
            };
            if (selection.isEmpty()) {
                return LoadBalancer.PickResult.withDrop(Constants.SERVICE_UNAVAILABLE);
            }
            log.trace("Picked sub-channel:{} for tenant:{}", selection.get(), tenantId);
            if (collectSelection) {
                RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().setServerId(selection.get());
            }
            Optional<LoadBalancer.Subchannel> selectedSubChannel = matcher.getSubchannel(selection.get());
            return selectedSubChannel.map(LoadBalancer.PickResult::withSubchannel)
                .orElseGet(() -> LoadBalancer.PickResult.withDrop(Constants.SERVER_UNREACHABLE));
        }
    }

    @Override
    public boolean exists(String tenantId, String serverId, MethodDescriptor<?, ?> methodDescriptor) {
        assert bluePrint.semantic(methodDescriptor.getFullMethodName()).mode() == BluePrint.BalanceMode.DDBalanced;
        return currentMatcher.get().exists(serverId);
    }

    @Override
    public boolean isBalancable(String tenantId, String serverId, MethodDescriptor<?, ?> methodDescriptor) {
        IServerGroupRouter selector = currentMatcher.get().get(tenantId);
        return selector.exists(serverId);
    }

    @Override
    public Optional<String> hashing(String tenantId, String key, MethodDescriptor<?, ?> methodDescriptor) {
        assert bluePrint.semantic(methodDescriptor.getFullMethodName()).mode() == BluePrint.BalanceMode.WCHBalanced;
        // weighted-consistent-hashing mode
        IServerGroupRouter selector = currentMatcher.get().get(tenantId);
        return selector.hashing(key);
    }

    @Override
    public Optional<String> roundRobin(String tenantId, MethodDescriptor<?, ?> methodDescriptor) {
        assert bluePrint.semantic(methodDescriptor.getFullMethodName()).mode() == BluePrint.BalanceMode.WRRBalanced;
        // weighted-round-robin mode
        IServerGroupRouter selector = currentMatcher.get().get(tenantId);
        return selector.roundRobin();
    }

    @Override
    public Optional<String> random(String tenantId, MethodDescriptor<?, ?> methodDescriptor) {
        assert bluePrint.semantic(methodDescriptor.getFullMethodName()).mode() == BluePrint.BalanceMode.WRBalanced;
        // weighted-random mode
        IServerGroupRouter selector = currentMatcher.get().get(tenantId);
        return selector.random();
    }
}
