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

package com.baidu.bifromq.baserpc.loadbalancer;

import static com.baidu.bifromq.baserpc.loadbalancer.Constants.SERVER_GROUP_TAG_ATTR_KEY;
import static com.baidu.bifromq.baserpc.loadbalancer.Constants.SERVER_ID_ATTR_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.baidu.bifromq.baserpc.BluePrint;
import com.google.common.collect.Maps;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficDirectiveLoadBalancer extends LoadBalancer {

    private final Helper helper;

    private final IUpdateListener updateListener;

    private final TrafficDirectiveAwarePicker currentPicker;

    private final AtomicBoolean balancingStateUpdateScheduled = new AtomicBoolean(false);

    private volatile Map<String, Map<String, Integer>> currentTrafficDirective;

    // serverId->subchannelRegistry
    private final Map<String, List<Subchannel>> subchannelRegistry = Maps.newHashMap();
    private final Map<String, Set<String>> lbGroupAssignment = Maps.newHashMap();

    TrafficDirectiveLoadBalancer(Helper helper, BluePrint bluePrint,
                                 IUpdateListener updateListener) {
        this.helper = checkNotNull(helper, "helper");
        this.updateListener = updateListener;
        this.currentPicker = new TrafficDirectiveAwarePicker(bluePrint);
    }

    @Override
    public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
        log.debug("Handle traffic change: resolvedAddresses={}", resolvedAddresses);
        Map<String, EquivalentAddressGroup> newResolved = resolvedAddresses
            .getAddresses()
            .stream()
            .collect(Collectors
                .toMap(
                    eag -> eag.getAttributes().get(SERVER_ID_ATTR_KEY),
                    eag -> eag
                )
            );
        lbGroupAssignment.clear();
        for (EquivalentAddressGroup addressGroup : resolvedAddresses.getAddresses()) {
            lbGroupAssignment.put(addressGroup.getAttributes().get(SERVER_ID_ATTR_KEY),
                addressGroup.getAttributes().get(SERVER_GROUP_TAG_ATTR_KEY));
        }
        boolean updatePicker = (!resolvedAddresses.getAttributes()
            .get(Constants.TRAFFIC_DIRECTIVE_ATTR_KEY).equals(currentTrafficDirective));
        currentTrafficDirective = resolvedAddresses.getAttributes().get(Constants.TRAFFIC_DIRECTIVE_ATTR_KEY);
        int requested = Math.min(5, Runtime.getRuntime().availableProcessors());
        Set<String> currentServers = subchannelRegistry.keySet();
        Set<String> latestServers = newResolved.keySet();
        Set<String> addedServers = setsDifference(latestServers, currentServers);
        Set<String> removedServers = setsDifference(currentServers, latestServers);

        // make sure enough subchannelRegistry opened for existing servers.
        for (String serverId : currentServers) {
            if (!removedServers.contains(serverId)) {
                int openNow = subchannelRegistry.get(serverId).size();
                if (requested > openNow) {
                    updatePicker = true;
                    IntStream.range(0, requested - openNow).forEach(
                        i -> subchannelRegistry.get(serverId)
                            .add(setupSubchannel(serverId, newResolved.get(serverId))));
                }
            }
        }

        // Create new subchannelRegistry for new servers.
        for (String serverId : addedServers) {
            subchannelRegistry.compute(serverId, (k, v) -> {
                if (v != null) {
                    log.error("Illegal state: new server already exists: serverId={}", serverId);
                    return v;
                } else {
                    return IntStream.range(0, requested)
                        .mapToObj(i -> setupSubchannel(serverId, newResolved.get(serverId)))
                        .collect(Collectors.toList());
                }
            });
        }

        ArrayList<Subchannel> removedSubchannels = new ArrayList<>();
        for (String serverId : removedServers) {
            removedSubchannels.addAll(subchannelRegistry.remove(serverId));
        }

        // Shutdown removed subchannelRegistry
        for (Subchannel removedSubchannel : removedSubchannels) {
            shutdownSubchannel(removedSubchannel);
        }

        if (updatePicker) {
            // Update the picker before shutting down the subchannelRegistry, to reduce the chance of the race
            // between picking a subchannel and shutting it down.
            scheduleBalancingStateUpdate();
        }
    }

    @Override
    public void handleNameResolutionError(Status status) {
        log.error("Name resolution error:{}", status.getDescription());
        helper.updateBalancingState(TRANSIENT_FAILURE, currentPicker);
    }

    @Override
    public void shutdown() {
        log.debug("Shutting down all subchannels");
        for (List<Subchannel> subchannels : subchannelRegistry.values()) {
            for (Subchannel subchannel : subchannels) {
                shutdownSubchannel(subchannel);
            }
        }
    }

    @Override
    public boolean canHandleEmptyAddressListFromNameResolution() {
        return true;
    }

    private ConnectivityStateInfo getSubchannelState(Subchannel subchannel) {
        return subchannel.getAttributes().get(Constants.STATE_INFO).get();
    }

    private void scheduleBalancingStateUpdate() {
        if (balancingStateUpdateScheduled.compareAndSet(false, true)) {
            helper.getSynchronizationContext().schedule(this::updateBalancingState,
                1,
                TimeUnit.SECONDS,
                helper.getScheduledExecutorService());
        }
    }

    private void updateBalancingState() {
        ConnectivityState newState = determineChannelState();
        if (newState != SHUTDOWN) {
            log.debug("Update balancing state to {}", newState);
            currentPicker.refresh(currentTrafficDirective, subchannelRegistry, lbGroupAssignment);
            helper.updateBalancingState(newState, currentPicker);
            updateListener.onUpdate(currentPicker);
        }
        balancingStateUpdateScheduled.set(false);
    }

    private ConnectivityState determineChannelState() {
        // channel connectivity state aggregation rule:
        // if there is no subchannel or all subchannelRegistry are in TRANSIENT_FAILURE state, the final state is
        // TRANSIENT_FAILURE
        // if no ready subchannel, the final state is CONNECTING
        // if all subchannel shutdown, the final state is SHUTDOWN
        // otherwise state is READY
        ConnectivityState connectivityState = READY;
        if (subchannelRegistry.isEmpty() || subchannelRegistry.values().stream().flatMap(Collection::stream)
            .map(this::getSubchannelState)
            .allMatch(state -> state.getState() == TRANSIENT_FAILURE)) {
            connectivityState = TRANSIENT_FAILURE;
        } else {
            if (subchannelRegistry.values().stream()
                .flatMap(Collection::stream)
                .map(this::getSubchannelState)
                .allMatch(state -> state.getState() == SHUTDOWN)) {
                connectivityState = SHUTDOWN;
            } else if (subchannelRegistry.values().stream()
                .flatMap(Collection::stream)
                .map(this::getSubchannelState)
                .allMatch(state -> state.getState() != READY)) {
                connectivityState = CONNECTING;
            }
        }
        return connectivityState;
    }

    private Subchannel setupSubchannel(String serverId, EquivalentAddressGroup equivalentAddressGroup) {
        final Subchannel subchannel = checkNotNull(
            helper.createSubchannel(CreateSubchannelArgs.newBuilder()
                .setAddresses(equivalentAddressGroup)
                .setAttributes(Attributes.newBuilder()
                    .set(Constants.STATE_INFO,
                        new AtomicReference<>(ConnectivityStateInfo.forNonError(IDLE)))
                    .set(SERVER_ID_ATTR_KEY, serverId)
                    .build())
                .build()),
            "subchannel");
        subchannel.start(state -> handleSubchannelStateChange(subchannel, state));
        subchannel.requestConnection();
        return subchannel;
    }

    private void handleSubchannelStateChange(Subchannel subchannel, ConnectivityStateInfo state) {
        updateSubchannelState(subchannel, state);
        scheduleBalancingStateUpdate();
    }

    private void shutdownSubchannel(Subchannel subchannel) {
        log.trace("Shutdown subchannel: {}", subchannel);
        subchannel.shutdown();
        updateSubchannelState(subchannel, ConnectivityStateInfo.forNonError(SHUTDOWN));
    }

    private void updateSubchannelState(Subchannel subchannel, ConnectivityStateInfo state) {
        log.trace("Subchannel[{}] state change to {}", subchannel, state);
        subchannel.getAttributes().get(Constants.STATE_INFO).set(state);
        if (state.getState() == IDLE) {
            subchannel.requestConnection();
        }
    }

    private static <T> Set<T> setsDifference(Set<T> a, Set<T> b) {
        Set<T> aCopy = new HashSet<>(a);
        aCopy.removeAll(b);
        return aCopy;
    }
}
