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

package com.baidu.bifromq.baserpc.nameresolver;

import static com.baidu.bifromq.baserpc.loadbalancer.Constants.SERVER_GROUP_TAG_ATTR_KEY;

import com.baidu.bifromq.baserpc.loadbalancer.Constants;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TrafficGovernorNameResolver extends NameResolver {
    private final String serviceUniqueName;
    private final IRPCServiceTrafficDirector trafficDirector;
    private final CompositeDisposable disposable = new CompositeDisposable();

    public TrafficGovernorNameResolver(String serviceUniqueName, IRPCServiceTrafficDirector trafficDirector) {
        this.serviceUniqueName = serviceUniqueName;
        this.trafficDirector = trafficDirector;
    }

    @Override
    public String getServiceAuthority() {
        return serviceUniqueName;
    }

    @Override
    public void start(Listener listener) {
        log.info("Starting TrafficGovernorNameResolver for service[{}]", serviceUniqueName);
        disposable.add(Observable.combineLatest(trafficDirector.trafficDirective(),
                trafficDirector.serverList(), (td, sl) -> {
                    log.info("[{}]Update TrafficGovernorNameResolver with \nservers={}, \ntrafficDirective={}",
                        serviceUniqueName, sl, td);
                    return (Runnable) () -> listener.onAddresses(toAddressGroup(sl), toAttributes(td));
                })
            .subscribe(Runnable::run, e -> listener.onError(Status.INTERNAL.withCause(e))));
    }

    @Override
    public void shutdown() {
        log.info("Start to shutdown trafficGovernor nameResolver, service={}", serviceUniqueName);
        disposable.dispose();
        trafficDirector.destroy();
    }

    @Override
    public void refresh() {
    }

    private List<EquivalentAddressGroup> toAddressGroup(Set<IRPCServiceTrafficDirector.Server> servers) {
        return servers.stream().map(s -> new EquivalentAddressGroup(s.hostAddr, Attributes.newBuilder()
                .set(Constants.SERVER_ID_ATTR_KEY, s.id)
                .set(SERVER_GROUP_TAG_ATTR_KEY, s.groupTags)
                .build()))
            .collect(Collectors.toList());
    }

    private Attributes toAttributes(Map<String, Map<String, Integer>> td) {
        return Attributes.newBuilder().set(Constants.TRAFFIC_DIRECTIVE_ATTR_KEY, td).build();
    }
}

