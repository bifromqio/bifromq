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

package com.baidu.bifromq.baserpc.trafficgovernor;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.proto.RPCServer;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

final class RPCServiceTrafficGovernor extends RPCServiceTrafficDirector implements IRPCServiceTrafficGovernor {

    public RPCServiceTrafficGovernor(String serviceUniqueName, ICRDTService crdtService) {
        super(serviceUniqueName, crdtService);
    }

    @Override
    public CompletableFuture<Void> setServerGroups(String serverId, Set<String> groupTags) {
        Optional<RPCServer> announced = announcedServer(serverId);
        if (announced.isPresent()) {
            if (!groupTags.equals(Sets.newHashSet(announced.get().getGroupList()))) {
                RPCServer updated = announced.get().toBuilder()
                    .clearGroup()
                    .addAllGroup(groupTags)
                    .setAnnouncedTS(HLC.INST.get())
                    .build();
                return announce(updated);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
        return CompletableFuture.failedFuture(new RuntimeException("Server not found: " + serverId));
    }

    @Override
    public CompletableFuture<Void> updateTrafficDirective(Map<String, Map<String, Integer>> trafficDirective) {
        return announce(trafficDirective);
    }
}
