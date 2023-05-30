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

package com.baidu.bifromq.baserpc.trafficgovernor;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.proto.RPCServer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class RPCServiceTrafficGovernor extends RPCServiceTrafficDirector implements IRPCServiceTrafficGovernor {

    public RPCServiceTrafficGovernor(String serviceUniqueName, ICRDTService crdtService) {
        super(serviceUniqueName, crdtService);
    }

    @Override
    public void assignLBGroups(String id, Set<String> groupTags) {
        Optional<RPCServer> announced = announcedServer(id);
        if (announced.isPresent() && (!groupTags.containsAll(announced.get().getGroupList()) ||
            !announced.get().getGroupList().containsAll(groupTags))) {
            RPCServer updated = announced.get().toBuilder()
                .clearGroup()
                .addAllGroup(groupTags)
                .setAnnouncedTS(System.currentTimeMillis())
                .build();
            announce(updated);
        }
    }

    @Override
    public void updateTrafficDirective(Map<String, Map<String, Integer>> trafficDirective) {
        announce(trafficDirective);
    }
}
