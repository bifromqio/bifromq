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

import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Map;
import org.testng.annotations.Test;

public class RPCServiceTrafficGovernorTest extends RPCServiceAnnouncerTest {

    @Test(groups = "integration")
    public void defaultTrafficDirective() {
        String service = "service";
        ICRDTService crdtService = newCRDTService();
        IRPCServiceTrafficGovernor trafficGovernor = IRPCServiceTrafficGovernor.newInstance(service, crdtService);
        assertTrue(trafficGovernor.trafficDirective().blockingFirst().isEmpty());

        trafficGovernor.destroy();
        crdtService.stop();
    }

    @Test(groups = "integration")
    public void updateTrafficDirective() {
        String service = "service";
        Map<String, Map<String, Integer>> td = singletonMap("tenantA", singletonMap("group1", 1));
        ICRDTService tgCrdtService = newCRDTService();
        IRPCServiceTrafficGovernor trafficGovernor = IRPCServiceTrafficGovernor.newInstance(service, tgCrdtService);

        ICRDTService tdCrdtService = newCRDTService();
        IRPCServiceTrafficDirector trafficDirector = IRPCServiceTrafficDirector.newInstance(service, tdCrdtService);

        trafficGovernor.setTrafficDirective("tenantA", singletonMap("group1", 1));

        await().until(() -> trafficGovernor.trafficDirective().blockingFirst().equals(td)
            && trafficDirector.trafficDirective().blockingFirst().equals(td));

        Map<String, Map<String, Integer>> td1 =
            Map.of("tenantA", singletonMap("group1", 1), "tenantB", singletonMap("group2", 1));
        trafficGovernor.setTrafficDirective("tenantB", singletonMap("group2", 1));

        await().until(() -> trafficGovernor.trafficDirective().blockingFirst().equals(td1)
            && trafficDirector.trafficDirective().blockingFirst().equals(td1));

        trafficGovernor.unsetTrafficDirective("tenantB");

        await().until(() -> trafficGovernor.trafficDirective().blockingFirst().equals(td)
            && trafficDirector.trafficDirective().blockingFirst().equals(td));

        trafficGovernor.destroy();
        tgCrdtService.stop();

        trafficDirector.destroy();
        tdCrdtService.stop();
    }

    @Test(groups = "integration")
    public void updateGroupAssignments() {
        String service = "service";
        String server = "server";
        String lbGroup = "group";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);
        ICRDTService srCrdtService = newCRDTService();
        IRPCServiceServerRegister serverRegister = IRPCServiceServerRegister.newInstance(service, srCrdtService);
        serverRegister.start(server, hostAddr);

        ICRDTService tgCrdtService = newCRDTService();
        IRPCServiceTrafficGovernor trafficGovernor = IRPCServiceTrafficGovernor.newInstance(service, tgCrdtService);

        ICRDTService tdCrdtService = newCRDTService();
        IRPCServiceTrafficDirector trafficDirector = IRPCServiceTrafficDirector.newInstance(service, tdCrdtService);
        await().until(() -> !trafficGovernor.serverList().blockingFirst().isEmpty());

        trafficGovernor.setServerGroups(server, Sets.newHashSet(lbGroup)).join();

        await().until(() -> trafficGovernor.serverList().blockingFirst().stream()
            .anyMatch(s -> s.groupTags.contains(lbGroup)) &&
            trafficDirector.serverList().blockingFirst().stream().anyMatch(s -> s.groupTags.contains(lbGroup)));

        serverRegister.stop();
        srCrdtService.stop();

        trafficDirector.destroy();
        tdCrdtService.stop();

        trafficGovernor.destroy();
        tgCrdtService.stop();
    }
}
