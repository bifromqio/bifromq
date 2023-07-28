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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import io.grpc.netty.InProcSocketAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class RPCServiceLandscapeTest extends RPCServiceAnnouncerTest {
    @Test(groups = "integration")
    public void startAndStop() {
        ICRDTService crdtService = newCRDTService();
        IRPCServiceServerRegister serverRegister = IRPCServiceServerRegister
            .newInstance("serviceAAA", crdtService);
        serverRegister.start("server1", new InetSocketAddress("127.0.0.1", 90));
        serverRegister.stop();
    }

    @Test(groups = "integration")
    public void localServerDiscovery() {
        String service = "service";
        String server = "server";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);
        ICRDTService crdtService = newCRDTService();
        IRPCServiceServerRegister serverRegister = IRPCServiceServerRegister
            .newInstance(service, crdtService);
        serverRegister.start(server, hostAddr);
        IRPCServiceTrafficDirector trafficDirector = IRPCServiceTrafficDirector.newInstance(service, crdtService);
        await().until(() -> {
            Set<IRPCServiceTrafficDirector.Server> servers = trafficDirector.serverList().blockingFirst();
            return servers.stream().anyMatch(s -> s.id.equals(server) && s.hostAddr instanceof InProcSocketAddress);
        });

        // stop the server
        serverRegister.stop();
        await().until(() -> {
            Set<IRPCServiceTrafficDirector.Server> servers = trafficDirector.serverList().blockingFirst();
            return servers.isEmpty();
        });

        trafficDirector.destroy();
        crdtService.stop();
    }

    @Test(groups = "integration")
    public void remoteServerDiscovery() {
        String service = "service";
        String server = "server";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);

        ICRDTService clientCrdtService = newCRDTService();
        IRPCServiceTrafficDirector trafficDirector = IRPCServiceTrafficDirector.newInstance(service, clientCrdtService);
        assertTrue(trafficDirector.serverList().blockingFirst().isEmpty());

        // start a server
        ICRDTService serverCrdtService = newCRDTService();
        IRPCServiceServerRegister serverRegister = IRPCServiceServerRegister
            .newInstance(service, serverCrdtService);
        serverRegister.start(server, hostAddr);

        // new server discovered
        await().until(() -> {
            Set<IRPCServiceTrafficDirector.Server> servers = trafficDirector.serverList().blockingFirst();
            return servers.stream().anyMatch(s -> s.id.equals(server) && s.hostAddr instanceof InProcSocketAddress);
        });
        // stop the server
        serverRegister.stop();
        serverCrdtService.stop();

        await().until(() -> {
            Set<IRPCServiceTrafficDirector.Server> servers = trafficDirector.serverList().blockingFirst();
            return servers.isEmpty();
        });

        // start a server again
        serverCrdtService = newCRDTService();
        serverRegister = IRPCServiceServerRegister.newInstance(service, serverCrdtService);
        serverRegister.start(server, hostAddr);

        // server discovered again
        await().until(() -> {
            Set<IRPCServiceTrafficDirector.Server> servers = trafficDirector.serverList().blockingFirst();
            return servers.stream().anyMatch(s -> s.id.equals(server) && s.hostAddr instanceof InProcSocketAddress);
        });

        trafficDirector.destroy();
        clientCrdtService.stop();

        // stop the server
        serverRegister.stop();
        serverCrdtService.stop();
    }
}
