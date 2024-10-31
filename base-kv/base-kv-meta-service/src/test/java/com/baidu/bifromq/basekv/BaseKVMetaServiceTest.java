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

package com.baidu.bifromq.basekv;

import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import io.reactivex.rxjava3.observers.TestObserver;
import java.net.InetSocketAddress;
import java.util.Set;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVMetaServiceTest {
    private IAgentHost agentHost1;
    private IAgentHost agentHost2;
    private ICRDTService crdtService1;
    private ICRDTService crdtService2;
    private IBaseKVMetaService metaService1;
    private IBaseKVMetaService metaService2;

    @BeforeMethod
    void setup() {
        agentHost1 = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        agentHost1.start();
        crdtService1 = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService1.start(agentHost1);
        metaService1 = new BaseKVMetaService(crdtService1);

        agentHost2 = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        agentHost2.start();

        agentHost1.join(Set.of(new InetSocketAddress(agentHost2.local().getAddress(), agentHost2.local().getPort())))
            .join();
        crdtService2 = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService2.start(agentHost2);
        metaService2 = new BaseKVMetaService(crdtService2);
    }

    @AfterMethod
    void tearDown() {
        metaService1.stop();
        crdtService1.stop();
        agentHost1.shutdown();
        metaService2.stop();
        crdtService2.stop();
        agentHost2.shutdown();
    }

    @Test
    public void clusterIds() {
        assertTrue(metaService1.clusterIds().blockingFirst().isEmpty());
        assertTrue(metaService2.clusterIds().blockingFirst().isEmpty());

        TestObserver<Set<String>> clusterIdObserver1 = metaService1.clusterIds().test();
        TestObserver<Set<String>> clusterIdObserver2 = metaService2.clusterIds().test();

        IBaseKVClusterMetadataManager metadataManager1 = metaService1.metadataManager("testCluster1");
        metadataManager1.report(KVRangeStoreDescriptor.newBuilder().setId("testStoreId1").build());

        clusterIdObserver1.awaitCount(2);
        clusterIdObserver1.assertValueAt(1, Set.of("testCluster1"));

        clusterIdObserver2.awaitCount(2);
        clusterIdObserver2.assertValueAt(1, Set.of("testCluster1"));

        IBaseKVClusterMetadataManager metadataManager2 = metaService1.metadataManager("testCluster2");
        metadataManager2.report(KVRangeStoreDescriptor.newBuilder().setId("testStoreId2").build()).join();

        clusterIdObserver1.awaitCount(3);
        clusterIdObserver1.assertValueAt(2, Set.of("testCluster1", "testCluster2"));

        clusterIdObserver2.awaitCount(3);
        clusterIdObserver2.assertValueAt(2, Set.of("testCluster1", "testCluster2"));

        metadataManager1.stopReport("testStoreId1").join();

        clusterIdObserver1.awaitCount(4);
        clusterIdObserver1.assertValueAt(3, Set.of("testCluster2"));

        clusterIdObserver2.awaitCount(4);
        clusterIdObserver2.assertValueAt(3, Set.of("testCluster2"));
    }
}
