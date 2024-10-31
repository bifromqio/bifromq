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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BaseKVClusterMetadataManagerTest {
    private IAgentHost agentHost;
    private ICRDTService crdtService;
    private IBaseKVMetaService metaService;

    @BeforeMethod
    void setup() {
        agentHost = IAgentHost.newInstance(AgentHostOptions.builder().addr("127.0.0.1").build());
        agentHost.start();
        crdtService = ICRDTService.newInstance(CRDTServiceOptions.builder().build());
        crdtService.start(agentHost);
        metaService = new BaseKVMetaService(crdtService);
    }

    @AfterMethod
    void tearDown() {
        metaService.stop();
        crdtService.stop();
        agentHost.shutdown();
    }


    @Test
    public void noLoadRules() {
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        String loadRules = manager.loadRules().blockingFirst();
        assertEquals(loadRules, "{}");
    }

    @Test
    public void setLoadRules() {
        String loadRulesJSON = "{\"key\":\"value\"}";
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");

        manager.setLoadRules(loadRulesJSON).join();
        assertEquals(loadRulesJSON, manager.loadRules().blockingFirst());

        loadRulesJSON = "";
        manager.setLoadRules(loadRulesJSON).join();
        assertEquals(loadRulesJSON, manager.loadRules().blockingFirst());
    }

    @Test
    public void noLandscape() {
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        assertTrue(manager.landscape().blockingFirst().isEmpty());
    }

    @Test
    public void reportStoreDescriptor() {
        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.report(descriptor).join();
        assertEquals(Map.of(descriptor.getId(), descriptor), manager.landscape().blockingFirst());
        assertEquals(descriptor, manager.getStoreDescriptor(descriptor.getId()).get());
    }

    @Test
    public void stopReport() {
        IBaseKVClusterMetadataManager manager = metaService.metadataManager("test");
        manager.stopReport("testStoreId").join();

        KVRangeStoreDescriptor descriptor = KVRangeStoreDescriptor.newBuilder()
            .setId("testStoreId")
            .setHlc(HLC.INST.get())
            .build();
        manager.report(descriptor).join();
        assertTrue(manager.getStoreDescriptor(descriptor.getId()).isPresent());

        manager.stopReport("testStoreId").join();
        assertEquals(Collections.emptyMap(), manager.landscape().blockingFirst());
        assertTrue(manager.getStoreDescriptor(descriptor.getId()).isEmpty());
    }
}
