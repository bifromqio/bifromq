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

package com.baidu.bifromq.inbox.server.benchmark;

import com.baidu.bifromq.basecluster.AgentHostOptions;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecrdt.service.CRDTServiceOptions;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.localengine.RocksDBKVEngineConfigurator;
import com.baidu.bifromq.basekv.store.option.KVRangeStoreOptions;
import com.baidu.bifromq.baseutils.PortUtil;
import com.baidu.bifromq.inbox.client.IInboxBrokerClient;
import com.baidu.bifromq.inbox.client.IInboxReaderClient;
import com.baidu.bifromq.inbox.server.IInboxServer;
import com.baidu.bifromq.inbox.store.IInboxStore;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@Slf4j
@State(Scope.Benchmark)
abstract class InboxServiceState {
    private static final String DB_NAME = "testDB";
    private static final String DB_CHECKPOINT_DIR_NAME = "testDB_cp";

    private static final String DB_WAL_NAME = "testWAL";
    private static final String DB_WAL_CHECKPOINT_DIR = "testWAL_cp";

    protected IInboxReaderClient inboxReaderClient;
    protected IInboxBrokerClient inboxBrokerClient;
    private IAgentHost agentHost;
    private ICRDTService clientCrdtService;
    private ICRDTService serverCrdtService;
    private ISettingProvider settingProvider = Setting::current;
    private IBaseKVStoreClient inboxStoreKVStoreClient;

    private IInboxStore inboxStore;

    private IInboxServer inboxServer;

    private TemporaryFolder dbRootDir = new TemporaryFolder();

    private IEventCollector eventCollector = new IEventCollector() {
        @Override
        public <T extends Event> void report(T event) {

        }
    };

    public InboxServiceState() {
        try {
            dbRootDir.create();
        } catch (IOException e) {
        }
        AgentHostOptions agentHostOpts = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .port(PortUtil.freePort())
            .baseProbeInterval(Duration.ofSeconds(10))
            .joinRetryInSec(5)
            .joinTimeout(Duration.ofMinutes(5))
            .build();
        agentHost = IAgentHost.newInstance(agentHostOpts);
        agentHost.start();
        CRDTServiceOptions crdtServiceOptions = CRDTServiceOptions.builder().build();
        clientCrdtService = ICRDTService.newInstance(crdtServiceOptions);
        clientCrdtService.start(agentHost);

        serverCrdtService = ICRDTService.newInstance(crdtServiceOptions);
        serverCrdtService.start(agentHost);

        inboxBrokerClient = IInboxBrokerClient.inProcClientBuilder().build();
        inboxReaderClient = IInboxReaderClient.inProcClientBuilder().build();


        KVRangeStoreOptions kvRangeStoreOptions = new KVRangeStoreOptions();
//        kvRangeStoreOptions.setDataEngineConfigurator(new InMemoryKVEngineConfigurator());
//        kvRangeStoreOptions.setWalEngineConfigurator(new InMemoryKVEngineConfigurator());
        kvRangeStoreOptions.setDataEngineConfigurator(new RocksDBKVEngineConfigurator());
        String uuid = UUID.randomUUID().toString();
        ((RocksDBKVEngineConfigurator) kvRangeStoreOptions.getDataEngineConfigurator())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_CHECKPOINT_DIR_NAME, uuid)
                .toString())
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_NAME, uuid).toString());
        kvRangeStoreOptions.setWalEngineConfigurator(new RocksDBKVEngineConfigurator());
        ((RocksDBKVEngineConfigurator) kvRangeStoreOptions
            .getWalEngineConfigurator())
            .setDbCheckpointRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_WAL_CHECKPOINT_DIR, uuid)
                .toString())
            .setDbRootDir(Paths.get(dbRootDir.getRoot().toString(), DB_WAL_NAME, uuid).toString());

        inboxStoreKVStoreClient = IBaseKVStoreClient
            .inProcClientBuilder()
            .clusterId(IInboxStore.CLUSTER_NAME)
            .crdtService(clientCrdtService)
            .build();
        inboxStore = IInboxStore.inProcBuilder()
            .agentHost(agentHost)
            .crdtService(serverCrdtService)
            .storeClient(inboxStoreKVStoreClient)
            .eventCollector(eventCollector)
            .kvRangeStoreOptions(kvRangeStoreOptions)
            .build();
        inboxServer = IInboxServer.inProcBuilder()
            .settingProvider(settingProvider)
            .storeClient(inboxStoreKVStoreClient)
            .build();
    }

    @Setup(Level.Trial)
    public void setup() {
        inboxStore.start(true);
        inboxServer.start();
        inboxStoreKVStoreClient.join();
        afterSetup();
        log.info("Setup finished, and start testing");
    }

    protected abstract void afterSetup();

    @TearDown(Level.Trial)
    public void teardown() {
        log.info("Finish testing, and tearing down");
        beforeTeardown();
        inboxBrokerClient.close();
        inboxReaderClient.stop();
        log.debug("Inbox server stopping");
        inboxServer.shutdown();
        log.debug("Inbox store client stopping");
        inboxStoreKVStoreClient.stop();
        log.debug("Inbox store stopping");
        inboxStore.stop();
        log.debug("crdt stopping");
        clientCrdtService.stop();
        serverCrdtService.stop();
        log.debug("agent host stopping");
        agentHost.shutdown();
        dbRootDir.delete();
    }

    protected abstract void beforeTeardown();
}
