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

package com.baidu.bifromq.basecluster;

import com.baidu.bifromq.basecluster.annotation.StoreCfg;
import com.baidu.bifromq.basecluster.annotation.StoreCfgs;
import com.baidu.bifromq.basecrdt.core.api.CRDTEngineOptions;
import com.baidu.bifromq.basecrdt.store.CRDTStoreOptions;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

@Slf4j
public abstract class AgentTestTemplate {
    protected AgentTestCluster storeMgr;

    AgentTestTemplate() {
    }

    @Rule
    public final TestRule rule = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            super.starting(description);
            storeMgr = new AgentTestCluster();
            StoreCfgs storeCfgs = description.getAnnotation(StoreCfgs.class);
            StoreCfg storeCfg = description.getAnnotation(StoreCfg.class);
            String seedStoreId = null;
            if (storeMgr != null) {
                if (storeCfgs != null) {
                    for (StoreCfg cfg : storeCfgs.stores()) {
                        storeMgr.newHost(cfg.id(), build(cfg));
                        if (cfg.isSeed()) {
                            seedStoreId = cfg.id();
                        }
                    }
                }
                if (storeCfg != null) {
                    storeMgr.newHost(storeCfg.id(), build(storeCfg));
                }
                if (seedStoreId != null && storeCfgs != null) {
                    for (StoreCfg cfg : storeCfgs.stores()) {
                        if (!cfg.id().equals(seedStoreId)) {
                            storeMgr.join(cfg.id(), seedStoreId);
                        }
                    }
                }
            }
            log.info("Starting test: " + description.getMethodName());
        }
    };

    @BeforeClass
    public static void setupOnce() {
        LoggingRegistryConfig registryConfig = new LoggingRegistryConfig() {
            @Override
            public String get(String s) {
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(1);
            }
        };
        LoggingMeterRegistry meterRegistry = new LoggingMeterRegistry(registryConfig, Clock.SYSTEM);
//        Metrics.addRegistry(meterRegistry);
    }

    @After
    public void teardown() {
        if (storeMgr != null) {
            log.info("Shutting down test cluster");
            storeMgr.shutdown();
        }
    }

    private AgentHostOptions build(StoreCfg cfg) {
        return AgentHostOptions.builder()
            .addr(cfg.bindAddr())
            .port(cfg.bindPort())
            .udpPacketLimit(1400)
            .maxChannelsPerHost(1)
            .joinRetryInSec(cfg.joinRetryInSec())
            .joinTimeout(Duration.ofSeconds(cfg.joinTimeout()))
            .crdtStoreOptions(CRDTStoreOptions.builder()
                .engineOptions(CRDTEngineOptions.builder()
                    .orHistoryExpireTime(Duration.ofSeconds(cfg.compactDelayInSec()))
                    .build())
                .maxEventsInDelta(100)
                .build()
            ).build();
    }
}
