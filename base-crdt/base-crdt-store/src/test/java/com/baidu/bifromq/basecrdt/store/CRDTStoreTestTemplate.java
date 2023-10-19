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

package com.baidu.bifromq.basecrdt.store;

import static org.testng.Assert.fail;

import com.baidu.bifromq.basecrdt.core.api.CRDTEngineOptions;
import com.baidu.bifromq.basecrdt.store.annotation.StoreCfg;
import com.baidu.bifromq.basecrdt.store.annotation.StoreCfgs;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class CRDTStoreTestTemplate {
    protected CRDTStoreTestCluster storeMgr;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        storeMgr = new CRDTStoreTestCluster();
        createClusterByAnnotation(method);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
                method.getDeclaringClass().getName(), method.getName());
        CRDTStoreTestCluster lastStoreMgr = storeMgr;
        new Thread(lastStoreMgr::shutdown).start();
    }

    public void createClusterByAnnotation(Method testMethod) {
        StoreCfgs storeCfgs = testMethod.getAnnotation(StoreCfgs.class);
        StoreCfg storeCfg = testMethod.getAnnotation(StoreCfg.class);
        if (storeCfgs != null) {
            for (StoreCfg cfg : storeCfgs.stores()) {
                storeMgr.newStore(cfg.id(), build(cfg));
            }
        }
        if (storeCfg != null) {
            storeMgr.newStore(storeCfg.id(), build(storeCfg));
        }
    }

    public void awaitUntilTrue(Callable<Boolean> condition, long timeoutInMS) {
        try {
            long waitingTime = 0;
            while (!condition.call()) {
                Thread.sleep(500);
                waitingTime += 500;
                if (waitingTime > timeoutInMS) {
                    fail("timeout");
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void awaitUntilTrue(Callable<Boolean> condition) {
        awaitUntilTrue(condition, 15000);
    }

    private CRDTStoreTestCluster.CRDTStoreMeta build(StoreCfg cfg) {
        return new CRDTStoreTestCluster.CRDTStoreMeta(
            CRDTStoreOptions.builder()
                .maxEventsInDelta(cfg.maxEventsInBatch())
                .engineOptions(new CRDTEngineOptions()
                    .inflationInterval(Duration.ofMillis(cfg.inflationInterval()))
                    .orHistoryExpireTime(Duration.ofMillis(cfg.historyExpireTime())))
                .build(),
            cfg.packetLossPercent(),
            cfg.packetDelayTime(),
            cfg.packetRandom()
        );
    }
}
