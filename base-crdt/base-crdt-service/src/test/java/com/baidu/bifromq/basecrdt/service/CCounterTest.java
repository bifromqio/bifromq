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

package com.baidu.bifromq.basecrdt.service;

import com.baidu.bifromq.basecrdt.core.api.CCounterOperation;
import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.ICCounter;
import com.baidu.bifromq.basecrdt.service.annotation.ServiceCfg;
import com.baidu.bifromq.basecrdt.service.annotation.ServiceCfgs;
import org.junit.Assert;
import org.junit.Test;

public class CCounterTest extends CRDTServiceTestTemplate {
    @ServiceCfgs(services =
        {
            @ServiceCfg(id = "s1", bindPort = 11111, isSeed = true),
            @ServiceCfg(id = "s2", bindPort = 22222)
        })
    @Test
    public void testZeroOut() {
        ICRDTService service1 = testCluster.getService("s1");
        ICRDTService service2 = testCluster.getService("s2");
        String uri = CRDTURI.toURI(CausalCRDTType.cctr, "test");
        service1.host(uri);
        service2.host(uri);
        awaitUntilTrue(() -> service1.aliveReplicas(uri).blockingFirst().size() == 2);
        awaitUntilTrue(() -> service2.aliveReplicas(uri).blockingFirst().size() == 2);

        ICCounter counter1 = (ICCounter) service1.get(uri).get();
        ICCounter counter2 = (ICCounter) service2.get(uri).get();
        counter1.execute(CCounterOperation.add(1)).join();
        counter2.execute(CCounterOperation.add(2)).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        Assert.assertEquals(3, counter1.read());

        counter2.execute(CCounterOperation.zeroOut()).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        awaitUntilTrue(() -> counter1.read() == 1);

        service1.stopHosting(uri).join();
        service2.stopHosting(uri).join();
    }
}
