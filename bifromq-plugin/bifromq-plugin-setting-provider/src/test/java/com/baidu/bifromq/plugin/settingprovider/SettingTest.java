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

package com.baidu.bifromq.plugin.settingprovider;

import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.MQTT3ClientInfo;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SettingTest {
    @Test
    public void enumInitialValue() {
        for (Setting setting : Setting.values()) {
            assertTrue(setting.isValid(setting.current(ClientInfo.getDefaultInstance())));
        }
    }

    @Test
    public void customClassifier() {
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("abc")
            .setMqtt3ClientInfo(MQTT3ClientInfo.newBuilder()
                .setUserId("123")
                .build())
            .build();
        Setting.MaxTopicLevels.current(clientInfo, 32);
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(32));

        Setting.MaxTopicLevels.setClientClassifier(c -> c.getTenantId());
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(16));

        Setting.MaxTopicLevels.current(clientInfo, 32);
        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(32));
    }

    @Test
    public void customValueExpiry() {
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("abc")
            .setMqtt3ClientInfo(MQTT3ClientInfo.newBuilder()
                .setUserId("123")
                .build())
            .build();
        Setting.MaxTopicLevels.current(clientInfo, 32);

        Setting.MaxTopicLevels.currentVals.invalidateAll();

        assertTrue(Setting.MaxTopicLevels.current(clientInfo).equals(16));
    }

    @Test
    public void systemPropertyOverride() {
        System.setProperty("MsgPubPerSec", "100");
        assertEquals((int) Setting.MsgPubPerSec.resolve(200), 100);

        System.setProperty("MsgPubPerSec", "sdfa");
        assertEquals((int) Setting.MsgPubPerSec.resolve(200), 200);
    }
}
