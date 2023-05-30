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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.type.ClientInfo;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ProvideCallHelperTest {
    @Mock
    private ISettingProvider mockProvider;

    private ClientInfo clientInfo = ClientInfo.getDefaultInstance();
    private ProvideCallHelper helper;

    @Test
    public void provideDelay() {
        helper = new ProvideCallHelper(8, 1, mockProvider);
        when(mockProvider.provide(Setting.MaxTopicLevels, clientInfo)).thenAnswer((Answer<Integer>) invocation -> {
            Thread.sleep(100);
            return 32;
        });
        await().until(() -> 32 == (int) helper.provide(Setting.MaxTopicLevels, clientInfo));
        helper.close();
    }

    @SneakyThrows
    @Test
    public void provideReturnError() {
        helper = new ProvideCallHelper(8, 1, mockProvider);
        assertEquals(255, (int) helper.provide(Setting.MaxTopicLength, clientInfo));
        when(mockProvider.provide(Setting.MaxTopicLength, clientInfo)).thenThrow(new RuntimeException("Intended Exception"));
        Thread.sleep(100);
        assertEquals(255, (int) helper.provide(Setting.MaxTopicLength, clientInfo));
        helper.close();
    }
}
