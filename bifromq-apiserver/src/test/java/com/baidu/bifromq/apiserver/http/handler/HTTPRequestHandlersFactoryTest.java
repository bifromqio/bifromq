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

package com.baidu.bifromq.apiserver.http.handler;

import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.apiserver.MockableTest;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import java.util.Collection;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPRequestHandlersFactoryTest extends MockableTest {
    @Mock
    private IDistClient distClient;
    @Mock
    private IMqttBrokerClient mqttBrokerClient;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private ISessionDictClient sessionDictClient;

    @Test
    public void build() {
        HTTPRequestHandlersFactory handlersFactory = new HTTPRequestHandlersFactory(sessionDictClient,
                distClient, mqttBrokerClient, inboxClient);
        Collection<IHTTPRequestHandler> handlers = handlersFactory.build();
        assertEquals(handlers.size(), 4);
    }
}
