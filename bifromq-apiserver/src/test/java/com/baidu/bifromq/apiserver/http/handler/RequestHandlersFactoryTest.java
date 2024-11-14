/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.apiserver.MockableTest;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import io.reactivex.rxjava3.core.Observable;
import java.util.Collection;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class RequestHandlersFactoryTest extends MockableTest {
    @Mock
    private IAgentHost agentHost;
    @Mock
    private IBaseKVMetaService metaService;
    @Mock
    private IRPCServiceTrafficGovernor trafficGovernor;
    @Mock
    private IMqttBrokerClient brokerClient;
    @Mock
    private IDistClient distClient;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private ISessionDictClient sessionDictClient;
    @Mock
    private IRetainClient retainClient;
    @Mock
    private ISettingProvider settingProvider;

    @Test
    public void build() {
        when(metaService.clusterIds()).thenReturn(Observable.empty());
        when(trafficGovernor.serverList()).thenReturn(Observable.empty());
        when(trafficGovernor.trafficRules()).thenReturn(Observable.empty());
        when(brokerClient.trafficGovernor()).thenReturn(trafficGovernor);
        when(distClient.trafficGovernor()).thenReturn(trafficGovernor);
        when(inboxClient.trafficGovernor()).thenReturn(trafficGovernor);
        when(sessionDictClient.trafficGovernor()).thenReturn(trafficGovernor);
        when(retainClient.trafficGovernor()).thenReturn(trafficGovernor);
        RequestHandlersFactory handlersFactory =
            new RequestHandlersFactory(agentHost, metaService, brokerClient, sessionDictClient,
                distClient, inboxClient, retainClient, settingProvider);
        Collection<IHTTPRequestHandler> handlers = handlersFactory.build();
        assertEquals(handlers.size(), 32);
    }
}
