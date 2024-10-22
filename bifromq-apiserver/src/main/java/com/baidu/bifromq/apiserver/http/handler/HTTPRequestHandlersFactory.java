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

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandlersFactory;
import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HTTPRequestHandlersFactory implements IHTTPRequestHandlersFactory {
    private final Map<Class<? extends IHTTPRequestHandler>, IHTTPRequestHandler> handlers = new HashMap<>();

    public HTTPRequestHandlersFactory(IAgentHost agentHost,
                                      IMqttBrokerClient brokerClient,
                                      ISessionDictClient sessionDictClient,
                                      IDistClient distClient,
                                      IInboxClient inboxClient,
                                      IRetainClient retainClient,
                                      ISettingProvider settingProvider) {
        register(new HTTPGetDictTrafficDirective(sessionDictClient));
        register(new HTTPGetDistTrafficDirective(distClient));
        register(new HTTPGetInboxTrafficDirective(inboxClient));
        register(new HTTPGetRetainTrafficDirective(retainClient));

        register(new HTTPSetDictTrafficDirective(sessionDictClient));
        register(new HTTPSetDistTrafficDirective(distClient));
        register(new HTTPSetInboxTrafficDirective(inboxClient));
        register(new HTTPSetRetainTrafficDirective(retainClient));

        register(new HTTPUnsetDictTrafficDirective(sessionDictClient));
        register(new HTTPUnsetDistTrafficDirective(distClient));
        register(new HTTPUnsetInboxTrafficDirective(inboxClient));
        register(new HTTPUnsetRetainTrafficDirective(retainClient));

        register(new HTTPGetBrokerServerLandscapeHandler(brokerClient));
        register(new HTTPGetDictServerLandscapeHandler(sessionDictClient));
        register(new HTTPGetDistServerLandscapeHandler(distClient));
        register(new HTTPGetInboxServerLandscapeHandler(inboxClient));
        register(new HTTPGetRetainServerLandscapeHandler(retainClient));
        register(new HTTPGetClusterHandler(agentHost));
        register(new HTTPGetSessionInfoHandler(settingProvider, sessionDictClient));
        register(new HTTPSetDistServerGroupsHandler(distClient));
        register(new HTTPSetInboxServerGroupsHandler(inboxClient));
        register(new HTTPSetRetainServerGroupsHandler(retainClient));
        register(new HTTPSetDictServerGroupsHandler(sessionDictClient));

        register(new HTTPKillHandler(settingProvider, sessionDictClient));
        register(new HTTPRetainHandler(settingProvider, retainClient));
        register(new HTTPExpireRetainHandler(settingProvider, retainClient));
        register(new HTTPPubHandler(settingProvider, distClient));
        register(new HTTPSubHandler(settingProvider, sessionDictClient));
        register(new HTTPUnsubHandler(settingProvider, sessionDictClient));
        register(new HTTPExpireSessionHandler(settingProvider, inboxClient));
    }

    @Override
    public Collection<IHTTPRequestHandler> build() {
        return handlers.values();
    }

    private void register(IHTTPRequestHandler handler) {
        handlers.put(handler.getClass(), handler);
    }
}
