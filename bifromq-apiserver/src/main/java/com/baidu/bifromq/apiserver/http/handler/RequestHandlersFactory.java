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
import com.baidu.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RequestHandlersFactory implements IHTTPRequestHandlersFactory {
    private final Map<Class<? extends IHTTPRequestHandler>, IHTTPRequestHandler> handlers = new HashMap<>();

    public RequestHandlersFactory(IAgentHost agentHost,
                                  IRPCServiceTrafficService trafficService,
                                  IBaseKVMetaService metaService,
                                  ISessionDictClient sessionDictClient,
                                  IDistClient distClient,
                                  IInboxClient inboxClient,
                                  IRetainClient retainClient,
                                  ISettingProvider settingProvider) {
        register(new GetLoadRulesHandler(metaService));
        register(new SetLoadRulesHandler(metaService));

        register(new GetTrafficRulesHandler(trafficService));
        register(new SetTrafficRulesHandler(trafficService));
        register(new UnsetTrafficRulesHandler(trafficService));

        register(new GetClusterHandler(agentHost));
        register(new ListAllServicesHandler(trafficService));
        register(new GetServiceLandscapeHandler(trafficService));
        register(new SetServerGroupTagsHandler(trafficService));

        register(new GetSessionInfoHandler(settingProvider, sessionDictClient));
        register(new KillHandler(settingProvider, sessionDictClient));
        register(new RetainHandler(settingProvider, retainClient));
        register(new ExpireRetainHandler(settingProvider, retainClient));
        register(new PubHandler(settingProvider, distClient));
        register(new SubHandler(settingProvider, sessionDictClient));
        register(new UnsubHandler(settingProvider, sessionDictClient));
        register(new ExpireSessionHandler(settingProvider, inboxClient));
    }

    @Override
    public Collection<IHTTPRequestHandler> build() {
        return handlers.values();
    }

    private void register(IHTTPRequestHandler handler) {
        handlers.put(handler.getClass(), handler);
    }
}
