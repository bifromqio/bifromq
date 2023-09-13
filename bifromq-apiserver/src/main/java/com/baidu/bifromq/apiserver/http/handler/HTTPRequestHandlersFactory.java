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

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.http.IHTTPRequestHandlersFactory;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HTTPRequestHandlersFactory implements IHTTPRequestHandlersFactory {
    private final Map<Class<? extends IHTTPRequestHandler>, IHTTPRequestHandler> handlers = new HashMap<>();

    public HTTPRequestHandlersFactory(ISessionDictClient sessionDictClient,
                                      IDistClient distClient,
                                      IMqttBrokerClient mqttBrokerClient,
                                      IInboxClient inboxClient) {
        register(new HTTPKickHandler(sessionDictClient));
        register(new HTTPPubHandler(distClient));
        register(new HTTPSubHandler(mqttBrokerClient, inboxClient));
        register(new HTTPUnsubHandler(mqttBrokerClient, inboxClient));
    }

    @Override
    public Collection<IHTTPRequestHandler> build() {
        return handlers.values();
    }

    private void register(IHTTPRequestHandler handler) {
        handlers.put(handler.getClass(), handler);
    }
}
