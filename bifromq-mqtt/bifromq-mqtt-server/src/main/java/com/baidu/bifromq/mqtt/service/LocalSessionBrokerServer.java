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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.baserpc.IRPCServer;
import com.baidu.bifromq.mqtt.session.IMQTTSession;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;

abstract class LocalSessionBrokerServer implements ILocalSessionBrokerServer {
    private final LocalSessionBrokerService service;
    private final IRPCServer server;

    public LocalSessionBrokerServer() {
        service = new LocalSessionBrokerService();
        server = buildRPCServer(service);
    }

    protected abstract IRPCServer buildRPCServer(LocalSessionBrokerService service);

    @Override
    public String id() {
        return server.id();
    }

    @Override
    public CompletableFuture<Void> disconnectAll(int disconnectRate) {
        return service.disconnectAll(disconnectRate);
    }

    @Override
    public void start() {
        server.start();
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        server.shutdown();
        service.close();
    }

    @Override
    public void add(String sessionId, IMQTTSession session) {
        service.reg(sessionId, session);
    }

    @Override
    public boolean remove(String sessionId, IMQTTSession session) {
        return service.unreg(sessionId, session);
    }

    @Override
    public List<IMQTTSession> removeAll() {
        return null;
    }
}
