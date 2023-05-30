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

package com.baidu.bifromq.sessiondict.server;

import com.baidu.bifromq.baserpc.IRPCServer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class SessionDictServer implements ISessionDictionaryServer {
    private final String serviceUniqueName;
    private final IRPCServer rpcServer;
    private final SessionDictionaryService service = new SessionDictionaryService();

    SessionDictServer(String serviceUniqueName) {
        this.serviceUniqueName = serviceUniqueName;
        this.rpcServer = buildRPCServer(service);
    }

    protected abstract IRPCServer buildRPCServer(SessionDictionaryService distService);

    @Override
    public void start() {
        log.info("Starting session dict server");
        log.debug("Starting rpc server");
        rpcServer.start();
        log.info("Session dict Server started");
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        log.info("Shutting down session dict server");
        log.debug("Shutting down rpc server");
        rpcServer.shutdown();
        log.debug("Closing session dict service");
        service.close();
        log.info("Session dict server shutdown");
    }
}