/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.session;

import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTTSessionAuthProvider implements IAuthProvider {
    private final IAuthProvider delegate;
    private final ChannelHandlerContext ctx;
    private final LinkedHashMap<CompletableFuture<CheckResult>, CompletableFuture<CheckResult>>
        checkPermissionTaskQueue =
        new LinkedHashMap<>();

    public MQTTSessionAuthProvider(IAuthProvider delegate, ChannelHandlerContext ctx) {
        this.delegate = delegate;
        this.ctx = ctx;
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        return delegate.auth(authData);
    }

    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        return delegate.auth(authData);
    }

    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return delegate.extendedAuth(authData);
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        return delegate.check(client, action);
    }

    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        assert ctx.executor().inEventLoop();
        CompletableFuture<CheckResult> task = delegate.checkPermission(client, action);
        if (task.isDone()) {
            return task;
        } else {
            // queue it for fifo semantic
            CompletableFuture<CheckResult> onDone = new CompletableFuture<>();
            // in case authProvider returns same future object;
            task = task.thenApply(v -> v);
            checkPermissionTaskQueue.put(task, onDone);
            task.whenCompleteAsync((_v, _e) -> {
                Iterator<CompletableFuture<CheckResult>> itr = checkPermissionTaskQueue.keySet().iterator();
                while (itr.hasNext()) {
                    CompletableFuture<CheckResult> k = itr.next();
                    if (k.isDone()) {
                        CompletableFuture<CheckResult> r = checkPermissionTaskQueue.get(k);
                        try {
                            r.complete(k.join());
                        } catch (Throwable e) {
                            r.completeExceptionally(e);
                        }
                        itr.remove();
                    } else {
                        break;
                    }
                }
            }, ctx.executor());
            return onDone;
        }
    }
}
