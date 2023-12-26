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

package com.baidu.bifromq.plugin.authprovider;


import com.baidu.bifromq.plugin.authprovider.type.Failed;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Success;
import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import org.pf4j.ExtensionPoint;

public interface IAuthProvider extends ExtensionPoint {
    /**
     * Implement this method to hook authentication logic of mqtt3 client into BifroMQ.
     *
     * @param authData the authentication data
     */
    CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData);

    default CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        MQTT3AuthData.Builder mqtt3AuthDataBuilder = MQTT3AuthData.newBuilder();
        if (authData.hasCert()) {
            mqtt3AuthDataBuilder.setCert(authData.getCert());
        }
        if (authData.hasUsername()) {
            mqtt3AuthDataBuilder.setUsername(authData.getUsername());
        }
        if (authData.hasPassword()) {
            mqtt3AuthDataBuilder.setPassword(authData.getPassword());
        }
        if (authData.hasClientId()) {
            mqtt3AuthDataBuilder.setClientId(authData.getClientId());
        }
        mqtt3AuthDataBuilder.setRemoteAddr(authData.getRemoteAddr());
        mqtt3AuthDataBuilder.setRemotePort(authData.getRemotePort());
        mqtt3AuthDataBuilder.setClientId(authData.getClientId());
        return auth(mqtt3AuthDataBuilder.build()).thenApply(mqtt3AuthResult -> {
            MQTT5AuthResult.Builder mqtt5AuthResultBuilder = MQTT5AuthResult.newBuilder();
            switch (mqtt3AuthResult.getTypeCase()) {
                case OK -> {
                    mqtt5AuthResultBuilder.setSuccess(Success.newBuilder()
                        .setTenantId(mqtt3AuthResult.getOk().getTenantId())
                        .setUserId(mqtt3AuthResult.getOk().getUserId())
                        .putAllAttrs(mqtt3AuthResult.getOk().getAttrsMap())
                        .build());
                }
                case REJECT -> {
                    Failed.Builder failedBuilder = Failed.newBuilder();
                    switch (mqtt3AuthResult.getReject().getCode()) {
                        case BadPass -> failedBuilder.setCode(Failed.Code.BadPass);
                        case NotAuthorized -> failedBuilder.setCode(Failed.Code.NotAuthorized);
                        case Error -> failedBuilder.setCode(Failed.Code.Error);
                    }
                    if (mqtt3AuthResult.getReject().hasReason()) {
                        failedBuilder.setReason(mqtt3AuthResult.getReject().getReason());
                    }
                    mqtt5AuthResultBuilder.setFailed(failedBuilder.build());
                }
            }
            return mqtt5AuthResultBuilder.build();
        });
    }

    /**
     * Implement this method to hook action permission check logic.
     *
     * @param client the client to check permission
     * @param action the action
     */
    CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {
    }
}
