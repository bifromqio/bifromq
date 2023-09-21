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

package com.baidu.bifromq.plugin.authprovider;


import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
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
