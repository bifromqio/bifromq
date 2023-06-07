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

import com.baidu.bifromq.plugin.authprovider.authdata.MQTTBasicAuth;
import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;

class DevOnlyAuthProvider implements IAuthProvider {
    @Override
    public <T extends AuthData<?>> CompletableFuture<AuthResult> auth(T authData) {
        MQTTBasicAuth data = (MQTTBasicAuth) authData;
        if (data.username().isPresent()) {
            String[] username = data.username().get().split("/");
            if (username.length == 2) {
                return CompletableFuture.completedFuture(AuthResult
                    .pass()
                    .trafficId(username[0])
                    .userId(username[1])
                    .build());
            } else {
                return CompletableFuture.completedFuture(AuthResult.pass()
                    .trafficId("DevOnly").userId("DevUser_" + System.nanoTime()).build());
            }
        } else {
            return CompletableFuture.completedFuture(AuthResult.pass()
                .trafficId("DevOnly").userId("DevUser_" + System.nanoTime()).build());
        }
    }

    @Override
    public <A extends ActionInfo<?>> CompletableFuture<CheckResult> check(ClientInfo clientInfo, A actionInfo) {
        return CompletableFuture.completedFuture(CheckResult.ALLOW);
    }
}