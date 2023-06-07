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

import com.baidu.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import org.pf4j.Extension;

@Extension
public class AuthProviderTestStub implements IAuthProvider {
    public volatile CompletableFuture<AuthResult> nextAuthResult =
        CompletableFuture.completedFuture(AuthResult.NO_PASS);

    public volatile CompletableFuture<CheckResult> nextCheckResult =
        CompletableFuture.completedFuture(CheckResult.DISALLOW);

    @Override
    public <T extends AuthData<?>> CompletableFuture<AuthResult> auth(T authData) {
        return nextAuthResult;
    }

    @Override
    public <A extends ActionInfo<?>> CompletableFuture<CheckResult> check(ClientInfo clientInfo, A actionInfo) {
        return nextCheckResult;
    }
}
