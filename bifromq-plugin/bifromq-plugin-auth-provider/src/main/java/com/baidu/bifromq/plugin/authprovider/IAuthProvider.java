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
import org.pf4j.ExtensionPoint;

public interface IAuthProvider extends ExtensionPoint {
    /**
     * Implement this method to hook authentication logic.
     * <p/>
     * Note: The authData will be reused by calling thread, make a clone if needed.
     *
     * @param authData the authentication data
     */
    <T extends AuthData<?>, R extends AuthResult> CompletableFuture<R> auth(T authData);

    /**
     * Implement this method to hook action permission check logic.
     * <p/>
     * Note: The actionInfo will be reused by calling thread, make a clone if needed.
     *
     * @param clientInfo the client info
     * @param actionInfo the action to authorize
     */
    <A extends ActionInfo<?>, R extends CheckResult> CompletableFuture<R> check(ClientInfo clientInfo, A actionInfo);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {
    }
}
