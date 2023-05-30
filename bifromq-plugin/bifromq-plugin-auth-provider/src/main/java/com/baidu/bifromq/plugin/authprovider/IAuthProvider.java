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
     * Note: To reduce memory pressure, the passed authData object is shared in the calling thread, and will be reused
     * afterwards. Make a clone if implementation's logic needs to pass it to another thread for processing.
     *
     * @param authData the authentication data
     */
    <T extends AuthData, R extends AuthResult> CompletableFuture<R> auth(T authData);

    /**
     * Implement this method to hook action permission check logic.
     * <p/>
     * Note: The clientInfo object is immutable. To reduce memory pressure, the passed actionInfo object is shared in
     * the calling thread, and will be reused afterwards. Make a clone if implementation's logic needs to pass it to
     * another thread for processing.
     *
     * @param clientInfo the authentication data
     * @param actionInfo
     */
    <A extends ActionInfo, R extends CheckResult> CompletableFuture<R> check(ClientInfo clientInfo, A actionInfo);

    /**
     * This method will be called during broker shutdown
     */
    default void close() {
    }
}
