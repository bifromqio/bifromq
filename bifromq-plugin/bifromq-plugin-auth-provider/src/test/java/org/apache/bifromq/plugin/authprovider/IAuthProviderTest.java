/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.plugin.authprovider;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class IAuthProviderTest {
    @Test
    public void mqtt3RejectTenantAndUserIdArePreservedInMqtt5FailedResult() {
        IAuthProvider authProvider = new IAuthProvider() {
            @Override
            public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
                return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                    .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.NotAuthorized)
                        .setTenantId("tenant")
                        .setUserId("user")
                        .setReason("denied")
                        .build())
                    .build());
            }

            @Override
            public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
                return CompletableFuture.completedFuture(true);
            }
        };

        MQTT5AuthResult result = authProvider.auth(MQTT5AuthData.getDefaultInstance()).join();

        assertTrue(result.hasFailed());
        assertEquals(result.getFailed().getCode(), Failed.Code.NotAuthorized);
        assertTrue(result.getFailed().hasTenantId());
        assertEquals(result.getFailed().getTenantId(), "tenant");
        assertTrue(result.getFailed().hasUserId());
        assertEquals(result.getFailed().getUserId(), "user");
        assertEquals(result.getFailed().getReason(), "denied");
    }
}
