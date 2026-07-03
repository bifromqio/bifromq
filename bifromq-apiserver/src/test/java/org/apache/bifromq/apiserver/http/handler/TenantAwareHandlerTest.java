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

package org.apache.bifromq.apiserver.http.handler;

import static org.apache.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.testng.annotations.Test;

public class TenantAwareHandlerTest {
    @Test
    public void handleDoesNotRetainRequest() {
        String tenantId = "tenantId";
        ISettingProvider settingProvider = mock(ISettingProvider.class);
        when(settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId)).thenReturn(1024);
        TenantAwareHandler handler = new TenantAwareHandler(settingProvider) {
            @Override
            protected CompletableFuture<FullHttpResponse> handle(long reqId,
                                                                 String tenantId,
                                                                 FullHttpRequest req) {
                return CompletableFuture.completedFuture(
                    new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.OK));
            }
        };
        FullHttpRequest request =
            new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/pub");
        request.headers().set(HEADER_TENANT_ID.header, tenantId);
        int initialRefCnt = request.refCnt();
        FullHttpResponse response = handler.handle(1, request).join();

        try {
            assertEquals(request.refCnt(), initialRefCnt);
        } finally {
            response.release();
            while (request.refCnt() > 0) {
                request.release();
            }
        }
    }
}
