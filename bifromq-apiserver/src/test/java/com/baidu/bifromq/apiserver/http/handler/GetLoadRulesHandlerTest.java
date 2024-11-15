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

package com.baidu.bifromq.apiserver.http.handler;

import static com.baidu.bifromq.apiserver.Headers.HEADER_SERVICE_NAME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import com.google.protobuf.Struct;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetLoadRulesHandlerTest extends AbstractHTTPRequestHandlerTest<GetLoadRulesHandler> {

    @Mock
    private IBaseKVClusterMetadataManager metadataManager;
    private Subject<Map<String, Struct>> mockLoadRulesSubject = BehaviorSubject.create();
    private Subject<Set<String>> mockClusterIdSubject = BehaviorSubject.create();

    @BeforeMethod
    public void setup() {
        super.setup();
        when(metaService.clusterIds()).thenReturn(mockClusterIdSubject);
    }

    @Override
    protected Class<GetLoadRulesHandler> handlerClass() {
        return GetLoadRulesHandler.class;
    }

    @Test
    public void noClusterFound() {
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set(HEADER_SERVICE_NAME.header, "fakeUserId");
        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void clusterChanged() {
        String clusterId = "dist.worker";
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set(HEADER_SERVICE_NAME.header, clusterId);
        when(metaService.metadataManager(eq(clusterId))).thenReturn(metadataManager);
        when(metadataManager.loadRules()).thenReturn(mockLoadRulesSubject);

        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);

        mockClusterIdSubject.onNext(Set.of(clusterId));
        resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.OK);

        mockClusterIdSubject.onNext(Collections.emptySet());
        resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void loadRules() {
        String clusterId = "dist.worker";
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        Map<String, Struct> loadRules = Map.of("balancerClass", Struct.getDefaultInstance());
        req.headers().set(HEADER_SERVICE_NAME.header, clusterId);
        when(metaService.metadataManager(eq(clusterId))).thenReturn(metadataManager);
        when(metadataManager.loadRules()).thenReturn(mockLoadRulesSubject);
        mockClusterIdSubject.onNext(Set.of(clusterId));
        mockLoadRulesSubject.onNext(loadRules);
        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.OK);
    }
}
