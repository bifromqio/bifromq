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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.baserpc.proto.RPCServer;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Observable;
import java.util.Base64;
import java.util.Set;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPBrokerServerLandscapeHandlerTest
    extends AbstractHTTPRequestHandlerTest<GetBrokerServerLandscapeHandler> {
    @Mock
    private IRPCServiceTrafficGovernor trafficGovernor;
    @Mock
    private IMqttBrokerClient brokerClient;

    @Override
    protected Class<GetBrokerServerLandscapeHandler> handlerClass() {
        return GetBrokerServerLandscapeHandler.class;
    }

    @SneakyThrows
    @Test
    public void testHandle() {
        when(brokerClient.trafficGovernor()).thenReturn(trafficGovernor);
        RPCServer rpcServer = RPCServer.newBuilder()
            .setAgentHostId(ByteString.copyFromUtf8("agentHostId"))
            .setHost("host")
            .setPort(123)
            .putAttrs("key1", "value1")
            .putAttrs("key2", "value2")
            .addGroup("group1")
            .addGroup("group2")
            .build();
        when(trafficGovernor.serverList()).thenReturn(
            Observable.just(Set.of(new IRPCServiceTrafficDirector.Server(rpcServer))));

        GetBrokerServerLandscapeHandler handler = new GetBrokerServerLandscapeHandler(brokerClient);
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        FullHttpResponse resp = handler.handle(111, req).join();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.OK);
        assertEquals(resp.headers().get("Content-Type"), "application/json");

        ObjectMapper objectMapper = new ObjectMapper();
        String responseContent = resp.content().toString(io.netty.util.CharsetUtil.UTF_8);
        ArrayNode jsonResponse = (ArrayNode) objectMapper.readTree(responseContent);

        assertEquals(jsonResponse.size(), 1);
        ObjectNode serverNode = (ObjectNode) jsonResponse.get(0);

        assertEquals(serverNode.get("agentHostId").asText(),
            Base64.getEncoder().encodeToString(rpcServer.getAgentHostId().toByteArray()));
        assertEquals(serverNode.get("address").asText(), rpcServer.getHost());
        assertEquals(serverNode.get("port").asInt(), rpcServer.getPort());

        ObjectNode attributes = (ObjectNode) serverNode.get("attributes");
        assertEquals(attributes.get("key1").asText(), "value1");
        assertEquals(attributes.get("key2").asText(), "value2");

        ArrayNode groups = (ArrayNode) serverNode.get("groups");
        assertEquals(groups.size(), 2);
        assertEquals(Set.of(groups.get(0).asText(), groups.get(1).asText()), Set.of("group1", "group2"));
    }
}
