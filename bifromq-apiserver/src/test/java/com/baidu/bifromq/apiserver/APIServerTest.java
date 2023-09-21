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

package com.baidu.bifromq.apiserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.sessiondict.client.ISessionDictClient;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class APIServerTest extends MockableTest {
    private final String host = "127.0.0.1";
    private APIServer apiServer;
    @Mock
    private IDistClient distClient;
    @Mock
    private IMqttBrokerClient mqttBrokerClient;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private ISessionDictClient sessionDictClient;
    @Mock
    private IRetainClient retainClient;
    private ISettingProvider settingProvider = Setting::current;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        super.setup();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        apiServer = new APIServer(host, 0, 0,
            bossGroup, workerGroup, null, distClient, mqttBrokerClient, inboxClient,
                sessionDictClient, retainClient, settingProvider);
        apiServer.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        super.teardown();
        apiServer.shutdown();
    }

    @Test(groups = "integration")
    @SneakyThrows
    public void pub() {
        HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        HttpRequest pubRequest = HttpRequest.newBuilder()
            .uri(URI.create(String.format("http://%s:%d/pub", host, apiServer.listeningPort())))
            .header(Headers.HEADER_TENANT_ID.header, "BifroMQ-Dev")
            .header(Headers.HEADER_TOPIC.header, "/greeting")
            .header(Headers.HEADER_CLIENT_TYPE.header, "BifroMQ Fan")
            .header(Headers.HEADER_RETAIN.header,"true" )
            .POST(HttpRequest.BodyPublishers.ofString("Hello BifroMQ"))
            .build();
        when(distClient.pub(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(null));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        HttpResponse<?> resp = httpClient.send(pubRequest, HttpResponse.BodyHandlers.discarding());
        assertEquals(resp.statusCode(), 200);
    }
}
