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

package com.baidu.bifromq.basecluster.transport;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.reactivex.rxjava3.observers.TestObserver;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mafei01 in 2020-04-22 16:53
 */
@Slf4j
public class TCPTransportFuncTest {

    InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 11111);
    InetSocketAddress address2 = new InetSocketAddress("127.0.0.1", 22222);
    InetSocketAddress address3 = new InetSocketAddress("127.0.0.1", 33333);
    InetSocketAddress fakeAddr = new InetSocketAddress("127.0.0.1", 54321);
    TCPTransport transport1;
    TCPTransport transport2;

    @After
    public void shutdown() {
        if (transport1 != null) {
            transport1.shutdown();
        }
        if (transport2 != null) {
            transport2.shutdown();
        }
    }

    @Test
    public void testSendAndReceive() {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        List<ByteString> data = Arrays.asList(copyFromUtf8("test"));
        transport1.receive().subscribe(t -> Assert.assertEquals(t.data, data));
        transport1.send(data, address1).join();
    }

    @Test
    public void testConnectFail() {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        List<ByteString> data = Arrays.asList(copyFromUtf8("test"));
        transport1.receive().subscribe(t -> Assert.assertEquals(t.data, data));
        try {
            transport1.send(data, fakeAddr).join();
            fail();
        } catch (Exception e) {

        }
    }

    @Test
    public void testConnectionInActive() {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        transport2 = TCPTransport.builder()
            .bindAddr(address2)
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        List<ByteString> data = Arrays.asList(copyFromUtf8("test"));
        transport1.send(data, address2).join();
        // stop transport2
        log.info("Stop transport2");
        transport2.shutdown().exceptionally(e -> null).join();
        // restart transport2
        log.info("Restart transport2");
        transport2 = TCPTransport.builder()
            .bindAddr(address2)
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        transport1.send(data, address2).join();

        transport1.shutdown().join();
        transport2.shutdown().join();
    }

    @Test
    public void testSendAndReceiveViaTls() {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .serverSslContext(buildServerAuthSslContext())
            .clientSslContext(buildClientAuthSslContext())
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        List<ByteString> data = Arrays.asList(copyFromUtf8("test"));
        transport1.receive().subscribe(t -> Assert.assertEquals(t.data, data));
        transport1.send(data, address1).join();
    }

    @Test
    public void testGetChannel() throws ExecutionException, InterruptedException {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .serverSslContext(buildServerAuthSslContext())
            .clientSslContext(buildClientAuthSslContext())
            .opts(new TCPTransport.TCPTransportOptions().maxChannelsPerHost(2))
            .build();
        CompletableFuture<Channel> cf1 = Executors.newSingleThreadScheduledExecutor()
            .schedule(() -> transport1.getChannel(address1), 0, TimeUnit.MILLISECONDS).get();

        CompletableFuture<Channel> cf2 = Executors.newSingleThreadScheduledExecutor()
            .schedule(() -> transport1.getChannel(address1), 0, TimeUnit.MILLISECONDS).get();
        assertNotEquals(cf1.get(), cf2.get());
    }

    @Test
    public void testSharedToken() {
        transport1 = TCPTransport.builder()
            .bindAddr(address1)
            .serverSslContext(buildServerAuthSslContext())
            .clientSslContext(buildClientAuthSslContext())
            .sharedToken("token1")
            .opts(new TCPTransport.TCPTransportOptions().maxChannelsPerHost(2))
            .build();
        transport2 = TCPTransport.builder()
            .bindAddr(address2)
            .serverSslContext(buildServerAuthSslContext())
            .clientSslContext(buildClientAuthSslContext())
            .sharedToken("token1")
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        ITransport transport3 = TCPTransport.builder()
            .bindAddr(address3)
            .serverSslContext(buildServerAuthSslContext())
            .clientSslContext(buildClientAuthSslContext())
            .sharedToken("token2")
            .opts(new TCPTransport.TCPTransportOptions())
            .build();

        List<ByteString> data = Arrays.asList(copyFromUtf8("test"));

        transport1.send(data, address2);
        Assert.assertEquals(transport2.receive().blockingFirst().data, data);

        TestObserver<PacketEnvelope> result = TestObserver.create();
        transport2.receive().subscribeWith(result);
        transport1.send(data, address3).join();
        assertTrue(result.values().isEmpty());
        transport3.shutdown().join();
    }

    public static SslContext buildServerAuthSslContext() {
        try {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(
                    TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("test.crt"),
                    TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("test.pem"))
                .trustManager(TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("ca.crt"))
                .clientAuth(ClientAuth.REQUIRE)
                .sslProvider(SslProvider.OPENSSL);
            return sslContextBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize shared server SSLContext", e);
        }
    }

    public static SslContext buildClientAuthSslContext() {
        try {
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient()
                .trustManager(TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("ca.crt"))
                .keyManager(
                    TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("test.crt"),
                    TCPTransportFuncTest.class.getClassLoader().getResourceAsStream("test.pem"))
                .sslProvider(SslProvider.OPENSSL);
            return sslContextBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize shared server SSLContext", e);
        }
    }
}
