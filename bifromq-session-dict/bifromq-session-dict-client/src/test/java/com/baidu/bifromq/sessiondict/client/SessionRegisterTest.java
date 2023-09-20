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

package com.baidu.bifromq.sessiondict.client;

import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.type.ClientInfo;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegisterTest {
    private AutoCloseable closeable;
    @Mock
    private Consumer<ClientInfo> onKick;
    @Mock
    private IRPCClient.IMessageStream<Quit, Session> messageStream;
    private SessionRegPipeline regPipeline;
    private final String tenantId = "tenantId";
    private final String userId = "userId";
    private final String clientId = "clientId";
    private final ClientInfo owner = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, userId)
        .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
        .build();
    private final AtomicReference<Subject<Quit>> quitSubject = new AtomicReference<>();

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(messageStream.msg()).thenAnswer((Answer<Observable<Quit>>) invocation -> {
            quitSubject.set(PublishSubject.create());
            return quitSubject.get();
        });
        regPipeline = new SessionRegPipeline(messageStream);
    }

    @AfterMethod
    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void regAfterInit() {
        new SessionRegister(owner, onKick, regPipeline);
        ArgumentCaptor<Session> argCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream).ack(argCaptor.capture());
        assertEquals(argCaptor.getValue().getOwner(), owner);
        assertTrue(argCaptor.getValue().getKeep());
    }

    @Test
    public void reRegAfterError() {
        new SessionRegister(owner, onKick, regPipeline);
        quitSubject.get().onError(new RuntimeException("Test Exception"));
        ArgumentCaptor<Session> argCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream, times(2)).ack(argCaptor.capture());
        assertEquals(argCaptor.getValue().getOwner(), owner);
        assertTrue(argCaptor.getValue().getKeep());
    }

    @Test
    public void reRegAfterComplete() {
        new SessionRegister(owner, onKick, regPipeline);
        quitSubject.get().onComplete();
        ArgumentCaptor<Session> argCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream, times(2)).ack(argCaptor.capture());
        assertEquals(argCaptor.getValue().getOwner(), owner);
        assertTrue(argCaptor.getValue().getKeep());
    }

    @Test
    public void quitAndStop() {
        new SessionRegister(owner, onKick, regPipeline);
        Quit quit = Quit.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner)
            .setKiller(ClientInfo.newBuilder().setTenantId(tenantId).setType("MockKiller").build())
            .build();
        quitSubject.get().onNext(quit);
        ArgumentCaptor<ClientInfo> killerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
        verify(onKick).accept(killerCaptor.capture());
        assertEquals(killerCaptor.getValue(), quit.getKiller());
    }

    @Test
    public void ignoreQuit() {
        new SessionRegister(owner, onKick, regPipeline);
        Quit quit = Quit.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner.toBuilder().putMetadata(MQTT_CLIENT_ID_KEY, "FakeClient").build())
            .setKiller(ClientInfo.newBuilder().setTenantId(tenantId).setType("MockKiller").build())
            .build();
        quitSubject.get().onNext(quit);
        verify(onKick, times(0)).accept(any());
        verify(messageStream, times(1)).ack(any());
    }

    @Test
    public void stop() {
        SessionRegister register = new SessionRegister(owner, onKick, regPipeline);
        register.stop();
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream, times(2)).ack(sessionCaptor.capture());
        Session session = sessionCaptor.getAllValues().get(1);
        assertEquals(session.getOwner(), owner);
        assertFalse(session.getKeep());
    }
}
