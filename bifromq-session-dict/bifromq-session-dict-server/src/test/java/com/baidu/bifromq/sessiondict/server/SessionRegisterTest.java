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

package com.baidu.bifromq.sessiondict.server;

import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static com.baidu.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Collections;
import java.util.UUID;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegisterTest {
    private AutoCloseable closeable;
    private final String tenantId = "tenantId";
    private final String userId = "userId";
    private final String clientId = "clientId";
    private final ClientInfo owner = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, userId)
        .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
        .putMetadata(MQTT_CHANNEL_ID_KEY, UUID.randomUUID().toString())
        .build();

    private final ClientInfo owner2 = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, userId)
        .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
        .putMetadata(MQTT_CHANNEL_ID_KEY, UUID.randomUUID().toString())
        .build();
    private final ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("killer").build();

    @Mock
    private ISessionRegister.IRegistrationListener listener;

    @Mock
    private ServerCallStreamObserver<Quit> responseObserver;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void reg() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> keepCaptor = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<ISessionRegister> registerCaptor = ArgumentCaptor.forClass(ISessionRegister.class);
            verify(listener).on(ownerCaptor.capture(), keepCaptor.capture(), registerCaptor.capture());
            assertEquals(ownerCaptor.getValue(), owner);
            assertTrue(keepCaptor.getValue());
            assertEquals(registerCaptor.getValue(), register);
        });
    }

    @Test
    public void unreg() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(false)
                .build());
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> keepCaptor = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<ISessionRegister> registerCaptor = ArgumentCaptor.forClass(ISessionRegister.class);
            verify(listener, times(2))
                .on(ownerCaptor.capture(), keepCaptor.capture(), registerCaptor.capture());
            assertEquals(ownerCaptor.getAllValues().get(1), owner);
            assertFalse(keepCaptor.getAllValues().get(1));
            assertEquals(registerCaptor.getAllValues().get(1), register);
        });
    }

    @Test
    public void stop() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.stop();
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> keepCaptor = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<ISessionRegister> registerCaptor = ArgumentCaptor.forClass(ISessionRegister.class);
            verify(listener, times(2))
                .on(ownerCaptor.capture(), keepCaptor.capture(), registerCaptor.capture());
            assertEquals(ownerCaptor.getAllValues().get(1), owner);
            assertFalse(keepCaptor.getAllValues().get(1));
            assertEquals(registerCaptor.getAllValues().get(1), register);

            verify(responseObserver, times(0)).onNext(any());
        });
    }

    @Test
    public void localKick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner2)
                .setKeep(true)
                .build());
            ArgumentCaptor<Quit> quitCaptor = ArgumentCaptor.forClass(Quit.class);
            verify(responseObserver).onNext(quitCaptor.capture());
            Quit quit = quitCaptor.getValue();
            assertEquals(quit.getOwner(), owner);
            assertEquals(quit.getKiller(), owner2);

            // won't trigger second reg
            verify(listener, times(1)).on(any(), anyBoolean(), any());
        });
    }

    @Test
    public void kick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.kick(tenantId, new ISessionRegister.ClientKey(userId, clientId), killer);
            ArgumentCaptor<Quit> quitCaptor = ArgumentCaptor.forClass(Quit.class);
            verify(responseObserver).onNext(quitCaptor.capture());
            Quit quit = quitCaptor.getValue();
            assertEquals(quit.getOwner(), owner);
            assertEquals(quit.getKiller(), killer);
        });
    }

    @Test
    public void ignoreKick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.kick(tenantId, new ISessionRegister.ClientKey(userId, "FakeClientId"), killer);
            verify(responseObserver, times(0)).onNext(any());
        });
    }

    @Test
    public void ignoreSelfKick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.kick(tenantId,
                new ISessionRegister.ClientKey(userId, owner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, clientId)),
                owner);
            verify(responseObserver, times(0)).onNext(any());
        });
    }


    private void test(Runnable runnable) {
        Context ctx = Context.ROOT
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, Collections.emptyMap())
            .withValue(RPCContext.METER_KEY_CTX_KEY,
                RPCMeters.MeterKey.builder().service("SessionDict").method("dict").tenantId(tenantId).build());
        ctx.run(runnable);
    }
}
