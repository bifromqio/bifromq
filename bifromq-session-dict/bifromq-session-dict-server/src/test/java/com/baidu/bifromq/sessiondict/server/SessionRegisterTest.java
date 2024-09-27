/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.IRPCMeter;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.baidu.bifromq.sessiondict.rpc.proto.Quit;
import com.baidu.bifromq.sessiondict.rpc.proto.ServerRedirection;
import com.baidu.bifromq.sessiondict.rpc.proto.Session;
import com.baidu.bifromq.type.ClientInfo;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Timer;
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
    public void tearDown() {
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
            verify(listener, times(1)).on(eq(owner), eq(true), eq(register));
            reset(listener);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner2)
                .setKeep(true)
                .build());
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> regFlagCaptor = ArgumentCaptor.forClass(Boolean.class);
            verify(listener, times(2)).on(ownerCaptor.capture(), regFlagCaptor.capture(), eq(register));
            assertEquals(ownerCaptor.getAllValues().get(0), owner);
            assertEquals(ownerCaptor.getAllValues().get(1), owner2);
            assertFalse(regFlagCaptor.getAllValues().get(0));
            assertTrue(regFlagCaptor.getAllValues().get(1));

            ArgumentCaptor<Quit> quitCaptor = ArgumentCaptor.forClass(Quit.class);
            verify(responseObserver).onNext(quitCaptor.capture());
            Quit quit = quitCaptor.getValue();
            assertEquals(quit.getOwner(), owner);
            assertEquals(quit.getKiller(), owner2);
        });
    }

    @Test
    public void ignoreSelfLocalKick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            verify(listener, times(1)).on(eq(owner), eq(true), eq(register));
            reset(listener);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            verify(listener, never()).on(any(), anyBoolean(), any());
            verify(responseObserver, never()).onNext(any());
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
            verify(listener).on(eq(owner), eq(true), eq(register));
            reset(listener);
            assertTrue(register.kick(tenantId, new ISessionRegister.ClientKey(userId, clientId), killer,
                ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));
            ArgumentCaptor<Quit> quitCaptor = ArgumentCaptor.forClass(Quit.class);
            verify(responseObserver).onNext(quitCaptor.capture());
            verify(listener).on(eq(owner), eq(false), eq(register));
            Quit quit = quitCaptor.getValue();
            assertEquals(quit.getOwner(), owner);
            assertEquals(quit.getKiller(), killer);
            assertEquals(quit.getServerRedirection().getType(), ServerRedirection.Type.NO_MOVE);
        });
    }

    @Test
    public void serverRedirect() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            verify(listener).on(eq(owner), eq(true), eq(register));
            reset(listener);
            ServerRedirection serverRedirection = ServerRedirection.newBuilder()
                .setType(ServerRedirection.Type.PERMANENT_MOVE)
                .setServerReference("serverId")
                .build();
            assertTrue(
                register.kick(tenantId, new ISessionRegister.ClientKey(userId, clientId), killer, serverRedirection));
            ArgumentCaptor<Quit> quitCaptor = ArgumentCaptor.forClass(Quit.class);
            verify(responseObserver).onNext(quitCaptor.capture());
            verify(listener).on(eq(owner), eq(false), eq(register));
            Quit quit = quitCaptor.getValue();
            assertEquals(quit.getOwner(), owner);
            assertEquals(quit.getKiller(), killer);
            assertEquals(serverRedirection, quit.getServerRedirection());
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
            assertFalse(register.kick(tenantId, new ISessionRegister.ClientKey(userId, "FakeClientId"), killer,
                ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));
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
            assertFalse(register.kick(tenantId,
                new ISessionRegister.ClientKey(userId, owner.getMetadataOrDefault(MQTT_CLIENT_ID_KEY, clientId)),
                owner, ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build()));
            verify(responseObserver, times(0)).onNext(any());
        });
    }

    private void test(Runnable runnable) {
        Context ctx = Context.ROOT
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, Collections.emptyMap())
            .withValue(RPCContext.METER_KEY_CTX_KEY, new IRPCMeter.IRPCMethodMeter() {
                @Override
                public void recordCount(RPCMetric metric) {

                }

                @Override
                public void recordCount(RPCMetric metric, double inc) {

                }

                @Override
                public Timer timer(RPCMetric metric) {
                    return null;
                }

                @Override
                public void recordSummary(RPCMetric metric, int depth) {

                }
            });
        ctx.run(runnable);
    }
}
