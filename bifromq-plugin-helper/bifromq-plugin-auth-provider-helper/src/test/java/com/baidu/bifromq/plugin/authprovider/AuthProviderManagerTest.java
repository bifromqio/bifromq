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

import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.PubAction;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.plugin.authprovider.type.SubAction;
import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pf4j.PluginManager;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class AuthProviderManagerTest {
    @Mock
    PluginManager pluginManager;
    @Mock
    private IAuthProvider mockProvider;
    @Mock
    private ISettingProvider settingProvider;
    @Mock
    private IEventCollector eventCollector;
    private ClientInfo clientInfo = ClientInfo.getDefaultInstance();
    private MQTT3AuthData mockAuthData = MQTT3AuthData.newBuilder().build();
    private MQTT3AuthData authData = MQTT3AuthData.newBuilder().build();
    private MQTTAction mockActionInfo = MQTTAction.newBuilder().setPub(PubAction.getDefaultInstance()).build();
    private AuthProviderManager manager;

    @Before
    public void setup() {
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(
            Collections.singletonList(mockProvider));
    }

    @Test
    public void devOnlyMode() {
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(Collections.emptyList());
        manager = new AuthProviderManager(null, pluginManager, settingProvider, eventCollector);
        MQTT3AuthResult result = manager.auth(authData).join();
        assertEquals(MQTT3AuthResult.TypeCase.OK, result.getTypeCase());
        assertEquals("DevOnly", result.getOk().getTrafficId());

        boolean allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        manager.close();
    }

    @Test
    public void authPluginSpecified() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(authData)).thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
            .setReject(Reject.newBuilder()
                .setCode(Reject.Code.BadPass)
                .build()).build()));
        MQTT3AuthResult result = manager.auth(authData).join();
        assertEquals(MQTT3AuthResult.TypeCase.REJECT, result.getTypeCase());
        assertEquals(Reject.Code.BadPass, result.getReject().getCode());
        manager.close();
    }

    @Test
    public void authPluginNotFound() {
        try {
            manager = new AuthProviderManager("Fake", pluginManager, settingProvider, eventCollector);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void authDelayedOK() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return MQTT3AuthResult.newBuilder()
                    .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.BadPass)
                        .build())
                    .build();
            }));
        MQTT3AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(MQTT3AuthResult.TypeCase.REJECT, result.getTypeCase());
        assertEquals(Reject.Code.BadPass, result.getReject().getCode());
        manager.close();
    }

    @Test
    public void authReturnError() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        MQTT3AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(MQTT3AuthResult.TypeCase.REJECT, result.getTypeCase());
        assertEquals(Reject.Code.Error, result.getReject().getCode());
        assertEquals("Intend Error", result.getReject().getReason());
        manager.close();
    }

    @Test
    public void authTimeout() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class))).thenReturn(new CompletableFuture<>());
        MQTT3AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(MQTT3AuthResult.TypeCase.REJECT, result.getTypeCase());
        assertEquals(Reject.Code.Error, result.getReject().getCode());
        assertEquals("java.util.concurrent.TimeoutException", result.getReject().getReason());
        manager.close();
    }


    @Test
    public void authThrottled() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class))).thenReturn(new CompletableFuture<>());
        List<CompletableFuture<MQTT3AuthResult>> results = new ArrayList<>();
        await().pollDelay(Duration.ofMillis(5)).until(() -> {
            results.add(manager.auth(
                MQTT3AuthData.newBuilder().setUsername("User_" + ThreadLocalRandom.current().nextInt()).build()));
            return results.stream()
                .anyMatch(result -> result.isDone() && MQTT3AuthResult.TypeCase.REJECT == result.join().getTypeCase()
                    && result.join().getReject().getReason().contains("throttled"));
        });
        manager.close();
    }

    @Test
    public void checkReturnOk() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(true));
        boolean result = manager.check(clientInfo, mockActionInfo).join();
        assertTrue(result);
        manager.close();
    }

    @Test
    public void checkReturnDelayedOk() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            }));

        boolean result = manager.check(clientInfo, mockActionInfo).join();
        assertTrue(result);
        manager.close();
    }


    @Test
    public void checkReturnDelayedOkTest() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenAnswer((Answer<CompletableFuture<Boolean>>) invocationOnMock ->
                CompletableFuture.supplyAsync(() -> {
                    log.info("Checking");
                    return true;
                }));
        int i = 0;
        while (i++ < 100) {
            boolean result =
                manager.check(ClientInfo.newBuilder().setUserId("abc" + i).build(), MQTTAction.newBuilder()
                    .setPub(PubAction.getDefaultInstance())
                    .build()).join();
            assertTrue(result);
        }
        manager.close();
    }


    @Test
    public void checkReturnErrorAndNoPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo)).thenReturn(false);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertFalse(manager.check(clientInfo, mockActionInfo).join());
        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(EventType.ACCESS_CONTROL_ERROR, eventArgumentCaptor.getValue().type());
        assertTrue(((AccessControlError) eventArgumentCaptor.getValue()).cause().getMessage().contains("Intend Error"));
    }

    @Test
    public void checkReturnErrorAndPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo)).thenReturn(true);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertTrue(manager.check(clientInfo, mockActionInfo).join());
        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(EventType.ACCESS_CONTROL_ERROR, eventArgumentCaptor.getValue().type());
    }

    @Test
    public void checkTimeoutAndPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo)).thenReturn(true);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(new CompletableFuture<>());
        assertTrue(manager.check(clientInfo, mockActionInfo).join());
        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
    }

    @Test
    public void checkTimeoutAndNoPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo)).thenReturn(false);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(new CompletableFuture<>());
        assertFalse(manager.check(clientInfo, mockActionInfo).join());

        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(EventType.ACCESS_CONTROL_ERROR, eventArgumentCaptor.getValue().type());
        assertTrue(((AccessControlError) eventArgumentCaptor.getValue()).cause() instanceof TimeoutException);
    }

    @Test
    public void checkThrottled() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class))).thenReturn(new CompletableFuture<>());
        List<CompletableFuture<Boolean>> results = new ArrayList<>();
        await().pollDelay(Duration.ofMillis(5)).until(() -> {
            results.add(
                manager.check(clientInfo, MQTTAction.newBuilder()
                    .setPub(PubAction.newBuilder()
                        .setTopic("Topic_" + ThreadLocalRandom.current().nextInt())
                        .build())
                    .build()));
            return results.stream().anyMatch(CompletableFuture::isCompletedExceptionally);
        });
        manager.close();
    }

    @Test
    public void stop() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        manager.close();
        try {
            manager.auth(authData);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    protected void verifyEvent(int count, EventType... types) {
    }
}
