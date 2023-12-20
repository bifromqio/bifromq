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

package com.baidu.bifromq.plugin.authprovider;

import static com.baidu.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
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
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        clientInfo = ClientInfo.getDefaultInstance();
        mockAuthData = MQTT3AuthData.newBuilder().build();
        authData = MQTT3AuthData.newBuilder().build();
        mockActionInfo = MQTTAction.newBuilder().setPub(PubAction.getDefaultInstance()).build();
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(
            Collections.singletonList(mockProvider));
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void devOnlyMode() {
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(Collections.emptyList());
        manager = new AuthProviderManager(null, pluginManager, settingProvider, eventCollector);
        MQTT3AuthResult result = manager.auth(authData).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.OK);
        assertEquals(result.getOk().getTenantId(), "DevOnly");

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
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.BadPass);
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
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.BadPass);
        manager.close();
    }

    @Test
    public void authReturnError() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        MQTT3AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.Error);
        assertEquals(result.getReject().getReason(), "Intend Error");
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
                manager.check(ClientInfo.newBuilder()
                        .putMetadata("userId", "abc" + i)
                        .build(),
                    MQTTAction.newBuilder()
                        .setPub(PubAction.getDefaultInstance())
                        .build()).join();
            assertTrue(result);
        }
        manager.close();
    }


    @Test
    public void checkReturnErrorAndNoPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo.getTenantId())).thenReturn(false);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertFalse(manager.check(clientInfo, mockActionInfo).join());
        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(eventArgumentCaptor.getValue().type(), EventType.ACCESS_CONTROL_ERROR);
        assertTrue(((AccessControlError) eventArgumentCaptor.getValue()).cause().getMessage().contains("Intend Error"));
    }

    @Test
    public void checkReturnErrorAndPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo.getTenantId())).thenReturn(true);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.check(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertTrue(manager.check(clientInfo, mockActionInfo).join());
        ArgumentCaptor<Event<?>> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(eventArgumentCaptor.getValue().type(), EventType.ACCESS_CONTROL_ERROR);
    }
}
