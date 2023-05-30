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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.plugin.authprovider.action.PubAction;
import com.baidu.bifromq.plugin.authprovider.action.SubAction;
import com.baidu.bifromq.plugin.authprovider.authdata.MQTTBasicAuth;
import com.baidu.bifromq.plugin.authprovider.authdata.MQTTVer;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pf4j.DefaultPluginManager;
import org.pf4j.PluginManager;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class AuthProviderManagerTest {
    @Mock
    private IAuthProvider mockProvider;

    private ClientInfo clientInfo = ClientInfo.getDefaultInstance();
    private AuthData mockAuthData = new MQTTBasicAuth().protocol(MQTTVer.MQTT3_1_1);
    private AuthData authData = new MQTTBasicAuth().protocol(MQTTVer.MQTT3_1_1);

    private PubAction mockActionInfo = new PubAction();
    private AuthProviderManager manager;
    private PluginManager pluginManager;

    @Before
    public void setup() {
        pluginManager = new DefaultPluginManager();
        pluginManager.loadPlugins();
        pluginManager.startPlugins();
    }

    @After
    public void teardown() {
        pluginManager.stopPlugins();
        pluginManager.unloadPlugins();
    }

    @Test
    public void devOnlyMode() {
        manager = new AuthProviderManager(null, pluginManager);
        AuthResult result = manager.auth(authData).join();
        assertEquals(AuthResult.Type.PASS, result.type());
        assertEquals("DevOnly", ((AuthResult.Pass) result).trafficId);

        CheckResult checkResult = manager.check(ClientInfo.getDefaultInstance(), new SubAction()).join();
        assertEquals(CheckResult.Type.ALLOW, checkResult.type());
        checkResult = manager.check(ClientInfo.getDefaultInstance(), new SubAction()).join();
        assertEquals(CheckResult.Type.ALLOW, checkResult.type());
        manager.close();
    }

    @Test
    public void authPluginSpecified() {
        manager = new AuthProviderManager(AuthProviderTestStub.class.getName(), pluginManager);
        AuthProviderTestStub stub = (AuthProviderTestStub) manager.get();
        stub.nextAuthResult = CompletableFuture.completedFuture(AuthResult.NO_PASS);
        AuthResult result = manager.auth(authData).join();
        assertEquals(AuthResult.Type.NO_PASS, result.type());
        manager.close();
    }

    @Test
    public void authPluginNotFound() {
        try {
            manager = new AuthProviderManager("Fake", pluginManager);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void authDelayedOK() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return AuthResult.NO_PASS;
            }));
        AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(AuthResult.Type.NO_PASS, result.type());
        manager.close();
    }

    @Test
    public void authReturnError() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.auth(any(AuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(AuthResult.Type.ERROR, result.type());
        assertTrue(((AuthResult.Error) result).cause.getMessage().equals("Intend Error"));
        manager.close();
    }

    @Test
    public void authTimeout() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.auth(any(AuthData.class))).thenReturn(new CompletableFuture<>());
        AuthResult result = manager.auth(mockAuthData).join();
        assertEquals(AuthResult.Type.ERROR, result.type());
        assertTrue(((AuthResult.Error) result).cause instanceof TimeoutException);
        manager.close();
    }


    @Test
    public void authThrottled() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.auth(any(AuthData.class))).thenReturn(new CompletableFuture<>());
        List<CompletableFuture<AuthResult>> results = new ArrayList<>();
        await().pollDelay(Duration.ofMillis(5)).until(() -> {
            results.add(manager.auth(new MQTTBasicAuth().protocol(MQTTVer.MQTT3_1_1).username("User_" +
                ThreadLocalRandom.current().nextInt())));
            return results.stream().anyMatch(result -> result.isDone() && AuthResult.Type.ERROR == result.join().type()
                && ((AuthResult.Error) result.join()).cause instanceof ThrottledException);
        });
        manager.close();
    }

    @Test
    public void checkReturnOk() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.ALLOW));
        CheckResult result = manager.check(clientInfo, mockActionInfo).join();
        assertEquals(CheckResult.Type.ALLOW, result.type());
        manager.close();
    }

    @Test
    public void checkReturnDelayedOk() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return CheckResult.ALLOW;
            }));

        CheckResult result = manager.check(clientInfo, mockActionInfo).join();
        assertEquals(CheckResult.Type.ALLOW, result.type());
        manager.close();
    }


    @Test
    public void checkReturnDelayedOkTest() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenAnswer((Answer<CompletableFuture<CheckResult>>) invocationOnMock ->
                CompletableFuture.supplyAsync(() -> {
                    log.info("Checking");
                    return CheckResult.ALLOW;
                }));
        int i = 0;
        while (i++ < 100) {
            CheckResult result =
                manager.check(ClientInfo.newBuilder().setUserId("abc" + i).build(), new PubAction()).join();
            assertEquals(CheckResult.Type.ALLOW, result.type());
        }
        manager.close();
    }


    @Test
    public void checkReturnError() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.error(new RuntimeException("Intend Error"))));
        CheckResult result = manager.check(clientInfo, mockActionInfo).join();
        assertEquals(CheckResult.Type.ERROR, result.type());
        assertTrue(((CheckResult.Error) result).cause.getMessage().equals("Intend Error"));
        manager.close();
    }

    @Test
    public void checkTimeout() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class))).thenReturn(new CompletableFuture<>());
        CheckResult result = manager.check(clientInfo, mockActionInfo).join();
        assertEquals(CheckResult.Type.ERROR, result.type());
        assertTrue(((CheckResult.Error) result).cause instanceof TimeoutException);
        manager.close();
    }

    @Test
    public void checkThrottled() {
        manager = new AuthProviderManager(mockProvider);
        when(mockProvider.check(any(ClientInfo.class), any(ActionInfo.class))).thenReturn(new CompletableFuture<>());
        List<CompletableFuture<CheckResult>> results = new ArrayList<>();
        await().pollDelay(Duration.ofMillis(5)).until(() -> {
            results.add(
                manager.check(clientInfo, new PubAction().topic("Topic_" + ThreadLocalRandom.current().nextInt())));
            return results.stream().anyMatch(result -> result.isDone() && CheckResult.Type.ERROR == result.join().type()
                && ((CheckResult.Error) result.join()).cause instanceof ThrottledException);
        });
        manager.close();
    }

    @Test
    public void stop() {
        manager = new AuthProviderManager(AuthProviderTestStub.class.getName(), pluginManager);
        manager.close();
        try {
            manager.auth(authData);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }
}
