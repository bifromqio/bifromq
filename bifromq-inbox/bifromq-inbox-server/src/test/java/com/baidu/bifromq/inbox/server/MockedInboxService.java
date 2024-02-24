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

package com.baidu.bifromq.inbox.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.IRPCMeter;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.server.scheduler.IInboxAttachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxCreateScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDeleteScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxDetachScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxGetScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import com.baidu.bifromq.inbox.util.PipelineUtil;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.client.IRetainClient;
import io.grpc.Context;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class MockedInboxService {
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected IBaseKVStoreClient inboxStoreClient;
    @Mock
    protected IInboxFetchScheduler fetchScheduler;
    @Mock
    protected IInboxGetScheduler getScheduler;
    @Mock
    protected IInboxInsertScheduler insertScheduler;
    @Mock
    protected IInboxCommitScheduler commitScheduler;
    @Mock
    protected IInboxTouchScheduler touchScheduler;
    @Mock
    protected IInboxCreateScheduler createScheduler;
    @Mock
    protected IInboxAttachScheduler attachScheduler;
    @Mock
    protected IInboxDetachScheduler detachScheduler;
    @Mock
    protected IInboxDeleteScheduler deleteScheduler;
    @Mock
    protected IInboxSubScheduler subScheduler;
    @Mock
    protected IInboxUnsubScheduler unsubScheduler;

    protected String tenantId = "testTenantId";
    protected String serviceName = "inboxService";
    protected String methodName = "testMethod";
    protected InboxService inboxService;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(inboxClient.id()).thenReturn(1);
        when(settingProvider.provide(any(), anyString())).thenAnswer(invocation -> {
            Setting setting = invocation.getArgument(0);
            return setting.current(invocation.getArgument(1));
        });
        Map<String, String> metaData = new HashMap<>();
        metaData.put(PipelineUtil.PIPELINE_ATTR_KEY_ID, "id");
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, new IRPCMeter.IRPCMethodMeter() {
                @Override
                public void recordCount(RPCMetric metric) {

                }

                @Override
                public void recordCount(RPCMetric metric, double inc) {

                }

                @Override
                public Timer timer(RPCMetric metric) {
                    return Timer.builder("dummy").register(new SimpleMeterRegistry());
                }

                @Override
                public void recordSummary(RPCMetric metric, int depth) {

                }
            })
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metaData)
            .attach();
        inboxService = InboxService.builder()
            .settingProvider(settingProvider)
            .inboxClient(inboxClient)
            .distClient(distClient)
            .retainClient(retainClient)
            .inboxStoreClient(inboxStoreClient)
            .getScheduler(getScheduler)
            .fetchScheduler(fetchScheduler)
            .insertScheduler(insertScheduler)
            .commitScheduler(commitScheduler)
            .createScheduler(createScheduler)
            .attachScheduler(attachScheduler)
            .detachScheduler(detachScheduler)
            .deleteScheduler(deleteScheduler)
            .subScheduler(subScheduler)
            .unsubScheduler(unsubScheduler)
            .touchScheduler(touchScheduler)
            .build();
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }
}
