/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.inbox.server.InboxWriterTest.matchInfo;
import static com.baidu.bifromq.inbox.server.InboxWriterTest.sendRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.baseenv.MemUsage;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.IRPCMeter;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.util.PipelineUtil;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryResult;
import com.baidu.bifromq.plugin.subbroker.DeliveryResults;
import com.baidu.bifromq.sysprops.props.IngressSlowDownDirectMemoryUsage;
import com.baidu.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Slf4j
public class InboxWriterPipelineTest {

    private AutoCloseable closeable;

    @Mock
    private MemUsage memUsage;

    @Mock
    private InboxWriter inboxWriter;

    @Mock
    private FetcherSignaler fetcherSignaler;

    @Mock
    private ServerCallStreamObserver<SendReply> responseObserver;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        setupContext();
    }

    private void setupContext() {
        Map<String, String> metaData = new HashMap<>();
        metaData.put(PipelineUtil.PIPELINE_ATTR_KEY_ID, "id");
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, createMockMeter())
            .withValue(RPCContext.TENANT_ID_CTX_KEY, "tenantId")
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metaData)
            .attach();
    }

    private IRPCMeter.IRPCMethodMeter createMockMeter() {
        return new IRPCMeter.IRPCMethodMeter() {
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
        };
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void handleRequestDeliveryOk() {
        testHandleRequest(DeliveryResult.Code.OK);
    }

    @Test
    public void handleRequestDeliveryError() {
        testHandleRequest(DeliveryResult.Code.ERROR);
    }

    private void testHandleRequest(DeliveryResult.Code code) {
        SendReply mockSendReply = createSendReply(code);
        when(inboxWriter.handle(any())).thenReturn(CompletableFuture.completedFuture(mockSendReply));
        doNothing().when(fetcherSignaler).afterWrite(any(), any());
        InboxWriterPipeline writerPipeline = new InboxWriterPipeline(fetcherSignaler, inboxWriter, responseObserver);
        SendReply sendReply = writerPipeline.handleRequest("_", sendRequest()).join();
        assertEquals(sendReply, mockSendReply);
    }

    private SendReply createSendReply(DeliveryResult.Code code) {
        return SendReply.newBuilder()
            .setReqId(1)
            .setReply(DeliveryReply.newBuilder()
                .putResult("tenantId", DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo())
                        .setCode(code).build()).build())
                .build())
            .build();
    }


    @Test
    public void testConstructorDirectMemoryUsageCatch() {
        when(memUsage.nettyDirectMemoryUsage())
            .thenReturn(IngressSlowDownDirectMemoryUsage.INSTANCE.get() + 0.1f);
        when(memUsage.heapMemoryUsage()).thenReturn(IngressSlowDownHeapMemoryUsage.INSTANCE.get() - 0.1f);

        testMemoryUsageThresholdExceed();
    }

    @Test
    public void testConstructorHeapMemoryUsageCatch() {
        when(memUsage.nettyDirectMemoryUsage())
            .thenReturn(IngressSlowDownDirectMemoryUsage.INSTANCE.get() - 0.1f);
        when(memUsage.heapMemoryUsage()).thenReturn(IngressSlowDownHeapMemoryUsage.INSTANCE.get() + 0.1f);
        testMemoryUsageThresholdExceed();
    }

    @Test
    public void testConstructorHeapMemoryUsageAllNotCatch() {
        when(memUsage.nettyDirectMemoryUsage())
            .thenReturn(IngressSlowDownDirectMemoryUsage.INSTANCE.get() - 0.1f);
        when(memUsage.heapMemoryUsage()).thenReturn(IngressSlowDownHeapMemoryUsage.INSTANCE.get() - 0.1f);
        testMemoryUsageThresholdExceed();
    }


    private void testMemoryUsageThresholdExceed() {
        when(inboxWriter.handle(any())).thenReturn(CompletableFuture.completedFuture(SendReply.getDefaultInstance()));
        doNothing().when(fetcherSignaler).afterWrite(any(), any());

        try (MockedStatic<MemUsage> mocked = Mockito.mockStatic(MemUsage.class)) {
            mocked.when(MemUsage::local).thenReturn(memUsage);
            SendRequest sendRequest = SendRequest.getDefaultInstance();
            InboxWriterPipeline writerPipeline =
                new InboxWriterPipeline(fetcherSignaler, inboxWriter, responseObserver);
            writerPipeline.onNext(sendRequest);
            SendReply sendReply = writerPipeline.handleRequest("_", sendRequest).join();
            assertEquals(sendReply, SendReply.getDefaultInstance());
        }
    }

}