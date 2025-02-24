package com.baidu.bifromq.mqtt.inbox;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.baserpc.exception.ServerNotFoundException;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.OnlineInboxBrokerGrpc;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import com.baidu.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import com.baidu.bifromq.mqtt.inbox.util.DeliveryGroupKeyUtil;
import com.baidu.bifromq.plugin.subbroker.*;
import com.baidu.bifromq.type.MatchInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class MqttBrokerClientTest {
    private AutoCloseable closeable;
    @Mock
    private IRPCClient rpcClient;
    private MqttBrokerClient mqttBrokerClient;
    @Mock
    private IDeliverer deliverer;
    @Mock
    private IRPCClient.IRequestPipeline<WriteRequest, WriteReply> pipeline;
    private final String delivererKey = "test:DelivererKey";
    private final String tenantId = "testTenantId";
    private final MatchInfo matchInfo = MatchInfo.newBuilder()
            .setTopicFilter("testTopicFilter")
            .setReceiverId("testReceiverId")
            .build();
    private final DeliveryRequest request = DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                    .addPack(DeliveryPack.newBuilder()
                            .addMatchInfo(matchInfo)
                            .addMatchInfo(matchInfo).build())
                    .build())
            .build();

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        mqttBrokerClient = new MqttBrokerClient(rpcClient);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void  deliveryPipelineWithOK() {
        CompletableFuture<WriteReply> future = new CompletableFuture<>();
        future.complete(WriteReply.newBuilder()
                        .setReply(DeliveryReply.newBuilder()
                                .putResult(tenantId, DeliveryResults.newBuilder()
                                        .addResult(DeliveryResult.newBuilder()
                                                .setCode(DeliveryResult.Code.OK)
                                                .setMatchInfo(matchInfo)
                                                .build())
                                        .build())
                                .build())
                .build());

        when(rpcClient.createRequestPipeline("", DeliveryGroupKeyUtil.parseServerId(delivererKey), "",
                emptyMap(), OnlineInboxBrokerGrpc.getWriteMethod())).thenReturn(pipeline);
        when(pipeline.invoke(any())).thenReturn(future);

        deliverer = mqttBrokerClient.open(delivererKey);
        DeliveryReply reply = deliverer.deliver(request).join();
        DeliveryResults results = reply.getResultMap().get(tenantId);
        assert  results != null;
        DeliveryResult result = results.getResult(0);
        assert result.getCode() == DeliveryResult.Code.OK;
        assert result.getMatchInfo().equals(matchInfo);
    }

    @Test
    public void deliveryPipelineWithSpecialException() {
        CompletableFuture<WriteReply> future = new CompletableFuture<>();
        future.completeExceptionally(new ServerNotFoundException("Test exception"));

        when(rpcClient.createRequestPipeline("", DeliveryGroupKeyUtil.parseServerId(delivererKey), "",
                emptyMap(), OnlineInboxBrokerGrpc.getWriteMethod())).thenReturn(pipeline);
        when(pipeline.invoke(any())).thenReturn(future);

        deliverer = mqttBrokerClient.open(delivererKey);
        DeliveryReply reply = deliverer.deliver(request).join();
        DeliveryResults results = reply.getResultMap().get(tenantId);
        assert  results != null;
        assert results.getResultList().size() == 1;
        DeliveryResult result = results.getResult(0);
        assert result.getCode() == DeliveryResult.Code.NO_SUB;
        assert result.getMatchInfo().equals(matchInfo);
    }
}
