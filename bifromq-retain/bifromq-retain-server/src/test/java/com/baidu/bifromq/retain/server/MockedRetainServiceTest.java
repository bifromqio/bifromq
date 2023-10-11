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

package com.baidu.bifromq.retain.server;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IMutationPipeline;
import com.baidu.bifromq.basekv.client.IQueryPipeline;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.store.proto.*;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.retain.rpc.proto.*;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class MockedRetainServiceTest {
    @Mock
    private IBaseKVStoreClient retainStoreClient;
    private RetainService service;
    private String topic = "testRetain";
    private String serviceName = "retainService";
    private String methodName = "testMethod";
    private String tenantId = "testTenantId";
    private String clusterId = "testClusterId";
    private String leaderStoreId = "testLeaderStoreId";
    private KVRangeDescriptor descriptor = KVRangeDescriptor.getDefaultInstance();
    private KVRangeSetting setting = new KVRangeSetting(clusterId, leaderStoreId, descriptor);
    private ClientInfo clientInfo = ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .build();
    private RetainRequest request = RetainRequest.newBuilder()
            .setTopic(topic)
            .setMessage(Message.getDefaultInstance())
            .setPublisher(clientInfo)
            .build();
    private MatchRequest matchRequest = MatchRequest.newBuilder()
            .setTenantId(tenantId)
            .setTopicFilter(topic)
            .setLimit(1)
            .build();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        service = new RetainService(retainStoreClient);
        Context.current()
                .withValue(RPCContext.METER_KEY_CTX_KEY, RPCMeters.MeterKey.builder()
                        .service(serviceName)
                        .method(methodName)
                        .tenantId(tenantId)
                        .build())
                .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
                .attach();
        when(retainStoreClient.findByKey(any()))
                .thenReturn(Optional.of(setting));
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void testPutRetainNormally() {
        Map<String, RetainResult> pack = new HashMap<>();
        pack.put(topic, RetainResult.RETAINED);
        when(retainStoreClient.createMutationPipeline(anyString()))
                .thenReturn(new DummyMutationPipeline(getKVRangeRWReply(ReplyCode.Ok,
                        RetainResultPack.newBuilder().putAllResults(pack).build()), false));
        RetainReply.Builder builder = RetainReply.newBuilder();
        StreamObserver<RetainReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RetainReply retainReply) {
                builder.setResult(retainReply.getResult());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.retain(request, responseObserver);
        Assert.assertEquals(builder.getResult(), RetainReply.Result.RETAINED);
    }

    @Test
    public void testPutRetainWithErrorCode() {
        Map<String, RetainResult> pack = new HashMap<>();
        pack.put(topic, RetainResult.RETAINED);
        when(retainStoreClient.createMutationPipeline(anyString()))
                .thenReturn(new DummyMutationPipeline(getKVRangeRWReply(ReplyCode.BadRequest,
                        RetainResultPack.newBuilder().putAllResults(pack).build()), false));
        RetainReply.Builder builder = RetainReply.newBuilder();
        StreamObserver<RetainReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RetainReply retainReply) {
                builder.setResult(retainReply.getResult());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.retain(request, responseObserver);
        Assert.assertEquals(builder.getResult(), RetainReply.Result.ERROR);
    }

    @Test
    public void testPutRetainWithException() {
        Map<String, RetainResult> pack = new HashMap<>();
        pack.put(topic, RetainResult.RETAINED);
        when(retainStoreClient.createMutationPipeline(anyString()))
                .thenReturn(new DummyMutationPipeline(getKVRangeRWReply(ReplyCode.Ok,
                        RetainResultPack.newBuilder().putAllResults(pack).build()), true));
        RetainReply.Builder builder = RetainReply.newBuilder();
        StreamObserver<RetainReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RetainReply retainReply) {
                builder.setResult(retainReply.getResult());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.retain(request, responseObserver);
        Assert.assertEquals(builder.getResult(), RetainReply.Result.ERROR);
    }

    @Test
    public void testMatchRetainNormally() {
        Map<String, MatchResult> pack = new HashMap<>();
        pack.put(topic, MatchResult.newBuilder()
                .setOk(Matched.newBuilder().addAllMessages(List.of(TopicMessage.getDefaultInstance())))
                .build());
        when(retainStoreClient.createLinearizedQueryPipeline(anyString()))
                .thenReturn(new DummyQueryPipeline(getKVRangeROReply(ReplyCode.Ok,
                        MatchResultPack.newBuilder().putAllResults(pack).build()), false));
        MatchReply.Builder builder = MatchReply.newBuilder();
        StreamObserver<MatchReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(MatchReply matchReply) {
                builder.setResult(matchReply.getResult());
                builder.addAllMessages(matchReply.getMessagesList());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.match(matchRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), MatchReply.Result.OK);
        Assert.assertEquals(builder.getMessagesCount(), 1);
        Assert.assertEquals(builder.getMessages(0), TopicMessage.getDefaultInstance());
    }

    @Test
    public void testMatchRetainWithErrorCode() {
        Map<String, MatchResult> pack = new HashMap<>();
        pack.put(topic, MatchResult.newBuilder()
                .setOk(Matched.newBuilder().addAllMessages(List.of(TopicMessage.getDefaultInstance())))
                .build());
        when(retainStoreClient.createLinearizedQueryPipeline(anyString()))
                .thenReturn(new DummyQueryPipeline(getKVRangeROReply(ReplyCode.BadRequest,
                        MatchResultPack.newBuilder().putAllResults(pack).build()), false));
        MatchReply.Builder builder = MatchReply.newBuilder();
        StreamObserver<MatchReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(MatchReply matchReply) {
                builder.setResult(matchReply.getResult());
                builder.addAllMessages(matchReply.getMessagesList());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.match(matchRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), MatchReply.Result.ERROR);
    }

    @Test
    public void testMatchRetainWithException() {
        Map<String, MatchResult> pack = new HashMap<>();
        pack.put(topic, MatchResult.newBuilder()
                .setOk(Matched.newBuilder().addAllMessages(List.of(TopicMessage.getDefaultInstance())))
                .build());
        when(retainStoreClient.createLinearizedQueryPipeline(anyString()))
                .thenReturn(new DummyQueryPipeline(getKVRangeROReply(ReplyCode.Ok,
                        MatchResultPack.newBuilder().putAllResults(pack).build()), true));
        MatchReply.Builder builder = MatchReply.newBuilder();
        StreamObserver<MatchReply> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(MatchReply matchReply) {
                builder.setResult(matchReply.getResult());
                builder.addAllMessages(matchReply.getMessagesList());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.match(matchRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), MatchReply.Result.ERROR);
    }

    private KVRangeRWReply getKVRangeRWReply(ReplyCode code, RetainResultPack pack) {
        KVRangeRWReply.Builder builder = KVRangeRWReply.newBuilder();
        builder.setCode(code)
                .setRwCoProcResult(RWCoProcOutput.newBuilder()
                        .setRetainService(RetainServiceRWCoProcOutput.newBuilder()
                                .setBatchRetain(BatchRetainReply.newBuilder()
                                        .putResults(tenantId, pack)
                                        .build())
                                .build())
                        .build());
        return builder.build();
    }

    private KVRangeROReply getKVRangeROReply(ReplyCode code, MatchResultPack pack) {
        KVRangeROReply.Builder builder = KVRangeROReply.newBuilder();
        builder.setCode(code);
        builder.setRoCoProcResult(ROCoProcOutput.newBuilder()
                .setRetainService(RetainServiceROCoProcOutput.newBuilder()
                        .setBatchMatch(BatchMatchReply.newBuilder()
                                .putResultPack(tenantId, pack)
                                .build())
                        .build())
                .build());
        return builder.build();
    }

    class DummyMutationPipeline implements IMutationPipeline {
        private KVRangeRWReply reply;
        private boolean isException;

        DummyMutationPipeline(KVRangeRWReply reply, boolean isException) {
            this.reply = reply;
            this.isException = isException;
        }

        @Override
        public CompletableFuture<KVRangeRWReply> execute(KVRangeRWRequest request) {
            if (isException) {
                return CompletableFuture.failedFuture(new RuntimeException("test failure in mutation"));
            }else {
                return CompletableFuture.completedFuture(reply);
            }
        }

        @Override
        public void close() {

        }
    }

    class DummyQueryPipeline implements IQueryPipeline {
        private KVRangeROReply reply;
        private boolean isException;

        public DummyQueryPipeline(KVRangeROReply reply, boolean isException) {
            this.reply = reply;
            this.isException = isException;
        }

        @Override
        public CompletableFuture<KVRangeROReply> query(KVRangeRORequest request) {
            if (isException) {
                return CompletableFuture.failedFuture(new RuntimeException("test failure in query"));
            }else {
                return CompletableFuture.completedFuture(reply);
            }
        }

        @Override
        public void close() {

        }
    }
}
