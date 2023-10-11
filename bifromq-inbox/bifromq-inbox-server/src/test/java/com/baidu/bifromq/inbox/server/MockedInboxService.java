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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IMutationPipeline;
import com.baidu.bifromq.basekv.client.IQueryPipeline;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxMessagePack;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.inbox.rpc.proto.SubRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.util.PipelineUtil;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class MockedInboxService {
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IBaseKVStoreClient inboxStoreClient;
    @Mock
    protected IQueryPipeline queryPipeline;
    @Mock
    protected IMutationPipeline mutPipeline;
    private ISettingProvider settingProvider = Setting::current;
    protected InboxService inboxService;
    protected String tenantId = "testTenantId";
    protected String inboxId = "testInboxId";
    protected ByteString scopedInboxId = scopedInboxId(tenantId, inboxId);
    protected String scopedInboxIdUtf8 = scopedInboxId.toStringUtf8();
    protected String clusterId = "testClusterId";
    private String leaderId = "testLeaderId";
    private String serviceName = "inboxService";
    private String methodName = "testMethod";
    protected List<String> topicFilters = new ArrayList<>() {{
        add("test");
    }};
    private ClientInfo clientInfo = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .build();
    protected SubInfo subInfo = SubInfo.newBuilder()
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setSubQoS(QoS.AT_MOST_ONCE)
        .setTopicFilter(topicFilters.get(0))
        .build();
    protected List<InboxMessagePack> inboxMessagePacks = new ArrayList<>() {{
        add(InboxMessagePack.newBuilder()
            .setMessages(TopicMessagePack.newBuilder()
                .setTopic(topicFilters.get(0))
                .addAllMessage(List.of(TopicMessagePack.PublisherPack.newBuilder()
                    .setPublisher(clientInfo)
                    .addAllMessage(List.of(Message.getDefaultInstance()))
                    .build()))
                .build())
            .addAllSubInfo(List.of(subInfo))
            .build());
    }};
    private long reqId = System.nanoTime();
    protected HasInboxRequest hasRequest = HasInboxRequest.newBuilder()
        .setReqId(reqId)
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .build();
    protected CreateInboxRequest createRequest = CreateInboxRequest.newBuilder()
        .setReqId(reqId)
        .setInboxId(inboxId)
        .setClientInfo(clientInfo)
        .build();
    protected DeleteInboxRequest deleteRequest = DeleteInboxRequest.newBuilder()
        .setReqId(reqId)
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .build();
    protected SubRequest subRequest = SubRequest.newBuilder()
        .setReqId(reqId)
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setSubQoS(QoS.AT_MOST_ONCE)
        .setTopicFilter(topicFilters.get(0))
        .build();
    protected UnsubRequest unsubRequest = UnsubRequest.newBuilder()
        .setReqId(reqId)
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setTopicFilter(topicFilters.get(0))
        .build();
    protected SendRequest sendRequest = SendRequest.newBuilder()
        .setReqId(reqId)
        .addAllInboxMsgPack(inboxMessagePacks)
        .build();
    protected InboxFetchHint fetchHint = InboxFetchHint.newBuilder()
        .setIncarnation(System.nanoTime())
        .setCapacity(1)
        .setInboxId(inboxId)
        .build();
    protected CommitRequest commitRequest = CommitRequest.newBuilder()
        .setReqId(reqId)
        .setTenantId(tenantId)
        .setInboxId(inboxId)
        .setQos(QoS.AT_LEAST_ONCE)
        .setUpToSeq(1l)
        .build();

    protected ExpireInboxRequest expireInboxRequest = ExpireInboxRequest.newBuilder()
        .setReqId(System.nanoTime())
        .setTenantId(tenantId)
        .setExpirySeconds(10)
        .build();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        inboxService = new InboxService(settingProvider, distClient, inboxStoreClient, null);
        Map<String, String> metaData = new HashMap<>();
        metaData.put(PipelineUtil.PIPELINE_ATTR_KEY_ID, "id");
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, RPCMeters.MeterKey.builder()
                .service(serviceName)
                .method(methodName)
                .tenantId(tenantId)
                .build())
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metaData)
            .attach();
        when(inboxStoreClient.createMutationPipeline(anyString()))
            .thenReturn(mutPipeline);
        when(inboxStoreClient.findByKey(any()))
            .thenReturn(Optional.of(new KVRangeSetting(clusterId, leaderId, KVRangeDescriptor.getDefaultInstance())));
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
        inboxService.stop();
    }

    protected void mockExecutePipeline(ReplyCode code, RWCoProcOutput rwCoProcOutput) {
        when(mutPipeline.execute(any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeRWReply.newBuilder()
                .setCode(code)
                .setRwCoProcResult(rwCoProcOutput)
                .build()));
    }

    protected <T> RWCoProcOutput generateRWCoProcResult(T data) {
        RWCoProcOutput.Builder rwBuilder = RWCoProcOutput.newBuilder();
        InboxServiceRWCoProcOutput.Builder builder = InboxServiceRWCoProcOutput.newBuilder();
        if (data instanceof BatchCreateReply) {
            builder.setBatchCreate((BatchCreateReply) data);
        } else if (data instanceof BatchTouchReply) {
            builder.setBatchTouch((BatchTouchReply) data);
        } else if (data instanceof BatchSubReply) {
            builder.setBatchSub((BatchSubReply) data);
        } else if (data instanceof BatchUnsubReply) {
            builder.setBatchUnsub((BatchUnsubReply) data);
        } else if (data instanceof BatchInsertReply) {
            builder.setBatchInsert((BatchInsertReply) data);
        } else if (data instanceof BatchCommitReply) {
            builder.setBatchCommit((BatchCommitReply) data);
        }
        rwBuilder.setInboxService(builder.build());
        return rwBuilder.build();
    }

    protected ROCoProcOutput generateBatchCheckROCoProcResult(Map<String, Boolean> existMap) {
        ROCoProcOutput.Builder builder = ROCoProcOutput.newBuilder();
        InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.newBuilder()
            .setReqId(System.nanoTime())
            .setBatchCheck(BatchCheckReply.newBuilder()
                .putAllExists(existMap)
                .build())
            .build();
        builder.setInboxService(output);
        return builder.build();
    }

    protected ROCoProcOutput generateBatchFetchROCoProcResult(Map<String, Fetched> fetchedMap) {
        ROCoProcOutput.Builder builder = ROCoProcOutput.newBuilder();
        InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.newBuilder()
            .setReqId(System.nanoTime())
            .setBatchFetch(BatchFetchReply.newBuilder().putAllResult(fetchedMap).build())
            .build();
        builder.setInboxService(output);
        return builder.build();
    }

    protected void mockInboxStoreLinearizedQuery(ReplyCode code, ROCoProcOutput roCoProcResult) {
        when(inboxStoreClient.createLinearizedQueryPipeline(anyString())).thenReturn(queryPipeline);
        when(queryPipeline.query(any())).thenReturn(CompletableFuture.completedFuture(KVRangeROReply.newBuilder()
            .setCode(code)
            .setRoCoProcResult(roCoProcResult)
            .build()));
    }

    protected void mockDistUnMatch(UnmatchResult result) {
        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(result));
    }

    protected void mockDistMatch(MatchResult result) {
        when(distClient.match(anyLong(), anyString(), anyString(), any(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(result));
    }

    protected void mockScanRange(KVRangeSetting setting, KVRangeROReply reply) {
        when(inboxStoreClient.findById(eq(setting.id))).thenReturn(Optional.of(setting));
        when(inboxStoreClient.query(eq(setting.leader), any(KVRangeRORequest.class)))
            .thenReturn(CompletableFuture.completedFuture(reply));
    }

    class TestingStreamObserver<T> extends ServerCallStreamObserver<T> {

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable runnable) {

        }

        @Override
        public void setCompression(String s) {

        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {

        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public void request(int i) {

        }

        @Override
        public void setMessageCompression(boolean b) {

        }

        @Override
        public void onNext(T t) {

        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
    }
}
