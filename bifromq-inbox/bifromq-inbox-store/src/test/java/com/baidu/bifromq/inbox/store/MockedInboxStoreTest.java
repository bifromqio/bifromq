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

package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchSubRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.CommitParams;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.GCRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.Mockito.when;

public abstract class MockedInboxStoreTest {
    private KVRangeId id;
    @Mock
    protected IKVReader reader;
    @Mock
    protected IKVWriter writer;
    @Mock
    protected IKVIterator kvIterator;
    @Mock
    private ILoadTracker loadTracker;
    @Mock
    protected final IEventCollector eventCollector = event -> {
    };
    private final Supplier<IKVRangeReader> rangeReaderProvider = () -> null;
    private final ISettingProvider settingProvider = Setting::current;
    protected final String tenantId = "tenantA";
    protected final String inboxId = "inboxId";
    protected final ClientInfo clientInfo = ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .putMetadata("agent", "mqtt")
            .putMetadata("protocol", "3.1.1")
            .putMetadata("userId", "testUser")
            .putMetadata("clientId", "testClientId")
            .putMetadata("ip", "127.0.0.1")
            .putMetadata("port", "8888")
            .build();
    protected final String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
    protected final ByteString scopedInboxId = scopedInboxId(tenantId, inboxId);
    protected final Clock clock = Clock.systemUTC();
    private InboxStoreCoProc coProc;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.iterator()).thenReturn(kvIterator);
        id = KVRangeIdUtil.generate();
        coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    protected InboxServiceRWCoProcOutput requestRW(InboxServiceRWCoProcInput input) {
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();
        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            return output;
        }catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    protected InboxServiceROCoProcOutput requestRO(InboxServiceROCoProcInput input) {
        ByteString result = coProc.query(input.toByteString(), reader).join();
        try {
            InboxServiceROCoProcOutput output = InboxServiceROCoProcOutput.parseFrom(result);
            return output;
        }catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    protected InboxServiceRWCoProcInput getInsertInput(String topicFilter,
                                                       String topic,
                                                       QoS pubQoS,
                                                       QoS subQoS,
                                                       String ...messages) {
        InboxServiceRWCoProcInput.Builder builder = InboxServiceRWCoProcInput.newBuilder();
        List<TopicMessagePack.PublisherPack> messagePacks = Arrays.stream(messages).map(m -> TopicMessagePack
                        .PublisherPack.newBuilder()
                        .addMessage(Message.newBuilder()
                                .setPubQoS(pubQoS)
                                .setPayload(ByteString.copyFromUtf8(m))
                                .setMessageId(System.nanoTime())
                                .build()).build())
                .collect(Collectors.toList());
        builder.setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                        .setSubInfo(SubInfo.newBuilder()
                                .setTenantId(tenantId)
                                .setInboxId(inboxId)
                                .setSubQoS(subQoS)
                                .setTopicFilter(topicFilter)
                                .build())
                        .addMessages(TopicMessagePack.newBuilder()
                                .setTopic(topic)
                                .addAllMessage(messagePacks)
                                .build())
                        .build())
                .build());
        return builder.build();
    }

    protected InboxServiceRWCoProcInput getCommitInput(long qos0UpToSeq, long qos1UpToSeq, long qos2UpToSeq) {
        return InboxServiceRWCoProcInput.newBuilder()
                .setBatchCommit(BatchCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, CommitParams.newBuilder()
                                .setQos0UpToSeq(qos0UpToSeq)
                                .setQos1UpToSeq(qos1UpToSeq)
                                .setQos2UpToSeq(qos2UpToSeq)
                                .build())
                        .build())
                .build();
    }

    protected InboxServiceRWCoProcInput getCreateInput() {
        return InboxServiceRWCoProcInput.newBuilder()
                .setBatchCreate(BatchCreateRequest.newBuilder()
                        .putInboxes(scopedInboxIdUtf8, CreateParams.newBuilder()
                                .setClient(clientInfo)
                                .build())
                        .build())
                .build();
    }

    protected InboxServiceRWCoProcInput getDeleteInput(Map<String, Boolean> allScopedInboxId) {
        return InboxServiceRWCoProcInput.newBuilder()
                .setBatchTouch(BatchTouchRequest.newBuilder()
                        .putAllScopedInboxId(allScopedInboxId)
                        .build())
                .build();
    }

    protected InboxServiceRWCoProcInput getSubInput(Map<String, QoS> allTopicFilters) {
        return InboxServiceRWCoProcInput.newBuilder()
                .setBatchSub(BatchSubRequest.newBuilder()
                        .putAllTopicFilters(allTopicFilters)
                        .build())
                .build();
    }

    protected InboxServiceRWCoProcInput getUnsubInput(ByteString ...topicFilters) {
        return InboxServiceRWCoProcInput.newBuilder()
                .setBatchUnsub(BatchUnsubRequest.newBuilder()
                        .addAllTopicFilters(List.of(topicFilters))
                        .build())
                .build();
    }

    protected InboxServiceROCoProcInput getGCScanInput(int limit) {
        return InboxServiceROCoProcInput.newBuilder()
                .setGc(GCRequest.newBuilder()
                        .setLimit(limit).build())
                .build();
    }

    protected InboxServiceROCoProcInput getHasInput(ByteString ...inboxIds) {
        return InboxServiceROCoProcInput.newBuilder()
                .setBatchCheck(BatchCheckRequest.newBuilder()
                        .addAllScopedInboxId(List.of(inboxIds))
                        .build())
                .build();
    }

    protected InboxServiceROCoProcInput getFetchInput(Map<String, FetchParams> allInboxFetch) {
        return InboxServiceROCoProcInput.newBuilder()
                .setBatchFetch(BatchFetchRequest.newBuilder()
                        .putAllInboxFetch(allInboxFetch)
                        .build())
                .build();
    }
}
