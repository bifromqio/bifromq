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

import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.fail;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertReply;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MockedInboxInsertTest {
    private KVRangeId id;
    @Mock
    private IKVReader reader;
    @Mock
    private IKVWriter writer;
    @Mock
    private IKVIterator kvIterator;
    @Mock
    private ILoadTracker loadTracker;
    private final Supplier<IKVRangeReader> rangeReaderProvider = () -> null;
    private final ISettingProvider settingProvider = Setting::current;
    @Mock
    private final IEventCollector eventCollector = event -> {
    };
    private final String tenantId = "tenantA";
    private final String inboxId = "inboxId";
    private final String scopedInboxIdUtf8 = scopedInboxId(tenantId, inboxId).toStringUtf8();
    private final Clock clock = Clock.systemUTC();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.iterator()).thenReturn(kvIterator);
        id = KVRangeIdUtil.generate();
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void testInsertQoS0InboxWithNoInbox() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.getDefaultInstance())
                        .build())
                    .build())
                .build())
            .build();
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsList().size(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.NO_INBOX);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithExpiration() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.getDefaultInstance())
                        .build())
                    .build())
                .build())
            .build();

        when(reader.get(any())).thenReturn(Optional.of(InboxMetadata.newBuilder()
            .setQos0NextSeq(10)
            .setLastFetchTime(clock.millis() - 30 * 1000)
            .setExpireSeconds(1)
            .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        coProc.mutate(input.toByteString(), reader, writer);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsList().size(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.NO_INBOX);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertWithNoSub() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(true)
                .setLimit(100)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.NO_INBOX);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxNormallyForDropOldestPolicy() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(true)
                .setLimit(100)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).insert(argumentCaptor.capture(), argumentCaptor.capture());
        verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
        List<ByteString> args = argumentCaptor.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 4);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithDropOldestPartially() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-1"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-2"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(true)
                .setLimit(11)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).delete(argumentCaptor.capture());
        verify(writer, times(2)).insert(argumentCaptor.capture(), argumentCaptor.capture());
        verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
        List<ByteString> args = argumentCaptor.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 7);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0),
                args.get(0));
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1),
                args.get(1));
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                args.get(3));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(6));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 2);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithDropOldestPartiallyAndMultiEntries() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-1"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-2"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-3"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 5;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 3));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(true)
                .setLimit(4)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> writerArgsCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).insert(writerArgsCap.capture(), writerArgsCap.capture());
        verify(writer).put(writerArgsCap.capture(), writerArgsCap.capture());
        List<ByteString> writerArgs = writerArgsCap.getAllValues();

        ArgumentCaptor<Range> rangeArgsCap = ArgumentCaptor.forClass(Range.class);
        verify(writer).deleteRange(rangeArgsCap.capture());

        ArgumentCaptor<Overflowed> overflowArgs = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(overflowArgs.capture());
        List<Overflowed> overflow = overflowArgs.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(writerArgs.size(), 6);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 4),
                writerArgs.get(0));
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                writerArgs.get(2));
            InboxMetadata metadata = InboxMetadata.parseFrom(writerArgs.get(5));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 3);

            Assert.assertEquals(overflow.size(), 1);
            Assert.assertEquals(overflow.get(0).dropCount(), 3);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithDropOldestFully() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-1"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-2"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-3"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 2;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(true)
                .setLimit(2)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> writerArgs = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).insert(writerArgs.capture(), writerArgs.capture());
        verify(writer).put(writerArgs.capture(), writerArgs.capture());
        List<ByteString> args = writerArgs.getAllValues();

        ArgumentCaptor<Overflowed> overflowArgs = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(overflowArgs.capture());
        List<Overflowed> overflow = overflowArgs.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 4);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                args.get(0));
            Assert.assertEquals(ByteString.copyFromUtf8(scopedInboxIdUtf8),
                args.get(2));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 2);

            Assert.assertEquals(overflow.size(), 1);
            Assert.assertEquals(overflow.get(0).dropCount(), 3);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxNormallyWithDropYoungestPolicy() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(false)
                .setLimit(100)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).insert(argumentCaptor.capture(), argumentCaptor.capture());
        verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
        List<ByteString> args = argumentCaptor.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 4);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithDropYoungestPartially() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-1"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-2"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-3"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;
        int limit = 12;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(false)
                .setLimit(limit)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> writerArgs = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).insert(writerArgs.capture(), writerArgs.capture());
        verify(writer).put(writerArgs.capture(), writerArgs.capture());
        List<ByteString> args = writerArgs.getAllValues();

        ArgumentCaptor<Overflowed> overflowArgs = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(overflowArgs.capture());
        List<Overflowed> overflow = overflowArgs.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 4);
            Assert.assertEquals(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), nextSeq),
                args.get(0));
            InboxMessageList inboxMessageList = InboxMessageList.parseFrom(args.get(1));
            Assert.assertEquals(inboxMessageList.getMessageCount(), 2);
            Assert.assertEquals(inboxMessageList.getMessage(0).getMsg().getMessage().getPayload().toStringUtf8(),
                "test-1");
            Assert.assertEquals(inboxMessageList.getMessage(1).getMsg().getMessage().getPayload().toStringUtf8(),
                "test-2");
            Assert.assertEquals(ByteString.copyFromUtf8(scopedInboxIdUtf8),
                args.get(2));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq + 2);

            Assert.assertEquals(overflow.size(), 1);
            Assert.assertEquals(overflow.get(0).dropCount(), 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testInsertQoS0InboxWithDropYoungestFully() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
            .setBatchInsert(BatchInsertRequest.newBuilder()
                .addSubMsgPack(MessagePack.newBuilder()
                    .setSubInfo(SubInfo.newBuilder()
                        .setTenantId(tenantId)
                        .setInboxId(inboxId)
                        .setSubQoS(QoS.AT_MOST_ONCE)
                        .setTopicFilter("test/#")
                        .build())
                    .addMessages(TopicMessagePack.newBuilder()
                        .setTopic("test/qos0")
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-1"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-2"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .addMessage(TopicMessagePack.PublisherPack.newBuilder()
                            .addMessage(Message.newBuilder()
                                .setPubQoS(QoS.AT_MOST_ONCE)
                                .setPayload(ByteString.copyFromUtf8("test-3"))
                                .setMessageId(System.nanoTime())
                                .build())
                            .build())
                        .build())
                    .build())
                .build())
            .build();
        int nextSeq = 10;
        int limit = 10;

        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setQos0NextSeq(nextSeq)
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setDropOldest(false)
                .setLimit(limit)
                .putTopicFilters("test/#", QoS.AT_MOST_ONCE)
                .build().toByteString()));
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector,
            clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> writerArgs = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).put(writerArgs.capture(), writerArgs.capture());
        List<ByteString> args = writerArgs.getAllValues();

        ArgumentCaptor<Overflowed> overflowArgs = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(overflowArgs.capture());
        List<Overflowed> overflow = overflowArgs.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            BatchInsertReply reply = output.getBatchInsert();
            Assert.assertEquals(reply.getResultsCount(), 1);
            Assert.assertEquals(reply.getResults(0).getResult(), InsertResult.Result.OK);

            Assert.assertEquals(args.size(), 2);
            Assert.assertEquals(ByteString.copyFromUtf8(scopedInboxIdUtf8), args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            Assert.assertEquals(metadata.getQos0NextSeq(), nextSeq);

            Assert.assertEquals(overflow.size(), 1);
            Assert.assertEquals(overflow.get(0).dropCount(), 3);
        } catch (Exception exception) {
            fail();
        }
    }
}
