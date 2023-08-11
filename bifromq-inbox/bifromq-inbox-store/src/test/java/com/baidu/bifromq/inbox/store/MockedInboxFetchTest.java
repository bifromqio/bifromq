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
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchReply;
import com.baidu.bifromq.inbox.storage.proto.InboxFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.google.protobuf.ByteString;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Supplier;

import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.fail;

public class MockedInboxFetchTest {
    private KVRangeId id;
    @Mock
    private IKVReader reader;
    @Mock
    private IKVIterator kvIterator;
    @Mock
    private ILoadTracker loadTracker;
    private final Supplier<IKVRangeReader> rangeReaderProvider = () -> null;
    private final IEventCollector eventCollector = event -> {};
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
    public void testFetchQoS0WithNoInbox() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(100)
                                .setQos0StartAfter(0L)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(ByteString.empty());
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Assert.assertEquals(fetchReply.getResultMap().get(scopedInboxIdUtf8).getResult(), Fetched.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchExpiredInbox() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(100)
                                .setQos0StartAfter(0L)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8));
        when(kvIterator.value()).thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString());
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Assert.assertEquals(fetchReply.getResultMap().get(scopedInboxIdUtf8).getResult(), Fetched.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0FromOneEntry() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(1)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key())
                .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
                .thenReturn(InboxMetadata.newBuilder()
                    .setLastFetchTime(clock.millis() + 30 * 1000)
                    .setQos0LastFetchBeforeSeq(0)
                    .setQos0NextSeq(20)
                    .setExpireSeconds(Integer.MAX_VALUE)
                    .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                                InboxMessage.getDefaultInstance()))
                        .build().toByteString());
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 1);
            Assert.assertEquals(fetched.getQos0Msg(0), InboxMessage.getDefaultInstance());
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0FromMultipleEntries() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(3)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
                .thenReturn(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis() + 30 * 1000)
                        .setQos0LastFetchBeforeSeq(0)
                        .setQos0NextSeq(20)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                                InboxMessage.getDefaultInstance(), InboxMessage.getDefaultInstance()))
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 3);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0UtilEntryInvalid() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(100)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(kvIterator.key())
                .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
                .thenReturn(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis() + 30 * 1000)
                        .setQos0LastFetchBeforeSeq(0)
                        .setQos0NextSeq(20)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                                InboxMessage.getDefaultInstance()))
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 3);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS1FromOneEntry() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(1)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
                .thenReturn(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis() + 30 * 1000)
                        .setQos1LastCommitBeforeSeq(0)
                        .setQos1NextSeq(20)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                                InboxMessage.getDefaultInstance()))
                        .build().toByteString());
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos1MsgCount(), 1);
            Assert.assertEquals(fetched.getQos1MsgList().get(0), InboxMessage.getDefaultInstance());
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS1FromMultipleEntries() {
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
                .setFetch(InboxFetchRequest.newBuilder()
                        .putInboxFetch(scopedInboxIdUtf8, FetchParams.newBuilder()
                                .setMaxFetch(3)
                                .build())
                        .build())
                .build();

        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
                .thenReturn(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis() + 30 * 1000)
                        .setQos1LastCommitBeforeSeq(0)
                        .setQos1NextSeq(20)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                        .build().toByteString())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                                InboxMessage.getDefaultInstance()))
                        .build().toByteString());
        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString output = coProc.query(input.toByteString(), reader).join();

        try {
            InboxFetchReply fetchReply = InboxServiceROCoProcOutput.parseFrom(output).getFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos1MsgCount(), 3);
            Assert.assertEquals(fetched.getQos1SeqCount(), 3);
            Assert.assertEquals(fetched.getQos1MsgList().get(0), InboxMessage.getDefaultInstance());
        }catch (Exception exception) {
            fail();
        }
    }

}
