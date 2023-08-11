package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.inbox.storage.proto.InboxCommit;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitReply;
import com.baidu.bifromq.inbox.storage.proto.InboxCommitRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.google.protobuf.ByteString;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.fail;

public class MockedInboxCommitTest {
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
    @Mock
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
    public void testCommitQoS0InboxWithNoEntry() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos0UpToSeq(10)
                                .build())
                        .build())
                .build();

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(!commitReply.getResultMap().get(scopedInboxIdUtf8));
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCommitQoS0InboxWithExpiration() {
        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos0UpToSeq(10)
                                .build())
                        .build())
                .build();

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                         .setLastFetchTime(clock.millis() - 30 * 1000)
                         .setExpireSeconds(1)
                        .build().toByteString()));

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(!commitReply.getResultMap().get(scopedInboxIdUtf8));
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCommitQoS0InboxWithErrorSeq() {
        long qos0UpToSeq = 10;
        long oldestSeq = 15;

        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos0UpToSeq(qos0UpToSeq)
                                .build())
                        .build())
                .build();

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(false);
        when(kvIterator.key())
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq));

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).put(argCap.capture(), argCap.capture());
        List<ByteString> args = argCap.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            Assert.assertEquals(metadata.getQos0LastFetchBeforeSeq(), oldestSeq);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS0InboxFromOneEntry() {
        long qos0UpToSeq = 1;
        long oldestSeq = 0;

        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos0UpToSeq(qos0UpToSeq)
                                .build())
                        .build())
                .build();

        List<InboxMessage> messages = new ArrayList<>() {{
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
        }};

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq));
        when(kvIterator.value())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(messages)
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argCap.capture(), argCap.capture());
        verify(writer).delete(argCap.capture());
        List<ByteString> args = argCap.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            InboxMessageList messageList = InboxMessageList.parseFrom(args.get(1));
            Assert.assertEquals(messageList.getMessageCount(), messages.size() - qos0UpToSeq - oldestSeq - 1);

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0LastFetchBeforeSeq(), qos0UpToSeq + 1);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS0InboxFromMultipleEntries() {
        long qos0UpToSeq = 11;
        long currentEntrySeq = 9;
        long oldestSeq = 5;

        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos0UpToSeq(qos0UpToSeq)
                                .build())
                        .build())
                .build();

        List<InboxMessage> messages = new ArrayList<>() {{
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
        }};

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq))
                .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), currentEntrySeq));
        when(kvIterator.value())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(messages)
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argCap.capture(), argCap.capture());
        verify(writer).delete(argCap.capture());
        List<ByteString> args = argCap.getAllValues();

        ArgumentCaptor<Range> rangeArgumentCaptor = ArgumentCaptor.forClass(Range.class);
        verify(writer).deleteRange(rangeArgumentCaptor.capture());

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            InboxMessageList messageList = InboxMessageList.parseFrom(args.get(1));
            Assert.assertEquals(messageList.getMessageCount(), messages.size() -
                    (qos0UpToSeq - currentEntrySeq + 1));

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos0LastFetchBeforeSeq(), qos0UpToSeq + 1);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS1InboxFromOneEntry() {
        long qos1UpToSeq = 1;
        long oldestSeq = 0;

        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos1UpToSeq(qos1UpToSeq)
                                .build())
                        .build())
                .build();

        List<InboxMessage> messages = new ArrayList<>() {{
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
        }};

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq));
        when(kvIterator.value())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(messages)
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argCap.capture(), argCap.capture());
        verify(writer).delete(argCap.capture());
        List<ByteString> args = argCap.getAllValues();

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            InboxMessageList messageList = InboxMessageList.parseFrom(args.get(1));
            Assert.assertEquals(messageList.getMessageCount(), messages.size() - qos1UpToSeq - oldestSeq - 1);

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos1LastCommitBeforeSeq(), qos1UpToSeq + 1);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS1InboxFromMultipleEntries() {
        long qos1UpToSeq = 11;
        long currentEntrySeq = 9;
        long oldestSeq = 5;

        InboxServiceRWCoProcInput input = InboxServiceRWCoProcInput.newBuilder()
                .setCommit(InboxCommitRequest.newBuilder()
                        .putInboxCommit(scopedInboxIdUtf8, InboxCommit.newBuilder()
                                .setQos1UpToSeq(qos1UpToSeq)
                                .build())
                        .build())
                .build();

        List<InboxMessage> messages = new ArrayList<>() {{
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
            add(InboxMessage.getDefaultInstance());
        }};

        when(reader.get(any()))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq))
                .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), currentEntrySeq));
        when(kvIterator.value())
                .thenReturn(InboxMessageList.newBuilder()
                        .addAllMessage(messages)
                        .build().toByteString());

        InboxStoreCoProc coProc = new InboxStoreCoProc(id, rangeReaderProvider, eventCollector,
                clock, Duration.ofMinutes(30), loadTracker);
        ByteString result = coProc.mutate(input.toByteString(), reader, writer).get();

        ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argCap.capture(), argCap.capture());
        verify(writer).delete(argCap.capture());
        List<ByteString> args = argCap.getAllValues();

        ArgumentCaptor<Range> rangeArgumentCaptor = ArgumentCaptor.forClass(Range.class);
        verify(writer).deleteRange(rangeArgumentCaptor.capture());

        try {
            InboxServiceRWCoProcOutput output = InboxServiceRWCoProcOutput.parseFrom(result);
            InboxCommitReply commitReply = output.getCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            InboxMessageList messageList = InboxMessageList.parseFrom(args.get(1));
            Assert.assertEquals(messageList.getMessageCount(), messages.size() -
                    (qos1UpToSeq - currentEntrySeq + 1));

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(3));
            Assert.assertEquals(metadata.getQos1LastCommitBeforeSeq(), qos1UpToSeq + 1);
        }catch (Exception exception) {
            fail();
        }
    }
}
