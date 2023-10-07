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
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.fail;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.inbox.storage.proto.BatchCommitReply;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MockedInboxCommitTest extends MockedInboxStoreTest {

    private List<InboxMessage> messages = new ArrayList<>() {{
        add(InboxMessage.getDefaultInstance());
        add(InboxMessage.getDefaultInstance());
        add(InboxMessage.getDefaultInstance());
        add(InboxMessage.getDefaultInstance());
    }};

    @Test
    public void testCommitQoS0InboxWithNoEntry() {
        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(10, 0, 0))
                .getBatchCommit();
            Assert.assertTrue(!commitReply.getResultMap().get(scopedInboxIdUtf8));
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCommitQoS0InboxWithExpiration() {
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString()));

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(10, 0, 0))
                .getBatchCommit();
            Assert.assertTrue(!commitReply.getResultMap().get(scopedInboxIdUtf8));
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCommitQoS0InboxWithErrorSeq() {
        long oldestSeq = 15;

        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setQos0StartSeq(oldestSeq)
                .setQos0NextSeq(oldestSeq + 1)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq));

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(10, 0, 0))
                .getBatchCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argCap.capture(), argCap.capture());
            List<ByteString> args = argCap.getAllValues();

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(metadata.getQos0StartSeq(), oldestSeq);
        } catch (Exception exception) {
            fail();
        }
    }

    @SneakyThrows
    @Test
    public void commitQoS0InboxFromOneEntry() {
        long qos0UpToSeq = 1;
        long oldestSeq = 0;

        when(reader.get(scopedInboxId)).thenReturn(Optional.of(InboxMetadata.newBuilder()
            .setLastFetchTime(clock.millis())
            .setExpireSeconds(Integer.MAX_VALUE)
            .setQos0StartSeq(0)
            .setQos0NextSeq(4)
            .build().toByteString()));
        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(qos0InboxMsgKey(scopedInboxId, oldestSeq));
        when(kvIterator.value()).thenReturn(InboxMessageList.newBuilder()
            .addAllMessage(messages)
            .build().toByteString());

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(qos0UpToSeq, 0, 0))
                .getBatchCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            ArgumentCaptor<Boundary> rangeCaptor = ArgumentCaptor.forClass(Boundary.class);
            verify(writer).clear(rangeCaptor.capture());
            assertEquals(qos0InboxMsgKey(scopedInboxId, 0), rangeCaptor.getValue().getStartKey());
            assertEquals(qos0InboxMsgKey(scopedInboxId, 2), rangeCaptor.getValue().getEndKey());


            ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).insert(argCap.capture(), argCap.capture());
            assertEquals(qos0InboxMsgKey(scopedInboxId, 2), argCap.getAllValues().get(0));
            InboxMessageList messageList = InboxMessageList.parseFrom(argCap.getAllValues().get(1));
            assertEquals(messageList.getMessageCount(), 2);

            argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argCap.capture(), argCap.capture());
            List<ByteString> args = argCap.getAllValues();

            assertEquals(scopedInboxId, args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(metadata.getQos0StartSeq(), qos0UpToSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS0InboxFromMultipleEntries() {
        long qos0UpToSeq = 11;
        long currentEntrySeq = 9;
        long oldestSeq = 5;

        when(reader.get(any())).thenReturn(Optional.of(InboxMetadata.newBuilder()
            .setLastFetchTime(clock.millis())
            .setExpireSeconds(Integer.MAX_VALUE)
            .setQos0StartSeq(0)
            .setQos0NextSeq(12)
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

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(qos0UpToSeq, 0, 0))
                .getBatchCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            ArgumentCaptor<Boundary> rangeCaptor = ArgumentCaptor.forClass(Boundary.class);
            verify(writer).clear(rangeCaptor.capture());
            assertEquals(qos0InboxMsgKey(scopedInboxId, 0), rangeCaptor.getValue().getStartKey());
            assertEquals(qos0InboxMsgKey(scopedInboxId, 12), rangeCaptor.getValue().getEndKey());

            ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argCap.capture(), argCap.capture());
            List<ByteString> args = argCap.getAllValues();
            assertEquals(scopedInboxId, args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(metadata.getQos0StartSeq(), qos0UpToSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @SneakyThrows
    @Test
    public void commitQoS1InboxFromOneEntry() {
        long qos1UpToSeq = 1;
        long oldestSeq = 0;

        when(reader.get(scopedInboxId))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setQos1StartSeq(0)
                .setQos1NextSeq(4)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), oldestSeq));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(messages)
                .build().toByteString());

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(0, qos1UpToSeq, 0))
                .getBatchCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            ArgumentCaptor<Boundary> rangeCaptor = ArgumentCaptor.forClass(Boundary.class);
            verify(writer).clear(rangeCaptor.capture());
            assertEquals(qos1InboxMsgKey(scopedInboxId, 0), rangeCaptor.getValue().getStartKey());
            assertEquals(qos1InboxMsgKey(scopedInboxId, 2), rangeCaptor.getValue().getEndKey());


            ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).insert(argCap.capture(), argCap.capture());
            assertEquals(qos1InboxMsgKey(scopedInboxId, 2), argCap.getAllValues().get(0));
            InboxMessageList messageList = InboxMessageList.parseFrom(argCap.getAllValues().get(1));
            assertEquals(messageList.getMessageCount(), 2);


            argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argCap.capture(), argCap.capture());
            List<ByteString> args = argCap.getAllValues();

            assertEquals(scopedInboxId, args.get(0));
            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(metadata.getQos1StartSeq(), qos1UpToSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void commitQoS1InboxFromMultipleEntries() {
        long qos1UpToSeq = 11;
        long currentEntrySeq = 9;
        long oldestSeq = 5;

        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .setQos1StartSeq(0)
                .setQos1NextSeq(12)
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

        try {
            BatchCommitReply commitReply = requestRW(getCommitInput(0, qos1UpToSeq, 0))
                .getBatchCommit();
            Assert.assertTrue(commitReply.getResultMap().get(scopedInboxIdUtf8));

            ArgumentCaptor<Boundary> rangeCaptor = ArgumentCaptor.forClass(Boundary.class);
            verify(writer).clear(rangeCaptor.capture());
            assertEquals(qos1InboxMsgKey(scopedInboxId, 0), rangeCaptor.getValue().getStartKey());
            assertEquals(qos1InboxMsgKey(scopedInboxId, 12), rangeCaptor.getValue().getEndKey());

            ArgumentCaptor<ByteString> argCap = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argCap.capture(), argCap.capture());
            List<ByteString> args = argCap.getAllValues();

            assertEquals(scopedInboxId, args.get(0));

            InboxMetadata metadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(metadata.getQos1StartSeq(), qos1UpToSeq + 1);
        } catch (Exception exception) {
            fail();
        }
    }
}
