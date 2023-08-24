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

import com.baidu.bifromq.inbox.storage.proto.BatchFetchReply;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.InboxMessage;
import com.baidu.bifromq.inbox.storage.proto.InboxMessageList;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static com.baidu.bifromq.inbox.util.KeyUtil.qos0InboxMsgKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.qos1InboxMsgKey;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.fail;

public class MockedInboxFetchTest extends MockedInboxStoreTest {
    @Test
    public void testFetchQoS0WithNoInbox() {
        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(ByteString.empty());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(100)
                        .setQos0StartAfter(0L)
                        .build());
            }})).getBatchFetch();
            Assert.assertEquals(fetchReply.getResultMap().get(scopedInboxIdUtf8).getResult(), Fetched.Result.NO_INBOX);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchExpiredInbox() {
        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8));
        when(kvIterator.value()).thenReturn(InboxMetadata.newBuilder()
            .setLastFetchTime(clock.millis() - 30 * 1000)
            .setExpireSeconds(1)
            .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(100)
                        .setQos0StartAfter(0L)
                        .build());
            }})).getBatchFetch();
            Assert.assertEquals(fetchReply.getResultMap().get(scopedInboxIdUtf8).getResult(), Fetched.Result.NO_INBOX);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0FromOneEntry() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() + 30 * 1000)
                .setQos0StartSeq(0)
                .setQos0NextSeq(20)
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(kvIterator.isValid()).thenReturn(true);
        when(kvIterator.key()).thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value()).thenReturn(InboxMessageList.newBuilder()
            .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                InboxMessage.getDefaultInstance()))
            .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(1)
                        .build());
            }})).getBatchFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 1);
            Assert.assertEquals(fetched.getQos0Msg(0), InboxMessage.getDefaultInstance());
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0FromMultipleEntries() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() + 30 * 1000)
                .setQos0StartSeq(0)
                .setQos0NextSeq(20)
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                .build().toByteString())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance(), InboxMessage.getDefaultInstance()))
                .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(3)
                        .build());
            }})).getBatchFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 3);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS0UtilEntryInvalid() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() + 30 * 1000)
                .setQos0StartSeq(0)
                .setQos0NextSeq(20)
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos0InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                .build().toByteString())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(100)
                        .build());
            }})).getBatchFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos0MsgCount(), 3);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS1FromOneEntry() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() + 30 * 1000)
                .setQos1StartSeq(0)
                .setQos1NextSeq(20)
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(1)
                        .build());
            }})).getBatchFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos1MsgCount(), 1);
            Assert.assertEquals(fetched.getQos1MsgList().get(0), InboxMessage.getDefaultInstance());
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testFetchQoS1FromMultipleEntries() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() + 30 * 1000)
                .setQos1StartSeq(0)
                .setQos1NextSeq(20)
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(kvIterator.isValid())
            .thenReturn(true);
        when(kvIterator.key())
            .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 0))
            .thenReturn(qos1InboxMsgKey(ByteString.copyFromUtf8(scopedInboxIdUtf8), 1));
        when(kvIterator.value())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance()))
                .build().toByteString())
            .thenReturn(InboxMessageList.newBuilder()
                .addAllMessage(Arrays.asList(InboxMessage.getDefaultInstance(),
                    InboxMessage.getDefaultInstance()))
                .build().toByteString());

        try {
            BatchFetchReply fetchReply = requestRO(getFetchInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, FetchParams.newBuilder()
                        .setMaxFetch(3)
                        .build());
            }})).getBatchFetch();
            Fetched fetched = fetchReply.getResultMap().get(scopedInboxIdUtf8);
            Assert.assertEquals(fetched.getResult(), Fetched.Result.OK);
            Assert.assertEquals(fetched.getQos1MsgCount(), 3);
            Assert.assertEquals(fetched.getQos1SeqCount(), 3);
            Assert.assertEquals(fetched.getQos1MsgList().get(0), InboxMessage.getDefaultInstance());
        } catch (Exception exception) {
            fail();
        }
    }
}
