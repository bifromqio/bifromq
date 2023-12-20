/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.storage.proto.BatchCheckReply;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MockedInboxAdminTest extends MockedInboxStoreTest {
    @Test
    public void testCreateNewInbox() {
        when(reader.get(any())).thenReturn(Optional.empty());
        doNothing().when(writer).put(any(), any());

        try {
            requestRW(getCreateInput());
            ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
            List<ByteString> args = argumentCaptor.getAllValues();
            assertEquals(args.size(), 2);
            assertEquals(ByteString.copyFromUtf8(scopedInboxIdUtf8), args.get(0));
            InboxMetadata inboxMetadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(0, inboxMetadata.getQos0NextSeq());
            assertEquals(0, inboxMetadata.getQos1NextSeq());
            assertEquals(0, inboxMetadata.getQos2NextSeq());
            assertEquals(clientInfo, inboxMetadata.getClient());
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testCreateExpiredInbox() {
        when(reader.get(any())).thenReturn(Optional.of(InboxMetadata.newBuilder()
            .setLastFetchTime(clock.millis() - 30 * 1000)
            .setQos0NextSeq(1)
            .setQos1NextSeq(1)
            .setQos2NextSeq(1)
            .setExpireSeconds(1)
            .build().toByteString()));
        doNothing().when(kvIterator).seek(any());
        when(kvIterator.isValid()).thenReturn(false);
        doNothing().when(writer).put(any(), any());

        try {
            requestRW(getCreateInput());
            ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
            verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
            List<ByteString> args = argumentCaptor.getAllValues();
            assertEquals(args.size(), 2);
            assertEquals(ByteString.copyFromUtf8(scopedInboxIdUtf8), args.get(0));
            InboxMetadata inboxMetadata = InboxMetadata.parseFrom(args.get(1));
            assertEquals(0, inboxMetadata.getQos0NextSeq());
            assertEquals(0, inboxMetadata.getQos1NextSeq());
            assertEquals(0, inboxMetadata.getQos2NextSeq());
            assertEquals(clientInfo, inboxMetadata.getClient());
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testHasInbox() {
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8))).thenReturn(Optional.empty());
        when(reader.get(ByteString.copyFromUtf8("dev-" + scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .build().toByteString()));
        when(reader.get(ByteString.copyFromUtf8("expire-" + scopedInboxIdUtf8)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString()));

        try {
            BatchCheckReply batchCheckReply = requestRO(getHasInput(ByteString.copyFromUtf8(scopedInboxIdUtf8),
                ByteString.copyFromUtf8("dev-" + scopedInboxIdUtf8),
                ByteString.copyFromUtf8("expire-" + scopedInboxIdUtf8)))
                .getBatchCheck();
            Assert.assertFalse(batchCheckReply.getExistsMap().get(scopedInboxIdUtf8));
            Assert.assertTrue(batchCheckReply.getExistsMap().get("dev-" + scopedInboxIdUtf8));
            Assert.assertFalse(batchCheckReply.getExistsMap().get("expire-" + scopedInboxIdUtf8));
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testDeleteInbox() {
        String inboxId2 = scopedInboxId(tenantId, inboxId + "_2").toStringUtf8();
        String inboxId3 = scopedInboxId(tenantId, inboxId + "_expire").toStringUtf8();
        when(distClient.unmatch(anyLong(), anyString(), anyString(), anyString(), anyString(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        when(reader.get(ByteString.copyFromUtf8(scopedInboxIdUtf8)))
            .thenReturn(Optional.empty());
        when(reader.get(ByteString.copyFromUtf8(inboxId2)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(Integer.MAX_VALUE)
                .putTopicFilters("topicFilter2", QoS.AT_LEAST_ONCE)
                .build().toByteString()));
        when(reader.get(ByteString.copyFromUtf8(inboxId3)))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .putTopicFilters("topicFilter3", QoS.AT_LEAST_ONCE)
                .build().toByteString()));
        doNothing().when(writer).delete(any());

        try {
            requestRW(getDeleteInput(new HashMap<>() {{
                put(scopedInboxIdUtf8, false);
                put(inboxId2, false);
                put(inboxId3, true);
            }}));

            ArgumentCaptor<ByteString> inboxIdCaptor = ArgumentCaptor.forClass(ByteString.class);
            ArgumentCaptor<String> tfCaptor = ArgumentCaptor.forClass(String.class);

            verify(writer, times(2)).delete(inboxIdCaptor.capture());
            verify(distClient, times(2)).unmatch(anyLong(), anyString(), tfCaptor.capture(), anyString(), anyString(), anyInt());
            Set<ByteString> inboxIds = new HashSet<>(inboxIdCaptor.getAllValues());
            assertEquals(inboxIds.size(), 2);
            assertTrue(inboxIds.contains(ByteString.copyFromUtf8(inboxId2)));
            assertTrue(inboxIds.contains(ByteString.copyFromUtf8(inboxId3)));
            Set<String> tfs = new HashSet<>(tfCaptor.getAllValues());
            assertEquals(tfs.size(), 2);
            assertTrue(tfs.contains("topicFilter2"));
            assertTrue(tfs.contains("topicFilter3"));
        } catch (Exception exception) {
            exception.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGCScan() {
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString()));

        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8));
        when(kvIterator.value())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setExpireSeconds(1)
                .build().toByteString());

        try {
            GCReply reply = requestRO(getGCScanInput(10)).getGc();
            verify(kvIterator).seekToFirst();
            assertEquals(reply.getScopedInboxIdCount(), 1);
            assertEquals(reply.getScopedInboxIdList().get(0), ByteString.copyFromUtf8(scopedInboxIdUtf8));
            assertFalse(reply.hasNextScopedInboxId());
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testGCScanWithMore() {
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString()));

        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(scopedInboxId)
            .thenReturn(scopedInboxId(tenantId, "nextInboxId"));

        when(kvIterator.value())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setExpireSeconds(1)
                .build().toByteString())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setExpireSeconds(1)
                .build().toByteString());

        try {
            GCReply reply = requestRO(getGCScanInput(1)).getGc();
            verify(kvIterator).seekToFirst();
            assertEquals(reply.getScopedInboxIdCount(), 1);
            assertEquals(reply.getScopedInboxIdList().get(0), ByteString.copyFromUtf8(scopedInboxIdUtf8));
            assertEquals(reply.getNextScopedInboxId(), scopedInboxId(tenantId, "nextInboxId"));
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testGCScanWithNoExpiredInbox() {
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(1)
                .build().toByteString()));

        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(ByteString.copyFromUtf8(scopedInboxIdUtf8));
        when(kvIterator.value())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis())
                .setExpireSeconds(1)
                .build().toByteString());

        try {
            GCReply reply = requestRO(getGCScanInput(10)).getGc();
            verify(kvIterator).seekToFirst();
            assertEquals(reply.getScopedInboxIdCount(), 0);
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testGCScanWithLimit() {
        String scopedInboxId1 = scopedInboxId(tenantId, "inbox1").toStringUtf8();
        String scopedInboxId2 = scopedInboxId(tenantId, "inbox2").toStringUtf8();
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));

        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 120 * 1000 * 60)
                .setExpireSeconds(1)
                .build().toByteString()));

        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(ByteString.copyFromUtf8(scopedInboxId1))
            .thenReturn(ByteString.copyFromUtf8(scopedInboxId2));
        when(kvIterator.value())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setExpireSeconds(1)
                .build().toByteString());

        try {
            GCReply reply = requestRO(getGCScanInput(1)).getGc();
            verify(kvIterator).seekToFirst();
            assertEquals(reply.getScopedInboxIdCount(), 1);
            assertEquals(reply.getScopedInboxIdList().get(0), ByteString.copyFromUtf8(scopedInboxId1));
        } catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testGCScanWithSpecificTenantId() {
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));
        when(reader.get(any()))
            .thenReturn(Optional.of(InboxMetadata.newBuilder()
                .setLastFetchTime(clock.millis() - 30 * 1000)
                .setExpireSeconds(1)
                .build().toByteString()));

        when(kvIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(kvIterator.key())
            .thenReturn(scopedInboxId)
            .thenReturn(scopedInboxId(tenantId + "_2", "inboxId_2"));

        when(kvIterator.value())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setClient(ClientInfo.newBuilder().setTenantId(tenantId).build())
                .setExpireSeconds(1)
                .build().toByteString())
            .thenReturn(InboxMetadata.newBuilder()
                .setLastFetchTime(lastFetchTime.toMillis())
                .setClient(ClientInfo.newBuilder().setTenantId(tenantId + "_2").build())
                .setExpireSeconds(1)
                .build().toByteString());

        try {
            GCReply reply = requestRO(getGCScanInput(2, tenantId)).getGc();
            verify(kvIterator).seekToFirst();
            assertEquals(reply.getScopedInboxIdCount(), 1);
            assertEquals(reply.getScopedInboxIdList().get(0), ByteString.copyFromUtf8(scopedInboxIdUtf8));
            assertFalse(reply.hasNextScopedInboxId());
        } catch (Exception exception) {
            fail();
        }
    }
}
