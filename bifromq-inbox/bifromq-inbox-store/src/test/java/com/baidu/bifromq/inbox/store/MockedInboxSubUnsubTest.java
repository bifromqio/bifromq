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

import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.InboxMetadata;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.fail;

public class MockedInboxSubUnsubTest extends MockedInboxStoreTest {
    private String topicFilter = "test/#";
    // sub test section
    @Test
    public void testSubWithNoInbox() {
        when(reader.get(scopedInboxId)).thenReturn(Optional.empty());

        try {
            BatchSubReply reply = requestRW(getSubInput(new HashMap<>() {{
                put(scopedInboxIdUtf8 + topicFilter, QoS.AT_MOST_ONCE);
            }})).getBatchSub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchSubReply.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testSubWithExpirationInbox() {
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(lastFetchTime.toMillis())
                        .setExpireSeconds(1)
                        .build().toByteString()));
        try {
            BatchSubReply reply = requestRW(getSubInput(new HashMap<>() {{
                put(scopedInboxIdUtf8 + topicFilter, QoS.AT_MOST_ONCE);
            }})).getBatchSub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchSubReply.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testSubWithExceedingLimit() {
        Map<String, QoS> map = new HashMap<>();
        for (int index = 0; index < (int)Setting.MaxTopicFiltersPerInbox.current(scopedInboxIdUtf8); index++) {
            map.put(scopedInboxIdUtf8 + index + "-" + topicFilter, QoS.AT_MOST_ONCE);
        }
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .putAllTopicFilters(map)
                        .setExpireSeconds(1)
                        .build().toByteString()));

        try {
            BatchSubReply reply = requestRW(getSubInput(new HashMap<>() {{
                put(scopedInboxIdUtf8 + 100 + "-" + topicFilter, QoS.AT_MOST_ONCE);
            }})).getBatchSub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + 100 + "-" + topicFilter),
                    BatchSubReply.Result.EXCEED_LIMIT);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testSubWithOK() {
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .setExpireSeconds(1)
                        .build().toByteString()));

        try {
            BatchSubReply reply = requestRW(getSubInput(new HashMap<>() {{
                put(scopedInboxIdUtf8 + topicFilter, QoS.AT_MOST_ONCE);
            }})).getBatchSub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchSubReply.Result.OK);
        }catch (Exception exception) {
            fail();
        }
    }
    //unsub test section
    @Test
    public void testUnsubWithNoInbox() {
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.empty());

        try {
            BatchUnsubReply reply = requestRW(getUnsubInput(ByteString
                    .copyFromUtf8(scopedInboxIdUtf8 + topicFilter))).getBatchUnsub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchUnsubReply.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testUnsubWithExpirationInbox() {
        Duration lastFetchTime = Duration.ofMillis(clock.millis()).minus(Duration.ofHours(3));
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(lastFetchTime.toMillis())
                        .setExpireSeconds(1)
                        .build().toByteString()));
        try {
            BatchUnsubReply reply = requestRW(getUnsubInput(ByteString
                    .copyFromUtf8(scopedInboxIdUtf8 + topicFilter))).getBatchUnsub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchUnsubReply.Result.NO_INBOX);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testUnsubWithNoSub() {
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .putTopicFilters(1 + "-" + topicFilter, QoS.AT_MOST_ONCE)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        try {
            BatchUnsubReply reply = requestRW(getUnsubInput(ByteString
                    .copyFromUtf8(scopedInboxIdUtf8 + topicFilter))).getBatchUnsub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchUnsubReply.Result.NO_SUB);
        }catch (Exception exception) {
            fail();
        }
    }

    @Test
    public void testUnsubOK() {
        when(reader.get(scopedInboxId))
                .thenReturn(Optional.of(InboxMetadata.newBuilder()
                        .setLastFetchTime(clock.millis())
                        .putTopicFilters(topicFilter, QoS.AT_MOST_ONCE)
                        .setExpireSeconds(Integer.MAX_VALUE)
                        .build().toByteString()));
        try {
            BatchUnsubReply reply = requestRW(getUnsubInput(ByteString
                    .copyFromUtf8(scopedInboxIdUtf8 + topicFilter))).getBatchUnsub();
            Assert.assertEquals(reply.getResultsMap().get(scopedInboxIdUtf8 + topicFilter),
                    BatchUnsubReply.Result.OK);
        }catch (Exception exception) {
            fail();
        }
    }
}
