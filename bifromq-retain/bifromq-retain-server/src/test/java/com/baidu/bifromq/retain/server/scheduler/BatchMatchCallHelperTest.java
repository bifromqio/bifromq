/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.retain.server.scheduler;

import static com.baidu.bifromq.retain.server.scheduler.BatchMatchCallHelper.parallelMatch;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.basekv.client.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.retain.rpc.proto.MatchError;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.Matched;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchMatchCallHelperTest {
    @Mock
    private BatchMatchCallHelper.IRetainMatcher matcher;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void testParallelMatch() {
        Map<KVRangeSetting, Set<String>> rangeAssignment = new LinkedHashMap<>();
        rangeAssignment.put(rangeSetting("1"), Set.of("topic1", "topic2"));
        rangeAssignment.put(rangeSetting("2"), Set.of("topic3", "topic4"));

        when(matcher.match(anyLong(), anyLong(), any(), any())).thenReturn(new CompletableFuture<>());

        parallelMatch(1L, 2L, rangeAssignment, matcher);

        ArgumentCaptor<Map<String, Integer>> topicFiltersCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<KVRangeSetting> rangeSettingCaptor = ArgumentCaptor.forClass(KVRangeSetting.class);
        verify(matcher, times(2)).match(eq(1L), eq(2L),
            topicFiltersCaptor.capture(), rangeSettingCaptor.capture());
        assertEquals(topicFiltersCaptor.getAllValues().size(), 2);
        assertEquals(topicFiltersCaptor.getAllValues().get(0), Map.of("topic1", 1, "topic2", 1));
        assertEquals(topicFiltersCaptor.getAllValues().get(1), Map.of("topic3", 1, "topic4", 1));
        assertEquals(rangeSettingCaptor.getAllValues().size(), 2);
        assertEquals(rangeSettingCaptor.getAllValues().get(0), rangeSetting("1"));
        assertEquals(rangeSettingCaptor.getAllValues().get(1), rangeSetting("2"));
    }

    @Test
    public void testParallelMatchAggregateResult() {
        Map<KVRangeSetting, Set<String>> rangeAssignment = new LinkedHashMap<>();
        rangeAssignment.put(rangeSetting("1"), Set.of("topic1", "topic2"));
        rangeAssignment.put(rangeSetting("2"), Set.of("topic3", "topic4"));
        Map<String, MatchResult> map1 =
            Map.of("topic1", MatchResult.newBuilder().setOk(Matched.getDefaultInstance()).build(),
                "topic2", MatchResult.newBuilder().setError(MatchError.getDefaultInstance()).build());
        Map<String, MatchResult> map2 =
            Map.of("topic3", MatchResult.newBuilder().setOk(Matched.getDefaultInstance()).build(),
                "topic4", MatchResult.newBuilder().setError(MatchError.getDefaultInstance()).build());
        Map<String, MatchResult> all = new HashMap<>();
        all.putAll(map1);
        all.putAll(map2);

        when(matcher.match(anyLong(), anyLong(), any(), eq(rangeSetting("1"))))
            .thenReturn(CompletableFuture.completedFuture(map1));
        when(matcher.match(anyLong(), anyLong(), any(), eq(rangeSetting("2"))))
            .thenReturn(CompletableFuture.completedFuture(map2));

        Map<String, MatchResult> resultMap = parallelMatch(1L, 2L, rangeAssignment, matcher).join();
        assertEquals(resultMap, all);
    }

    @Test
    public void testSerialMatch() {
        Map<KVRangeSetting, Set<String>> rangeAssignment = new LinkedHashMap<>();
        rangeAssignment.put(rangeSetting("1"), Set.of("topic1", "topic2"));
        rangeAssignment.put(rangeSetting("2"), Set.of("topic1", "topic2"));

        Map<String, MatchResult> response1 = new HashMap<>();
        response1.put("topic1", MatchResult.newBuilder()
            .setOk(Matched.newBuilder()
                .addMessages(TopicMessage.newBuilder().setTopic("topic1")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg1-1"))
                        .build())
                    .build())
                .addMessages(TopicMessage.newBuilder().setTopic("topic1")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg1-2"))
                        .build())
                    .build())
                .build())
            .build());
        response1.put("topic2", MatchResult.newBuilder()
            .setOk(Matched.newBuilder()
                .addMessages(TopicMessage.newBuilder().setTopic("topic2")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg2-1"))
                        .build())
                    .build())
                .build())
            .build());

        Map<String, MatchResult> response2 = new HashMap<>();
        response2.put("topic1", MatchResult.newBuilder()
            .setOk(Matched.newBuilder()
                .addMessages(TopicMessage.newBuilder().setTopic("topic1")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg1-3"))
                        .build())
                    .build())
                .addMessages(TopicMessage.newBuilder().setTopic("topic1")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg1-4"))
                        .build())
                    .build())
                .build())
            .build());
        response2.put("topic2", MatchResult.newBuilder()
            .setOk(Matched.newBuilder()
                .addMessages(TopicMessage.newBuilder().setTopic("topic2")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg2-2"))
                        .build())
                    .build())
                .addMessages(TopicMessage.newBuilder().setTopic("topic2")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg2-3"))
                        .build())
                    .build())
                .addMessages(TopicMessage.newBuilder().setTopic("topic3")
                    .setMessage(Message.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("msg2-4"))
                        .build())
                    .build())
                .build())
            .build());

        when(matcher.match(eq(1L), eq(2L), any(), eq(rangeSetting("1"))))
            .thenReturn(CompletableFuture.completedFuture(response1));
        when(matcher.match(eq(1L), eq(2L), any(), eq(rangeSetting("2"))))
            .thenReturn(CompletableFuture.completedFuture(response2));

        int limit = 3;
        Map<String, MatchResult> result =
            BatchMatchCallHelper.serialMatch(1L, 2L, rangeAssignment, limit, matcher).join();

        verify(matcher, times(1)).match(eq(1L), eq(2L), any(), eq(rangeSetting("1")));
        verify(matcher, times(1)).match(eq(1L), eq(2L), any(), eq(rangeSetting("2")));

        MatchResult topic1Result = result.get("topic1");
        assertEquals(topic1Result.getOk().getMessagesCount(), 3);
        assertEquals(topic1Result.getOk().getMessages(0).getMessage().getPayload().toStringUtf8(), "msg1-1");
        assertEquals(topic1Result.getOk().getMessages(1).getMessage().getPayload().toStringUtf8(), "msg1-2");
        assertEquals(topic1Result.getOk().getMessages(2).getMessage().getPayload().toStringUtf8(), "msg1-3");

        MatchResult topic2Result = result.get("topic2");
        assertEquals(topic2Result.getOk().getMessagesCount(), 3);
        assertEquals(topic2Result.getOk().getMessages(0).getMessage().getPayload().toStringUtf8(), "msg2-1");
        assertEquals(topic2Result.getOk().getMessages(1).getMessage().getPayload().toStringUtf8(), "msg2-2");
        assertEquals(topic2Result.getOk().getMessages(2).getMessage().getPayload().toStringUtf8(), "msg2-3");
    }

    private KVRangeSetting rangeSetting(String storeId) {
        return new KVRangeSetting("clusterId", storeId, KVRangeDescriptor.newBuilder().build());
    }
}
