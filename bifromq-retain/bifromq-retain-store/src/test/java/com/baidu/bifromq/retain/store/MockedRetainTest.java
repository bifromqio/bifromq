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

package com.baidu.bifromq.retain.store;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.api.IKVWriter;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.*;
import com.baidu.bifromq.retain.utils.KeyUtil;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.TopicMessage;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainedTopicLimit;
import static com.baidu.bifromq.retain.utils.KeyUtil.retainKey;
import static com.baidu.bifromq.retain.utils.KeyUtil.tenantNS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MockedRetainTest {
    @Mock
    private IKVReader reader;
    @Mock
    private IKVWriter writer;
    @Mock
    protected IKVIterator kvIterator;
    private KVRangeId id = KVRangeIdUtil.generate();
    private Supplier<IKVReader> rangeReaderProvider = () -> null;
    private Clock clock = Clock.systemUTC();
    private ISettingProvider settingProvider = Setting::current;
    private String tenantId = "testTenantId";
    private ByteString tenantNS = tenantNS(tenantId);
    private String nonWildcardTopicFilter = "a/b/c";
    private String wildcardTopicFilter = "a/#";
    private Message message = Message.newBuilder()
            .setPayload(ByteString.copyFromUtf8("payload"))
            .setExpireTimestamp(clock.millis() + Duration.ofDays(7).toMillis())
            .build();
    private TopicMessage topicMessage = TopicMessage.newBuilder()
            .setTopic(nonWildcardTopicFilter)
            .build();
    private RetainStoreCoProc coProc;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(reader.iterator()).thenReturn(kvIterator);
        coProc = new RetainStoreCoProc(id, rangeReaderProvider, settingProvider, clock);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void testNonWildcardTopic() {
        Map<String, MatchParam> map = new HashMap<>();
        Map<String, Integer> topicFilterMap = new HashMap<>();
        topicFilterMap.put(nonWildcardTopicFilter, 1);
        MatchParam param = MatchParam.newBuilder()
                .putAllTopicFilters(topicFilterMap)
                .build();
        map.put(tenantId, param);
        ROCoProcInput input = getROInput(map);
        TopicMessage nonExpiredTopicMessage = topicMessage.toBuilder().setMessage(message).build();

        when(kvIterator.isValid())
                .thenReturn(true);
        when(reader.get(any()))
                .thenReturn(Optional.of(nonExpiredTopicMessage.toByteString()));

        ROCoProcOutput output = coProc.query(input, reader).join();
        Assert.assertTrue(output.hasRetainService());
        RetainServiceROCoProcOutput retainOutput = output.getRetainService();
        MatchResult result = retainOutput.getBatchMatch().getResultPackMap()
                .get(tenantId).getResultsMap().get(nonWildcardTopicFilter);
        Assert.assertEquals(result.getOk().getMessagesCount(), 1);
        Assert.assertEquals(result.getOk().getMessagesList().get(0), nonExpiredTopicMessage);
    }

    @Test
    public void testNonWildcardTopicWithExpiration() {
        Map<String, MatchParam> map = new HashMap<>();
        Map<String, Integer> topicFilterMap = new HashMap<>();
        topicFilterMap.put(nonWildcardTopicFilter, 1);
        MatchParam param = MatchParam.newBuilder()
                .putAllTopicFilters(topicFilterMap)
                .build();
        map.put(tenantId, param);
        ROCoProcInput input = getROInput(map);

        when(kvIterator.isValid())
                .thenReturn(true);
        when(reader.get(any()))
                .thenReturn(Optional.of(topicMessage.toByteString()));

        ROCoProcOutput output = coProc.query(input, reader).join();
        Assert.assertTrue(output.hasRetainService());
        RetainServiceROCoProcOutput retainOutput = output.getRetainService();
        MatchResult result = retainOutput.getBatchMatch().getResultPackMap()
                .get(tenantId).getResultsMap().get(nonWildcardTopicFilter);
        Assert.assertEquals(result.getOk().getMessagesCount(), 0);
    }

    @Test
    public void testNonWildcardTopicExceedLimit() {
        Map<String, MatchParam> map = new HashMap<>();
        Map<String, Integer> topicFilterMap = new HashMap<>();
        topicFilterMap.put(nonWildcardTopicFilter, 0);
        MatchParam param = MatchParam.newBuilder()
                .putAllTopicFilters(topicFilterMap)
                .build();
        map.put(tenantId, param);
        ROCoProcInput input = getROInput(map);

        when(kvIterator.isValid())
                .thenReturn(true);
        when(reader.get(any()))
                .thenReturn(Optional.of(topicMessage.toBuilder().setMessage(message).build().toByteString()));

        ROCoProcOutput output = coProc.query(input, reader).join();
        Assert.assertTrue(output.hasRetainService());
        RetainServiceROCoProcOutput retainOutput = output.getRetainService();
        MatchResult result = retainOutput.getBatchMatch().getResultPackMap()
                .get(tenantId).getResultsMap().get(nonWildcardTopicFilter);
        Assert.assertEquals(result.getOk().getMessagesCount(), 0);
    }

    @Test
    public void testWildcardTopicFilter() {
        Map<String, MatchParam> map = new HashMap<>();
        Map<String, Integer> topicFilterMap = new HashMap<>();
        topicFilterMap.put(wildcardTopicFilter, 2);
        MatchParam param = MatchParam.newBuilder()
                .putAllTopicFilters(topicFilterMap)
                .build();
        map.put(tenantId, param);
        ROCoProcInput input = getROInput(map);

        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true);
        when(kvIterator.key())
                .thenReturn(retainKey(tenantNS, "a/b/c"))
                .thenReturn(retainKey(tenantNS, "a/b/d"));
        when(kvIterator.value())
                .thenReturn(topicMessage.toBuilder().setTopic("a/b/c").setMessage(message)
                        .build().toByteString())
                .thenReturn(topicMessage.toBuilder().setTopic("a/b/d").setMessage(message)
                        .build().toByteString());

        ROCoProcOutput output = coProc.query(input, reader).join();
        Assert.assertTrue(output.hasRetainService());
        RetainServiceROCoProcOutput retainOutput = output.getRetainService();
        MatchResult result = retainOutput.getBatchMatch().getResultPackMap()
                .get(tenantId).getResultsMap().get(wildcardTopicFilter);
        Assert.assertEquals(result.getOk().getMessagesCount(), 2);
        Assert.assertEquals(result.getOk().getMessagesList().get(0).getTopic(), "a/b/c");
        Assert.assertEquals(result.getOk().getMessagesList().get(1).getTopic(), "a/b/d");
    }

    @Test
    public void testPutRetainMessageWithExpiration() {
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.getDefaultInstance());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.ERROR);
    }

    @Test
    public void testPutRetainMessageWithEmptyMetaData() {
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.RETAINED);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.size(), 4);
        Assert.assertEquals(args.get(0), tenantNS);
        Assert.assertEquals(args.get(2), KeyUtil.retainKey(tenantNS, nonWildcardTopicFilter));
    }

    @Test
    public void testPutRetainMessageNormally() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setEstExpire(clock.millis() + Duration.ofDays(7).toMillis())
                        .build().toByteString()));
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.RETAINED);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.size(), 4);
        Assert.assertEquals(args.get(0), KeyUtil.retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(2), tenantNS);
    }

    @Test
    public void testPutRetainMessageViaGC() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setCount(10)
                        .setEstExpire(clock.millis() - Duration.ofDays(7).toMillis())
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(false);
        when(kvIterator.key())
                .thenReturn(retainKey(tenantNS, "a/b/c"));
        when(kvIterator.value())
                .thenReturn(TopicMessage.getDefaultInstance().toByteString());
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).delete(argumentCaptor.capture());
        verify(writer, times(2)).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.RETAINED);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.size(), 5);
        Assert.assertEquals(args.get(0), KeyUtil.retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(1), KeyUtil.retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(3), tenantNS);
    }

    @Test
    public void testPutRetainMessageWithoutEnoughRoom() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setCount(10)
                        .setEstExpire(clock.millis() - Duration.ofDays(7).toMillis())
                        .build().toByteString()));
        when(kvIterator.isValid())
                .thenReturn(false);
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.ERROR);
    }

    @Test
    public void testDeleteRetainMessageWithExpiration() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .build().toByteString()));
        when(reader.get(retainKey(tenantNS, nonWildcardTopicFilter)))
                .thenReturn(Optional.of(topicMessage.toByteString()));

        Message emptyMsg = Message.newBuilder()
                .setExpireTimestamp(clock.millis() + Duration.ofDays(7).toMillis())
                .build();
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(emptyMsg).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.CLEARED);
    }

    @Test
    public void testDeleteRetainMessageWithoutExpiration() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .build().toByteString()));
        when(reader.get(retainKey(tenantNS, nonWildcardTopicFilter)))
                .thenReturn(Optional.of(topicMessage.toBuilder()
                        .setMessage(message).build().toByteString()));

        Message emptyMsg = Message.newBuilder()
                .setExpireTimestamp(clock.millis() + Duration.ofDays(7).toMillis())
                .build();
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(emptyMsg).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).delete(argumentCaptor.capture());
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.get(0), retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(1), tenantNS);
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.CLEARED);

    }

    @Test
    public void testUpdateRetainMessageNormally() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setEstExpire(clock.millis() + Duration.ofDays(7).toMillis())
                        .build().toByteString()));
        when(reader.get(retainKey(tenantNS, nonWildcardTopicFilter)))
                .thenReturn(Optional.of(topicMessage.toBuilder()
                        .setMessage(message).build().toByteString()));
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer, times(2)).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.RETAINED);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.size(), 4);
        Assert.assertEquals(args.get(0), retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(2), tenantNS);
    }

    @Test
    public void testUpdateRetainMessageWithExpiration() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setEstExpire(clock.millis())
                        .setCount(Integer.MAX_VALUE)
                        .build().toByteString()));
        when(reader.get(retainKey(tenantNS, nonWildcardTopicFilter)))
                .thenReturn(Optional.of(topicMessage.toByteString()));
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.ERROR);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.get(0), tenantNS);
    }

    @Test
    public void testUpdateRetainMessageViaGC() {
        when(reader.get(tenantNS))
                .thenReturn(Optional.of(RetainSetMetadata.newBuilder()
                        .setEstExpire(clock.millis())
                        .setCount(settingProvider.provide(RetainedTopicLimit, tenantId))
                        .build().toByteString()));
        when(reader.get(retainKey(tenantNS, nonWildcardTopicFilter)))
                .thenReturn(Optional.of(topicMessage.toByteString()));
        when(kvIterator.isValid())
                .thenReturn(true)
                .thenReturn(false);
        when(kvIterator.key())
                .thenReturn(retainKey(tenantNS, nonWildcardTopicFilter));
        when(kvIterator.value())
                .thenReturn(TopicMessage.getDefaultInstance().toByteString());
        Map<String, RetainMessage> topicMessages = new HashMap<>() {{
            put(nonWildcardTopicFilter, RetainMessage.newBuilder().setMessage(message).build());
        }};
        Map<String, RetainMessagePack> packs = new HashMap<>() {{
            put(tenantId, RetainMessagePack.newBuilder().putAllTopicMessages(topicMessages).build());
        }};
        RWCoProcInput input = getRWInput(packs);

        RWCoProcOutput output = coProc.mutate(input, reader, writer).get();
        ArgumentCaptor<ByteString> argumentCaptor = ArgumentCaptor.forClass(ByteString.class);
        verify(writer).delete(argumentCaptor.capture());
        verify(writer, times(2)).put(argumentCaptor.capture(), argumentCaptor.capture());
        Assert.assertTrue(output.hasRetainService());
        Assert.assertEquals(output.getRetainService().getBatchRetain()
                .getResultsMap().get(tenantId).getResultsMap().get(nonWildcardTopicFilter), RetainResult.RETAINED);
        List<ByteString> args = argumentCaptor.getAllValues();
        Assert.assertEquals(args.size(), 5);
        Assert.assertEquals(args.get(0), retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(1), retainKey(tenantNS, nonWildcardTopicFilter));
        Assert.assertEquals(args.get(3), tenantNS);
    }

    private ROCoProcInput getROInput(Map<String, MatchParam> map) {
        ROCoProcInput.Builder builder = ROCoProcInput.newBuilder();
        builder.setRetainService(RetainServiceROCoProcInput.newBuilder()
                .setBatchMatch(BatchMatchRequest.newBuilder()
                        .putAllMatchParams(map)
                        .build())
                .build());
        return builder.build();
    }

    private RWCoProcInput getRWInput(Map<String, RetainMessagePack> packs) {
        RWCoProcInput.Builder builder = RWCoProcInput.newBuilder();
        builder.setRetainService(RetainServiceRWCoProcInput.newBuilder()
                        .setBatchRetain(BatchRetainRequest.newBuilder()
                                .setReqId(System.nanoTime())
                                .putAllRetainMessagePack(packs)
                                .build())
                .build());
        return builder.build();
    }
}
