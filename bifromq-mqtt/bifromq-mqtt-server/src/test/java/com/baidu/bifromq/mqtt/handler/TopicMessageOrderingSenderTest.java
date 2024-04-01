/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageId;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.syncWindowSequence;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.inbox.storage.proto.TopicFilterOption;
import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.mqtt.MockableTest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicMessageOrderingSenderTest extends MockableTest {
    @Mock
    private TopicMessageOrderingSender.MessageSender sender;

    @Mock
    private EventExecutor executor;
    @Mock
    private ScheduledFuture timeoutFuture;

    @Mock
    private ITenantMeter meter;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setup(Method method) {
        super.setup(method);
        when(executor.inEventLoop()).thenReturn(true);
    }

    @Test
    public void testByPassOrdering() {
        TopicMessageOrderingSender orderingSender = new TopicMessageOrderingSender(sender, executor, 1000, 100, meter);
        MQTTSessionHandler.SubMessage subMessage = mockSubMessage(0, "publisher");
        orderingSender.submit(1, subMessage);
        verify(sender).send(1, subMessage);
    }

    @Test
    public void testFastPath() {
        TopicMessageOrderingSender orderingSender = new TopicMessageOrderingSender(sender, executor, 1000, 100, meter);
        long syncWinSeq = 10;
        MQTTSessionHandler.SubMessage subMessage = mockSubMessage(messageId(syncWinSeq, 1), "publisher");
        orderingSender.submit(1, subMessage);
        verify(sender).send(1, subMessage);

        MQTTSessionHandler.SubMessage subMessage1 = mockSubMessage(messageId(syncWinSeq, 2), "publisher");
        orderingSender.submit(2, subMessage1);
        verify(sender).send(2, subMessage1);

        MQTTSessionHandler.SubMessage subMessage2 = mockSubMessage(messageId(syncWinSeq + 1, 3), "publisher");
        orderingSender.submit(3, subMessage2);
        verify(sender).send(3, subMessage2);

        MQTTSessionHandler.SubMessage subMessage3 = mockSubMessage(messageId(syncWinSeq + 3, 0), "publisher");
        orderingSender.submit(4, subMessage3);
        verify(sender).send(4, subMessage3);
    }

    @Test
    public void testDropOldMessage() {
        TopicMessageOrderingSender orderingSender = new TopicMessageOrderingSender(sender, executor, 1000, 100, meter);
        long syncWinSeq = 10;
        long msgSeq = 0;
        MQTTSessionHandler.SubMessage subMessage = mockSubMessage(messageId(syncWinSeq, msgSeq), "publisher");
        orderingSender.submit(1, subMessage);
        orderingSender.submit(1, subMessage);
        verify(sender, only()).send(1, subMessage);
    }

    @Test
    public void testDropObsoleteMessage() {
        TopicMessageOrderingSender orderingSender = new TopicMessageOrderingSender(sender, executor, 1000, 100, meter);
        long syncWinSeq = 10;
        long msgSeq = 0;
        MQTTSessionHandler.SubMessage subMessage = mockSubMessage(messageId(syncWinSeq, msgSeq), "publisher");
        orderingSender.submit(1, subMessage);

        syncWinSeq = 9;

        MQTTSessionHandler.SubMessage subMessage1 = mockSubMessage(messageId(syncWinSeq, msgSeq), "publisher");
        orderingSender.submit(1, subMessage1);
        verify(sender, only()).send(1, subMessage);

    }

    @Test
    public void testTimeoutTaskScheduled() {
        long syncWindowIntervalMillis = 1000;
        long nowMillis = 1010;
        long syncWinSeq = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 100, meter);
        MQTTSessionHandler.SubMessage subMessage =
            mockSubMessage(messageId(syncWinSeq, 0), "publisher", nowMillis);
        orderingSender.submit(1, subMessage);
        verify(sender).send(1, subMessage);

        MQTTSessionHandler.SubMessage subMessage1 =
            mockSubMessage(messageId(syncWinSeq, 2), "publisher", nowMillis);
        orderingSender.submit(2, subMessage1);

        verify(executor).schedule(any(Runnable.class), eq(syncWindowIntervalMillis), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testOutOfOrder() {
        long syncWindowIntervalMillis = 1000;
        long nowMillis = 1010;
        long syncWinSeq = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
        long msgSeq = 1;
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 100, meter);
        MQTTSessionHandler.SubMessage subMessage =
            mockSubMessage(messageId(syncWinSeq, msgSeq), "publisher", nowMillis);
        orderingSender.submit(1, subMessage);

        msgSeq = 3;
        MQTTSessionHandler.SubMessage subMessage1 =
            mockSubMessage(messageId(syncWinSeq, msgSeq), "publisher", nowMillis);
        orderingSender.submit(2, subMessage1);
        verify(sender, only()).send(1, subMessage);
    }

    @Test
    public void testReorderAndSendAll() {
        long syncWindowIntervalMillis = 1000;
        long nowMillis = 1010;
        long syncWinSeq = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
        when(executor.schedule(any(Runnable.class), anyLong(), any())).thenReturn(timeoutFuture);
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 100, meter);
        MQTTSessionHandler.SubMessage subMessage0 =
            mockSubMessage(messageId(syncWinSeq, 0), "publisher", nowMillis);
        orderingSender.submit(1, subMessage0);

        MQTTSessionHandler.SubMessage subMessage2 =
            mockSubMessage(messageId(syncWinSeq, 2), "publisher", nowMillis);
        orderingSender.submit(2, subMessage2);

        MQTTSessionHandler.SubMessage subMessage1 =
            mockSubMessage(messageId(syncWinSeq, 1), "publisher", nowMillis);
        orderingSender.submit(2, subMessage1);

        ArgumentCaptor<MQTTSessionHandler.SubMessage> msgCaptor =
            ArgumentCaptor.forClass(MQTTSessionHandler.SubMessage.class);
        verify(sender, times(3)).send(anyLong(), msgCaptor.capture());
        // the timeout task should be canceled
        verify(timeoutFuture).cancel(false);

        List<MQTTSessionHandler.SubMessage> subMessageList = msgCaptor.getAllValues();
        assertEquals(subMessageList.get(0), subMessage0);
        assertEquals(subMessageList.get(1), subMessage1);
        assertEquals(subMessageList.get(2), subMessage2);
    }

    @Test
    public void testReorderAndSendPartial() {
        long syncWindowIntervalMillis = 1000;
        long nowMillis = 1010;
        long syncWinSeq = syncWindowSequence(nowMillis, syncWindowIntervalMillis);
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 100, meter);
        when(executor.schedule(any(Runnable.class), anyLong(), any())).thenReturn(timeoutFuture);

        MQTTSessionHandler.SubMessage subMessage0 =
            mockSubMessage(messageId(syncWinSeq, 0), "publisher", nowMillis);
        MQTTSessionHandler.SubMessage subMessage1 =
            mockSubMessage(messageId(syncWinSeq, 1), "publisher", nowMillis);
        MQTTSessionHandler.SubMessage subMessage2 =
            mockSubMessage(messageId(syncWinSeq, 2), "publisher", nowMillis);
        MQTTSessionHandler.SubMessage subMessage3 =
            mockSubMessage(messageId(syncWinSeq, 3), "publisher", nowMillis);
        MQTTSessionHandler.SubMessage subMessage4 =
            mockSubMessage(messageId(syncWinSeq, 4), "publisher", nowMillis);

        orderingSender.submit(0, subMessage0);
        orderingSender.submit(2, subMessage2);
        orderingSender.submit(4, subMessage4);


        orderingSender.submit(1, subMessage1);
        ArgumentCaptor<MQTTSessionHandler.SubMessage> msgCaptor =
            ArgumentCaptor.forClass(MQTTSessionHandler.SubMessage.class);
        verify(sender, times(3)).send(anyLong(), msgCaptor.capture());
        // the timeout task should not be canceled
        verify(timeoutFuture).cancel(false);
        verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), any());
        List<MQTTSessionHandler.SubMessage> subMessageList = msgCaptor.getAllValues();
        assertEquals(subMessageList.get(0), subMessage0);
        assertEquals(subMessageList.get(1), subMessage1);
        assertEquals(subMessageList.get(2), subMessage2);

        Mockito.reset(sender);
        Mockito.reset(timeoutFuture);

        orderingSender.submit(3, subMessage3);
        msgCaptor = ArgumentCaptor.forClass(MQTTSessionHandler.SubMessage.class);
        verify(sender, times(2)).send(anyLong(), msgCaptor.capture());
        subMessageList = msgCaptor.getAllValues();
        assertEquals(subMessageList.get(0), subMessage3);
        assertEquals(subMessageList.get(1), subMessage4);
        verify(timeoutFuture).cancel(false);
    }

    @Test
    public void testForceDrain() {
        long syncWindowIntervalMillis = 1000;
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 1, meter);
        when(executor.schedule(any(Runnable.class), anyLong(), any())).thenReturn(timeoutFuture);

        MQTTSessionHandler.SubMessage subMessage1_1 =
            mockSubMessage("topic1", messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 1), "publisher",
                1010);
        MQTTSessionHandler.SubMessage subMessage1_3 =
            mockSubMessage("topic1", messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 3), "publisher",
                1010);

        MQTTSessionHandler.SubMessage subMessage2_0 =
            mockSubMessage("topic2", messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 0), "publisher",
                1010);

        orderingSender.submit(1, subMessage1_1);
        orderingSender.submit(2, subMessage1_3);
        orderingSender.submit(1, subMessage2_0);

        ArgumentCaptor<MQTTSessionHandler.SubMessage> msgCaptor =
            ArgumentCaptor.forClass(MQTTSessionHandler.SubMessage.class);

        verify(sender, times(3)).send(anyLong(), msgCaptor.capture());
        List<MQTTSessionHandler.SubMessage> subMessageList = msgCaptor.getAllValues();
        assertEquals(subMessageList.get(0), subMessage1_1);
        assertEquals(subMessageList.get(1), subMessage1_3);
        assertEquals(subMessageList.get(2), subMessage2_0);
        verify(timeoutFuture).cancel(false);
        verify(meter, times(1)).recordSummary(eq(TenantMetric.MqttReorderBytes), anyDouble());
        verify(meter, times(1)).recordSummary(eq(TenantMetric.MqttOutOfOrderSendBytes), anyDouble());

        verify(meter).recordCount(eq(TenantMetric.MqttTopicSorterAbortCount));
    }

    @Test
    public void testTimeoutDrain() {
        long syncWindowIntervalMillis = 1000;
        TopicMessageOrderingSender orderingSender =
            new TopicMessageOrderingSender(sender, executor, syncWindowIntervalMillis, 100, meter);
        when(executor.schedule(any(Runnable.class), anyLong(), any())).thenReturn(timeoutFuture);

        MQTTSessionHandler.SubMessage subMessage1_0 =
            mockSubMessage(messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 0), "publisher", 1010);
        orderingSender.submit(0, subMessage1_0);
        verify(sender).send(anyLong(), eq(subMessage1_0));

        MQTTSessionHandler.SubMessage subMessage1_2 =
            mockSubMessage(messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 2), "publisher", 1010);
        orderingSender.submit(1, subMessage1_2);

        MQTTSessionHandler.SubMessage subMessage1_4 =
            mockSubMessage(messageId(syncWindowSequence(1010, syncWindowIntervalMillis), 4), "publisher", 1010);
        orderingSender.submit(4, subMessage1_4);

        ArgumentCaptor<Runnable> timeoutTaskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).schedule(timeoutTaskCaptor.capture(), eq(syncWindowIntervalMillis), eq(TimeUnit.MILLISECONDS));

        // trigger run
        timeoutTaskCaptor.getValue().run();

        ArgumentCaptor<MQTTSessionHandler.SubMessage> subMsgCaptor =
            ArgumentCaptor.forClass(MQTTSessionHandler.SubMessage.class);
        verify(sender, times(3)).send(anyLong(), subMsgCaptor.capture());


        assertEquals(subMsgCaptor.getAllValues(), List.of(subMessage1_0, subMessage1_2, subMessage1_4));
        verify(meter, times(2)).recordSummary(eq(TenantMetric.MqttReorderBytes), anyDouble());
        verify(meter).recordSummary(eq(TenantMetric.MqttOutOfOrderSendBytes), anyDouble());
    }

    private MQTTSessionHandler.SubMessage mockSubMessage(long messageId, String publisher) {
        return mockSubMessage(messageId, publisher, System.currentTimeMillis());
    }

    private MQTTSessionHandler.SubMessage mockSubMessage(long messageId, String publisher,
                                                         long msgTimestamp) {
        return mockSubMessage("topic", messageId, publisher, msgTimestamp);
    }

    private MQTTSessionHandler.SubMessage mockSubMessage(String topic, long messageId, String publisher,
                                                         long msgTimestamp) {
        return new MQTTSessionHandler.SubMessage(topic,
            Message.newBuilder()
                .setMessageId(messageId)
                .setTimestamp(msgTimestamp)
                .build(), ClientInfo.newBuilder()
            .setTenantId(publisher)
            .build(), topic, TopicFilterOption.getDefaultInstance());
    }
}
