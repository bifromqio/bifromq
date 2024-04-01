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

import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.messageSequence;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.syncWindowSequence;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.metrics.TenantMetric;
import com.baidu.bifromq.mqtt.MockableTest;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class TopicMessageIdGeneratorTest extends MockableTest {
    @Mock
    private ITenantMeter meter;

    @SneakyThrows
    @Test
    public void testNextMessageIdInSameWindow() {
        TopicMessageIdGenerator generator = new TopicMessageIdGenerator(Duration.ofSeconds(1), 1, meter);

        long messageId = generator.nextMessageId("test", 0L);
        long swseq = syncWindowSequence(messageId);
        long mseq = messageSequence(messageId);
        assertEquals(mseq, 0);

        long messageId1 = generator.nextMessageId("test", 0L);
        long swseq1 = syncWindowSequence(messageId1);
        long mseq1 = messageSequence(messageId1);

        assertEquals(swseq, swseq1);
        assertEquals(mseq + 1, mseq1);
    }

    @Test
    public void testNextMessageIdInSuccessiveWindow() {
        TopicMessageIdGenerator generator = new TopicMessageIdGenerator(Duration.ofSeconds(1), 1, meter);

        long messageId = generator.nextMessageId("test", 0L);
        long swseq = syncWindowSequence(messageId);
        long mseq = messageSequence(messageId);
        assertEquals(mseq, 0);

        long messageId1 = generator.nextMessageId("test", 1000L);
        long swseq1 = syncWindowSequence(messageId1);
        long mseq1 = messageSequence(messageId1);

        assertEquals(swseq + 1, swseq1);
        assertEquals(1, mseq1);
    }

    @Test
    public void testNextMessageIdInGapWindow() {
        TopicMessageIdGenerator generator = new TopicMessageIdGenerator(Duration.ofSeconds(1), 1, meter);

        long messageId = generator.nextMessageId("test", 0L);
        long swseq = syncWindowSequence(messageId);
        long mseq = messageSequence(messageId);
        assertEquals(mseq, 0);

        long messageId1 = generator.nextMessageId("test", 2000L);
        long swseq1 = syncWindowSequence(messageId1);
        long mseq1 = messageSequence(messageId1);

        assertEquals(swseq + 2, swseq1);
        assertEquals(0, mseq1);
    }

    @Test
    public void testPrematureEviction() {
        TopicMessageIdGenerator generator = new TopicMessageIdGenerator(Duration.ofSeconds(1), 1, meter);
        generator.nextMessageId("test", 0L);
        long messageId = generator.nextMessageId("test", 0L);
        long swseq = syncWindowSequence(messageId);
        long mseq = messageSequence(messageId);
        assertEquals(mseq, 1);

        long messageId1 = generator.nextMessageId("test1", 0L);
        long swseq1 = syncWindowSequence(messageId1);
        long mseq1 = messageSequence(messageId1);
        assertEquals(swseq, swseq1);
        assertEquals(mseq1, 0);

        long messageId2 = generator.nextMessageId("test", 1000L);
        long swseq2 = syncWindowSequence(messageId2);
        long mseq2 = messageSequence(messageId2);
        assertEquals(swseq2, swseq + 1);
        assertEquals(mseq2, 0);
        verify(meter).recordCount(TenantMetric.MqttTopicSeqAbortCount);
    }
}
