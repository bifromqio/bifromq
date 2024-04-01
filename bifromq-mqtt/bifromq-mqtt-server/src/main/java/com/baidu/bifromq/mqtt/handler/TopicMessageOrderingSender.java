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

import static com.baidu.bifromq.metrics.TenantMetric.MqttOutOfOrderSendBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttReorderBytes;
import static com.baidu.bifromq.metrics.TenantMetric.MqttTopicSorterAbortCount;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.isSuccessive;
import static com.baidu.bifromq.mqtt.utils.MessageIdUtil.previousMessageId;

import com.baidu.bifromq.metrics.ITenantMeter;
import com.baidu.bifromq.type.ClientInfo;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class TopicMessageOrderingSender {
    public interface MessageSender {
        void send(long inboxSeqNo, MQTTSessionHandler.SubMessage subMessage);
    }

    private final ITenantMeter meter;
    private final long syncWindowIntervalMillis;
    private final EventExecutor executor;
    private final LinkedHashMap<SorterKey, SortingBuffer> sortingBuffers;
    private final MessageSender sender;

    public TopicMessageOrderingSender(MessageSender sender,
                                      EventExecutor executor,
                                      long syncWindowIntervalMillis,
                                      int maxSize,
                                      ITenantMeter meter) {
        this.sender = sender;
        this.executor = executor;
        this.syncWindowIntervalMillis = syncWindowIntervalMillis;
        this.meter = meter;
        sortingBuffers = new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<SorterKey, SortingBuffer> eldest) {
                if (size() > maxSize) {
                    if (!eldest.getValue().isEmpty()) {
                        eldest.getValue().drain();
                        TopicMessageOrderingSender.this.meter.recordCount(MqttTopicSorterAbortCount);
                    }
                    return true;
                }
                return false;
            }
        };
    }

    public boolean submit(long inboxSeqNo, MQTTSessionHandler.SubMessage subMessage) {
        assert executor.inEventLoop();
        if (subMessage.messageId() == 0) {
            // 0 means the message is not ordered
            sender.send(inboxSeqNo, subMessage);
            return true;
        }
        SorterKey key = new SorterKey(subMessage.publisher(), subMessage.topic());
        return sortingBuffers.computeIfAbsent(key,
                k -> new SortingBuffer(this.sender, syncWindowIntervalMillis, executor, subMessage.messageId(),
                    meter))
            .submit(inboxSeqNo, subMessage);
    }

    private record SorterKey(ClientInfo publisher, String topic) {
    }

    private record SortingMessage(long inboxSeqNo, MQTTSessionHandler.SubMessage subMessage) {
    }

    private static class SortingBuffer {
        final MessageSender sender;
        final long syncWindowIntervalMillis;
        final EventExecutor executor;
        final SortedMap<Long, SortingMessage> sortingBuffer = new TreeMap<>();
        final ITenantMeter meter;
        long headMsgId;
        long tailMsgId;
        ScheduledFuture<?> timeout;
        boolean afterDrain;

        SortingBuffer(MessageSender sender,
                      long syncWindowIntervalMillis,
                      EventExecutor executor,
                      long firstMsgId,
                      ITenantMeter meter) {
            this.sender = sender;
            this.syncWindowIntervalMillis = syncWindowIntervalMillis;
            this.executor = executor;
            this.meter = meter;
            // reset to previous sequence
            headMsgId = previousMessageId(firstMsgId);
            tailMsgId = headMsgId;
        }

        boolean isEmpty() {
            return sortingBuffer.isEmpty();
        }

        boolean submit(long inboxSeqNo, MQTTSessionHandler.SubMessage subMessage) {
            assert executor.inEventLoop();
            long msgId = subMessage.messageId();
            boolean success = true;
            if (isSuccessive(tailMsgId, msgId) || afterDrain) {
                if (headMsgId == tailMsgId) {
                    // fast path
                    headMsgId = msgId;
                    tailMsgId = msgId;
                    sender.send(inboxSeqNo, subMessage);
                    afterDrain = false;
                } else {
                    tailMsgId = msgId;
                    meter.recordSummary(MqttReorderBytes, subMessage.estBytes());
                    sortingBuffer.put(msgId, new SortingMessage(inboxSeqNo, subMessage));
                }
            } else if (msgId > tailMsgId) {
                // out of order happens
                meter.recordSummary(MqttReorderBytes, subMessage.estBytes());
                sortingBuffer.put(msgId, new SortingMessage(inboxSeqNo, subMessage));
                tailMsgId = msgId;
                if (timeout == null) {
                    timeout = executor.schedule(this::drain, syncWindowIntervalMillis, TimeUnit.MILLISECONDS);
                }
            } else {
                // tailMsgSeq <= msgSeq
                if (msgId <= headMsgId) {
                    // headMsgSeq <= msgSeq
                    success = false;
                } else if (isSuccessive(headMsgId, msgId)) {
                    // insert the message into the buffer and send the message
                    sortingBuffer.put(msgId, new SortingMessage(inboxSeqNo, subMessage));
                    send(false);
                } else if (msgId < tailMsgId) {
                    // tailMsgSeq < msgSeq < headMsgSeq + 1
                    sortingBuffer.put(msgId, new SortingMessage(inboxSeqNo, subMessage));
                }
            }
            return success;
        }

        void drain() {
            assert executor.inEventLoop();
            long oodSentBytes = send(true);
            meter.recordSummary(MqttOutOfOrderSendBytes, oodSentBytes);
            afterDrain = true;
        }

        private long send(boolean drain) {
            assert executor.inEventLoop();
            // cancel timeout task
            if (timeout != null && !timeout.isDone()) {
                timeout.cancel(false);
                timeout = null;
            }
            long oodBytes = 0;
            Iterator<Map.Entry<Long, SortingMessage>> entryIterator = sortingBuffer.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<Long, SortingMessage> entry = entryIterator.next();
                boolean isSuccessive = isSuccessive(headMsgId, entry.getKey());
                if (isSuccessive || drain) {
                    headMsgId = entry.getKey();
                    SortingMessage subMessage = entry.getValue();
                    if (!isSuccessive) {
                        oodBytes += subMessage.subMessage().estBytes();
                    }
                    sender.send(subMessage.inboxSeqNo(), subMessage.subMessage());
                    entryIterator.remove();
                } else {
                    break;
                }
            }
            if (!sortingBuffer.isEmpty()) {
                timeout = executor.schedule(this::drain, syncWindowIntervalMillis, TimeUnit.MILLISECONDS);
            } else {
                // the invariant must be hold
                assert headMsgId == tailMsgId;
            }
            return oodBytes;
        }
    }
}
