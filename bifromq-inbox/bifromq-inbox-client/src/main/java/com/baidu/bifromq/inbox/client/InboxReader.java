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

package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxReader implements IInboxClient.IInboxReader {
    private final String inboxId;
    private final long incarnation;
    private final InboxFetchPipeline ppln;
    private final long sessionId = HLC.INST.get();
    private int latestBufferCapacity = 100;
    private volatile long lastFetchQoS0Seq = -1;
    private volatile long lastFetchQoS1Seq = -1;
    private volatile long lastFetchQoS2Seq = -1;

    public InboxReader(String inboxId,
                       long incarnation,
                       InboxFetchPipeline ppln) {
        this.inboxId = inboxId;
        this.incarnation = incarnation;
        this.ppln = ppln;
    }

    @Override
    public void fetch(Consumer<Fetched> consumer) {
        ppln.fetch(sessionId, inboxId, incarnation, (fetched) -> {
            if (fetched.getResult() == Fetched.Result.OK) {
                if (fetched.getQos0SeqCount() > 0) {
                    lastFetchQoS0Seq = fetched.getQos0Seq(fetched.getQos0SeqCount() - 1);
                }
                if (fetched.getQos1MsgCount() > 0) {
                    lastFetchQoS1Seq = fetched.getQos1Seq(fetched.getQos1MsgCount() - 1);
                }
                if (fetched.getQos2SeqCount() > 0) {
                    lastFetchQoS2Seq = fetched.getQos2Seq(fetched.getQos2SeqCount() - 1);
                }
            }
            consumer.accept(fetched);
        });
    }

    @Override
    public void hint(int bufferCapacity) {
        latestBufferCapacity = bufferCapacity;
        try {
            ppln.hint(sessionId, inboxId, incarnation,
                bufferCapacity, lastFetchQoS0Seq, lastFetchQoS1Seq, lastFetchQoS2Seq);
        } catch (Throwable e) {
            log.warn("Failed to send hint: inboxId={}", inboxId, e);
        }
    }

    @Override
    public void close() {
        // tell server side to remove FetchState
        hint(-1);
        ppln.stopFetch(sessionId, inboxId, incarnation);
    }
}
