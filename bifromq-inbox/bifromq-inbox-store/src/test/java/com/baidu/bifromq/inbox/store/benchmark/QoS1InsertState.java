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

package com.baidu.bifromq.inbox.store.benchmark;

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;

import com.baidu.bifromq.inbox.storage.proto.InboxInsertReply;
import com.baidu.bifromq.type.Message;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@Slf4j
@State(Scope.Benchmark)
public class QoS1InsertState extends InboxStoreState {
    private static final String tenantId = "testTraffic";

    private static final String topic = "greeting";
    private final Message msg = message(AT_LEAST_ONCE, "hello");
    private static final int inboxCount = 100;

    @Override
    void afterSetup() {
        int i = 0;
        while (i < inboxCount) {
            requestCreate(tenantId, i + "", 100, 600, false);
            i++;
        }
    }

    @Override
    void beforeTeardown() {

    }

    public InboxInsertReply insert() {
        return requestInsert(tenantId, ThreadLocalRandom.current()
            .nextInt(0, inboxCount) + "", topic, msg);
    }
}
