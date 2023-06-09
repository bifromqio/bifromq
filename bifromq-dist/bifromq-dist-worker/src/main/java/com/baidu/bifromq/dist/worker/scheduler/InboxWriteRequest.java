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

package com.baidu.bifromq.dist.worker.scheduler;

import com.baidu.bifromq.dist.entity.NormalMatching;

public class InboxWriteRequest {
    public final NormalMatching matching;
    public final MessagePackWrapper msgPackWrapper;
    public final InboxWriterKey writerKey;

    public InboxWriteRequest(NormalMatching matching, MessagePackWrapper msgPackWrapper) {
        this.matching = matching;
        this.msgPackWrapper = msgPackWrapper;
        writerKey = new InboxWriterKey(matching.brokerId, matching.inboxGroupKey);
    }

    public record InboxWriterKey(int brokerId, String inboxGroupKey) {
    }
}
