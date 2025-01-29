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

package com.baidu.bifromq.mqtt.service;

import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.mqtt.session.IMQTTTransientSession;
import com.baidu.bifromq.plugin.subbroker.CheckReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.type.MatchInfo;
import java.util.concurrent.CompletableFuture;

public interface ILocalDistService {
    static String localize(String receiverId) {
        return "1" + receiverId;
    }

    static String globalize(String receiverId) {
        return "0" + receiverId;
    }

    static String parseReceiverId(String scopedReceiverId) {
        return scopedReceiverId.substring(1);
    }

    static boolean isGlobal(String receiverId) {
        return receiverId.startsWith("0");
    }

    CompletableFuture<MatchResult> match(long reqId, String topicFilter, IMQTTTransientSession session);

    CompletableFuture<UnmatchResult> unmatch(long reqId, String topicFilter, IMQTTTransientSession session);

    CompletableFuture<DeliveryReply> dist(DeliveryRequest request);

    CheckReply.Code checkMatchInfo(String tenantId, MatchInfo matchInfo);
}
