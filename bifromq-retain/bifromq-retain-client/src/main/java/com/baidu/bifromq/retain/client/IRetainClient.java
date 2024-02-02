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

package com.baidu.bifromq.retain.client;

import com.baidu.bifromq.baserpc.IConnectable;
import com.baidu.bifromq.retain.rpc.proto.MatchReply;
import com.baidu.bifromq.retain.rpc.proto.MatchRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainReply;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;

public interface IRetainClient extends IConnectable {
    static RetainClientBuilder newBuilder() {
        return new RetainClientBuilder();
    }

    CompletableFuture<MatchReply> match(MatchRequest request);

    CompletableFuture<RetainReply> retain(long reqId,
                                          String topic,
                                          QoS qos,
                                          ByteString payload,
                                          int expirySeconds,
                                          ClientInfo publisher);

    void stop();
}
