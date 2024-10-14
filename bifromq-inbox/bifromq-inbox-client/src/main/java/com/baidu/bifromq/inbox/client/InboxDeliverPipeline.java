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

import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.inbox.rpc.proto.SendReply;
import com.baidu.bifromq.inbox.rpc.proto.SendRequest;
import com.baidu.bifromq.plugin.subbroker.DeliveryReply;
import com.baidu.bifromq.plugin.subbroker.DeliveryRequest;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class InboxDeliverPipeline implements IDeliverer {
    private final IRPCClient.IRequestPipeline<SendRequest, SendReply> ppln;

    InboxDeliverPipeline(String delivererKey, IRPCClient rpcClient) {
        ppln = rpcClient.createRequestPipeline("", null, delivererKey,
            Map.of(PIPELINE_ATTR_KEY_DELIVERERKEY, delivererKey), InboxServiceGrpc.getReceiveMethod());
    }

    @Override
    public CompletableFuture<DeliveryReply> deliver(DeliveryRequest request) {
        long reqId = System.nanoTime();
        return ppln.invoke(SendRequest.newBuilder()
                .setReqId(reqId)
                .setRequest(request)
                .build())
            .thenApply(SendReply::getReply);
    }

    @Override
    public void close() {
        ppln.close();
    }
}
