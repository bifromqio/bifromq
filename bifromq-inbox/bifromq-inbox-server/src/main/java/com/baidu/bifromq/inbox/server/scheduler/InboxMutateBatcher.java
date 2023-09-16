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

package com.baidu.bifromq.inbox.server.scheduler;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.client.IExecutionPipeline;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.CompletableFuture;

public abstract class InboxMutateBatcher<Req, Resp> extends Batcher<Req, Resp, KVRangeSetting> {
    private final IExecutionPipeline execPipeline;

    InboxMutateBatcher(String name,
                       long tolerableLatencyNanos,
                       long burstLatencyNanos,
                       KVRangeSetting range,
                       IBaseKVStoreClient inboxStoreClient) {
        super(range, name, tolerableLatencyNanos, burstLatencyNanos);
        this.execPipeline = inboxStoreClient.createExecutionPipeline(range.leader);
    }

    protected CompletableFuture<InboxServiceRWCoProcOutput> mutate(InboxServiceRWCoProcInput request) {
        return execPipeline.execute(KVRangeRWRequest.newBuilder()
                .setReqId(request.getReqId())
                .setVer(batcherKey.ver)
                .setKvRangeId(batcherKey.id)
                .setRwCoProc(request.toByteString())
                .build())
            .thenApply(reply -> {
                if (reply.getCode() == ReplyCode.Ok) {
                    try {
                        return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }
                throw new RuntimeException(reply.getCode().name());
            });
    }

    @Override
    public void close() {
        super.close();
        execPipeline.close();
    }
}

