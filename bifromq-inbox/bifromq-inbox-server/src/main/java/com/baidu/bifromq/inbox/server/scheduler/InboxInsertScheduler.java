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
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.BatchInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.InsertResult;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxInsertScheduler extends InboxMutateScheduler<MessagePack, SendResult.Result>
    implements IInboxInsertScheduler {
    public InboxInsertScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_insert");
    }

    @Override
    protected Batcher<MessagePack, SendResult.Result, KVRangeSetting> newBatcher(String name,
                                                                                 long tolerableLatencyNanos,
                                                                                 long burstLatencyNanos,
                                                                                 KVRangeSetting range) {
        return new InboxInsertBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(MessagePack request) {
        return KeyUtil.scopedInboxId(request.getSubInfo().getTenantId(), request.getSubInfo().getInboxId());
    }

    private static class InboxInsertBatcher extends InboxMutateBatcher<MessagePack, SendResult.Result> {
        private class InboxBatchInsert implements IBatchCall<MessagePack, SendResult.Result> {
            // key: scopedInboxIdUtf8
            private final Queue<CallTask<MessagePack, SendResult.Result>> inboxInserts = new ArrayDeque<>(128);

            @Override
            public void reset() {
            }

            @Override
            public void add(CallTask<MessagePack, SendResult.Result> callTask) {
                inboxInserts.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                BatchInsertRequest.Builder reqBuilder = BatchInsertRequest.newBuilder();
                inboxInserts.forEach(insertTask -> reqBuilder.addSubMsgPack(insertTask.call));
                return mutate(InboxServiceRWCoProcInput.newBuilder()
                    .setReqId(reqId)
                    .setBatchInsert(reqBuilder.build())
                    .build())
                    .thenApply(InboxServiceRWCoProcOutput::getBatchInsert)
                    .handle((v, e) -> {
                        if (e != null) {
                            log.debug("Failed to insert", e);
                            CallTask<MessagePack, SendResult.Result> task;
                            while ((task = inboxInserts.poll()) != null) {
                                task.callResult.complete(SendResult.Result.ERROR);
                            }
                        } else {
                            Map<SubInfo, SendResult.Result> insertResults = new HashMap<>();
                            for (InsertResult result : v.getResultsList()) {
                                switch (result.getResult()) {
                                    case OK -> insertResults.put(result.getSubInfo(), SendResult.Result.OK);
                                    case NO_INBOX -> insertResults.put(result.getSubInfo(), SendResult.Result.NO_INBOX);
                                    case ERROR -> insertResults.put(result.getSubInfo(), SendResult.Result.ERROR);
                                }
                            }
                            CallTask<MessagePack, SendResult.Result> task;
                            while ((task = inboxInserts.poll()) != null) {
                                task.callResult.complete(insertResults.get(task.call.getSubInfo()));
                            }
                        }
                        return null;
                    });
            }
        }

        InboxInsertBatcher(String name,
                           long tolerableLatencyNanos,
                           long burstLatencyNanos,
                           KVRangeSetting range,
                           IBaseKVStoreClient inboxStoreClient) {
            super(name, tolerableLatencyNanos, burstLatencyNanos, range, inboxStoreClient);
        }

        @Override
        protected IBatchCall<MessagePack, SendResult.Result> newBatch() {
            return new InboxBatchInsert();
        }
    }
}
