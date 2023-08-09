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
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCall2;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.inbox.rpc.proto.SendResult;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxInsertResult;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.MessagePack;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.baidu.bifromq.type.SubInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxInsertScheduler2 extends InboxMutateScheduler<MessagePack, SendResult.Result>
    implements IInboxInsertScheduler {
    public InboxInsertScheduler2(IBaseKVStoreClient inboxStoreClient) {
        super(inboxStoreClient, "inbox_server_insert");
    }

    @Override
    protected Batcher<MessagePack, SendResult.Result, KVRangeSetting> newBatcher(String name,
                                                                                 long maxTolerantLatencyNanos,
                                                                                 KVRangeSetting range) {
        return new InboxInsertBatcher(name, maxTolerantLatencyNanos, range, inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(MessagePack request) {
        return KeyUtil.scopedInboxId(request.getSubInfo().getTenantId(), request.getSubInfo().getInboxId());
    }

    private static class InboxInsertBatcher extends Batcher<MessagePack, SendResult.Result, KVRangeSetting> {
        private class InboxBatchInsert extends BatchCall2<MessagePack, SendResult.Result> {
            // key: scopedInboxIdUtf8
            private final Queue<CallTask<MessagePack, SendResult.Result>> inboxInserts = new ConcurrentLinkedQueue<>();

            @Override
            public void add(CallTask<MessagePack, SendResult.Result> callTask) {
                inboxInserts.add(callTask);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                InboxInsertRequest.Builder reqBuilder = InboxInsertRequest.newBuilder();
                inboxInserts.forEach(insertTask -> reqBuilder.addSubMsgPack(insertTask.call));
                Timer.Sample start = Timer.start();
                return inboxStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setInsert(reqBuilder.build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult());
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException("Unable to parse rw co-proc output", e);
                            }
                        }
                        throw new RuntimeException(
                            String.format("Failed to exec rw co-proc[code=%s]", reply.getCode()));
                    })
                    .handle((v, e) -> {
                        if (e != null) {
                            while (!inboxInserts.isEmpty()) {
                                CallTask<MessagePack, SendResult.Result> task = inboxInserts.poll();
                                task.callResult.completeExceptionally(e);
                            }
                        } else {
                            Map<SubInfo, SendResult.Result> insertResults = new HashMap<>();
                            for (InboxInsertResult result : v.getInsert().getResultsList()) {
                                if (result.getResult() == InboxInsertResult.Result.NO_INBOX) {
                                    insertResults.put(result.getSubInfo(), SendResult.Result.NO_INBOX);
                                } else {
                                    insertResults.put(result.getSubInfo(),
                                        com.baidu.bifromq.inbox.rpc.proto.SendResult.Result.OK);
                                }
                            }
                            while (!inboxInserts.isEmpty()) {
                                CallTask<MessagePack, SendResult.Result> task = inboxInserts.poll();
                                task.callResult.complete(insertResults.get(task.call.getSubInfo()));
                            }
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient inboxStoreClient;
        private final KVRangeSetting range;

        InboxInsertBatcher(String name, long maxTolerantLatencyNanos, KVRangeSetting range,
                           IBaseKVStoreClient inboxStoreClient) {
            super(range, name, maxTolerantLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
        }

        @Override
        protected BatchCall2<MessagePack, SendResult.Result> newBatch() {
            return new InboxBatchInsert();
        }
    }
}
