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

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineExpireTimeSeconds;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineOverflowDropOldest;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OfflineQueueSize;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxRequest;
import com.baidu.bifromq.inbox.storage.proto.CreateParams;
import com.baidu.bifromq.inbox.storage.proto.CreateRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.ClientInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxCreateScheduler extends InboxMutateScheduler<CreateInboxRequest, CreateInboxReply>
    implements IInboxCreateScheduler {
    private final ISettingProvider settingProvider;

    public InboxCreateScheduler(IBaseKVStoreClient inboxStoreClient, ISettingProvider settingProvider) {
        super(inboxStoreClient, "inbox_server_create");
        this.settingProvider = settingProvider;
    }

    @Override
    protected Batcher<CreateInboxRequest, CreateInboxReply, KVRangeSetting> newBatcher(String name,
                                                                                       long expectLatencyNanos,
                                                                                       long maxTolerantLatencyNanos,
                                                                                       KVRangeSetting range) {
        return new InboxCreateBatcher(name, expectLatencyNanos, maxTolerantLatencyNanos, range, inboxStoreClient,
            settingProvider);
    }

    @Override
    protected ByteString rangeKey(CreateInboxRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private static class InboxCreateBatcher extends Batcher<CreateInboxRequest, CreateInboxReply, KVRangeSetting> {
        private class InboxCreateBatch implements IBatchCall<CreateInboxRequest, CreateInboxReply> {
            // key: scopedInboxIdUtf8
            private final Map<String, CreateParams> inboxCreates = new HashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<CreateInboxRequest, CompletableFuture<CreateInboxReply>> onInboxCreated = new HashMap<>();

            @Override
            public void reset() {
                inboxCreates.clear();
                onInboxCreated.clear();
            }

            @Override
            public void add(CallTask<CreateInboxRequest, CreateInboxReply> callTask) {
                CreateInboxRequest request = callTask.call;
                ClientInfo client = request.getClientInfo();
                String tenantId = client.getTenantId();
                String scopedInboxIdUtf8 = scopedInboxId(tenantId, request.getInboxId()).toStringUtf8();
                inboxCreates.computeIfAbsent(scopedInboxIdUtf8, k -> CreateParams.newBuilder()
                    .setExpireSeconds(settingProvider.provide(OfflineExpireTimeSeconds, tenantId))
                    .setLimit(settingProvider.provide(OfflineQueueSize, tenantId))
                    .setDropOldest(settingProvider.provide(OfflineOverflowDropOldest, tenantId))
                    .setClient(client)
                    .build());
                onInboxCreated.put(request, callTask.callResult);
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                Timer.Sample start = Timer.start();
                return inboxStoreClient.execute(range.leader,
                        KVRangeRWRequest.newBuilder()
                            .setReqId(reqId)
                            .setVer(range.ver)
                            .setKvRangeId(range.id)
                            .setRwCoProc(InboxServiceRWCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setCreateInbox(CreateRequest.newBuilder()
                                    .putAllInboxes(inboxCreates)
                                    .build())
                                .build().toByteString())
                            .build())
                    .thenApply(reply -> {
                        if (reply.getCode() == ReplyCode.Ok) {
                            try {
                                return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult())
                                    .getCreateInbox();
                            } catch (InvalidProtocolBufferException e) {
                                log.error("Unable to parse rw co-proc output", e);
                                throw new RuntimeException(e);
                            }
                        }
                        log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                        throw new RuntimeException();
                    })
                    .handle((v, e) -> {
                        for (CreateInboxRequest request : onInboxCreated.keySet()) {
                            onInboxCreated.get(request).complete(CreateInboxReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setResult(e == null ? CreateInboxReply.Result.OK : CreateInboxReply.Result.ERROR)
                                .build());
                        }
                        return null;
                    });
            }
        }

        private final IBaseKVStoreClient inboxStoreClient;
        private final ISettingProvider settingProvider;
        private final KVRangeSetting range;

        private InboxCreateBatcher(String name,
                                   long expectLatencyNanos,
                                   long maxTolerantLatencyNanos,
                                   KVRangeSetting range,
                                   IBaseKVStoreClient inboxStoreClient,
                                   ISettingProvider settingProvider) {
            super(range, name, expectLatencyNanos, maxTolerantLatencyNanos);
            this.range = range;
            this.inboxStoreClient = inboxStoreClient;
            this.settingProvider = settingProvider;
        }

        @Override
        protected IBatchCall<CreateInboxRequest, CreateInboxReply> newBatch() {
            return new InboxCreateBatch();
        }
    }
}
