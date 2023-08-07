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
import static com.baidu.bifromq.sysprops.BifroMQSysProp.INBOX_MAX_INBOXES_PER_CREATE;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.basescheduler.BatchCallBuilder;
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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;

@Slf4j
public class InboxCreateScheduler extends InboxUpdateScheduler<CreateInboxRequest, CreateInboxReply> {
    private final ISettingProvider settingProvider;
    private final int maxInboxesPerCreate;

    public InboxCreateScheduler(IBaseKVStoreClient kvStoreClient, ISettingProvider settingProvider) {
        super(kvStoreClient, "inbox_server_create");
        this.settingProvider = settingProvider;
        maxInboxesPerCreate = INBOX_MAX_INBOXES_PER_CREATE.get();

    }

    @Override
    protected BatchCallBuilder<CreateInboxRequest, CreateInboxReply> newBuilder(String name, int maxInflights,
                                                                                KVRangeSetting rangeSetting) {
        return new BatchCreateBuilder(name, maxInflights, rangeSetting, kvStoreClient);
    }

    @Override
    protected ByteString rangeKey(CreateInboxRequest request) {
        return scopedInboxId(request.getClientInfo().getTenantId(), request.getInboxId());
    }

    private class BatchCreateBuilder extends BatchCallBuilder<CreateInboxRequest, CreateInboxReply> {
        private class BatchCreate implements IBatchCall<CreateInboxRequest, CreateInboxReply> {
            // key: scopedInboxIdUtf8
            private final Map<String, CreateParams> inboxCreates = new NonBlockingHashMap<>();

            // key: scopedInboxIdUtf8
            private final Map<CreateInboxRequest, CompletableFuture<CreateInboxReply>> onInboxCreated =
                new NonBlockingHashMap<>();

            @Override
            public boolean isEmpty() {
                return inboxCreates.isEmpty();
            }

            @Override
            public CompletableFuture<CreateInboxReply> add(CreateInboxRequest request) {
                ClientInfo client = request.getClientInfo();
                String tenantId = client.getTenantId();
                String scopedInboxIdUtf8 = scopedInboxId(tenantId, request.getInboxId()).toStringUtf8();
                inboxCreates.computeIfAbsent(scopedInboxIdUtf8, k -> CreateParams.newBuilder()
                    .setExpireSeconds(settingProvider.provide(OfflineExpireTimeSeconds, tenantId))
                    .setLimit(settingProvider.provide(OfflineQueueSize, tenantId))
                    .setDropOldest(settingProvider.provide(OfflineOverflowDropOldest, tenantId))
                    .setClient(client)
                    .build());
                return onInboxCreated.computeIfAbsent(request, k -> new CompletableFuture<>());
            }

            @Override
            public void reset() {
                inboxCreates.clear();
                onInboxCreated.clear();
            }

            @Override
            public CompletableFuture<Void> execute() {
                long reqId = System.nanoTime();
                batchInboxCount.record(inboxCreates.size());
                Timer.Sample start = Timer.start();
                return kvStoreClient.execute(range.leader,
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
                        start.stop(batchCreateTimer);
                        switch (reply.getCode()) {
                            case Ok:
                                try {
                                    return InboxServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult())
                                        .getCreateInbox();
                                } catch (InvalidProtocolBufferException e) {
                                    log.error("Unable to parse rw co-proc output", e);
                                    throw new RuntimeException(e);
                                }
                            default:
                                log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                                throw new RuntimeException();
                        }
                    })
                    .handle((v, e) -> {
                        for (CreateInboxRequest request : onInboxCreated.keySet()) {
                            onInboxCreated.get(request).complete(CreateInboxReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setResult(e == null ? CreateInboxReply.Result.OK :
                                    CreateInboxReply.Result.ERROR)
                                .build());
                        }
                        return null;
                    });

            }

            @Override
            public boolean isEnough() {
                return inboxCreates.size() > maxInboxesPerCreate;
            }
        }

        private final IBaseKVStoreClient kvStoreClient;

        private final KVRangeSetting range;

        private final DistributionSummary batchInboxCount;
        private final Timer batchCreateTimer;

        BatchCreateBuilder(String name, int maxInflights, KVRangeSetting range,
                           IBaseKVStoreClient kvStoreClient) {
            super(name, maxInflights);
            this.range = range;
            this.kvStoreClient = kvStoreClient;
            Tags tags = Tags.of("rangeId", KVRangeIdUtil.toShortString(range.id));
            batchInboxCount = DistributionSummary.builder("inbox.server.create.inboxes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            batchCreateTimer = Timer.builder("inbox.server.create.latency")
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        @Override
        public BatchCreate newBatch() {
            return new BatchCreate();
        }

        @Override
        public void close() {
            Metrics.globalRegistry.remove(batchInboxCount);
            Metrics.globalRegistry.remove(batchCreateTimer);
        }
    }
}
