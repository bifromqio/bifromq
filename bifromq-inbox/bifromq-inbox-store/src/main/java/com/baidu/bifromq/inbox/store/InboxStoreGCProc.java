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

package com.baidu.bifromq.inbox.store;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import com.baidu.bifromq.inbox.store.gc.InboxGCProc;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxStoreGCProc extends InboxGCProc {

    public InboxStoreGCProc(IBaseKVStoreClient storeClient,
                            ExecutorService gcExecutor) {
        super(storeClient, gcExecutor);
    }

    @Override
    protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
        Optional<KVRangeSetting> rangeSetting = storeClient.findByKey(scopedInboxId);
        if (rangeSetting.isPresent()) {
            BatchTouchRequest.Builder reqBuilder = BatchTouchRequest.newBuilder();
            reqBuilder.putScopedInboxId(scopedInboxId.toStringUtf8(), true);
            long reqId = System.nanoTime();
            KVRangeRWRequest rwRequest = KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setVer(rangeSetting.get().ver)
                .setKvRangeId(rangeSetting.get().id)
                .setRwCoProc(RWCoProcInput.newBuilder()
                    .setInboxService(InboxServiceRWCoProcInput.newBuilder()
                        .setReqId(reqId)
                        .setBatchTouch(reqBuilder.build())
                        .build())
                    .build())
                .build();
            return storeClient.execute(rangeSetting.get().leader, rwRequest)
                .thenApply(reply -> {
                    if (!ReplyCode.Ok.equals(reply.getCode())) {
                        throw new RuntimeException(String.format("Failed to touch inbox: %s, code=%s",
                            scopedInboxId.toStringUtf8(), reply.getCode()));
                    }
                    return null;
                });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

}
