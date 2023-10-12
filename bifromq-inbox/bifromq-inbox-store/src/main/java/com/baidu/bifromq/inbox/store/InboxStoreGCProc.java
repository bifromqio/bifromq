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
