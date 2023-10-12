package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler.Touch;
import com.baidu.bifromq.inbox.store.gc.InboxGCProc;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxServiceGCProc extends InboxGCProc {

    private final IInboxTouchScheduler touchScheduler;


    public InboxServiceGCProc(IBaseKVStoreClient storeClient,
                              IInboxTouchScheduler touchScheduler,
                              ExecutorService gcExecutor) {
        super(storeClient, gcExecutor);
        this.touchScheduler = touchScheduler;
    }


    @Override
    protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
        return touchScheduler.schedule(new Touch(scopedInboxId, false)).thenApply(r -> null);
    }

}
