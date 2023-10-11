package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.store.gc.InboxGCProc;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxStoreGCProc extends InboxGCProc {

    private final IInboxClient inboxClient;


    public InboxStoreGCProc(IBaseKVStoreClient storeClient,
                            IInboxClient inboxClient,
                            ExecutorService gcExecutor) {
        super(storeClient, gcExecutor);
        this.inboxClient = inboxClient;
    }

    @Override
    protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
        return inboxClient.touch(System.nanoTime(), parseTenantId(scopedInboxId), parseInboxId(scopedInboxId));
    }

}
