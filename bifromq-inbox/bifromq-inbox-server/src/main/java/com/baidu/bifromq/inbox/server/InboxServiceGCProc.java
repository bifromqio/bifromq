package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.util.DelivererKeyUtil.getDelivererKey;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseTenantId;

import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler;
import com.baidu.bifromq.inbox.server.scheduler.IInboxTouchScheduler.Touch;
import com.baidu.bifromq.inbox.store.gc.InboxGCProc;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxServiceGCProc extends InboxGCProc {

    private final IInboxTouchScheduler touchScheduler;
    private final IDistClient distClient;


    public InboxServiceGCProc(IBaseKVStoreClient storeClient,
                              IInboxTouchScheduler touchScheduler,
                              IDistClient distClient,
                              ExecutorService gcExecutor) {
        super(storeClient, gcExecutor);
        this.touchScheduler = touchScheduler;
        this.distClient = distClient;
    }


    @Override
    protected CompletableFuture<Void> gcInbox(ByteString scopedInboxId) {
        return touchScheduler.schedule(new Touch(scopedInboxId, false))
            .thenCompose(topicFilters -> {
                if (topicFilters.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    String tenantId = parseTenantId(scopedInboxId);
                    String inboxId = parseInboxId(scopedInboxId);
                    List<CompletableFuture<UnmatchResult>> unsubFutures = topicFilters.stream().map(
                        topicFilter -> distClient.unmatch(System.nanoTime(),
                            tenantId,
                            topicFilter,
                            inboxId,
                            getDelivererKey(inboxId), 1
                        )).toList();
                    return CompletableFuture.allOf(unsubFutures.toArray(CompletableFuture[]::new));
                }
            });
    }

}
