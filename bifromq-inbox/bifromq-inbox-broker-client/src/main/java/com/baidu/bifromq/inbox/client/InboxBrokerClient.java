package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.plugin.inboxbroker.HasResult;
import com.baidu.bifromq.plugin.inboxbroker.IInboxWriter;
import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxBrokerClient implements IInboxBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    InboxBrokerClient(@NonNull IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public IInboxWriter openInboxWriter(String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        return new InboxPipeline(inboxGroupKey, rpcClient);
    }

    @Override
    public CompletableFuture<HasResult> hasInbox(long reqId,
                                                 @NonNull String trafficId,
                                                 @NonNull String inboxId,
                                                 @Nullable String inboxGroupKey) {
        Preconditions.checkState(!hasStopped.get());
        return rpcClient.invoke(trafficId, inboxGroupKey,
                HasInboxRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).build(),
                InboxServiceGrpc.getHasInboxMethod())
            .thenApply(hasInboxReply -> {
                switch (hasInboxReply.getResult()) {
                    case YES:
                        return HasResult.YES;
                    case NO:
                        return HasResult.NO;
                    case ERROR:
                    default:
                        return HasResult.error(new RuntimeException("Inbox service internal error"));
                }
            })
            .exceptionally(HasResult::error);
    }


    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.info("Closing inbox broker client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Inbox broker client closed");
        }
    }
}
