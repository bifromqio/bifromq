package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
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
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new DeliveryPipeline(delivererKey, rpcClient);
    }

    @Override
    public CompletableFuture<Boolean> hasInbox(long reqId,
                                               @NonNull String trafficId,
                                               @NonNull String inboxId,
                                               @Nullable String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return rpcClient.invoke(trafficId, delivererKey,
                HasInboxRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).build(),
                InboxServiceGrpc.getHasInboxMethod())
            .thenApply(HasInboxReply::getResult);
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
