package com.baidu.bifromq.plugin.subbroker;

import com.baidu.bifromq.type.SubInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class NoInboxSubBroker implements ISubBroker {
    private static final CompletableFuture<Boolean> FALSE = CompletableFuture.completedFuture(false);
    public static final ISubBroker INSTANCE = new NoInboxSubBroker();

    @Override
    public int id() {
        return Integer.MIN_VALUE;
    }

    @Override
    public IDeliverer open(String delivererKey) {
        return new IDeliverer() {
            @Override
            public CompletableFuture<Map<SubInfo, DeliveryResult>> deliver(Iterable<DeliveryPack> packs) {
                Map<SubInfo, DeliveryResult> deliveryResults = new HashMap<>();
                for (DeliveryPack pack : packs) {
                    pack.inboxes.forEach(subInfo -> deliveryResults.put(subInfo, DeliveryResult.NO_INBOX));
                }
                return CompletableFuture.completedFuture(deliveryResults);
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public CompletableFuture<Boolean> hasInbox(long reqId, String tenantId, String inboxId, String delivererKey) {
        return FALSE;
    }

    @Override
    public void close() {

    }
}
