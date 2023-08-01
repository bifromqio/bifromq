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

package com.baidu.bifromq.inbox.client;

import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.inbox.RPCBluePrint;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import com.baidu.bifromq.plugin.subbroker.IDeliverer;
import com.google.common.base.Preconditions;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxBrokerClient implements IInboxBrokerClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;

    InboxBrokerClient(InboxBrokerClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .crdtService(builder.crdtService)
            .sslContext(builder.sslContext)
            .build();
    }

    @Override
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new DeliveryPipeline(delivererKey, rpcClient);
    }

    @Override
    public CompletableFuture<Boolean> hasInbox(long reqId,
                                               @NonNull String tenantId,
                                               @NonNull String inboxId,
                                               @Nullable String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return rpcClient.invoke(tenantId, null,
                HasInboxRequest.newBuilder().setReqId(reqId).setInboxId(inboxId).build(),
                InboxServiceGrpc.getHasInboxMethod())
            .thenApply(HasInboxReply::getResult);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
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
