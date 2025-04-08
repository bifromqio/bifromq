/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.inbox.store.delay;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.record.TenantInboxInstance;
import com.baidu.bifromq.inbox.rpc.proto.DetachReply;
import com.baidu.bifromq.inbox.rpc.proto.DetachRequest;
import com.baidu.bifromq.type.ClientInfo;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Retry-able task for detaching an inbox after a delay.
 */
public class DetachInboxTask extends RetryableDelayedTask<DetachReply> {
    private final IInboxClient inboxClient;
    private final int expireSeconds;
    private final ClientInfo clientInfo;

    public DetachInboxTask(Duration delay,
                           long version,
                           int expireSeconds,
                           ClientInfo clientInfo,
                           IInboxClient inboxClient) {
        this(delay, version, expireSeconds, clientInfo, inboxClient, 0);
    }

    private DetachInboxTask(Duration delay,
                            long version,
                            int expireSeconds,
                            ClientInfo clientInfo,
                            IInboxClient inboxClient,
                            int retryCount) {
        super(delay, version, retryCount);
        this.inboxClient = inboxClient;
        this.expireSeconds = expireSeconds;
        this.clientInfo = clientInfo;
    }

    @Override
    protected CompletableFuture<DetachReply> callOperation(TenantInboxInstance key,
                                                           IDelayTaskRunner<TenantInboxInstance> runner) {
        return inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(System.nanoTime())
            .setInboxId(key.instance().inboxId())
            .setIncarnation(key.instance().incarnation())
            .setVersion(version)
            .setExpirySeconds(expireSeconds)
            .setDiscardLWT(false)
            .setClient(clientInfo)
            .setNow(HLC.INST.getPhysical())
            .setSender(runner.owner())
            .build());
    }

    @Override
    protected boolean shouldRetry(DetachReply reply) {
        return reply.getCode() == DetachReply.Code.TRY_LATER
            || reply.getCode() == DetachReply.Code.BACK_PRESSURE_REJECTED;
    }

    @Override
    protected RetryableDelayedTask<DetachReply> createRetryTask(Duration newDelay) {
        return new DetachInboxTask(newDelay, version, expireSeconds, clientInfo, inboxClient, retryCount + 1);
    }

    @Override
    protected String getTaskName() {
        return "Detach inbox";
    }
}
