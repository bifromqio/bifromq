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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_INBOX_ID;
import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ;
import static com.baidu.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ;

import com.baidu.bifromq.baserpc.AckStream;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.inbox.rpc.proto.FetchHint;
import com.baidu.bifromq.inbox.server.scheduler.InboxFetchScheduler;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.storage.proto.Fetched.Result;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class InboxFetchPipeline extends AckStream<FetchHint, Fetched> implements IInboxQueueFetcher {
    private static final int MAX_FETCH_COUNT = 100;
    private final Fetcher fetcher;
    private final Function<ByteString, CompletableFuture<Void>> toucher;
    private final AtomicBoolean fetchStarted = new AtomicBoolean(false);

    private final String delivererKey;
    private final String inboxId;
    // indicate downstream free buffer capacity with MAX_FETCH_COUNT
    private final AtomicInteger downStreamCapacity = new AtomicInteger(MAX_FETCH_COUNT);
    private final ByteString scopedInboxId;
    private volatile boolean closed = false;
    private volatile long fetchStartTS = 0;
    private volatile long signalTS = 0;
    private volatile long lastFetchQoS0Seq = -1;
    private volatile long lastFetchQoS1Seq = -1;
    private volatile long lastFetchQoS2Seq = -1;

    public InboxFetchPipeline(StreamObserver<Fetched> responseObserver,
                              Fetcher fetcher,
                              Function<ByteString, CompletableFuture<Void>> toucher,
                              InboxFetcherRegistry registry,
                              RateLimiter limiter) {
        super(responseObserver);
        this.delivererKey = RPCContext.WCH_HASH_KEY_CTX_KEY.get();
        this.inboxId = metadata.get(PIPELINE_ATTR_KEY_INBOX_ID);
        this.fetcher = fetcher;
        this.toucher = toucher;
        scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
        if (limiter.tryAcquire()) {
            if (hasMetadata(PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ)) {
                lastFetchQoS0Seq = Long.parseLong(metadata(PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ));
            }
            if (hasMetadata(PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ)) {
                lastFetchQoS2Seq = Long.parseLong(metadata(PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ));
            }
            registry.reg(this);
            ack().doFinally(() -> {
                    touch();
                    registry.unreg(this);
                    closed = true;
                })
                .subscribe(fetchHint -> {
                    log.trace("Got hint: tenantId={}, inboxId={}, capacity={}",
                        tenantId, inboxId, fetchHint.getCapacity());
                    downStreamCapacity.set(Math.max(0, fetchHint.getCapacity()));
                    signalFetch();
                });
            signalFetch();
        } else {
            close();
        }
    }

    @Override
    public String delivererKey() {
        return delivererKey;
    }

    @Override
    public String tenantId() {
        return tenantId;
    }

    @Override
    public String inboxId() {
        return inboxId;
    }

    @Override
    public long lastFetchTS() {
        return fetchStartTS;
    }

    @Override
    public long lastFetchQoS0Seq() {
        return lastFetchQoS0Seq;
    }

    @Override
    public void signalFetch() {
        log.trace("Signal fetch: tenantId={}, inboxId={}, hint={}, fetching={}",
            tenantId, inboxId, downStreamCapacity.get(), fetchStarted.get());
        signalTS = System.nanoTime();
        if (!closed && fetchStarted.compareAndSet(false, true)) {
            int batchSize = Math.min(downStreamCapacity.get(), MAX_FETCH_COUNT);
            if (batchSize > 0) {
                fetch(batchSize);
            } else {
                fetchStarted.set(false);
            }
        }
    }

    @Override
    public void touch() {
        toucher.apply(scopedInboxId);
    }

    private void fetch(int batchSize) {
        FetchParams.Builder fb = FetchParams.newBuilder().setMaxFetch(batchSize);
        if (lastFetchQoS0Seq >= 0) {
            fb.setQos0StartAfter(lastFetchQoS0Seq);
        }
        if (lastFetchQoS1Seq >= 0) {
            fb.setQos1StartAfter(lastFetchQoS1Seq);
        }
        if (lastFetchQoS2Seq >= 0) {
            fb.setQos2StartAfter(lastFetchQoS2Seq);
        }
        fetcher.fetch(new InboxFetchScheduler.InboxFetch(scopedInboxId, fb.build()))
            .whenComplete((reply, e) -> {
                if (e != null) {
                    log.debug("Fetch failed: tenantId={}, inboxId={}", tenantId, inboxId, e);
                    if (!closed) {
                        send(Fetched.newBuilder().setResult(Result.ERROR).build());
                    }
                    fetchStarted.set(false);
                    return;
                }
                log.trace("Fetch success: tenantId={}, inboxId={}\n{}", tenantId, inboxId, reply);
                int fetchedCount = 0;
                if (reply.getQos0MsgCount() > 0 || reply.getQos1MsgCount() > 0 || reply.getQos2MsgCount() > 0) {
                    if (reply.getQos0MsgCount() > 0) {
                        fetchedCount += reply.getQos0MsgCount();
                        downStreamCapacity.accumulateAndGet(reply.getQos0MsgCount(), (a, b) -> Math.max(a - b, 0));
                        lastFetchQoS0Seq = reply.getQos0Seq(reply.getQos0SeqCount() - 1);
                    }
                    if (reply.getQos1MsgCount() > 0) {
                        fetchedCount += reply.getQos1MsgCount();
                        downStreamCapacity.accumulateAndGet(reply.getQos1MsgCount(), (a, b) -> Math.max(a - b, 0));
                        lastFetchQoS1Seq = reply.getQos1Seq(reply.getQos1SeqCount() - 1);
                    }
                    if (reply.getQos2MsgCount() > 0) {
                        fetchedCount += reply.getQos2MsgCount();
                        downStreamCapacity.accumulateAndGet(reply.getQos2MsgCount(), (a, b) -> Math.max(a - b, 0));
                        lastFetchQoS2Seq = reply.getQos2Seq(reply.getQos2SeqCount() - 1);
                    }
                    if (!closed) {
                        send(reply);
                    }
                    fetchStarted.set(false);
                    if (downStreamCapacity.get() > 0 && fetchedCount >= batchSize) {
                        signalFetch();
                    }
                } else {
                    fetchStarted.set(false);
                    if (signalTS >= fetchStartTS) {
                        signalFetch();
                    }
                }
            });
        fetchStartTS = System.nanoTime();
    }

    interface Fetcher {
        CompletableFuture<Fetched> fetch(InboxFetchScheduler.InboxFetch fetch);
    }
}
