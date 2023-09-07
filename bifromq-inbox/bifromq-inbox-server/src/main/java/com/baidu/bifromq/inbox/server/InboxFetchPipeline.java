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

import static com.baidu.bifromq.inbox.util.KeyUtil.parseInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;

import com.baidu.bifromq.baserpc.AckStream;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.storage.proto.FetchParams;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.util.KeyUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxFetchPipeline extends AckStream<InboxFetchHint, InboxFetched> implements IInboxFetcher {
    private static final int NOT_KNOWN_CAPACITY = -1;
    private final String delivererKey;
    private final Cache<ByteString, FetchState> inboxFetchStates;
    private final Fetcher fetcher;
    private final Function<ByteString, CompletableFuture<Void>> toucher;
    private volatile boolean closed = false;

    public InboxFetchPipeline(StreamObserver<InboxFetched> responseObserver,
                              Fetcher fetcher,
                              Function<ByteString, CompletableFuture<Void>> toucher,
                              InboxFetcherRegistry registry) {
        super(responseObserver);
        this.delivererKey = RPCContext.WCH_HASH_KEY_CTX_KEY.get();

        inboxFetchStates = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(60))
            .build();
        this.fetcher = fetcher;
        this.toucher = toucher;
        registry.reg(this);
        ack().doFinally(() -> {
                touch();
                registry.unreg(this);
                closed = true;
            })
            .subscribe(fetchHint -> {
                String inboxId = fetchHint.getInboxId();
                log.trace("Got hint: tenantId={}, inboxId={}\n{}", tenantId, inboxId, fetchHint);
                ByteString scopedInboxId = KeyUtil.scopedInboxId(tenantId, inboxId);
                FetchState fetchState = inboxFetchStates.asMap().compute(scopedInboxId, (k, v) -> {
                    if (v == null) {
                        v = new FetchState(fetchHint.getIncarnation());
                    } else if (v.incarnation.get() < fetchHint.getIncarnation()) {
                        v.reset(fetchHint.getIncarnation());
                    }
                    v.lastFetchQoS0Seq.set(Math.max(fetchHint.getLastFetchQoS0Seq(), v.lastFetchQoS0Seq.get()));
                    v.lastFetchQoS1Seq.set(Math.max(fetchHint.getLastFetchQoS1Seq(), v.lastFetchQoS1Seq.get()));
                    v.lastFetchQoS2Seq.set(Math.max(fetchHint.getLastFetchQoS2Seq(), v.lastFetchQoS2Seq.get()));
                    v.downStreamCapacity.set(Math.max(0, fetchHint.getCapacity()));
                    return v;
                });
                log.trace("Fetch state update: tenantId={}, inbox={}\n{}", tenantId, inboxId, fetchState);
                fetch(scopedInboxId, fetchState);
            });
    }

    @Override
    public String tenantId() {
        return tenantId;
    }

    @Override
    public String delivererKey() {
        return delivererKey;
    }

    @Override
    public String inboxId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastFetchTS() {
        return 0;
    }

    @Override
    public void signalFetch() {
//        log.trace("Signal fetch: tenantId={}, fetching={}", tenantId, fetchStarted.get());
//        if (!closed && fetchStarted.compareAndSet(false, true)) {
//            fetch(buildInboxFetchList());
//        }
    }

    @Override
    public void signalFetch(String inboxId) {
        log.trace("Signal fetch: tenantId={}, inboxId={}", tenantId, inboxId);
        // signal fetch won't refresh expiry
        FetchState fetchState = inboxFetchStates.getIfPresent(scopedInboxId(tenantId, inboxId));
        if (fetchState != null) {
            fetchState.hasMore.set(true);
            fetchState.signalFetchTS.set(System.nanoTime());
            fetch(scopedInboxId(tenantId, inboxId), fetchState);
        }
    }

    @Override
    public void touch() {
        // touch won't refresh expiry
        inboxFetchStates.asMap().keySet().forEach(toucher::apply);
    }

    @Override
    public void close() {
        super.close();
    }

    private void fetch(ByteString scopedInboxId) {
        FetchState fetchState = inboxFetchStates.getIfPresent(scopedInboxId);
        if (fetchState != null) {
            fetch(scopedInboxId, fetchState);
        }
    }

    private void fetch(ByteString scopedInboxId, FetchState fetchState) {
        if (closed) {
            return;
        }
        if (fetchState.fetching.compareAndSet(false, true)) {
            String inboxId = parseInboxId(scopedInboxId);
            log.trace("Fetching inbox: tenantId={}, inboxId={}", tenantId, inboxId);
            IInboxFetchScheduler.InboxFetch inboxFetch =
                new IInboxFetchScheduler.InboxFetch(scopedInboxId, FetchParams.newBuilder()
                    .setMaxFetch(fetchState.downStreamCapacity.get())
                    .setQos0StartAfter(fetchState.lastFetchQoS0Seq.get())
                    .setQos1StartAfter(fetchState.lastFetchQoS1Seq.get())
                    .setQos2StartAfter(fetchState.lastFetchQoS2Seq.get())
                    .build());
            long fetchTS = System.nanoTime();
            fetcher.fetch(inboxFetch).whenComplete((fetched, e) -> {
                if (e != null) {
                    log.error("Failed to fetch inbox: tenantId={}, inboxId={}", tenantId, inboxId, e);
                    inboxFetchStates.invalidate(scopedInboxId);
                    send(InboxFetched.newBuilder()
                        .setInboxId(inboxId)
                        .setFetched(Fetched.newBuilder()
                            .setResult(Fetched.Result.ERROR)
                            .build())
                        .build());
                } else {
                    log.trace("Fetched inbox: tenantId={}, inboxId={}", tenantId, inboxId);
                    int fetchedCount = 0;
                    if (fetched.getQos0MsgCount() > 0 ||
                        fetched.getQos1MsgCount() > 0 ||
                        fetched.getQos2MsgCount() > 0) {
                        if (fetched.getQos0MsgCount() > 0) {
                            fetchedCount += fetched.getQos0MsgCount();
                            fetchState.downStreamCapacity.accumulateAndGet(fetched.getQos0MsgCount(),
                                (a, b) -> a == NOT_KNOWN_CAPACITY ? a : Math.max(a - b, 0));
                            fetchState.lastFetchQoS0Seq.set(fetched.getQos0Seq(fetched.getQos0SeqCount() - 1));
                        }
                        if (fetched.getQos1MsgCount() > 0) {
                            fetchedCount += fetched.getQos1MsgCount();
                            fetchState.downStreamCapacity.accumulateAndGet(fetched.getQos1MsgCount(),
                                (a, b) -> a == NOT_KNOWN_CAPACITY ? a : Math.max(a - b, 0));
                            fetchState.lastFetchQoS1Seq.set(fetched.getQos1Seq(fetched.getQos1SeqCount() - 1));
                        }
                        if (fetched.getQos2MsgCount() > 0) {
                            fetchedCount += fetched.getQos2MsgCount();
                            fetchState.downStreamCapacity.accumulateAndGet(fetched.getQos2MsgCount(),
                                (a, b) -> a == NOT_KNOWN_CAPACITY ? a : Math.max(a - b, 0));
                            fetchState.lastFetchQoS2Seq.set(fetched.getQos2Seq(fetched.getQos2SeqCount() - 1));
                        }
                        fetchState.hasMore.set(fetchedCount >= inboxFetch.params.getMaxFetch() ||
                            fetchState.signalFetchTS.get() > fetchTS);
                    } else {
                        fetchState.hasMore.set(fetchState.signalFetchTS.get() > fetchTS);
                    }
                    send(InboxFetched.newBuilder()
                        .setInboxId(inboxId)
                        .setFetched(fetched)
                        .build());
                    fetchState.fetching.set(false);
                    inboxFetchStates.put(scopedInboxId, fetchState);
                    if (fetchState.downStreamCapacity.get() > 0 && fetchState.hasMore.get()) {
                        fetch(scopedInboxId);
                    }
                }
            });
        }
    }

    interface Fetcher {
        CompletableFuture<Fetched> fetch(IInboxFetchScheduler.InboxFetch fetch);
    }

    @ToString
    private static class FetchState {
        final AtomicLong incarnation = new AtomicLong();
        final AtomicBoolean fetching = new AtomicBoolean(false);
        final AtomicBoolean hasMore = new AtomicBoolean(true);
        final AtomicLong signalFetchTS = new AtomicLong();
        final AtomicInteger downStreamCapacity = new AtomicInteger(NOT_KNOWN_CAPACITY);
        final AtomicLong lastFetchQoS0Seq = new AtomicLong(-1);
        final AtomicLong lastFetchQoS1Seq = new AtomicLong(-1);
        final AtomicLong lastFetchQoS2Seq = new AtomicLong(-1);

        FetchState(long incarnation) {
            this.incarnation.set(incarnation);
        }

        void reset(long incarnation) {
            this.incarnation.set(incarnation);
            hasMore.set(true);
            signalFetchTS.set(0);
            downStreamCapacity.set(NOT_KNOWN_CAPACITY);
            lastFetchQoS0Seq.set(-1);
            lastFetchQoS1Seq.set(-1);
            lastFetchQoS2Seq.set(-1);
        }
    }
}
