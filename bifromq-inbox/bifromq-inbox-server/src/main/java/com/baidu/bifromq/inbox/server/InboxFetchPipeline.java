/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import com.baidu.bifromq.baserpc.AckStream;
import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetchHint;
import com.baidu.bifromq.inbox.rpc.proto.InboxFetched;
import com.baidu.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import com.baidu.bifromq.inbox.storage.proto.BatchFetchRequest;
import com.baidu.bifromq.inbox.storage.proto.Fetched;
import com.baidu.bifromq.inbox.util.PipelineUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.disposables.Disposable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxFetchPipeline extends AckStream<InboxFetchHint, InboxFetched> implements IInboxFetcher {
    private static final int NOT_KNOWN_CAPACITY = -1;
    private final String id;
    private final String delivererKey;
    private final Cache<Long, FetchState> inboxFetchSessions;
    private final Map<InboxId, Set<Long>> inboxSessionMap = new ConcurrentHashMap<>();
    private final Fetcher fetcher;
    private final Disposable disposable;
    private volatile boolean closed = false;

    public InboxFetchPipeline(StreamObserver<InboxFetched> responseObserver,
                              Fetcher fetcher,
                              InboxFetcherRegistry registry) {
        super(responseObserver);
        this.id = metadata.get(PipelineUtil.PIPELINE_ATTR_KEY_ID);
        this.delivererKey = RPCContext.WCH_HASH_KEY_CTX_KEY.get();

        inboxFetchSessions = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(120))
            .evictionListener((RemovalListener<Long, FetchState>) (key, value, cause) -> {
                if (value != null) {
                    inboxSessionMap.remove(new InboxId(value.inboxId, value.incarnation));
                }
            })
            .build();
        this.fetcher = fetcher;
        registry.reg(this);
        disposable = ack().doFinally(() -> {
                registry.unreg(this);
                closed = true;
            })
            .subscribe(fetchHint -> {
                String inboxId = fetchHint.getInboxId();
                log.trace("Got hint: tenantId={}, inboxId={}\n{}", tenantId, inboxId, fetchHint);
                if (fetchHint.getCapacity() < 0) {
                    inboxFetchSessions.invalidate(fetchHint.getSessionId());
                } else {
                    FetchState fetchState = inboxFetchSessions.asMap().compute(fetchHint.getSessionId(), (k, v) -> {
                        if (v == null) {
                            v = new FetchState(fetchHint.getInboxId(),
                                fetchHint.getIncarnation(),
                                fetchHint.getSessionId());
                            inboxSessionMap.computeIfAbsent(
                                new InboxId(fetchHint.getInboxId(), fetchHint.getIncarnation()),
                                k1 -> new HashSet<>()).add(fetchHint.getSessionId());
                        }
                        v.lastFetchQoS0Seq.set(fetchHint.getLastFetchQoS0Seq());
                        v.lastFetchQoS1Seq.set(fetchHint.getLastFetchQoS1Seq());
                        v.lastFetchQoS2Seq.set(fetchHint.getLastFetchQoS2Seq());
                        v.downStreamCapacity.set(Math.max(0, fetchHint.getCapacity()));
                        return v;
                    });
                    log.trace("Fetch state update: tenantId={}, inbox={}\n{}", tenantId, inboxId, fetchState);
                    fetch(fetchState);
                }
            });
    }

    @Override
    public String id() {
        return id;
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
    public void send(InboxFetched message) {
        synchronized (this) {
            super.send(message);
        }
    }

    @Override
    public boolean signalFetch(String inboxId, long incarnation) {
        log.trace("Signal fetch: tenantId={}, inboxId={}", tenantId, inboxId);
        // signal fetch won't refresh expiry
        Set<Long> sessionIds = inboxSessionMap.getOrDefault(new InboxId(inboxId, incarnation), Collections.emptySet());
        for (Long sessionId : sessionIds) {
            FetchState fetchState = inboxFetchSessions.getIfPresent(sessionId);
            if (fetchState != null) {
                fetchState.hasMore.set(true);
                fetchState.signalFetchTS.set(System.nanoTime());
                fetch(fetchState);
            }
        }
        return !sessionIds.isEmpty();
    }

    @Override
    public void close() {
        super.close();
        disposable.dispose();
    }

    private void fetch(long sessionId) {
        FetchState fetchState = inboxFetchSessions.getIfPresent(sessionId);
        if (fetchState != null) {
            fetch(fetchState);
        }
    }

    private void fetch(FetchState fetchState) {
        if (closed) {
            return;
        }
        if (fetchState.fetching.compareAndSet(false, true)) {
            long sessionId = fetchState.sessionId;
            String inboxId = fetchState.inboxId;
            long incarnation = fetchState.incarnation;
            log.trace("Fetching inbox: tenantId={}, inboxId={}", tenantId, inboxId);
            IInboxFetchScheduler.InboxFetch inboxFetch =
                new IInboxFetchScheduler.InboxFetch(tenantId, inboxId, incarnation,
                    BatchFetchRequest.Params.newBuilder()
                        .setMaxFetch(fetchState.downStreamCapacity.get())
                        .setQos0StartAfter(fetchState.lastFetchQoS0Seq.get())
                        .setQos1StartAfter(fetchState.lastFetchQoS1Seq.get())
                        .setQos2StartAfter(fetchState.lastFetchQoS2Seq.get())
                        .build());
            long fetchTS = System.nanoTime();
            fetcher.fetch(inboxFetch).whenComplete((fetched, e) -> {
                if (closed) {
                    return;
                }
                if (e != null) {
                    log.debug("Failed to fetch inbox: tenantId={}, inboxId={}, incarnation={}",
                        tenantId, inboxId, incarnation, e);
                    try {
                        inboxFetchSessions.invalidate(sessionId);
                        send(InboxFetched.newBuilder()
                            .setSessionId(fetchState.sessionId)
                            .setInboxId(inboxId)
                            .setIncarnation(incarnation)
                            .setFetched(Fetched.newBuilder()
                                .setResult(Fetched.Result.ERROR)
                                .build())
                            .build());
                    } catch (Throwable t) {
                        log.debug("Send error", t);
                    }
                } else {
                    log.trace("Fetched inbox: tenantId={}, inboxId={}, incarnation={}", tenantId, inboxId, incarnation);
                    try {
                        send(InboxFetched.newBuilder()
                            .setSessionId(sessionId)
                            .setInboxId(inboxId)
                            .setIncarnation(incarnation)
                            .setFetched(fetched).build());
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
                        fetchState.fetching.set(false);
                        inboxFetchSessions.asMap().compute(sessionId, (k, v) -> {
                            if (v == null) {
                                inboxSessionMap.computeIfAbsent(new InboxId(fetchState.inboxId, fetchState.incarnation),
                                    k1 -> new HashSet<>()).add(fetchState.sessionId);
                            }
                            return fetchState;
                        });
                        if (fetchState.downStreamCapacity.get() > 0 && fetchState.hasMore.get()) {
                            fetch(sessionId);
                        }
                    } catch (Throwable t) {
                        log.debug("Send error", t);
                    }
                }
            });
        }
    }

    interface Fetcher {
        CompletableFuture<Fetched> fetch(IInboxFetchScheduler.InboxFetch fetch);
    }

    private record InboxId(String inboxId, long incarnation) {
    }

    @ToString
    private static class FetchState {
        final String inboxId;
        final long incarnation;
        final long sessionId;
        final AtomicBoolean fetching = new AtomicBoolean(false);
        final AtomicBoolean hasMore = new AtomicBoolean(true);
        final AtomicLong signalFetchTS = new AtomicLong();
        final AtomicInteger downStreamCapacity = new AtomicInteger(NOT_KNOWN_CAPACITY);
        final AtomicLong lastFetchQoS0Seq = new AtomicLong(-1);
        final AtomicLong lastFetchQoS1Seq = new AtomicLong(-1);
        final AtomicLong lastFetchQoS2Seq = new AtomicLong(-1);

        FetchState(String inboxId, long incarnation, long sessionId) {
            this.inboxId = inboxId;
            this.incarnation = incarnation;
            this.sessionId = sessionId;
        }
    }
}
