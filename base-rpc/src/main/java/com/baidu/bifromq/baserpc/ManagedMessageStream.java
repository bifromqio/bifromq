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

package com.baidu.bifromq.baserpc;

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;

import com.baidu.bifromq.baserpc.exception.RequestRejectedException;
import com.baidu.bifromq.baserpc.exception.ServerNotFoundException;
import com.baidu.bifromq.baserpc.metrics.IRPCMeter;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.baidu.bifromq.baserpc.utils.Backoff;
import com.baidu.bifromq.baserpc.utils.BehaviorSubject;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class ManagedMessageStream<MsgT, AckT> implements IRPCClient.IMessageStream<MsgT, AckT> {

    private enum State {
        Normal,
        ServerNotFound,
        ServiceUnavailable,
        Closed
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.Normal);
    private final ConcurrentLinkedQueue<AckT> ackSendingBuffers;
    private final IRPCMeter.IRPCMethodMeter meter;
    private final String tenantId;
    private final String wchKey;
    private final Supplier<Map<String, String>> metadataSupplier;
    private final BluePrint.MethodSemantic semantic;
    private final MethodDescriptor<AckT, MsgT> methodDescriptor;
    private final BluePrint bluePrint;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final BehaviorSubject<Long> signal = BehaviorSubject.createDefault(System.nanoTime());
    private final RPCClient.ChannelHolder channelHolder;
    private final CallOptions callOptions;
    private final AtomicReference<ClientCallStreamObserver<AckT>> requester = new AtomicReference<>();
    private final AtomicReference<String> desiredServerId = new AtomicReference<>();
    private final AtomicReference<String> selectedServerId = new AtomicReference<>();
    // traffic control
    private final AtomicBoolean sending = new AtomicBoolean(false);
    private final Backoff retargetBackoff = new Backoff(5, 10, 60000);

    private volatile PublishSubject<MsgT> msgSubject = PublishSubject.create();

    ManagedMessageStream(
        String tenantId,
        @Nullable String wchKey,
        @Nullable String targetServerId,
        Supplier<Map<String, String>> metadataSupplier,
        RPCClient.ChannelHolder channelHolder,
        CallOptions callOptions,
        MethodDescriptor<AckT, MsgT> methodDescriptor,
        BluePrint bluePrint,
        IRPCMeter.IRPCMethodMeter meter) {
        assert methodDescriptor.getType() == MethodDescriptor.MethodType.BIDI_STREAMING;
        this.bluePrint = bluePrint;
        semantic = bluePrint.semantic(methodDescriptor.getFullMethodName());
        assert semantic instanceof BluePrint.Streaming;
        switch (semantic.mode()) {
            case DDBalanced -> {
                assert targetServerId != null;
                this.desiredServerId.set(targetServerId);
            }
            case WCHBalanced -> {
                assert wchKey != null;
            }
        }
        this.tenantId = tenantId;
        this.wchKey = wchKey;
        this.metadataSupplier = metadataSupplier;
        this.meter = meter;
        ackSendingBuffers = new ConcurrentLinkedQueue<>();
        this.methodDescriptor = methodDescriptor;
        this.channelHolder = channelHolder;
        this.callOptions = callOptions;
        // react to CHash ring change
        disposables.add(Observable.combineLatest(channelHolder.serverSelectorObservable()
                // reset backoff when new selector available
                .doOnNext(s -> retargetBackoff.reset()), signal, (s, t) -> s)
            .subscribeOn(Schedulers.from(channelHolder.rpcExecutor()))
            .subscribe(selector -> {
                synchronized (this) {
                    if (state.get() == State.Closed) {
                        return;
                    }
                    switch (semantic.mode()) {
                        case DDBalanced -> {
                            boolean available = selector.direct(tenantId, desiredServerId.get(),
                                methodDescriptor);
                            if (available) {
                                state.set(State.Normal);
                                if (selectedServerId.get() == null) {
                                    log.debug("MsgStream@{} targeting to server[{}]",
                                        this.hashCode(), desiredServerId.get());
                                    target();
                                } else {
                                    assert desiredServerId.get().equals(selectedServerId.get());
                                }
                            } else {
                                state.set(State.ServerNotFound);
                                if (selectedServerId.get() != null) {
                                    log.debug("MsgStream@{} stop targeting to server[{}]",
                                        this.hashCode(), selectedServerId.get());
                                    requester.getAndSet(null).onCompleted();
                                    selectedServerId.set(null);
                                }
                            }
                        }
                        case WCHBalanced -> {
                            Optional<String> newServer = selector.hashing(tenantId, wchKey, methodDescriptor);
                            if (newServer.isEmpty()) {
                                state.set(State.ServiceUnavailable);
                                if (selectedServerId.get() != null) {
                                    log.debug("MsgStream@{} stop targeting to server[{}]",
                                        this.hashCode(), selectedServerId.get());
                                    desiredServerId.set(null);
                                    if (selectedServerId.get() != null) {
                                        requester.getAndSet(null).onCompleted();
                                        selectedServerId.set(null);
                                    }
                                }
                            } else {
                                state.set(State.Normal);
                                if (!newServer.get().equals(desiredServerId.get())) {
                                    log.debug("MsgStream@{} retargeting to server[{}] from server[{}]",
                                        this.hashCode(), newServer.get(), selectedServerId.get());
                                    desiredServerId.set(newServer.get());
                                    if (selectedServerId.get() != null) {
                                        requester.getAndSet(null).onCompleted();
                                        selectedServerId.set(null);
                                    } else {
                                        target();
                                    }
                                } else if (!desiredServerId.get().equals(selectedServerId.get())) {
                                    target();
                                }
                            }
                        }
                        case WRBalanced -> {
                            Optional<String> newServer = selector.random(tenantId, methodDescriptor);
                            if (newServer.isEmpty()) {
                                state.set(State.ServiceUnavailable);
                                if (selectedServerId.get() != null) {
                                    log.debug("MsgStream@{} stop targeting to server[{}]",
                                        this.hashCode(), selectedServerId.get());
                                    requester.getAndSet(null).onCompleted();
                                    selectedServerId.set(null);
                                }
                            } else {
                                state.set(State.Normal);
                                if (selectedServerId.get() == null) {
                                    target();
                                }
                            }
                        }
                        case WRRBalanced -> {
                            Optional<String> newServer = selector.roundRobin(tenantId, methodDescriptor);
                            if (newServer.isEmpty()) {
                                state.set(State.ServiceUnavailable);
                                if (selectedServerId.get() != null) {
                                    log.debug("MsgStream@{} stop targeting to server[{}]",
                                        this.hashCode(), selectedServerId.get());
                                    requester.getAndSet(null).onCompleted();
                                    selectedServerId.set(null);
                                }
                            } else {
                                state.set(State.Normal);
                                if (selectedServerId.get() == null) {
                                    target();
                                }
                            }
                        }
                    }
                }
            }));
        meter.recordCount(RPCMetric.MsgStreamCreateCount);
    }

    @Override
    public boolean isClosed() {
        return state.get() == State.Closed;
    }

    @Override
    public void ack(AckT ack) {
        switch (state.get()) {
            case Normal, ServiceUnavailable -> {
                ackSendingBuffers.offer(ack);
                // check if pipeline is still open
                sendUntilStreamNotReadyOrNoTask();
                meter.recordCount(RPCMetric.StreamAckAcceptCount);
            }
            case ServerNotFound -> throw new ServerNotFoundException("Server not found");
            case Closed ->
                // pipeline has already closed, finish it with close reason
                throw new RequestRejectedException("Pipeline has closed");
        }
    }

    @Override
    public Observable<MsgT> msg() {
        return msgSubject;
    }

    @Override
    public void close() {
        state.set(State.Closed);
        ackSendingBuffers.clear();
        disposables.dispose();
        msgSubject.onComplete();
        ClientCallStreamObserver<AckT> r = requester.getAndSet(null);
        if (r != null) {
            r.onCompleted();
        }
    }

    private void target() {
        // currently, context attributes from caller are not allowed
        // start a new context by forking from ROOT,so no scaring warning should appear in the log
        Context ctx = Context.ROOT.fork()
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.SELECTED_SERVER_ID_CTX_KEY, new RPCContext.ServerSelection())
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadataSupplier.get());
        switch (semantic.mode()) {
            case DDBalanced -> ctx = ctx.withValue(RPCContext.DESIRED_SERVER_ID_CTX_KEY, desiredServerId.get());
            case WCHBalanced -> ctx = ctx.withValue(RPCContext.WCH_HASH_KEY_CTX_KEY, wchKey);
        }
        ctx.run(() -> {
            log.trace("MsgStream@{} creating request stream", hashCode());
            ClientCallStreamObserver<AckT> reqStream = (ClientCallStreamObserver<AckT>)
                asyncBidiStreamingCall(channelHolder.channel()
                    .newCall(bluePrint.methodDesc(methodDescriptor.getFullMethodName()),
                        callOptions), new ResponseObserver());
            if (RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().getServerId() != null) {
                requester.set(reqStream);
                log.trace("MsgStream@{} request stream@{} created", hashCode(), reqStream.hashCode());
                selectedServerId.set(RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().getServerId());
                sendUntilStreamNotReadyOrNoTask();
            } else {
                log.trace("MsgStream@{} retry request stream creation in 5 seconds", hashCode());
                scheduleSignal(5, TimeUnit.SECONDS);
            }
        });
    }

    private void scheduleSignal(long delay, TimeUnit timeUnit) {
        disposables.add(Observable.timer(delay, timeUnit).subscribe(t -> {
            if (!isClosed()) {
                signal.onNext(System.nanoTime());
            }
        }));
    }

    private void scheduleSignal() {
        scheduleSignal(retargetBackoff.backoff(), TimeUnit.MILLISECONDS);
    }

    private void sendUntilStreamNotReadyOrNoTask() {
        if (sending.compareAndSet(false, true)) {
            synchronized (this) {
                ClientCallStreamObserver<AckT> requestStream = requester.get();
                if (requestStream == null) {
                    sending.set(false);
                    return;
                }
                while (requestStream.isReady() && !ackSendingBuffers.isEmpty()) {
                    AckT ack = ackSendingBuffers.poll();
                    requestStream.onNext(ack);
                    meter.recordCount(RPCMetric.StreamAckSendCount);
                }
                sending.set(false);
                if (requestStream.isReady() && !ackSendingBuffers.isEmpty()) {
                    // deal with the spurious notification
                    sendUntilStreamNotReadyOrNoTask();
                }
            }
        }
    }

    private class ResponseObserver implements ClientResponseObserver<AckT, MsgT> {
        private ClientCallStreamObserver<AckT> requestStream;

        @Override
        public void beforeStart(ClientCallStreamObserver<AckT> requestStream) {
            this.requestStream = requestStream;
            requestStream.setOnReadyHandler(() -> {
                if (requestStream == requester.get()) {
                    sendUntilStreamNotReadyOrNoTask();
                }
            });
        }

        @Override
        public void onNext(MsgT resp) {
            ClientCallStreamObserver<AckT> currentRequestStream = requester.get();
            if (currentRequestStream == null || currentRequestStream == requestStream) {
                msgSubject.onNext(resp);
                meter.recordCount(RPCMetric.StreamMsgReceiveCount);
            } else {
                log.debug("Drop response from orphan stream");
                throw new IllegalStateException();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (throwable.getCause() instanceof IllegalStateException) {
                // orphan stream
                return;
            }
            log.trace("MsgStream@{} internal stream@{} error: state={}",
                ManagedMessageStream.this.hashCode(), requestStream.hashCode(), state.get(), throwable);
            meter.recordCount(RPCMetric.MsgStreamErrorCount);
            this.onCompleted();
        }

        @Override
        public void onCompleted() {
            log.trace("MsgStream@{} internal stream@{} completed",
                ManagedMessageStream.this.hashCode(), requestStream.hashCode());
            synchronized (ManagedMessageStream.this) {
                PublishSubject<MsgT> oldSubject = msgSubject;
                msgSubject = PublishSubject.create();
                oldSubject.onComplete();
                if (requestStream.isReady()) {
                    requestStream.onCompleted();
                }
                if (requester.get() != null && !requester.compareAndSet(requestStream, null)) {
                    // don't schedule target if the completed one is not the active one
                    return;
                }
                if (state.get() != State.Normal) {
                    return;
                }
                switch (semantic.mode()) {
                    case DDBalanced -> {
                        if (selectedServerId.get() != null) {
                            log.trace("MsgStream@{} schedule targeting to server[{}]",
                                ManagedMessageStream.this.hashCode(), selectedServerId.get());
                            selectedServerId.set(null);
                            scheduleSignal();
                        }
                    }
                    case WCHBalanced -> {
                        if (desiredServerId.get() != null) {
                            log.trace("MsgStream@{} schedule targeting to server[{}]",
                                ManagedMessageStream.this.hashCode(), desiredServerId.get());
                            selectedServerId.set(null);
                            scheduleSignal();
                        }
                    }
                    case WRBalanced -> {
                        if (selectedServerId.get() != null) {
                            log.trace("MsgStream@{} schedule targeting to random server",
                                ManagedMessageStream.this.hashCode());
                            selectedServerId.set(null);
                            scheduleSignal();
                        }
                    }
                    case WRRBalanced -> {
                        if (selectedServerId.get() != null) {
                            log.trace("MsgStream@{} schedule targeting to next server",
                                ManagedMessageStream.this.hashCode());
                            selectedServerId.set(null);
                            scheduleSignal();
                        }
                    }
                }
            }
        }
    }
}
