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

package com.baidu.bifromq.baserpc;

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;

import com.baidu.bifromq.baserpc.exception.RequestAbortException;
import com.baidu.bifromq.baserpc.exception.RequestRejectedException;
import com.baidu.bifromq.baserpc.exception.RequestThrottledException;
import com.baidu.bifromq.baserpc.exception.ServerNotFoundException;
import com.baidu.bifromq.baserpc.exception.ServiceUnavailableException;
import com.baidu.bifromq.baserpc.loadbalancer.Constants;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.baidu.bifromq.baserpc.utils.Backoff;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ManagedRequestPipeline<ReqT, RespT> implements IRPCClient.IRequestPipeline<ReqT, RespT> {
    private enum State {
        Normal,
        ServiceUnavailable,
        Closed
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.Normal);
    private final ConcurrentLinkedDeque<RequestTask<ReqT, RespT>> preflightTaskQueue;
    private final ConcurrentLinkedDeque<RequestTask<ReqT, RespT>> inflightTaskQueue;
    private final RPCMeters.MeterKey meterKey;
    private final String tenantId;
    private final String wchKey;
    private final Supplier<Map<String, String>> metadataSupplier;
    private final BluePrint.MethodSemantic<ReqT> semantic;
    private final MethodDescriptor<ReqT, RespT> methodDescriptor;
    private final BluePrint bluePrint;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final BehaviorSubject<Long> signal = BehaviorSubject.createDefault(System.nanoTime());
    private final RPCClient.ChannelHolder channelHolder;
    private final CallOptions callOptions;
    private final AtomicReference<ClientCallStreamObserver<ReqT>> requester = new AtomicReference<>();
    private final AtomicReference<String> desiredServerId = new AtomicReference<>();
    private final AtomicReference<String> selectedServerId = new AtomicReference<>();
    private final Backoff retargetBackoff = new Backoff(5, 10, 60000);
    private final AtomicInteger taskCount = new AtomicInteger(0);
    private final AtomicBoolean sending = new AtomicBoolean(false);

    ManagedRequestPipeline(
        String tenantId,
        @Nullable String wchKey,
        @Nullable String targetServerId,
        Supplier<Map<String, String>> metadataSupplier,
        String serviceUniqueName,
        RPCClient.ChannelHolder channelHolder,
        CallOptions callOptions,
        MethodDescriptor<ReqT, RespT> methodDescriptor,
        BluePrint bluePrint) {
        assert methodDescriptor.getType() == MethodDescriptor.MethodType.BIDI_STREAMING;
        this.bluePrint = bluePrint;
        semantic = bluePrint.semantic(methodDescriptor.getFullMethodName());
        assert semantic instanceof BluePrint.PipelineUnary;
        if (semantic instanceof BluePrint.DDBalanced) {
            assert targetServerId != null;
            this.desiredServerId.set(targetServerId);
        } else if (semantic instanceof BluePrint.WCHBalanced) {
            assert wchKey != null;
        }
        this.tenantId = tenantId;
        this.wchKey = wchKey;
        this.metadataSupplier = metadataSupplier;
        this.meterKey = RPCMeters.MeterKey.builder()
            .service(serviceUniqueName)
            .method(methodDescriptor.getBareMethodName())
            .tenantId(tenantId)
            .build();
        preflightTaskQueue = new ConcurrentLinkedDeque<>();
        inflightTaskQueue = new ConcurrentLinkedDeque<>();
        this.methodDescriptor = methodDescriptor;
        this.channelHolder = channelHolder;
        this.callOptions = callOptions.withDeadline(null);
        // react to CHash ring change
        disposables.add(Observable.combineLatest(channelHolder.serverSelectorObservable()
                // reset backoff when new selector available
                .doOnNext(s -> retargetBackoff.reset()), signal, (s, t) -> s)
            .observeOn(Schedulers.from(channelHolder.rpcExecutor()))
            .subscribe(selector -> {
                synchronized (this) {
                    if (state.get() == State.Closed) {
                        return;
                    }
                    if (semantic instanceof BluePrint.DDBalanced) {
                        boolean available = selector.direct(tenantId, desiredServerId.get(),
                            methodDescriptor);
                        if (available) {
                            state.set(State.Normal);
                            if (selectedServerId.get() == null) {
                                log.debug("ReqPipeline@{} of {} targeting to server[{}]",
                                    this.hashCode(),
                                    methodDescriptor.getBareMethodName(),
                                    desiredServerId.get());
                                target();
                            } else {
                                assert desiredServerId.get().equals(selectedServerId.get());
                            }
                        } else {
                            state.set(State.ServiceUnavailable);
                            if (selectedServerId.get() != null) {
                                log.debug("ReqPipeline@{} of {} stop targeting to server[{}]",
                                    this.hashCode(),
                                    methodDescriptor.getBareMethodName(),
                                    selectedServerId.get());
                                requester.getAndSet(null).onCompleted();
                                selectedServerId.set(null);
                            } else {
                                // abort all pending requests
                                abortFlightRequests(new ServerNotFoundException("Server "
                                    + desiredServerId.get() + " not found"));
                            }
                        }
                    } else if (semantic instanceof BluePrint.WCHBalanced) {
                        Optional<String> newServer = selector.hashing(tenantId, wchKey, methodDescriptor);
                        if (newServer.isEmpty()) {
                            state.set(State.ServiceUnavailable);
                            if (selectedServerId.get() != null) {
                                log.debug("ReqPipeline@{} of {} stop targeting to server[{}]",
                                    this.hashCode(),
                                    methodDescriptor.getBareMethodName(),
                                    selectedServerId.get());
                                desiredServerId.set(null);
                                if (selectedServerId.get() != null) {
                                    requester.getAndSet(null).onCompleted();
                                    selectedServerId.set(null);
                                }
                            } else {
                                // abort all pending requests
                                abortFlightRequests(new ServiceUnavailableException(
                                    "Service unavailable for tenant " + tenantId));
                            }
                        } else {
                            state.set(State.Normal);
                            if (!newServer.get().equals(desiredServerId.get())) {
                                log.debug("ReqPipeline@{} of {} retargeting to server[{}] from server[{}]",
                                    this.hashCode(),
                                    methodDescriptor.getBareMethodName(),
                                    newServer.get(), selectedServerId.get());
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
                    } else if (semantic instanceof BluePrint.WRBalanced) {
                        Optional<String> newServer = selector.random(tenantId, methodDescriptor);
                        if (newServer.isEmpty()) {
                            state.set(State.ServiceUnavailable);
                            if (selectedServerId.get() != null) {
                                log.debug("ReqPipeline@{} stop targeting to server[{}]",
                                    this.hashCode(), selectedServerId.get());
                                requester.getAndSet(null).onCompleted();
                                selectedServerId.set(null);
                            } else {
                                // abort all pending requests
                                abortFlightRequests(new ServiceUnavailableException(
                                    "Service unavailable for tenant " + tenantId));
                            }
                        } else {
                            state.set(State.Normal);
                            if (selectedServerId.get() == null) {
                                target();
                            }
                        }
                    } else if (semantic instanceof BluePrint.WRRBalanced) {
                        Optional<String> newServer = selector.roundRobin(tenantId, methodDescriptor);
                        if (newServer.isEmpty()) {
                            state.set(State.ServiceUnavailable);
                            if (selectedServerId.get() != null) {
                                log.debug("ReqPipeline@{} of {} stop targeting to server[{}]",
                                    this.hashCode(),
                                    methodDescriptor.getBareMethodName(),
                                    selectedServerId.get());
                                requester.getAndSet(null).onCompleted();
                                selectedServerId.set(null);
                            } else {
                                // abort all pending requests
                                abortFlightRequests(new ServiceUnavailableException(
                                    "Service unavailable for tenant " + tenantId));
                            }
                        } else {
                            state.set(State.Normal);
                            if (selectedServerId.get() == null) {
                                target();
                            }
                        }
                    }
                }
            }));
        RPCMeters.recordCount(meterKey, RPCMetric.ReqPipelineCreateCount);
    }

    @Override
    public boolean isClosed() {
        return state.get() == State.Closed;
    }

    @Override
    public CompletableFuture<RespT> invoke(ReqT req) {
        RequestTask<ReqT, RespT> newRequest = new RequestTask<>(req);
        switch (state.get()) {
            case Normal: {
                int currentCount = taskCount.get();
                trace("ReqPipeline@{} of {} queue request: queueSize={},req={}",
                    this.hashCode(), methodDescriptor.getBareMethodName(), currentCount, req);
                preflightTaskQueue.offer(newRequest);
                sendUntilStreamNotReadyOrNoTask();
                RPCMeters.recordCount(meterKey, RPCMetric.PipelineReqAcceptCount);
                RPCMeters.recordSummary(meterKey, RPCMetric.ReqPipelineDepth, currentCount);
                break;
            }
            case ServiceUnavailable: {
                int currentCount = taskCount.get();
                trace("ReqPipeline@{} of {} queue request: queueSize={},req={}",
                    this.hashCode(), methodDescriptor.getBareMethodName(), currentCount, req);
                preflightTaskQueue.offer(newRequest);
                if (semantic instanceof BluePrint.DDBalanced) {
                    abortFlightRequests(new ServerNotFoundException("Server not found: " + desiredServerId.get()));
                } else {
                    abortFlightRequests(new ServiceUnavailableException("Service unavailable now"));
                }
                break;
            }
            case Closed:
                trace("ReqPipeline@{} of {} drop request due to already close: req={}",
                    this.hashCode(), methodDescriptor.getBareMethodName(), req);
                // pipeline has already closed, finish it with close reason
                newRequest.finish(new RequestRejectedException("Pipeline has closed"));
                break;
        }
        return newRequest.future;
    }

    @Override
    public void close() {
        state.set(State.Closed);
        // stop react to lb changes
        disposables.dispose();
        log.debug("ReqPipeline@{} of {} closing, abort remaining tasks",
            hashCode(), methodDescriptor.getBareMethodName());
        abortFlightRequests(new RequestAbortException("Client closed"));
        ClientCallStreamObserver<ReqT> prev = requester.getAndSet(null);
        if (prev != null) {
            prev.onCompleted();
        }
    }

    private void target() {
        // currently, context attributes from caller are not allowed
        // start a new context by forking from ROOT,so no scaring warning should appear in the log
        Context ctx = Context.ROOT.fork()
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.SELECTED_SERVER_ID_CTX_KEY, new RPCContext.ServerSelection())
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadataSupplier.get());
        if (semantic instanceof BluePrint.DDBalanced) {
            ctx = ctx.withValue(RPCContext.DESIRED_SERVER_ID_CTX_KEY, desiredServerId.get());
        } else if (semantic instanceof BluePrint.WCHBalanced) {
            ctx = ctx.withValue(RPCContext.WCH_HASH_KEY_CTX_KEY, wchKey);
        }
        ctx.run(() -> {
            trace("ReqPipeline@{} creating request stream", hashCode());
            ResponseObserver observer = new ResponseObserver();
            ClientCallStreamObserver<ReqT> reqStream = (ClientCallStreamObserver<ReqT>)
                asyncBidiStreamingCall(channelHolder.channel()
                    .newCall(bluePrint.methodDesc(methodDescriptor.getFullMethodName()), callOptions), observer);
            // under In-Proc + DirectExecutor setting, asyncBidiStreamingCall will run on calling thread, this may cause
            // problem when client starts first. In that case the returned reqStream is already onError so we need a
            // flag
            // to distinguish this situation
            if (RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().getServerId() != null && !observer.hasTerminated()) {
                requester.set(reqStream);
                log.trace("ReqPipeline@{} request stream@{} created",
                    ManagedRequestPipeline.this.hashCode(), reqStream.hashCode());
                selectedServerId.set(RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().getServerId());
                sendUntilStreamNotReadyOrNoTask();
            } else {
                log.trace("ReqPipeline@{} retry request stream creation in 5 seconds", hashCode());
                scheduleSignal(5, TimeUnit.SECONDS);
            }
        });
    }

    private void scheduleSignal(long delay, TimeUnit timeUnit) {
        log.debug("ReqPipeline@{} of {} schedule targeting in {} ms",
            this.hashCode(), methodDescriptor.getBareMethodName(), delay);
        disposables.add(Observable.timer(delay, timeUnit).subscribe(t -> signal.onNext(System.nanoTime())));
    }

    private void scheduleSignal() {
        scheduleSignal(retargetBackoff.backoff(), TimeUnit.MILLISECONDS);
    }

    private void sendUntilStreamNotReadyOrNoTask() {
        if (sending.compareAndSet(false, true)) {
            synchronized (this) {
                ClientCallStreamObserver<ReqT> requestStream = requester.get();
                if (requestStream == null) {
                    sending.set(false);
                    return;
                }
                while (requestStream.isReady()) {
                    Optional<RequestTask<ReqT, RespT>> requestTask = prepareForFly();
                    if (requestTask.isPresent() && !requestTask.get().future.isCancelled()) {
                        // only send non-canceled requests
                        RPCMeters.timer(meterKey, RPCMetric.PipelineReqQueueTime)
                            .record(System.nanoTime() - requestTask.get().enqueueTS, TimeUnit.NANOSECONDS);
                        requestStream.onNext(requestTask.get().request);
                        RPCMeters.recordCount(meterKey, RPCMetric.PipelineReqSendCount);
                    } else {
                        break;
                    }
                }
                sending.set(false);
                if (requestStream.isReady() && !preflightTaskQueue.isEmpty()) {
                    // deal with the spurious notification
                    sendUntilStreamNotReadyOrNoTask();
                }
            }
        }
    }

    private Optional<RequestTask<ReqT, RespT>> prepareForFly() {
        RequestTask<ReqT, RespT> requestTask = preflightTaskQueue.poll();
        if (requestTask != null) {
            inflightTaskQueue.offer(requestTask);
        }
        return Optional.ofNullable(requestTask);
    }

    private void prepareForReFly() {
        while (!inflightTaskQueue.isEmpty()) {
            RequestTask<ReqT, RespT> refly = inflightTaskQueue.pollLast();
            log.trace("ReqPipeline@{} of {} re-fly task: {}",
                hashCode(), methodDescriptor.getBareMethodName(), refly.request);
            preflightTaskQueue.addFirst(refly);
        }
    }

    private Optional<RequestTask<ReqT, RespT>> prepareForAbort() {
        RequestTask<ReqT, RespT> requestTask = inflightTaskQueue.poll();
        if (requestTask != null) {
            return Optional.of(requestTask);
        }
        return Optional.ofNullable(preflightTaskQueue.poll());
    }

    private synchronized void abortFlightRequests(Throwable cause) {
        if (inflightTaskQueue.isEmpty() && state.get() == State.Normal) {
            trace("No in-flight request to abort");
        } else {
            trace("Abort flight requests: count={}", taskCount.get());
            while (true) {
                Optional<RequestTask<ReqT, RespT>> requestTask = prepareForAbort();
                if (requestTask.isPresent()) {
                    trace("Abort request: {}", requestTask.get().request);
                    requestTask.get().finish(cause);
                } else {
                    break;
                }
            }
        }
    }

    private class RequestTask<Req, Resp> {
        final Long enqueueTS = System.nanoTime();
        final Req request;
        final CompletableFuture<Resp> future;

        RequestTask(Req request) {
            this.request = request;
            taskCount.incrementAndGet();
            this.future = new CompletableFuture<>();
            this.future.whenComplete((v, e) -> taskCount.decrementAndGet());
        }

        public void finish(Throwable throwable) {
            if (future.completeExceptionally(throwable)) {
                trace("ReqPipeline@{} of {} request finished with error: req={}, error={}",
                    ManagedRequestPipeline.this.hashCode(),
                    methodDescriptor.getBareMethodName(),
                    request, throwable.getMessage());
            }
            if (throwable instanceof RequestRejectedException ||
                throwable instanceof RequestThrottledException) {
                RPCMeters.recordCount(meterKey, RPCMetric.PipelineReqDropCount);
            } else if (throwable instanceof RequestAbortException) {
                RPCMeters.recordCount(meterKey, RPCMetric.PipelineReqAbortCount);
            }
        }

        public void finish(Resp resp) {
            long finishTime = System.nanoTime() - enqueueTS;
            RPCMeters.timer(meterKey, RPCMetric.PipelineReqLatency).record(finishTime, TimeUnit.NANOSECONDS);
            if (future.complete(resp)) {
                trace("ReqPipeline@{} of {} request finished: req={}, resp={}, flights={}",
                    ManagedRequestPipeline.this.hashCode(),
                    methodDescriptor.getBareMethodName(), request, resp, taskCount.get());
            }
            RPCMeters.recordCount(meterKey, RPCMetric.PipelineReqCompleteCount);
        }
    }

    private void trace(String msg, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(msg, args);
        }
    }

    private class ResponseObserver implements ClientResponseObserver<ReqT, RespT> {
        private ClientCallStreamObserver<ReqT> requestStream;
        private boolean terminated = false;

        public boolean hasTerminated() {
            return terminated;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
            this.requestStream = requestStream;
            requestStream.setOnReadyHandler(() -> {
                if (requestStream == requester.get()) {
                    sendUntilStreamNotReadyOrNoTask();
                }
            });
        }

        @Override
        public void onNext(RespT resp) {
            ClientCallStreamObserver<ReqT> currentRequestStream = requester.get();
            if (currentRequestStream == null || currentRequestStream == requestStream) {
                RequestTask<ReqT, RespT> requestTask = inflightTaskQueue.poll();
                if (requestTask != null) {
                    requestTask.finish(resp);
                } else {
                    onError(new IllegalStateException("No matching request found for the response frame"));
                }
            } else {
                log.debug("Drop response from orphan stream");
                throw new IllegalStateException();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            terminated = true;
            if (throwable.getCause() instanceof IllegalStateException) {
                // orphan stream
                return;
            }
            log.trace("ReqPipeline@{} of {} internal stream@{} error: state={}",
                ManagedRequestPipeline.this.hashCode(),
                methodDescriptor.getBareMethodName(),
                requestStream.hashCode(), state.get(), throwable);
            RPCMeters.recordCount(meterKey, RPCMetric.ReqPipelineErrorCount);
            synchronized (ManagedRequestPipeline.this) {
                abortFlightRequests(Constants.toConcreteException(throwable));
                if (requestStream != null && requester.compareAndSet(requestStream, null)) {
                    // if the requestStream is the active, complete it
                    requestStream.onCompleted();
                }
                if (state.get() == State.Closed) {
                    return;
                }
                if (semantic instanceof BluePrint.DDBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to server[{}]",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName(),
                            selectedServerId.get());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WCHBalanced) {
                    if (desiredServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to server[{}]",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName(),
                            desiredServerId.get());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WRBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to random server",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WRRBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to next server",
                            ManagedRequestPipeline.this.hashCode(), methodDescriptor.getBareMethodName());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                }
            }
        }

        @Override
        public void onCompleted() {
            terminated = true;
            log.trace("ReqPipeline@{} of {} internal stream@{} completed",
                ManagedRequestPipeline.this.hashCode(),
                methodDescriptor.getBareMethodName(),
                requestStream.hashCode());
            synchronized (ManagedRequestPipeline.this) {
                prepareForReFly();
                requester.set(null);
                if (state.get() == State.Closed) {
                    return;
                }
                if (semantic instanceof BluePrint.DDBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to server[{}]",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName(),
                            selectedServerId.get());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WCHBalanced) {
                    if (desiredServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to server[{}]",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName(),
                            desiredServerId.get());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WRBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to random server",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                } else if (semantic instanceof BluePrint.WRRBalanced) {
                    if (selectedServerId.get() != null) {
                        log.trace("ReqPipeline@{} of {} schedule targeting to next server",
                            ManagedRequestPipeline.this.hashCode(),
                            methodDescriptor.getBareMethodName());
                        selectedServerId.set(null);
                        scheduleSignal();
                    }
                }
            }
        }
    }
}
