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

import static com.baidu.bifromq.baserpc.RPCContext.CUSTOM_METADATA_CTX_KEY;
import static com.baidu.bifromq.baserpc.RPCContext.DESIRED_SERVER_ID_CTX_KEY;
import static com.baidu.bifromq.baserpc.RPCContext.SELECTED_SERVER_ID_CTX_KEY;
import static com.baidu.bifromq.baserpc.RPCContext.TENANT_ID_CTX_KEY;
import static com.baidu.bifromq.baserpc.RPCContext.WCH_HASH_KEY_CTX_KEY;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;

import com.baidu.bifromq.baserpc.loadbalancer.Constants;
import com.baidu.bifromq.baserpc.loadbalancer.IUpdateListener;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.core.Observable;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class RPCClient implements IRPCClient {
    private final String serviceUniqueName;
    private final BluePrint bluePrint;
    private final ChannelHolder channelHolder;
    private final CallOptions defaultCallOptions;
    private final Map<String, AtomicInteger> unaryInflightCounts = Maps.newHashMap();
    private final Map<String, LoadingCache<String, RPCMeters.MeterKey>> unaryMeterKeys = Maps.newHashMap();

    RPCClient(@NonNull String serviceUniqueName,
              @NonNull BluePrint bluePrint,
              @NonNull ChannelHolder channelHolder) {
        this.serviceUniqueName = serviceUniqueName;
        this.channelHolder = channelHolder;
        this.bluePrint = bluePrint;
        this.defaultCallOptions = CallOptions.DEFAULT;
        for (String fullMethodName : bluePrint.allMethods()) {
            if (bluePrint.semantic(fullMethodName) instanceof BluePrint.Unary) {
                MethodDescriptor<?, ?> methodDesc = bluePrint.methodDesc(fullMethodName);
                unaryInflightCounts.put(fullMethodName, new AtomicInteger());
                unaryMeterKeys.put(fullMethodName, Caffeine.newBuilder()
                    .expireAfterAccess(Duration.ofSeconds(30))
                    .build(tenantId -> RPCMeters.MeterKey.builder()
                        .service(serviceUniqueName)
                        .method(methodDesc.getBareMethodName())
                        .tenantId(tenantId)
                        .build()));
            }
        }
    }

    public void stop() {
        this.channelHolder.shutdown(5, TimeUnit.SECONDS);
    }

    @Override
    public Observable<Set<String>> serverList() {
        return channelHolder.serverList();
    }

    @Override
    public Observable<ConnState> connState() {
        return channelHolder.connState();
    }

    public <Req, Resp> CompletableFuture<Resp> invoke(String tenantId,
                                                      @Nullable String desiredServerId,
                                                      Req req,
                                                      @NonNull Map<String, String> metadata,
                                                      MethodDescriptor<Req, Resp> methodDesc) {
        assert methodDesc.getType() == MethodDescriptor.MethodType.UNARY;
        BluePrint.MethodSemantic<?> semantic = bluePrint.semantic(methodDesc.getFullMethodName());
        assert semantic instanceof BluePrint.Unary;
        assert !(semantic instanceof BluePrint.DDBalanced) || desiredServerId != null;
        Context ctx = prepareContext(tenantId, desiredServerId, metadata);
        if (semantic instanceof BluePrint.WCHBalancedReq) {
            @SuppressWarnings("unchecked")
            String wchKey = ((BluePrint.WCHBalancedReq<Req>) semantic).hashKey(req);
            assert wchKey != null;
            ctx = ctx.withValue(WCH_HASH_KEY_CTX_KEY, wchKey);
        }
        RPCMeters.MeterKey meterKey = unaryMeterKeys.get(methodDesc.getFullMethodName()).get(tenantId);
        AtomicInteger counter = unaryInflightCounts.get(methodDesc.getFullMethodName());
        ctx = ctx.attach();
        try {
            long startNano = System.nanoTime();
            CompletableFuture<Resp> future = new CompletableFuture<>();
            int currentCount = counter.incrementAndGet();
            RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqSendCount);
            RPCMeters.recordSummary(meterKey, RPCMetric.UnaryReqDepth, currentCount);
            MethodDescriptor<Req, Resp> md = bluePrint.methodDesc(methodDesc.getFullMethodName());
            asyncUnaryCall(this.channelHolder.channel().newCall(md, defaultCallOptions), req,
                new StreamObserver<>() {
                    @Override
                    public void onNext(Resp resp) {
                        long l = System.nanoTime() - startNano;
                        RPCMeters.timer(meterKey, RPCMetric.UnaryReqLatency).record(l, TimeUnit.NANOSECONDS);
                        future.complete(resp);
                        RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqCompleteCount);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.error("Unary call of method {} failure:\n{}",
                            methodDesc.getFullMethodName(), req, throwable);
                        future.completeExceptionally(Constants.toConcreteException(throwable));
                        RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqAbortCount);
                    }

                    @Override
                    public void onCompleted() {
                        counter.decrementAndGet();
                        // do nothing
                        RPCMeters.recordSummary(meterKey, RPCMetric.UnaryReqDepth, counter.get());
                    }
                });
            return future;
        } finally {
            Context.current().detach(ctx);
        }
    }

    @Override
    public <Req, Resp> IRequestPipeline<Req, Resp> createRequestPipeline(String tenantId,
                                                                         @Nullable String desiredServerId,
                                                                         @Nullable String wchKey,
                                                                         Supplier<Map<String, String>> metadataSupplier,
                                                                         MethodDescriptor<Req, Resp> methodDesc) {
        return new ManagedRequestPipeline<>(
            tenantId,
            wchKey,
            desiredServerId,
            metadataSupplier,
            serviceUniqueName,
            channelHolder,
            defaultCallOptions,
            methodDesc,
            bluePrint);
    }

    @Override
    public <ReqT, RespT> IRequestPipeline<ReqT, RespT>
    createRequestPipeline(String tenantId,
                          @Nullable String desiredServerId,
                          @Nullable String wchKey,
                          Supplier<Map<String, String>> metadataSupplier,
                          MethodDescriptor<ReqT, RespT> methodDesc,
                          Executor executor) {
        return new ManagedRequestPipeline<>(
            tenantId,
            wchKey,
            desiredServerId,
            metadataSupplier,
            serviceUniqueName,
            channelHolder,
            defaultCallOptions.withExecutor(executor),
            methodDesc,
            bluePrint);
    }

    @Override
    public <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String tenantId,
                                                                       @Nullable String desiredServerId,
                                                                       String wchKey,
                                                                       Supplier<Map<String, String>> metadataSupplier,
                                                                       MethodDescriptor<AckT, MsgT> methodDesc) {
        return new ManagedMessageStream<>(
            tenantId,
            wchKey,
            desiredServerId,
            metadataSupplier,
            serviceUniqueName,
            channelHolder,
            defaultCallOptions,
            methodDesc,
            bluePrint);
    }

    private Context prepareContext(String tenantId, @Nullable String desiredServerId, Map<String, String> metadata) {
        // currently, context attributes from caller are not allowed
        // start a new context by forking from ROOT,so no scaring warning should appear in the log
        return Context.ROOT.fork()
            .withValues(TENANT_ID_CTX_KEY, tenantId,
                DESIRED_SERVER_ID_CTX_KEY, desiredServerId,
                // context key to hold selection result by load balancer
                SELECTED_SERVER_ID_CTX_KEY, null,
                CUSTOM_METADATA_CTX_KEY, metadata);
    }


    interface ChannelHolder {

        Executor rpcExecutor();

        Channel channel();

        Observable<ConnState> connState();

        Observable<Set<String>> serverList();

        Observable<IUpdateListener.IServerSelector> serverSelectorObservable();

        boolean shutdown(long timeout, TimeUnit unit);
    }
}
