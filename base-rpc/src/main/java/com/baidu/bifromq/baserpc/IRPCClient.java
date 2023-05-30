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

import static java.util.Collections.emptyMap;

import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public interface IRPCClient {
    enum ConnState {
        CONNECTING,

        READY,

        TRANSIENT_FAILURE,

        IDLE,

        SHUTDOWN
    }

    static RPCClientBuilder builder() {
        return new RPCClientBuilder();
    }

    interface IRequestPipeline<ReqT, RespT> {
        boolean isClosed();

        CompletableFuture<RespT> invoke(ReqT req);

        void close();
    }

    interface IMessageStream<MsgT, AckT> {
        boolean isClosed();

        void ack(AckT msg);

        Observable<MsgT> msg();

        void close();
    }

    /**
     * The observable of live servers
     *
     * @return
     */
    Observable<Set<String>> serverList();

    /**
     * The observable of rpc connectivity state
     *
     * @return
     */
    Observable<ConnState> connState();

    default <ReqT, RespT> CompletableFuture<RespT> invoke(String trafficId,
                                                          @Nullable String desiredServerId,
                                                          ReqT req,
                                                          MethodDescriptor<ReqT, RespT> methodDesc) {
        return invoke(trafficId, desiredServerId, req, emptyMap(), methodDesc);
    }

    <ReqT, RespT> CompletableFuture<RespT> invoke(String trafficId,
                                                  @Nullable String desiredServerId,
                                                  ReqT req,
                                                  Map<String, String> metadata,
                                                  MethodDescriptor<ReqT, RespT> methodDesc);

    /**
     * Create a caller-managed auto-rebalanced request-response pipeline
     *
     * @param trafficId
     * @param desiredServerId
     * @param wchKey
     * @param metadata        associated with the pipeline
     * @param methodDesc
     * @return
     */
    default <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String trafficId,
                                                                              @Nullable String desiredServerId,
                                                                              @Nullable String wchKey,
                                                                              Map<String, String> metadata,
                                                                              MethodDescriptor<ReqT, RespT>
                                                                                  methodDesc) {
        return createRequestPipeline(trafficId, desiredServerId, wchKey, () -> metadata, methodDesc);
    }

    default <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String trafficId,
                                                                              @Nullable String desiredServerId,
                                                                              @Nullable String wchKey,
                                                                              Map<String, String> metadata,
                                                                              MethodDescriptor<ReqT, RespT> methodDesc,
                                                                              Executor executor) {
        return createRequestPipeline(trafficId, desiredServerId, wchKey, () -> metadata, methodDesc, executor);
    }


    /**
     * Create a caller-managed auto-rebalanced request-response pipeline with default executor
     *
     * @param trafficId
     * @param desiredServerId
     * @param wchKey
     * @param metadataSupplier supply the metadata of the pipeline
     * @param methodDesc
     * @return
     */
    <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String trafficId,
                                                                      @Nullable String desiredServerId,
                                                                      @Nullable String wchKey,
                                                                      Supplier<Map<String, String>> metadataSupplier,
                                                                      MethodDescriptor<ReqT, RespT> methodDesc);

    /**
     * Create a caller-managed auto-rebalanced request-response pipeline with specified executor
     *
     * @param trafficId
     * @param desiredServerId
     * @param wchKey
     * @param metadataSupplier supply the metadata of the pipeline
     * @param methodDesc
     * @param executor         the executor for async callback
     * @return
     */
    <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String trafficId,
                                                                      @Nullable String desiredServerId,
                                                                      @Nullable String wchKey,
                                                                      Supplier<Map<String, String>> metadataSupplier,
                                                                      MethodDescriptor<ReqT, RespT> methodDesc,
                                                                      Executor executor);

    /**
     * Create a caller-managed auto-rebalanced bi-directional message stream with at-most-once delivery guarantee.
     *
     * @param trafficId
     * @param desiredServerId
     * @param wchKey
     * @param metadata
     * @param methodDesc
     * @return
     */
    default <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String trafficId,
                                                                        @Nullable String desiredServerId,
                                                                        @Nullable String wchKey,
                                                                        Map<String, String> metadata,
                                                                        MethodDescriptor<AckT, MsgT> methodDesc) {
        return createMessageStream(trafficId, desiredServerId, wchKey, () -> metadata, methodDesc);
    }

    <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String trafficId,
                                                                @Nullable String desiredServerId,
                                                                @Nullable String wchKey,
                                                                Supplier<Map<String, String>> metadataSupplier,
                                                                MethodDescriptor<AckT, MsgT> methodDesc);


    void stop();
}
