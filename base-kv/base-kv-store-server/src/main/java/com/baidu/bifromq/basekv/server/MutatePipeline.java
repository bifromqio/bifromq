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

package com.baidu.bifromq.basekv.server;

import com.baidu.bifromq.basekv.store.IKVRangeStore;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.baserpc.ResponsePipeline;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class MutatePipeline extends ResponsePipeline<KVRangeRWRequest, KVRangeRWReply> {
    private final IKVRangeStore kvRangeStore;

    MutatePipeline(IKVRangeStore kvRangeStore, StreamObserver<KVRangeRWReply> responseObserver) {
        super(responseObserver);
        this.kvRangeStore = kvRangeStore;
    }

    @Override
    protected CompletableFuture<KVRangeRWReply> handleRequest(String s, KVRangeRWRequest request) {
        log.trace("Handling rw range request:req={}", request);
        return switch (request.getRequestTypeCase()) {
            case DELETE -> mutate(request, this::delete).toCompletableFuture();
            case PUT -> mutate(request, this::put).toCompletableFuture();
            default -> mutate(request, this::mutateCoProc).toCompletableFuture();
        };
    }

    private CompletionStage<KVRangeRWReply> delete(KVRangeRWRequest request) {
        return kvRangeStore.delete(request.getVer(), request.getKvRangeId(), request.getDelete())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setDeleteResult(v)
                .build());
    }

    private CompletionStage<KVRangeRWReply> put(KVRangeRWRequest request) {
        return kvRangeStore.put(request.getVer(), request.getKvRangeId(), request.getPut().getKey(),
                request.getPut().getValue())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setPutResult(v)
                .build());
    }

    private CompletionStage<KVRangeRWReply> mutateCoProc(KVRangeRWRequest request) {
        return kvRangeStore.mutateCoProc(request.getVer(), request.getKvRangeId(), request.getRwCoProc())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setRwCoProcResult(v)
                .build());
    }


    private CompletionStage<KVRangeRWReply> mutate(KVRangeRWRequest request, Function<KVRangeRWRequest,
        CompletionStage<KVRangeRWReply>> mutateFn) {
        return mutateFn.apply(request)
            .exceptionally(e -> {
                if (e instanceof KVRangeException.BadVersion || e.getCause() instanceof KVRangeException.BadVersion) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.BadVersion)
                        .build();
                }
                if (e instanceof KVRangeException.TryLater || e.getCause() instanceof KVRangeException.TryLater) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof KVRangeException.BadRequest || e.getCause() instanceof KVRangeException.BadRequest) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.BadRequest)
                        .build();
                }
                log.debug("Handle rw request error: reqId={}", request.getReqId(), e);
                return KVRangeRWReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            });
    }
}
