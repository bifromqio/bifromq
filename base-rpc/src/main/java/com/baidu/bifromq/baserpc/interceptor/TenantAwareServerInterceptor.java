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

package com.baidu.bifromq.baserpc.interceptor;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.loadbalancer.Constants;
import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.baserpc.proto.PipelineMetadata;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TenantAwareServerInterceptor implements ServerInterceptor {
    private static final ServerCall.Listener NOOP_LISTENER = new ServerCall.Listener<>() {
    };
    // top key: methodFullName
    // nest key: tenantId
    private final Map<String, LoadingCache<String, RPCMeters.MeterKey>> meterKeys = new HashMap<>();

    public TenantAwareServerInterceptor(ServerServiceDefinition serviceDefinition) {
        ServiceDescriptor serviceDescriptor = serviceDefinition.getServiceDescriptor();
        for (MethodDescriptor<?, ?> methodDesc : serviceDescriptor.getMethods()) {
            meterKeys.put(methodDesc.getFullMethodName(), Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofSeconds(30))
                .build(tenantId -> RPCMeters.MeterKey.builder()
                    .service(serviceDescriptor.getName())
                    .method(methodDesc.getBareMethodName())
                    .tenantId(tenantId)
                    .build()));
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        try {
            Context ctx = Context.current();
            assert headers.containsKey(Constants.TENANT_ID_META_KEY);
            String tenantId = headers.get(Constants.TENANT_ID_META_KEY);
            ctx = ctx.withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId);

            if (headers.containsKey(Constants.WCH_KEY_META_KEY)) {
                ctx = ctx.withValue(RPCContext.WCH_HASH_KEY_CTX_KEY, headers.get(Constants.WCH_KEY_META_KEY));
            }

            if (headers.containsKey(Constants.CUSTOM_METADATA_META_KEY)) {
                PipelineMetadata metadata = PipelineMetadata.parseFrom(headers.get(Constants.CUSTOM_METADATA_META_KEY));
                ctx = ctx.withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadata.getEntryMap());
            } else {
                ctx = ctx.withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, Collections.emptyMap());
            }
            ctx = ctx.withValue(RPCContext.METER_KEY_CTX_KEY,
                meterKeys.get(call.getMethodDescriptor().getFullMethodName()).get(tenantId));

            ServerCall.Listener<ReqT> listener = Contexts.interceptCall(ctx, call, headers, next);
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                @Override
                public void onHalfClose() {
                    try {
                        super.onHalfClose();
                    } catch (Exception e) {
                        log.error("Failed to execute server call.", e);
                        call.close(Status.INTERNAL.withCause(e).withDescription(e.getMessage()), headers);
                    }
                }
            };
        } catch (UnsupportedOperationException e) {
            log.error("Failed to determine traffic identifier from the call", e);
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid Client Certificate"), headers);
            return NOOP_LISTENER;
        } catch (Throwable e) {
            log.error("Failed to make server call", e);
            call.close(Status.INTERNAL.withDescription("Server handling request error"), headers);
            return NOOP_LISTENER;
        }
    }
}
