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

package com.baidu.bifromq.baserpc.interceptor;

import com.baidu.bifromq.baserpc.RPCContext;
import com.baidu.bifromq.baserpc.loadbalancer.Constants;
import com.baidu.bifromq.baserpc.proto.PipelineMetadata;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficAwareClientInterceptor implements ClientInterceptor {
    private final String serviceUniqueName;

    public TrafficAwareClientInterceptor() {
        this(null);
    }

    // used in in-process mode
    public TrafficAwareClientInterceptor(String serviceUniqueName) {
        this.serviceUniqueName = serviceUniqueName;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(Constants.TRAFFIC_ID_META_KEY, RPCContext.TRAFFIC_ID_CTX_KEY.get());
                if (RPCContext.DESIRED_SERVER_ID_CTX_KEY.get() != null) {
                    headers.put(Constants.DESIRED_SERVER_META_KEY, RPCContext.DESIRED_SERVER_ID_CTX_KEY.get());
                }
                if (RPCContext.WCH_HASH_KEY_CTX_KEY.get() != null) {
                    headers.put(Constants.WCH_KEY_META_KEY, RPCContext.WCH_HASH_KEY_CTX_KEY.get());
                }
                if (RPCContext.CUSTOM_METADATA_CTX_KEY.get() != null) {
                    headers.put(Constants.CUSTOM_METADATA_META_KEY, PipelineMetadata.newBuilder()
                        .putAllEntry(RPCContext.CUSTOM_METADATA_CTX_KEY.get()).build().toByteArray());
                }
                headers.put(Constants.COLLECT_SELECTION_METADATA_META_KEY,
                    Boolean.toString(RPCContext.SELECTED_SERVER_ID_CTX_KEY.get() != null));
                if (RPCContext.SELECTED_SERVER_ID_CTX_KEY.get() != null && serviceUniqueName != null) {
                    // in-process mode will bypass lb
                    RPCContext.SELECTED_SERVER_ID_CTX_KEY.get().setServerId(serviceUniqueName);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
