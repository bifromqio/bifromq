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

package com.baidu.bifromq.retain.utils;

import com.baidu.bifromq.retain.rpc.proto.BatchMatchRequest;
import com.baidu.bifromq.retain.rpc.proto.BatchRetainRequest;
import com.baidu.bifromq.retain.rpc.proto.GCRequest;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;
import com.baidu.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;

public class MessageUtil {
    public static RetainServiceRWCoProcInput buildGCRequest(long reqId,
                                                            long now,
                                                            String tenantId,
                                                            Integer expirySeconds) {
        GCRequest.Builder reqBuilder = GCRequest.newBuilder().setReqId(reqId).setNow(now);
        if (tenantId != null) {
            reqBuilder.setTenantId(tenantId);
        }
        if (expirySeconds != null) {
            reqBuilder.setExpirySeconds(expirySeconds);
        }
        return RetainServiceRWCoProcInput.newBuilder()
            .setGc(reqBuilder.build())
            .build();
    }

    public static RetainServiceRWCoProcInput buildRetainRequest(BatchRetainRequest request) {
        return RetainServiceRWCoProcInput.newBuilder()
            .setBatchRetain(request)
            .build();
    }

    public static RetainServiceROCoProcInput buildMatchRequest(BatchMatchRequest request) {
        return RetainServiceROCoProcInput.newBuilder()
            .setBatchMatch(request)
            .build();
    }
}
