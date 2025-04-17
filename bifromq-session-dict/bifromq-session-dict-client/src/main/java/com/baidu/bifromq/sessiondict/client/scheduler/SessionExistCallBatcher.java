/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.sessiondict.client.scheduler;

import static com.baidu.bifromq.sessiondict.SessionRegisterKeyUtil.parseTenantId;

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.sessiondict.client.type.ExistResult;
import com.baidu.bifromq.sessiondict.client.type.TenantClientId;
import com.baidu.bifromq.sessiondict.rpc.proto.ExistReply;
import com.baidu.bifromq.sessiondict.rpc.proto.ExistRequest;
import com.baidu.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import java.util.Collections;

class SessionExistCallBatcher extends Batcher<TenantClientId, ExistResult, String> {
    private final IRPCClient.IRequestPipeline<ExistRequest, ExistReply> ppln;

    protected SessionExistCallBatcher(IRPCClient rpcClient,
                                      String batcherKey,
                                      String name,
                                      long tolerableLatencyNanos,
                                      long burstLatencyNanos) {
        super(batcherKey, name, tolerableLatencyNanos, burstLatencyNanos);
        ppln = rpcClient.createRequestPipeline(parseTenantId(batcherKey), null,
            batcherKey, Collections.emptyMap(), SessionDictServiceGrpc.getExistMethod());
    }

    @Override
    protected IBatchCall<TenantClientId, ExistResult, String> newBatch() {
        return new BatchSessionExistCall(ppln);
    }

    @Override
    public void close() {
        super.close();
        ppln.close();
    }
}
