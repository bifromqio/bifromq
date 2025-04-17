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

import com.baidu.bifromq.baserpc.client.IRPCClient;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.sessiondict.SessionRegisterKeyUtil;
import com.baidu.bifromq.sessiondict.client.type.ExistResult;
import com.baidu.bifromq.sessiondict.client.type.TenantClientId;
import com.baidu.bifromq.sysprops.props.DataPlaneBurstLatencyMillis;
import com.baidu.bifromq.sysprops.props.DataPlaneTolerableLatencyMillis;
import java.time.Duration;
import java.util.Optional;

/**
 * Scheduler for the session exist call.
 */
public class SessionExistScheduler extends BatchCallScheduler<TenantClientId, ExistResult, String>
    implements ISessionExistScheduler {
    private final IRPCClient rpcClient;

    public SessionExistScheduler(IRPCClient rpcClient) {
        super("session_dict_exist",
            Duration.ofMillis(DataPlaneTolerableLatencyMillis.INSTANCE.get()),
            Duration.ofMillis(DataPlaneBurstLatencyMillis.INSTANCE.get()));
        this.rpcClient = rpcClient;
    }

    @Override
    protected Batcher<TenantClientId, ExistResult, String> newBatcher(String name,
                                                                  long tolerableLatencyNanos,
                                                                  long burstLatencyNanos,
                                                                  String key) {
        return new SessionExistCallBatcher(rpcClient, key, name, tolerableLatencyNanos, burstLatencyNanos);
    }

    @Override
    protected Optional<String> find(TenantClientId call) {
        return Optional.of(SessionRegisterKeyUtil.toRegisterKey(call.tenantId(), call.userId(), call.clientId()));
    }
}
