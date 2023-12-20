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

import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import com.baidu.bifromq.baserpc.metrics.RPCMetric;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class UnaryResponse {
    public static <Resp> void response(Function<String, CompletionStage<Resp>> reqHandler,
                                       StreamObserver<Resp> observer) {
        response((tenantId, metadata) -> reqHandler.apply(tenantId), observer);
    }

    public static <Resp> void response(BiFunction<String, Map<String, String>, CompletionStage<Resp>> reqHandler,
                                       StreamObserver<Resp> observer) {
        RPCMeters.MeterKey meterKey = RPCContext.METER_KEY_CTX_KEY.get();
        String tenantId = RPCContext.TENANT_ID_CTX_KEY.get();
        Map<String, String> metadata = RPCContext.CUSTOM_METADATA_CTX_KEY.get();
        Timer.Sample sample = Timer.start();
        RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqReceivedCount);
        reqHandler.apply(tenantId, metadata)
            .whenComplete((v, e) -> {
                sample.stop(RPCMeters.timer(meterKey, RPCMetric.UnaryReqProcessLatency));
                if (e != null) {
                    observer.onError(e);
                    RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqFailCount);
                } else {
                    observer.onNext(v);
                    observer.onCompleted();
                    RPCMeters.recordCount(meterKey, RPCMetric.UnaryReqFulfillCount);
                }
            });
    }
}
