/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.baserpc.client.exception;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class ExceptionUtil {
    public static final Status TRANSIENT_FAILURE = Status.UNAVAILABLE.withDescription("transient failure");
    public static final Status SERVICE_UNAVAILABLE = Status.UNAVAILABLE.withDescription("service unavailable");
    public static final Status REQUEST_THROTTLED =
        Status.RESOURCE_EXHAUSTED.withDescription("request dropped due to downstream overloaded");
    public static final Status SERVER_NOT_FOUND =
        Status.UNAVAILABLE.withDescription("direct targeted server not found");
    public static final Status SERVER_UNREACHABLE =
        Status.UNAVAILABLE.withDescription("server is unreachable now");

    public static Throwable toConcreteException(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException statusRuntimeException) {
            if (statusRuntimeException.getStatus().equals(TRANSIENT_FAILURE)) {
                return new TransientFailureException(throwable);
            }
            if (statusRuntimeException.getStatus().equals(REQUEST_THROTTLED)) {
                return new RequestThrottledException(throwable);
            }
            if (statusRuntimeException.getStatus().equals(SERVICE_UNAVAILABLE)) {
                return new ServiceUnavailableException(throwable);
            }
            if (statusRuntimeException.getStatus().equals(SERVER_NOT_FOUND)) {
                return new ServerNotFoundException(throwable);
            }
            if (statusRuntimeException.getStatus().equals(SERVER_UNREACHABLE)) {
                return new ServerUnreachableException(throwable);
            }
            if (statusRuntimeException.getStatus().equals(Status.CANCELLED)) {
                return new RequestCanceledException(throwable);
            }
        }
        return throwable;
    }
}
