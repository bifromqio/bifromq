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

package com.baidu.bifromq.baserpc.loadbalancer;

import com.baidu.bifromq.baserpc.exception.RequestCanceledException;
import com.baidu.bifromq.baserpc.exception.RequestThrottledException;
import com.baidu.bifromq.baserpc.exception.ServerNotFoundException;
import com.baidu.bifromq.baserpc.exception.ServerUnreachableException;
import com.baidu.bifromq.baserpc.exception.ServiceUnavailableException;
import com.baidu.bifromq.baserpc.exception.TransientFailureException;
import io.grpc.Attributes;
import io.grpc.ConnectivityStateInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Constants {
    public static final Attributes.Key<AtomicReference<ConnectivityStateInfo>> STATE_INFO =
        Attributes.Key.create("state-info");

    public static final Attributes.Key<Map<String, Map<String, Integer>>> TRAFFIC_DIRECTIVE_ATTR_KEY =
        Attributes.Key.create("TrafficDirective");
    public static final Attributes.Key<String> SERVER_ID_ATTR_KEY = Attributes.Key.create("ServerId");
    public static final Attributes.Key<Set<String>> SERVER_GROUP_TAG_ATTR_KEY = Attributes.Key.create("ServerGroupTag");
    public static final Attributes.Key<Boolean> IN_PROC_SERVER_ATTR_KEY = Attributes.Key.create("InProc");

    public static final Metadata.Key<String> TENANT_ID_META_KEY =
        Metadata.Key.of("tenant_id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> DESIRED_SERVER_META_KEY =
        Metadata.Key.of("desired_server_id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> WCH_KEY_META_KEY =
        Metadata.Key.of("wch_hash_key", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> COLLECT_SELECTION_METADATA_META_KEY =
        Metadata.Key.of("collect_selection", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> CUSTOM_METADATA_META_KEY =
        Metadata.Key.of("custom_metadata-bin", Metadata.BINARY_BYTE_MARSHALLER);

    public static final Status TRANSIENT_FAILURE = Status.UNAVAILABLE.withDescription("transient failure");
    public static final Status SERVICE_UNAVAILABLE = Status.UNAVAILABLE.withDescription("service unavailable");
    public static final Status REQUEST_THROTTLED =
        Status.RESOURCE_EXHAUSTED.withDescription("request dropped due to downstream overloaded");
    public static final Status SERVER_NOT_FOUND =
        Status.UNAVAILABLE.withDescription("direct targeted server not found");
    public static final Status SERVER_UNREACHABLE =
        Status.UNAVAILABLE.withDescription("server is unreachable now");

    public static Throwable toConcreteException(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) throwable;
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
