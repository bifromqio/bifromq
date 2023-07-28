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

import com.baidu.bifromq.baserpc.metrics.RPCMeters;
import io.grpc.Context;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

public class RPCContext {
    public static final String GPID = UUID.randomUUID().toString();

    public static class ServerSelection {
        @Getter
        @Setter
        private String serverId;
    }

    public static final Context.Key<String> TENANT_ID_CTX_KEY = Context.key("TenantId");
    public static final Context.Key<RPCMeters.MeterKey> METER_KEY_CTX_KEY = Context.key("MeterKey");
    public static final Context.Key<String> DESIRED_SERVER_ID_CTX_KEY = Context.key("DesiredServerId");
    public static final Context.Key<ServerSelection> SELECTED_SERVER_ID_CTX_KEY = Context.key("SelectedServerId");
    public static final Context.Key<String> WCH_HASH_KEY_CTX_KEY = Context.key("WeightConsistentHashKey");
    public static final Context.Key<Map<String, String>> CUSTOM_METADATA_CTX_KEY = Context.key("CustomMetadata");
}
