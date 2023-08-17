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

package com.baidu.bifromq.inbox.util;

import com.baidu.bifromq.type.ClientInfo;
import java.util.Base64;
import lombok.SneakyThrows;

public class PipelineUtil {
    public static final String PIPELINE_ATTR_KEY_INBOX_ID = "0";
    public static final String PIPELINE_ATTR_KEY_QOS0_LAST_FETCH_SEQ = "1";
    public static final String PIPELINE_ATTR_KEY_QOS2_LAST_FETCH_SEQ = "2";

    public static String encode(ClientInfo clientInfo) {
        return Base64.getEncoder().encodeToString(clientInfo.toByteArray());
    }

    @SneakyThrows
    public static ClientInfo decode(String clientInfoBase64) {
        return ClientInfo.parseFrom(Base64.getDecoder().decode(clientInfoBase64));
    }
}
