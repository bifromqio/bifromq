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

package com.baidu.bifromq.dist.worker.scheduler;

import com.baidu.bifromq.type.SubInfo;

public class DeliveryRequest {
    public final SubInfo subInfo;
    public final MessagePackWrapper msgPackWrapper;
    public final DelivererKey writerKey;

    public DeliveryRequest(SubInfo subInfo, int brokerId, String delivererKey, MessagePackWrapper msgPackWrapper) {
        this.subInfo = subInfo;
        this.msgPackWrapper = msgPackWrapper;
        writerKey = new DelivererKey(brokerId, delivererKey);
    }
}
