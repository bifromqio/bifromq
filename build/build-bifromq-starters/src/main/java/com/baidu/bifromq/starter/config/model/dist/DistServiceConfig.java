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

package com.baidu.bifromq.starter.config.model.dist;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DistServiceConfig {
    @JsonSetter(nulls = Nulls.SKIP)
    private DistClientConfig client = new DistClientConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private DistServerConfig server = new DistServerConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private DistWorkerClientConfig workerClient = new DistWorkerClientConfig();

    @JsonSetter(nulls = Nulls.SKIP)
    private DistWorkerConfig worker = new DistWorkerConfig();
}