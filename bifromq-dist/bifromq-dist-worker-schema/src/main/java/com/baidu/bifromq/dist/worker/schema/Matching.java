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

package com.baidu.bifromq.dist.worker.schema;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public abstract class Matching {
    public enum Type {
        Normal, Group
    }

    public final String tenantId;

    @EqualsAndHashCode.Exclude
    public final String escapedTopicFilter;

    protected Matching(RouteDetail detail) {
        this.tenantId = detail.tenantId();
        this.escapedTopicFilter = detail.escapedTopicFilter();
    }

    public abstract Type type();

    public abstract String originalTopicFilter();
}
