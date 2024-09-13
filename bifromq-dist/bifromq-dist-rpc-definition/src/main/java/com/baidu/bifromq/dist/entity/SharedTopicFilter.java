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

package com.baidu.bifromq.dist.entity;

public class SharedTopicFilter {
    public String originTopicFilter;
    public boolean ordered;
    public String shareGroup;
    public String topicFilter;

    public SharedTopicFilter(String originTopicFilter, boolean ordered, String shareName, String filter) {
        this.originTopicFilter = originTopicFilter;
        this.ordered = ordered;
        this.shareGroup = shareName;
        this.topicFilter = filter;
    }
}
