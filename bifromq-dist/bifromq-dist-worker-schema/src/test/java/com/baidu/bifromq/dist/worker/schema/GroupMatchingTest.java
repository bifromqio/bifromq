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

package com.baidu.bifromq.dist.worker.schema;


import static com.baidu.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.rpc.proto.RouteGroup;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class GroupMatchingTest {

    @Test
    public void groupMatch() {
        matchInfo("$share/group1/home/sensor/temperature");
        matchInfo("$oshare/group1/home/sensor/temperature");
    }

    private void matchInfo(String topicFilter) {
        String tenantId = "tenant1";

        ByteString key = KVSchemaUtil.toGroupRouteKey(tenantId, topicFilter);
        RouteGroup groupMembers = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox1", "deliverer1"), 1)
            .build();

        GroupMatching matching = (GroupMatching) KVSchemaUtil.buildMatchRoute(key, groupMembers.toByteString());

        for (NormalMatching normalMatching : matching.receiverList) {
            assertEquals(normalMatching.matchInfo().getTopicFilter(), topicFilter);
        }
    }
}