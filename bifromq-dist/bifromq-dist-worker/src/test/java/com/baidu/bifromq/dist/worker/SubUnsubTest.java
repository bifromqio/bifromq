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

package com.baidu.bifromq.dist.worker;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class SubUnsubTest extends DistWorkerTest {
    @Test(groups = "integration")
    public void normalSub() {
        String topicFilter = "/a/b/c";

        BatchMatchReply.Result result = match("tenantA", topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = match("tenantA", topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedSub() {
        String topicFilter = "$share/group/a/b/c";

        BatchMatchReply.Result result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchMatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void normalUnsub() {
        BatchUnmatchReply.Result result = unmatch(tenantB, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.NOT_EXISTED);
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        result = unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server2");
        result = unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedUnsub() {
        BatchUnmatchReply.Result result = unmatch(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.NOT_EXISTED);
        match(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        result = unmatch(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        match(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");
        result = unmatch(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedSubExceedLimit() {
        when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantA)).thenReturn(2);
        BatchMatchReply.Result result =
            match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox2", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox3", "server1");
        assertEquals(result, BatchMatchReply.Result.EXCEED_LIMIT);

        unmatch(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox2", "server1");

        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox3", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
    }
}
