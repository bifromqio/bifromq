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

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.dist.rpc.proto.BatchMatchReply;
import com.baidu.bifromq.dist.rpc.proto.BatchUnmatchReply;
import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.type.QoS;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class SubUnsubTest extends DistWorkerTest {
    @Test(groups = "integration")
    public void normalSub() {
        String topicFilter = "/a/b/c";

        BatchMatchReply.Result result = sub("tenantA", topicFilter, AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub("tenantA", topicFilter, QoS.AT_LEAST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub("tenantA", topicFilter, EXACTLY_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedSub() {
        String topicFilter = "$share/group/a/b/c";

        BatchMatchReply.Result result = sub(tenantA, topicFilter, AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub(tenantA, topicFilter, AT_LEAST_ONCE, MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub(tenantA, topicFilter, EXACTLY_ONCE, MqttBroker, "inbox1", "server3");
        assertEquals(result, BatchMatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void normalUnsub() {
        BatchUnmatchReply.Result result = unsub(tenantB, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.NOT_EXISTED);
        sub(tenantA, "/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        result = unsub(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        sub(tenantA, "/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server2");
        result = unsub(tenantA, "/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        sub(tenantA, "/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server3");
        result = unsub(tenantA, "/a/b/c", MqttBroker, "inbox1", "server3");
        assertEquals(result, BatchUnmatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedUnsub() {
        BatchUnmatchReply.Result result = unsub(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.NOT_EXISTED);
        sub(tenantA, "$share/group/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        result = unsub(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        sub(tenantA, "$share/group/a/b/c", AT_LEAST_ONCE, MqttBroker, "inbox1", "server2");
        result = unsub(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.Result.OK);

        sub(tenantA, "$share/group/a/b/c", EXACTLY_ONCE, MqttBroker, "inbox1", "server3");
        result = unsub(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server3");
        assertEquals(result, BatchUnmatchReply.Result.OK);
    }

    @Test(groups = "integration")
    public void sharedSubExceedLimit() {
        when(settingProvider.provide(Setting.MaxSharedGroupMembers, tenantA)).thenReturn(2);
        BatchMatchReply.Result result =
            sub(tenantA, "$share/sharedSubExceedLimit/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
        result = sub(tenantA, "$share/sharedSubExceedLimit/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox2", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub(tenantA, "$share/sharedSubExceedLimit/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox2", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);

        result = sub(tenantA, "$share/sharedSubExceedLimit/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox3", "server1");
        assertEquals(result, BatchMatchReply.Result.EXCEED_LIMIT);

        unsub(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox2", "server1");

        result = sub(tenantA, "$share/sharedSubExceedLimit/a/b/c", AT_MOST_ONCE, MqttBroker, "inbox3", "server1");
        assertEquals(result, BatchMatchReply.Result.OK);
    }
}
