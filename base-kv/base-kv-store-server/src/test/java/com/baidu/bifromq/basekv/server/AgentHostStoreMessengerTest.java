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

package com.baidu.bifromq.basekv.server;

import static com.baidu.bifromq.basekv.server.AgentHostStoreMessenger.agentId;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.IAgentHost;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMessage;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgentMember;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.StoreMessage;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.subjects.PublishSubject;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class AgentHostStoreMessengerTest {
    @Mock
    private IAgentHost agentHost;
    @Mock
    private IAgent agent;

    private String clusterId = "testCluster";
    private String srcStore = "store1";

    @Mock
    private IAgentMember srcStoreAgentMember;
    private KVRangeId srcRange;
    private String targetStore = "store2";
    private PublishSubject<AgentMessage> tgtStoreMessageSubject;
    @Mock
    private IAgentMember tgtStoreAgentMember;
    private KVRangeId targetRange;
    private AutoCloseable closeable;
    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        tgtStoreMessageSubject = PublishSubject.create();
        srcRange = KVRangeIdUtil.generate();
        targetRange = KVRangeIdUtil.generate();
        when(agentHost.host(agentId(clusterId))).thenReturn(agent);
        when(agent.register(srcStore)).thenReturn(srcStoreAgentMember);
        when(agent.register(targetStore)).thenReturn(tgtStoreAgentMember);
        when(tgtStoreAgentMember.receive()).thenReturn(tgtStoreMessageSubject);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void init() {
        when(agentHost.host(agentId(clusterId))).thenReturn(agent);
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);

        ArgumentCaptor<String> agentMemberCap = ArgumentCaptor.forClass(String.class);
        verify(agent).register(agentMemberCap.capture());
        assertEquals(srcStore, agentMemberCap.getValue());
    }

    @Test
    public void send() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);
        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setHostStoreId(targetStore).setRangeId(targetRange).build())
            .build();
        messenger.send(message);
        ArgumentCaptor<String> targetMemberCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ByteString> msgCap = ArgumentCaptor.forClass(ByteString.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(srcStoreAgentMember).multicast(targetMemberCap.capture(), msgCap.capture(),
            reliableCap.capture());

        assertEquals(targetStore, targetMemberCap.getValue());
        assertEquals(message.toByteString(), msgCap.getValue());
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void broadcast() {
        when(agentHost.host(agentId(clusterId))).thenReturn(agent);
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);
        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setRangeId(targetRange).build())
            .build();
        messenger.send(message);
        ArgumentCaptor<ByteString> msgCap = ArgumentCaptor.forClass(ByteString.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(srcStoreAgentMember).broadcast(msgCap.capture(), reliableCap.capture());
        assertEquals(message.toByteString(), msgCap.getValue());
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void receiveSend() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, targetStore);
        TestObserver<StoreMessage> testObserver = TestObserver.create();
        messenger.receive().subscribe(testObserver);

        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setHostStoreId(targetStore).setRangeId(targetRange).build())
            .build();
        AgentMessage nodeMessage = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(srcStore).build())
            .setPayload(message.toByteString())
            .build();
        tgtStoreMessageSubject.onNext(nodeMessage);
        testObserver.awaitCount(1);
        assertEquals(message, testObserver.values().get(0));
    }

    @Test
    public void receiveBroadcast() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, targetStore);
        TestObserver<StoreMessage> testObserver = TestObserver.create();
        messenger.receive().subscribe(testObserver);

        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setRangeId(targetRange).build())
            .build();
        AgentMessage nodeMessage = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(srcStore).build())
            .setPayload(message.toByteString())
            .build();
        tgtStoreMessageSubject.onNext(nodeMessage);
        testObserver.awaitCount(1);
        assertEquals(targetStore, testObserver.values().get(0).getPayload().getHostStoreId());
    }
}
