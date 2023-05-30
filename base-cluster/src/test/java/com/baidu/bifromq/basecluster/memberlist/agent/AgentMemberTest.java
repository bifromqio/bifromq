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

package com.baidu.bifromq.basecluster.memberlist.agent;

import static com.baidu.bifromq.basecluster.memberlist.agent.MockUtil.toAgentMemberAddr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import com.baidu.bifromq.basecluster.agent.proto.AgentMessage;
import com.baidu.bifromq.basecluster.agent.proto.AgentMessageEnvelope;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class AgentMemberTest {
    private String agentId = "agentId";
    private ByteString hostId = ByteString.copyFromUtf8("host1");
    private HostEndpoint endpoint1 = HostEndpoint.newBuilder()
        .setId(hostId)
        .setAddress("localhost")
        .setPort(1111)
        .build();
    private HostEndpoint endpoint2 = HostEndpoint.newBuilder()
        .setId(hostId)
        .setAddress("localhost")
        .setPort(2222)
        .build();
    private String agentMemberName = "agentMemberName";
    private AgentMemberAddr agentMemberAddr = AgentMemberAddr.newBuilder()
        .setName(agentMemberName)
        .setEndpoint(endpoint1)
        .build();
    private Replica replica = Replica.newBuilder().setUri(CRDTUtil.toAgentURI(agentId)).build();
    @Mock
    private IAgentMessenger agentMessenger;
    private Scheduler scheduler = Schedulers.from(MoreExecutors.directExecutor()); // make test more deterministic
    @Mock
    private ICRDTStore crdtStore;
    @Mock
    private Supplier<Set<AgentMemberAddr>> memberAddresses;
    @Mock
    private IAgentHostProvider hostProvider;
    @Mock
    private IORMap orMap;
    private PublishSubject<Long> inflationSubject = PublishSubject.create();
    private PublishSubject<Set<HostEndpoint>> hostsSubjects = PublishSubject.create();
    private PublishSubject<AgentMessageEnvelope> messageSubject = PublishSubject.create();

    @Before
    public void setup() {
        when(orMap.execute(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(orMap.inflation()).thenReturn(inflationSubject);
        when(agentMessenger.send(any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        when(agentMessenger.receive()).thenReturn(messageSubject);
    }

    @SneakyThrows
    @Test
    public void updateMetadata() {
        ByteString meta2 = ByteString.copyFromUtf8("Hello");

        AgentMember agentMember = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);
        AgentMemberMetadata metadata = agentMember.metadata();
        assertEquals(ByteString.EMPTY, metadata.getValue());

        agentMember.metadata(ByteString.EMPTY); // nothing will happen to crdt
        assertEquals(metadata, agentMember.metadata());

        agentMember.metadata(meta2);

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap, times(2)).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getAllValues().get(0);
        assertTrue(op instanceof ORMapOperation.ORMapUpdate);
        assertTrue(((ORMapOperation.ORMapUpdate) op).valueOp instanceof MVRegOperation);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(agentMemberName, key.getName());
        assertEquals(endpoint1, key.getEndpoint());

        AgentMemberMetadata agentMemberMetadata1 =
            AgentMemberMetadata.parseFrom(((MVRegOperation) ((ORMapOperation.ORMapUpdate) op).valueOp).value);
        assertEquals(ByteString.EMPTY, agentMemberMetadata1.getValue());
        assertTrue(agentMemberMetadata1.getHlc() > 0);

        op = orMapOpCap.getAllValues().get(1);
        AgentMemberMetadata agentMemberMetadata2 =
            AgentMemberMetadata.parseFrom(((MVRegOperation) ((ORMapOperation.ORMapUpdate) op).valueOp).value);
        assertEquals(meta2, agentMemberMetadata2.getValue());
        assertTrue(agentMemberMetadata1.getHlc() < agentMemberMetadata2.getHlc());
    }

    @Test
    public void broadcast() {
        AgentMember agentMember = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);

        when(memberAddresses.get()).thenReturn(Sets.newHashSet(agentMemberAddr));
        agentMember.broadcast(ByteString.EMPTY, true).join();

        ArgumentCaptor<AgentMessage> msgCap = ArgumentCaptor.forClass(AgentMessage.class);
        ArgumentCaptor<AgentMemberAddr> addrCap = ArgumentCaptor.forClass(AgentMemberAddr.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(agentMessenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(ByteString.EMPTY, msgCap.getValue().getPayload());
        assertEquals(agentMemberAddr, msgCap.getValue().getSender());
        assertEquals(agentMemberAddr, addrCap.getValue());
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void illegalStateAfterDestroy() {
        illegalStateAfterDestroy(agentMember -> agentMember.broadcast(ByteString.EMPTY, true));
        illegalStateAfterDestroy(
            agentMember -> agentMember.send(AgentMemberAddr.getDefaultInstance(), ByteString.EMPTY, true));
        illegalStateAfterDestroy(agentMember -> agentMember.multicast("ReceiverGroup", ByteString.EMPTY, true));
    }

    private void illegalStateAfterDestroy(Consumer<IAgentMember> test) {
        AgentMember agentMember = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);
        agentMember.destroy().join();
        try {
            test.accept(agentMember);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void sendToUnknownTarget() {
        String agentMemberName2 = "agentMember2";
        AgentMember agentMember = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);
        try {
            when(memberAddresses.get()).thenReturn(Sets.newHashSet(agentMemberAddr));
            agentMember.send(AgentMemberAddr.newBuilder().setName(agentMemberName2).setEndpoint(endpoint1).build(),
                ByteString.EMPTY, false).join();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof UnknownHostException);
        }
    }

    @Test
    public void send() {
        String agentMemberName2 = "agentMember2";
        AgentMember agentMember1 = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);
        AgentMemberAddr target = toAgentMemberAddr(agentMemberName2, endpoint1);
        when(memberAddresses.get()).thenReturn(Sets.newHashSet(agentMemberAddr, target));
        agentMember1.send(target, ByteString.EMPTY, false);

        ArgumentCaptor<AgentMessage> msgCap = ArgumentCaptor.forClass(AgentMessage.class);
        ArgumentCaptor<AgentMemberAddr> addrCap = ArgumentCaptor.forClass(AgentMemberAddr.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(agentMessenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(ByteString.EMPTY, msgCap.getValue().getPayload());
        assertEquals(agentMemberName, msgCap.getValue().getSender().getName());
        assertEquals(endpoint1, msgCap.getValue().getSender().getEndpoint());
        assertEquals(agentMemberName2, addrCap.getValue().getName());
        assertEquals(endpoint1, addrCap.getValue().getEndpoint());
        assertFalse(reliableCap.getValue());
    }

    @Test
    public void multicast() {
        String senderName = "sender";
        String receiverGroupName = "receiverGroup";
        AgentMember sender = new AgentMember(toAgentMemberAddr(senderName, endpoint1),
            orMap, agentMessenger, scheduler, memberAddresses);

        when(memberAddresses.get()).thenReturn(Sets.newHashSet(
            toAgentMemberAddr(receiverGroupName, endpoint1),
            toAgentMemberAddr(receiverGroupName, endpoint2)));

        when(agentMessenger.send(any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        sender.multicast(receiverGroupName, ByteString.EMPTY, false);

        ArgumentCaptor<AgentMessage> msgCap = ArgumentCaptor.forClass(AgentMessage.class);
        ArgumentCaptor<AgentMemberAddr> addrCap = ArgumentCaptor.forClass(AgentMemberAddr.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(agentMessenger, times(2)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(ByteString.EMPTY, msgCap.getAllValues().get(0).getPayload());
        assertEquals(1, Sets.newHashSet(msgCap.getAllValues()).size());

        assertEquals(senderName, msgCap.getAllValues().get(0).getSender().getName());
        assertEquals(senderName, msgCap.getAllValues().get(1).getSender().getName());
        assertEquals(endpoint1, msgCap.getAllValues().get(0).getSender().getEndpoint());
        assertEquals(endpoint1, msgCap.getAllValues().get(1).getSender().getEndpoint());

        Set<HostEndpoint> receiverEndpoints = addrCap.getAllValues().stream().map(AgentMemberAddr::getEndpoint).collect(
            Collectors.toSet());
        assertTrue(receiverEndpoints.contains(endpoint1));
        assertTrue(receiverEndpoints.contains(endpoint2));

        assertFalse(reliableCap.getAllValues().get(0));
        assertFalse(reliableCap.getAllValues().get(1));
    }

    @Test
    public void receive() {
        String agentMemberName2 = "agentMember2";
        AgentMember agentMember1 = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);
        AgentMember agentMember2 = new AgentMember(toAgentMemberAddr(agentMemberName2, endpoint1),
            orMap, agentMessenger, scheduler, memberAddresses);


        TestObserver<AgentMessage> testObserver = new TestObserver<>();
        agentMember1.receive().subscribe(testObserver);
        AgentMessage message1 = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(agentMemberName2).setEndpoint(endpoint2).build())
            .setPayload(ByteString.copyFromUtf8("hello"))
            .build();
        AgentMessage message2 = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(agentMemberName2).setEndpoint(endpoint2).build())
            .setPayload(ByteString.copyFromUtf8("world"))
            .build();
        messageSubject.onNext(AgentMessageEnvelope.newBuilder()
            .setReceiver(AgentMemberAddr.newBuilder().setName(agentMemberName).setEndpoint(endpoint2))
            .setMessage(message1)
            .build());
        messageSubject.onNext(AgentMessageEnvelope.newBuilder()
            .setReceiver(AgentMemberAddr.newBuilder().setName(agentMemberName).setEndpoint(endpoint1))
            .setMessage(message2)
            .build());
        testObserver.awaitCount(1);
        assertEquals(message2, testObserver.values().get(0));
    }

    @SneakyThrows
    @Test
    public void destroy() {
        AgentMember agentMember = new AgentMember(agentMemberAddr, orMap, agentMessenger, scheduler, memberAddresses);

        TestObserver<AgentMessage> msgObserver = new TestObserver<>();
        agentMember.receive().subscribe(msgObserver);
        agentMember.destroy().join();

        msgObserver.assertComplete();

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap, atLeast(1)).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getAllValues().get(orMapOpCap.getAllValues().size() - 1);
        assertTrue(op instanceof ORMapOperation.ORMapRemove);
        assertEquals(CausalCRDTType.mvreg, ((ORMapOperation.ORMapRemove) op).valueType);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(key, agentMember.address());
    }
}
