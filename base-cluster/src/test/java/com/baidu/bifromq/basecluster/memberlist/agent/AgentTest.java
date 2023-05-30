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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basecluster.agent.proto.AgentMember;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberAddr;
import com.baidu.bifromq.basecluster.agent.proto.AgentMemberMetadata;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
public class AgentTest {
    private String agentId = "agentA";
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
    private Replica replica = Replica.newBuilder().setUri(CRDTUtil.toAgentURI(agentId)).build();
    @Mock
    private IAgentMessenger agentMessenger;
    private Scheduler scheduler = Schedulers.from(MoreExecutors.directExecutor()); // make test more deterministic
    @Mock
    private ICRDTStore crdtStore;
    @Mock
    private IAgentHostProvider hostProvider;
    @Mock
    private IORMap orMap;
    private PublishSubject<Long> inflationSubject = PublishSubject.create();
    private PublishSubject<Set<HostEndpoint>> hostsSubjects = PublishSubject.create();
    private PublishSubject<AgentMessageEnvelope> messageSubject = PublishSubject.create();

    @Before
    public void setup() {
        when(crdtStore.host(CRDTUtil.toAgentURI(agentId), endpoint1.toByteString())).thenReturn(replica);
        when(crdtStore.get(CRDTUtil.toAgentURI(agentId))).thenReturn(Optional.of(orMap));
        when(orMap.execute(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(orMap.id()).thenReturn(replica);
        when(orMap.inflation()).thenReturn(inflationSubject);
        when(hostProvider.getHostEndpoints()).thenReturn(hostsSubjects);
        when(agentMessenger.receive()).thenReturn(messageSubject);
    }

    @Test
    public void init() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        assertEquals(agentId, agent.id());
        assertEquals(endpoint1, agent.endpoint());
    }

    @SneakyThrows
    @Test
    public void register() {
        String agentMemberName = "agentMember1";
        ByteString meta1 = ByteString.EMPTY;
        ByteString meta2 = ByteString.copyFromUtf8("Hello");

        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register(agentMemberName);
        assertEquals(agentMemberName, agentMember.address().getName());
        assertEquals(endpoint1, agentMember.address().getEndpoint());

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap, times(1)).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getAllValues().get(0);
        assertTrue(op instanceof ORMapOperation.ORMapUpdate);
        assertTrue(((ORMapOperation.ORMapUpdate) op).valueOp instanceof MVRegOperation);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(agentMemberName, key.getName());
        assertEquals(endpoint1, key.getEndpoint());

        AgentMemberMetadata agentMemberMetadata1 =
            AgentMemberMetadata.parseFrom(((MVRegOperation) ((ORMapOperation.ORMapUpdate) op).valueOp).value);
        assertEquals(meta1, agentMemberMetadata1.getValue());
        assertTrue(agentMemberMetadata1.getHlc() > 0);
    }

    @Test
    public void deregister() {
        String agentMemberName = "agentMember1";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register(agentMemberName);
        agent.deregister(agentMember).join();
        // nothing should happen
        agent.deregister(agentMember).join();

        // register again should return distinct object
        IAgentMember newAgentMember = agent.register(agentMemberName);
        assertNotEquals(agentMember, newAgentMember);
    }

    @Test
    public void membership() {
        String agentMemberName = "agentMember1";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        TestObserver<Map<AgentMemberAddr, AgentMemberMetadata>> testObserver = new TestObserver<>();
        agent.membership().subscribe(testObserver);
        IAgentMember agentMember = agent.register(agentMemberName);
        AgentMember member = AgentMember.newBuilder()
            .setAddr(AgentMemberAddr.newBuilder().setName(agentId).setEndpoint(endpoint1).build())
            .setMetadata(AgentMemberMetadata.newBuilder().setValue(ByteString.EMPTY).build())
            .build();
        MockUtil.mockAgentMemberCRDT(orMap, Collections.singletonMap(
            MockUtil.toAgentMemberAddr(agentMemberName, endpoint1),
            MockUtil.toAgentMemberMetadata(ByteString.EMPTY)));
        inflationSubject.onNext(System.currentTimeMillis());
        testObserver.awaitCount(2);
        assertEquals(1, testObserver.values().get(1).size());
    }

    @Test
    public void hostUpdate() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        MockUtil.mockAgentMemberCRDT(orMap, Collections.emptyMap());
        Set<HostEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);
        hostsSubjects.onNext(endpoints);

        verify(crdtStore).join(replica.getUri(), endpoint1.toByteString(),
            endpoints.stream().map(HostEndpoint::toByteString).collect(
                Collectors.toSet()));
    }

    @SneakyThrows
    @Test
    public void dropMemberWhileHostUpdate() {
        String agentMember1 = "agentMember1";
        String agentMember2 = "agentMember2";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        Map<AgentMemberAddr, AgentMemberMetadata> members = new HashMap<>();
        members.put(MockUtil.toAgentMemberAddr(agentMember1, endpoint1), MockUtil.toAgentMemberMetadata(ByteString.EMPTY));
        members.put(MockUtil.toAgentMemberAddr(agentMember2, endpoint2), MockUtil.toAgentMemberMetadata(ByteString.EMPTY));
        MockUtil.mockAgentMemberCRDT(orMap, members);
        Set<HostEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);
        hostsSubjects.onNext(endpoints);
        // mock ormap again
        MockUtil.mockAgentMemberCRDT(orMap, members);
        endpoints = Sets.newHashSet(endpoint1);
        hostsSubjects.onNext(endpoints);

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getValue();
        assertTrue(op instanceof ORMapOperation.ORMapRemove);
        assertEquals(CausalCRDTType.mvreg, ((ORMapOperation.ORMapRemove) op).valueType);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(agentMember2, key.getName());
        assertEquals(endpoint2, key.getEndpoint());

        verify(crdtStore).join(replica.getUri(), endpoint1.toByteString(),
            endpoints.stream().map(HostEndpoint::toByteString).collect(
                Collectors.toSet()));
    }

    @Test
    public void quit() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register("agentMember");
        when(crdtStore.stopHosting(replica.getUri())).thenReturn(CompletableFuture.completedFuture(null));
        TestObserver membersObserver = new TestObserver();
        agent.membership().subscribe(membersObserver);
        agent.quit().join();
        membersObserver.assertComplete();
        try {
            agentMember.broadcast(ByteString.copyFromUtf8("hello"), false);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
        // nothing should happen
        agent.quit().join();
    }
}
