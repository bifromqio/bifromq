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

package com.baidu.bifromq.basecluster.memberlist;

import static com.baidu.bifromq.basecluster.memberlist.Fixtures.LOCAL_ADDR;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.LOCAL_ENDPOINT;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.LOCAL_REPLICA;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.LOCAL_REPLICA_ID;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.REMOTE_ADDR_1;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.REMOTE_HOST_1_ENDPOINT;
import static com.baidu.bifromq.basecluster.memberlist.Fixtures.ZOMBIE_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basecluster.memberlist.agent.Agent;
import com.baidu.bifromq.basecluster.memberlist.agent.IAgent;
import com.baidu.bifromq.basecluster.membership.proto.Doubt;
import com.baidu.bifromq.basecluster.membership.proto.Fail;
import com.baidu.bifromq.basecluster.membership.proto.HostEndpoint;
import com.baidu.bifromq.basecluster.membership.proto.HostMember;
import com.baidu.bifromq.basecluster.membership.proto.Join;
import com.baidu.bifromq.basecluster.membership.proto.Quit;
import com.baidu.bifromq.basecluster.messenger.IMessenger;
import com.baidu.bifromq.basecluster.messenger.MessageEnvelope;
import com.baidu.bifromq.basecluster.proto.ClusterMessage;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class HostMemberListTest {
    @Mock
    private IMessenger messenger;
    @Mock
    private ICRDTStore store;
    @Mock
    private IORMap hostListCRDT;
    @Mock
    private IHostAddressResolver addressResolver;
    private Scheduler scheduler = Schedulers.from(MoreExecutors.directExecutor());
    private PublishSubject<Long> inflationSubject = PublishSubject.create();
    private PublishSubject<Timed<MessageEnvelope>> messageSubject = PublishSubject.create();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(store.host(CRDTUtil.AGENT_HOST_MAP_URI)).thenReturn(LOCAL_REPLICA);
        when(store.get(CRDTUtil.AGENT_HOST_MAP_URI))
            .thenAnswer((Answer<Optional<IORMap>>) invocation -> Optional.of(hostListCRDT));
        when(hostListCRDT.id()).thenReturn(LOCAL_REPLICA);
        when(hostListCRDT.inflation()).thenReturn(inflationSubject);
        when(messenger.receive()).thenReturn(messageSubject);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void init() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        HostMember local = memberList.local();
        assertEquals(local.getEndpoint().getId(), LOCAL_REPLICA_ID);
        assertEquals(local.getEndpoint().getAddress(), LOCAL_ADDR.getHostName());
        assertEquals(local.getEndpoint().getPort(), LOCAL_ADDR.getPort());
        assertTrue(local.getIncarnation() >= 0);
        assertTrue(local.getAgentIdList().isEmpty());
        assertTrue(memberList.agents().isEmpty());
        Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
        assertEquals(hostMap.size(), 1);
        assertTrue(local.getIncarnation() == hostMap.get(local.getEndpoint()));
    }

    @Test
    public void host() {
        try (MockedConstruction<Agent> mockAgent = mockConstruction(Agent.class)) {
            String agentId = "agentId";
            IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
                messenger, scheduler, store, addressResolver);
            HostMember local = memberList.local();
            IAgent agent = memberList.host(agentId);
            assertEquals(memberList.agents().size(), 1);
            assertTrue(memberList.agents().contains(agentId));
            assertEquals(mockAgent.constructed().size(), 1);
            assertTrue(local.getIncarnation() + 1 == memberList.local().getIncarnation());
            Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
            assertEquals(hostMap.size(), 1);
            assertTrue(local.getIncarnation() + 1 == hostMap.get(local.getEndpoint()));

            verify(hostListCRDT, times(2)).execute(any(ORMapOperation.ORMapUpdate.class));
        }
    }

    @Test
    public void stopHosting() {
        try (MockedConstruction<Agent> mockAgent = mockConstruction(Agent.class)) {
            String agentId = "agentId";
            IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
                messenger, scheduler, store, addressResolver);
            HostMember local = memberList.local();
            memberList.host(agentId);
            when(mockAgent.constructed().get(0).quit()).thenReturn(CompletableFuture.completedFuture(null));
            memberList.stopHosting(agentId);
            assertEquals(memberList.local().getAgentIdCount(), 0);
            assertEquals(memberList.agents().size(), 0);
            assertTrue(local.getIncarnation() + 2 == memberList.local().getIncarnation());
            Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
            assertTrue(local.getIncarnation() + 2 == hostMap.get(local.getEndpoint()));

            verify(hostListCRDT, times(3)).execute(any(ORMapOperation.ORMapUpdate.class));
        }
    }

    @Test
    public void isZombie() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        assertFalse(memberList.isZombie(memberList.local().getEndpoint()));
        assertTrue(memberList.isZombie(memberList.local()
            .getEndpoint()
            .toBuilder()
            .setId(ByteString.copyFromUtf8("zombie"))
            .build()));
    }

    @Test
    public void handleJoin() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        verify(store, times(2)).join(anyString(), any(), any());
    }

    @Test
    public void handleJoinAndClearZombie() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), ZOMBIE_ENDPOINT));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(1)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(1).keyPath[0], ZOMBIE_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());

        assertEquals(msgCap.getValue().getQuit().getEndpoint(), ZOMBIE_ENDPOINT);
        assertEquals(msgCap.getValue().getQuit().getIncarnation(), Integer.MAX_VALUE);
    }

    @Test
    public void handleJoinFromHealing() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), LOCAL_ENDPOINT));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
        assertEquals(addrCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleFailAndClearZombie() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(failMsg(ZOMBIE_ENDPOINT, 1));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(1)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(1).keyPath[0], ZOMBIE_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());

        assertEquals(msgCap.getValue().getQuit().getEndpoint(), ZOMBIE_ENDPOINT);
        assertEquals(msgCap.getValue().getQuit().getIncarnation(), Integer.MAX_VALUE);
    }

    @Test
    public void handleFailAndDrop() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 1));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(3)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(2)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(2).keyPath[0], REMOTE_HOST_1_ENDPOINT.toByteString());
    }

    @Test
    public void handleFailAndRenew() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 0);


        messageSubject.onNext(failMsg(LOCAL_ENDPOINT, 0));
        messageSubject.onNext(failMsg(LOCAL_ENDPOINT, 0)); // this time will be ignored

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(opCap.getAllValues().get(1).keyPath[0], LOCAL_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 1);

        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 1);
    }

    @Test
    public void handleQuitZombie() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(quitMsg(ZOMBIE_ENDPOINT, 1));
        // nothing will happen
        verify(hostListCRDT, times(0)).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(anyString(), any(), any());
    }

    @Test
    public void handleQuitNotExistMember() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(quitMsg(REMOTE_HOST_1_ENDPOINT, 1));
        // nothing will happen
        verify(hostListCRDT, times(1)).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(anyString(), any(), any());
    }

    @Test
    public void handleQuitSelf() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(quitMsg(LOCAL_ENDPOINT, 0));
        // nothing will happen
        verify(hostListCRDT, times(0)).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(anyString(), any(), any());
    }

    @Test
    public void handleQuitAndDrop() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(0)
            .build()));
        messageSubject.onNext(quitMsg(REMOTE_HOST_1_ENDPOINT, 0));
        // nothing will happen
        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(3)).execute(opCap.capture());
        assertTrue(opCap.getAllValues().get(2) instanceof ORMapOperation.ORMapRemove);

        verify(store, times(3)).join(anyString(), any(), any());
    }

    @Test
    public void handleDoubt() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(doubtMsg(LOCAL_ENDPOINT, 0));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(opCap.getAllValues().get(1).keyPath[0], LOCAL_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 1);
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 1);
    }

    @Test
    public void handleDoubtAndIgnore() {
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(doubtMsg(REMOTE_HOST_1_ENDPOINT, 0));
        verify(messenger, times(0)).spread(any());
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 0);
    }


    private Timed<MessageEnvelope> joinMsg(HostMember member) {
        return to(ClusterMessage.newBuilder()
            .setJoin(Join.newBuilder()
                .setMember(member)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> joinMsg(HostMember member, HostEndpoint expected) {
        return to(ClusterMessage.newBuilder()
            .setJoin(Join.newBuilder()
                .setMember(member)
                .setExpectedHost(expected)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> quitMsg(HostEndpoint quitEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setQuit(Quit.newBuilder()
                .setEndpoint(quitEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> doubtMsg(HostEndpoint doubtEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setDoubt(Doubt.newBuilder()
                .setEndpoint(doubtEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> failMsg(HostEndpoint failedEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setFail(Fail.newBuilder()
                .setEndpoint(failedEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> to(ClusterMessage clusterMessage) {
        return new Timed<>(MessageEnvelope.builder()
            .message(clusterMessage)
            .build(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
}
