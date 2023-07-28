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

package com.baidu.bifromq.baserpc.trafficgovernor;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

import com.baidu.bifromq.basecrdt.core.api.CRDTURI;
import com.baidu.bifromq.basecrdt.core.api.CausalCRDTType;
import com.baidu.bifromq.basecrdt.core.api.IMVReg;
import com.baidu.bifromq.basecrdt.core.api.IORMap;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.core.api.ORMapOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baseenv.EnvProvider;
import com.baidu.bifromq.baserpc.proto.LoadAssignment;
import com.baidu.bifromq.baserpc.proto.RPCServer;
import com.baidu.bifromq.baserpc.proto.TrafficDirective;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class RPCServiceAnnouncer {
    private static final Scheduler RPC_SHARED_SCHEDULER = Schedulers.from(
        newSingleThreadExecutor(EnvProvider.INSTANCE.newThreadFactory("RPC-Service-Cluster-CRDT", true)));
    private static final ByteString SERVER_LIST_KEY = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString TRAFFIC_DIRECTIVE_KEY = ByteString.copyFrom(new byte[] {0x01});

    protected final String serviceUniqueName;
    private final ICRDTService crdtService;
    private final Replica crdtReplica;
    private final IORMap rpcServiceCRDT;
    private final BehaviorSubject<Map<String, RPCServer>> svrSubject;
    private final BehaviorSubject<TrafficDirective> tdSubject;
    private final CompositeDisposable disposable = new CompositeDisposable();

    protected RPCServiceAnnouncer(String serviceUniqueName, ICRDTService crdtService) {
        assert crdtService.isStarted();
        this.serviceUniqueName = serviceUniqueName;
        this.crdtService = crdtService;
        this.crdtReplica = crdtService.host(CRDTURI.toURI(CausalCRDTType.ormap, "RPC:" + serviceUniqueName));
        this.rpcServiceCRDT = (IORMap) crdtService.get(crdtReplica.getUri()).get();
        Map<String, RPCServer> serverMap = buildAnnouncedServers(System.currentTimeMillis());
        svrSubject = serverMap.isEmpty() ? BehaviorSubject.create() : BehaviorSubject.createDefault(serverMap);
        tdSubject = BehaviorSubject.createDefault(buildAnnouncedTrafficDirective(System.currentTimeMillis())
            .orElse(TrafficDirective.getDefaultInstance()));
        disposable.add(rpcServiceCRDT.getORMap(SERVER_LIST_KEY)
            .inflation()
            .observeOn(RPC_SHARED_SCHEDULER)
            .map(this::buildAnnouncedServers)
            .subscribe(svrSubject::onNext));
        disposable.add(rpcServiceCRDT.getMVReg(TRAFFIC_DIRECTIVE_KEY)
            .inflation()
            .observeOn(RPC_SHARED_SCHEDULER)
            .map(this::buildAnnouncedTrafficDirective)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .subscribe(td -> {
                if (td.getAnnouncedTS() > tdSubject.getValue().getAnnouncedTS()) {
                    tdSubject.onNext(td);
                }
            }));
    }

    protected ByteString id() {
        return crdtReplica.getId();
    }

    protected void destroy() {
        disposable.dispose();
    }

    protected CompletableFuture<Void> announce(RPCServer server) {
        return rpcServiceCRDT.execute(ORMapOperation
            .update(SERVER_LIST_KEY, copyFromUtf8(server.getId()))
            .with(MVRegOperation.write(server.toByteString())));
    }

    protected CompletableFuture<Void> announce(Map<String, Map<String, Integer>> trafficDirective) {
        return rpcServiceCRDT.execute(ORMapOperation.update(TRAFFIC_DIRECTIVE_KEY)
            .with(MVRegOperation.write(TrafficDirective.newBuilder()
                .putAllAssignment(Maps.transformValues(trafficDirective,
                    v -> LoadAssignment.newBuilder().putAllWeightedGroup(v).build()))
                .setAnnouncedTS(System.currentTimeMillis())
                .build().toByteString())));
    }

    protected CompletableFuture<Void> revoke(String id) {
        return rpcServiceCRDT.execute(ORMapOperation
            .remove(SERVER_LIST_KEY, copyFromUtf8(id))
            .of(CausalCRDTType.mvreg));
    }

    protected Optional<RPCServer> announcedServer(String id) {
        return announcedServer(rpcServiceCRDT.getMVReg(SERVER_LIST_KEY, copyFromUtf8(id)));
    }

    private Optional<RPCServer> announcedServer(IMVReg mvReg) {
        RPCServer server = null;
        Iterator<ByteString> itr = mvReg.read();
        while (itr.hasNext()) {
            try {
                RPCServer s = RPCServer.parseFrom(itr.next());
                if (server == null) {
                    server = s;
                } else {
                    server = server.getAnnouncedTS() < s.getAnnouncedTS() ? s : server;
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse RPCServer from crdt", e);
            }
        }
        return Optional.ofNullable(server);
    }

    protected Observable<Map<String, RPCServer>> announcedServers() {
        return svrSubject.observeOn(RPC_SHARED_SCHEDULER);
    }

    private Map<String, RPCServer> buildAnnouncedServers(long t) {
        IORMap serverListORMap = rpcServiceCRDT.getORMap(SERVER_LIST_KEY);
        Iterator<IORMap.ORMapKey> keyItr = serverListORMap.keys();
        Map<String, RPCServer> announced = Maps.newHashMap();
        while (keyItr.hasNext()) {
            IORMap.ORMapKey orMapKey = keyItr.next();
            assert orMapKey.valueType() == CausalCRDTType.mvreg;
            Optional<RPCServer> rpcServer = announcedServer(serverListORMap.getMVReg(orMapKey.key()));
            if (rpcServer.isPresent()) {
                announced.put(rpcServer.get().getId(), rpcServer.get());
            }
        }
        log.debug("Build service[{}]'s server list at {}:{}", serviceUniqueName, t, announced);
        return announced;
    }

    protected Observable<Map<String, Map<String, Integer>>> trafficDirective() {
        return tdSubject.map(td -> Maps.transformValues(td.getAssignmentMap(), v -> v.getWeightedGroupMap()))
            .observeOn(RPC_SHARED_SCHEDULER);
    }

    protected Observable<Set<ByteString>> aliveAnnouncers() {
        return crdtService.aliveReplicas(crdtReplica.getUri())
            .map(r -> r.stream().map(Replica::getId).collect(Collectors.toSet()));
    }

    private Optional<TrafficDirective> buildAnnouncedTrafficDirective(long t) {
        TrafficDirective td = null;
        Iterator<ByteString> itr = rpcServiceCRDT.getMVReg(TRAFFIC_DIRECTIVE_KEY).read();
        while (itr.hasNext()) {
            try {
                TrafficDirective next = TrafficDirective.parseFrom(itr.next());
                if (td == null) {
                    td = next;
                } else {
                    td = td.getAnnouncedTS() < next.getAnnouncedTS() ? next : td;
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse RPCServer from crdt", e);
            }
        }
        return Optional.ofNullable(td);
    }

}
