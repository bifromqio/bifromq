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

import static com.baidu.bifromq.baserpc.RPCContext.GPID;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.proto.RPCServer;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class RPCServiceServerRegister extends RPCServiceAnnouncer implements IRPCServiceServerRegister {
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private final CompositeDisposable disposable = new CompositeDisposable();
    private RPCServer localServer;

    public RPCServiceServerRegister(String serviceUniqueName, ICRDTService crdtService) {
        super(serviceUniqueName, crdtService);
    }

    @Override
    public void start(String id, InetSocketAddress hostAddr, Set<String> groupTags, Map<String, String> attrs) {
        assert !stopped.get();
        if (started.compareAndSet(false, true)) {
            localServer = RPCServer.newBuilder()
                .setId(id)
                .setHost(hostAddr.getAddress().getHostAddress())
                .setPort(hostAddr.getPort())
                .setGpid(GPID)
                .addAllGroup(groupTags)
                .putAllAttrs(attrs)
                .setAnnouncerId(id())
                .setAnnouncedTS(System.currentTimeMillis())
                .build();

            // make an announcement via rpcServiceCRDT
            log.debug("Announce local server[{}]:{}", serviceUniqueName, localServer);
            announce(localServer).join();

            // enforce the announcement consistent eventually
            disposable.add(announcedServers()
                .subscribe(serverMap -> {
                    if (!serverMap.containsKey(localServer.getId())) {
                        localServer = localServer.toBuilder().setAnnouncedTS(System.currentTimeMillis()).build();
                        log.debug("Re-announce local server: {}", localServer);
                        // refresh announcement time
                        announce(localServer);
                    } else if (localServer.getAnnouncedTS() < serverMap.get(localServer.getId()).getAnnouncedTS()) {
                        localServer = serverMap.get(localServer.getId());
                        log.debug("Update local server from announcement: server={}", localServer);
                    }
                }));
        }
    }

    @Override
    public void stop() {
        assert started.get();
        if (stopped.compareAndSet(false, true)) {
            // stop the announcement
            revoke(localServer.getId()).join();
            disposable.dispose();
            super.destroy();
        }
    }
}
