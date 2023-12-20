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

package com.baidu.bifromq.baserpc.trafficgovernor;

import static com.baidu.bifromq.baserpc.RPCContext.GPID;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.baserpc.proto.RPCServer;
import com.google.common.collect.Sets;
import io.grpc.netty.InProcSocketAddress;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import lombok.ToString;

public interface IRPCServiceTrafficDirector {
    static IRPCServiceTrafficDirector newInstance(String serviceUniqueName, ICRDTService crdtService) {
        return new RPCServiceTrafficDirector(serviceUniqueName, crdtService);
    }

    @ToString
    class Server {
        public final String id;
        public final SocketAddress hostAddr;
        public final Set<String> groupTags;
        public final Map<String, String> attrs;
        public final boolean inProc;

        public Server(RPCServer server) {
            this.id = server.getId();
            this.attrs = server.getAttrsMap();
            this.groupTags = Sets.newHashSet(server.getGroupList());
            this.hostAddr = GPID.equals(server.getGpid()) ?
                new InProcSocketAddress(server.getId()) : new InetSocketAddress(server.getHost(), server.getPort());
            this.inProc = GPID.equals(server.getGpid());
        }
    }

    /**
     * Current traffic directive of the service
     *
     * @return an observable of traffic directive
     */
    Observable<Map<String, Map<String, Integer>>> trafficDirective();

    /**
     * Watch the ever-updating server list of the service
     *
     * @return an observable of servers
     */
    Observable<Set<Server>> serverList();

    void destroy();
}
