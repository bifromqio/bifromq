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

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.ToString;

public interface IRPCServiceTrafficDirector {
    static IRPCServiceTrafficDirector newInstance(String serviceUniqueName, ICRDTService crdtService) {
        return new RPCServiceTrafficDirector(serviceUniqueName, crdtService);
    }

    @AllArgsConstructor
    @ToString
    class Server {
        public final String id;
        public final InetSocketAddress hostAddr;
        public final Set<String> groupTags;
        public final Map<String, String> attrs;
    }

    /**
     * Current traffic directive of the service
     *
     * @return
     */
    Observable<Map<String, Map<String, Integer>>> trafficDirective();

    /**
     * Watch the ever-updating server list of the service
     *
     * @return
     */
    Observable<Set<Server>> serverList();

    void destroy();
}
