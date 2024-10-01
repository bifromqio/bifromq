/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.apiserver.http.handler;

import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import com.baidu.bifromq.dist.client.IDistClient;
import io.reactivex.rxjava3.core.Single;
import java.util.Collections;
import java.util.Set;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/server/dist")
public class HTTPDistServerLandscapeHandler extends HTTPServerLandscapeHandler {
    private final Single<Set<IRPCServiceTrafficDirector.Server>> landscapeSingle;

    public HTTPDistServerLandscapeHandler(IDistClient distClient) {
        landscapeSingle = distClient.trafficGovernor().serverList().first(Collections.emptySet());
    }

    @Override
    protected Single<Set<IRPCServiceTrafficDirector.Server>> landscapeSingle() {
        return landscapeSingle;
    }
}
