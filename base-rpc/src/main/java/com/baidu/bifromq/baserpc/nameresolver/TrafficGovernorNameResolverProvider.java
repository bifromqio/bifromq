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

package com.baidu.bifromq.baserpc.nameresolver;

import com.baidu.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficDirector;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class TrafficGovernorNameResolverProvider extends NameResolverProvider {
    public static final String SCHEME = "tgov";

    private final String serviceUniqueName;

    private final IRPCServiceTrafficDirector trafficDirector;

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new TrafficGovernorNameResolver(serviceUniqueName, trafficDirector);
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 6;
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }
}
