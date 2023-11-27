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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.hasScopedInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseScopedInboxId;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class InboxStoreCoProcFactory implements IKVRangeCoProcFactory {
    private final IDistClient distClient;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Clock clock;
    private final Duration loadEstWindow;
    private final Duration purgeDelay;

    public InboxStoreCoProcFactory(IDistClient distClient,
                                   ISettingProvider settingProvider,
                                   IEventCollector eventCollector,
                                   Clock clock,
                                   Duration loadEstimateWindow,
                                   Duration purgeDelay) {
        this.distClient = distClient;
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.clock = clock;
        this.loadEstWindow = loadEstimateWindow;
        this.purgeDelay = purgeDelay;
    }

    @Override
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVReader> rangeReaderProvider) {
        return Collections.singletonList(new MutationKVLoadBasedSplitHinter(loadEstWindow, key -> {
            if (hasScopedInboxId(key)) {
                return Optional.of(upperBound(parseScopedInboxId(key)));
            }
            return Optional.empty();
        }, "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId, String storeId, KVRangeId id,
                                       Supplier<IKVReader> rangeReaderProvider) {
        return new InboxStoreCoProc(distClient, settingProvider, eventCollector, clock, purgeDelay);
    }

    public void close() {
    }
}
