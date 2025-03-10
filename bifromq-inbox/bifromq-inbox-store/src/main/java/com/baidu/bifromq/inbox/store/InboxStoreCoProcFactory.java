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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxBucketPrefix;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVCloseableReader;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.baidu.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class InboxStoreCoProcFactory implements IKVRangeCoProcFactory {
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Duration loadEstWindow;


    public InboxStoreCoProcFactory(ISettingProvider settingProvider,
                                   IEventCollector eventCollector,
                                   Duration loadEstimateWindow) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.loadEstWindow = loadEstimateWindow;
    }

    @Override
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVCloseableReader> rangeReaderProvider) {
        // load-based hinter only split range around up to the inbox bucket boundary
        return Collections.singletonList(new MutationKVLoadBasedSplitHinter(loadEstWindow, key ->
            Optional.ofNullable(upperBound(parseInboxBucketPrefix(key))),
            "clusterId", clusterId, "storeId", storeId, "rangeId",
            KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId,
                                       String storeId,
                                       KVRangeId id,
                                       Supplier<IKVCloseableReader> rangeReaderProvider) {
        return new InboxStoreCoProc(clusterId, storeId, id, settingProvider, eventCollector, rangeReaderProvider);
    }

    public void close() {
    }
}
