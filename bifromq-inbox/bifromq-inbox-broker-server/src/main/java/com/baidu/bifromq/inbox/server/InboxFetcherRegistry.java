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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.metrics.TenantMeter.gauging;
import static com.baidu.bifromq.metrics.TenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.InboxFetcherGauge;
import static java.util.Collections.emptyMap;

import com.google.common.collect.Iterators;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class InboxFetcherRegistry implements Iterable<IInboxQueueFetcher> {
    private final ConcurrentMap<String, Map<String, IInboxQueueFetcher>> fetchers = new ConcurrentHashMap<>();

    void reg(IInboxQueueFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (key, val) -> {
            if (val == null) {
                val = new HashMap<>();
                gauging(fetcher.tenantId(), InboxFetcherGauge,
                    () -> fetchers.getOrDefault(fetcher.tenantId(), emptyMap()).size());
            }
            IInboxQueueFetcher prevFetcher = val.put(fetcher.inboxId(), fetcher);
            if (prevFetcher != null) {
                // close previous fetcher if any
                prevFetcher.close();
            }
            return val;
        });
    }

    void unreg(IInboxQueueFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (tenantId, m) -> {
            if (m != null) {
                m.remove(fetcher.inboxId(), fetcher);
                if (m.size() == 0) {
                    stopGauging(fetcher.tenantId(), InboxFetcherGauge);
                    return null;
                }
            }
            return m;
        });
    }

    IInboxQueueFetcher get(String tenantId, String inboxId) {
        return fetchers.getOrDefault(tenantId, emptyMap()).get(inboxId);
    }

    @Override
    public Iterator<IInboxQueueFetcher> iterator() {
        return Iterators.concat(fetchers.values().stream().map(m -> m.values().iterator()).iterator());
    }
}
