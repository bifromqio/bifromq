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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.metrics.TenantMeter.gauging;
import static com.baidu.bifromq.metrics.TenantMeter.stopGauging;
import static com.baidu.bifromq.metrics.TenantMetric.InboxFetcherGauge;
import static java.util.Collections.emptyMap;

import com.google.common.collect.Iterators;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class InboxFetcherRegistry implements Iterable<IInboxFetcher> {
    private final ConcurrentMap<String, Map<String, Map<String, IInboxFetcher>>> fetchers = new ConcurrentHashMap<>();

    void reg(IInboxFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (key, val) -> {
            if (val == null) {
                val = new HashMap<>();
                gauging(fetcher.tenantId(), InboxFetcherGauge,
                    () -> fetchers.getOrDefault(fetcher.tenantId(), emptyMap()).size());
            }
            IInboxFetcher prevFetcher = val.computeIfAbsent(fetcher.delivererKey(), k -> new ConcurrentHashMap<>())
                .put(fetcher.id(), fetcher);
            if (prevFetcher != null) {
                prevFetcher.close();
            }
            return val;
        });
    }

    void unreg(IInboxFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (tenantId, m) -> {
            if (m != null) {
                m.computeIfPresent(fetcher.delivererKey(), (k, v) -> {
                    v.remove(fetcher.id(), fetcher);
                    if (v.isEmpty()) {
                        return null;
                    }
                    return v;
                });
                if (m.isEmpty()) {
                    stopGauging(fetcher.tenantId(), InboxFetcherGauge);
                    return null;
                }
            }
            return m;
        });
    }

    Collection<IInboxFetcher> get(String tenantId, String delivererKey) {
        return fetchers.getOrDefault(tenantId, emptyMap()).getOrDefault(delivererKey, emptyMap()).values();
    }

    @Override
    public Iterator<IInboxFetcher> iterator() {
        return Iterators.concat(
            Iterators.transform(
                Iterators.concat(
                    fetchers.values().stream().map(m -> m.values().iterator()).iterator()
                ),
                e -> e.values().iterator()
            )
        );
    }
}
