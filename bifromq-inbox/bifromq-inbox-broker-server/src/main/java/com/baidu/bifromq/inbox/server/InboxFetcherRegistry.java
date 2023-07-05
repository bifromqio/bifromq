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

import static com.baidu.bifromq.metrics.TrafficMeter.gauging;
import static com.baidu.bifromq.metrics.TrafficMeter.stopGauging;
import static com.baidu.bifromq.metrics.TrafficMetric.InboxFetcherGauge;

import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

final class InboxFetcherRegistry implements Iterable<IInboxQueueFetcher> {
    // how do we handle multiple fetchers under same (trafficId, inboxId, qos) combination which may happen when
    // "persistent session" clients kicking each other
    // delivererKey+trafficId -> inboxId -> InboxFetcher
    private final NavigableMap<String, Map<String, IInboxQueueFetcher>> fetchers = new ConcurrentSkipListMap<>();

    void reg(IInboxQueueFetcher fetcher) {
        fetchers.compute(fetcher.delivererKey() + fetcher.trafficId(), (key, val) -> {
            if (val == null) {
                val = new HashMap<>();
                gauging(fetcher.trafficId(), InboxFetcherGauge,
                    () -> fetchers.getOrDefault(fetcher.trafficId(), Collections.EMPTY_MAP).size());
            }
            val.put(fetcher.inboxId(), fetcher);
            return val;
        });
    }

    void unreg(IInboxQueueFetcher fetcher) {
        fetchers.compute(fetcher.delivererKey() + fetcher.trafficId(), (traffic, m) -> {
            if (m != null) {
                m.remove(fetcher.inboxId(), fetcher);
                if (m.size() == 0) {
                    stopGauging(fetcher.trafficId(), InboxFetcherGauge);
                    return null;
                }
            }
            return m;
        });
    }

    boolean has(String trafficId, String inboxId, String delivererKey) {
        return fetchers.getOrDefault(delivererKey + trafficId, Collections.emptyMap()).containsKey(inboxId);
    }

    IInboxQueueFetcher get(String trafficId, String inboxId, String delivererKey) {
        return fetchers.getOrDefault(delivererKey + trafficId, Collections.emptyMap()).get(inboxId);
    }

    void signalFetch(String delivererKey) {
        SortedMap<String, Map<String, IInboxQueueFetcher>> subMap = fetchers.tailMap(delivererKey);
        for (String key : subMap.keySet()) {
            if (key.startsWith(delivererKey)) {
                return;
            }
            for (IInboxQueueFetcher fetcher : subMap.get(key).values()) {
                fetcher.signalFetch();
            }
        }
    }

    @Override
    public Iterator<IInboxQueueFetcher> iterator() {
        return Iterators.concat(fetchers.values().stream().map(m -> m.values().iterator()).iterator());
    }
}
