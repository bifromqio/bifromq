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

package com.baidu.bifromq.basekv.localengine;

import static com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMeters.getTimer;

import com.baidu.bifromq.basekv.localengine.metrics.KVSpaceMetric;
import com.baidu.bifromq.basekv.localengine.proto.KeyBoundary;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;

public abstract class AbstractKVSpaceReader implements IKVSpaceReader {

    protected final String id;
    private final MetricManager metricMgr;

    protected AbstractKVSpaceReader(String id, Tags tags) {
        this.id = id;
        this.metricMgr = new MetricManager(tags);
    }

    @Override
    public final String id() {
        return id;
    }

    @Override
    public final Optional<ByteString> metadata(ByteString metaKey) {
        return metricMgr.metadataCallTimer.record(() -> doMetadata(metaKey));
    }

    protected abstract Optional<ByteString> doMetadata(ByteString metaKey);

    @Override
    public final long size() {
        return size(KeyBoundary.getDefaultInstance());
    }

    @Override
    public final long size(KeyBoundary boundary) {
        return metricMgr.sizeCallTimer.record(() -> doSize(boundary));
    }

    protected abstract long doSize(KeyBoundary boundary);

    @Override
    public final boolean exist(ByteString key) {
        return metricMgr.existCallTimer.record(() -> doExist(key));
    }

    protected abstract boolean doExist(ByteString key);

    @Override
    public final Optional<ByteString> get(ByteString key) {
        return metricMgr.getCallTimer.record(() -> doGet(key));
    }

    protected abstract Optional<ByteString> doGet(ByteString key);

    @Override
    public final IKVSpaceIterator newIterator() {
        return metricMgr.iterNewCallTimer.record(() -> new MonitoredKeyRangeIterator(doNewIterator()));
    }

    protected abstract IKVSpaceIterator doNewIterator();

    @Override
    public final IKVSpaceIterator newIterator(KeyBoundary subBoundary) {
        return metricMgr.iterNewCallTimer.record(() -> new MonitoredKeyRangeIterator(doNewIterator(subBoundary)));
    }

    protected abstract IKVSpaceIterator doNewIterator(KeyBoundary subBoundary);

    private class MonitoredKeyRangeIterator implements IKVSpaceIterator {
        final IKVSpaceIterator delegate;

        private MonitoredKeyRangeIterator(IKVSpaceIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteString key() {
            return delegate.key();
        }

        @Override
        public ByteString value() {
            return delegate.value();
        }

        @Override
        public boolean isValid() {
            return delegate.isValid();
        }

        @Override
        public void next() {
            metricMgr.iterNextCallTimer.record(delegate::next);
        }

        @Override
        public void prev() {
            metricMgr.iterPrevCallTimer.record(delegate::prev);
        }

        @Override
        public void seekToFirst() {
            metricMgr.iterSeekToFirstCallTimer.record(delegate::seekToFirst);
        }

        @Override
        public void seekToLast() {
            metricMgr.iterSeekToLastCallTimer.record(delegate::seekToLast);
        }

        @Override
        public void seek(ByteString target) {
            metricMgr.iterSeekCallTimer.record(() -> delegate.seek(target));
        }

        @Override
        public void seekForPrev(ByteString target) {
            metricMgr.iterSeekForPrevCallTimer.record(() -> delegate.seekForPrev(target));
        }

        @Override
        public void refresh() {
            metricMgr.iterRefreshTimer.record(delegate::refresh);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private class MetricManager {
        private final Timer metadataCallTimer;
        private final Timer sizeCallTimer;
        private final Timer existCallTimer;
        private final Timer getCallTimer;
        private final Timer iterNewCallTimer;
        private final Timer iterSeekCallTimer;
        private final Timer iterSeekForPrevCallTimer;
        private final Timer iterSeekToFirstCallTimer;
        private final Timer iterSeekToLastCallTimer;
        private final Timer iterNextCallTimer;
        private final Timer iterPrevCallTimer;
        private final Timer iterRefreshTimer;

        MetricManager(Tags tags) {
            metadataCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "metadata"));

            sizeCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "size"));
            existCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "exist"));
            getCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "get"));
            iterNewCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "newitr"));

            iterSeekCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "seek"));
            iterSeekForPrevCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "pseek"));
            iterSeekToFirstCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "fseek"));
            iterSeekToLastCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "lseek"));
            iterNextCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "next"));
            iterPrevCallTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "prev"));
            iterRefreshTimer = getTimer(id, KVSpaceMetric.CallTimer, tags.and("op", "refresh"));
        }
    }
}
