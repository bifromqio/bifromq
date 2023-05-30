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

package com.baidu.bifromq.basekv;

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeStoreDescriptor;
import com.baidu.bifromq.basekv.proto.Range;
import com.baidu.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.baidu.bifromq.basekv.utils.KeyRangeUtil;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

public final class KVRangeRouter implements IKVRangeRouter {
    private final StampedLock stampedLock = new StampedLock();
    private final Comparator<ByteString> comparator = unsignedLexicographicalComparator();
    private final NavigableMap<ByteString, KVRangeSetting> rangeTable = new TreeMap<>(comparator);
    private final Map<KVRangeId, KVRangeSetting> rangeMap = new HashMap<>();

    public void reset(KVRangeStoreDescriptor storeDescriptor) {
        final long stamp = stampedLock.writeLock();
        try {
            rangeTable.clear();
            rangeMap.clear();
            storeDescriptor.getRangesList()
                .forEach(rangeDesc -> this.upsertWithoutLock(storeDescriptor.getId(), rangeDesc));
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public void upsert(KVRangeStoreDescriptor storeDescriptor) {
        final long stamp = stampedLock.writeLock();
        try {
            storeDescriptor.getRangesList()
                .forEach(rangeDesc -> this.upsertWithoutLock(storeDescriptor.getId(), rangeDesc));
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public boolean isFullRangeCovered() {
        final long stamp = stampedLock.readLock();
        try {
            if (rangeTable.isEmpty()) {
                return false;
            }
            ByteString firstKey = rangeTable.firstKey();
            if (!firstKey.equals(Constants.MIN_KEY) || rangeTable.firstEntry().getValue().range.hasStartKey()) {
                // the lower bound of the first range is explicitly set to empty byte string.
                return false;
            }
            ByteString endKey = Constants.MIN_KEY;
            for (Map.Entry<ByteString, KVRangeSetting> entry : rangeTable.entrySet()) {
                Range range = entry.getValue().range;
                if (!endKey.equals(range.getStartKey())) {
                    return false;
                } else {
                    endKey = range.getEndKey();
                }
            }
            // the upper bound of the last range is open
            return !rangeTable.lastEntry().getValue().range.hasEndKey();
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public Optional<KVRangeSetting> findByKey(ByteString key) {
        final long stamp = stampedLock.readLock();
        try {
            Map.Entry<ByteString, KVRangeSetting> entry = rangeTable.floorEntry(key);
            if (entry != null) {
                KVRangeSetting setting = entry.getValue();
                if (KeyRangeUtil.inRange(key, setting.range)) {
                    return Optional.of(setting);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public List<KVRangeSetting> findByRange(Range range) {
        final long stamp = stampedLock.readLock();
        try {
            return findByRangeWithoutLock(range);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public Optional<KVRangeSetting> findById(KVRangeId id) {
        return Optional.ofNullable(rangeMap.get(id));
    }

    private List<KVRangeSetting> findByRangeWithoutLock(Range range) {
        List<KVRangeSetting> ranges = new ArrayList<>();
        // range before range.start
        Map.Entry<ByteString, KVRangeSetting> before = rangeTable.lowerEntry(range.getStartKey());
        if (before != null && KeyRangeUtil.inRange(range.getStartKey(), before.getValue().range)) {
            ranges.add(before.getValue());
        }
        // ranges after range.start
        NavigableMap<ByteString, KVRangeSetting> after = rangeTable.tailMap(range.getStartKey(), true);
        for (Map.Entry<ByteString, KVRangeSetting> entry : after.entrySet()) {
            if (KeyRangeUtil.isOverlap(entry.getValue().range, range)) {
                ranges.add(entry.getValue());
            } else {
                break;
            }
        }
        return ranges;
    }

    private boolean upsertWithoutLock(String storeId, KVRangeDescriptor descriptor) {
        if (descriptor.getRole() != RaftNodeStatus.Leader) {
            return false;
        }
        if (descriptor.getRange().equals(Constants.EMPTY_RANGE)) {
            return false;
        }
        switch (descriptor.getState()) {
            case Removed:
            case Purged:
            case Merged:
            case MergedQuiting:
                return false;
        }
        KVRangeSetting setting = new KVRangeSetting(storeId, descriptor);

        List<KVRangeSetting> overlapped = findByRangeWithoutLock(setting.range);
        if (overlapped.isEmpty()) {
            rangeTable.put(setting.range.getStartKey(), setting);
            rangeMap.put(setting.id, setting);
            return true;
        } else {
            if (overlapped.stream().allMatch(o -> o.ver <= setting.ver)) {
                overlapped.forEach(o -> {
                    rangeTable.remove(o.range.getStartKey());
                    rangeMap.remove(o.id);
                });
                rangeTable.put(setting.range.getStartKey(), setting);
                rangeMap.put(setting.id, setting);
                return true;
            }
            return false;
        }
    }
}
