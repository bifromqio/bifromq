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

package com.baidu.bifromq.basekv.utils;

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.proto.Range;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import java.util.Optional;

public class KeyRangeUtil {

    public static boolean isOverlap(Range range1, Range range2) {
        if (range1.equals(EMPTY_RANGE) || range2.equals(EMPTY_RANGE)) {
            return false;
        }
        if (!range1.hasEndKey() && !range2.hasEndKey()) {
            return true;
        } else if (!range1.hasEndKey()) {
            return compare(range1.getStartKey(), range2.getEndKey()) < 0;
        } else if (!range2.hasEndKey()) {
            return compare(range2.getStartKey(), range1.getEndKey()) < 0;
        }
        return !(compare(range1.getEndKey(), range2.getStartKey()) <= 0
            || compare(range2.getEndKey(), range1.getStartKey()) <= 0);
    }

    public static Range intersect(Range range1, Range range2) {
        if (isOverlap(range1, range2)) {
            Optional<ByteString> lowerBound = greaterLowerBound(range1, range2);
            Optional<ByteString> upperBound = leastUpperBound(range1, range2);
            Range.Builder rangeBuilder = Range.newBuilder();
            if (lowerBound.isPresent()) {
                rangeBuilder.setStartKey(lowerBound.get());
            }
            if (upperBound.isPresent()) {
                rangeBuilder.setEndKey(upperBound.get());
            }
            return rangeBuilder.build();
        }
        return EMPTY_RANGE;
    }

    public static boolean contains(Range containeeRange, Range containerRange) {
        if (containeeRange.equals(EMPTY_RANGE)) {
            return true;
        }
        if (!containerRange.hasEndKey()) {
            return compare(containerRange.getStartKey(), containeeRange.getStartKey()) <= 0;
        }
        if (!containeeRange.hasEndKey()) {
            return false;
        }
        return compare(containerRange.getStartKey(), containeeRange.getStartKey()) <= 0
            && compare(containeeRange.getEndKey(), containerRange.getEndKey()) <= 0;
    }

    public static Optional<ByteString> greaterLowerBound(Range range1, Range range2) {
        if (!range1.hasStartKey() && !range2.hasStartKey()) {
            return Optional.empty();
        }
        if (!range1.hasStartKey()) {
            return Optional.of(range2.getStartKey());
        }
        if (!range2.hasStartKey()) {
            return Optional.of(range1.getStartKey());
        }
        return Optional.of(compare(range1.getStartKey(),
            range2.getStartKey()) < 0 ? range2.getStartKey() : range1.getStartKey());
    }

    public static Optional<ByteString> leastUpperBound(Range range1, Range range2) {
        if (!range1.hasEndKey() && !range2.hasEndKey()) {
            return Optional.empty();
        }
        if (!range1.hasEndKey()) {
            return Optional.of(range2.getEndKey());
        }
        if (!range2.hasEndKey()) {
            return Optional.of(range1.getEndKey());
        }
        return Optional.of(compare(range1.getEndKey(), range2.getEndKey()) < 0 ?
            range1.getEndKey() : range2.getEndKey());
    }

    public static boolean inRange(ByteString key, Range range) {
        if (!range.hasEndKey()) {
            // right open range
            return compare(range.getStartKey(), key) <= 0;
        } else {
            return compare(range.getStartKey(), key) <= 0 && compare(key, range.getEndKey()) < 0;
        }
    }

    public static Range combine(Range... ranges) {
        assert ranges.length >= 2;
        Range range = ranges[0];
        for (int i = 1; i < ranges.length; i++) {
            range = combine2Range(range, ranges[i]);
        }
        return range;
    }

    private static Range combine2Range(Range range1, Range range2) {
        assert canCombine(range1, range2);
        if (isEmptyRange(range1)) {
            return range2;
        }
        if (isEmptyRange(range2)) {
            return range1;
        }
        if (isStartOpen(range1)) {
            if (range2.hasEndKey()) {
                return Range.newBuilder().setEndKey(range2.getEndKey()).build();
            } else {
                return FULL_RANGE;
            }
        }
        if (isStartOpen(range2)) {
            if (range1.hasEndKey()) {
                return Range.newBuilder().setEndKey(range1.getEndKey()).build();
            } else {
                return FULL_RANGE;
            }
        }
        if (isEndOpen(range1)) {
            return Range.newBuilder().setStartKey(range2.getStartKey()).build();
        }
        if (isEndOpen(range2)) {
            return Range.newBuilder().setStartKey(range1.getStartKey()).build();
        }
        if (range1.getStartKey().equals(range2.getEndKey())) {
            return Range.newBuilder()
                .setStartKey(range2.getStartKey())
                .setEndKey(range1.getEndKey())
                .build();
        } else {
            return Range.newBuilder()
                .setStartKey(range1.getStartKey())
                .setEndKey(range2.getEndKey())
                .build();
        }
    }

    public static boolean canCombine(Range range1, Range range2) {
        return isEmptyRange(range1) ||
            isEmptyRange(range2) ||
            (isStartOpen(range1) && range1.getEndKey().equals(range2.getStartKey())) ||
            (isStartOpen(range2)) && range2.getEndKey().equals(range1.getStartKey()) ||
            (isEndOpen(range1) && range1.getStartKey().equals(range2.getEndKey())) ||
            (isEndOpen(range2) && range1.getEndKey().equals(range2.getStartKey())) ||
            (isClose(range1) && isClose(range2) && (range1.getStartKey().equals(range2.getEndKey()) ||
                range1.getEndKey().equals(range2.getStartKey())));
    }

    public static boolean isEmptyRange(Range range) {
        return range.equals(EMPTY_RANGE);
    }

    public static boolean isStartOpen(Range range) {
        return !range.hasStartKey();
    }

    public static boolean isEndOpen(Range range) {
        return !range.hasEndKey();
    }

    public static boolean isClose(Range range) {
        return range.hasStartKey() && range.hasEndKey();
    }

    public static int compare(byte[] a, byte[] b) {
        return compare(unsafeWrap(a), unsafeWrap(b));
    }

    public static int compare(ByteString a, ByteString b) {
        return ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    public static String toString(Range range) {
        if (range.equals(EMPTY_RANGE)) {
            return "[EMPTY]";
        }
        if (range.equals(FULL_RANGE)) {
            return "[FULL]";
        }
        if (range.getStartKey().isEmpty()) {
            return String.format("(,%s]", toString(range.getEndKey()));
        }
        if (range.getEndKey().isEmpty()) {
            return String.format("[%s,)", toString(range.getStartKey()));
        }
        return String.format("[%s,%s)", toString(range.getStartKey()), toString(range.getEndKey()));
    }

    private static String toString(ByteString bs) {
        StringBuilder s = new StringBuilder();
        if (!bs.isEmpty()) {
            s.append("0x");
            s.append(BaseEncoding.base16().encode(bs.toByteArray()));
            if (bs.isValidUtf8() && !bs.toStringUtf8().trim().isEmpty()) {
                s.append("(\"").append(bs.toStringUtf8()).append("\")");
            }
        }
        return s.toString();
    }
}
