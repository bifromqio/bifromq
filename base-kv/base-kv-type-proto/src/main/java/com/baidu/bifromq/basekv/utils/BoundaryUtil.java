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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.Optional;

public class BoundaryUtil {
    public static final ByteString MIN_KEY = ByteString.EMPTY;
    public static final Boundary EMPTY_BOUNDARY = Boundary.newBuilder().setEndKey(MIN_KEY).build();
    public static final Boundary FULL_BOUNDARY = Boundary.getDefaultInstance();

    public static boolean isValid(ByteString start, ByteString end) {
        if (start == null || end == null) {
            return true;
        }
        return compare(start, end) <= 0;
    }

    public static boolean isValid(Boundary boundary) {
        return isValid(startKey(boundary), endKey(boundary));
    }

    public static boolean inRange(ByteString key, ByteString start, ByteString end) {
        assert isValid(start, end);
        if (start == null && end == null) {
            return true;
        }
        if (end == null) {
            // right open range
            return compare(start, key) <= 0;
        }
        if (start == null) {
            return compare(key, end) < 0;
        }
        return compare(start, key) <= 0 && compare(key, end) < 0;
    }

    public static boolean inRange(ByteString key, Boundary boundary) {
        return inRange(key, startKey(boundary), endKey(boundary));
    }

    public static boolean inRange(Boundary boundary1, Boundary boundary2) {
        return inRange(startKey(boundary1), endKey(boundary1), startKey(boundary2), endKey(boundary2));
    }

    public static boolean inRange(ByteString start1,
                                  ByteString end1,
                                  ByteString start2,
                                  ByteString end2) {
        assert isValid(start1, end1) && isValid(start2, end2);

        if (start2 == null && end2 == null) {
            // open-ended range
            return true;
        }
        if (start2 != null && end2 == null) {
            // right open-ended
            if (start1 == null) {
                return ByteString.EMPTY.equals(end1);
            }
            // start2 <= start1
            return compare(start1, start2) >= 0;
        }
        if (start2 == null) {
            // left open-ended
            if (end1 == null) {
                return false;
            }
            // end1 <= end2
            return compare(end1, end2) <= 0;
        }
        // range2 is closed ended
        if (start1 == null && end1 == null) {
            return false;
        } else if (start1 == null) {
            return ByteString.EMPTY.equals(end1);
        } else {
            return compare(start2, start1) <= 0 && compare(end1, end2) <= 0;
        }
    }

    public static ByteString upperBound(ByteString key) {
        return UnsafeByteOperations.unsafeWrap(upperBoundInternal(key.toByteArray()));
    }

    public static byte[] upperBound(byte[] key) {
        byte[] upperBound = new byte[key.length];
        System.arraycopy(key, 0, upperBound, 0, key.length);
        return upperBoundInternal(upperBound);
    }

    private static byte[] upperBoundInternal(byte[] upperBound) {
        int i = upperBound.length;
        while (--i >= 0) {
            byte b = upperBound[i];
            if (compare(new byte[] {b}, new byte[] {(byte) 0xFF}) < 0) {
                upperBound[i]++;
                break;
            }
        }
        return upperBound;
    }

    public static int compare(byte[] a, byte[] b) {
        return compare(unsafeWrap(a), unsafeWrap(b));
    }

    public static int compare(ByteString a, ByteString b) {
        return ByteString.unsignedLexicographicalComparator().compare(a, b);
    }

    public static ByteString startKey(Boundary range) {
        return range.hasStartKey() ? range.getStartKey() : null;
    }

    public static ByteString endKey(Boundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey() : null;
    }

    public static byte[] startKeyBytes(Boundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey().toByteArray() : null;
    }

    public static byte[] endKeyBytes(Boundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey().toByteArray() : null;
    }

    public static Optional<ByteString> greaterLowerBound(Boundary range1, Boundary range2) {
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

    public static Optional<ByteString> leastUpperBound(Boundary range1, Boundary range2) {
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

    public static boolean isOverlap(Boundary boundary1, Boundary boundary2) {
        if (boundary1.equals(EMPTY_BOUNDARY) || boundary2.equals(EMPTY_BOUNDARY)) {
            return false;
        }
        if (!boundary1.hasEndKey() && !boundary2.hasEndKey()) {
            return true;
        } else if (!boundary1.hasEndKey()) {
            return compare(boundary1.getStartKey(), boundary2.getEndKey()) < 0;
        } else if (!boundary2.hasEndKey()) {
            return compare(boundary2.getStartKey(), boundary1.getEndKey()) < 0;
        }
        return !(compare(boundary1.getEndKey(), boundary2.getStartKey()) <= 0
            || compare(boundary2.getEndKey(), boundary1.getStartKey()) <= 0);
    }

    public static Boundary intersect(Boundary boundary1, Boundary boundary2) {
        if (isOverlap(boundary1, boundary2)) {
            Optional<ByteString> lowerBound = greaterLowerBound(boundary1, boundary2);
            Optional<ByteString> upperBound = leastUpperBound(boundary1, boundary2);
            Boundary.Builder rangeBuilder = Boundary.newBuilder();
            lowerBound.ifPresent(rangeBuilder::setStartKey);
            upperBound.ifPresent(rangeBuilder::setEndKey);
            return rangeBuilder.build();
        }
        return EMPTY_BOUNDARY;
    }

    public static boolean canCombine(Boundary boundary1, Boundary boundary2) {
        return isEmptyRange(boundary1) ||
            isEmptyRange(boundary2) ||
            (isStartOpen(boundary1) && boundary1.getEndKey().equals(boundary2.getStartKey())) ||
            (isStartOpen(boundary2)) && boundary2.getEndKey().equals(boundary1.getStartKey()) ||
            (isEndOpen(boundary1) && boundary1.getStartKey().equals(boundary2.getEndKey())) ||
            (isEndOpen(boundary2) && boundary1.getEndKey().equals(boundary2.getStartKey())) ||
            (isClose(boundary1) && isClose(boundary2) && (boundary1.getStartKey().equals(boundary2.getEndKey()) ||
                boundary1.getEndKey().equals(boundary2.getStartKey())));
    }

    public static Boundary combine(Boundary... ranges) {
        assert ranges.length >= 2;
        Boundary range = ranges[0];
        for (int i = 1; i < ranges.length; i++) {
            range = combine2Range(range, ranges[i]);
        }
        return range;
    }

    private static Boundary combine2Range(Boundary range1, Boundary range2) {
        assert canCombine(range1, range2);
        if (isEmptyRange(range1)) {
            return range2;
        }
        if (isEmptyRange(range2)) {
            return range1;
        }
        if (isStartOpen(range1)) {
            if (range2.hasEndKey()) {
                return Boundary.newBuilder().setEndKey(range2.getEndKey()).build();
            } else {
                return FULL_BOUNDARY;
            }
        }
        if (isStartOpen(range2)) {
            if (range1.hasEndKey()) {
                return Boundary.newBuilder().setEndKey(range1.getEndKey()).build();
            } else {
                return FULL_BOUNDARY;
            }
        }
        if (isEndOpen(range1)) {
            return Boundary.newBuilder().setStartKey(range2.getStartKey()).build();
        }
        if (isEndOpen(range2)) {
            return Boundary.newBuilder().setStartKey(range1.getStartKey()).build();
        }
        if (range1.getStartKey().equals(range2.getEndKey())) {
            return Boundary.newBuilder()
                .setStartKey(range2.getStartKey())
                .setEndKey(range1.getEndKey())
                .build();
        } else {
            return Boundary.newBuilder()
                .setStartKey(range1.getStartKey())
                .setEndKey(range2.getEndKey())
                .build();
        }
    }

    public static boolean isEmptyRange(Boundary boundary) {
        return boundary.equals(EMPTY_BOUNDARY);
    }

    public static boolean isStartOpen(Boundary boundary) {
        return !boundary.hasStartKey();
    }

    public static boolean isEndOpen(Boundary boundary) {
        return !boundary.hasEndKey();
    }

    public static boolean isClose(Boundary boundary) {
        return boundary.hasStartKey() && boundary.hasEndKey();
    }


}
