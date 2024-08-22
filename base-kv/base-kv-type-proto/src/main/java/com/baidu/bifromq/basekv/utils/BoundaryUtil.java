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

package com.baidu.bifromq.basekv.utils;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BoundaryUtil {
    public static final ByteString MIN_KEY = ByteString.EMPTY;
    public static final Boundary EMPTY_BOUNDARY = Boundary.newBuilder().setEndKey(MIN_KEY).build();
    public static final Boundary FULL_BOUNDARY = Boundary.getDefaultInstance();

    public static List<Boundary> findGaps(List<Boundary> boundaries) {
        // Sort boundaries based on their start keys
        boundaries.sort((b1, b2) -> {
            // Compare startKey first
            ByteString startKey1 = startKey(b1);
            ByteString startKey2 = startKey(b2);

            if (startKey1 == null && startKey2 == null) {
                return 0; // both are open on the left
            }
            if (startKey1 == null) {
                return -1; // open on the left is less than any specific startKey
            }
            if (startKey2 == null) {
                return 1; // any specific startKey is greater than open on the left
            }

            return compare(startKey1, startKey2);
        });

        List<Boundary> gaps = new ArrayList<>();

        // Track the current coverage of the key space
        ByteString currentEndKey = null;

        for (Boundary boundary : boundaries) {
            ByteString startKey = startKey(boundary);
            ByteString endKey = endKey(boundary);

            // If currentEndKey is null, this is the first range
            if (currentEndKey == null) {
                if (boundary.equals(FULL_BOUNDARY)) {
                    break;
                }
                // check for a gap at the beginning
                if (!isStartOpen(boundary)) {
                    gaps.add(Boundary.newBuilder().setEndKey(startKey).build());
                }
            } else {
                // Check if there is a gap between the current endKey and the next startKey
                if (compare(currentEndKey, startKey) < 0) {
                    gaps.add(Boundary.newBuilder().setStartKey(currentEndKey).setEndKey(startKey).build());
                }
            }

            // Update the currentEndKey
            currentEndKey = endKey;
            if (currentEndKey == null) {
                // If the range is open on the right
                break;
            }
        }

        // After processing all boundaries, check if there is a gap at the end
        if (currentEndKey != null) {
            gaps.add(Boundary.newBuilder().setStartKey(currentEndKey).build());
        }

        return gaps;
    }

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

    public static ByteString startKey(Boundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey() : null;
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

    public static Optional<ByteString> greaterLowerBound(Boundary boundary1, Boundary boundary2) {
        if (!boundary1.hasStartKey() && !boundary2.hasStartKey()) {
            return Optional.empty();
        }
        if (!boundary1.hasStartKey()) {
            return Optional.of(boundary2.getStartKey());
        }
        if (!boundary2.hasStartKey()) {
            return Optional.of(boundary1.getStartKey());
        }
        return Optional.of(compare(boundary1.getStartKey(),
            boundary2.getStartKey()) < 0 ? boundary2.getStartKey() : boundary1.getStartKey());
    }

    public static Optional<ByteString> leastUpperBound(Boundary boundary1, Boundary boundary2) {
        if (!boundary1.hasEndKey() && !boundary2.hasEndKey()) {
            return Optional.empty();
        }
        if (!boundary1.hasEndKey()) {
            return Optional.of(boundary2.getEndKey());
        }
        if (!boundary2.hasEndKey()) {
            return Optional.of(boundary1.getEndKey());
        }
        return Optional.of(compare(boundary1.getEndKey(), boundary2.getEndKey()) < 0 ?
            boundary1.getEndKey() : boundary2.getEndKey());
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

    public static Boundary combine(Boundary... boundaries) {
        assert boundaries.length >= 2;
        Boundary range = boundaries[0];
        for (int i = 1; i < boundaries.length; i++) {
            range = combine2Range(range, boundaries[i]);
        }
        return range;
    }

    public static boolean isSplittable(Boundary boundary, ByteString splitKey) {
        if (compare(MIN_KEY, splitKey) >= 0 || !inRange(splitKey, boundary)) {
            return false;
        }
        return !splitKey.equals(boundary.getStartKey()) ||
            splitKey.equals(boundary.getStartKey()) && !upperBound(splitKey).equals(boundary.getEndKey());
    }

    public static Boundary[] split(Boundary boundary, ByteString splitKey) {
        assert isSplittable(boundary, splitKey);
        if (boundary.getStartKey().equals(splitKey)) {
            Boundary left = boundary.toBuilder().setEndKey(upperBound(splitKey)).build();
            Boundary right = boundary.toBuilder().setStartKey(upperBound(splitKey)).build();
            return new Boundary[] {left, right};
        }
        Boundary left = boundary.toBuilder().setEndKey(splitKey).build();
        Boundary right = boundary.toBuilder().setStartKey(splitKey).build();
        return new Boundary[] {left, right};
    }

    private static Boundary combine2Range(Boundary boundary1, Boundary boundary2) {
        assert canCombine(boundary1, boundary2);
        if (isEmptyRange(boundary1)) {
            return boundary2;
        }
        if (isEmptyRange(boundary2)) {
            return boundary1;
        }
        if (isStartOpen(boundary1)) {
            if (boundary2.hasEndKey()) {
                return Boundary.newBuilder().setEndKey(boundary2.getEndKey()).build();
            } else {
                return FULL_BOUNDARY;
            }
        }
        if (isStartOpen(boundary2)) {
            if (boundary1.hasEndKey()) {
                return Boundary.newBuilder().setEndKey(boundary1.getEndKey()).build();
            } else {
                return FULL_BOUNDARY;
            }
        }
        if (isEndOpen(boundary1)) {
            return Boundary.newBuilder().setStartKey(boundary2.getStartKey()).build();
        }
        if (isEndOpen(boundary2)) {
            return Boundary.newBuilder().setStartKey(boundary1.getStartKey()).build();
        }
        if (boundary1.getStartKey().equals(boundary2.getEndKey())) {
            return Boundary.newBuilder()
                .setStartKey(boundary2.getStartKey())
                .setEndKey(boundary1.getEndKey())
                .build();
        } else {
            return Boundary.newBuilder()
                .setStartKey(boundary1.getStartKey())
                .setEndKey(boundary2.getEndKey())
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
