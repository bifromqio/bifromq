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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basekv.localengine.proto.KeyBoundary;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import javax.annotation.Nullable;

public class RangeUtil {
    public static boolean isValid(ByteString start, ByteString end) {
        if (start == null || end == null) {
            return true;
        }
        return compare(start, end) <= 0;
    }

    public static boolean isValid(KeyBoundary boundary) {
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

    public static boolean inRange(ByteString key, KeyBoundary boundary) {
        return inRange(key, startKey(boundary), endKey(boundary));
    }

    public static boolean inRange(KeyBoundary boundary1, KeyBoundary boundary2) {
        return inRange(startKey(boundary1), endKey(boundary2), startKey(boundary2), endKey(boundary2));
    }

    public static boolean inRange(@Nullable ByteString start1,
                                  @Nullable ByteString end1,
                                  @Nullable ByteString start2,
                                  @Nullable ByteString end2) {
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

    public static ByteString startKey(KeyBoundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey() : null;
    }

    public static ByteString endKey(KeyBoundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey() : null;
    }

    public static byte[] startKeyBytes(KeyBoundary boundary) {
        return boundary.hasStartKey() ? boundary.getStartKey().toByteArray() : null;
    }

    public static byte[] endKeyBytes(KeyBoundary boundary) {
        return boundary.hasEndKey() ? boundary.getEndKey().toByteArray() : null;
    }
}
