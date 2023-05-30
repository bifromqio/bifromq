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

import static com.baidu.bifromq.basekv.Constants.EMPTY_RANGE;
import static com.baidu.bifromq.basekv.Constants.FULL_RANGE;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.canCombine;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.combine;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.contains;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.greaterLowerBound;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.inRange;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.intersect;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.isOverlap;
import static com.baidu.bifromq.basekv.utils.KeyRangeUtil.leastUpperBound;
import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFrom;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.baidu.bifromq.basekv.proto.Range;
import org.junit.Test;

public class KeyRangeUtilTest {

    @Test
    public void testLeastUpperBound() {
        assertFalse(leastUpperBound(FULL_RANGE, FULL_RANGE).isPresent());
        assertEquals(EMPTY, leastUpperBound(FULL_RANGE, EMPTY_RANGE).get());
        assertEquals(copyFromUtf8("b"), leastUpperBound(FULL_RANGE, range("a", "b")).get());
        assertEquals(copyFromUtf8("b"), leastUpperBound(range("a", "b"), range("a", "b")).get());
        assertEquals(copyFromUtf8("b"), leastUpperBound(range("a", "b"), range("a", "b1")).get());
    }

    @Test
    public void testGreaterLowerBound() {
        assertFalse(greaterLowerBound(FULL_RANGE, FULL_RANGE).isPresent());
        assertFalse(greaterLowerBound(FULL_RANGE, EMPTY_RANGE).isPresent());
        assertEquals(copyFromUtf8("a"), greaterLowerBound(range("a", "b"), FULL_RANGE).get());
        assertEquals(copyFromUtf8("a"), greaterLowerBound(range("a", "b"), range("a", "a")).get());
        assertEquals(copyFromUtf8("a1"), greaterLowerBound(range("a", "b"), range("a1", "a")).get());
    }

    @Test
    public void testIsOverlap() {
        Range _a = range(null, "a");
        Range a_b = range("a", "b");
        Range b_c = range("b", "c");
        Range a_e = range("a", "e");
        Range b_f = range("b", "f");
        Range f_ = range("f", null);
        assertTrue(isOverlap(FULL_RANGE, _a));
        assertTrue(isOverlap(_a, FULL_RANGE));
        assertFalse(isOverlap(a_b, b_c));
        assertFalse(isOverlap(b_c, a_b));
        assertTrue(isOverlap(a_b, FULL_RANGE));
        assertTrue(isOverlap(FULL_RANGE, f_));
        assertTrue(isOverlap(f_, FULL_RANGE));
        assertTrue(isOverlap(a_b, a_e));
        assertTrue(isOverlap(b_f, a_e));
        assertTrue(isOverlap(a_e, b_f));
        assertFalse(isOverlap(EMPTY_RANGE, EMPTY_RANGE));
        assertFalse(isOverlap(EMPTY_RANGE, FULL_RANGE));
        assertFalse(isOverlap(FULL_RANGE, EMPTY_RANGE));
        assertFalse(isOverlap(a_e, EMPTY_RANGE));
        assertFalse(isOverlap(EMPTY_RANGE, a_e));
        assertFalse(isOverlap(_a, EMPTY_RANGE));
        assertFalse(isOverlap(EMPTY_RANGE, _a));
        assertFalse(isOverlap(f_, EMPTY_RANGE));
        assertFalse(isOverlap(EMPTY_RANGE, f_));
    }

    @Test
    public void testIntersect() {
        Range _a = range(null, "a");
        Range a_e = range("a", "e");
        Range b_f = range("b", "f");
        Range f_g = range("f", "g");
        Range f_ = range("f", null);
        assertEquals(a_e, intersect(a_e, a_e));
        assertEquals(_a, intersect(_a, _a));
        assertEquals(f_, intersect(f_, f_));
        assertEquals(range("b", "e"), intersect(a_e, b_f));
        assertEquals(range("b", "e"), intersect(b_f, a_e));
        assertEquals(range("f", "g"), intersect(f_g, FULL_RANGE));
        assertEquals(range("f", "g"), intersect(f_g, f_));
        assertEquals(_a, intersect(_a, FULL_RANGE));
        assertEquals(f_, intersect(f_, FULL_RANGE));
        assertEquals(EMPTY_RANGE, intersect(_a, EMPTY_RANGE));
        assertEquals(EMPTY_RANGE, intersect(f_, EMPTY_RANGE));
        assertEquals(EMPTY_RANGE, intersect(_a, a_e));
        assertEquals(EMPTY_RANGE, intersect(b_f, f_g));
        assertEquals(EMPTY_RANGE, intersect(EMPTY_RANGE, FULL_RANGE));
        assertEquals(EMPTY_RANGE, intersect(EMPTY_RANGE, EMPTY_RANGE));
        assertEquals(FULL_RANGE, intersect(FULL_RANGE, FULL_RANGE));
    }

    @Test
    public void testContains() {
        Range a_e = range("a", "e");
        Range b_d = range("b", "d");

        assertFalse(contains(a_e, b_d));
        assertTrue(contains(b_d, a_e));
        assertTrue(contains(a_e, a_e));

        assertTrue(contains(EMPTY_RANGE, a_e));
        assertFalse(contains(a_e, EMPTY_RANGE));

        assertTrue(contains(EMPTY_RANGE, FULL_RANGE));
        assertFalse(contains(FULL_RANGE, EMPTY_RANGE));

        assertTrue(contains(a_e, FULL_RANGE));
        assertFalse(contains(FULL_RANGE, a_e));

        assertTrue(contains(FULL_RANGE, FULL_RANGE));
        assertTrue(contains(EMPTY_RANGE, EMPTY_RANGE));
    }

    @Test
    public void testInFullRange() {
        assertTrue(inRange(copyFromUtf8(""), FULL_RANGE));
        assertTrue(inRange(copyFrom(new byte[0]), FULL_RANGE));
        assertTrue(inRange(copyFromUtf8("abc"), FULL_RANGE));
    }

    @Test
    public void testInRangeWithEmptyStartKey() {
        assertTrue(inRange(copyFromUtf8(""), Range.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFrom(new byte[0]), Range.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Range.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("a1"), Range.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertFalse(inRange(copyFromUtf8("b"), Range.newBuilder().setEndKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void testInRangeWithEmptyEndKey() {
        assertFalse(inRange(copyFromUtf8(""), Range.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFrom(new byte[0]), Range.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Range.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFromUtf8("a"), Range.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b"), Range.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b1"), Range.newBuilder().setStartKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void testInRange() {
        Range range = Range.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("z")).build();
        assertFalse(inRange(copyFromUtf8("1"), range));
        assertTrue(inRange(copyFromUtf8("a"), range));
        assertFalse(inRange(copyFromUtf8("z"), range));
    }

    @Test
    public void testCanCombine() {
        Range a_b = Range.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("b")).build();
        Range b_c = Range.newBuilder().setStartKey(copyFromUtf8("b")).setEndKey(copyFromUtf8("c")).build();
        Range d_e = Range.newBuilder().setStartKey(copyFromUtf8("d")).setEndKey(copyFromUtf8("e")).build();
        Range _a = Range.newBuilder().setEndKey(copyFromUtf8("a")).build();
        Range a_ = Range.newBuilder().setStartKey(copyFromUtf8("a")).build();
        Range e_ = Range.newBuilder().setStartKey(copyFromUtf8("e")).build();

        assertFalse(canCombine(a_b, a_b));
        assertTrue(canCombine(EMPTY_RANGE, EMPTY_RANGE));

        assertTrue(canCombine(a_b, b_c));
        assertTrue(canCombine(b_c, a_b));

        assertFalse(canCombine(a_b, d_e));
        assertFalse(canCombine(d_e, a_b));

        assertTrue(canCombine(a_b, EMPTY_RANGE));
        assertTrue(canCombine(EMPTY_RANGE, a_b));

        assertTrue(canCombine(_a, a_b));
        assertTrue(canCombine(a_b, _a));

        assertTrue(canCombine(e_, d_e));
        assertTrue(canCombine(d_e, e_));

        assertTrue(canCombine(_a, a_));
        assertTrue(canCombine(a_, _a));

        assertFalse(canCombine(_a, e_));
        assertFalse(canCombine(e_, _a));

        assertTrue(canCombine(a_, EMPTY_RANGE));
        assertTrue(canCombine(EMPTY_RANGE, a_));
    }

    @Test
    public void testCombine() {
        Range a_b = range("a", "b");
        Range b_c = range("b", "c");
        Range a_c = range("a", "c");
        Range d_e = range("d", "e");
        Range _a = range(null, "a");
        Range _b = range(null, "b");
        Range _c = range(null, "c");
        Range d_ = range("d", null);
        Range a_ = range("a", null);
        Range e_ = range("e", null);

        assertTrue(combine(EMPTY_RANGE, EMPTY_RANGE).equals(EMPTY_RANGE));

        assertTrue(combine(a_b, b_c).equals(a_c));
        assertTrue(combine(b_c, a_b).equals(a_c));

        assertTrue(combine(a_b, EMPTY_RANGE).equals(a_b));
        assertTrue(combine(EMPTY_RANGE, a_b).equals(a_b));

        assertTrue(combine(_a, a_b).equals(_b));
        assertTrue(combine(a_b, _a).equals(_b));

        assertTrue(combine(e_, d_e).equals(d_));
        assertTrue(combine(d_e, e_).equals(d_));

        assertTrue(combine(_a, a_).equals(FULL_RANGE));
        assertTrue(combine(a_, _a).equals(FULL_RANGE));

        assertTrue(combine(a_, EMPTY_RANGE).equals(a_));
        assertTrue(combine(EMPTY_RANGE, a_).equals(a_));

        assertTrue(combine(_a, a_b, b_c).equals(_c));
        assertTrue(combine(a_b, _a, b_c).equals(_c));
        assertTrue(combine(b_c, a_b, _a).equals(_c));
    }

    private Range range(String startKey, String endKey) {
        Range.Builder builder = Range.newBuilder();
        if (startKey != null) {
            builder.setStartKey(copyFromUtf8(startKey));
        }
        if (endKey != null) {
            builder.setEndKey(copyFromUtf8(endKey));
        }
        return builder.build();
    }
}
