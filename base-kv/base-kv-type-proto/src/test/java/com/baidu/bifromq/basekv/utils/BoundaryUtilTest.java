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

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.EMPTY_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.inRange;
import static com.google.protobuf.ByteString.EMPTY;
import static com.google.protobuf.ByteString.copyFrom;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class BoundaryUtilTest {
    @Test
    public void boundaryValidation() {
        assertTrue(BoundaryUtil.isValid(null, null));
        assertTrue(BoundaryUtil.isValid(null, ByteString.EMPTY));
        assertTrue(BoundaryUtil.isValid(ByteString.EMPTY, null));
        assertTrue(BoundaryUtil.isValid(ByteString.EMPTY, ByteString.EMPTY));

        assertTrue(BoundaryUtil.isValid(ByteString.EMPTY, copyFromUtf8("a")));

        assertFalse(BoundaryUtil.isValid(copyFromUtf8("a"), ByteString.EMPTY));
        assertFalse(BoundaryUtil.isValid(copyFromUtf8("b"), copyFromUtf8("a")));
    }

    @Test
    public void keyInBoundary() {
        Boundary range = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("z")).build();
        assertFalse(inRange(copyFromUtf8("1"), range));
        assertTrue(inRange(copyFromUtf8("a"), range));
        assertFalse(inRange(copyFromUtf8("z"), range));

        assertTrue(inRange(copyFromUtf8("a"), null, null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("a"), null));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null));

        assertFalse(inRange(copyFromUtf8("a"), null, copyFromUtf8("a")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("c")));

        assertTrue(inRange(copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(inRange(copyFromUtf8("d"), copyFromUtf8("b"), copyFromUtf8("c")));

        try {
            assertFalse(inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a")));
            fail();
        } catch (AssertionError error) {

        }
    }

    @Test
    public void boundaryInBoundary() {

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, null));
        assertTrue(inRange(null, EMPTY, null, null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), null));
        assertTrue(inRange(null, EMPTY, copyFromUtf8("a"), null));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), null, copyFromUtf8("b")));
        assertTrue(inRange(null, EMPTY, null, copyFromUtf8("b")));

        assertTrue(inRange(copyFromUtf8("a"), copyFromUtf8("b"), copyFromUtf8("a"), copyFromUtf8("c")));
        assertTrue(inRange(null, EMPTY, copyFromUtf8("a"), copyFromUtf8("c")));

        assertTrue(inRange(copyFromUtf8("b"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("c")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("a"), copyFromUtf8("b")));

        assertFalse(inRange(copyFromUtf8("a"), copyFromUtf8("c"), copyFromUtf8("b"), copyFromUtf8("d")));

        Boundary a_e = boundary("a", "e");
        Boundary b_d = boundary("b", "d");

        assertFalse(inRange(a_e, b_d));
        assertTrue(inRange(b_d, a_e));
        assertTrue(inRange(a_e, a_e));

        assertTrue(inRange(EMPTY_BOUNDARY, a_e));
        assertFalse(inRange(a_e, EMPTY_BOUNDARY));

        assertTrue(inRange(EMPTY_BOUNDARY, FULL_BOUNDARY));
        assertFalse(inRange(FULL_BOUNDARY, EMPTY_BOUNDARY));

        assertTrue(inRange(a_e, FULL_BOUNDARY));
        assertFalse(inRange(FULL_BOUNDARY, a_e));

        assertTrue(inRange(FULL_BOUNDARY, FULL_BOUNDARY));
        assertTrue(inRange(EMPTY_BOUNDARY, EMPTY_BOUNDARY));
    }

    @Test
    public void findUpperBound() {
        assertEquals(ByteString.copyFrom(new byte[] {1, 2, 4}),
            BoundaryUtil.upperBound(ByteString.copyFrom(new byte[] {1, 2, 3})));
        assertEquals(ByteString.copyFrom(new byte[] {1, 3, (byte) 0xFF}),
            BoundaryUtil.upperBound(ByteString.copyFrom(new byte[] {1, 2, (byte) 0xFF})));
        assertEquals(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
            BoundaryUtil.upperBound(ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})));
    }

    @Test
    public void leastUpperBound() {
        assertFalse(BoundaryUtil.leastUpperBound(FULL_BOUNDARY, FULL_BOUNDARY).isPresent());
        assertEquals(BoundaryUtil.leastUpperBound(FULL_BOUNDARY, EMPTY_BOUNDARY).get(), EMPTY);
        assertEquals(BoundaryUtil.leastUpperBound(FULL_BOUNDARY, boundary("a", "b")).get(), copyFromUtf8("b"));
        assertEquals(BoundaryUtil.leastUpperBound(boundary("a", "b"), boundary("a", "b")).get(), copyFromUtf8("b"));
        assertEquals(BoundaryUtil.leastUpperBound(boundary("a", "b"), boundary("a", "b1")).get(), copyFromUtf8("b"));
    }

    @Test
    public void greaterLowerBound() {
        assertFalse(BoundaryUtil.greaterLowerBound(FULL_BOUNDARY, FULL_BOUNDARY).isPresent());
        assertFalse(BoundaryUtil.greaterLowerBound(FULL_BOUNDARY, EMPTY_BOUNDARY).isPresent());
        assertEquals(BoundaryUtil.greaterLowerBound(boundary("a", "b"), FULL_BOUNDARY).get(), copyFromUtf8("a"));
        assertEquals(BoundaryUtil.greaterLowerBound(boundary("a", "b"), boundary("a", "a")).get(), copyFromUtf8("a"));
        assertEquals(BoundaryUtil.greaterLowerBound(boundary("a", "b"), boundary("a1", "a")).get(), copyFromUtf8("a1"));
    }

    @Test
    public void isOverlap() {
        Boundary _a = boundary(null, "a");
        Boundary a_b = boundary("a", "b");
        Boundary b_c = boundary("b", "c");
        Boundary a_e = boundary("a", "e");
        Boundary b_f = boundary("b", "f");
        Boundary f_ = boundary("f", null);
        assertTrue(BoundaryUtil.isOverlap(FULL_BOUNDARY, _a));
        assertTrue(BoundaryUtil.isOverlap(_a, FULL_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(a_b, b_c));
        assertFalse(BoundaryUtil.isOverlap(b_c, a_b));
        assertTrue(BoundaryUtil.isOverlap(a_b, FULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(FULL_BOUNDARY, f_));
        assertTrue(BoundaryUtil.isOverlap(f_, FULL_BOUNDARY));
        assertTrue(BoundaryUtil.isOverlap(a_b, a_e));
        assertTrue(BoundaryUtil.isOverlap(b_f, a_e));
        assertTrue(BoundaryUtil.isOverlap(a_e, b_f));
        assertFalse(BoundaryUtil.isOverlap(EMPTY_BOUNDARY, EMPTY_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(EMPTY_BOUNDARY, FULL_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(FULL_BOUNDARY, EMPTY_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(a_e, EMPTY_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(EMPTY_BOUNDARY, a_e));
        assertFalse(BoundaryUtil.isOverlap(_a, EMPTY_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(EMPTY_BOUNDARY, _a));
        assertFalse(BoundaryUtil.isOverlap(f_, EMPTY_BOUNDARY));
        assertFalse(BoundaryUtil.isOverlap(EMPTY_BOUNDARY, f_));
    }

    @Test
    public void intersect() {
        Boundary _a = boundary(null, "a");
        Boundary a_e = boundary("a", "e");
        Boundary b_f = boundary("b", "f");
        Boundary f_g = boundary("f", "g");
        Boundary f_ = boundary("f", null);
        assertEquals(BoundaryUtil.intersect(a_e, a_e), a_e);
        assertEquals(BoundaryUtil.intersect(_a, _a), _a);
        assertEquals(BoundaryUtil.intersect(f_, f_), f_);
        assertEquals(BoundaryUtil.intersect(a_e, b_f), boundary("b", "e"));
        assertEquals(BoundaryUtil.intersect(b_f, a_e), boundary("b", "e"));
        assertEquals(BoundaryUtil.intersect(f_g, FULL_BOUNDARY), boundary("f", "g"));
        assertEquals(BoundaryUtil.intersect(f_g, f_), boundary("f", "g"));
        assertEquals(BoundaryUtil.intersect(_a, FULL_BOUNDARY), _a);
        assertEquals(BoundaryUtil.intersect(f_, FULL_BOUNDARY), f_);
        assertEquals(BoundaryUtil.intersect(_a, EMPTY_BOUNDARY), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(f_, EMPTY_BOUNDARY), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(_a, a_e), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(b_f, f_g), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(EMPTY_BOUNDARY, FULL_BOUNDARY), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(EMPTY_BOUNDARY, EMPTY_BOUNDARY), EMPTY_BOUNDARY);
        assertEquals(BoundaryUtil.intersect(FULL_BOUNDARY, FULL_BOUNDARY), FULL_BOUNDARY);
    }

    @Test
    public void inFullBoundary() {
        assertTrue(inRange(copyFromUtf8(""), FULL_BOUNDARY));
        assertTrue(inRange(copyFrom(new byte[0]), FULL_BOUNDARY));
        assertTrue(inRange(copyFromUtf8("abc"), FULL_BOUNDARY));
    }

    @Test
    public void inBounaryWithEmptyStartKey() {
        assertTrue(inRange(copyFromUtf8(""), Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFrom(new byte[0]), Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("a1"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
        assertFalse(inRange(copyFromUtf8("b"), Boundary.newBuilder().setEndKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void inBoundaryWithEmptyEndKey() {
        assertFalse(inRange(copyFromUtf8(""), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFrom(new byte[0]), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertTrue(inRange(copyFromUtf8("a"), Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build()));
        assertFalse(inRange(copyFromUtf8("a"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
        assertTrue(inRange(copyFromUtf8("b1"), Boundary.newBuilder().setStartKey(copyFromUtf8("b")).build()));
    }

    @Test
    public void canCombine() {
        Boundary a_b = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).setEndKey(copyFromUtf8("b")).build();
        Boundary b_c = Boundary.newBuilder().setStartKey(copyFromUtf8("b")).setEndKey(copyFromUtf8("c")).build();
        Boundary d_e = Boundary.newBuilder().setStartKey(copyFromUtf8("d")).setEndKey(copyFromUtf8("e")).build();
        Boundary _a = Boundary.newBuilder().setEndKey(copyFromUtf8("a")).build();
        Boundary a_ = Boundary.newBuilder().setStartKey(copyFromUtf8("a")).build();
        Boundary e_ = Boundary.newBuilder().setStartKey(copyFromUtf8("e")).build();

        assertFalse(BoundaryUtil.canCombine(a_b, a_b));
        assertTrue(BoundaryUtil.canCombine(EMPTY_BOUNDARY, EMPTY_BOUNDARY));

        assertTrue(BoundaryUtil.canCombine(a_b, b_c));
        assertTrue(BoundaryUtil.canCombine(b_c, a_b));

        assertFalse(BoundaryUtil.canCombine(a_b, d_e));
        assertFalse(BoundaryUtil.canCombine(d_e, a_b));

        assertTrue(BoundaryUtil.canCombine(a_b, EMPTY_BOUNDARY));
        assertTrue(BoundaryUtil.canCombine(EMPTY_BOUNDARY, a_b));

        assertTrue(BoundaryUtil.canCombine(_a, a_b));
        assertTrue(BoundaryUtil.canCombine(a_b, _a));

        assertTrue(BoundaryUtil.canCombine(e_, d_e));
        assertTrue(BoundaryUtil.canCombine(d_e, e_));

        assertTrue(BoundaryUtil.canCombine(_a, a_));
        assertTrue(BoundaryUtil.canCombine(a_, _a));

        assertFalse(BoundaryUtil.canCombine(_a, e_));
        assertFalse(BoundaryUtil.canCombine(e_, _a));

        assertTrue(BoundaryUtil.canCombine(a_, EMPTY_BOUNDARY));
        assertTrue(BoundaryUtil.canCombine(EMPTY_BOUNDARY, a_));
    }

    @Test
    public void combine() {
        Boundary a_b = boundary("a", "b");
        Boundary b_c = boundary("b", "c");
        Boundary a_c = boundary("a", "c");
        Boundary d_e = boundary("d", "e");
        Boundary _a = boundary(null, "a");
        Boundary _b = boundary(null, "b");
        Boundary _c = boundary(null, "c");
        Boundary d_ = boundary("d", null);
        Boundary a_ = boundary("a", null);
        Boundary e_ = boundary("e", null);

        assertEquals(EMPTY_BOUNDARY, BoundaryUtil.combine(EMPTY_BOUNDARY, EMPTY_BOUNDARY));

        assertEquals(a_c, BoundaryUtil.combine(a_b, b_c));
        assertEquals(a_c, BoundaryUtil.combine(b_c, a_b));

        assertEquals(a_b, BoundaryUtil.combine(a_b, EMPTY_BOUNDARY));
        assertEquals(a_b, BoundaryUtil.combine(EMPTY_BOUNDARY, a_b));

        assertEquals(_b, BoundaryUtil.combine(_a, a_b));
        assertEquals(_b, BoundaryUtil.combine(a_b, _a));

        assertEquals(d_, BoundaryUtil.combine(e_, d_e));
        assertEquals(d_, BoundaryUtil.combine(d_e, e_));

        assertEquals(FULL_BOUNDARY, BoundaryUtil.combine(_a, a_));
        assertEquals(FULL_BOUNDARY, BoundaryUtil.combine(a_, _a));

        assertEquals(a_, BoundaryUtil.combine(a_, EMPTY_BOUNDARY));
        assertEquals(a_, BoundaryUtil.combine(EMPTY_BOUNDARY, a_));

        assertEquals(_c, BoundaryUtil.combine(_a, a_b, b_c));
        assertEquals(_c, BoundaryUtil.combine(a_b, _a, b_c));
        assertEquals(_c, BoundaryUtil.combine(b_c, a_b, _a));
    }

    private Boundary boundary(String startKey, String endKey) {
        Boundary.Builder builder = Boundary.newBuilder();
        if (startKey != null) {
            builder.setStartKey(copyFromUtf8(startKey));
        }
        if (endKey != null) {
            builder.setEndKey(copyFromUtf8(endKey));
        }
        return builder.build();
    }
}
