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

package com.baidu.bifromq.basecrdt.core.internal;

import static com.baidu.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static com.baidu.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;
import static java.util.Collections.emptyIterator;
import static org.junit.Assert.assertEquals;

import com.baidu.bifromq.basecrdt.core.api.IMVReg;
import com.baidu.bifromq.basecrdt.core.api.MVRegOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import org.junit.Test;

public class MVRegTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(mvreg, "mvreg"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(mvreg, "mvreg"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();
    private final ByteString val1 = ByteString.copyFromUtf8("v1");
    private final ByteString val2 = ByteString.copyFromUtf8("v2");
    private final ByteString val3 = ByteString.copyFromUtf8("v3");

    @Test
    public void testOperation() {
        MVRegInflater mvRegInflater = new MVRegInflater(0, leftReplica,
            newStateLattice(leftReplica.getId(), 1000), executor, Duration.ofMillis(100));
        IMVReg mvReg = mvRegInflater.getCRDT();
        assertEquals(leftReplica, mvReg.id());

        mvReg.execute(MVRegOperation.write(val1)).join();
        TestUtil.assertSame(Sets.<ByteString>newHashSet(val1).iterator(), mvReg.read());

        mvReg.execute(MVRegOperation.reset());
        mvReg.execute(MVRegOperation.write(val2)).join();
        TestUtil.assertSame(Sets.<ByteString>newHashSet(val2).iterator(), mvReg.read());

        mvReg.execute(MVRegOperation.reset()).join();
        TestUtil.assertSame(emptyIterator(), mvReg.read());
    }

    @Test
    public void testJoin() {
        MVRegInflater leftInflater = new MVRegInflater(0, leftReplica, newStateLattice(leftReplica.getId(), 10000),
            executor, Duration.ofMillis(100));
        IMVReg left = leftInflater.getCRDT();

        MVRegInflater rightInflater = new MVRegInflater(1, rightReplica, newStateLattice(rightReplica.getId(), 10000),
            executor, Duration.ofMillis(100));
        IMVReg right = rightInflater.getCRDT();

        left.execute(MVRegOperation.write(val1)).join();
        right.execute(MVRegOperation.write(val2)).join();
        sync(leftInflater, rightInflater);

        TestUtil.assertUnorderedSame(Sets.newHashSet(val1, val2).iterator(), left.read());
        TestUtil.assertSame(left.read(), right.read());

        left.execute(MVRegOperation.write(val3)).join();
        right.execute(MVRegOperation.write(val3)).join();
        sync(leftInflater, rightInflater);
        TestUtil.assertSame(Lists.newArrayList(val3, val3).iterator(), left.read());
        TestUtil.assertSame(left.read(), right.read());
    }

    @Test
    public void testJoin1() throws InterruptedException {
        MVRegInflater leftInflater = new MVRegInflater(0, leftReplica, newStateLattice(leftReplica.getId(), 1000),
            executor, Duration.ofMillis(100));
        IMVReg left = leftInflater.getCRDT();

        MVRegInflater rightInflater = new MVRegInflater(1, rightReplica, newStateLattice(rightReplica.getId(), 1000),
            executor, Duration.ofMillis(100));
        IMVReg right = rightInflater.getCRDT();

        left.execute(MVRegOperation.write(val1)).join();
        sync(leftInflater, rightInflater);

        TestUtil.assertUnorderedSame(Sets.<ByteString>newHashSet(val1).iterator(), left.read());
        TestUtil.assertSame(left.read(), right.read());

        left.execute(MVRegOperation.reset()).join();
        Thread.sleep(3000);
        sync(leftInflater, rightInflater);

        TestUtil.assertSame(Lists.<ByteString>newArrayList(val1).iterator(), left.read());
        TestUtil.assertSame(left.read(), right.read());
    }
}
