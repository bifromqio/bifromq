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

package com.baidu.bifromq.dist.worker;

import static com.google.common.hash.Hashing.murmur3_128;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class RendezvousHashTest {
    @Test
    public void hash() {
        RendezvousHash<Integer, String> hash = new RendezvousHash<>(murmur3_128(),
            (from, into) -> into.putInt(from), (from, into) -> into.putBytes(from.getBytes()), String::compareTo);
        Set<String> nodes = Sets.newHashSet("Node1", "Node2", "Node3", "Node4");
        nodes.forEach(hash::add);
        assertTrue(nodes.contains(hash.get(ThreadLocalRandom.current().nextInt())));
    }
}
