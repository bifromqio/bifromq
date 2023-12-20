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

package com.baidu.bifromq.dist.worker;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class RendezvousHash<K, N> {

    /**
     * A hashing function from guava, ie Hashing.murmur3_128()
     */
    private final HashFunction hasher;

    /**
     * A funnel to describe how to take the key and add it to a hash.
     *
     * @see Funnel
     */
    private final Funnel<K> keyFunnel;

    /**
     * Funnel describing how to take the type of the node and add it to a hash
     */
    private final Funnel<N> nodeFunnel;

    /**
     * All the current nodes in the pool
     */
    private final SortedSet<N> ordered;

    /**
     * Creates a new RendezvousHash using guava provided hash function.
     *
     * @param hasher     hash function
     * @param keyFunnel  key funnel for calculate hashing
     * @param nodeFunnel node funnel for calculate hashing
     * @param comparator node comparator
     */
    public RendezvousHash(HashFunction hasher, Funnel<K> keyFunnel, Funnel<N> nodeFunnel, Comparator<N> comparator) {
        if (hasher == null) {
            throw new IllegalArgumentException("hasher");
        }
        if (keyFunnel == null) {
            throw new IllegalArgumentException("keyFunnel");
        }
        if (nodeFunnel == null) {
            throw new IllegalArgumentException("nodeFunnel");
        }
        if (comparator == null) {
            throw new IllegalArgumentException("comparator");
        }
        this.hasher = hasher;
        this.keyFunnel = keyFunnel;
        this.nodeFunnel = nodeFunnel;
        this.ordered = new TreeSet<N>(comparator);
    }

    /**
     * Removes a node from the pool. Keys that referenced it should after this be evenly distributed amongst the other
     * nodes
     *
     * @return true if the node was in the pool
     */
    public boolean remove(N node) {
        return ordered.remove(node);
    }

    /**
     * Add a new node to pool and take an even distribution of the load off existing nodes
     *
     * @return true if node did not previously exist in pool
     */
    public boolean add(N node) {
        return ordered.add(node);
    }

    /**
     * return a node for a given key
     */
    public N get(K key) {
        long maxValue = Long.MIN_VALUE;
        N max = null;
        for (N node : ordered) {
            long nodesHash = hasher
                .newHasher()
                .putObject(key, keyFunnel)
                .putObject(node, nodeFunnel)
                .hash()
                .asLong();
            if (nodesHash > maxValue) {
                max = node;
                maxValue = nodesHash;
            }
        }
        return max;
    }
}
