/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.dist.worker.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Branch<V> {
    final INode<V> iNode;
    final Set<V> values;

    Branch(INode<V> iNode) {
        this.iNode = iNode;
        this.values = new HashSet<>();
    }

    Branch(V value) {
        this.iNode = null;
        this.values = new HashSet<>(Collections.singleton(value));
    }

    Branch<V> updated(INode<V> iNode) {
        return new Branch<V>(iNode, values);
    }

    Branch<V> updated(V value) {
        Set<V> newSubs = new HashSet<>(values);
        newSubs.add(value);
        return new Branch<>(iNode, newSubs);
    }

    Branch<V> removed(V sub) {
        Set<V> newSubs = new HashSet<>(values);
        newSubs.remove(sub);
        return new Branch<>(iNode, newSubs);
    }

    List<V> values() {
        return new ArrayList<>(values);
    }

    Branch(INode<V> iNode, Set<V> values) {
        this.iNode = iNode;
        this.values = values;
    }
}
