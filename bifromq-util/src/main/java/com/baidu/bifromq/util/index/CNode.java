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

package com.baidu.bifromq.util.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CNode<V> {
    final Map<String, Branch<V>> branches;

    CNode() {
        this.branches = new HashMap<>();
    }

    CNode(Map<String, Branch<V>> branches) {
        this.branches = branches;
    }

    CNode(List<String> topicLevels, V value) {
        this();
        if (topicLevels.size() == 1) {
            branches.put(topicLevels.get(0), new Branch<>(value));
        } else {
            INode<V> nin = new INode<>(
                new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value)));
            branches.put(topicLevels.get(0), new Branch<>(nin));
        }
    }

    CNode<V> inserted(List<String> topicLevels, V value) {
        Map<String, Branch<V>> newBranches = new HashMap<>(branches);
        if (topicLevels.size() == 1) {
            newBranches.put(topicLevels.get(0), new Branch<>(value));
        } else {
            INode<V> nin = new INode<>(new MainNode<>(new CNode<>(topicLevels.subList(1, topicLevels.size()), value)));
            newBranches.put(topicLevels.get(0), new Branch<>(nin));
        }
        return new CNode<>(newBranches);
    }

    // updatedBranch returns a copy of this C-node with the specified branch updated.
    CNode<V> updatedBranch(String topicLevel, INode<V> iNode, Branch<V> br) {
        Map<String, Branch<V>> newBranches = new HashMap<>(branches);
        newBranches.put(topicLevel, br.updated(iNode));
        return new CNode<>(newBranches);
    }

    CNode<V> updated(String topicLevel, V value) {
        Map<String, Branch<V>> newBranches = new HashMap<>(branches);
        Branch<V> br = newBranches.get(topicLevel);
        if (br != null) {
            newBranches.put(topicLevel, br.updated(value));
        } else {
            newBranches.put(topicLevel, new Branch<>(value));
        }
        return new CNode<>(newBranches);
    }

    CNode<V> removed(String topicLevel, V value) {
        Map<String, Branch<V>> newBranches = new HashMap<>(branches);
        Branch<V> br = newBranches.get(topicLevel);
        if (br != null) {
            Branch<V> updatedBranch = br.removed(value);
            if (updatedBranch.values.isEmpty() && updatedBranch.iNode == null) {
                newBranches.remove(topicLevel);
            } else {
                newBranches.put(topicLevel, updatedBranch);
            }
        }
        return new CNode<>(newBranches);
    }
}
