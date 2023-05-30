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

package com.baidu.bifromq.basekv.raft;

import java.util.Set;

public class ClusterConfigHelper {
    public static <T> boolean isIntersect(Set<T> s1, Set<T> s2) {
        Set<T> small = s1.size() > s2.size() ? s2 : s1;
        Set<T> large = s1.size() > s2.size() ? s1 : s2;
        for (T item : small) {
            if (large.contains(item)) {
                return true;
            }
        }
        return false;
    }
}
