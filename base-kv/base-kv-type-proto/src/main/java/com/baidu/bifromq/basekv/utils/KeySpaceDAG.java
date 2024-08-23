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

package com.baidu.bifromq.basekv.utils;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class KeySpaceDAG {
    private record Node(ByteString key) {
    }

    private record EdgeKey(String storeId, KVRangeId rangeId) {

    }

    private static final Node OPEN_START_KEY = new Node(null);
    private static final Node OPEN_END_KEY = new Node(null);

    private final NavigableMap<Node, Map<Node, SortedMap<EdgeKey, KVRangeDescriptor>>> graph =
        new TreeMap<>((node1, node2) -> BoundaryUtil.compareStartKey(node1.key, node2.key));


    public KeySpaceDAG(Map<String, Set<KVRangeDescriptor>> rangeDescriptorsByStoreId) {
        for (Map.Entry<String, Set<KVRangeDescriptor>> entry : rangeDescriptorsByStoreId.entrySet()) {
            String storeId = entry.getKey();
            Set<KVRangeDescriptor> rangeDescriptors = entry.getValue();
            for (KVRangeDescriptor rangeDescriptor : rangeDescriptors) {
                Boundary boundary = rangeDescriptor.getBoundary();
                Node start = boundary.hasStartKey() ? new Node(boundary.getStartKey()) : OPEN_START_KEY;
                Node end = boundary.hasEndKey() ? new Node(boundary.getEndKey()) : OPEN_END_KEY;
                graph.computeIfAbsent(start, k -> new HashMap<>())
                    .computeIfAbsent(end,
                        k -> new TreeMap<>(Comparator.comparingLong((EdgeKey id) -> id.rangeId.getId())
                            .thenComparing(id -> id.storeId)))
                    .put(new EdgeKey(storeId, rangeDescriptor.getId()), rangeDescriptor);
            }
        }
    }

    public record LeaderRange(String storeId, KVRangeDescriptor descriptor) {
    }

    public NavigableMap<Boundary, LeaderRange> getEffectiveFullCoveredRoute() {
        NavigableMap<Boundary, LeaderRange> effectiveRoute = new TreeMap<>(BoundaryUtil::compare);

        // DFS to find all paths from OPEN_START_KEY to OPEN_END_KEY
        dfs(OPEN_START_KEY, effectiveRoute);

        return effectiveRoute;
    }

    /**
     * The method to find gaps in the key space.
     * <br>
     * Note. the method only find gaps when there is no effective full covered route
     *
     * @return the list of gaps
     */
    public List<Boundary> findGaps() {
        List<Boundary> gaps = new ArrayList<>();
        // Start from OPEN_START_KEY and identify gaps
        identifyGaps(OPEN_START_KEY, gaps);
        return gaps;
    }

    private void identifyGaps(Node currentNode, List<Boundary> gaps) {
        // If we have reached OPEN_END_KEY, then no gap exists in this path
        if (currentNode == OPEN_END_KEY) {
            return;
        }

        Map<Node, SortedMap<EdgeKey, KVRangeDescriptor>> edgesFromCurrentNode = graph.get(currentNode);
        if (edgesFromCurrentNode == null || edgesFromCurrentNode.isEmpty()) {
            // If we can't proceed further, this is a gap
            Node nextNode = graph.ceilingKey(currentNode);
            Boundary gapBoundary = createBoundary(currentNode, nextNode == null ? OPEN_END_KEY : nextNode);
            gaps.add(gapBoundary);
            identifyGaps(nextNode, gaps);
            return;
        }

        for (Map.Entry<Node, SortedMap<EdgeKey, KVRangeDescriptor>> entry : edgesFromCurrentNode.entrySet()) {
            // Recursively continue to the next node
            identifyGaps(entry.getKey(), gaps);
        }
    }

    private boolean dfs(Node currentNode, NavigableMap<Boundary, LeaderRange> currentPath) {
        // If reached the OPEN_END_KEY, the currentPath is valid and complete
        if (currentNode == OPEN_END_KEY) {
            return true;
        }

        Map<Node, SortedMap<EdgeKey, KVRangeDescriptor>> edgesFromCurrentNode = graph.get(currentNode);
        if (edgesFromCurrentNode == null || edgesFromCurrentNode.isEmpty()) {
            return false; // No further paths from this node
        }

        for (Map.Entry<Node, SortedMap<EdgeKey, KVRangeDescriptor>> entry : edgesFromCurrentNode.entrySet()) {
            Node nextNode = entry.getKey();
            SortedMap<EdgeKey, KVRangeDescriptor> edgeMap = entry.getValue();

            // Choose the best edge according to the strategy:
            EdgeKey bestEdgeKey = edgeMap.firstKey();
            KVRangeDescriptor bestDescriptor = edgeMap.get(bestEdgeKey);

            // Define the boundary of this edge (from current node to next node)
            Boundary boundary = createBoundary(currentNode, nextNode);

            // Add this edge to the current path
            currentPath.put(boundary, new LeaderRange(bestEdgeKey.storeId(), bestDescriptor));

            // Recursively continue to the next node
            if (dfs(nextNode, currentPath)) {
                return true; // Found a valid path, return true
            }

            // Backtrack if this path doesn't lead to a valid route
            currentPath.remove(boundary);
        }
        return false; // No valid path found from this node
    }

    private Boundary createBoundary(Node startNode, Node endNode) {
        Boundary.Builder boundaryBuilder = Boundary.newBuilder();
        if (startNode.key != null) {
            boundaryBuilder.setStartKey(startNode.key);
        }
        if (endNode.key != null) {
            boundaryBuilder.setEndKey(endNode.key);
        }
        return boundaryBuilder.build();
    }

}
