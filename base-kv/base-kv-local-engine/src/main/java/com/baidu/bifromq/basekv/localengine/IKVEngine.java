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

import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

public interface IKVEngine {
    String DEFAULT_NS = "default";

    /**
     * The unique identifier of the engine
     *
     * @return
     */
    String id();

    /**
     * Register a key range of particular namespace for accessing KV within it. It is allowed to register ranges with
     * boundary overlapping at the same time. The range is used for internal statistics and bookkeeping at runtime, and
     * it will not be persisted.
     *
     * @param namespace
     * @param start     the left end of the range, null for left open end
     * @param end       the right end of the range, null for right open end
     * @return unique
     */
    int registerKeyRange(String namespace, ByteString start, ByteString end);

    /**
     * Unregister a range
     *
     * @param rangeId
     */
    void unregisterKeyRange(int rangeId);

    ByteString skip(String namespace, long count);

    ByteString skip(int rangeId, long count);

    long size(String namespace);

    long size(String namespace, ByteString start, ByteString end);

    long size(int rangeId);

    long size(String checkpointId, String namespace);

    long size(String checkpointId, String namespace, ByteString start, ByteString end);

    long size(String checkpointId, int rangeId);

    String checkpoint();

    void checkpoint(String checkpointId);

    boolean hasCheckpoint(String checkpointId);

    boolean exist(String namespace, ByteString key);

    boolean exist(int rangeId, ByteString key);

    boolean exist(String checkpointId, int rangeId, ByteString key);

    boolean exist(String checkpointId, String namespace, ByteString key);

    Optional<ByteString> get(String namespace, ByteString key);

    Optional<ByteString> get(int rangeId, ByteString key);

    Optional<ByteString> get(String checkpointId, int rangeId, ByteString key);

    Optional<ByteString> get(String checkpointId, String namespace, ByteString key);

    IKVEngineIterator newIterator(String namespace);

    IKVEngineIterator newIterator(String namespace, ByteString start, ByteString end);

    /**
     * Create a iterator for the range
     *
     * @param rangeId
     * @return
     */
    IKVEngineIterator newIterator(int rangeId);

    /**
     * Create a sub-range iterator
     *
     * @param rangeId the id of the range
     * @param start   the left end of the range, null for left open end
     * @param end     the right end of the range, null for right open end
     * @return
     */
    IKVEngineIterator newIterator(int rangeId, ByteString start, ByteString end);

    IKVEngineIterator newIterator(String checkpointId, String namespace);

    IKVEngineIterator newIterator(String checkpointId, int rangeId);

    IKVEngineIterator newIterator(String checkpointId, int rangeId, ByteString start, ByteString end);

    /**
     * Start a batch operation
     *
     * @return
     */
    int startBatch();

    /**
     * End the batch operation
     *
     * @param batchId
     */
    void endBatch(int batchId);

    /**
     * Abort the batch operation
     *
     * @param batchId
     */
    void abortBatch(int batchId);

    /**
     * Number of operations contained in the batch
     *
     * @param batchId
     * @return
     */
    int batchSize(int batchId);

    /**
     * Delete the key. The key must be within the range.
     *
     * @param batchId
     * @param rangeId
     * @param key
     */
    void delete(int batchId, int rangeId, ByteString key);

    void delete(int batchId, String namespace, ByteString key);

    void delete(int rangeId, ByteString key);

    void delete(String namespace, ByteString key);

    void clearRange(String namespace);

    /**
     * Delete all keys in the range
     *
     * @param rangeId
     */
    void clearRange(int rangeId);

    void clearRange(int batchId, int rangeId);

    void clearRange(int batchId, String namespace);

    /**
     * Delete all keys in sub-range
     *
     * @param batchId
     * @param rangeId
     * @param start
     * @param end
     */
    void clearSubRange(int batchId, int rangeId, ByteString start, ByteString end);

    void clearSubRange(int batchId, String namespace, ByteString start, ByteString end);

    /**
     * Delete all keys in sub-range
     *
     * @param rangeId
     * @param start
     * @param end
     */
    void clearSubRange(int rangeId, ByteString start, ByteString end);

    void clearSubRange(String namespace, ByteString start, ByteString end);

    /**
     * Insert a key-value, the key must be within the range and do not exist otherwise it will not be deleted correctly
     * later. An assertion of non-existent is made using java language-level assert statement, which causing a little
     * more cpu overhead and can be controlled using -ea jvm options.
     *
     * @param batchId
     * @param rangeId
     * @param key
     * @param value
     */
    void insert(int batchId, int rangeId, ByteString key, ByteString value);

    void insert(int batchId, String namespace, ByteString key, ByteString value);

    void insert(int rangeId, ByteString key, ByteString value);

    void insert(String namespace, ByteString key, ByteString value);

    /**
     * Put a key-value. Unlike Insert, the key could be overwritten with new value.
     *
     * @param batchId
     * @param rangeId
     * @param key
     * @param value
     */
    void put(int batchId, int rangeId, ByteString key, ByteString value);

    void put(int batchId, String namespace, ByteString key, ByteString value);

    void put(int rangeId, ByteString key, ByteString value);

    void put(String namespace, ByteString key, ByteString value);

    void flush();

    void start(ScheduledExecutorService bgTaskExecutor, String... metricTags);

    void stop();

}
