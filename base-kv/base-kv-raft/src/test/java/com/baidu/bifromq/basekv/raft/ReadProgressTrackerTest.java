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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReadProgressTrackerTest {
    ReadProgressTracker readProgressTracker;
    @Mock
    IRaftStateStore stateStorage;
    @Mock
    IRaftNodeLogger logger;

    @Before
    public void setup() {
        readProgressTracker = new ReadProgressTracker(stateStorage, logger);
    }

    @Test
    public void testHighestReadIndexAfterInit() {
        assertEquals(0, readProgressTracker.highestReadIndex());
    }

    @Test
    public void testAdd() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone);
        assertEquals(1, readProgressTracker.underConfirming());
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone);
        assertEquals(2, readProgressTracker.underConfirming());
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone);
        assertEquals(3, readProgressTracker.underConfirming());
        assertFalse(onDone.isDone());
    }

    @Test
    public void testAbort() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Long> onDone1 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone1);

        CompletableFuture<Long> onDone2 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone2);

        CompletableFuture<Long> onDone3 = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone3);
        readProgressTracker.abort();
        assertEquals(0, readProgressTracker.underConfirming());
        assertTrue(onDone1.isCompletedExceptionally());
        assertTrue(onDone2.isCompletedExceptionally());
        assertTrue(onDone3.isCompletedExceptionally());
    }

    @Test
    public void testConfirm() {
        try {
            readProgressTracker.confirm(-1L, "FakePeer");
        } catch (Exception e) {
            fail();
        }
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Long> onDone1 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone1);

        CompletableFuture<Long> onDone2 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone2);

        CompletableFuture<Long> onDone3 = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone3);

        CompletableFuture<Long> onDone4 = new CompletableFuture<>();
        readProgressTracker.add(7L, onDone4);

        readProgressTracker.confirm(6L, "V1");
        assertFalse(onDone1.isDone());
        assertFalse(onDone2.isDone());
        assertFalse(onDone3.isDone());
        assertFalse(onDone4.isDone());
        readProgressTracker.confirm(6L, "V2");
        assertTrue(onDone1.isDone());
        assertTrue(onDone2.isDone());
        assertTrue(onDone3.isDone());
        assertFalse(onDone4.isDone());
        assertEquals(1, readProgressTracker.underConfirming());
        readProgressTracker.confirm(7L, "V2");
        assertFalse(onDone4.isDone());
        readProgressTracker.confirm(7L, "V3");
        assertTrue(onDone4.isDone());
        assertEquals(0, readProgressTracker.underConfirming());
    }
}
