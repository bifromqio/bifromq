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

package com.baidu.bifromq.basekv.store.range;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.proto.State;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.store.exception.KVRangeException;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeQueryRunnerTest {
    @Mock
    private IKVRangeState accessor;
    @Mock
    private IKVRangeReader rangeReader;
    @Mock
    private IKVReader kvReader;
    @Mock
    private IKVRangeQueryLinearizer linearizer;
    @Mock
    private IKVRangeCoProc coProc;
    private AutoCloseable closeable;

    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void badVersionQuery() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.ver()).thenReturn(1L);

        CompletableFuture<ROCoProcOutput> queryFuture = runner.queryCoProc(0, ROCoProcInput.newBuilder()
            .setRaw(ByteString.copyFromUtf8("key")).build(), false);
        verify(accessor).returnBorrowed(rangeReader);
        try {
            queryFuture.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof KVRangeException.BadVersion);
        }
    }

    @Test
    public void internalErrorByMergedState() {
        internalErrorByWrongState(State.StateType.Merged);
    }

    @Test
    public void internalErrorByRemovedState() {
        internalErrorByWrongState(State.StateType.Removed);
    }

    @Test
    public void internalErrorByPurgedState() {
        internalErrorByWrongState(State.StateType.Purged);
    }

    private void internalErrorByWrongState(State.StateType stateType) {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(stateType).build());

        CompletableFuture<ROCoProcOutput> queryFuture =
            runner.queryCoProc(0, ROCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("key")).build(), false);
        verify(accessor).returnBorrowed(rangeReader);
        try {
            queryFuture.join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof KVRangeException.TryLater);
        }
    }

    @Test
    public void get() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReader.ver()).thenReturn(0L);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(State.StateType.Normal).build());
        when(kvReader.get(any(ByteString.class))).thenReturn(Optional.empty());
        CompletionStage<Optional<ByteString>> queryFuture = runner.get(0, ByteString.copyFromUtf8("key"), false);
        verify(accessor).returnBorrowed(rangeReader);
        try {
            Optional<ByteString> result = queryFuture.toCompletableFuture().join();
            assertFalse(result.isPresent());
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void exist() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReader.ver()).thenReturn(0L);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(State.StateType.Normal).build());
        when(kvReader.exist(any(ByteString.class))).thenReturn(false);
        CompletionStage<Boolean> queryFuture = runner.exist(0, ByteString.copyFromUtf8("key"), false);
        verify(accessor).returnBorrowed(rangeReader);
        try {
            assertFalse(queryFuture.toCompletableFuture().join());
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void roCoProc() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        ROCoProcInput key = ROCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("key")).build();
        ROCoProcOutput value = ROCoProcOutput.newBuilder().setRaw(ByteString.copyFromUtf8("value")).build();
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReader.ver()).thenReturn(0L);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(State.StateType.Normal).build());
        when(coProc.query(any(ROCoProcInput.class), any(IKVReader.class)))
            .thenReturn(CompletableFuture.completedFuture(value));
        CompletableFuture<ROCoProcOutput> queryFuture = runner.queryCoProc(0, key, false);
        verify(accessor).returnBorrowed(rangeReader);
        ArgumentCaptor<ROCoProcInput> inputCap = ArgumentCaptor.forClass(ROCoProcInput.class);
        ArgumentCaptor<IKVReader> kvReaderCap = ArgumentCaptor.forClass(IKVReader.class);
        verify(coProc).query(inputCap.capture(), kvReaderCap.capture());
        assertEquals(inputCap.getValue(), key);
        assertEquals(kvReaderCap.getValue(), kvReader);
        try {
            assertEquals(queryFuture.join(), value);
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void linearizedRoCoProc() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        ROCoProcInput key = ROCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("key")).build();
        ROCoProcOutput value = ROCoProcOutput.newBuilder().setRaw(ByteString.copyFromUtf8("value")).build();
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReader.ver()).thenReturn(0L);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(State.StateType.Normal).build());
        when(coProc.query(any(ROCoProcInput.class), any(IKVReader.class)))
            .thenReturn(CompletableFuture.completedFuture(value));
        when(linearizer.linearize()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<ROCoProcOutput> queryFuture = runner.queryCoProc(0, key, true);
        verify(accessor).returnBorrowed(rangeReader);
        ArgumentCaptor<ROCoProcInput> inputCap = ArgumentCaptor.forClass(ROCoProcInput.class);
        ArgumentCaptor<IKVReader> kvReaderCap = ArgumentCaptor.forClass(IKVReader.class);
        verify(coProc).query(inputCap.capture(), kvReaderCap.capture());
        assertEquals(inputCap.getValue(), key);
        assertEquals(kvReaderCap.getValue(), kvReader);
        try {
            assertEquals(queryFuture.join(), value);
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void close() {
        KVRangeQueryRunner runner = new KVRangeQueryRunner(accessor, coProc, directExecutor(), linearizer);
        ROCoProcInput key = ROCoProcInput.newBuilder().setRaw(ByteString.copyFromUtf8("key")).build();
        when(accessor.borrow()).thenReturn(rangeReader);
        when(rangeReader.kvReader()).thenReturn(kvReader);
        when(rangeReader.ver()).thenReturn(0L);
        when(rangeReader.state()).thenReturn(State.newBuilder().setType(State.StateType.Normal).build());

        when(linearizer.linearize()).thenReturn(new CompletableFuture<>());
        when(coProc.query(any(ROCoProcInput.class), any(IKVReader.class))).thenReturn(new CompletableFuture<>());

        CompletableFuture<ROCoProcOutput> queryFuture = runner.queryCoProc(0, key, false);
        CompletableFuture<ROCoProcOutput> linearizedQueryFuture = runner.queryCoProc(0, key, true);

        runner.close();

        assertTrue(queryFuture.isCancelled());
        assertTrue(linearizedQueryFuture.isCancelled());
        assertTrue(runner.queryCoProc(0, key, false).isCancelled());
    }
}
