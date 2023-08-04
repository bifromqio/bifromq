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

import static com.baidu.bifromq.basekv.store.range.TrackableKVOperation.KEY_ITR_GET;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeIteratorTest {
    @Mock
    private ILoadTracker loadTracker;
    @Mock
    private IKVEngineIterator engineIterator;
    private AutoCloseable closeable;

    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @SneakyThrows
    @Test
    public void close() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(engineIterator.key()).thenReturn(KVRangeKeys.dataKey(userKey));
        AtomicBoolean closed = new AtomicBoolean();
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator, () -> closed.set(true));
        itr.close();
        assertTrue(closed.get());
    }

    @Test
    public void key() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(engineIterator.isValid()).thenReturn(true);
        when(engineIterator.key()).thenReturn(KVRangeKeys.dataKey(userKey));
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seekToFirst();
        assertEquals(itr.key(), userKey);
        verify(loadTracker).track(userKey, KEY_ITR_GET);
    }

    @Test
    public void value() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(engineIterator.isValid()).thenReturn(true);
        when(engineIterator.key()).thenReturn(KVRangeKeys.dataKey(userKey));
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seekToFirst();
        itr.value();
        verify(engineIterator).value();
    }

    @Test
    public void isValid() {
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.isValid();
        verify(engineIterator).isValid();
    }

    @Test
    public void next() {
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.next();
        verify(engineIterator).next();
        verify(engineIterator).isValid();
    }

    @Test
    public void prev() {
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.prev();
        verify(engineIterator).prev();
        verify(engineIterator).isValid();
    }

    @Test
    public void seekToFirst() {
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seekToFirst();
        verify(engineIterator).seekToFirst();
        verify(engineIterator).isValid();
    }

    @Test
    public void seekToLast() {
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seekToLast();
        verify(engineIterator).seekToLast();
        verify(engineIterator).isValid();
    }

    @Test
    public void seek() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seek(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(engineIterator).seek(captor.capture());
        assertEquals(captor.getValue(), KVRangeKeys.dataKey(userKey));
        verify(engineIterator).isValid();
    }

    @Test
    public void seekForPrev() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVRangeIterator(loadTracker, () -> engineIterator);
        itr.seekForPrev(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(engineIterator).seekForPrev(captor.capture());
        assertEquals(captor.getValue(), KVRangeKeys.dataKey(userKey));
        verify(engineIterator).isValid();
    }
}
