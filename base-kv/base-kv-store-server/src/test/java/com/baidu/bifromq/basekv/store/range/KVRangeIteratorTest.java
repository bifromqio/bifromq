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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.baidu.bifromq.basekv.localengine.IKVEngineIterator;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeIteratorTest {
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

    @Test
    public void close() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(engineIterator.key()).thenReturn(KVRangeKeys.dataKey(userKey));
        AtomicBoolean closed = new AtomicBoolean();
        try (IKVIterator itr = new KVRangeIterator(() -> engineIterator, () -> closed.set(true))) {
            assertEquals(itr.key(), userKey);
        } catch (Exception e) {
            fail();
        }
        assertTrue(closed.get());
    }

    @Test
    public void key() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        when(engineIterator.key()).thenReturn(KVRangeKeys.dataKey(userKey));
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        assertEquals(itr.key(), userKey);
    }

    @Test
    public void value() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.value();
        verify(engineIterator).value();
    }

    @Test
    public void isValid() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.isValid();
        verify(engineIterator).isValid();
    }

    @Test
    public void next() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.next();
        verify(engineIterator).next();
    }

    @Test
    public void prev() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.prev();
        verify(engineIterator).prev();
    }

    @Test
    public void seekToFirst() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.seekToFirst();
        verify(engineIterator).seekToFirst();
    }

    @Test
    public void seekToLast() {
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.seekToLast();
        verify(engineIterator).seekToLast();
    }

    @Test
    public void seek() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.seek(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(engineIterator).seek(captor.capture());
        assertEquals(captor.getValue(), KVRangeKeys.dataKey(userKey));
    }

    @Test
    public void seekForPrev() {
        ByteString userKey = ByteString.copyFromUtf8("key");
        IKVIterator itr = new KVRangeIterator(() -> engineIterator);
        itr.seekForPrev(userKey);
        ArgumentCaptor<ByteString> captor = ArgumentCaptor.forClass(ByteString.class);
        verify(engineIterator).seekForPrev(captor.capture());
        assertEquals(captor.getValue(), KVRangeKeys.dataKey(userKey));
    }

}
