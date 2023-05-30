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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KVRangeStateAccessorTest {

    @Mock
    private Runnable refresh;

    @Mock
    private Runnable mutate;

    @Test
    public void refreshAsNeeded() {
        KVRangeStateAccessor accessor = new KVRangeStateAccessor();
        KVRangeStateAccessor.KVRangeReaderRefresher refresher1 = accessor.refresher();
        KVRangeStateAccessor.KVRangeReaderRefresher refresher2 = accessor.refresher();
        KVRangeStateAccessor.KVRangeWriterMutator mutator = accessor.mutator();

        refresher1.run(refresh);
        refresher2.run(refresh);
        verify(refresh, never()).run();

        mutator.run(mutate);
        verify(mutate, times(1)).run();

        refresher1.run(refresh);
        refresher2.run(refresh);
        verify(refresh, times(2)).run();

        refresher1.run(refresh);
        refresher2.run(refresh);
        verify(refresh, times(2)).run();
    }
}
