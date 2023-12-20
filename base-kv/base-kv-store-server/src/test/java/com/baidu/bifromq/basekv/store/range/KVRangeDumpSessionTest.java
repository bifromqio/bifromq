/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.basekv.MockableTest;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.proto.KVRangeMessage;
import com.baidu.bifromq.basekv.proto.KVRangeSnapshot;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataReply;
import com.baidu.bifromq.basekv.proto.SaveSnapshotDataRequest;
import com.baidu.bifromq.basekv.proto.SnapshotSyncRequest;
import com.baidu.bifromq.basekv.store.api.IKVIterator;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.api.IKVReader;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.time.Duration;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KVRangeDumpSessionTest extends MockableTest {
    @Mock
    private IKVRange rangeAccessor;
    @Mock
    private IKVRangeMessenger messenger;
    @Mock
    private IKVRangeReader rangeCPReader;
    @Mock
    private IKVReader rangeCPDataReader;
    @Mock
    private IKVIterator rangeCPDataItr;

    @Mock
    private KVRangeDumpSession.DumpBytesRecorder dumpBytesRecorder;

    @Test
    public void dumpEmptySnapshot() {
        String peerStoreId = "follower";
        String sessionId = "session";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(KVRangeSnapshot.newBuilder()
                .setId(rangeId)
                .build())
            .build();
        when(rangeAccessor.id()).thenReturn(rangeId);
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofSeconds(5), 1024, dumpBytesRecorder);
        assertTrue(dumpSession.awaitDone().toCompletableFuture().isDone());
        ArgumentCaptor<KVRangeMessage> messageCap = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCap.capture());
        KVRangeMessage message = messageCap.getValue();
        assertEquals(message.getHostStoreId(), peerStoreId);
        assertEquals(message.getRangeId(), rangeId);
        assertEquals(message.getSaveSnapshotDataRequest().getSessionId(), sessionId);
        assertEquals(message.getSaveSnapshotDataRequest().getFlag(), SaveSnapshotDataRequest.Flag.End);
    }

    @Test
    public void dumpNonExistSnapshot() {
        String peerStoreId = "follower";
        String sessionId = "session";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId("checkpoint")
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(false);
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofSeconds(5), 1024, dumpBytesRecorder);
        assertTrue(dumpSession.awaitDone().toCompletableFuture().isDone());
        ArgumentCaptor<KVRangeMessage> messageCap = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCap.capture());
        KVRangeMessage message = messageCap.getValue();
        assertEquals(message.getHostStoreId(), peerStoreId);
        assertEquals(message.getRangeId(), rangeId);
        assertEquals(message.getSaveSnapshotDataRequest().getSessionId(), sessionId);
        assertEquals(message.getSaveSnapshotDataRequest().getFlag(), SaveSnapshotDataRequest.Flag.Error);
    }

    @Test
    public void noSessionFound() {
        sessionEndWithFlag(SaveSnapshotDataReply.Result.NoSessionFound);
    }

    @Test
    public void sessionEndAbnormally() {
        sessionEndWithFlag(SaveSnapshotDataReply.Result.Error);
    }

    @Test
    public void sessionEndNormally() {
        sessionEndWithFlag(SaveSnapshotDataReply.Result.OK);
    }

    private void sessionEndWithFlag(SaveSnapshotDataReply.Result flag) {
        String localStoreId = "leader";
        String peerStoreId = "follower";
        String sessionId = "session";
        String checkpointId = "checkpoint";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        PublishSubject<KVRangeMessage> incomingMsgs = PublishSubject.create();

        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(true);
        when(rangeAccessor.open(snapshot)).thenReturn(rangeCPReader);
        when(rangeCPReader.newDataReader()).thenReturn(rangeCPDataReader);
        when(rangeCPDataReader.iterator()).thenReturn(rangeCPDataItr);
        when(messenger.receive()).thenReturn(incomingMsgs);

        when(rangeCPDataItr.isValid()).thenReturn(true, false);
        when(rangeCPDataItr.key()).thenReturn(ByteString.copyFromUtf8("key"));
        when(rangeCPDataItr.value()).thenReturn(ByteString.copyFromUtf8("value"));
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofSeconds(5), 1024, dumpBytesRecorder);
        assertEquals(dumpSession.checkpointId(), checkpointId);
        verify(rangeCPDataItr).seekToFirst();
        verify(rangeCPDataItr).next();
        assertFalse(dumpSession.awaitDone().toCompletableFuture().isDone());
        ArgumentCaptor<KVRangeMessage> messageCap = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCap.capture());

        incomingMsgs.onNext(KVRangeMessage.newBuilder()
            .setHostStoreId(localStoreId)
            .setRangeId(rangeId)
            .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                .setReqId(messageCap.getValue().getSaveSnapshotDataRequest().getReqId())
                .setSessionId(sessionId)
                .setResult(flag)
                .build())
            .build());
        verify(dumpBytesRecorder).record(anyInt());
        assertTrue(dumpSession.awaitDone().toCompletableFuture().isDone());
    }

    @Test
    public void rateLimit() {
        String peerStoreId = "follower";
        String sessionId = "session";
        String checkpointId = "checkpoint";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        PublishSubject<KVRangeMessage> incomingMsgs = PublishSubject.create();

        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(true);
        when(rangeAccessor.open(snapshot)).thenReturn(rangeCPReader);
        when(rangeCPReader.newDataReader()).thenReturn(rangeCPDataReader);
        when(rangeCPDataReader.iterator()).thenReturn(rangeCPDataItr);

        when(messenger.receive()).thenReturn(incomingMsgs);

        when(rangeCPDataItr.isValid()).thenReturn(true);
        when(rangeCPDataItr.key()).thenReturn(ByteString.copyFromUtf8("key"));
        when(rangeCPDataItr.value()).thenReturn(ByteString.copyFromUtf8("value"));
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofMillis(100), 5, dumpBytesRecorder);
        ArgumentCaptor<KVRangeMessage> messageCap = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger, times(1)).send(messageCap.capture());
        assertEquals(messageCap.getValue().getSaveSnapshotDataRequest().getFlag(), SaveSnapshotDataRequest.Flag.More);
    }

    @SneakyThrows
    @Test
    public void resend() {
        String peerStoreId = "follower";
        String sessionId = "session";
        String checkpointId = "checkpoint";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        PublishSubject<KVRangeMessage> incomingMsgs = PublishSubject.create();

        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(true);
        when(rangeAccessor.open(snapshot)).thenReturn(rangeCPReader);
        when(rangeCPReader.newDataReader()).thenReturn(rangeCPDataReader);
        when(rangeCPDataReader.iterator()).thenReturn(rangeCPDataItr);

        when(messenger.receive()).thenReturn(incomingMsgs);

        when(rangeCPDataItr.isValid()).thenReturn(true, false);
        when(rangeCPDataItr.key()).thenReturn(ByteString.copyFromUtf8("key"));
        when(rangeCPDataItr.value()).thenReturn(ByteString.copyFromUtf8("value"));
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofMillis(100), 1024, dumpBytesRecorder);
        Thread.sleep(60);
        dumpSession.tick();
        ArgumentCaptor<KVRangeMessage> messageCap = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger, times(2)).send(messageCap.capture());
        assertEquals(messageCap.getAllValues().get(1), messageCap.getAllValues().get(0));
    }

    @SneakyThrows
    @Test
    public void idle() {
        String peerStoreId = "follower";
        String sessionId = "session";
        String checkpointId = "checkpoint";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        PublishSubject<KVRangeMessage> incomingMsgs = PublishSubject.create();

        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(true);
        when(rangeAccessor.open(snapshot)).thenReturn(rangeCPReader);
        when(rangeCPReader.newDataReader()).thenReturn(rangeCPDataReader);
        when(rangeCPDataReader.iterator()).thenReturn(rangeCPDataItr);

        when(messenger.receive()).thenReturn(incomingMsgs);

        when(rangeCPDataItr.isValid()).thenReturn(true, false);
        when(rangeCPDataItr.key()).thenReturn(ByteString.copyFromUtf8("key"));
        when(rangeCPDataItr.value()).thenReturn(ByteString.copyFromUtf8("value"));
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofMillis(10), 1024, dumpBytesRecorder);
        Thread.sleep(20);
        dumpSession.tick();
        verify(messenger, times(1)).send(any());
        assertTrue(dumpSession.awaitDone().toCompletableFuture().isDone());
    }

    @Test
    public void cancel() {
        String peerStoreId = "follower";
        String sessionId = "session";
        String checkpointId = "checkpoint";
        KVRangeId rangeId = KVRangeIdUtil.generate();
        KVRangeSnapshot snapshot = KVRangeSnapshot.newBuilder()
            .setId(rangeId)
            .setCheckpointId(checkpointId)
            .build();
        SnapshotSyncRequest request = SnapshotSyncRequest.newBuilder()
            .setSessionId(sessionId)
            .setSnapshot(snapshot)
            .build();
        PublishSubject<KVRangeMessage> incomingMsgs = PublishSubject.create();

        when(rangeAccessor.id()).thenReturn(rangeId);
        when(rangeAccessor.hasCheckpoint(snapshot)).thenReturn(true);
        when(rangeAccessor.open(snapshot)).thenReturn(rangeCPReader);
        when(rangeCPReader.newDataReader()).thenReturn(rangeCPDataReader);
        when(rangeCPDataReader.iterator()).thenReturn(rangeCPDataItr);

        when(messenger.receive()).thenReturn(incomingMsgs);

        when(rangeCPDataItr.isValid()).thenReturn(true, false);
        when(rangeCPDataItr.key()).thenReturn(ByteString.copyFromUtf8("key"));
        when(rangeCPDataItr.value()).thenReturn(ByteString.copyFromUtf8("value"));
        KVRangeDumpSession dumpSession = new KVRangeDumpSession(peerStoreId, request, rangeAccessor, messenger,
            MoreExecutors.directExecutor(), Duration.ofMillis(10), 1024, dumpBytesRecorder);
        assertFalse(dumpSession.awaitDone().toCompletableFuture().isDone());
        dumpSession.cancel();
        verify(messenger, times(1)).send(any());
        assertTrue(dumpSession.awaitDone().toCompletableFuture().isDone());
    }
}
