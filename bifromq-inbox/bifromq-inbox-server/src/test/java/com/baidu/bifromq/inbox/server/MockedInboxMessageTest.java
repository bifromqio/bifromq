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

package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.inbox.rpc.proto.*;
import com.baidu.bifromq.inbox.storage.proto.*;
import com.baidu.bifromq.type.TopicMessage;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockedInboxMessageTest extends MockedInboxService {
    @Test
    public void testReceiveWithOK() {
        InsertResult insertResult = InsertResult.newBuilder()
                .setSubInfo(subInfo)
                .setResult(InsertResult.Result.OK)
                .build();
        mockExecutePipeline(ReplyCode.Ok,
                generateRWCoProcResult(BatchInsertReply.newBuilder()
                        .addAllResults(List.of(insertResult))
                        .build()));
        SendReply.Builder builder = SendReply.newBuilder();
        ServerCallStreamObserver<SendReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(SendReply sendReply) {
                builder.addAllResult(sendReply.getResultList());
            }
        };
        StreamObserver<SendRequest> requester = inboxService.receive(responseObserver);
        requester.onNext(sendRequest);
        Assert.assertEquals(builder.getResultList().size(), 1);
        Assert.assertEquals(builder.getResultList().get(0).getResult(), SendResult.Result.OK);
    }

    @Test
    public void testReceiveWithErrorCode() {
        mockExecutePipeline(ReplyCode.BadRequest,
                generateRWCoProcResult(BatchInsertReply.newBuilder()
                        .addAllResults(List.of(InsertResult.getDefaultInstance()))
                        .build()));
        SendReply.Builder builder = SendReply.newBuilder();
        ServerCallStreamObserver<SendReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(SendReply sendReply) {
                builder.addAllResult(sendReply.getResultList());
            }
        };
        StreamObserver<SendRequest> requester = inboxService.receive(responseObserver);
        requester.onNext(sendRequest);
        Assert.assertEquals(builder.getResultList().size(), 1);
        Assert.assertEquals(builder.getResultList().get(0).getResult(), SendResult.Result.ERROR);
    }

    @Test
    public void testFetchInboxWithOKThenClose() {
        InboxMessage inboxMessage = InboxMessage.newBuilder()
                .setTopicFilter(topicFilters.get(0))
                .setMsg(TopicMessage.getDefaultInstance())
                .build();
        Map<String, Fetched> fetchedMap = new HashMap<>();
        fetchedMap.put(scopedInboxIdUtf8, Fetched.newBuilder()
                        .setResult(Fetched.Result.OK)
                        .addAllQos0Seq(List.of(0l))
                        .addAllQos0Msg(List.of(inboxMessage))
                .build());
        mockInboxStoreQuery(ReplyCode.Ok, generateBatchFetchROCoProcResult(fetchedMap));
        InboxFetched.Builder builder = InboxFetched.newBuilder();
        ServerCallStreamObserver<InboxFetched> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(InboxFetched fetched) {
                builder.setFetched(fetched.getFetched());
            }
        };
        StreamObserver<InboxFetchHint> fetchRequester = inboxService.fetchInbox(responseObserver);
        fetchRequester.onNext(fetchHint);

        mockExecutePipeline(ReplyCode.Ok, generateRWCoProcResult(BatchTouchReply.getDefaultInstance()));
        fetchRequester.onCompleted();
        Assert.assertEquals(builder.getFetched(), fetchedMap.get(scopedInboxIdUtf8));
    }

    @Test
    public void testFetchInboxWithErrorCode() {
        mockInboxStoreQuery(ReplyCode.BadRequest, generateBatchFetchROCoProcResult(new HashMap<>()));
        InboxFetched.Builder builder = InboxFetched.newBuilder();
        ServerCallStreamObserver<InboxFetched> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(InboxFetched fetched) {
                builder.setFetched(fetched.getFetched());
            }
        };
        StreamObserver<InboxFetchHint> fetchRequester = inboxService.fetchInbox(responseObserver);
        fetchRequester.onNext(fetchHint);
        Assert.assertEquals(builder.getFetched().getResult(), Fetched.Result.ERROR);
    }

    @Test
    public void testFetchInboxWithSignalHasMore() {
        InboxMessage inboxMessage = InboxMessage.newBuilder()
                .setTopicFilter(topicFilters.get(0))
                .setMsg(TopicMessage.getDefaultInstance())
                .build();
        Map<String, Fetched> fetchedMap = new HashMap<>();
        fetchedMap.put(scopedInboxIdUtf8, Fetched.newBuilder()
                .setResult(Fetched.Result.OK)
                .addAllQos0Seq(List.of(0l))
                .addAllQos0Msg(List.of(inboxMessage))
                .build());
        mockInboxStoreQuery(ReplyCode.Ok, generateBatchFetchROCoProcResult(fetchedMap));
        mockInboxStoreLinearizedQuery(ReplyCode.Ok, generateBatchFetchROCoProcResult(fetchedMap));
        List<Fetched> fetchedList = new ArrayList<>();
        ServerCallStreamObserver<InboxFetched> fetchObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(InboxFetched fetched) {
                fetchedList.add(fetched.getFetched());
            }
        };
        StreamObserver<InboxFetchHint> fetchRequester = inboxService.fetchInbox(fetchObserver);
        fetchRequester.onNext(fetchHint);
        signalFetch();
        Assert.assertEquals(fetchedList.size(), 2);
        Assert.assertEquals(fetchedList.get(0), fetchedMap.get(scopedInboxIdUtf8));
        Assert.assertEquals(fetchedList.get(1), fetchedMap.get(scopedInboxIdUtf8));
    }

    @Test
    public void testFetchInboxWithRstHint() {
        List<InboxMessage> inboxMessages = List.of(InboxMessage.newBuilder()
                .setTopicFilter(topicFilters.get(0))
                .setMsg(TopicMessage.getDefaultInstance())
                .build());
        Map<String, Fetched> firstMap = new HashMap<>();
        Map<String, Fetched> secondMap = new HashMap<>();
        firstMap.put(scopedInboxIdUtf8, Fetched.newBuilder()
                .setResult(Fetched.Result.OK)
                .addAllQos0Seq(List.of(0l))
                .addAllQos0Msg(inboxMessages)
                .build());
        secondMap.put(scopedInboxIdUtf8, Fetched.newBuilder()
                .setResult(Fetched.Result.OK)
                .addAllQos0Seq(List.of(1l))
                .addAllQos0Msg(inboxMessages)
                .addAllQos1Seq(List.of(0l))
                .addAllQos1Msg(inboxMessages)
                .addAllQos2Seq(List.of(0l))
                .addAllQos2Msg(inboxMessages)
                .build());
        mockInboxStoreQuery(ReplyCode.Ok, generateBatchFetchROCoProcResult(firstMap));
        List<Fetched> fetchedList = new ArrayList<>();
        ServerCallStreamObserver<InboxFetched> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(InboxFetched fetched) {
                fetchedList.add(fetched.getFetched());
            }
        };
        StreamObserver<InboxFetchHint> fetchRequester = inboxService.fetchInbox(responseObserver);
        fetchRequester.onNext(fetchHint);
        mockInboxStoreQuery(ReplyCode.Ok, generateBatchFetchROCoProcResult(secondMap));
        InboxFetchHint rstHint = InboxFetchHint.newBuilder()
                .setIncarnation(System.nanoTime())
                .setCapacity(3)
                .setInboxId(inboxId)
                .build();
        fetchRequester.onNext(rstHint);
        Assert.assertEquals(fetchedList.get(0), firstMap.get(scopedInboxIdUtf8));
        Assert.assertEquals(fetchedList.get(1), secondMap.get(scopedInboxIdUtf8));
    }

    @Test
    public void testCommitInboxWithOK() {
        Map<String, Boolean> commitMap = new HashMap<>();
        commitMap.put(scopedInboxIdUtf8, true);
        mockExecutePipeline(ReplyCode.Ok, generateRWCoProcResult(BatchCommitReply.newBuilder()
                .putAllResult(commitMap).build()));
        CommitReply.Builder builder = CommitReply.newBuilder();
        StreamObserver<CommitReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CommitReply commitReply) {
                builder.setResult(commitReply.getResult());
            }
        };
        inboxService.commit(commitRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CommitReply.Result.OK);
    }

    @Test
    public void testCommitInboxWithErrorCode() {
        mockExecutePipeline(ReplyCode.BadRequest, generateRWCoProcResult(BatchCommitReply.newBuilder().build()));
        CommitReply.Builder builder = CommitReply.newBuilder();
        StreamObserver<CommitReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CommitReply commitReply) {
                builder.setResult(commitReply.getResult());
            }
        };
        inboxService.commit(commitRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CommitReply.Result.ERROR);
    }

    private void signalFetch() {
        InsertResult insertResult = InsertResult.newBuilder()
                .setSubInfo(subInfo)
                .setResult(InsertResult.Result.OK)
                .build();
        mockExecutePipeline(ReplyCode.Ok,
                generateRWCoProcResult(BatchInsertReply.newBuilder()
                        .addAllResults(List.of(insertResult))
                        .build()));
        ServerCallStreamObserver<SendReply> receiveObserver = new TestingStreamObserver<>();
        StreamObserver<SendRequest> receiveRequester = inboxService.receive(receiveObserver);
        receiveRequester.onNext(sendRequest);
    }
}
