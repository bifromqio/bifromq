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

package com.baidu.bifromq.inbox.server;

import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedInboxId;
import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;
import static com.baidu.bifromq.inbox.util.KeyUtil.tenantPrefix;
import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWReply;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.rpc.proto.CreateInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.DeleteInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.ExpireInboxReply.Result;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxReply;
import com.baidu.bifromq.inbox.rpc.proto.SubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchCreateReply;
import com.baidu.bifromq.inbox.storage.proto.BatchSubReply;
import com.baidu.bifromq.inbox.storage.proto.BatchTouchReply;
import com.baidu.bifromq.inbox.storage.proto.BatchUnsubReply;
import com.baidu.bifromq.inbox.storage.proto.GCReply;
import com.baidu.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import com.baidu.bifromq.inbox.storage.proto.TopicFilterList;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MockedInboxAdminTest extends MockedInboxService {
    @Test
    public void testHasInboxWithNoInbox() {
        Map<String, Boolean> existMap = new HashMap<>();
        existMap.put(scopedInboxIdUtf8, false);
        mockInboxStoreLinearizedQuery(ReplyCode.Ok, generateBatchCheckROCoProcResult(existMap));
        HasInboxReply.Builder builder = HasInboxReply.newBuilder();
        StreamObserver<HasInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(HasInboxReply hasInboxReply) {
                builder.setResult(hasInboxReply.getResult());
            }
        };
        inboxService.hasInbox(hasRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), HasInboxReply.Result.NO_INBOX);
    }

    @Test
    public void testHasInboxWithOK() {
        Map<String, Boolean> existMap = new HashMap<>();
        existMap.put(scopedInboxIdUtf8, true);
        mockInboxStoreLinearizedQuery(ReplyCode.Ok, generateBatchCheckROCoProcResult(existMap));
        HasInboxReply.Builder builder = HasInboxReply.newBuilder();
        StreamObserver<HasInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(HasInboxReply hasInboxReply) {
                builder.setResult(hasInboxReply.getResult());
            }
        };
        inboxService.hasInbox(hasRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), HasInboxReply.Result.EXIST);
    }

    @Test
    public void testHasInboxWithException() {
        Map<String, Boolean> existMap = new HashMap<>();
        existMap.put(scopedInboxIdUtf8, true);
        mockInboxStoreLinearizedQuery(ReplyCode.BadRequest, generateBatchCheckROCoProcResult(existMap));
        AtomicBoolean exceptionOccur = new AtomicBoolean();
        StreamObserver<HasInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onError(Throwable throwable) {
                exceptionOccur.set(true);
            }
        };
        inboxService.hasInbox(hasRequest, responseObserver);
        Assert.assertTrue(exceptionOccur.get());
    }

    @Test
    public void testHasInboxWithNonExist() {
        mockInboxStoreLinearizedQuery(ReplyCode.Ok, generateBatchCheckROCoProcResult(new HashMap<>()));
        AtomicBoolean exceptionOccur = new AtomicBoolean();
        StreamObserver<HasInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onError(Throwable throwable) {
                exceptionOccur.set(true);
            }
        };
        inboxService.hasInbox(hasRequest, responseObserver);
        Assert.assertTrue(exceptionOccur.get());
    }

    @Test
    public void testCreateInboxWithErrorCode() {
        mockExecutePipeline(ReplyCode.BadVersion,
            generateRWCoProcResult(BatchCreateReply.newBuilder().putAllSubs(new HashMap<>()).build()));
        CreateInboxReply.Builder builder = CreateInboxReply.newBuilder();
        StreamObserver<CreateInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CreateInboxReply createInboxReply) {
                builder.setResult(createInboxReply.getResult());
            }
        };
        inboxService.createInbox(createRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CreateInboxReply.Result.ERROR);
    }

    @Test
    public void testCreateInboxWithEmptyTopicFilter() {
        Map<String, TopicFilterList> allSubs = new HashMap<>();
        allSubs.put(scopedInboxIdUtf8, TopicFilterList.newBuilder()
            .addAllTopicFilters(emptyList())
            .build());
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchCreateReply.newBuilder().putAllSubs(allSubs).build()));
        CreateInboxReply.Builder builder = CreateInboxReply.newBuilder();
        StreamObserver<CreateInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CreateInboxReply createInboxReply) {
                builder.setResult(createInboxReply.getResult());
            }
        };
        inboxService.createInbox(createRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CreateInboxReply.Result.OK);
    }

    @Test
    public void testCreateInboxWithUnMatchError() {
        Map<String, TopicFilterList> allSubs = new HashMap<>();
        allSubs.put(scopedInboxIdUtf8, TopicFilterList.newBuilder()
            .addAllTopicFilters(topicFilters)
            .build());
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchCreateReply.newBuilder().putAllSubs(allSubs).build()));
        mockDistUnMatch(UnmatchResult.ERROR);
        CreateInboxReply.Builder builder = CreateInboxReply.newBuilder();
        StreamObserver<CreateInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CreateInboxReply createInboxReply) {
                builder.setResult(createInboxReply.getResult());
            }
        };
        inboxService.createInbox(createRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CreateInboxReply.Result.ERROR);
    }

    @Test
    public void testCreateInboxWithUnMatchOK() {
        Map<String, TopicFilterList> allSubs = new HashMap<>();
        allSubs.put(scopedInboxIdUtf8, TopicFilterList.newBuilder()
            .addAllTopicFilters(topicFilters)
            .build());
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchCreateReply.newBuilder().putAllSubs(allSubs).build()));
        mockDistUnMatch(UnmatchResult.OK);
        CreateInboxReply.Builder builder = CreateInboxReply.newBuilder();
        StreamObserver<CreateInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(CreateInboxReply createInboxReply) {
                builder.setResult(createInboxReply.getResult());
            }
        };
        inboxService.createInbox(createRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), CreateInboxReply.Result.OK);
    }

    @Test
    public void testDeleteInboxWithErrorCode() {
        mockExecutePipeline(ReplyCode.BadVersion,
            generateRWCoProcResult(BatchTouchReply.newBuilder().build()));
        DeleteInboxReply.Builder builder = DeleteInboxReply.newBuilder();
        StreamObserver<DeleteInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(DeleteInboxReply deleteInboxReply) {
                builder.setResult(deleteInboxReply.getResult());
            }
        };
        inboxService.deleteInbox(deleteRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), DeleteInboxReply.Result.ERROR);
    }

    @Test
    public void testDeleteInbox() {
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchTouchReply.newBuilder().build()));
        DeleteInboxReply.Builder builder = DeleteInboxReply.newBuilder();
        StreamObserver<DeleteInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(DeleteInboxReply deleteInboxReply) {
                builder.setResult(deleteInboxReply.getResult());
            }
        };
        inboxService.deleteInbox(deleteRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), DeleteInboxReply.Result.OK);
    }

    @Test
    public void testSubInboxWithMatchErrorAndExceedLimit() {
        List<SubReply> replies = new ArrayList<>();
        StreamObserver<SubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(SubReply subReply) {
                replies.add(subReply);
            }
        };
        mockDistMatch(MatchResult.ERROR);
        inboxService.sub(subRequest, responseObserver);

        mockDistMatch(MatchResult.EXCEED_LIMIT);
        inboxService.sub(subRequest, responseObserver);
        Assert.assertEquals(replies.get(0).getResult(), SubReply.Result.ERROR);
        Assert.assertEquals(replies.get(1).getResult(), SubReply.Result.EXCEED_LIMIT);
    }

    @Test
    public void testSubInboxWithErrorCode() {
        mockExecutePipeline(ReplyCode.BadRequest,
            generateRWCoProcResult(BatchSubReply.newBuilder().putAllResults(new HashMap<>()).build()));
        mockDistMatch(MatchResult.OK);
        SubReply.Builder builder = SubReply.newBuilder();
        StreamObserver<SubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(SubReply subReply) {
                builder.setResult(subReply.getResult());
            }
        };
        inboxService.sub(subRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), SubReply.Result.ERROR);
    }

    @Test
    public void testSubInboxWithOK() {
        Map<String, BatchSubReply.Result> results = new HashMap<>() {{
            put(scopedTopicFilter(scopedInboxId, topicFilters.get(0)).toStringUtf8(), BatchSubReply.Result.OK);
        }};
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchSubReply.newBuilder().putAllResults(new HashMap<>(results)).build()));
        mockDistMatch(MatchResult.OK);
        SubReply.Builder builder = SubReply.newBuilder();
        StreamObserver<SubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(SubReply subReply) {
                builder.setResult(subReply.getResult());
            }
        };
        inboxService.sub(subRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), SubReply.Result.OK);
    }

    @Test
    public void testUnsubInboxWithUnMatchError() {
        mockDistUnMatch(UnmatchResult.ERROR);
        UnsubReply.Builder builder = UnsubReply.newBuilder();
        StreamObserver<UnsubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(UnsubReply unsubReply) {
                builder.setResult(unsubReply.getResult());
            }
        };
        inboxService.unsub(unsubRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), UnsubReply.Result.ERROR);
    }

    @Test
    public void testUnsubInboxWithErrorCode() {
        mockDistUnMatch(UnmatchResult.OK);
        mockExecutePipeline(ReplyCode.BadRequest,
            generateRWCoProcResult(BatchUnsubReply.newBuilder().putAllResults(new HashMap<>()).build()));
        UnsubReply.Builder builder = UnsubReply.newBuilder();
        StreamObserver<UnsubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(UnsubReply unsubReply) {
                builder.setResult(unsubReply.getResult());
            }
        };
        inboxService.unsub(unsubRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), UnsubReply.Result.ERROR);
    }

    @Test
    public void testUnsubInboxWithOK() {
        Map<String, BatchUnsubReply.Result> results = new HashMap<>() {{
            put(scopedTopicFilter(scopedInboxId, topicFilters.get(0)).toStringUtf8(), BatchUnsubReply.Result.OK);
        }};
        mockDistUnMatch(UnmatchResult.OK);
        mockExecutePipeline(ReplyCode.Ok,
            generateRWCoProcResult(BatchUnsubReply.newBuilder().putAllResults(results).build()));
        UnsubReply.Builder builder = UnsubReply.newBuilder();
        StreamObserver<UnsubReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(UnsubReply unsubReply) {
                builder.setResult(unsubReply.getResult());
            }
        };
        inboxService.unsub(unsubRequest, responseObserver);
        Assert.assertEquals(builder.getResult(), UnsubReply.Result.OK);
    }

    @Test
    public void testExpireInbox() throws InterruptedException {
        mockInboxServiceGCProc(3);
        AtomicReference<ExpireInboxReply> result = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<ExpireInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(ExpireInboxReply reply) {
                result.set(reply);
                countDownLatch.countDown();
            }
        };
        inboxService.expireInbox(expireInboxRequest, responseObserver);
        countDownLatch.await();
        Assert.assertEquals(result.get().getResult(), Result.OK);
    }

    @Test
    public void testExpireInboxWithNoRange() throws InterruptedException {
        mockFindBoundary(tenantId, 0);
        AtomicReference<ExpireInboxReply> result = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<ExpireInboxReply> responseObserver = new TestingStreamObserver<>() {
            @Override
            public void onNext(ExpireInboxReply reply) {
                result.set(reply);
                countDownLatch.countDown();
            }
        };
        inboxService.expireInbox(expireInboxRequest, responseObserver);
        countDownLatch.await();
        Assert.assertEquals(result.get().getResult(), Result.OK);
    }

    private void mockInboxServiceGCProc(int rangeCount) {
        List<KVRangeSetting> rangeSettings = mockFindBoundary(tenantId, rangeCount);
        List<CompletableFuture<KVRangeRWReply>> touchResults = new ArrayList<>();
        for (int i = 0; i < rangeSettings.size(); i++) {
            KVRangeSetting setting = rangeSettings.get(i);
            ByteString scopedInboxId = scopedInboxId(tenantId, inboxId + i);
            KVRangeROReply scanReply = KVRangeROReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(ROCoProcOutput.newBuilder()
                    .setInboxService(InboxServiceROCoProcOutput.newBuilder()
                        .setGc(GCReply.newBuilder()
                            .addScopedInboxId(scopedInboxId)
                            .build())
                        .build())
                    .build())
                .build();
            mockScanRange(setting, scanReply);
            BatchTouchReply batchTouchReply = BatchTouchReply.newBuilder()
                .addScopedInboxId(scopedInboxId.toStringUtf8())
                .build();
            touchResults.add(CompletableFuture.completedFuture(KVRangeRWReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .setRwCoProcResult(generateRWCoProcResult(batchTouchReply))
                .build()));
        }
        OngoingStubbing<CompletableFuture<KVRangeRWReply>> stubbing = when(mutPipeline.execute(any()));
        for (int i = 0; i < rangeSettings.size(); i++) {
            stubbing = stubbing.thenReturn(touchResults.get(i));
        }
        mockDistUnMatch(UnmatchResult.OK);
    }

    private List<KVRangeSetting> mockFindBoundary(String tenantId, int count) {
        List<KVRangeSetting> ranges = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ranges.add(
                new KVRangeSetting(clusterId, "store_" + i,
                    KVRangeDescriptor.newBuilder()
                        .setId(KVRangeIdUtil.generate())
                        .build())
            );
        }
        ByteString tenantPrefix = tenantPrefix(tenantId);
        when(inboxStoreClient.findByBoundary(eq(
            Boundary.newBuilder().setStartKey(tenantPrefix).setEndKey(upperBound(tenantPrefix)).build())))
            .thenReturn(ranges);
        return ranges;
    }
}
