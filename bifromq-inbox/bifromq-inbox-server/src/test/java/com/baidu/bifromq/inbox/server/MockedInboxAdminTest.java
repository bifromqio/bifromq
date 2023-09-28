package com.baidu.bifromq.inbox.server;

import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.dist.client.MatchResult;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.rpc.proto.*;
import com.baidu.bifromq.inbox.storage.proto.*;
import io.grpc.stub.StreamObserver;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.baidu.bifromq.inbox.util.KeyUtil.scopedTopicFilter;
import static java.util.Collections.emptyList;

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
        Map<String, TopicFilterList> allSubs  = new HashMap<>();
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
        Map<String, TopicFilterList> allSubs  = new HashMap<>();
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
        Map<String, TopicFilterList> allSubs  = new HashMap<>();
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
                generateRWCoProcResult(BatchTouchReply.newBuilder().putAllSubs(new HashMap<>()).build()));
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
    public void testDeleteInboxWithEmptyTopicFilter() {
        mockExecutePipeline(ReplyCode.Ok,
                generateRWCoProcResult(BatchTouchReply.newBuilder().putAllSubs(new HashMap<>()).build()));
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
    public void testDeleteInboxWithUnMatchError() {
        Map<String, TopicFilterList> allSubs  = new HashMap<>();
        allSubs.put(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                .addAllTopicFilters(topicFilters)
                .build());
        mockExecutePipeline(ReplyCode.Ok,
                generateRWCoProcResult(BatchTouchReply.newBuilder().putAllSubs(allSubs).build()));
        mockDistUnMatch(UnmatchResult.ERROR);
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
    public void testDeleteInboxWithUnMatchOK() {
        Map<String, TopicFilterList> allSubs  = new HashMap<>();
        allSubs.put(scopedInboxIdUtf8, TopicFilterList.newBuilder()
                .addAllTopicFilters(topicFilters)
                .build());
        mockExecutePipeline(ReplyCode.Ok,
                generateRWCoProcResult(BatchTouchReply.newBuilder().putAllSubs(allSubs).build()));
        mockDistUnMatch(UnmatchResult.OK);
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
}
