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

package com.baidu.bifromq.basekv.balance;

import com.baidu.bifromq.basekv.balance.command.BalanceCommand;
import com.baidu.bifromq.basekv.balance.command.ChangeConfigCommand;
import com.baidu.bifromq.basekv.balance.command.MergeCommand;
import com.baidu.bifromq.basekv.balance.command.SplitCommand;
import com.baidu.bifromq.basekv.balance.command.TransferLeadershipCommand;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import com.baidu.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeMergeRequest;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeSplitRequest;
import com.baidu.bifromq.basekv.store.proto.RecoverRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipReply;
import com.baidu.bifromq.basekv.store.proto.TransferLeadershipRequest;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandRunner {

    private final Cache<KVRangeId, Long> historyCommandCache;
    private final IBaseKVStoreClient kvStoreClient;

    public enum Result {
        Succeed,
        Failed
    }

    public CommandRunner(IBaseKVStoreClient kvStoreClient) {
        this.kvStoreClient = kvStoreClient;
        this.historyCommandCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    }

    public CompletableFuture<Result> run(BalanceCommand command) {
        if (command.getExpectedVer() != null) {
            Long historyCommand = historyCommandCache.getIfPresent(command.getKvRangeId());
            if (historyCommand != null && historyCommand >= command.getExpectedVer()) {
                log.warn("[{}]Command version is duplicated with prev one: {}", kvStoreClient.clusterId(), command);
                return CompletableFuture.completedFuture(Result.Failed);
            }
        }
        log.debug("[{}]Send balanceCommand: {}", kvStoreClient.clusterId(), command);
        switch (command.type()) {
            case CHANGE_CONFIG:
                ChangeReplicaConfigRequest changeConfigRequest = ChangeReplicaConfigRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .addAllNewVoters(((ChangeConfigCommand) command).getVoters())
                    .addAllNewLearners(((ChangeConfigCommand) command).getLearners())
                    .build();
                return handleStoreReplyCode(command,
                    kvStoreClient.changeReplicaConfig(command.getToStore(), changeConfigRequest)
                        .thenApply(ChangeReplicaConfigReply::getCode)
                );
            case MERGE:
                KVRangeMergeRequest rangeMergeRequest = KVRangeMergeRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setVer(command.getExpectedVer())
                    .setMergerId(command.getKvRangeId())
                    .setMergeeId(((MergeCommand) command).getMergeeId())
                    .build();
                return handleStoreReplyCode(command,
                    kvStoreClient.mergeRanges(command.getToStore(), rangeMergeRequest)
                        .thenApply(KVRangeMergeReply::getCode));
            case SPLIT:
                KVRangeSplitRequest kvRangeSplitRequest = KVRangeSplitRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .setSplitKey(((SplitCommand) command).getSplitKey())
                    .build();
                return handleStoreReplyCode(command,
                    kvStoreClient.splitRange(command.getToStore(), kvRangeSplitRequest)
                        .thenApply(KVRangeSplitReply::getCode));
            case TRANSFER_LEADERSHIP:
                TransferLeadershipRequest transferLeadershipRequest = TransferLeadershipRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .setKvRangeId(command.getKvRangeId())
                    .setVer(command.getExpectedVer())
                    .setNewLeaderStore(((TransferLeadershipCommand) command).getNewLeaderStore())
                    .build();
                return handleStoreReplyCode(command,
                    kvStoreClient.transferLeadership(command.getToStore(), transferLeadershipRequest)
                        .thenApply(TransferLeadershipReply::getCode));
            case RECOVERY:
                RecoverRequest recoverRequest = RecoverRequest.newBuilder()
                    .setReqId(System.nanoTime())
                    .build();
                return kvStoreClient.recover(command.getToStore(), recoverRequest)
                    .handle((r, e) -> {
                        if (e != null) {
                            log.error("[{}]Unexpected error when recover, req: {}", kvStoreClient.clusterId(),
                                recoverRequest, e);
                        }
                        return Result.Succeed;
                    });
            default:
                return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Result> handleStoreReplyCode(BalanceCommand command,
                                                           CompletableFuture<ReplyCode> storeReply) {
        CompletableFuture<Result> onDone = new CompletableFuture<>();
        storeReply.whenComplete((code, e) -> {
            if (e != null) {
                log.error("[{}]Unexpected error when run command: {}", kvStoreClient.clusterId(), command, e);
                onDone.complete(Result.Failed);
                return;
            }
            switch (code) {
                case Ok -> {
                    if (command.getExpectedVer() != null) {
                        historyCommandCache.put(command.getKvRangeId(), command.getExpectedVer());
                    }
                    onDone.complete(Result.Succeed);
                }
                case BadRequest, BadVersion, TryLater, InternalError -> {
                    log.warn("[{}]Failed with reply: {}, command: {}", kvStoreClient.clusterId(), code, command);
                    onDone.complete(Result.Failed);
                }
                default -> onDone.complete(Result.Failed);
            }
        });
        return onDone;
    }
}
