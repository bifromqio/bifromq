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

package com.baidu.bifromq.dist.server;

import static com.baidu.bifromq.baserpc.UnaryResponse.response;
import static com.baidu.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basescheduler.ICallScheduler;
import com.baidu.bifromq.dist.rpc.proto.DistReply;
import com.baidu.bifromq.dist.rpc.proto.DistRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.dist.rpc.proto.SubReply;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.UnsubReply;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.dist.server.scheduler.DistCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.DistWorkerCall;
import com.baidu.bifromq.dist.server.scheduler.IDistCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.IGlobalDistCallRateSchedulerFactory;
import com.baidu.bifromq.dist.server.scheduler.ISubCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.IUnsubCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.SubCallScheduler;
import com.baidu.bifromq.dist.server.scheduler.UnsubCallScheduler;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.eventcollector.distservice.SubscribeError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Subscribed;
import com.baidu.bifromq.plugin.eventcollector.distservice.UnsubscribeError;
import com.baidu.bifromq.plugin.eventcollector.distservice.Unsubscribed;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistService extends DistServiceGrpc.DistServiceImplBase {
    private final IEventCollector eventCollector;
    private final ICallScheduler<DistWorkerCall> distCallRateScheduler;
    private final IDistCallScheduler distCallScheduler;
    private final ISubCallScheduler subCallScheduler;
    private final IUnsubCallScheduler unsubCallScheduler;
    private final LoadingCache<String, RunningAverage> tenantFanouts;

    DistService(IBaseKVStoreClient distWorkerClient,
                ISettingProvider settingProvider,
                IEventCollector eventCollector,
                ICRDTService crdtService,
                IGlobalDistCallRateSchedulerFactory distCallRateScheduler) {
        this.eventCollector = eventCollector;
        this.distCallRateScheduler = distCallRateScheduler.createScheduler(settingProvider, crdtService);
        this.distCallScheduler = new DistCallScheduler(this.distCallRateScheduler, distWorkerClient);
        this.subCallScheduler = new SubCallScheduler(distWorkerClient);
        this.unsubCallScheduler = new UnsubCallScheduler(distWorkerClient);
        tenantFanouts = Caffeine.newBuilder()
            .expireAfterAccess(120, TimeUnit.SECONDS)
            .build(k -> new RunningAverage(5));
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        response(tenantId -> subCallScheduler.schedule(request)
            .handle((v, e) -> {
                if (e != null) {
                    log.error("Failed to exec SubRequest, tenantId={}, req={}", tenantId, request, e);
                    eventCollector.report(getLocal(SubscribeError.class)
                        .reqId(request.getReqId())
                        .qos(request.getSubQoS())
                        .topicFilter(request.getTopicFilter())
                        .tenantId(request.getTenantId())
                        .inboxId(request.getInboxId())
                        .subBrokerId(request.getBroker())
                        .delivererKey(request.getDelivererKey()));
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(SubReply.Result.ERROR)
                        .build();

                }
                eventCollector.report(getLocal(Subscribed.class)
                    .reqId(request.getReqId())
                    .qos(request.getSubQoS())
                    .topicFilter(request.getTopicFilter())
                    .tenantId(request.getTenantId())
                    .inboxId(request.getInboxId())
                    .subBrokerId(request.getBroker())
                    .delivererKey(request.getDelivererKey()));
                return v;
            }), responseObserver);
    }

    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        response(tenantId -> unsubCallScheduler.schedule(request)
            .handle((v, e) -> {
                if (e != null) {
                    log.error("Failed to exec UnsubRequest, tenantId={}, req={}", tenantId, request, e);
                    eventCollector.report(getLocal(UnsubscribeError.class)
                        .reqId(request.getReqId())
                        .topicFilter(request.getTopicFilter())
                        .tenantId(request.getTenantId())
                        .inboxId(request.getInboxId())
                        .subBrokerId(request.getBroker())
                        .delivererKey(request.getDelivererKey()));
                    return UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(UnsubReply.Result.ERROR)
                        .build();
                }
                eventCollector.report(getLocal(Unsubscribed.class)
                    .reqId(request.getReqId())
                    .topicFilter(request.getTopicFilter())
                    .tenantId(request.getTenantId())
                    .inboxId(request.getInboxId())
                    .subBrokerId(request.getBroker())
                    .delivererKey(request.getDelivererKey()));
                return v;
            }), responseObserver);
    }

    @Override
    public StreamObserver<DistRequest> dist(StreamObserver<DistReply> responseObserver) {
        return new DistResponsePipeline(distCallScheduler, responseObserver, eventCollector, tenantFanouts);
    }

    public void stop() {
        log.debug("stop dist call scheduler");
        distCallScheduler.close();
        log.debug("stop dist call rate limiter");
        distCallRateScheduler.close();
    }
}
