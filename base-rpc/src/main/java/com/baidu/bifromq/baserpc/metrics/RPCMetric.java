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

package com.baidu.bifromq.baserpc.metrics;

import io.micrometer.core.instrument.Meter;

public enum RPCMetric {
    UnaryReqDepth("unary.depth.summary", Meter.Type.DISTRIBUTION_SUMMARY),
    UnaryReqSendCount("unary.req.send.count", Meter.Type.COUNTER),
    UnaryReqCompleteCount("unary.req.complete.count", Meter.Type.COUNTER),
    UnaryReqAbortCount("unary.req.abort.count", Meter.Type.COUNTER),
    UnaryReqDropCount("unary.req.drop.count", Meter.Type.COUNTER),
    UnaryReqLatency("unary.req.finish.time", Meter.Type.TIMER),

    UnaryReqReceivedCount("unary.req.recv.count", Meter.Type.COUNTER),
    UnaryReqFulfillCount("unary.req.fulfil.count", Meter.Type.COUNTER),
    UnaryReqFailCount("unary.req.fail.count", Meter.Type.COUNTER),
    UnaryReqProcessLatency("unary.req.process.time", Meter.Type.TIMER),

    ReqPipelineCreateCount("ppln.create.count", Meter.Type.COUNTER),
    ReqPipelineErrorCount("ppln.error.count", Meter.Type.COUNTER),
    ReqPipelineCompleteCount("ppln.complete.count", Meter.Type.COUNTER),
    ReqPipelineDepth("ppln.depth.summary", Meter.Type.DISTRIBUTION_SUMMARY),

    PipelineReqAcceptCount("ppln.req.enqueue.count", Meter.Type.COUNTER),
    PipelineReqSendCount("ppln.req.send.count", Meter.Type.COUNTER),
    PipelineReqCompleteCount("ppln.req.complete.count", Meter.Type.COUNTER),
    PipelineReqAbortCount("ppln.req.abort.count", Meter.Type.COUNTER),
    PipelineReqDropCount("ppln.req.drop.count", Meter.Type.COUNTER),
    PipelineReqQueueTime("ppln.req.queue.time", Meter.Type.TIMER),
    PipelineReqLatency("ppln.req.finish.time", Meter.Type.TIMER),

    PipelineReqReceivedCount("ppln.req.recv.count", Meter.Type.COUNTER),
    PipelineReqFulfillCount("ppln.req.fulfil.count", Meter.Type.COUNTER),
    PipelineReqFailCount("ppln.req.fail.count", Meter.Type.COUNTER),
    PipelineReqProcessTime("ppln.req.process.time", Meter.Type.TIMER),

    MsgStreamCreateCount("stream.create.count", Meter.Type.COUNTER),
    MsgStreamErrorCount("stream.error.count", Meter.Type.COUNTER),

    StreamAckAcceptCount("stream.ack.enqueue.count", Meter.Type.COUNTER),
    StreamAckSendCount("stream.ack.send.count", Meter.Type.COUNTER),
    StreamMsgReceiveCount("stream.msg.recv.count", Meter.Type.COUNTER),

    StreamMsgSendCount("stream.msg.send.count", Meter.Type.COUNTER),
    StreamAckReceiveCount("stream.ack.recv.count", Meter.Type.COUNTER);

    public final String metricName;
    public final Meter.Type meterType;

    RPCMetric(String metricName, Meter.Type meterType) {
        this.metricName = metricName;
        this.meterType = meterType;
    }
}
