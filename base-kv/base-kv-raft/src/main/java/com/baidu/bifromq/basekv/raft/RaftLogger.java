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

package com.baidu.bifromq.basekv.raft;

import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.logger.SiftLogger;
import org.slf4j.MDC;
import org.slf4j.Marker;

public class RaftLogger extends SiftLogger {
    private static final String MDC_KEY_ID = "id";
    private static final String MDC_KEY_STATE = "state";
    private static final String MDC_KEY_LEADER = "leader";
    private static final String MDC_KEY_TERM = "term";
    private static final String MDC_KEY_FIRST = "first";
    private static final String MDC_KEY_LAST = "last";
    private static final String MDC_KEY_COMMIT = "commit";
    private static final String MDC_KEY_CONFIG = "config";
    private final IRaftNodeState state;

    protected RaftLogger(IRaftNodeState state, String... tags) {
        super(buildSiftKey(tags), state.getClass());
        this.state = state;
    }

    @Override
    protected void doLog(LogMsg logFunc, String msg) {
        setupMDC();
        super.doLog(logFunc, msg);
        clearMDC();
    }

    @Override
    protected void doLog(LogFormatAndArg logFunc, String format, Object arg) {
        setupMDC();
        super.doLog(logFunc, format, arg);
        clearMDC();
    }

    @Override
    protected void doLog(LogFormatAndArg1Arg2 logFunc, String format, Object arg1, Object arg2) {
        setupMDC();
        super.doLog(logFunc, format, arg1, arg2);
        clearMDC();
    }

    @Override
    protected void doLogVarArgs(LogFormatAndVarArgs logFunc, String format, Object... arguments) {
        setupMDC();
        super.doLogVarArgs(logFunc, format, arguments);
        clearMDC();
    }

    @Override
    protected void doLogThrowable(LogMsgAndThrowable logFunc, String msg, Throwable t) {
        setupMDC();
        super.doLogThrowable(logFunc, msg, t);
        clearMDC();
    }

    @Override
    protected void doLog(LogMarkerMsg logFunc, Marker marker, String msg) {
        setupMDC();
        super.doLog(logFunc, marker, msg);
        clearMDC();
    }

    @Override
    protected void doLog(LogMarkerFormatAndArg logFunc, Marker marker, String format, Object arg) {
        setupMDC();
        super.doLog(logFunc, marker, format, arg);
        clearMDC();
    }

    @Override
    protected void doLog(LogMarkerFormatAndArg1Arg2 logFunc, Marker marker, String format, Object arg1, Object arg2) {
        setupMDC();
        super.doLog(logFunc, marker, format, arg1, arg2);
        clearMDC();
    }

    @Override
    protected void doLogVarArgs(LogMarkerFormatAndVarArgs logFunc, Marker marker, String format, Object... arguments) {
        setupMDC();
        super.doLogVarArgs(logFunc, marker, format, arguments);
        clearMDC();
    }

    @Override
    protected void doLogThrowable(LogMarkerMsgAndThrowable logFunc, Marker marker, String msg, Throwable t) {
        setupMDC();
        super.doLogThrowable(logFunc, marker, msg, t);
        clearMDC();
    }

    private void setupMDC() {
        MDC.put(MDC_KEY_ID, state.id());
        MDC.put(MDC_KEY_STATE, state.getState().name());
        MDC.put(MDC_KEY_LEADER, state.currentLeader());
        MDC.put(MDC_KEY_TERM, Long.toUnsignedString(state.currentTerm()));
        MDC.put(MDC_KEY_FIRST, Long.toUnsignedString(state.firstIndex()));
        MDC.put(MDC_KEY_LAST, Long.toUnsignedString(state.lastIndex()));
        MDC.put(MDC_KEY_COMMIT, Long.toUnsignedString(state.commitIndex()));
        MDC.put(MDC_KEY_CONFIG, printClusterConfig(state.latestClusterConfig()));
    }

    private void clearMDC() {
        MDC.remove(MDC_KEY_ID);
        MDC.remove(MDC_KEY_STATE);
        MDC.remove(MDC_KEY_LEADER);
        MDC.remove(MDC_KEY_TERM);
        MDC.remove(MDC_KEY_FIRST);
        MDC.remove(MDC_KEY_LAST);
        MDC.remove(MDC_KEY_COMMIT);
        MDC.remove(MDC_KEY_CONFIG);
    }

    private String printClusterConfig(ClusterConfig clusterConfig) {
        return String.format("[c:%s,v:%s,l:%s,nv:%s,nl:%s]",
            clusterConfig.getCorrelateId(),
            clusterConfig.getVotersList(),
            clusterConfig.getLearnersList(),
            clusterConfig.getNextVotersList(),
            clusterConfig.getNextLearnersList());
    }

    public static String buildSiftKey(String... tags) {
        StringBuilder logKey = new StringBuilder();
        for (int i = 0; i < tags.length; i += 2) {
            logKey.append(tags[i + 1]);
            if (i + 2 < tags.length) {
                logKey.append("-");
            }
        }
        return logKey.toString();
    }
}
