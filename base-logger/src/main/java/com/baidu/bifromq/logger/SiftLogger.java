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

package com.baidu.bifromq.logger;

import org.slf4j.Logger;
import org.slf4j.MDC;
import org.slf4j.Marker;

public class SiftLogger extends FormatableLogger {
    public static final String SIFT_KEY = "sift_key";

    public static Logger getLogger(String siftKey, Class<?> clazz) {
        return new SiftLogger(siftKey, clazz);
    }

    public static Logger getLogger(String siftKey, String name) {
        return new SiftLogger(siftKey, name);
    }

    private final String siftKey;

    protected SiftLogger(String siftKey, Class<?> clazz) {
        super(clazz);
        this.siftKey = siftKey;
    }

    protected SiftLogger(String siftKey, String name) {
        super(name);
        this.siftKey = siftKey;
    }

    protected void doLog(LogMsg logFunc, String msg) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, msg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogFormatAndArg logFunc, String format, Object arg) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, format, arg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogFormatAndArg1Arg2 logFunc, String format, Object arg1, Object arg2) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, format, arg1, arg2);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogVarArgs(LogFormatAndVarArgs logFunc, String format, Object... arguments) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLogVarArgs(logFunc, format, arguments);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogThrowable(LogMsgAndThrowable logFunc, String msg, Throwable t) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLogThrowable(logFunc, msg, t);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerMsg logFunc, Marker marker, String msg) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, marker, msg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerFormatAndArg logFunc, Marker marker, String format, Object arg) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, marker, format, arg);
        MDC.remove(SIFT_KEY);
    }

    protected void doLog(LogMarkerFormatAndArg1Arg2 logFunc, Marker marker, String format, Object arg1, Object arg2) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLog(logFunc, marker, format, arg1, arg2);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogVarArgs(LogMarkerFormatAndVarArgs logFunc, Marker marker, String format, Object... arguments) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLogVarArgs(logFunc, marker, format, arguments);
        MDC.remove(SIFT_KEY);
    }

    protected void doLogThrowable(LogMarkerMsgAndThrowable logFunc, Marker marker, String msg, Throwable t) {
        MDC.put(SIFT_KEY, siftKey);
        super.doLogThrowable(logFunc, marker, msg, t);
        MDC.remove(SIFT_KEY);
    }
}
