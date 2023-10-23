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

import static com.baidu.bifromq.logger.LogFormatter.STRINGIFIER_MAP;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class FormatableLogger implements Logger {
    static {
        Logger root = LoggerFactory.getLogger(ROOT_LOGGER_NAME);
        if (root instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) root).getLoggerContext()
                .getFrameworkPackages().add(FormatableLogger.class.getName());
            Reflections reflections = new Reflections("com.baidu.bifromq");
            for (Class<? extends FormatableLogger> subLoggerClass : reflections.getSubTypesOf(FormatableLogger.class)) {
                ((ch.qos.logback.classic.Logger) root).getLoggerContext()
                    .getFrameworkPackages().add(subLoggerClass.getName());
            }
        }
    }

    public static Logger getLogger(Class<?> clazz) {
        return new FormatableLogger(clazz);
    }

    public static Logger getLogger(String name) {
        return new FormatableLogger(name);
    }

    public interface Stringifiable {
        String stringify();
    }

    public interface Stringifier<T> {
        String stringify(T object);
    }

    protected interface LogMsg {
        void log(String msg);
    }

    protected interface LogFormatAndArg {
        void log(String format, Object arg);
    }

    protected interface LogFormatAndArg1Arg2 {
        void log(String format, Object arg1, Object arg2);
    }

    protected interface LogFormatAndVarArgs {
        void log(String format, Object... arguments);
    }

    protected interface LogMsgAndThrowable {
        void log(String msg, Throwable t);
    }

    protected interface LogMarkerMsg {
        void log(Marker marker, String msg);
    }

    protected interface LogMarkerFormatAndArg {
        void log(Marker marker, String format, Object arg);
    }

    protected interface LogMarkerFormatAndArg1Arg2 {
        void log(Marker marker, String format, Object arg1, Object arg2);
    }

    protected interface LogMarkerFormatAndVarArgs {
        void log(Marker marker, String format, Object... arguments);
    }

    protected interface LogMarkerMsgAndThrowable {
        void log(Marker marker, String msg, Throwable t);
    }

    private final Logger delegate;

    protected FormatableLogger(Class<?> clazz) {
        this(clazz.getName());
    }

    protected FormatableLogger(String name) {
        this.delegate = LoggerFactory.getLogger(name);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public final void trace(String msg) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, msg);
    }

    @Override
    public final void trace(String format, Object arg) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, format, arg);
    }

    @Override
    public final void trace(String format, Object arg1, Object arg2) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, format, arg1, arg2);
    }

    @Override
    public final void trace(String format, Object... arguments) {
        if (!isTraceEnabled()) {
            return;
        }
        doLogVarArgs(this::trace, format, arguments);
    }

    @Override
    public final void trace(String msg, Throwable t) {
        if (!isTraceEnabled()) {
            return;
        }
        doLogThrowable(delegate::trace, msg, t);
    }

    @Override
    public final boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    @Override
    public final void trace(Marker marker, String msg) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, marker, msg);
    }

    @Override
    public final void trace(Marker marker, String format, Object arg) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, marker, format, arg);
    }

    @Override
    public final void trace(Marker marker, String format, Object arg1, Object arg2) {
        if (!isTraceEnabled()) {
            return;
        }
        doLog(delegate::trace, marker, format, arg1, arg2);
    }

    @Override
    public final void trace(Marker marker, String format, Object... argArray) {
        if (!isTraceEnabled()) {
            return;
        }
        doLogVarArgs(this::trace, marker, format, argArray);
    }

    @Override
    public final void trace(Marker marker, String msg, Throwable t) {
        if (!isTraceEnabled()) {
            return;
        }
        doLogThrowable(delegate::trace, marker, msg, t);
    }

    @Override
    public final boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public final void debug(String msg) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, msg);
    }

    @Override
    public final void debug(String format, Object arg) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, format, arg);
    }

    @Override
    public final void debug(String format, Object arg1, Object arg2) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, format, arg1, arg2);
    }

    @Override
    public final void debug(String format, Object... arguments) {
        if (!isDebugEnabled()) {
            return;
        }
        doLogVarArgs(delegate::debug, format, arguments);
    }

    @Override
    public final void debug(String msg, Throwable t) {
        if (!isDebugEnabled()) {
            return;
        }
        doLogThrowable(delegate::debug, msg, t);
    }

    @Override
    public final boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    @Override
    public final void debug(Marker marker, String msg) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, marker, msg);
    }

    @Override
    public final void debug(Marker marker, String format, Object arg) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, marker, format, arg);
    }

    @Override
    public final void debug(Marker marker, String format, Object arg1, Object arg2) {
        if (!isDebugEnabled()) {
            return;
        }
        doLog(delegate::debug, marker, format, arg1, arg2);
    }

    @Override
    public final void debug(Marker marker, String format, Object... arguments) {
        if (!isDebugEnabled()) {
            return;
        }
        doLogVarArgs(delegate::debug, marker, format, arguments);
    }

    @Override
    public final void debug(Marker marker, String msg, Throwable t) {
        if (!isDebugEnabled()) {
            return;
        }
        doLogThrowable(delegate::debug, marker, msg, t);
    }

    @Override
    public final boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public final void info(String msg) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, msg);
    }

    @Override
    public final void info(String format, Object arg) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, format, arg);
    }

    @Override
    public final void info(String format, Object arg1, Object arg2) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, format, arg1, arg2);
    }

    @Override
    public final void info(String format, Object... arguments) {
        if (!isInfoEnabled()) {
            return;
        }
        doLogVarArgs(delegate::info, format, arguments);
    }

    @Override
    public final void info(String msg, Throwable t) {
        if (!isInfoEnabled()) {
            return;
        }
        doLogThrowable(delegate::info, msg, t);
    }

    @Override
    public final boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    @Override
    public final void info(Marker marker, String msg) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, marker, msg);
    }

    @Override
    public final void info(Marker marker, String format, Object arg) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, marker, format, arg);
    }

    @Override
    public final void info(Marker marker, String format, Object arg1, Object arg2) {
        if (!isInfoEnabled()) {
            return;
        }
        doLog(delegate::info, marker, format, arg1, arg2);
    }

    @Override
    public final void info(Marker marker, String format, Object... arguments) {
        if (!isInfoEnabled()) {
            return;
        }
        doLogVarArgs(delegate::info, marker, format, arguments);
    }

    @Override
    public final void info(Marker marker, String msg, Throwable t) {
        if (!isInfoEnabled()) {
            return;
        }
        doLogThrowable(delegate::info, msg, t);
    }

    @Override
    public final boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public final void warn(String msg) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, msg);
    }

    @Override
    public final void warn(String format, Object arg) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, format, arg);
    }

    @Override
    public final void warn(String format, Object... arguments) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, format, arguments);
    }

    @Override
    public final void warn(String format, Object arg1, Object arg2) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, format, arg1, arg2);
    }

    @Override
    public final void warn(String msg, Throwable t) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, msg, t);
    }

    @Override
    public final boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    @Override
    public final void warn(Marker marker, String msg) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, marker, msg);
    }

    @Override
    public final void warn(Marker marker, String format, Object arg) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, marker, format, arg);
    }

    @Override
    public final void warn(Marker marker, String format, Object arg1, Object arg2) {
        if (!isWarnEnabled()) {
            return;
        }
        doLog(delegate::warn, marker, format, arg1, arg2);
    }

    @Override
    public final void warn(Marker marker, String format, Object... arguments) {
        if (!isWarnEnabled()) {
            return;
        }
        doLogVarArgs(delegate::warn, marker, format, arguments);
    }

    @Override
    public final void warn(Marker marker, String msg, Throwable t) {
        if (!isWarnEnabled()) {
            return;
        }
        doLogThrowable(delegate::warn, marker, msg, t);
    }

    @Override
    public final boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public final void error(String msg) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, msg);
    }

    @Override
    public final void error(String format, Object arg) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, format, arg);
    }

    @Override
    public final void error(String format, Object arg1, Object arg2) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, format, arg1, arg2);
    }

    @Override
    public final void error(String format, Object... arguments) {
        if (!isErrorEnabled()) {
            return;
        }
        doLogVarArgs(delegate::error, format, arguments);
    }

    @Override
    public final void error(String msg, Throwable t) {
        if (!isErrorEnabled()) {
            return;
        }
        doLogThrowable(delegate::error, msg, t);
    }

    @Override
    public final boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(marker);
    }

    @Override
    public final void error(Marker marker, String msg) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, marker, msg);
    }

    @Override
    public final void error(Marker marker, String format, Object arg) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, marker, format, arg);
    }

    @Override
    public final void error(Marker marker, String format, Object arg1, Object arg2) {
        if (!isErrorEnabled()) {
            return;
        }
        doLog(delegate::error, marker, format, arg1, arg2);
    }

    @Override
    public final void error(Marker marker, String format, Object... arguments) {
        if (!isErrorEnabled()) {
            return;
        }
        doLogVarArgs(delegate::error, format, arguments);
    }

    @Override
    public final void error(Marker marker, String msg, Throwable t) {
        if (!isErrorEnabled()) {
            return;
        }
        doLogThrowable(delegate::error, marker, msg, t);
    }

    protected void doLog(LogMsg logFunc, String msg) {
        logFunc.log(msg);
    }

    protected void doLog(LogFormatAndArg logFunc, String format, Object arg) {
        logFunc.log(format, stringify(arg));
    }

    protected void doLog(LogFormatAndArg1Arg2 logFunc, String format, Object arg1, Object arg2) {
        logFunc.log(format, stringify(arg1), stringify(arg2));
    }

    protected void doLogVarArgs(LogFormatAndVarArgs logFunc, String format, Object... arguments) {
        logFunc.log(format, stringify(arguments));
    }

    protected void doLogThrowable(LogMsgAndThrowable logFunc, String msg, Throwable t) {
        logFunc.log(msg, t);
    }

    protected void doLog(LogMarkerMsg logFunc, Marker marker, String msg) {
        logFunc.log(marker, msg);
    }

    protected void doLog(LogMarkerFormatAndArg logFunc, Marker marker, String format, Object arg) {
        logFunc.log(marker, format, stringify(arg));
    }

    protected void doLog(LogMarkerFormatAndArg1Arg2 logFunc, Marker marker, String format, Object arg1, Object arg2) {
        logFunc.log(marker, format, stringify(arg1), stringify(arg2));
    }

    protected void doLogVarArgs(LogMarkerFormatAndVarArgs logFunc, Marker marker, String format, Object... arguments) {
        logFunc.log(marker, format, stringify(arguments));
    }

    protected void doLogThrowable(LogMarkerMsgAndThrowable logFunc, Marker marker, String msg, Throwable t) {
        logFunc.log(marker, msg, t);
    }

    private Object[] stringify(Object... arguments) {
        Object[] strings = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            Object obj = arguments[i];
            strings[i] = stringify(obj);
        }
        return strings;
    }

    @SuppressWarnings("unchecked")
    private <T> String stringify(T obj) {
        if (obj instanceof Stringifiable) {
            return ((Stringifiable) obj).stringify();
        } else {
            Stringifier<T> stringifier =
                (Stringifier<T>) STRINGIFIER_MAP.getOrDefault(obj.getClass(), Object::toString);
            return stringifier.stringify(obj);
        }
    }
}
