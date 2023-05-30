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

package com.baidu.bifromq.basecrdt.core.util;

import com.baidu.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.Marker;

public class Log {
    public interface Stringify {
        String stringify();
    }

    public static void error(Logger log, String format, Object... args) {
        if (log.isErrorEnabled()) {
            log.error(format, stringify(args));
        }
    }

    public static void error(Logger log, Marker marker, String format, Object... args) {
        if (log.isErrorEnabled()) {
            log.error(format, marker, stringify(args));
        }
    }

    public static void warn(Logger log, String format, Object... args) {
        if (log.isWarnEnabled()) {
            log.warn(format, stringify(args));
        }
    }

    public static void warn(Logger log, Marker marker, String format, Object... args) {
        if (log.isWarnEnabled()) {
            log.warn(format, marker, stringify(args));
        }
    }

    public static void info(Logger log, String format, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(format, stringify(args));
        }
    }

    public static void info(Logger log, Marker marker, String format, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(format, marker, stringify(args));
        }
    }

    public static void debug(Logger log, String format, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(format, stringify(args));
        }
    }

    public static void debug(Logger log, Marker marker, String format, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(marker, format, stringify(args));
        }
    }

    public static void trace(Logger log, String format, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(format, stringify(args));
        }
    }

    public static void trace(Logger log, Marker marker, String format, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(marker, format, stringify(args));
        }
    }

    private static Object[] stringify(Object... args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = toString(args[i]);
        }
        return args;
    }

    private static Object toString(Object object) {
        if (object instanceof ByteString) {
            return toBase64((ByteString) object);
        }
        if (object instanceof Replica) {
            return "id=" + toBase64(((Replica) object).getId()) + ",uri=" + ((Replica) object).getUri();
        }
        if (object instanceof Stringify) {
            return ((Stringify) object).stringify();
        }
        if (object instanceof MessageOrBuilder) {
            try {
                return JsonFormat.printer().print((MessageOrBuilder) object);
            } catch (Exception e) {
                // ignore
            }
        }
        if (object instanceof Throwable) {
            return object;
        }
        return object.toString();
    }

    private static String toBase64(ByteString bs) {
        return Base64.getEncoder().encodeToString(bs.toByteArray());
    }
}
