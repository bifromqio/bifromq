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

package com.baidu.bifromq.apiserver.http;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import java.lang.reflect.Method;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

public class AnnotationUtil {
    public static <T extends IHTTPRequestHandler> HttpMethod getHTTPMethod(Class<T> handlerClass) {
        for (Method handleMethod : handlerClass.getMethods()) {
            if (handleMethod.getAnnotation(GET.class) != null) {
                return HttpMethod.GET;
            } else if (handleMethod.getAnnotation(PUT.class) != null) {
                return HttpMethod.PUT;
            } else if (handleMethod.getAnnotation(POST.class) != null) {
                return HttpMethod.POST;
            } else if (handleMethod.getAnnotation(DELETE.class) != null) {
                return HttpMethod.DELETE;
            } else if (handleMethod.getAnnotation(OPTIONS.class) != null) {
                return HttpMethod.OPTIONS;
            } else if (handleMethod.getAnnotation(HEAD.class) != null) {
                return HttpMethod.HEAD;
            } else if (handleMethod.getAnnotation(PATCH.class) != null) {
                return HttpMethod.PATCH;
            }
        }
        return null;
    }

    public static <T extends IHTTPRequestHandler> Path getPath(Class<T> handlerClass) {
        return handlerClass.getAnnotation(Path.class);
    }
}
