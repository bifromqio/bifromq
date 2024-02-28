/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.demo.plugin;


import com.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.bifromq.plugin.resourcethrottler.TenantResourceType;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.pf4j.Extension;

@Extension
public class DemoResourceThrottler implements IResourceThrottler {
    private static final String PLUGIN_RESOURCE_THROTTLER_URL = "plugin.resourcethrottler.url";
    private final IResourceThrottler delegate;

    public DemoResourceThrottler() {
        IResourceThrottler delegate1;
        String webhookUrl = System.getProperty(PLUGIN_RESOURCE_THROTTLER_URL);
        if (webhookUrl == null) {
            delegate1 = (tenantId, type) -> true;
        } else {
            try {
                URI webhookURI = URI.create(webhookUrl);
                delegate1 = new WebHookBasedResourceThrottler(webhookURI);
            } catch (Throwable e) {
                delegate1 = (tenantId, type) -> true;
            }
        }
        delegate = delegate1;
    }

    @Override

    public boolean hasResource(String tenantId, TenantResourceType type) {
        return delegate.hasResource(tenantId, type);
    }
}
