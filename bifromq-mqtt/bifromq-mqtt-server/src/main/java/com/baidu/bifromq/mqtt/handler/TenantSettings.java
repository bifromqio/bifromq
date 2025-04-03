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

package com.baidu.bifromq.mqtt.handler;

import static com.baidu.bifromq.plugin.settingprovider.Setting.DebugModeEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ForceTransient;
import static com.baidu.bifromq.plugin.settingprovider.Setting.InBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MQTT3Enabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MQTT4Enabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MQTT5Enabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxLastWillBytes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxResendTimes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxSessionExpirySeconds;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicAlias;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerInbox;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicFiltersPerSub;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevelLength;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxTopicLevels;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaxUserPayloadBytes;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MaximumQoS;
import static com.baidu.bifromq.plugin.settingprovider.Setting.MsgPubPerSec;
import static com.baidu.bifromq.plugin.settingprovider.Setting.OutBoundBandWidth;
import static com.baidu.bifromq.plugin.settingprovider.Setting.PayloadFormatValidationEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.QoS0DropOldest;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ReceivingMaximum;
import static com.baidu.bifromq.plugin.settingprovider.Setting.ResendTimeoutSeconds;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.RetainMessageMatchLimit;
import static com.baidu.bifromq.plugin.settingprovider.Setting.SessionInboxSize;
import static com.baidu.bifromq.plugin.settingprovider.Setting.SharedSubscriptionEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.SubscriptionIdentifierEnabled;
import static com.baidu.bifromq.plugin.settingprovider.Setting.WildcardSubscriptionEnabled;

import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.baidu.bifromq.type.QoS;

public class TenantSettings {
    public final boolean mqtt3Enabled;
    public final boolean mqtt4Enabled;
    public final boolean mqtt5Enabled;
    public final boolean debugMode;
    public final boolean forceTransient;
    public final boolean payloadFormatValidationEnabled;
    public final boolean retainEnabled;
    public final boolean wildcardSubscriptionEnabled;
    public final boolean subscriptionIdentifierEnabled;
    public final boolean sharedSubscriptionEnabled;
    public final QoS maxQoS;
    public final int maxSEI;
    public final int maxTopicLevelLength;
    public final int maxTopicLevels;
    public final int maxTopicLength;
    public final int maxPacketSize;
    public final int maxLastWillSize;
    public final int maxTopicAlias;
    public final long inboundBandwidth;
    public final long outboundBandwidth;
    public final int receiveMaximum;
    public final int maxMsgPerSec;
    public final int maxResendTimes;
    public final int resendTimeoutSeconds;
    public final int maxTopicFiltersPerSub;
    public final int inboxQueueLength;
    public final boolean inboxDropOldest;
    public final int retainMatchLimit;
    public final int maxTopicFiltersPerInbox;

    public TenantSettings(String tenantId, ISettingProvider provider) {
        mqtt3Enabled = provider.provide(MQTT3Enabled, tenantId);
        mqtt4Enabled = provider.provide(MQTT4Enabled, tenantId);
        mqtt5Enabled = provider.provide(MQTT5Enabled, tenantId);
        debugMode = provider.provide(DebugModeEnabled, tenantId);
        forceTransient = provider.provide(ForceTransient, tenantId);
        payloadFormatValidationEnabled = provider.provide(PayloadFormatValidationEnabled, tenantId);
        retainEnabled = provider.provide(RetainEnabled, tenantId);
        wildcardSubscriptionEnabled = provider.provide(WildcardSubscriptionEnabled, tenantId);
        subscriptionIdentifierEnabled = provider.provide(SubscriptionIdentifierEnabled, tenantId);
        sharedSubscriptionEnabled = provider.provide(SharedSubscriptionEnabled, tenantId);
        maxQoS = QoS.forNumber(provider.provide(MaximumQoS, tenantId));
        maxSEI = provider.provide(MaxSessionExpirySeconds, tenantId);
        maxTopicLevelLength = provider.provide(MaxTopicLevelLength, tenantId);
        maxTopicLevels = provider.provide(MaxTopicLevels, tenantId);
        maxTopicLength = provider.provide(MaxTopicLength, tenantId);
        maxPacketSize = provider.provide(MaxUserPayloadBytes, tenantId);
        maxLastWillSize = Math.min(provider.provide(MaxLastWillBytes, tenantId), maxPacketSize);
        maxTopicAlias = provider.provide(MaxTopicAlias, tenantId);
        inboundBandwidth = provider.provide(InBoundBandWidth, tenantId);
        outboundBandwidth = provider.provide(OutBoundBandWidth, tenantId);
        maxMsgPerSec = provider.provide(MsgPubPerSec, tenantId);
        maxResendTimes = provider.provide(MaxResendTimes, tenantId);
        resendTimeoutSeconds = provider.provide(ResendTimeoutSeconds, tenantId);
        receiveMaximum = provider.provide(ReceivingMaximum, tenantId);
        maxTopicFiltersPerSub = provider.provide(MaxTopicFiltersPerSub, tenantId);
        inboxQueueLength = provider.provide(SessionInboxSize, tenantId);
        inboxDropOldest = provider.provide(QoS0DropOldest, tenantId);
        retainMatchLimit = provider.provide(RetainMessageMatchLimit, tenantId);
        maxTopicFiltersPerInbox = provider.provide(MaxTopicFiltersPerInbox, tenantId);
    }
}
