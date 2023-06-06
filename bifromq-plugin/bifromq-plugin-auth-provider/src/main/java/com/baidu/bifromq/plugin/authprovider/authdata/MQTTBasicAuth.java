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

package com.baidu.bifromq.plugin.authprovider.authdata;

import com.baidu.bifromq.plugin.authprovider.AuthData;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public final class MQTTBasicAuth extends AuthData<MQTTBasicAuth> {
    private MQTTVer protocol;
    private String username;
    private ByteBuffer password;
    private ByteBuffer cert;
    private InetSocketAddress remoteAddr;
    private String channelId;

    @Override
    public AuthDataType type() {
        return AuthDataType.MQTT3BasicAuth;
    }

    @Override
    public void clone(MQTTBasicAuth orig) {
        protocol = orig.protocol;
        username = orig.username;
        password = orig.password;
        cert = orig.cert;
        remoteAddr = orig.remoteAddr;
        channelId = orig.channelId;
    }

    public MQTTVer protocol() {
        return protocol;
    }

    public Optional<String> username() {
        return Optional.ofNullable(username);
    }

    public Optional<ByteBuffer> password() {
        return Optional.ofNullable(password);
    }

    public Optional<ByteBuffer> cert() {
        return Optional.ofNullable(cert);
    }

    public InetSocketAddress remoteAddr() {
        return remoteAddr;
    }

    public String channelId() {
        return channelId;
    }
}
