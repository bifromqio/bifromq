package ${groupId};

import com.baidu.bifromq.plugin.authprovider.IAuthProvider;
import com.baidu.bifromq.plugin.authprovider.type.CheckResult;
import com.baidu.bifromq.plugin.authprovider.type.Error;
import com.baidu.bifromq.plugin.authprovider.type.Failed;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import com.baidu.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import com.baidu.bifromq.plugin.authprovider.type.MQTTAction;
import com.baidu.bifromq.plugin.authprovider.type.Reject;
import com.baidu.bifromq.type.ClientInfo;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Extension
public class ${pluginName}AuthProvider implements IAuthProvider {
    private static final Logger log = LoggerFactory.getLogger(${pluginName}AuthProvider.class);

    public ${pluginName}AuthProvider(${pluginContextName} context) {
        log.info("TODO: Initialize your AuthProvider using context");
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.Error)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        return CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.Banned)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return CompletableFuture.completedFuture(MQTT5ExtendedAuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.Banned)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Unimplemented"));
    }

    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        return CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setError(Error.newBuilder()
                        .setReason("Unimplemented")
                        .build())
                .build());
    }
}