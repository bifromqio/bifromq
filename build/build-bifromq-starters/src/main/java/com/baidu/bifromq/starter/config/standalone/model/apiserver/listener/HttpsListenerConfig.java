package com.baidu.bifromq.starter.config.standalone.model.apiserver.listener;

import com.baidu.bifromq.starter.config.standalone.model.ServerSSLContextConfig;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class HttpsListenerConfig {
    private boolean enable = false;
    private String host;
    private int port = 8090;
    private ServerSSLContextConfig sslConfig;
}
