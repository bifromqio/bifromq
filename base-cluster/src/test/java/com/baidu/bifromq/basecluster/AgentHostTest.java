package com.baidu.bifromq.basecluster;

import org.testng.annotations.Test;

public class AgentHostTest {
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidBindAddr() {
        AgentHostOptions options = AgentHostOptions.builder().build();
        AgentHost host = new AgentHost(options);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void invalidBindAddr1() {
        AgentHostOptions options = AgentHostOptions.builder().addr("0.0.0.0").build();
        AgentHost host = new AgentHost(options);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void clusterNameWithIncorrectPort() {
        AgentHostOptions options = AgentHostOptions.builder()
            .addr("127.0.0.1")
            .clusterDomainName("test.domain").build();
        AgentHost host = new AgentHost(options);
    }
}
