package com.baidu.bifromq.basecluster.transport;

import java.net.InetSocketAddress;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

public class UDPTransportTest {
    @Test
    public void bindEphemeralPort() {
        UDPTransport transport = UDPTransport.builder()
            .bindAddr(new InetSocketAddress(0)).build();
        assertTrue(transport.bindAddress().getPort() > 0);
    }
}
