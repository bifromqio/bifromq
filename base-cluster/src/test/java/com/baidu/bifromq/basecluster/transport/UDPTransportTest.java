package com.baidu.bifromq.basecluster.transport;

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.junit.Test;

public class UDPTransportTest {
    @Test
    public void bindEphemeralPort() {
        UDPTransport transport = UDPTransport.builder()
            .bindAddr(new InetSocketAddress(0)).build();
        assertTrue(transport.bindAddress().getPort() > 0);
    }
}
