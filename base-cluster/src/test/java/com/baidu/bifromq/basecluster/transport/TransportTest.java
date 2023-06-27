package com.baidu.bifromq.basecluster.transport;

import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

public class TransportTest {
    @Test
    public void bindPort() {
        Transport transport = Transport.builder().build();
        int port = transport.bindAddress().getPort();
        transport.shutdown().join();

        transport = Transport.builder()
            .bindAddr(new InetSocketAddress(port))
            .build();
        assertEquals(transport.bindAddress().getPort(), port);
    }

    @Test
    public void bindEphemeralPort() {
        Transport transport = Transport.builder().build();
        assertTrue(transport.bindAddress().getPort() > 0);
    }
}
