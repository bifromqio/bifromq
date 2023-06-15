package com.baidu.bifromq.basecluster.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.junit.Test;

public class TransportTest {
    @Test
    public void bindPort() {
        Transport transport = Transport.builder().build();
        int port = transport.bindAddress().getPort();
        transport.shutdown().join();

        transport = Transport.builder()
            .bindAddr(new InetSocketAddress(port))
            .build();
        assertEquals(port, transport.bindAddress().getPort());
    }

    @Test
    public void bindEphemeralPort() {
        Transport transport = Transport.builder().build();
        assertTrue(transport.bindAddress().getPort() > 0);
    }
}
