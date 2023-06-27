package com.baidu.bifromq.basecluster.transport;

import static org.testng.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.testng.annotations.Test;

public class TCPTransportTest {
    @Test
    public void bindEphemeralPort() {
        TCPTransport transport = TCPTransport.builder()
            .bindAddr(new InetSocketAddress(0))
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        InetSocketAddress socketAddress = transport.bindAddress();
        assertTrue(socketAddress.getPort() > 0);
        transport.shutdown().join();
    }
}
