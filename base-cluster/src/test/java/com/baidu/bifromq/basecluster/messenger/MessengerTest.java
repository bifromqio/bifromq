package com.baidu.bifromq.basecluster.messenger;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.reactivex.rxjava3.schedulers.Schedulers;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.junit.Test;

public class MessengerTest {
    @Test
    public void shutdown() {
        Messenger localMessenger = Messenger.builder()
            .bindAddr(new InetSocketAddress("127.0.0.1", 0))
            .scheduler(Schedulers.io())
            .opts(new MessengerOptions())
            .build();
        localMessenger.start(new IRecipientSelector() {
            @Override
            public Collection<? extends IRecipient> selectForSpread(int limit) {
                return Collections.emptyList();
            }

            @Override
            public int clusterSize() {
                return 1;
            }
        });
        localMessenger.shutdown().join();
        localMessenger.shutdown().join();
    }

    @Test
    public void shutdownWithoutStart() {
        Messenger localMessenger = Messenger.builder()
            .bindAddr(new InetSocketAddress("127.0.0.1", 0))
            .scheduler(Schedulers.io())
            .opts(new MessengerOptions())
            .build();
        try {
            localMessenger.shutdown().join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }
}
