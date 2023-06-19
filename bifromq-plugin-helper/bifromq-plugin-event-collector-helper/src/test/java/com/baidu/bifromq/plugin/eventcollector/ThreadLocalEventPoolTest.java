package com.baidu.bifromq.plugin.eventcollector;

import com.baidu.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.testng.AssertJUnit.assertNotSame;

public class ThreadLocalEventPoolTest {
    @Test
    public void getLocal() {
        PingReq pingReq = ThreadLocalEventPool.getLocal(PingReq.class);
        AtomicReference<PingReq> pingReqRef = new AtomicReference<>();
        Thread t = new Thread(() -> pingReqRef.set(pingReqRef.get()));
        t.start();
        Uninterruptibles.joinUninterruptibly(t);
        assertNotSame(pingReq, pingReqRef.get());
    }
}
