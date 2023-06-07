package com.baidu.bifromq.mqtt.utils;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;

public class TestTicker extends Ticker {
    private long nanos;

    public TestTicker() {
        reset();
    }

    public void reset() {
        nanos = System.nanoTime();
    }

    @Override
    public long read() {
        return nanos;
    }

    public void advanceTimeBy(long duration, TimeUnit unit) {
        nanos += unit.toNanos(duration);
    }
}
