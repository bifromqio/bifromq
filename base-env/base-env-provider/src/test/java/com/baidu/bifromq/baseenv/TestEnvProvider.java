package com.baidu.bifromq.baseenv;

import java.util.concurrent.ThreadFactory;

public class TestEnvProvider implements IEnvProvider {
    @Override
    public int availableProcessors() {
        return 1;
    }

    @Override
    public ThreadFactory newThreadFactory(String name, boolean daemon, int priority) {
        return r -> {
            Thread t = new Thread(r);
            t.setName(name);
            t.setDaemon(daemon);
            t.setPriority(priority);
            return t;
        };
    }
}
