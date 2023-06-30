package com.baidu.bifromq.baseenv;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.concurrent.ThreadFactory;
import org.testng.annotations.Test;

public class EnvProviderTest {
    @Test
    public void defaultProvider() {
        IEnvProvider envProvider = new EnvProvider();
        ThreadFactory threadFactory = envProvider.newThreadFactory("Test");
        Thread t = threadFactory.newThread(() -> {
        });
        assertFalse(t.isDaemon());
        assertEquals(t.getPriority(), Thread.NORM_PRIORITY);
        assertEquals(t.getName(), "Test");
        Thread t1 = threadFactory.newThread(() -> {
        });
        assertEquals(t1.getName(), "Test-1");
    }

    @Test
    public void customProvider() {
        IEnvProvider envProvider = EnvProvider.INSTANCE;
        assertEquals(envProvider.availableProcessors(), 1);
        ThreadFactory threadFactory = envProvider.newThreadFactory("test");
        Thread thread = threadFactory.newThread(() -> {
        });
        assertFalse(thread.isDaemon());
        assertEquals(thread.getName(), "test");
        assertEquals(thread.getPriority(), Thread.NORM_PRIORITY);
    }
}
