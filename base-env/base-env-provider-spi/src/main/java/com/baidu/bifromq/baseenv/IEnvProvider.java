package com.baidu.bifromq.baseenv;

import java.util.concurrent.ThreadFactory;

/**
 * This is a service provider interface(SPI) for customizing the thread running environment.
 */
public interface IEnvProvider {
    /**
     * Get current available processors number
     *
     * @return the available processors number
     */
    int availableProcessors();

    /**
     * Create a thread factory with given parameters
     *
     * @param name     the name of the created thread
     * @param daemon   if created thread is daemon thread
     * @param priority the priority of created thread
     * @return the created thread
     */
    ThreadFactory newThreadFactory(String name, boolean daemon, int priority);

    /**
     * Create a thread factory which creating thread with normal priority
     *
     * @param name   the name of the created thread
     * @param daemon if created thread is daemon thread
     * @return the created thread
     */
    default ThreadFactory newThreadFactory(String name, boolean daemon) {
        return newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Create a thread factory which creating non-daemon thread with normal priority
     *
     * @param name the name of the created thread
     * @return the created thread
     */
    default ThreadFactory newThreadFactory(String name) {
        return newThreadFactory(name, false);
    }
}
