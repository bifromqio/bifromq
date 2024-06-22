package com.baidu.bifromq.plugin;

public abstract class BifroMQPluginContext {
    protected final BifroMQPluginDescriptor descriptor;

    public BifroMQPluginContext(BifroMQPluginDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    protected void init() {
        // do nothing
    }

    protected void close() {
        // do nothing
    }
}
