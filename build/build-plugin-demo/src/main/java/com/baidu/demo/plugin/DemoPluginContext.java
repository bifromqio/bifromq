package com.baidu.demo.plugin;

import com.baidu.bifromq.plugin.BifroMQPluginContext;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoPluginContext extends BifroMQPluginContext {
    public DemoPluginContext(BifroMQPluginDescriptor descriptor) {
        super(descriptor);
    }

    @Override
    public void init() {
        log.info("Init Demo Plugin Context");
    }

    @Override
    public void close() {
        log.info("Close Demo Plugin Context");
    }
}
