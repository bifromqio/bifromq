package ${groupId};

import com.baidu.bifromq.plugin.BifroMQPluginContext;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ${pluginContextName} extends BifroMQPluginContext{
    private static final Logger log = LoggerFactory.getLogger(${pluginContextName}.class);

    public ${pluginContextName} (BifroMQPluginDescriptor descriptor){
        super(descriptor);
    }

    @Override
    protected void init() {
        log.info("TODO: Initialize your plugin context using descriptor {}", descriptor);
    }

    @Override
    protected void close() {
        log.info("TODO: Close your plugin context");
    }
}