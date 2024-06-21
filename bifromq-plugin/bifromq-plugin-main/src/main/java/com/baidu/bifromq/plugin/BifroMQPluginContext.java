package com.baidu.bifromq.plugin;

import lombok.Getter;
import org.pf4j.PluginDescriptor;
import org.pf4j.RuntimeMode;

import java.nio.file.Path;

/**
 * The context of BifroMQPlugin passed by BifroMQPluginManager during plugin initialization.
 */
@Getter
public class BifroMQPluginContext {
    private final PluginDescriptor descriptor;
    private final Path pluginRoot;
    private final RuntimeMode runtimeMode;

    /**
     * Constructor of BifroMQPluginContext.
     *
     * @param descriptor  the descriptor of the plugin
     * @param pluginRoot  the root path of the plugin
     * @param runtimeMode the runtime mode
     */
    public BifroMQPluginContext(PluginDescriptor descriptor, Path pluginRoot, RuntimeMode runtimeMode) {
        this.descriptor = descriptor;
        this.pluginRoot = pluginRoot;
        this.runtimeMode = runtimeMode;
    }
}
