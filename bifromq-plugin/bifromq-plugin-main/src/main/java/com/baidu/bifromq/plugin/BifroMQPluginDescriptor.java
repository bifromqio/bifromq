package com.baidu.bifromq.plugin;

import lombok.Getter;
import lombok.ToString;
import org.pf4j.PluginDescriptor;

import java.nio.file.Path;

/**
 * The descriptor of BifroMQPlugin passed by BifroMQPluginManager during plugin initialization.
 */
@Getter
@ToString
public class BifroMQPluginDescriptor {
    private final PluginDescriptor descriptor;
    private final Path pluginRoot;
    private final boolean isDevelopment;

    /**
     * Constructor of BifroMQPluginContext.
     *
     * @param descriptor    the descriptor of the plugin
     * @param pluginRoot    the root path of the plugin
     * @param isDevelopment the runtime mode
     */
    public BifroMQPluginDescriptor(PluginDescriptor descriptor, Path pluginRoot, boolean isDevelopment) {
        this.descriptor = descriptor;
        this.pluginRoot = pluginRoot;
        this.isDevelopment = isDevelopment;
    }
}
