package com.baidu.bifromq.plugin;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

/**
 * The base plugin class facilitating the development of custom BifroMQ plugin.
 */
@Slf4j
public abstract class BifroMQPlugin<C> extends Plugin {

    private final C context;

    /**
     * Subclass should override this constructor.
     *
     * @param context the context passed by plugin manager
     */
    protected BifroMQPlugin(BifroMQPluginContext context) {
        this.context = initContext(context);
        if (this.context == null) {
            log.warn("Plugin[{}] context is null", this.getClass().getName());
        }
    }

    private BifroMQPlugin(PluginWrapper wrapper) {
        throw new UnsupportedOperationException("Deprecated constructor is not allowed");
    }

    // hide the no-arg constructor
    private BifroMQPlugin() {
        throw new UnsupportedOperationException("No-arg constructor is not allowed");
    }

    /**
     * Subclass should override this method to initialize the concrete type of plugin context.
     *
     * @param context the context passed by plugin manager
     * @return the concrete type of plugin context
     */
    protected abstract C initContext(BifroMQPluginContext context);

    public C context() {
        return context;
    }
}
