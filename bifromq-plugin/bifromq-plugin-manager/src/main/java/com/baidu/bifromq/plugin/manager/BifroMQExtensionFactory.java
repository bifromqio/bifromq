package com.baidu.bifromq.plugin.manager;

import com.baidu.bifromq.plugin.BifroMQPlugin;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultExtensionFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginManager;
import org.pf4j.PluginRuntimeException;

import java.lang.reflect.Constructor;

@Slf4j
public class BifroMQExtensionFactory extends DefaultExtensionFactory {
    private final PluginManager pluginManager;

    public BifroMQExtensionFactory(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public <T> T create(Class<T> extensionClass) {
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(extensionClass.getClassLoader());
            Plugin ownerPlugin = pluginManager.whichPlugin(extensionClass).getPlugin();
            if (ownerPlugin instanceof BifroMQPlugin) {
                return createExtensionWithContext(extensionClass, (BifroMQPlugin<?>) ownerPlugin);
            }
            return super.create(extensionClass);
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
    }

    private <T> T createExtensionWithContext(Class<T> extensionClass, BifroMQPlugin<?> ownerPlugin) {
        try {
            Object pluginContext = ownerPlugin.context();
            Constructor<T> constructor = extensionClass.getConstructor(pluginContext.getClass());
            return constructor.newInstance(pluginContext);
        } catch (NoSuchMethodException e) {
            log.debug("No constructor with plugin context found for extension[{}], fallback to no-arg constructor",
                    extensionClass.getName());
            return super.create(extensionClass);
        } catch (Throwable e) {
            throw new PluginRuntimeException(e);
        }
    }
}
