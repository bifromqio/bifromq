package com.baidu.bifromq.plugin.manager;

import com.baidu.bifromq.plugin.BifroMQPlugin;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultPluginFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

import java.lang.reflect.Constructor;

import static org.pf4j.RuntimeMode.DEVELOPMENT;

@Slf4j
public class BifroMQPluginFactory extends DefaultPluginFactory {
    protected Plugin createInstance(Class<?> pluginClass, PluginWrapper pluginWrapper) {
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginWrapper.getPluginClassLoader());
            if (BifroMQPlugin.class.isAssignableFrom(pluginClass)) {
                return createBifroMQPluginInstance(pluginClass, pluginWrapper);
            }
            return super.createInstance(pluginClass, pluginWrapper);
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
    }

    private Plugin createBifroMQPluginInstance(Class<?> pluginClass, PluginWrapper pluginWrapper) {
        BifroMQPluginDescriptor context = new BifroMQPluginDescriptor(
                pluginWrapper.getDescriptor(),
                pluginWrapper.getPluginPath(),
                pluginWrapper.getRuntimeMode() == DEVELOPMENT);
        try {
            Constructor<?> constructor = pluginClass.getDeclaredConstructor(BifroMQPluginDescriptor.class);
            return (Plugin) constructor.newInstance(context);
        } catch (Exception e) {
            log.error("Failed to initialize BifroMQ Plugin[{}]", pluginClass.getName(), e);
        }
        return null;
    }
}
