package com.baidu.bifromq.plugin;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * The base plugin class facilitating the development of custom BifroMQ plugin.
 */
@Slf4j
public abstract class BifroMQPlugin<C extends BifroMQPluginContext> extends Plugin {

    protected final BifroMQPluginDescriptor descriptor;
    private final C context;

    /**
     * Subclass should override this constructor.
     *
     * @param descriptor the descriptor passed by plugin manager
     */
    @SneakyThrows
    protected BifroMQPlugin(BifroMQPluginDescriptor descriptor) {
        this.descriptor = descriptor;
        this.context = createContextInstance(descriptor);
    }

    private BifroMQPlugin(PluginWrapper wrapper) {
        throw new UnsupportedOperationException("Deprecated constructor is not allowed");
    }

    // hide the no-arg constructor
    private BifroMQPlugin() {
        throw new UnsupportedOperationException("No-arg constructor is not allowed");
    }

    @SneakyThrows
    private C createContextInstance(BifroMQPluginDescriptor descriptor) {
        Type genericSuperclass = getClass().getGenericSuperclass();
        if (genericSuperclass instanceof ParameterizedType parameterizedType) {
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (typeArguments.length > 0) {
                @SuppressWarnings("unchecked")
                Class<C> contextClass = (Class<C>) typeArguments[0];
                Constructor<C> constructor = contextClass.getDeclaredConstructor(BifroMQPluginDescriptor.class);
                // Check constructor visibility
                int modifiers = constructor.getModifiers();
                if (Modifier.isPrivate(modifiers)) {
                    throw new IllegalAccessException("Private constructor is not accessible: " + constructor.getName());
                }
                if (!Modifier.isPublic(modifiers)) {
                    log.warn("PluginContext's constructor should be public, visibility is {}: {}",
                            Modifier.isProtected(modifiers) ? "protected" : "package-private", constructor.getName());
                }
                constructor.setAccessible(true); // Allow protected and package-private access
                return constructor.newInstance(descriptor);
            }
        }
        throw new IllegalStateException("Unable to determine the context class type.");
    }

    public C context() {
        return context;
    }

    @Override
    public void start() {
        super.start();
        context.init();
    }

    @Override
    public void stop() {
        super.stop();
        context.close();
    }
}
