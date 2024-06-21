package com.baidu.bifromq.plugin;

import lombok.SneakyThrows;
import org.pf4j.PluginWrapper;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.testng.Assert.assertTrue;

public class BifroMQPluginTest {
    @SneakyThrows
    @Test
    void unwantedConstructorHided() {
        Constructor<?> noArgConstructor = BifroMQPlugin.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(noArgConstructor.getModifiers()));

        Constructor<?> deprecatedConstructor = BifroMQPlugin.class.getDeclaredConstructor(PluginWrapper.class);
        assertTrue(Modifier.isPrivate(deprecatedConstructor.getModifiers()));
    }
}
