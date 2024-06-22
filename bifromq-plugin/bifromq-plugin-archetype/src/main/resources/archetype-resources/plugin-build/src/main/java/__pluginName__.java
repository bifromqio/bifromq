package ${groupId};

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.baidu.bifromq.plugin.BifroMQPlugin;
import com.baidu.bifromq.plugin.BifroMQPluginDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class ${pluginName} extends BifroMQPlugin<${pluginContextName}> {
    private static final Logger log = LoggerFactory.getLogger(${pluginName}.class);
    private static final String LOGBACK_CONFIG_FILE = "conf/logback.xml";
    private static final String PLUGIN_CONFIG_FILE = "conf/config.yaml";


    public ${pluginName} (BifroMQPluginDescriptor descriptor){
        super(descriptor);
        // setup logger context using plugin's logback.xml
        configureLoggerContext(descriptor.getPluginRoot());
        try {
            log.info("TODO: Initialize your plugin using config: {}", findConfigFile(descriptor.getPluginRoot()));
            log.info("---config.yaml start---");
            for (String line : Files.readAllLines(findConfigFile(descriptor.getPluginRoot()))) {
                log.info("{}", line);
            }
            log.info("---config.yaml end---");
        } catch (Exception e) {
            log.error("Failed to initialize plugin", e);
        }
    }

    private void configureLoggerContext(Path rootPath) {
        try {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            File logbackConfig = new File(rootPath.resolve(LOGBACK_CONFIG_FILE).toAbsolutePath().toString());
            if (logbackConfig.exists()) {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                configurator.doConfigure(logbackConfig);
            } else {
                log.warn("logback.xml not found for {}", getClass().getName());
            }
        } catch (Exception e) {
            log.error("Failed to configure logging for {}", getClass().getName(), e);
        }
    }

    private Path findConfigFile(Path rootPath) {
        return rootPath.resolve(PLUGIN_CONFIG_FILE);
    }
}