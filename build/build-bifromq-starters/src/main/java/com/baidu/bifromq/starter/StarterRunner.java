/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.starter;

import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

@Slf4j
public class StarterRunner {

    public static void run(Class<? extends BaseStarter> starterClazz, String[] args) {
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine cmd = parser.parse(cliOptions(), args);
            File confFile = new File(cmd.getOptionValue("c"));
            if (!confFile.exists()) {
                throw new RuntimeException("Conf file does not exist: " + cmd.getOptionValue("c"));
            }
            BaseStarter starter = starterClazz.getDeclaredConstructor().newInstance();
            starter.init(starter.buildConfig(confFile));
            starter.start();
            Thread shutdownThread = new Thread(starter::stop);
            shutdownThread.setName("shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownThread);
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            formatter.printHelp("CMD", cliOptions());
        }
    }

    private static Options cliOptions() {
        return new Options()
            .addOption(Option.builder()
                .option("c")
                .longOpt("conf")
                .desc("the conf file for Starter")
                .hasArg(true)
                .optionalArg(false)
                .argName("CONF_FILE")
                .required(true)
                .build());
    }
}
