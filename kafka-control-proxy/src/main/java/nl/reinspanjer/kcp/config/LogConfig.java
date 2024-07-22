/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogConfig {

    private final static String DEFAULT_LOG_FORMAT = "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n";
    private static LogConfigInterface logConfig;

    public void build() {

    }

    // This is used to set the config in the different tests, this always overwrites the config in static context
    public static void build(SmallRyeConfig config) {
        LogConfig.logConfig = config.getConfigMapping(LogConfigInterface.class);
    }

    public static boolean configure() {
        if (logConfig == null) {
            SmallRyeConfig config = new SmallRyeConfigBuilder().addDefaultSources().withMapping(LogConfigInterface.class).build();
            logConfig = config.getConfigMapping(LogConfigInterface.class);

        }
        Map<String, LogConfigInterface.Level> logMap = logConfig.logs();

        String loglevel = logMap.getOrDefault("root", LogConfigInterface.Level.INFO).toString();
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.toLevel(loglevel));
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern(DEFAULT_LOG_FORMAT);
        encoder.start();

        consoleAppender.setEncoder(encoder);
        consoleAppender.start();

        rootLogger.addAppender(consoleAppender);

        logMap.remove("root");
        logMap.forEach((loggerName, level) -> {
            Logger logger = context.getLogger(loggerName);
            logger.setLevel(Level.toLevel(level.toString()));
        });

        return true;


    }
}
