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

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyConfig {

    private static ApplicationConfig config;
    private static Logger LOGGER = LoggerFactory.getLogger(ProxyConfig.class);

    public static ApplicationConfig build() {
        if (ProxyConfig.config != null) {
            return config;
        }

        SmallRyeConfig config = new SmallRyeConfigBuilder().addDefaultSources().withMapping(ApplicationConfig.class).build();
        ProxyConfig.config = config.getConfigMapping(ApplicationConfig.class);
        LOGGER.debug("host=" + ProxyConfig.config.server().host() +
                ":" + ProxyConfig.config.server().port() +
                ";ssl=" + ProxyConfig.config.server().ssl() + ";sasl=" + ProxyConfig.config.server().sasl() +
                ";keystore=" + ProxyConfig.config.server().keystorePath().orElse("not set"));


        return ProxyConfig.config;

    }

    // This is used to set the config in the different tests, this always overwrites the config in static context
    public static ApplicationConfig build(SmallRyeConfig config) {
        ApplicationConfig appConfig = config.getConfigMapping(ApplicationConfig.class);
        ProxyConfig.config = appConfig;

        return appConfig;
    }

}
