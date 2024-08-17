/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.examples.config;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldEncryptionConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldEncryptionConfig.class);

    private static FieldEncryptionConfigInterface config;

    public static FieldEncryptionConfigInterface build() {
        if (FieldEncryptionConfig.config != null) {
            return config;
        }

        SmallRyeConfig smallRyeConfig = new SmallRyeConfigBuilder().addDefaultSources()
                .withMapping(FieldEncryptionConfigInterface.class)
                .build();

        LOGGER.info(smallRyeConfig.getConfigSources().toString());
        config = smallRyeConfig.getConfigMapping(FieldEncryptionConfigInterface.class);

        return config;
    }

    public static void setConfig(FieldEncryptionConfigInterface config) {
        FieldEncryptionConfig.config = config;
    }



}
