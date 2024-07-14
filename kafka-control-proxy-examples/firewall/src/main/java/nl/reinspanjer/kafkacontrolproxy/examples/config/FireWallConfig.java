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

package nl.reinspanjer.kafkacontrolproxy.examples.config;

import inet.ipaddr.IPAddressString;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FireWallConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(FireWallConfig.class);

    private static FireWallConfig config;

    public Set<ApiKeys> admin = Set.of(ApiKeys.values());
    public Set<ApiKeys> producer = Set.of(
            ApiKeys.API_VERSIONS,
            ApiKeys.SASL_HANDSHAKE,
            ApiKeys.SASL_AUTHENTICATE,
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.METADATA,
            ApiKeys.PRODUCE
    );

    public Set<ApiKeys> consumer = Set.of(
            ApiKeys.API_VERSIONS,
            ApiKeys.SASL_HANDSHAKE,
            ApiKeys.SASL_AUTHENTICATE,
            ApiKeys.CONSUMER_GROUP_DESCRIBE,
            ApiKeys.CONSUMER_GROUP_HEARTBEAT,
            ApiKeys.JOIN_GROUP,
            ApiKeys.LEAVE_GROUP,
            ApiKeys.SYNC_GROUP,
            ApiKeys.FETCH
    );


    public Map<ApiKeys, Set<IPAddressString>> firewallRules = new HashMap<>();


    public static FireWallConfig build() {
        if (FireWallConfig.config != null) {
            return config;
        }

        SmallRyeConfig smallRyeConfig = new SmallRyeConfigBuilder().addDefaultSources()
                .withMapping(FireWallRules.class) // Match the prefix in your ConfigMapping
                .build();

        LOGGER.info(smallRyeConfig.getConfigSources().toString());

        FireWallRules fireWallRules = smallRyeConfig.getConfigMapping(FireWallRules.class);
        LOGGER.info(fireWallRules.producer().toString());

        FireWallConfig proxyConfig = new FireWallConfig();

        for (ApiKeys key : proxyConfig.admin) {
            addRules(key, proxyConfig, fireWallRules.admin());
        }

        for (ApiKeys key : proxyConfig.producer) {
            addRules(key, proxyConfig, fireWallRules.producer());
        }

        for (ApiKeys key : proxyConfig.consumer) {
            addRules(key, proxyConfig, fireWallRules.consumer());
        }

        FireWallConfig.config = proxyConfig;
        return proxyConfig;


    }

    private static void addRules(ApiKeys key, FireWallConfig proxyConfig, List<String> fireWallRules) {
        if (proxyConfig.firewallRules.get(key) == null) {

            Set<IPAddressString> ipAddressSet = new HashSet<>();
            ipAddressSet.addAll(fireWallRules.stream().map(IPAddressString::new).toList());
            proxyConfig.firewallRules.put(key, ipAddressSet);
        } else {
            proxyConfig.firewallRules.get(key).addAll(fireWallRules.stream().map(IPAddressString::new).toList());
        }
    }

}
