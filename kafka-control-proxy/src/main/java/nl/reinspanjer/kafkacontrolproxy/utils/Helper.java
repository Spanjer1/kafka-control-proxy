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

package nl.reinspanjer.kafkacontrolproxy.utils;

import nl.reinspanjer.kafkacontrolproxy.admin.Config;
import nl.reinspanjer.kafkacontrolproxy.admin.ConfigEntry;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class for mapping native and Vert.x Kafka objects
 */
public class Helper {

    private Helper() {
    }

    public static Config from(org.apache.kafka.clients.admin.Config config) {
        return new Config(Helper.fromConfigEntries(config.entries()));
    }

    public static org.apache.kafka.clients.admin.ConfigEntry to(ConfigEntry configEntry) {
        return new org.apache.kafka.clients.admin.ConfigEntry(configEntry.getName(), configEntry.getValue());
    }

    public static ConfigEntry from(org.apache.kafka.clients.admin.ConfigEntry configEntry) {
        return new ConfigEntry(configEntry.name(), configEntry.value());
    }

    public static List<ConfigEntry> fromConfigEntries(Collection<org.apache.kafka.clients.admin.ConfigEntry> configEntries) {
        return configEntries.stream().map(Helper::from).collect(Collectors.toList());
    }

    public static String buildJaasConfig(String username, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
    }


}
