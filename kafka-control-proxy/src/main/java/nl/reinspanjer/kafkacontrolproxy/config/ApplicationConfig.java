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

package nl.reinspanjer.kafkacontrolproxy.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;
import java.util.Optional;

@ConfigMapping(prefix = "proxy")
public interface ApplicationConfig {

    Server server();

    Origin origin();

    Ext ext();

    @ConfigMapping(prefix = "server")
    interface Server {

        String host();

        int port();

        @WithDefault("false")
        boolean ssl();

        @WithDefault("false")
        boolean sasl();

        Optional<String> keystorePath();

        Optional<String> keystorePassword();

    }

    @ConfigMapping(prefix = "origin")
    interface Origin {

        List<Broker> brokers();

        Optional<Admin> admin();

        TCPClientSettings tcpClient();

        @ConfigMapping(prefix = "broker")
        interface Broker {
            String host();

            int port();
        }

        interface Admin {
            String bootstrapHost();

            Integer bootstrapPort();

            boolean sasl();

            Optional<String> username();

            Optional<String> password();

        }

    }

    @ConfigMapping(prefix = "ext")
    interface Ext {
        SchemaRegistry schemaRegistry();

        Cache cache();

        @ConfigMapping(prefix = "schema-registry")
        interface SchemaRegistry {
            @WithDefault("false")
            Boolean enabled();

            Optional<String> host();

            Optional<Integer> port();
        }

        @ConfigMapping(prefix = "cache")
        interface Cache {

            Optional<Integer> periodicUpdate();
        }

    }


}
