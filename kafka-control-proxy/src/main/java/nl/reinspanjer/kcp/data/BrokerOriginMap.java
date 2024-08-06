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

package nl.reinspanjer.kcp.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BrokerOriginMap {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerOriginMap.class);
    private static BrokerOriginMap config;
    private final Map<Address, Address> brokerToServer = new HashMap<>();
    private final Map<Address, Address> serverToBroker = new HashMap<>();

    public static BrokerOriginMap build() {
        LOGGER.debug("Building BrokerOriginMap");
        if (config == null) {
            config = new BrokerOriginMap();
        }
        return config;
    }

    public synchronized BrokerOriginMap put(Address broker, Address server) {
        if (config == null) {
            config = new BrokerOriginMap();
        }

        config.brokerToServer.put(broker, server);
        config.serverToBroker.put(server, broker);
        return config;
    }

    public Address getServerFromBroker(Address broker) {

        if (config == null) {
            LOGGER.error("BrokerOriginMap not initialized");
            return null;
        }

        Address addr = config.brokerToServer.get(broker);
        if (addr == null) {
            LOGGER.error("Broker {} not found in BrokerOriginMap", broker);
            LOGGER.debug("BrokerOriginMap: {}", config.brokerToServer);
        }

        return addr;
    }

    public Address getBrokerFromServer(Address server) {
        if (config == null) {
            return null;
        }
        return config.serverToBroker.get(server);
    }


}
