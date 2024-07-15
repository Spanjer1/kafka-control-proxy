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

package nl.reinspanjer.kafkacontrolproxy.data;

import java.util.HashMap;
import java.util.Map;

public class BrokerOriginMap {
    private static BrokerOriginMap config;
    private Map<Address, Address> brokerToServer = new HashMap<>();
    private Map<Address, Address> serverToBroker = new HashMap<>();

    public static BrokerOriginMap build() {
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
            return null;
        }
        return config.brokerToServer.get(broker);
    }

    public Address getBrokerFromServer(Address server) {
        if (config == null) {
            return null;
        }
        return config.serverToBroker.get(server);
    }


}
