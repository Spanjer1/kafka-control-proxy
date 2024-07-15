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

package nl.reinspanjer.kafkacontrolproxy;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kafkacontrolproxy.config.ApplicationConfig;
import nl.reinspanjer.kafkacontrolproxy.config.LogConfig;
import nl.reinspanjer.kafkacontrolproxy.config.ProxyConfig;
import nl.reinspanjer.kafkacontrolproxy.control.NodeRegistrator;
import nl.reinspanjer.kafkacontrolproxy.control.impl.Audit;
import nl.reinspanjer.kafkacontrolproxy.control.impl.TransformHosts;
import nl.reinspanjer.kafkacontrolproxy.control.impl.UpdateConfig;
import nl.reinspanjer.kafkacontrolproxy.data.Address;
import nl.reinspanjer.kafkacontrolproxy.data.BrokerOriginMap;
import nl.reinspanjer.kafkacontrolproxy.verticles.Server;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/*
 *   This class is the entry point for the Kafka Control Proxy. It deploys the KafkaCache Verticle, which is used to cache Kafka brokers, and the SchemaRegistry verticle.
 */
public class KafkaControlProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaControlProxy.class);
    private static final ApplicationConfig config = ProxyConfig.build();
    private static final BrokerOriginMap originConfig = BrokerOriginMap.build();

    public static Future<String> deploy(Vertx vertx) {
        LogConfig.configure();

        // Register internal nodes
        NodeRegistrator.registerNode(List.of(ApiKeys.CREATE_TOPICS), new UpdateConfig().init(vertx));
        NodeRegistrator.registerNode(List.of(ApiKeys.METADATA, ApiKeys.FIND_COORDINATOR), new TransformHosts().init(vertx));
        NodeRegistrator.registerNode(List.of(ApiKeys.values()), new Audit().init(vertx));

        List<ApplicationConfig.Origin.Broker> brokers = config.origin().brokers();
        if (brokers.isEmpty()) {
            LOGGER.error("No brokers configured");
            return Future.failedFuture("No brokers configured");
        }

        List<Future<String>> futures = getFutures(vertx, brokers);
        Future<String> fut = Future.all(futures).compose(s -> Future.succeededFuture());

        return fut.onSuccess((s) -> {
            LOGGER.debug("Deployed all verticles");
        }).onFailure(
                (t) -> {
                    LOGGER.error("Failed to deploy verticles", t);
                    vertx.close();
                }
        );
    }

    /*
     *   Deploy a server for every broker in the list
     */
    private static List<Future<String>> getFutures(Vertx vertx, List<ApplicationConfig.Origin.Broker> brokers) {
        int times = 3;
        LOGGER.info("Deploying {} Verticles for each broker", times);
        List<Future<String>> futures = new ArrayList<>();

        for (int j = 0; j < times; j++) {
            for (int i = 0; i < brokers.size(); i++) {
                ApplicationConfig.Origin.Broker be = brokers.get(i);
                String originHost = be.host();
                int originPort = be.port();
                int proxyPort = config.server().port() + i;
                Address broker = new Address(originHost, originPort);
                Address serverAddress = new Address(config.server().host(), proxyPort);
                LOGGER.info("Server " + serverAddress + " -> Broker " + broker);
                originConfig.put(broker, serverAddress);
                Server server = new Server(proxyPort);
                vertx.deployVerticle(server);
                futures.add(server.listening());
            }
        }

        return futures;
    }

}
