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

package nl.reinspanjer.kcp.verticles.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.admin.Broker;
import nl.reinspanjer.kcp.admin.Config;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.connector.KafkaAdmin;
import nl.reinspanjer.kcp.utils.Helper;
import nl.reinspanjer.kcp.verticles.KafkaCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaCacheServiceImpl implements KafkaCacheService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCacheServiceImpl.class);

    private Map<String, Config> kafkaConfig = new HashMap<>();

    private KafkaAdmin adminClient;

    private Vertx vertx;

    private ApplicationConfig config = ProxyConfig.build();

    public KafkaCacheServiceImpl(Vertx vertx) {
        LOGGER.info("Creating KafkaCacheService");
        this.adminClient = KafkaAdmin.create(vertx);
        this.vertx = vertx;

        if (config.ext().cache().periodicUpdate().isPresent()) {
            Integer period = config.ext().cache().periodicUpdate().get();
            LOGGER.info("Periodic update set to: {} ms", period);
            vertx.setPeriodic(period, id -> {
                update();
            });
        }
    }

    @Override
    public Future<Config> getConfig(String topicName) {
        Config config = this.kafkaConfig.get(topicName);
        if (this.kafkaConfig.get(topicName) != null) {
            return Future.succeededFuture(config);
        } else {
            return this.adminClient.getConfig(topicName).map(v -> {
                Config rConfig = Helper.from(v);
                this.kafkaConfig.put(topicName, rConfig);
                return rConfig;
            });
        }
    }

    @Override
    public Future<Broker> getBrokers() {
        LOGGER.info("Fetching Cluster Info for Brokers information");
        return this.adminClient.getClusterInfo();
    }

    @Override
    public Future<Void> update() {
        Promise<Void> promise = Promise.promise();

        this.adminClient.getTopicConfigMap()
                .onSuccess(m -> {
                    kafkaConfig = m.entrySet()
                            .stream().collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> Helper.from(entry.getValue())
                            ));
                    LOGGER.info("Config synced");
                    promise.complete();
                })
                .onFailure(ex -> {
                    LOGGER.error("Failed to update config", ex);
                    promise.fail(ex);
                });

        return promise.future();
    }


}
