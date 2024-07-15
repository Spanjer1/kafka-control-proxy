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

package nl.reinspanjer.kcp.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.serviceproxy.ServiceBinder;
import nl.reinspanjer.kcp.verticles.impl.KafkaCacheServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCache extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCache.class);

    @Override
    public void start() {
        LOGGER.info("Starting KafkaCache");
        KafkaCacheService service = new KafkaCacheServiceImpl(vertx);

        new ServiceBinder(vertx)
                .setAddress("kafka.cache")
                .register(KafkaCacheService.class, service)
                .completionHandler(res -> {
                    if (res.succeeded()) {
                        LOGGER.info("KafkaCacheService published");
                    } else {
                        LOGGER.error("Failed to publish KafkaCacheService", res.cause());
                    }
                });
    }

}
