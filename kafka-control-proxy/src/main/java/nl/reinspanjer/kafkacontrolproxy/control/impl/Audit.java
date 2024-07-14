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

package nl.reinspanjer.kafkacontrolproxy.control.impl;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kafkacontrolproxy.control.ObserverNode;
import nl.reinspanjer.kafkacontrolproxy.request.RequestHeaderAndPayload;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Audit implements ObserverNode {
    Vertx vertx;
    Logger LOGGER = LoggerFactory.getLogger(Audit.class);

    @Override
    public Future<Void> request(RequestHeaderAndPayload request) {
        Context context = vertx.getOrCreateContext();
        String username = context.get("USERNAME");
        String brokerAddress = context.get("BrokerAddress");
        RequestHeader header = request.header;
        if (username != null) {
            LOGGER.info("User {} API {} Version {} proxy->origin {}",
                    username, header.apiKey(), header.apiVersion(), brokerAddress);
        } else {

            LOGGER.info("User ? API {} Version {} proxy->origin {}",
                    header.apiKey(), header.apiVersion(), brokerAddress);

        }

        return Future.succeededFuture();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        return Future.succeededFuture();
    }

    @Override
    public ObserverNode init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }
}
