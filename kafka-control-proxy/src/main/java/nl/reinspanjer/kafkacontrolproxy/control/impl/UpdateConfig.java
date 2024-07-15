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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kafkacontrolproxy.control.ObserverNode;
import nl.reinspanjer.kafkacontrolproxy.request.RequestHeaderAndPayload;
import nl.reinspanjer.kafkacontrolproxy.verticles.KafkaCacheService;
import org.apache.kafka.common.requests.AbstractResponse;

public class UpdateConfig implements ObserverNode {

    private KafkaCacheService service;
    private Vertx vertx;

    @Override
    public UpdateConfig init(Vertx vertx) {
        this.vertx = vertx;
        this.service = KafkaCacheService.createProxy(vertx, "kafka.cache");
        return this;
    }

    @Override
    public Future<Void> request(RequestHeaderAndPayload request) {
        Promise<Void> promise = Promise.promise();
        return promise.future();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        return this.service.update();
    }

}
