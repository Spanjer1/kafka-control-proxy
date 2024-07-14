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

package nl.reinspanjer.kafkacontrolproxy.examples;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kafkacontrolproxy.control.DecisionNode;
import nl.reinspanjer.kafkacontrolproxy.request.RequestHeaderAndPayload;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDecisionNode implements DecisionNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogDecisionNode.class);
    private Vertx vertx;

    @Override
    public LogDecisionNode init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    @Override
    public Future<Boolean> request(RequestHeaderAndPayload request) {
        Context context = this.vertx.getOrCreateContext();
        context.put("TEST", "TEST");
        Promise<Boolean> resultPromise = Promise.promise();

        vertx.runOnContext(v -> {
            LOGGER.info("Request: " + request.request.data());
            resultPromise.complete(true);
        });

        return resultPromise.future();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        Promise<Boolean> resultPromise = Promise.promise();

        vertx.runOnContext(v -> {
            Context context = this.vertx.getOrCreateContext();
            LOGGER.info("Request: " + request.request.data());
            resultPromise.complete(true);
            context.get("TEST");
        });


        return Future.succeededFuture();
    }

}
