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

package nl.reinspanjer.kcp.examples;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.ObserverNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This is a simple example of a DecisionNode implementation, which logs the request data and always allows the request to proceed.
public class LogObserverNode implements ObserverNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogObserverNode.class);
    private Vertx vertx;

    @Override
    public LogObserverNode init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    @Override
    public Future<Void> request(RequestHeaderAndPayload request) {

        Context context = this.vertx.getOrCreateContext();
        context.put("TEST", "TEST");
        Promise<Void> resultPromise = Promise.promise();

        vertx.runOnContext(v -> {
            LOGGER.info("Request: {}", request.request.data());
            resultPromise.complete();
        });

        return resultPromise.future();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        Promise<Boolean> resultPromise = Promise.promise();

        vertx.runOnContext(v -> {
            Context context = this.vertx.getOrCreateContext();
            String test = context.get("TEST");
            LOGGER.info("Context {} Response: {}", test, response.data());
            resultPromise.complete();
        });

        return Future.succeededFuture();
    }

}
