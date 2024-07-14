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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kafkacontrolproxy.control.TransformNode;
import nl.reinspanjer.kafkacontrolproxy.request.MutableProduceRequest;
import nl.reinspanjer.kafkacontrolproxy.request.RequestHeaderAndPayload;
import nl.reinspanjer.kafkacontrolproxy.utils.BufferUtil;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class HeaderTransformHead implements TransformNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderTransformHead.class);
    private Vertx vertx;

    @Override
    public HeaderTransformHead init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    @Override
    public Future<RequestHeaderAndPayload> request(RequestHeaderAndPayload request) {
        LOGGER.debug("Transforming request");
        Promise<RequestHeaderAndPayload> promise = Promise.promise();

        if (request.request.apiKey() == ApiKeys.PRODUCE) {
            ProduceRequest produceRequest = (ProduceRequest) request.request;

            MutableProduceRequest newRequest = new MutableProduceRequest(produceRequest);

            AtomicInteger n = new AtomicInteger();
            newRequest.transform((produceParts -> {
                if (produceParts == null) {
                    throw new RuntimeException("No parts found for record");
                }
                List<Header> headers = produceParts.getHeaders();
                Header newHeader = new RecordHeader("newHeader", "newValue".getBytes());
                headers.add(newHeader);

                String newKey = "" + n.getAndAdd(1);

                produceParts.setKey(BufferUtil.fromString(newKey));
                String value = new String(produceParts.getValue().array(), StandardCharsets.UTF_8);

                produceParts.setValue(BufferUtil.fromString(value + newKey));
                produceParts.setHeaders(headers);

                return produceParts;
            }));

            request.request = newRequest.toProduceRequest();
            promise.complete(request);
        } else {
            promise.complete(request);
        }

        return promise.future();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }

}
