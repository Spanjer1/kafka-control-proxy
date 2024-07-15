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
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.connector.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistry extends AbstractVerticle {

    private static final String address = "schema.registry";
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistry.class);
    private final Map<Integer, JsonObject> cache = new HashMap<>();
    private WebClient webClient;

    private ApplicationConfig config = ProxyConfig.build();

    @Override
    public void start() {
        LOGGER.info("Starting up WebClient Verticle");
        // or else throw with supplier
        String host = config.ext().schemaRegistry().host().orElseThrow(() -> new RuntimeException("Schema Registry host not set"));
        Integer port = config.ext().schemaRegistry().port().orElseThrow(() -> new RuntimeException("Schema Registry port not set"));

        WebClientOptions options = new WebClientOptions()
                .setLogActivity(true)
                .setMaxPoolSize(10)
                .setIdleTimeout(10)
                .setConnectTimeout(10)
                .setDefaultHost(host)
                .setDefaultPort(port);

        webClient = WebClient.create(context.owner(), options);

        EventBus eb = vertx.eventBus();
        eb.consumer(address, this::onMessage);
    }

    private <T> void onMessage(Message<T> tMessage) {
        RequestMessage message;
        try {
            message = RequestMessage.parse((JsonObject) tMessage.body());
        } catch (ClassCastException ex) {
            tMessage.fail(500, ex.getMessage());
            return;
        }

        Future<JsonObject> schema = fetchSchema(message.topic, message.id, message.field);
        schema.onSuccess(tMessage::reply).onFailure(fail -> tMessage.fail(500, fail.getMessage()));

    }

    private Future<JsonObject> fetchSchema(String topic, Integer id, SchemaField field) {
        Promise<JsonObject> promise = Promise.promise();

        JsonObject cachedSchema = cache.get(id);

        if (cachedSchema != null) {
            LOGGER.info("Returned cached Schema");
            promise.complete(cachedSchema);
            return promise.future();
        }

        String subject = topic + field.label;
        String endpoint = String.format("/subjects/%s/versions/%d", subject, id);

        webClient.get(endpoint)
                .putHeader("Accept", "application/vnd.schemaregistry.v1+json")
                .as(BodyCodec.jsonObject())
                .send(ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == 200) {
                            JsonObject schema = ar.result().body();
                            this.cache.put(id, schema);
                            LOGGER.info("Schema retrieved and cached successfully.");
                            LOGGER.debug("Cached Schema: " + schema);
                            promise.complete(schema);
                        } else {
                            LOGGER.error("Failed to retrieve schema. Status code: " + ar.result().statusCode());
                            promise.fail("Failed to retrieve schema. Status code: " + ar.result().statusCode());
                        }
                    } else {
                        LOGGER.error("Error retrieving schema: " + ar.cause().getMessage());
                        promise.fail("Failed to retrieve schema. Status code: " + ar.result().statusCode());
                    }
                });

        return promise.future();
    }

    public void stop() {
        webClient.close();
    }

    public static class RequestMessage {
        String topic;
        Integer id;
        SchemaField field;

        public static RequestMessage parse(JsonObject object) throws ClassCastException {
            RequestMessage requestMessage = new RequestMessage();
            requestMessage.topic = object.getString("topic");
            requestMessage.id = object.getInteger("id");
            requestMessage.field = SchemaField.valueOf(object.getString("field"));
            return requestMessage;
        }

        public static JsonObject transform(RequestMessage msg) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.put("id", msg.id);
            jsonObject.put("topic", msg.topic);
            jsonObject.put("field", msg.field.toString());
            return jsonObject;
        }

        @Override
        public String toString() {
            return "topic=" + topic + " id=" + id + " field=" + field;
        }
    }


}
