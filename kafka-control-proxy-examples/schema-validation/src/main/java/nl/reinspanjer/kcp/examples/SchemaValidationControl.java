/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.DecisionNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SchemaValidationControl implements DecisionNode {

    private static final JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
    private static final JsonMapper mapper = new JsonMapper();
    private static final Map<String, JsonSchema> schemas = new HashMap<>();
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SchemaValidationControl.class);

    public static void registerSchema(String topic, String schema) {
        JsonSchema jsonSchema = factory.getSchema(schema);
        schemas.put(topic, jsonSchema);
    }

    @Override
    public Future<Boolean> request(RequestHeaderAndPayload request) {
        AtomicBoolean result = new AtomicBoolean(true);

        if (request.request.apiKey() == ApiKeys.PRODUCE) {
            ProduceRequest produceRequest = (ProduceRequest) request.request;
            produceRequest.data().topicData().forEach(topicProduceData -> {
                String topic = topicProduceData.name();
                JsonSchema jsonSchema = schemas.get(topic);
                if (jsonSchema != null) {
                    topicProduceData.partitionData().forEach(
                            partitionProduceData -> {
                                MemoryRecords records = (MemoryRecords) partitionProduceData.records();
                                records.batches().forEach(batch -> {
                                    batch.iterator().forEachRemaining(
                                            record -> {
                                                DefaultRecord dr = (DefaultRecord) record;
                                                String jsonValue = new String(dr.value().array(), StandardCharsets.UTF_8);
                                                try {
                                                    JsonNode jsonNode = mapper.readTree(jsonValue);
                                                    Set<ValidationMessage> res = jsonSchema.validate(jsonNode);
                                                    if (!res.isEmpty()) {
                                                        result.set(false);
                                                        LOGGER.error("Schema validation failed for topic: " + topic + " with message: " + res);
                                                    }
                                                } catch (JsonProcessingException e) {
                                                    LOGGER.error("Json parsing fault", e);
                                                    result.set(false);
                                                }
                                            }
                                    );

                                });

                            }
                    );
                }
            });

        }
        return Future.succeededFuture(result.get());
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        return Future.succeededFuture();
    }

    @Override
    public SchemaValidationControl init(Vertx vertx) {
        return this;
    }
}
