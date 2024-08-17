/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.TransformNode;
import nl.reinspanjer.kcp.examples.config.FieldEncryptionConfig;
import nl.reinspanjer.kcp.examples.config.FieldEncryptionConfigInterface;
import nl.reinspanjer.kcp.examples.crypto.CryptoProvider;
import nl.reinspanjer.kcp.examples.crypto.impl.SimpleLocalCrypto;
import nl.reinspanjer.kcp.request.MutableProduceRequest;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.MutableFetchResponse;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

public class CryptoControl implements TransformNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(CryptoControl.class);
    private static final CryptoProvider cryptoProvider = new SimpleLocalCrypto();
    private Vertx vertx;
    public static FieldEncryptionConfigInterface config = FieldEncryptionConfig.build();

    @Override
    public CryptoControl init(Vertx vertx) {
        this.vertx = vertx;
        try {
            cryptoProvider.init();
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @Override
    public Future<RequestHeaderAndPayload> request(RequestHeaderAndPayload request) {
        Promise<RequestHeaderAndPayload> promise = Promise.promise();
        //encrypt
        if (request.request.apiKey() == ApiKeys.PRODUCE) {
            LOGGER.info("Produce " + vertx.getOrCreateContext().get("USERNAME"));
            ProduceRequest produceRequest = (ProduceRequest) request.request;
            MutableProduceRequest newRequest = new MutableProduceRequest(produceRequest);

            try {
                newRequest.transform((produceParts -> {
                    if (produceParts == null) {
                        throw new RuntimeException("No parts found for record");
                    }

                    String value = new String(produceParts.getValue().array(), StandardCharsets.UTF_8);
                    String topic = produceParts.getTopicName();

                    List<FieldEncryptionConfigInterface.Rule> rules = config.topic().getOrDefault(topic, List.of());
                    if (rules.isEmpty()) {
                        return produceParts;
                    }

                    String newValue;
                    try {
                        newValue = encryptFields(rules, value);
                    } catch (GeneralSecurityException | JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }


                    produceParts.setValue(ByteBuffer.wrap(newValue.getBytes()));

                    return produceParts;
                }));

            } catch (RuntimeException e) {
                LOGGER.error(e.getMessage());
                promise.fail(e);
                return promise.future();
            }

            request.request = newRequest.toProduceRequest();
            promise.complete(request);
        } else {
            promise.complete(request);
        }

        return promise.future();

    }

    @Override
    public Future<ResponseHeaderAndPayload> response(RequestHeaderAndPayload request, ResponseHeaderAndPayload response) {
        if (request.request.apiKey() == ApiKeys.FETCH) {
            LOGGER.info("Fetch " + vertx.getOrCreateContext().get("USERNAME"));
            FetchResponse fetchResponse = (FetchResponse) response.response;
            MutableFetchResponse newResponse = new MutableFetchResponse(fetchResponse);

            newResponse.transform(fetchParts -> {
                String topic = fetchParts.getTopicName();
                String value = new String(fetchParts.getValue().array(), StandardCharsets.UTF_8);

                List<FieldEncryptionConfigInterface.Rule> rules = config.topic().getOrDefault(topic, List.of());
                if (rules.isEmpty()) {
                    return fetchParts;
                }

                String newValue;
                try {
                    newValue = decryptFields(rules, value);
                } catch (GeneralSecurityException | JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                fetchParts.setValue(ByteBuffer.wrap(newValue.getBytes()));
                return fetchParts;
            });
            response.response = newResponse.toFetchResponse();
        }

        return Future.succeededFuture(response);
    }

    public String encryptFields(List<FieldEncryptionConfigInterface.Rule> rules, String value) throws GeneralSecurityException, JsonProcessingException {
        DocumentContext jsonContext = JsonPath.parse(value);
        ObjectMapper mapper = new ObjectMapper();

        for (FieldEncryptionConfigInterface.Rule rule : rules) {
            String rawPath = rule.path();
            Configuration conf = Configuration.builder().options(Option.AS_PATH_LIST).build();
            List<String> paths = JsonPath.using(conf).parse(value).read(rawPath);

            for (String path : paths) {
                Object obj = jsonContext.read(path);
                JsonNode node = mapper.convertValue(obj, JsonNode.class);
                String encryptedData = cryptoProvider.encrypt(node.toString());
                jsonContext.set(path, encryptedData);
            }

        }

        return jsonContext.jsonString();
    }

    public String decryptFields(List<FieldEncryptionConfigInterface.Rule> rules, String value) throws GeneralSecurityException, JsonProcessingException {
        DocumentContext jsonContext = JsonPath.parse(value);
        ObjectMapper mapper = new ObjectMapper();

        for (FieldEncryptionConfigInterface.Rule rule : rules) {
            String rawPath = rule.path();
            Configuration conf = Configuration.builder().options(Option.AS_PATH_LIST).build();
            List<String> paths = JsonPath.using(conf).parse(value).read(rawPath);

            for (String path : paths) {
                Object obj = jsonContext.read(path);
                String decryptedData = cryptoProvider.decrypt(obj.toString());
                JsonNode node = new ObjectMapper().readTree(decryptedData);

                if (JsonNodeType.STRING.equals(node.getNodeType())) {
                    jsonContext.set(path, node.asText());
                } else if (JsonNodeType.BOOLEAN.equals(node.getNodeType())) {
                    jsonContext.set(path, node.asBoolean());
                } else if (JsonNodeType.NUMBER.equals(node.getNodeType())) {
                    String numberString = node.asText();
                    // check if it includes a dot
                    if (numberString.contains(".")) {
                        jsonContext.set(path, node.asDouble());
                    } else {
                        jsonContext.set(path, node.asLong());
                    }

                } else if (JsonNodeType.OBJECT.equals(node.getNodeType())) {
                    Map nodeMap = mapper.convertValue(node, Map.class);
                    jsonContext.set(path, nodeMap);
                } else if (JsonNodeType.ARRAY.equals(node.getNodeType())) {
                    List nodeArray = mapper.convertValue(node, List.class);
                    jsonContext.set(path, nodeArray);
                } else {
                    throw new RuntimeException("Unknown type: " + node.getNodeType());
                }
            }

        }

        return jsonContext.jsonString();
    }
}
