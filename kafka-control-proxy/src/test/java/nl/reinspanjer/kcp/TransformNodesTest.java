package nl.reinspanjer.kcp;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.impl.TransformTestNode;
import nl.reinspanjer.kcp.request.MutableProduceRequest;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.MutableFetchResponse;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformNodesTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformNodesTest.class);
    private static final TransformTestNode transformTestNode = new TransformTestNode();
    static Integer KAFKA_PROXY_PORT = 8888;
    static Vertx vertx = Vertx.vertx();
    static KafkaUtils utils = KafkaUtils.getInstance();

    @ClassRule
    public static DockerComposeContainer environment = utils.register();

    @AfterAll
    static void stop() throws InterruptedException {
        utils.unregister();
    }

    @BeforeAll
    static void prepare() throws Throwable {
        KCPUtils.start(KAFKA_PROXY_PORT, vertx);
        NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE, ApiKeys.FETCH), transformTestNode);
    }

    @Test
    public void testTransformNodeRequest() throws ExecutionException, InterruptedException {
        String topic = "testTransformNodeRequest";

        Function<RequestHeaderAndPayload, Future<RequestHeaderAndPayload>> f = (r) -> {
            if (r.request.apiKey() == ApiKeys.FETCH) {
                return Future.succeededFuture(r); //don't transform fetch requests
            }
            MutableProduceRequest newRequest = new MutableProduceRequest((ProduceRequest) r.request);
            newRequest.transform((produceParts -> {
                if (produceParts == null) {
                    throw new RuntimeException("No parts found for record");
                }
                produceParts.setValue(ByteBuffer.wrap("transformed".getBytes())); // transform any message with "transformed"
                return produceParts;
            }));

            r.request = newRequest.toProduceRequest();
            return Future.succeededFuture(r);
        };
        transformTestNode.setRequestHandler(f);
        transformTestNode.setResponseHandler((req, res) -> Future.succeededFuture(res));

        utils.createTopic(topic, 1, (short) 1);
        utils.send(topic, "key", "value");

        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofMillis(1000), 10, 1);

        assertThat(records).isNotNull();
        assertThat(records.get(0).count()).isEqualTo(1);
        assertThat(records.get(0).iterator().next().value()).isEqualTo("transformed"); // expecting to be transformed
    }

    @Test
    public void testTransformKey() throws ExecutionException, InterruptedException {
        String topic = "testTransformKey";

        Function<RequestHeaderAndPayload, Future<RequestHeaderAndPayload>> f = (r) -> {
            if (r.request.apiKey() == ApiKeys.FETCH) {
                return Future.succeededFuture(r); //don't transform fetch requests
            }
            MutableProduceRequest newRequest = new MutableProduceRequest((ProduceRequest) r.request);
            newRequest.transform((produceParts -> {
                if (produceParts == null) {
                    throw new RuntimeException("No parts found for record");
                }
                produceParts.setKey(ByteBuffer.wrap("same".getBytes())); // transform any message with key.
                return produceParts;
            }));

            r.request = newRequest.toProduceRequest();
            return Future.succeededFuture(r);
        };

        transformTestNode.setRequestHandler(f);
        transformTestNode.setResponseHandler((req, res) -> Future.succeededFuture(res));

        utils.createTopic(topic, 3, (short) 1);
        for (int i = 0; i < 100; i++) {
            utils.send(topic, UUID.randomUUID().toString(), "value");
        }

        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofMillis(1000), 10, 100);
        assertThat(records).isNotNull();

        List<ConsumerRecord<String, String>> flatRecords = utils.flattenRecords(records);
        assertThat(flatRecords.size()).isEqualTo(100);

        assertThat(flatRecords.stream().allMatch((r) -> "same".equals(r.key()))).isTrue(); // expecting to be transformed

        Map<Integer, Integer> countPartition = new HashMap<>();

        for (ConsumerRecord<String, String> next : flatRecords) {
            int n = countPartition.getOrDefault(next.partition(), 1);
            countPartition.put(next.partition(), n);
        }

        assertThat(countPartition.size()).isEqualTo(1);
    }

    @Test
    public void testTransformNodeResponse() throws ExecutionException, InterruptedException {
        String topic = "testTransformNodeResponse";
        BiFunction<RequestHeaderAndPayload, ResponseHeaderAndPayload, Future<ResponseHeaderAndPayload>> f = (req, res) -> {
            if (res.response.apiKey() == ApiKeys.PRODUCE) {
                return Future.succeededFuture(res); //don't transform produce requests
            }
            FetchResponse fetchResponse = (FetchResponse) res.response;
            MutableFetchResponse mutableFetchResponse = new MutableFetchResponse(fetchResponse);
            mutableFetchResponse.transform((produceParts -> {
                if (produceParts == null) {
                    throw new RuntimeException("No parts found for record");
                }
                produceParts.setValue(ByteBuffer.wrap("transformed1".getBytes())); // transform any message with "transformed"
                return produceParts;
            }));

            res.response = mutableFetchResponse.toFetchResponse();
            return Future.succeededFuture(res);
        };

        transformTestNode.setResponseHandler(f);
        utils.createTopic(topic, 1, (short) 1);
        utils.send(topic, "key", "value");
        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofMillis(1000), 10, 1);

        assertThat(records).isNotNull();
        assertThat(records.get(0).count()).isEqualTo(1);
        assertThat(records.get(0).iterator().next().value()).isEqualTo("transformed1"); // expecting to be transformed


    }

    @Test
    public void testTransformNodeBatch() throws ExecutionException, InterruptedException {
        String topic = "testTransformNodeBatch";
        BiFunction<RequestHeaderAndPayload, ResponseHeaderAndPayload, Future<ResponseHeaderAndPayload>> f = (req, res) -> {
            if (res.response.apiKey() == ApiKeys.PRODUCE) {
                return Future.succeededFuture(res); //don't transform produce requests
            }
            FetchResponse fetchResponse = (FetchResponse) res.response;
            MutableFetchResponse mutableFetchResponse = new MutableFetchResponse(fetchResponse);
            mutableFetchResponse.transform((produceParts -> {
                if (produceParts == null) {
                    throw new RuntimeException("No parts found for record");
                }
                ByteBuffer buff = produceParts.getValue();
                String value = new String(buff.array());

                produceParts.setValue(ByteBuffer.wrap(("transformed-" + value).getBytes())); // transform any message with "transformed"
                return produceParts;
            }));

            res.response = mutableFetchResponse.toFetchResponse();
            return Future.succeededFuture(res);
        };
        transformTestNode.setResponseHandler(f);
        transformTestNode.setRequestHandler(Future::succeededFuture);

        utils.createTopic(topic, 1, (short) 1);


        for (int i = 0; i < 10; i++) {
            utils.send(topic, "key", "" + i);
        }
        List<ConsumerRecords<String, String>> batchRecord = utils.receive(topic, Duration.ofMillis(500), 10, 10);
        assertThat(batchRecord).isNotNull();

        int combinedCount = batchRecord.stream().mapToInt((r) -> KafkaUtils.getSizeOfConsumerRecord(r, topic)).sum();
        assertThat(combinedCount).isEqualTo(10);

        boolean combinedBoolean = batchRecord.stream().allMatch(r -> {
            boolean returnBool = true;
            int i = 0;
            for (ConsumerRecord<String, String> stringStringConsumerRecord : r) {
                String v = "transformed-" + i++;
                boolean l = stringStringConsumerRecord.value().equals(v);
                if (!l) {
                    LOGGER.error("Expected {} but got {}", v, stringStringConsumerRecord.value());
                    return false;
                }
            }
            return returnBool;
        });
        assertThat(combinedBoolean).isTrue();
    }

}
