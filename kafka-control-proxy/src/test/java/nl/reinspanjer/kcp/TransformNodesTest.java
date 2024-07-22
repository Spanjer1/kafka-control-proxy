package nl.reinspanjer.kcp;

import io.smallrye.config.SmallRyeConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.config.LogConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.data.Address;
import nl.reinspanjer.kcp.impl.TransformTestNode;
import nl.reinspanjer.kcp.request.MutableProduceRequest;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.MutableFetchResponse;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static nl.reinspanjer.kcp.TestUtils.getSizeOfConsumerRecord;
import static org.assertj.core.api.Assertions.assertThat;

public class TransformNodesTest
{
    static Integer KAFKA_PROXY_PORT = 8888;
    static String KAFKA_PROXY_HOST = "localhost";
    static Vertx vertx = Vertx.vertx();
    static AdminClient adminClient;
    static KafkaProducer<String, String> producer;
    static KafkaConsumer<String, String> consumer;
    static Properties producerProps = new Properties();
    static Properties consumerProps = new Properties();

    private static Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);
    private static TransformTestNode transformTestNode = new TransformTestNode();

    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                    .withExposedService("broker-1", 9092, Wait.forListeningPort())
                    .withExposedService("broker-2", 9092, Wait.forListeningPort())
                    .withExposedService("broker-3", 9092, Wait.forListeningPort());

    @AfterAll
    static void stop() {
        environment.stop();
    }

    @BeforeAll
    static void prepare() throws Throwable {
        environment.start();
        VertxTestContext testContext = new VertxTestContext();
        Map<String, String> overwrite = Map.of(
                "proxy.server.host", "localhost",
                "proxy.server.port", "" + KAFKA_PROXY_PORT,
                "proxy.origin.brokers[0].host", "localhost",
                "proxy.origin.brokers[0].port", "9092",
                "proxy.origin.brokers[1].host", "localhost",
                "proxy.origin.brokers[1].port", "9093",
                "proxy.origin.brokers[2].host", "localhost",
                "proxy.origin.brokers[2].port", "9094",
                "proxy.log.\"nl.reinspanjer\"", "INFO"
        );
        SmallRyeConfig config = TestUtils.loadConfigWithOverwrite(overwrite);
        ProxyConfig.build(config);
        LogConfig.build(config);
        NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE, ApiKeys.FETCH), transformTestNode);
        KafkaControlProxy.deploy(vertx).onComplete(testContext.succeedingThenComplete());
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_PROXY_HOST + ":" + KAFKA_PROXY_PORT + "," +
                KAFKA_PROXY_HOST + ":" + (KAFKA_PROXY_PORT + 1) + "," +
                KAFKA_PROXY_HOST + ":" + (KAFKA_PROXY_PORT + 2));

        adminClient = AdminClient.create(props);
        producerProps = (Properties) props.clone();
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(producerProps);

        consumerProps = (Properties) props.clone();
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(consumerProps);
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

        CreateTopicsResult result = adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get();
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "key", "value")).get(); // send a message with "value"
        consumer.subscribe(List.of(topic));
        List<ConsumerRecords<String, String>> records = TestUtils.pollTillResult(consumer, Duration.ofSeconds(5), 10, 1, topic);

        assertThat(records).isNotNull();
        assertThat(records.get(0).count()).isEqualTo(1);
        assertThat(records.get(0).iterator().next().value()).isEqualTo("transformed"); // expecting to be transformed
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

        CreateTopicsResult result = adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get();

        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "key", "value")).get(); // send a message with "value"
        consumer.subscribe(List.of(topic));
        List<ConsumerRecords<String, String>> records = TestUtils.pollTillResult(consumer, Duration.ofSeconds(5), 10, 1, topic);
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
                produceParts.setValue(ByteBuffer.wrap("transformed1".getBytes())); // transform any message with "transformed"
                return produceParts;
            }));

            res.response = mutableFetchResponse.toFetchResponse();
            return Future.succeededFuture(res);
        };
        transformTestNode.setResponseHandler(f);

        CreateTopicsResult result = adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get();

        for (int i = 0; i < 10; i++) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "key", "" + i)).get(); // send a message with "value"
            LOGGER.info("Sent message {}", i);
        }

        consumer.subscribe(List.of(topic));

        List<ConsumerRecords<String, String>> batchRecord = TestUtils.pollTillResult(consumer, Duration.ofSeconds(5), 10, 10, topic);
        assertThat(batchRecord).isNotNull();

        int combinedCount = batchRecord.stream().mapToInt((r) -> getSizeOfConsumerRecord(r, topic)).sum();
        assertThat(combinedCount).isEqualTo(10);

        boolean combinedBoolean = batchRecord.stream().allMatch(r -> {
            boolean returnBool = true;
            for (ConsumerRecord<String, String> stringStringConsumerRecord : r) {
                boolean l = stringStringConsumerRecord.value().equals("transformed1");
                if (!l) {
                    LOGGER.error("Expected transformed1 but got {}", stringStringConsumerRecord.value());
                    return false;
                }
            }
            return returnBool;
        });
        assertThat(combinedBoolean).isTrue();
    }

}
