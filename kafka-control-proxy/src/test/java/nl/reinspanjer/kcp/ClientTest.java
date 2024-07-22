package nl.reinspanjer.kcp;

import io.smallrye.config.SmallRyeConfig;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.config.LogConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.data.Address;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ClientTest {
    static Integer KAFKA_PROXY_PORT = 8888;
    static String KAFKA_PROXY_HOST = "localhost";
    static List<Address> addresses = List.of(new Address("localhost", 8888), new Address("localhost", 8889), new Address("localhost", 8890));
    static Vertx vertx = Vertx.vertx();
    static AdminClient adminClient;
    static KafkaProducer<String, String> producer;
    static KafkaConsumer<String, String> consumer;
    static Properties producerProps = new Properties();
    static Properties consumerProps = new Properties();

    private static Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);

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

        consumer = new KafkaConsumer<>(consumerProps);
    }

    @Test
    public void testDescribeClusterResult() throws ExecutionException, InterruptedException {

        DescribeClusterResult result = adminClient.describeCluster();

        String clusterId = result.clusterId().get();
        assertThat(clusterId).isNotNull();

        Collection<Node> nodes = result.nodes().get();

        assertThat(clusterId).isNotNull();

        assertThat(nodes).isNotNull();
        for (Node node : nodes) {
            Address address = new Address(node.host(), node.port());
            assertThat(address).isIn(addresses);
        }
        assertThat(nodes.size()).isEqualTo(addresses.size());

    }

    @Test
    public void testCreateAndDeleteTopics() throws ExecutionException, InterruptedException {
        String topic = "test-topic";
        CreateTopicsResult result = adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        assertThat(result.numPartitions(topic).get()).isEqualTo(1);
        assertThat(result.topicId(topic).get()).isNotNull();
        assertThat(result.replicationFactor(topic).get()).isEqualTo((short) 1);
        assertThat(result.values().size()).isEqualTo(1);

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(List.of(topic));
        assertThat(deleteTopicsResult.topicNameValues().size()).isEqualTo(1);

        //sleep to allow the topic to be deleted
        Thread.sleep(1000);

        //Try to find if it is gone
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(List.of(topic));

        try {
            describeTopicsResult.topicNameValues().get(topic).get();
            throw new AssertionError("Topic should not exist");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(UnknownTopicOrPartitionException.class);
        }

    }

    @Test
    public void consumeProduce() throws ExecutionException, InterruptedException {
        String topic = "consumeProduce";
        try {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                LOGGER.info("Topic already exists");
            } else {
                throw e;
            }
        }

        RecordMetadata data = producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "test", "test")).get();
        assertThat(data.hasOffset()).isTrue();

        consumer.assign(List.of(new org.apache.kafka.common.TopicPartition(topic, data.partition())));
        consumer.seek(new org.apache.kafka.common.TopicPartition(topic, data.partition()), data.offset());
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        assertThat(records.count()).isEqualTo(1);

        adminClient.deleteTopics(List.of(topic)).all().get();
    }

    @Test
    public void multipleConsumerAndProducers() throws InterruptedException, ExecutionException {
        String topic = "multipleConsumerAndProducers";
        try {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                LOGGER.info("Topic already exists");
            } else {
                throw e;
            }
        }

        Thread.sleep(3000);
        List<KafkaProducer<String,String>> producers = new ArrayList<>();
        List<KafkaConsumer<String,String>> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Properties pp = (Properties) producerProps.clone();
            pp.put("client.id", "producer-" + i);
            KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
            producers.add(producer);

            Properties cp = (Properties) consumerProps.clone();
            cp.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + i);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + i);
            cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
            consumers.add(consumer);
        }

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (KafkaProducer<String, String> producer : producers) {
            futures.add(
                    producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "test", "test")));
        }

        List<RecordMetadata>  metadata = futures.stream().map(f -> {
            try {
                return f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        for (RecordMetadata recordMetadata : metadata) {
            LOGGER.info("RecordMetadata: {}", recordMetadata.toString());
        }
        metadata.stream().map(RecordMetadata::offset).forEach(offset -> assertThat(offset).isNotNull());

        for (KafkaConsumer<String, String> consumer : consumers) {
            consumer.subscribe(List.of(topic));
        }

       // sleep for some time
        for (KafkaConsumer<String, String> consumer : consumers) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (int i = 0; i < 10; i++) {
                if (!records.isEmpty()) {

                    break;
                }
                LOGGER.info("records are empty trying again");
                records = consumer.poll(Duration.ofMillis(10000));
            }

            assertThat(records.count()).isEqualTo(3);
        }

        for (KafkaProducer<String, String> producer : producers) {
            producer.close();
        }

        for (KafkaConsumer<String, String> consumer : consumers) {
            consumer.close();
        }

    }

    @Test
    public void testServiceFunctionality() {
        short _version = 2;

        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, _version, "test", 1);

        ApiVersionsRequestData data = new ApiVersionsRequestData();
        data.setClientSoftwareName("test");
        data.setClientSoftwareVersion("1.0");

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(data, _version);
        assert apiVersionsRequest.isValid();

        ByteBuffer buffer = TestUtils.getRequest(header, apiVersionsRequest);

        AbstractResponse response = TestUtils.sendAndGetResponse(header, buffer, KAFKA_PROXY_HOST, KAFKA_PROXY_PORT);

        assertThat(response).isNotNull();
        assertThat(response.errorCounts().get(Errors.NONE)).isNotNull();

        ApiVersionsResponse apiVersionsResponse = (ApiVersionsResponse) response;
        ApiVersionsResponseData.ApiVersionCollection coll = apiVersionsResponse.data().apiKeys();
        assertThat(coll).isNotNull();
        assertThat(coll.size()).isGreaterThan(0);
    }

    @Test
    public void testMetaDataTransformation() throws InterruptedException, ExecutionException {
        short _version = 2;
        String topicName = "testMetaDataTransformation";
        try {
            adminClient.createTopics(List.of(new NewTopic(topicName, 3, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                LOGGER.info("Topic already exists");
            } else {
                throw e;
            }
        }

        RequestHeader header = new RequestHeader(ApiKeys.METADATA, _version, "test", 1);

        MetadataRequestData data = new MetadataRequestData();
        data.setAllowAutoTopicCreation(true);
        MetadataRequestData.MetadataRequestTopic topic = new MetadataRequestData.MetadataRequestTopic();
        topic.setName(topicName);
        data.setTopics(List.of(topic));

        MetadataRequest metadataRequest = new MetadataRequest(data, _version);

        ByteBuffer buffer = TestUtils.getRequest(header, metadataRequest);

        AbstractResponse response = TestUtils.sendAndGetResponse(header, buffer, KAFKA_PROXY_HOST, KAFKA_PROXY_PORT);

        assertThat(response).isNotNull();
        Map<Errors, Integer> errors = response.errorCounts();
        assertThat(errors.get(Errors.NONE)).isNotNull();

        MetadataResponseData metadataResponseData = (MetadataResponseData) response.data();

        int i = 0;
        for (MetadataResponseData.MetadataResponseBroker broker : metadataResponseData.brokers()) {
            assertThat(broker.host() + ":" + broker.port()).isEqualTo("localhost:" + (KAFKA_PROXY_PORT + i++));
        }

        assertThat(metadataResponseData.brokers().size()).isEqualTo(3);

    }



}
