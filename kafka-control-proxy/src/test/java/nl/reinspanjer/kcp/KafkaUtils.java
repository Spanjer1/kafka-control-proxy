package nl.reinspanjer.kcp;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {
    static Integer KAFKA_PROXY_PORT = 8888;
    static String KAFKA_PROXY_HOST = "localhost";
    static Properties producerProps = new Properties();
    static Properties consumerProps = new Properties();
    static int inUse = 0;
    private static DockerComposeContainer container;
    private static KafkaUtils instance;
    private final AdminClient adminClient;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    public KafkaUtils(AdminClient adminClient, KafkaProducer<String, String> kafkaProducer, KafkaConsumer<String, String> kafkaConsumer) {
        this.adminClient = adminClient;
        this.producer = kafkaProducer;
        this.consumer = kafkaConsumer;
    }


    public static KafkaUtils getInstance() {
        if (instance == null) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_PROXY_HOST + ":" + KAFKA_PROXY_PORT + "," +
                    KAFKA_PROXY_HOST + ":" + (KAFKA_PROXY_PORT + 1) + "," +
                    KAFKA_PROXY_HOST + ":" + (KAFKA_PROXY_PORT + 2));

            producerProps = (Properties) props.clone();
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            consumerProps = (Properties) props.clone();
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("group.id", "test-group");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            instance = new KafkaUtils(AdminClient.create(props), new KafkaProducer<>(producerProps), new KafkaConsumer<>(consumerProps));
        }
        return instance;
    }

    public static List<ConsumerRecords<String, String>> pollTillResult(KafkaConsumer<String, String> consumer, Duration pollingTime, int times, int expected, String topic) {
        List<ConsumerRecords<String, String>> recordsList = new ArrayList<>();
        int received = 0;
        for (int i = 0; i < times; i++) {
            var records = consumer.poll(pollingTime);
            if (!records.isEmpty()) {
                recordsList.add(records);
                received += getSizeOfConsumerRecord(records, topic);
            }
            if (received >= expected) {
                return recordsList;
            }
        }
        return null;
    }

    public static Integer getSizeOfConsumerRecord(ConsumerRecords<String, String> records, String topic) {
        Iterator<ConsumerRecord<String, String>> it = records.records(topic).iterator();
        int length = 0;
        while (it.hasNext()) {
            it.next();
            length += 1;
        }
        return length;
    }

    public DockerComposeContainer register() {
        inUse++;
        if (container != null) {
            return container;
        } else {
            container = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                    .withExposedService("broker-1", 9092, Wait.forListeningPort())
                    .withExposedService("broker-2", 9092, Wait.forListeningPort())
                    .withExposedService("broker-3", 9092, Wait.forListeningPort());
            container.start();

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return container;
    }

    public void unregister() {
        inUse--;
        consumer.unsubscribe();
    }

    public void createTopic(String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        adminClient.createTopics(List.of(new NewTopic(topicName, partitions, replicationFactor))).all().get();
    }

    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        adminClient.deleteTopics(List.of(topicName)).all().get();
    }

    public RecordMetadata send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        return producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value)).get();
    }

    public List<ConsumerRecords<String, String>> receive(String topic, Duration pollTime, int times, int expected) {
        consumer.subscribe(List.of(topic));
        return pollTillResult(consumer, pollTime, times, expected, topic);
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public List<ConsumerRecord<String, String>> flattenRecords(List<ConsumerRecords<String, String>> recordsList) {
        List<ConsumerRecord<String, String>> flatList = new ArrayList<>();
        for (ConsumerRecords<String, String> records : recordsList) {
            for (ConsumerRecord<String, String> record : records) {
                flatList.add(record);
            }
        }

        return flatList;
    }


}
