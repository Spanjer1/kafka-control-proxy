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

package nl.reinspanjer.kcp.examples.crypto;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.KafkaControlProxy;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.examples.CryptoControl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static nl.reinspanjer.kcp.examples.crypto.KafkaClient.getAdminClient;
import static nl.reinspanjer.kcp.examples.crypto.KafkaClient.pollTillResult;
import static org.assertj.core.api.Assertions.assertThat;

public class CryptoControlIntegration {
    private String PROXY_BOOTSTRAP = "localhost:8888";
    private String KAFKA_BOOTSTRAP = "localhost:9092";

    private static DockerComposeContainer container;
    String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19
                        }
                    }
                }
                """;

    @BeforeAll
    public static void start() throws Throwable {
        container = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                .withExposedService("broker-1", 9092, Wait.forListeningPort());
        container.start();

        Vertx vertx = Vertx.vertx();
        VertxTestContext testContext = new VertxTestContext();
        NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE, ApiKeys.FETCH), new CryptoControl().init(vertx));
        KafkaControlProxy.deploy(vertx).onComplete(testContext.succeedingThenComplete());
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        String scope = "scope-a";
        String user = "user-a";
        String password = user + "-password";
        String topic = "test-topic";

        AdminClient admin = getAdminClient(KAFKA_BOOTSTRAP, "admin", "admin-password");
        KafkaClient.createTopic(admin, topic, 1, (short) 1);


        FieldEncryptionConfigInterfaceTest config = new FieldEncryptionConfigInterfaceTest();

        RuleTest rule = new RuleTest("$.store.book[*].title", scope);
        config.addRule(topic, rule);

        GroupTest group = new GroupTest("group-a", List.of(user), List.of(scope));
        config.addGroup(group);

        CryptoControl.config = config;

        KafkaProducer<String, String> producer = KafkaClient.getKafkaProducer(PROXY_BOOTSTRAP, user, password);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonString);
        RecordMetadata data = producer.send(record).get();

        KafkaConsumer<String, String> consumer = KafkaClient.getKafkaConsumer(KAFKA_BOOTSTRAP, user, password, "group-a");

        Thread.sleep(1000);
        consumer.subscribe(List.of(topic));
        ConsumerRecords<String, String> records = pollTillResult(consumer, Duration.ofSeconds(5), 10, 1, topic).get(0);

        assertThat(records.count()).isEqualTo(1);
        String value = records.records(topic).iterator().next().value();

        assertThat(value).isNotEqualTo(jsonString);

        KafkaConsumer<String, String> consumerProxy = KafkaClient.getKafkaConsumer(PROXY_BOOTSTRAP, user, password, "group-b");

        consumerProxy.subscribe(List.of(topic));
        ConsumerRecords<String, String> proxyRecords = pollTillResult(consumerProxy, Duration.ofSeconds(5), 10, 1, topic).get(0);
        String proxyValue = proxyRecords.records(topic).iterator().next().value();
        assertThat(proxyValue).isEqualTo(value);

        consumer.close();
        consumerProxy.close();
    }



}
