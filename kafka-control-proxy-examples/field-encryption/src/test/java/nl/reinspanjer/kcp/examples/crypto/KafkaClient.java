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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaClient {

    public static KafkaProducer<String, String> getKafkaProducer(String bootstrap, String userName, String password) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("acks", "all");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName + "\" password=\"" + password + "\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> getKafkaConsumer(String bootstrap, String userName, String password, String groupId){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("security.protocol", "SASL_PLAINTEXT");

        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName + "\" password=\"" + password + "\";");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("group.id", groupId);
        return new KafkaConsumer<>(props);
    }

    public static AdminClient getAdminClient(String bootstrap, String userName, String password){
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName + "\" password=\"" + password + "\";");
        return AdminClient.create(props);
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
        return recordsList;
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

    public static void createTopic(AdminClient adminClient, String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
        adminClient.createTopics(List.of(new NewTopic(topicName, partitions, replicationFactor))).all().get();
    }
}
