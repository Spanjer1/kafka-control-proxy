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

package nl.reinspanjer.kcp.connector;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import nl.reinspanjer.kcp.admin.Broker;
import nl.reinspanjer.kcp.admin.BrokerEntry;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.utils.Helper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdmin.class);
    private static ApplicationConfig config = ProxyConfig.build();
    private Vertx vertx;
    private AdminClient adminClient;

    public KafkaAdmin(Vertx vertx, AdminClient adminClient) {
        this.vertx = vertx;
        this.adminClient = adminClient;
    }

    public static KafkaAdmin create(Vertx vertx) {
        Map<String, Object> kafkaConfig = new HashMap<>();
        ApplicationConfig.Origin.Admin admin = config.origin().admin().orElseThrow(()
                -> new RuntimeException("Admin config not set"));

        kafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, admin.bootstrapHost() + ":" + admin.bootstrapPort());

        if (config.origin().tcpClient().ssl()) {
            kafkaConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            kafkaConfig.put("ssl.truststore.location",
                    config.origin().tcpClient().truststorePath().orElseThrow(() -> new RuntimeException("Truststore path not set")));
            kafkaConfig.put("ssl.truststore.password",
                    config.origin().tcpClient().truststorePassword().orElseThrow(() -> new RuntimeException("Truststore password not set")));
            //set host verify algorithm to null
            kafkaConfig.put("ssl.endpoint.identification.algorithm", "");
        }

        if (admin.sasl()) {
            if (config.origin().tcpClient().ssl()) {
                kafkaConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            } else {
                kafkaConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            }

            kafkaConfig.put("sasl.mechanism", "PLAIN");

            String username = admin.username().orElseThrow(() -> new RuntimeException("Username not set"));
            String password = admin.password().orElseThrow(() -> new RuntimeException("Password not set"));
            kafkaConfig.put("sasl.jaas.config", Helper.buildJaasConfig(username, password));

            LOGGER.info("SASL mechanism {} with username {}", kafkaConfig.get("sasl.mechanism"), username);
        }

        return new KafkaAdmin(vertx, AdminClient.create(new HashMap<>(kafkaConfig)));
    }

    public Future<Broker> getClusterInfo() {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Broker> promise = ctx.promise();

        DescribeClusterResult describeClusterResult = this.adminClient.describeCluster();

        describeClusterResult.nodes().whenComplete((nodes, ex) -> {
            if (ex == null) {
                List<BrokerEntry> entries = nodes.stream().map(
                        node -> new BrokerEntry(
                                String.valueOf(node.id()),
                                node.host(),
                                node.port()
                        )
                ).toList();
                Broker broker = new Broker(entries);
                LOGGER.info("Cluster size: {}", entries.size());
                promise.complete(broker);
            } else {
                LOGGER.error("Failed to get cluster info", ex);
                promise.fail(ex);
            }
        });

        return promise.future();
    }

    public Future<Config> getConfig(String topicName) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Config> promise = ctx.promise();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

        this.adminClient.describeConfigs(Collections.singletonList(
                configResource)).all().whenComplete((m, ex) -> {
            if (ex == null) {
                Config config = m.get(configResource);
                if (config == null) {
                    promise.fail("Config not found");
                } else {
                    promise.complete(config);
                }
                promise.complete(config);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Map<String, Config>> getTopicConfigMap() {
        Future<List<ConfigResource>> fTopic = this.listTopics().map(topicsSet ->
                topicsSet.stream().map(topic ->
                        new ConfigResource(ConfigResource.Type.TOPIC, topic)).collect(Collectors.toList())
        );

        return fTopic.compose(this::describeConfigs).map(configs -> configs.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().name(),
                                Map.Entry::getValue
                        )
                ));
    }

    public Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Map<String, TopicDescription>> promise = ctx.promise();

        DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(topicNames);
        describeTopicsResult.all().whenComplete((t, ex) -> {
            if (ex == null) {
                promise.complete(t);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Map<String, TopicDescription>> describeTopics(List<String> topicNames, DescribeTopicsOptions options) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Map<String, TopicDescription>> promise = ctx.promise();

        DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(topicNames, options);
        describeTopicsResult.all().whenComplete((t, ex) -> {
            if (ex == null) {
                promise.complete(t);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Set<String>> listTopics() {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Set<String>> promise = ctx.promise();

        ListTopicsResult listTopicsResult = this.adminClient.listTopics();
        listTopicsResult.names().whenComplete((topics, ex) -> {
            if (ex == null) {
                promise.complete(topics);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Map<ConfigResource, Config>> describeConfigs(List<ConfigResource> configResources) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Map<ConfigResource, Config>> promise = ctx.promise();

        DescribeConfigsResult describeConfigsResult = this.adminClient.describeConfigs(configResources);
        describeConfigsResult.all().whenComplete((m, ex) -> {

            if (ex == null) {
                promise.complete(m);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Void> alterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<Void> promise = ctx.promise();

        AlterConfigsResult alterConfigsResult = this.adminClient.incrementalAlterConfigs(configs);
        alterConfigsResult.all().whenComplete((v, ex) -> {

            if (ex == null) {
                promise.complete();
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<List<AclBinding>> describeAcls(AclBindingFilter aclBindingFilter) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<List<AclBinding>> promise = ctx.promise();

        DescribeAclsResult describeAclsResult = this.adminClient.describeAcls(aclBindingFilter);
        describeAclsResult.values().whenComplete((o, ex) -> {
            if (ex == null) {
                List list = o.stream().map(entry -> new AclBinding(entry.pattern(), entry.entry())).collect(Collectors.toList());
                promise.complete(list);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<List<AclBinding>> createAcls(List<AclBinding> aclBindings) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<List<AclBinding>> promise = ctx.promise();

        CreateAclsResult createAclsResult = this.adminClient.createAcls(aclBindings);
        createAclsResult.all().whenComplete((o, ex) -> {
            if (ex == null) {
                promise.complete();
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<List<AclBinding>> deleteAcls(List<AclBindingFilter> aclBindingsFilters) {
        ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
        Promise<List<AclBinding>> promise = ctx.promise();

        DeleteAclsResult deleteAclsResult = this.adminClient.deleteAcls(aclBindingsFilters);
        deleteAclsResult.all().whenComplete((o, ex) -> {
            if (ex == null) {
                promise.complete((List<AclBinding>) o);
            } else {
                promise.fail(ex);
            }
        });
        return promise.future();
    }

    public Future<Void> close() {
        return close(0);
    }

    public Future<Void> close(long timeout) {
        return vertx.executeBlocking(() -> {
            if (timeout > 0) {
                adminClient.close(Duration.ofMillis(timeout));
            } else {
                adminClient.close();
            }
            return null;
        });
    }
}
