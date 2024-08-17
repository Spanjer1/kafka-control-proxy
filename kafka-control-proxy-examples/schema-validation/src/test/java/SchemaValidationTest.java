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

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.KafkaControlProxy;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.examples.SchemaValidationControl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaValidationTest {

    private static DockerComposeContainer container;

    @BeforeAll
    public static void start() throws Throwable {
        container = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                .withExposedService("broker-1", 9092, Wait.forListeningPort());
        container.start();

        Vertx vertx = Vertx.vertx();
        VertxTestContext testContext = new VertxTestContext();
        NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE, ApiKeys.FETCH), new SchemaValidationControl().init(vertx));
        KafkaControlProxy.deploy(vertx).onComplete(testContext.succeedingThenComplete());

        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();

        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
    }

}
