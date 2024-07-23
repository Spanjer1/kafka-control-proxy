package nl.reinspanjer.kcp;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.control.ObserverNode;
import nl.reinspanjer.kcp.impl.ObserverTestNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ObserverNodesTest {

    private static final ObserverTestNode observerTestNode = new ObserverTestNode();
    static Vertx vertx = Vertx.vertx();
    static Logger LOGGER = LoggerFactory.getLogger(ObserverNodesTest.class);
    private static final Integer KAFKA_PROXY_PORT = 8888;
    private static final KafkaUtils utils = KafkaUtils.getInstance();
    @ClassRule
    public static DockerComposeContainer container = utils.register();

    @AfterAll
    static void stop() {
        utils.unregister();
    }

    @BeforeAll
    static void prepare() throws Throwable {
        KCPUtils.start(KAFKA_PROXY_PORT, vertx);
        NodeRegistrator.registerNode(List.of(ApiKeys.values()), observerTestNode);
    }

    @Test
    public void testTimeNotImpactedRequest() throws ExecutionException, InterruptedException {
        String topic = "testTimeNotImpactedRequest";
        Function<RequestHeaderAndPayload, Future<Void>> requestHandler = (r) -> {
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(10000, id -> promise.complete());
            return promise.future();
        };

        observerTestNode.setRequestHandler(requestHandler);
        long current = System.currentTimeMillis();
        utils.createTopic(topic, 1, (short) 1);
        utils.send(topic, "", "" + current);
        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofSeconds(5), 2, 1);
        ConsumerRecord<String, String> res = records.get(0).iterator().next();
        assertThat(res.value()).isEqualTo("" + current);
        long diff = System.currentTimeMillis() - current;
        assertThat(diff).isLessThan(10000);

    }

    @Test
    public void testTimeNotImpactedResponse() throws ExecutionException, InterruptedException {
        String topic = "testTimeNotImpactedResponse";
        Function<RequestHeaderAndPayload, Future<Void>> responseHandler = (r) -> {
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(10000, id -> promise.complete());
            return promise.future();
        };

        observerTestNode.setResponseHandler(responseHandler);
        long current = System.currentTimeMillis();
        utils.createTopic(topic, 1, (short) 1);
        utils.send(topic, "", "" + current);
        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofSeconds(5), 2, 1);
        ConsumerRecord<String, String> res = records.get(0).iterator().next();
        assertThat(res.value()).isEqualTo("" + current);
        long diff = System.currentTimeMillis() - current;
        assertThat(diff).isLessThan(10000);

    }

    @Test
    public void testTimeNotImpactedMultiple() throws ExecutionException, InterruptedException {
        NodeRegistrator.removeNode(observerTestNode);

        String topic = "testTimeNotImpactedMultiple";
        AtomicInteger counter = new AtomicInteger(0);

        Function<RequestHeaderAndPayload, Future<Void>> handler = (r) -> {
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(10000, id -> {
                counter.incrementAndGet();
                promise.complete();
            });
            return promise.future();
        };

        observerTestNode.setResponseHandler(handler);
        observerTestNode.setRequestHandler(handler);

        //clone and register multiple observernodes
        for (int i = 0; i < 10; i++) {
            ObserverNode clone = observerTestNode.clone();
            NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE), clone);
        }

        long current = System.currentTimeMillis();
        utils.createTopic(topic, 1, (short) 1);
        utils.send(topic, "", "" + current);
        List<ConsumerRecords<String, String>> records = utils.receive(topic, Duration.ofSeconds(5), 2, 1);
        ConsumerRecord<String, String> res = records.get(0).iterator().next();
        assertThat(res.value()).isEqualTo("" + current);

        VertxTestContext testContext = new VertxTestContext();
        vertx.setTimer(15000, id -> {
            testContext.completeNow();
        });

        assertThat(testContext.awaitCompletion(16, TimeUnit.SECONDS)).isTrue();
        assertThat(counter.get()).isEqualTo(20); // request and response handler

    }


}
