package nl.reinspanjer.kcp;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.impl.DecisionTestNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class DecisionNodesTests {
    private static final DecisionTestNode decisionTestNode = new DecisionTestNode();
    static Integer KAFKA_PROXY_PORT = 8888;
    static Vertx vertx = Vertx.vertx();
    static KafkaUtils utils = KafkaUtils.getInstance();

    @ClassRule
    public static DockerComposeContainer environment = utils.register();

    @AfterAll
    static void stop() {
        utils.unregister();
    }

    @BeforeAll
    static void prepare() throws Throwable {
        KCPUtils.start(KAFKA_PROXY_PORT, vertx);
        NodeRegistrator.registerNode(List.of(ApiKeys.PRODUCE, ApiKeys.FETCH), decisionTestNode);
    }

    @Test
    public void testTwoTopicsOneBlock() throws ExecutionException, InterruptedException {
        String topic = "testTwoTopicsOneBlock";
        String blockTopic = "testTwoTopicsOneBlock2";

        Function<RequestHeaderAndPayload, Future<Boolean>> f = (r) -> {
            if (r.request.apiKey() == ApiKeys.PRODUCE) {
                ProduceRequestData produceRequestData = (ProduceRequestData) r.request.data();
                Boolean res = produceRequestData.topicData().stream().noneMatch(topicData -> topicData.name().equals(blockTopic));
                return Future.succeededFuture(res);
            } else {
                return Future.succeededFuture(true);
            }
        };
        decisionTestNode.setRequestHandler(f);

        utils.createTopic(topic, 1, (short) 1);
        utils.createTopic(blockTopic, 1, (short) 1);
        utils.send(topic, "key", "");

        try {
            utils.send(blockTopic, "key", "");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(ExecutionException.class);
        }
    }


}
