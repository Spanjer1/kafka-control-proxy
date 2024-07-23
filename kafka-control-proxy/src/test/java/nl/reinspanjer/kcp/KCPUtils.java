package nl.reinspanjer.kcp;

import io.smallrye.config.SmallRyeConfig;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.config.LogConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.control.NodeRegistrator;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static nl.reinspanjer.kcp.KafkaControlProxy.registerNodes;
import static org.assertj.core.api.Assertions.assertThat;

public class KCPUtils {

    public static boolean initialized = false;

    public static void start(int port, Vertx vertx) throws Throwable {
        if (initialized) {
            NodeRegistrator.clearNodes();
            registerNodes(vertx);
            return;
        }
        VertxTestContext testContext = new VertxTestContext();
        Map<String, String> overwrite = Map.of(
                "proxy.server.host", "localhost",
                "proxy.server.port", "" + port,
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
        initialized = true;
    }


}
