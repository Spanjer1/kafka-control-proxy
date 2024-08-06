package nl.reinspanjer.kcp.examples;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FireWallNodeTest {

    private static DockerComposeContainer container;
    private static GenericContainer consumerKcp;
    private static GenericContainer producerKcp;
    private static DockerClient dockerClient;

    @BeforeAll
    public static void start() throws Throwable {
        dockerClient = createDockerClient();
        createOrReuseNetwork(dockerClient);

        consumerKcp = createKcpContainer();
        producerKcp = createKcpContainer();

        container = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                .withExposedService("broker-1", 9092, Wait.forListeningPort());
        container.start();

        String consumerkCatIpAddress = consumerKcp.getContainerInfo().getNetworkSettings().getNetworks()
                .get("kcp-firewall").getIpAddress();

        String producerKcpIpAddress = producerKcp.getContainerInfo().getNetworkSettings().getNetworks()
                .get("kcp-firewall").getIpAddress();

        Map<String, String> config = Map.of(
                "PROXY_SERVER_HOST", "kcp",
                "PROXY_SERVER_PORT", "" + 8888,
                "PROXY_ORIGIN_BROKERS_0__HOST", "broker-1",
                "PROXY_ORIGIN_BROKERS_0__PORT", "" + 9092,
                "PROXY_LOG_NL_REINSPANJER", "DEBUG",
                "FIREWALL_ADMIN", "10.0.0.0/8",
                "FIREWALL_PRODUCER", producerKcpIpAddress,
                "FIREWALL_CONSUMER", consumerkCatIpAddress
        );

        MountableFile app = MountableFile.forHostPath("target/firewall-999-SNAPSHOT-fat.jar");

        GenericContainer kcpContainer = new GenericContainer<>("openjdk:17-slim")
                .withCopyFileToContainer(app, "/app/app.jar")
                .withCommand("java", "-jar", "/app/app.jar")
                .withNetwork(new org.testcontainers.containers.Network() {
                    @Override
                    public Statement apply(Statement base, Description description) {
                        return base;
                    }

                    public String getId() {
                        return "kcp-firewall";
                    }

                    public void close() { /* NOOP */ }
                })
                .withNetworkAliases("kcp")
                .withExposedPorts(8888)
                .withEnv(config);
        kcpContainer.start();

    }

    private static GenericContainer createKcpContainer() {
        GenericContainer kcp = new GenericContainer<>(DockerImageName.parse("edenhill/kcat:1.7.1"))
                .withNetworkMode("kcp-firewall")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(""))
                .withCommand("sleep", "10000");
        kcp.start();
        return kcp;
    }

    @AfterAll
    public static void stop() {
        container.stop();
        deleteNetwork(dockerClient, "kcp-firewall");
    }

    public static DockerClient createDockerClient() {

        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost("unix:///var/run/docker.sock")
                .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();

        return DockerClientBuilder.getInstance(config)
                .withDockerHttpClient(httpClient)
                .build();

    }

    public static void createOrReuseNetwork(DockerClient client) {
        List<Network> network = client.listNetworksCmd().withNameFilter("kcp-firewall").exec();
        if (network.isEmpty()) {
            client.createNetworkCmd().withName("kcp-firewall").exec();
        }
    }

    public static void deleteNetwork(DockerClient client, String networkName) {
        List<Network> network = client.listNetworksCmd().withNameFilter("kcp-firewall").exec();
        if (!network.isEmpty()) {
            try {
                client.removeNetworkCmd(networkName).exec();
            } catch (Exception e) {
                // ignore
            }

        }
    }

    @Test
    public void testAllowedMetaData() throws IOException, InterruptedException {
        Container.ExecResult result = consumerKcp.execInContainer("kcat", "-b", "kcp:8888", "-L");
        assertThat(result.getStdout()).isNotEmpty();
        assertThat(result.getStderr()).isEmpty();
        Container.ExecResult result2 = producerKcp.execInContainer("kcat", "-b", "kcp:8888", "-L");
        assertThat(result2.getStdout()).isNotEmpty();
        assertThat(result2.getStderr()).isEmpty();
    }

    @Test
    public void testAllowedProducer() throws IOException, InterruptedException {
        Container.ExecResult result = producerKcp.execInContainer("sh", "-c", "echo key1=value1 | kcat -b  kcp:8888 -t testAllowedConsumeAndProduce -P");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).isEmpty(); //when producing no output is expected
        assertThat(result.getStderr()).isEmpty();
    }

    @Test
    public void testAllowedConsumeAndProduce() throws IOException, InterruptedException {
        Container.ExecResult result = producerKcp.execInContainer("sh", "-c", "echo key1=value1 | kcat -b  kcp:8888 -t testAllowedConsumeAndProduce -P");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).isEmpty();
        assertThat(result.getStderr()).isEmpty();

        Container.ExecResult result2 = consumerKcp.execInContainer("sh", "-c", "kcat -b kcp:8888 -t testAllowedConsumeAndProduce -C -c 1");
        assertThat(result2.getExitCode()).isEqualTo(0);
        assertThat(result2.getStdout()).isNotEmpty();
        assertThat(result2.getStderr()).isEmpty();
    }

    @Test
    public void testNotAllowedProducer() throws IOException, InterruptedException {
        Container.ExecResult result = consumerKcp.execInContainer("sh", "-c", "echo key1=value1 | kcat -b  kcp:8888 -t testAllowedConsumeAndProduce -P");
        assertThat(result.getExitCode()).isNotEqualTo(0);
        assertThat(result.getStdout()).isEmpty();
        assertThat(result.getStderr()).isNotEmpty();
    }

    @Test
    public void testNotAllowedConsuming() throws IOException, InterruptedException {
        Container.ExecResult result = producerKcp.execInContainer("sh", "-c", "echo key1=value1 | kcat -b  kcp:8888 -t testNotAllowedConsuming -P");
        assertThat(result.getExitCode()).isEqualTo(0);
        assertThat(result.getStdout()).isEmpty();
        assertThat(result.getStderr()).isEmpty();

        Container.ExecResult result2 = producerKcp.execInContainer("sh", "-c", "kcat -b kcp:8888 -t testNotAllowedConsuming -C -c 1");
        assertThat(result2.getExitCode()).isNotEqualTo(0);
        assertThat(result2.getStdout()).isEmpty();
        assertThat(result2.getStderr()).isNotEmpty();
    }

}