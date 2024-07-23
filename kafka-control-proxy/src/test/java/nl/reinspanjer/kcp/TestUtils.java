package nl.reinspanjer.kcp;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.junit5.VertxTestContext;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.LogConfigInterface;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtils {
    public static SmallRyeConfig loadConfigWithOverwrite(Map<String, String> m) throws IOException {

        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .addDefaultSources()
                .withSources(new PropertiesConfigSource(m, "overwrite_values", 100000))
                .withMapping(ApplicationConfig.class).withMapping(LogConfigInterface.class)
                .build();

        return config;
    }

    public static ByteBuffer getRequest(RequestHeader header, AbstractRequest request) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        MessageSizeAccumulator messageSize = new MessageSizeAccumulator();
        header.data().addSize(messageSize, serializationCache, header.headerVersion());
        request.data().addSize(messageSize, serializationCache, header.apiVersion());

        ByteBuffer buffer = ByteBuffer.allocate(messageSize.sizeExcludingZeroCopy() + 4);
        ByteBufferAccessor builder = new ByteBufferAccessor(buffer);
        builder.writeInt(messageSize.totalSize());
        builder.writeByteBuffer(request.serializeWithHeader(header));
        assertThat(builder.remaining()).isEqualTo(0);
        builder.flip();
        return buffer;
    }

    public static SSLContext createSSLContext(String fl, String password) {
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(fl), password.toCharArray());

            // Create key manager
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, password.toCharArray());
            KeyManager[] km = keyManagerFactory.getKeyManagers();

            // Create trust manager
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
            trustManagerFactory.init(keyStore);
            TrustManager[] tm = trustManagerFactory.getTrustManagers();

            // Initialize SSLContext
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(km, tm, null);
            return sslContext;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static AbstractResponse sendAndGetResponseSSL(NetClient brokerClient, RequestHeader header, ByteBuffer buffer, String host, int port) throws Throwable {
        VertxTestContext testContext = new VertxTestContext();
        Promise<AbstractResponse> ab = Promise.promise();
        try {
            Future<NetSocket> socketFut = brokerClient.connect(port, host);
            socketFut.onSuccess(
                    socket -> {

                        socket.write(Buffer.buffer(buffer.array()));
                        socket.handler(buffer1 -> {
                            ByteBuffer responseSize = ByteBuffer.wrap(buffer1.getBytes(0, 4));
                            int size = responseSize.order(ByteOrder.BIG_ENDIAN).getInt();
                            ByteBuffer response = ByteBuffer.wrap(buffer1.getBytes(4, size + 4));
                            ab.complete(AbstractResponse.parseResponse(response, header));
                            testContext.completeNow();
                        });
                    }
            ).onFailure(
                    testContext::failNow
            );
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        if (testContext.failed()) {
            throw testContext.causeOfFailure();
        }
        return ab.future().result();
    }

    public static AbstractResponse sendAndGetResponse(RequestHeader header, ByteBuffer buffer, String host, int port) {

        try (Socket socket = new Socket(host, port);
             OutputStream out = socket.getOutputStream();
             InputStream in = socket.getInputStream()) {

            // Send binary data to server
            out.write(buffer.array());
            out.flush();  // Ensure data is sent

            // Wait for response
            byte[] responseSize = new byte[4];  // Adjust size according to expected response
            in.read(responseSize);

            //get size of the message
            int size = ByteBuffer.wrap(responseSize).order(ByteOrder.BIG_ENDIAN).getInt();


            byte[] response = new byte[size];
            int bytesRead = in.read(response);
            assert bytesRead == size;

            ByteBuffer responseBuffer = ByteBuffer.wrap(response);

            AbstractResponse res = AbstractResponse.parseResponse(responseBuffer, header);

            return res;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
}
