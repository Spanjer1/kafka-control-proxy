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

package nl.reinspanjer.kafkacontrolproxy.verticles;

import io.vertx.core.*;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.SocketAddress;
import nl.reinspanjer.kafkacontrolproxy.config.ApplicationConfig;
import nl.reinspanjer.kafkacontrolproxy.config.ProxyConfig;
import nl.reinspanjer.kafkacontrolproxy.data.Address;
import nl.reinspanjer.kafkacontrolproxy.data.BrokerOriginMap;
import nl.reinspanjer.kafkacontrolproxy.handler.ConnectHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;

/*
 * This class is responsible for creating a server that listens on a specific port and forwards the incoming requests to the appropriate broker.
 * For every broker in the Kafka cluster, a server is created.
 */
public class Server extends AbstractVerticle {

    public static final String BINDING_ADDRESS = "0.0.0.0";
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    private final Integer proxyPort;
    private final BrokerOriginMap brokerOriginMap = BrokerOriginMap.build();
    private final Promise<String> listening = Promise.promise();
    private ApplicationConfig config;
    private Vertx vertx;
    private ConnectHandler connectHandler;

    public Server(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    @Override
    public void init(Vertx vertx, Context context) {
        this.config = ProxyConfig.build();
        this.vertx = vertx;
    }

    @Override
    public void start() {

        SocketAddress address = SocketAddress.inetSocketAddress(proxyPort, BINDING_ADDRESS);
        NetServerOptions options = new NetServerOptions();
        if (this.config.server().ssl()) {
            options.setSsl(true);
            options.setKeyCertOptions(new JksOptions()
                    .setPath(config.server().keystorePath().orElseThrow())
                    .setPassword(config.server().keystorePassword().orElseThrow()));
        }

        LOGGER.debug("{}", options.toJson());

        this.connectHandler = new ConnectHandler(this.vertx, this.proxyPort);

        vertx.createNetServer(options).connectHandler(this.connectHandler)
                .exceptionHandler(ex -> {
                    // The healthcheck will close the connection, so we don't need to log this
                    if (ex instanceof ClosedChannelException) {
                        LOGGER.debug("Connection closed by client");
                    } else {
                        LOGGER.error("Failed to connect to remote server: " + ex.getMessage());
                    }

                }).listen(address, result -> {
                            if (result.succeeded()) {
                                LOGGER.info("Listening on " + address.host() + ":" + address.port() + " -> origin broker " +
                                        brokerOriginMap.getBrokerFromServer(new Address(config.server().host(), address.port())));
                                listening.complete("success");
                            } else {
                                LOGGER.error("Failed to bind to local port: {} {}", address.hostAddress(), result.cause().getMessage());
                                listening.fail("Failed to bind to local port: " + address.hostAddress() + " " + result.cause().getMessage());
                                vertx.close();
                            }
                        }
                );

    }

    @Override
    public void stop() {
        LOGGER.info("Server stopped");
        connectHandler.close().onComplete(ar -> {
            if (ar.succeeded()) {
                LOGGER.info("All connections closed");
            } else {
                LOGGER.error("Failed to close connections", ar.cause());
            }
        });
    }

    public Future<String> listening() {
        return this.listening.future();
    }
}
