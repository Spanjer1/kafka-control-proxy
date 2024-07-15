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

package nl.reinspanjer.kafkacontrolproxy.handler;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetSocket;
import nl.reinspanjer.kafkacontrolproxy.config.ApplicationConfig;
import nl.reinspanjer.kafkacontrolproxy.config.ProxyConfig;
import nl.reinspanjer.kafkacontrolproxy.data.Address;
import nl.reinspanjer.kafkacontrolproxy.data.BrokerOriginMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectHandler implements Handler<NetSocket> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectHandler.class);
    private static final BrokerOriginMap configBroker = BrokerOriginMap.build();
    private static final ApplicationConfig config = ProxyConfig.build();
    final Map<NetSocket, BufferHandler> activeHandlers = new HashMap<>();
    private final Vertx vertx;
    private final Integer proxyPort;
    private NetClientPool netClientPool;

    public ConnectHandler(Vertx vertx, Integer proxyPort) {
        this.vertx = vertx;
        this.proxyPort = proxyPort;
        initPool();
    }

    public void initPool() {
        String proxyHost = config.server().host();
        Address server = new Address(proxyHost, proxyPort);
        Address broker = configBroker.getBrokerFromServer(server);
        this.netClientPool = new NetClientPool(this.vertx, broker.getHost(), broker.getPort());
    }

    @Override
    public void handle(NetSocket clientSocket) {
        Context context = this.vertx.getOrCreateContext();
        context.put("ClientSocket", clientSocket);
        context.put("ProxyPort", proxyPort);

        LOGGER.debug("New connection from: {}", clientSocket.remoteAddress().toString());
        clientSocket.pause();
        Future<NetSocket> originSocketFuture = this.netClientPool.create(clientSocket);

        originSocketFuture.onSuccess(
                originSocket -> {
                    SocketPair socketPair = new SocketPair(clientSocket, originSocket, netClientPool);
                    socketPair.pauseClient();
                    BufferHandler bufferHandler = new BufferHandler(this.vertx, socketPair);
                    socketPair.registerRequestHandler(bufferHandler);
                    socketPair.resumeClient();
                }
        ).onFailure(
                e -> {
                    LOGGER.error("Failed to connect to origin broker", e);
                    clientSocket.close();
                }
        );

    }

    public Future<Void> close() {
        LOGGER.info("Closing {} connections", activeHandlers.size());
        List<Future<Void>> fVoids = new ArrayList<>();
        activeHandlers.forEach((k, v) -> {
            fVoids.add(v.close().andThen(x -> k.close()));
        });
        activeHandlers.clear();
        this.netClientPool.drain();
        return Future.all(fVoids).mapEmpty();
    }
}
