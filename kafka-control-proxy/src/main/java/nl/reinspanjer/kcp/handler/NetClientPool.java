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

package nl.reinspanjer.kcp.handler;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.net.KeyStoreOptions;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class NetClientPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetClientPool.class);
    private final NetClientOptions options;
    private final Map<String, NetSocket> pool = new HashMap<>();
    private final Queue<NetSocket> pending = new LinkedList<>();
    private final Vertx vertx;
    private final String targetHost;
    private final Integer targetPort;
    ApplicationConfig config = ProxyConfig.build();

    public NetClientPool(Vertx vertx, String targetHost, Integer targetPort) {
        this.vertx = vertx;
        this.targetHost = targetHost;
        this.targetPort = targetPort;

        options = new NetClientOptions()
                .setTcpNoDelay(true)
                .setTcpFastOpen(true)
                .setTcpQuickAck(true)
                .setReconnectAttempts(3)
                .setConnectTimeout(5000);

        if (this.config.origin().tcpClient().ssl() && !this.config.origin().tcpClient().trustAllCertificates()) {
            options.setSsl(true);

            options.setTrustOptions(new KeyStoreOptions()
                    .setPath(this.config.origin().tcpClient().truststorePath()
                            .orElseThrow(() -> new RuntimeException("truststorePath is not set")))
                    .setPassword(this.config.origin().tcpClient().truststorePassword()
                            .orElseThrow(() -> new RuntimeException("truststorePassword is not set")))
                    .setType("JKS")
            );
            options.setHostnameVerificationAlgorithm("");

        } else if (this.config.origin().tcpClient().ssl() && this.config.origin().tcpClient().trustAllCertificates()) {
            options.setSsl(true);
            options.setTrustAll(true);
            options.setHostnameVerificationAlgorithm("");
        }

    }

    public Future<Void> add() {
        NetClient client = this.vertx.createNetClient(options);
        Promise<Void> promise = Promise.promise();

        client.connect(this.targetPort, this.targetHost, res -> {
            if (res.succeeded()) {
                LOGGER.debug("Connected to {}:{}", this.targetHost, this.targetPort);
                res.result().closeHandler(v -> {
                    LOGGER.error("Connection closed {} {}", this.targetHost, this.targetPort);
                });
                pending.add(res.result());
                promise.complete();
            } else {
                LOGGER.error("Failed to connect to {}:{}", this.targetHost, this.targetPort, res.cause());
                promise.fail(res.cause());
            }
        });

        return promise.future();

    }

    public Future<NetSocket> create(NetSocket clientSocket) {
        NetClient client = this.vertx.createNetClient(options);
        Promise<NetSocket> promise = Promise.promise();

        client.connect(this.targetPort, this.targetHost, res -> {
            if (res.succeeded()) {
                LOGGER.debug("Connected to {}:{}", this.targetHost, this.targetPort);
                pool.put(clientSocket.remoteAddress().toString(), res.result());
                promise.complete(res.result());

            } else {
                LOGGER.error("Failed to connect to {}:{}", this.targetHost, this.targetPort, res.cause());
                promise.fail(res.cause());
            }
        });

        return promise.future();

    }

    public Future<NetSocket> get(NetSocket clientSocket) {

        Promise<NetSocket> promise = Promise.promise();

        if (pending.size() < 3) {
            for (int i = 0; i < 1; i++) {
                add();
            }
        }
        NetSocket socket = pending.poll();
        if (socket == null) {
            LOGGER.debug("No socket available, waiting for one to become available");
            add().andThen(r -> {
                if (r.failed()) {
                    LOGGER.debug("Failed to get socket", r.cause());
                    promise.fail(r.cause());
                } else {
                    NetSocket s = pending.poll();
                    if (s == null) {
                        promise.fail("No socket available");
                    } else {
                        promise.complete(s);
                        pool.put(clientSocket.remoteAddress().toString(), socket);
                    }
                }
            });
        } else {
            promise.complete(socket);
            pool.put(clientSocket.remoteAddress().toString(), socket);
        }

        return promise.future();
    }

    public void remove(NetSocket socket) {
        pool.remove(socket.remoteAddress().toString());
    }

    public void drain() {
        LOGGER.debug("Draining pool");
        pool.values().forEach(NetSocket::close);
    }
}
