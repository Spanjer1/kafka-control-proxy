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

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketPair {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketPair.class);
    private final NetSocket clientSocket;
    private final NetSocket originSocket;
    private boolean clientStatus = false;
    private boolean originStatus = false;

    public SocketPair(NetSocket clientSocket, NetSocket originSocket, NetClientPool pool) {
        this.clientSocket = clientSocket;
        this.originSocket = originSocket;
        clientSocket.closeHandler(v -> {
            LOGGER.debug("Client socket is closed, closing origin");
            LOGGER.debug("Closing sockets {} {}", clientSocket.remoteAddress(), originSocket.remoteAddress());
            this.originSocket.close();
            pool.remove(clientSocket);
        });
        originSocket.closeHandler(v -> {
            LOGGER.debug("Origin socket is closed, closing client");
            LOGGER.debug("Closing sockets {} {}", clientSocket.remoteAddress(), originSocket.remoteAddress());
            this.clientSocket.close();
            pool.remove(clientSocket);
        });
    }

    public void pauseClient() {
        clientStatus = false;
        clientSocket.pause();
    }

    public void resumeClient() {
        clientStatus = true;
        clientSocket.resume();
    }

    public Future<Void> close() {
        LOGGER.debug("Closing sockets {} {}", clientSocket.remoteAddress(), originSocket.remoteAddress());
        return clientSocket.close().andThen(c -> originSocket.close());
    }

    public Future<Void> writeOrigin(Buffer buffer) {
        return originSocket.write(buffer);
    }

    public Future<Void> writeClient(Buffer buffer) {
        if (!clientStatus) {
            LOGGER.error("Unexpected client socket state [paused]");
            this.resumeClient();
        }
        return clientSocket.write(buffer);
    }

    public void registerResponseHandler(Handler<Buffer> handler) {
        originSocket.handler(handler);
    }

    public void registerRequestHandler(Handler<Buffer> handler) {
        clientSocket.handler(handler);
    }
}
