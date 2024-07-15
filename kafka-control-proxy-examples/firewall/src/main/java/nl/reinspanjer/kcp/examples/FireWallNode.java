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

package nl.reinspanjer.kcp.examples;

import inet.ipaddr.IPAddressString;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import nl.reinspanjer.kcp.control.DecisionNode;
import nl.reinspanjer.kcp.examples.config.FireWallConfig;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class FireWallNode implements DecisionNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(FireWallNode.class);
    private Vertx vertx;
    private FireWallConfig config;

    @Override
    public FireWallNode init(Vertx vertx) {
        this.vertx = vertx;
        this.config = FireWallConfig.build();
        return this;
    }

    @Override
    public Future<Boolean> request(RequestHeaderAndPayload request) {
        Context context = this.vertx.getOrCreateContext();
        NetSocket clientSocket = context.get("ClientSocket");
        Promise<Boolean> resultPromise = Promise.promise();

        vertx.runOnContext(v -> {
            resultPromise.complete(checkIfAllowed(request.request.apiKey(), clientSocket.remoteAddress()));
        });

        return resultPromise.future();
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        return Future.succeededFuture();
    }

    public Boolean checkIfAllowed(ApiKeys apiKey, SocketAddress ip) {
        IPAddressString ipAddressString = new IPAddressString(ip.hostAddress());
        Set<IPAddressString> addrs = this.config.firewallRules.getOrDefault(apiKey, Set.of());

        boolean decision = false;
        for (IPAddressString addr : addrs) {
            if (addr.contains(ipAddressString)) {
                decision = true;
                break;
            }
        }

        LOGGER.info("Decision [" + decision + "] rules [" + addrs + "] for [" + apiKey + "] from [" + ip.hostAddress() + "]");
        return decision;

    }

}
