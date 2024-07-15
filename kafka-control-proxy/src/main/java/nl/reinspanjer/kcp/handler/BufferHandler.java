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
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import nl.reinspanjer.kcp.control.NodeOutcome;
import nl.reinspanjer.kcp.control.NodeRegistrator;
import nl.reinspanjer.kcp.control.NodeTemplate;
import nl.reinspanjer.kcp.data.BrokerOriginMap;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import nl.reinspanjer.kcp.utils.BufferCollector;
import nl.reinspanjer.kcp.utils.BufferUtil;
import nl.reinspanjer.kcp.utils.LogUtils;
import nl.reinspanjer.kcp.utils.MessageUtil;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferHandler implements Handler<Buffer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferHandler.class);
    private static final BrokerOriginMap configBroker = BrokerOriginMap.build();
    private final BufferCollector brokerBufferCollector = new BufferCollector();
    private final BufferCollector clientBufferCollector = new BufferCollector();
    private final Vertx vertx;
    private SocketPair socketPair;
    private Map<Integer, RequestHeaderAndPayload> fetchHeaderCache = new HashMap<>();

    private String USERNAME_CONTEXT = "USERNAME";

    public BufferHandler(Vertx vertx, SocketPair pair) {
        this.vertx = vertx;
        this.socketPair = pair;
        this.socketPair.registerResponseHandler(this::processBrokerResponse);
    }

    /**
     * This is the main entry to message handling, triggered by the arrival of data
     * from the Kafka client. The received buffer may not be a complete Kafka
     * message, so we assemble fragments until we have a complete message.
     */
    @Override
    public void handle(Buffer buffer) {
        LOGGER.debug("Request buffer from client arrived");
        clientBufferCollector.append(buffer);

        List<Buffer> sendBuffers = clientBufferCollector.take();
        LOGGER.debug("Number of complete Kafka msgs: {}", sendBuffers.size());

        if (sendBuffers.isEmpty()) {
            return;
        }

        for (Buffer sendBuffer : sendBuffers) {
            ApiKeys apiKey = BufferUtil.getApiKey(sendBuffer);

            if (apiKey == ApiKeys.SASL_AUTHENTICATE) {
                SaslAuthenticateRequest request = (SaslAuthenticateRequest) MessageUtil.parseRequest(sendBuffer).request;
                byte[] bytes = request.data().authBytes();
                String authBytes = new String(bytes);
                String[] str = authBytes.split("\u0000");
                if (str.length != 3) {
                    LOGGER.error("Unexpected SASL_AUTHENTICATE request, should be PLAIN");
                } else {
                    vertx.getOrCreateContext().put(USERNAME_CONTEXT, str[1]);
                }
            }

            LOGGER.debug("APIKEY: " + apiKey);
            NodeTemplate template = NodeRegistrator.getNodes(apiKey);

            LOGGER.debug("Number of interceptors {}", template.size());

            RequestHeaderAndPayload request = MessageUtil.parseRequest(sendBuffer);

            if (LOGGER.isTraceEnabled()) {
                LogUtils.hexDump("[" + request.header.correlationId() + "] client->proxy", sendBuffer.getBytes());
            }

            this.fetchHeaderCache.put(request.header.correlationId(), request);
            Future<NodeOutcome> nodeOutcomeFuture = template.processRequest(request);

            nodeOutcomeFuture.onSuccess(nodeOutcome -> {
                if (nodeOutcome.result) {
                    RequestHeader header = nodeOutcome.request.header;
                    AbstractRequest abstractRequest = nodeOutcome.request.request;
                    Buffer newBuffer = MessageUtil.serialize(header, abstractRequest);

                    forwardToBroker(newBuffer, request.header.correlationId()).onFailure(e -> {
                        LOGGER.error("Error forwarding request to broker", e);
                        errorToClient(request.header, request.request, e);
                        this.close();
                    });

                } else {
                    LOGGER.info("Policy violation detected. Returning ERROR to client");
                    errorToClient(request.header, request.request, new PolicyViolationException("Policy violation"));
                }
            }).onFailure(e -> {
                LOGGER.error("Error processing request", e);
                errorToClient(request.header, request.request, e);
                this.close();
            });

        }
    }

    public Future<Void> close() {
        return socketPair.close();
    }

    private void errorToClient(RequestHeader header, AbstractRequest request, Throwable err) {
        AbstractResponse response = request.getErrorResponse(new Exception());
        ResponseHeader responseHeader = new ResponseHeader(header.correlationId(), header.headerVersion());
        ResponseHeaderAndPayload responseHeaderAndPayload = new ResponseHeaderAndPayload(responseHeader, response);
        Buffer buff = responseHeaderAndPayload.writeTo(header.apiVersion());

        this.socketPair.resumeClient();

        if (LOGGER.isTraceEnabled()) {
            LogUtils.hexDump("[" + responseHeader.correlationId() + "] proxy->client", buff.getBytes());
        }

        this.socketPair.writeClient(buff);
    }

    private Future<Void> forwardToBroker(Buffer sendBuffer, int corrId) {
        if (sendBuffer == null || sendBuffer.length() == 0) {
            LOGGER.error("forwardToBroker(): empty send Buffer");
            return Future.failedFuture("Empty send Buffer");
        }

        if (LOGGER.isTraceEnabled()) {
            LogUtils.hexDump("[" + corrId + "] proxy->origin", sendBuffer.getBytes());
        }

        return this.socketPair.writeOrigin(sendBuffer);

    }

    public void processBrokerResponse(Buffer brokerRsp) {
        brokerBufferCollector.append(brokerRsp);

        List<Buffer> brokerRspMsgs = brokerBufferCollector.take();
        if (brokerRspMsgs.isEmpty()) {
            return;
        }

        for (Buffer brokerRspMsg : brokerRspMsgs) {
            int corrId = BufferUtil.getResponseCorrelationID(brokerRspMsg);
            if (LOGGER.isTraceEnabled()) {
                LogUtils.hexDump("[" + corrId + "] origin->proxy", brokerRspMsg.getBytes());
            }

            if (corrId != -1) {
                RequestHeaderAndPayload headerAndPayload = fetchHeaderCache.remove(corrId);

                if (headerAndPayload == null || headerAndPayload.request == null) {
                    LOGGER.error("Fetch req header not in cache corrId={}", corrId);
                    socketPair.close(); //TODO return error to client
                    return;
                } else {
                    RequestHeader reqHeader = headerAndPayload.header;
                    LOGGER.debug("Broker response matches cached {} req header corrId={}", reqHeader.apiKey().name, corrId);
                    NodeTemplate template = NodeRegistrator.getNodes(reqHeader.apiKey());
                    ResponseHeaderAndPayload completeRes = MessageUtil.parseResponse(brokerRspMsg, reqHeader);

                    Future<ResponseHeaderAndPayload> res = template.processResponse(headerAndPayload, completeRes);
                    res.onFailure(e -> {
                                LOGGER.error("Error processing response", e);
                                socketPair.close();
                            }
                    ).compose(response -> {
                        Buffer rspBuf = response.writeTo(reqHeader.apiVersion());
                        if (LOGGER.isTraceEnabled()) {
                            LogUtils.hexDump("[" + corrId + "] proxy->client", rspBuf.getBytes());
                        }
                        return socketPair.writeClient(rspBuf);
                    });
                }
            } else {
                LOGGER.error("Broker response does not contain correlation ID");
                socketPair.close(); //TODO return error to client
                return;
            }
        }

    }
}
