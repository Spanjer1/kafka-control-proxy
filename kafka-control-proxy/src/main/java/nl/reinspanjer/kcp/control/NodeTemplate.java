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

package nl.reinspanjer.kcp.control;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NodeTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTemplate.class);

    private List<DecisionNode> decisionNodes = new ArrayList<>();
    private List<ObserverNode> observerNodes = new ArrayList<>();
    private List<TransformNode> transformNodes = new ArrayList<>();
    private List<TransformResponseNode> transformResponsNodes = new ArrayList<>();

    public int size() {
        return decisionNodes.size() + observerNodes.size() + transformNodes.size();
    }

    public NodeTemplate setDecisionNodes(List<DecisionNode> decisionNodes) {
        if (decisionNodes == null) {
            return this;
        }
        this.decisionNodes = decisionNodes;
        return this;
    }

    public NodeTemplate setObserverNodes(List<ObserverNode> observerNodes) {
        if (observerNodes == null) {
            return this;
        }
        this.observerNodes = observerNodes;
        return this;
    }

    public NodeTemplate setTransformNodes(List<TransformNode> transformNodes) {
        if (transformNodes == null) {
            return this;
        }
        this.transformNodes = transformNodes;
        return this;
    }

    public NodeTemplate setTransformResponseNodes(List<TransformResponseNode> transformResponseNodes) {
        if (transformResponseNodes == null) {
            return this;
        }
        this.transformResponsNodes = transformResponseNodes;
        return this;
    }


    public Future<NodeOutcome> processRequest(RequestHeaderAndPayload request) {
        Promise<NodeOutcome> outcome = Promise.promise();
        for (ObserverNode observerNode : observerNodes) {
            observerNode.request(request);
        }

        Promise<RequestHeaderAndPayload> chainPromise = Promise.promise();

        if (transformNodes.isEmpty()) {
            chainPromise.complete(request);
        } else {
            Future<RequestHeaderAndPayload> chain = Future.succeededFuture(request);
            for (TransformNode transformNode : transformNodes) {
                chain.onSuccess(transformNode::request);
            }
            chain.onFailure(outcome::fail);
            chain.onComplete(ar -> {
                if (ar.succeeded()) {
                    chainPromise.complete(ar.result());
                } else {
                    chainPromise.fail(ar.cause());
                }
            });
        }

        Promise<Boolean> decision = Promise.promise();
        if (decisionNodes.isEmpty()) {
            decision.complete(true);
        } else {
            CompositeFuture decisions = Future.all(decisionNodes.stream().map(node -> node.request(request)).toList());
            decisions.onComplete(ar -> {
                if (ar.succeeded()) {
                    boolean decisionResult = ar.result().list().stream().allMatch(Boolean.TRUE::equals);
                    decision.complete(decisionResult);
                } else {
                    decision.fail(ar.cause());
                }
            });
        }

        NodeOutcome nodeOutcome = new NodeOutcome();

        Future.all(decision.future(), chainPromise.future()).onComplete(
                ar -> {
                    CompositeFuture compositeFuture = ar.result();
                    if (ar.succeeded()) {
                        nodeOutcome.result = compositeFuture.resultAt(0);
                        nodeOutcome.request = compositeFuture.resultAt(1);
                        outcome.complete(nodeOutcome);
                    } else {
                        LOGGER.error("Error processing request", ar.cause());
                        outcome.fail(ar.cause());
                    }
                }
        );

        return outcome.future();
    }

    public Future<ResponseHeaderAndPayload> processResponse(RequestHeaderAndPayload request, ResponseHeaderAndPayload r) {

        List<Future<Void>> futures = new ArrayList<>();
        for (ObserverNode observerNode : observerNodes) {
            futures.add(observerNode.response(request, r.response));
        }

        for (TransformNode transformNode : transformNodes) {
            futures.add(transformNode.response(request, r.response));
        }

        for (DecisionNode decisionNode : decisionNodes) {
            futures.add(decisionNode.response(request, r.response));
        }

        Future<ResponseHeaderAndPayload> t = Future.succeededFuture(r);
        for (TransformResponseNode transformResponseNode : transformResponsNodes) {
            t.onSuccess(res -> transformResponseNode.response(request, res));
        }

        return t;
    }


}
