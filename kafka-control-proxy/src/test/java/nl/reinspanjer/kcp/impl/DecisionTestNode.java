package nl.reinspanjer.kcp.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.DecisionNode;
import nl.reinspanjer.kcp.control.Node;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.function.Function;

public class DecisionTestNode implements DecisionNode {

    Vertx vertx;
    private Function<RequestHeaderAndPayload, Future<Boolean>> requestHandler = (r) -> Future.succeededFuture(true);
    private Function<RequestHeaderAndPayload, Future<Void>> responseHandler = (r) -> Future.succeededFuture();

    public void setRequestHandler(Function<RequestHeaderAndPayload, Future<Boolean>> requestHandler) {
        this.requestHandler = requestHandler;
    }

    public void setResponseHandler(Function<RequestHeaderAndPayload, Future<Void>> responseHandler) {
        this.responseHandler = responseHandler;
    }

    @Override
    public Future<Boolean> request(RequestHeaderAndPayload request) {
        return requestHandler.apply(request);
    }

    @Override
    public Future<Void> response(RequestHeaderAndPayload request, AbstractResponse response) {
        return responseHandler.apply(request);
    }

    @Override
    public Node init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

}
