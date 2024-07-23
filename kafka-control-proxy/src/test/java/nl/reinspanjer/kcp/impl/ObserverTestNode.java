package nl.reinspanjer.kcp.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.Node;
import nl.reinspanjer.kcp.control.ObserverNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.function.Function;

public class ObserverTestNode implements ObserverNode {

    Vertx vertx;
    private Function<RequestHeaderAndPayload, Future<Void>> requestHandler = (r) -> Future.succeededFuture();
    private Function<RequestHeaderAndPayload, Future<Void>> responseHandler = (r) -> Future.succeededFuture();


    public void setRequestHandler(Function<RequestHeaderAndPayload, Future<Void>> requestHandler) {
        this.requestHandler = requestHandler;
    }

    public void setResponseHandler(Function<RequestHeaderAndPayload, Future<Void>> responseHandler) {
        this.responseHandler = responseHandler;
    }

    @Override
    public Future<Void> request(RequestHeaderAndPayload request) {
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

    @Override
    public ObserverNode clone() {
        ObserverTestNode node = new ObserverTestNode();
        node.setRequestHandler(requestHandler);
        node.setResponseHandler(responseHandler);
        return node;
    }

}
