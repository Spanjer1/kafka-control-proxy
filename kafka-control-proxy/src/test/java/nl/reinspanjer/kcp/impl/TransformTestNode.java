package nl.reinspanjer.kcp.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.control.Node;
import nl.reinspanjer.kcp.control.TransformNode;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;

import java.util.function.BiFunction;
import java.util.function.Function;

public class TransformTestNode implements TransformNode {

    Vertx vertx;
    private Function<RequestHeaderAndPayload, Future<RequestHeaderAndPayload>> requestHandler = Future::succeededFuture;
    private BiFunction<RequestHeaderAndPayload, ResponseHeaderAndPayload, Future<ResponseHeaderAndPayload>> responseHandler = (req, res) -> Future.succeededFuture(res);

    @Override
    public Future<RequestHeaderAndPayload> request(RequestHeaderAndPayload request) {
        return requestHandler.apply(request);
    }

    @Override
    public Future<ResponseHeaderAndPayload> response(RequestHeaderAndPayload request, ResponseHeaderAndPayload response) {
        return responseHandler.apply(request, response);
    }

    @Override
    public Node init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    public void setRequestHandler(Function<RequestHeaderAndPayload, Future<RequestHeaderAndPayload>> requestHandler) {
        this.requestHandler = requestHandler;
    }

    public void setResponseHandler(BiFunction<RequestHeaderAndPayload, ResponseHeaderAndPayload, Future<ResponseHeaderAndPayload>> responseHandler) {
        this.responseHandler = responseHandler;
    }


}
