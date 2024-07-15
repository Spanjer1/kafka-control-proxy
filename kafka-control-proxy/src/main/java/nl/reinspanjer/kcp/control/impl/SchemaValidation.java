//package nl.nn.kafkasecureproxy.control.impl;
//
//import io.vertx.core.Future;
//import io.vertx.core.Promise;
//import io.vertx.core.Vertx;
//import nl.nn.kafkasecureproxy.admin.Config;
//import nl.nn.kafkasecureproxy.control.DecisionNode;
//import nl.nn.kafkasecureproxy.verticles.KafkaCacheService;
//import org.apache.kafka.common.message.ProduceRequestData;
//import org.apache.kafka.common.record.MemoryRecords;
//import org.apache.kafka.common.record.MutableRecordBatch;
//import org.apache.kafka.common.requests.AbstractRequest;
//import org.apache.kafka.common.requests.AbstractResponse;
//import org.apache.kafka.common.requests.ProduceRequest;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//
//public class SchemaValidation implements DecisionNode {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidation.class);
//    private Vertx vertx;
//
//    private KafkaCacheService service;
//
//    public SchemaValidation(Vertx vertx) {
//        this.vertx = vertx;
//        service = KafkaCacheService.createProxy(vertx, KafkaCacheService.ADDRESS);
//    }
//
//    public Map<String, List<byte[]>> getData(ProduceRequest request) {
//        Map<String, List<byte[]>> container = new HashMap<>();
//
//        Iterator<ProduceRequestData.TopicProduceData> it = request.data().topicData().stream().iterator();
//
//        it.forEachRemaining(
//                produceData -> {
//                    List<byte[]> topicData = new ArrayList<>();
//                    LOGGER.info("SCHEMAVALIDATION NAME " + produceData.name());
//                    produceData.partitionData().forEach(
//                            data -> {
//                                MemoryRecords memoryRecords = (MemoryRecords) data.records();
//
//                                Iterator<MutableRecordBatch> batch = memoryRecords.batches().iterator();
//                                batch.forEachRemaining(
//                                        r -> {
//                                            byte[] completeValue = r.iterator().next().value().array();
//                                            int payloadLength = r.iterator().next().valueSize();
//                                            int completeLength = r.iterator().next().value().array().length;
//                                            byte[] value = Arrays.copyOfRange(completeValue, completeLength-payloadLength-1, completeLength);
//                                            topicData.add(value);
//                                        }
//                                );
//                            }
//                    );
//
//                    if (container.containsKey(produceData.name())) {
//                        container.get(produceData.name()).addAll(topicData);
//                    } else {
//                        container.put(produceData.name(), topicData);
//                    }
//
//                }
//        );
//
//        return container;
//
//    }
//
//    @Override
//    public Future<Boolean> request(AbstractRequest request) {
//        Promise<Boolean> promise = Promise.promise();
//
//        ProduceRequest produceRequest = (ProduceRequest) request;
//        Map<String, List<byte[]>> produceRequestData = getData(produceRequest);
//
//        produceRequestData.forEach((key, value) -> {
//
//            service.getConfig(key).onComplete(res -> {
//                if (res.succeeded()) {
//                    Config config = res.result();
//                    LOGGER.info("SCHEMAVALIDATION " + config);
//                } else {
//                    LOGGER.error("SCHEMAVALIDATION " + res.cause());
//                }
//            });
//
//        });
//
//        return promise.future();
//    }
//
//    @Override
//    public void response(AbstractRequest request, AbstractResponse response) {
//
//    }
//
//}
