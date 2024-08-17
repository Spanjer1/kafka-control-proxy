package nl.reinspanjer.kcp.response;

import nl.reinspanjer.kcp.request.MutableProduceRequest;
import nl.reinspanjer.kcp.request.ProduceParts;
import nl.reinspanjer.kcp.utils.BufferUtil;
import nl.reinspanjer.kcp.utils.LogUtils;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.FetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

public class MutableFetchResponse {

    private Map<DefaultRecord, ProduceParts> produceParts = new HashMap<>();
    private FetchResponseData oData;
    private static final Logger LOGGER = LoggerFactory.getLogger(MutableFetchResponse.class);


    public MutableFetchResponse(FetchResponse fetchResponse) {
        read(fetchResponse);
    }

    private void read(FetchResponse fetchResponse) {
        oData = fetchResponse.data();
        fetchResponse.data().responses().forEach(
                fetchResponseData -> {
                    fetchResponseData.partitions().forEach(
                            fetchResponseDataPartition -> {
                                MemoryRecords records = (MemoryRecords) fetchResponseDataPartition.records();
                                Iterable<MutableRecordBatch> m = records.batches();
                                m.forEach(
                                        batch -> batch.iterator().forEachRemaining(
                                                record -> {
                                                    DefaultRecord defaultRecord = (DefaultRecord) record;
                                                    ProduceParts parts = new ProduceParts();
                                                    ByteBuffer copyKey = defaultRecord.key() != null ? BufferUtil.copy(defaultRecord.key()) : null;
                                                    ByteBuffer copyValue = defaultRecord.value() != null ? BufferUtil.copy(defaultRecord.value()) : null;
                                                    parts
                                                            .setKey(copyKey)
                                                            .setValue(copyValue)
                                                            .setHeaders(defaultRecord.headers());
                                                    produceParts.put(defaultRecord, parts);
                                                }
                                        )
                                );
                            }
                    );
                }
        );
    }

    public void transform(Function<ProduceParts, ProduceParts> f) {

        oData.responses().forEach(
                fetchResponseData -> {
                    fetchResponseData.partitions().forEach(
                            fetchResponseDataPartition -> {
                                MemoryRecords records = (MemoryRecords) fetchResponseDataPartition.records();
                                Iterable<MutableRecordBatch> m = records.batches();
                                m.forEach(
                                        batch -> batch.iterator().forEachRemaining(
                                                record -> {
                                                    DefaultRecord defaultRecord = (DefaultRecord) record;
                                                    ProduceParts parts = produceParts.get(defaultRecord);
                                                    parts = f.apply(parts);
                                                    produceParts.put(defaultRecord, parts);
                                                }
                                        )
                                );
                            }
                    );
                }
        );

    }


    public FetchResponse toFetchResponse() {
        List<FetchResponseData.FetchableTopicResponse> partitionResponses = new ArrayList<>();

        for (FetchResponseData.FetchableTopicResponse respons : oData.responses()) {
            FetchResponseData.FetchableTopicResponse topicResponse = new FetchResponseData.FetchableTopicResponse();
            topicResponse.setTopic(respons.topic());
            topicResponse.setTopicId(respons.topicId());

            List<FetchResponseData.PartitionData> partitionDataList = new ArrayList<>();
            for (FetchResponseData.PartitionData partitionData : respons.partitions()) {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                MemoryRecords newRecords = readFrom(records);
                partitionDataList.add(partitionData.duplicate().setRecords(newRecords));
            }

            topicResponse.setPartitions(partitionDataList);
            partitionResponses.add(topicResponse);
        }

        FetchResponseData data = new FetchResponseData();
        data.setSessionId(oData.sessionId());
        data.setThrottleTimeMs(oData.throttleTimeMs());
        data.setErrorCode(oData.errorCode());
        data.setNodeEndpoints(oData.nodeEndpoints());
        data.setResponses(partitionResponses);

        return new FetchResponse(data);
    }

    public MemoryRecords readFrom(MemoryRecords records) {
        if (LOGGER.isTraceEnabled()) {
            LogUtils.hexDump("memoryRecords", records.buffer());
        }

        Iterable<MutableRecordBatch> m = records.batches();
        MutableRecordBatch mrb = m.iterator().next(); //assuming its all the same
        DefaultRecordBatch drb = (DefaultRecordBatch) mrb;

        MemoryRecordsBuilder builder = records.builder(
                ByteBuffer.allocate(1),
                drb.magic(),
                drb.compressionType(),
                drb.timestampType(),
                drb.baseOffset(),
                -1,
                drb.partitionLeaderEpoch()
        );

        m.forEach(batch -> {
            DefaultRecordBatch defaultRecordBatch = (DefaultRecordBatch) batch;
            long baseOffset = defaultRecordBatch.baseOffset();
            long basetimestamp = defaultRecordBatch.baseTimestamp();

            Iterator<Record> defaultRecordIterator = defaultRecordBatch.iterator();

            defaultRecordIterator.forEachRemaining(record -> {
                DefaultRecord defaultRecord = (DefaultRecord) record;

                int delta = (int) (defaultRecord.offset() - baseOffset);
                LOGGER.trace("delta " + delta + " offset " + defaultRecord.offset() + " baseOffset " + baseOffset);
                long timeStampDelta = defaultRecord.timestamp() - basetimestamp;
                ProduceParts part = this.produceParts.get(record);
                ByteBuffer buffer = MutableProduceRequest.writeTo(delta, timeStampDelta, part.getKey(), part.getValue(), part.getArrayHeaders());
                DefaultRecord newRecord = DefaultRecord.readFrom(buffer, baseOffset, basetimestamp, record.sequence(), null);

                builder.append(newRecord);
                LOGGER.trace("Newrecord Offset " + newRecord.offset());
            });

        });

        MemoryRecords newRecords = builder.build();

        if (LOGGER.isTraceEnabled()) {
            LogUtils.hexDump("newRecords", newRecords.buffer());
        }

        return newRecords;
    }

}
