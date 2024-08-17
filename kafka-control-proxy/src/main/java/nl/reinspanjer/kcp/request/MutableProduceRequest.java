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

package nl.reinspanjer.kcp.request;

import nl.reinspanjer.kcp.utils.BufferUtil;
import nl.reinspanjer.kcp.utils.LogUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;


public class MutableProduceRequest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MutableProduceRequest.class);

    private int size;
    private ProduceRequestData oData;
    private ByteBuffer oBuffer;
    private Map<DefaultRecord, ProduceParts> produceParts = new HashMap<>();
    private short version;

    public MutableProduceRequest(ProduceRequest producerequest) {
        version = producerequest.version();
        read(producerequest);
    }

    public static ByteBuffer writeTo(int offsetDelta,
                                     long timestampDelta,
                                     ByteBuffer key,
                                     ByteBuffer value,
                                     Header[] headers) {

        int keysize = key == null ? -1 : key.remaining();
        int valuesize = value == null ? -1 : value.remaining();

        int sizeInBytes = DefaultRecord.sizeOfBodyInBytes(offsetDelta, timestampDelta, keysize, valuesize, headers);
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes + ByteUtils.sizeOfVarint(sizeInBytes));
        ByteUtils.writeVarint(sizeInBytes, buffer);

        byte attributes = 0; // there are no used record attributes at the moment
        buffer.put(attributes);

        ByteUtils.writeVarlong(timestampDelta, buffer);
        ByteUtils.writeVarint(offsetDelta, buffer);

        if (key == null) {
            ByteUtils.writeVarint(-1, buffer);
        } else {
            int keySize = key.remaining();
            ByteUtils.writeVarint(keySize, buffer);
            buffer.put(key.array(), key.arrayOffset() + key.position(), keySize);
        }

        if (value == null) {
            ByteUtils.writeVarint(-1, buffer);
        } else {
            int valueSize = value.remaining();
            ByteUtils.writeVarint(valueSize, buffer);
            buffer.put(value.array(), value.arrayOffset() + value.position(), valueSize);
        }

        ByteUtils.writeVarint(headers.length, buffer);

        for (Header header : headers) {
            String headerKey = header.key();
            if (headerKey == null)
                throw new IllegalArgumentException("Invalid null header key found in headers");

            byte[] utf8Bytes = Utils.utf8(headerKey);
            ByteUtils.writeVarint(utf8Bytes.length, buffer);
            buffer.put(utf8Bytes);

            byte[] headerValue = header.value();
            if (headerValue == null) {
                ByteUtils.writeVarint(-1, buffer);
            } else {
                ByteUtils.writeVarint(headerValue.length, buffer);
                buffer.put(headerValue);
            }
        }
        buffer.flip();
        return buffer;
    }

    public void put(DefaultRecord record, ProduceParts parts) {
        produceParts.put(record, parts);
    }

    public ProduceParts get(DefaultRecord record) {
        return produceParts.get(record);
    }

    public ProduceRequest toProduceRequest() {

        ProduceRequestData.TopicProduceDataCollection coll = new ProduceRequestData.TopicProduceDataCollection();

        oData.topicData().forEach(topicData -> {
            ProduceRequestData.TopicProduceData topicData1 = new ProduceRequestData.TopicProduceData();

            topicData1.setName(topicData.name());
            List<ProduceRequestData.PartitionProduceData> partitionData1 = new ArrayList<>();

            topicData.partitionData().forEach(partitionData -> {
                MemoryRecords records = (MemoryRecords) partitionData.records();
                MemoryRecords newRecords = readFrom(records);
                partitionData1.add(partitionData.duplicate().setRecords(newRecords));
            });

            topicData1.setPartitionData(partitionData1);

            coll.add(topicData1);
        });
        ProduceRequestData data = new ProduceRequestData().setTopicData(coll);
        data.setTransactionalId(oData.transactionalId());
        data.setAcks(oData.acks());
        data.setTimeoutMs(oData.timeoutMs());

        return new ProduceRequest(data, version);
    }

    private void read(ProduceRequest producerequest) {
        this.oBuffer = producerequest.serialize();
        this.size = oBuffer.remaining();
        this.oData = producerequest.data();


        this.oData.topicData().forEach(
                topicData -> {

                    topicData.partitionData().forEach(
                            partitionData -> {
                                MemoryRecords records = (MemoryRecords) partitionData.records();

                                Iterable<MutableRecordBatch> m = records.batches();

                                m.forEach(
                                        batch ->
                                                batch.iterator().forEachRemaining(
                                                record -> {
                                                    DefaultRecord defaultRecord = (DefaultRecord) record;
                                                    ProduceParts parts = new ProduceParts();
                                                    ByteBuffer copyKey = defaultRecord.key() != null ? BufferUtil.copy(defaultRecord.key()) : null;
                                                    ByteBuffer copyValue = defaultRecord.value() != null ? BufferUtil.copy(defaultRecord.value()) : null;
                                                    parts
                                                            .setKey(copyKey)
                                                            .setValue(copyValue)
                                                            .setHeaders(defaultRecord.headers())
                                                            .setTopicName(topicData.name());

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

        oData.topicData().forEach(topicData -> {
            topicData.partitionData().forEach(partitionData -> {
                MemoryRecords mr = (MemoryRecords) partitionData.records();
                mr.batches().forEach(batch -> {
                    batch.iterator().forEachRemaining(
                            record -> {
                                DefaultRecord dr = (DefaultRecord) record;
                                ProduceParts parts = this.get(dr);
                                parts = f.apply(parts);
                                this.put(dr, parts);
                            }
                    );

                });
            });
        });
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
