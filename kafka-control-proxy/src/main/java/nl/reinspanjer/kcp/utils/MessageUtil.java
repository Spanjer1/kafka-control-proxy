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

package nl.reinspanjer.kcp.utils;

import io.vertx.core.buffer.Buffer;
import nl.reinspanjer.kcp.request.MutableProduceRequest;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtil.class);

    public static RequestHeaderAndPayload parseRequest(Buffer buffer) {
        ByteBuffer kafkaPayload = ByteBuffer.wrap(buffer.getBytes(4, buffer.length()));
        RequestHeader requestHeader = RequestHeader.parse(kafkaPayload);
        return new RequestHeaderAndPayload(requestHeader,
                AbstractRequest.parseRequest(requestHeader.apiKey(), requestHeader.apiVersion(), kafkaPayload).request);
    }

    public static ResponseHeaderAndPayload parseResponse(Buffer buffer, RequestHeader requestHeader) {
        ApiKeys apiKey = requestHeader.apiKey();
        short apiVersion = requestHeader.apiVersion();

        ByteBuffer kafkaPayload = ByteBuffer.wrap(buffer.getBytes(4, buffer.length()));

        ResponseHeader responseHeader = ResponseHeader.parse(kafkaPayload, apiKey.responseHeaderVersion(apiVersion));

        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            throw new CorrelationIdMismatchException("Correlation id for response ("
                    + responseHeader.correlationId() + ") does not match request ("
                    + requestHeader.correlationId() + "), request header: " + requestHeader,
                    requestHeader.correlationId(), responseHeader.correlationId());
        }
        AbstractResponse response = AbstractResponse.parseResponse(apiKey, kafkaPayload, apiVersion);

        return new ResponseHeaderAndPayload(responseHeader, response);
    }

    public static Buffer serialize(RequestHeader header, AbstractRequest request) {
        ByteBuffer buffer = request.serializeWithHeader(header);
        int size = buffer.capacity();
        ByteBuffer newBuffer = ByteBuffer.allocate(size + 4);
        newBuffer.putInt(size);
        newBuffer.put(buffer);

        newBuffer.flip();
        Buffer vBuff = Buffer.buffer(newBuffer.array());
        newBuffer.clear();
        buffer.clear();
        return vBuff;
    }


    public static RequestHeader change(RequestHeader header) {
        RequestHeaderData data = header.data().duplicate();
        List<RawTaggedField> taggedFields = data.unknownTaggedFields();

        RawTaggedField taggedField = new RawTaggedField(1, "test".getBytes(StandardCharsets.UTF_8));
        taggedFields.add(taggedField);

        RequestHeader newHeader = new RequestHeader(data, (short) 2);
        LOGGER.info("Header size {}", newHeader.size());

        return newHeader;
    }

    public static int getSize(List<RawTaggedField> taggedFields) {
        int totalSize = 0;
        totalSize += ByteUtils.sizeOfUnsignedVarint(taggedFields.size());

        for (RawTaggedField _field : taggedFields) {
            totalSize += ByteUtils.sizeOfUnsignedVarint(_field.tag());
            totalSize += ByteUtils.sizeOfUnsignedVarint(_field.size());
            totalSize += _field.size();
        }

        return totalSize;
    }


    public static ProduceRequest logData(ProduceRequest request) {
        MutableProduceRequest req = new MutableProduceRequest(request);
        return request;
    }


}
