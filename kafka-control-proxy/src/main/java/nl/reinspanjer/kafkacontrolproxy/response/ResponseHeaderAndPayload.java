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

package nl.reinspanjer.kafkacontrolproxy.response;

import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;

public class ResponseHeaderAndPayload {
    public ResponseHeader header;
    public AbstractResponse response;

    public ResponseHeaderAndPayload(ResponseHeader header, AbstractResponse response) {
        this.header = header;
        this.response = response;
    }

    public Buffer writeTo(short version) {
        ByteBuffer buff = RequestUtils.serialize(header.data(), header.headerVersion(), response.data(), version);
        int size = buff.remaining();
        ByteBuffer finalBuff = ByteBuffer.allocate(size + 4);
        finalBuff.putInt(size);
        finalBuff.put(buff);
        finalBuff.flip();
        return Buffer.buffer(finalBuff.array());
    }
}
