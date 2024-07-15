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

package nl.reinspanjer.kafkacontrolproxy.utils;

import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class BufferUtil {

    public static int getResponseCorrelationID(Buffer buffer) {
        if (Objects.isNull(buffer) || buffer.length() < 8) {
            return -1;
        }
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put(buffer.getByte(4));
        bb.put(buffer.getByte(5));
        bb.put(buffer.getByte(6));
        bb.put(buffer.getByte(7));
        return bb.getInt(0);
    }

    public static ApiKeys getApiKey(Buffer buffer) {
        if (Objects.isNull(buffer) || buffer.length() < 6) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.put(buffer.getByte(4));
        bb.put(buffer.getByte(5));
        return ApiKeys.forId(bb.getShort(0));
    }

    public static ByteBuffer copy(ByteBuffer buffer) {
        ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
        copy.put(buffer);
        copy.flip();
        return copy;
    }

    public static ByteBuffer fromString(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }


}
