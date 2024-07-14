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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BufferCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferCollector.class);
    private static final int SIZE_LEN = 4;
    private Buffer buffer;

    public BufferCollector() {
        buffer = Buffer.buffer(0);
    }

    public void append(Buffer buffer) {
        if (LOGGER.isTraceEnabled()) {
            LogUtils.hexDump("Msg append", buffer.getBytes());
        }
        this.buffer.appendBuffer(buffer);
    }

    /**
     * @return a list containing the complete Kafka protocol messages accumulated.
     * These are removed from the accumulator.
     */
    public List<Buffer> take() {
        int pos = 0;
        int remaining = buffer.length();
        ArrayList<Buffer> result = new ArrayList<>();
        while (remaining > 0) {
            if (remaining < SIZE_LEN) {
                break;
            }

            int nextMsgLen = buffer.getInt(pos) + SIZE_LEN;
            if (nextMsgLen > remaining) {
                break;
            }

            result.add(buffer.slice(pos, pos + nextMsgLen));

            remaining -= nextMsgLen;
            pos += nextMsgLen;
        }

        if (pos == buffer.length()) {
            buffer = Buffer.buffer(0);
        } else {
            buffer = buffer.getBuffer(pos, buffer.length());
        }

        return result;
    }
}
