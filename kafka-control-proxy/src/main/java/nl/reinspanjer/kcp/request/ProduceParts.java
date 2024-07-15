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

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProduceParts {

    private List<Header> headers;
    private ByteBuffer key;
    private ByteBuffer value;

    public ProduceParts() {
        this.headers = new ArrayList<>();
    }

    public List<Header> getHeaders() {
        return headers;
    }

    public ProduceParts setHeaders(List<Header> headers) {
        this.headers = headers;
        return this;
    }

    public ProduceParts setHeaders(Header[] headers) {
        this.headers.addAll(Arrays.asList(headers));
        return this;
    }

    public Header[] getArrayHeaders() {
        return headers.toArray(new Header[0]);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ProduceParts setKey(ByteBuffer key) {
        this.key = key;
        return this;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public ProduceParts setValue(ByteBuffer value) {
        this.value = value;
        return this;
    }

}
