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

package nl.reinspanjer.kafkacontrolproxy.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

@DataObject
@JsonGen(publicConverter = false)
public class BrokerEntry {
    private String id;
    private String host;
    private int port;

    public BrokerEntry() {
    }

    public BrokerEntry(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public BrokerEntry(JsonObject json) {
        BrokerEntryConverter.fromJson(json, this);
    }

    public String getId() {
        return id;
    }

    public BrokerEntry setId(String id) {
        this.id = id;
        return this;
    }

    public String getHost() {
        return host;
    }

    public BrokerEntry setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public BrokerEntry setPort(int port) {
        this.port = port;
        return this;
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        BrokerEntryConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {
        return "BrokerEntry{id=" + id + ", host=" + host + ", port=" + port + "}";
    }


}
