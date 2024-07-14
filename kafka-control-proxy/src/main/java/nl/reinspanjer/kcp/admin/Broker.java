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

package nl.reinspanjer.kcp.admin;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * A configuration object containing the configuration entries for a resource
 */
@DataObject
@JsonGen(publicConverter = false)
public class Broker {

    private List<BrokerEntry> entries;

    /**
     * Constructor
     */
    public Broker() {

    }

    /**
     * Constructor
     *
     * @param entries configuration entries for a resource
     */
    public Broker(List<BrokerEntry> entries) {
        this.entries = entries;
    }

    /**
     * Constructor (from JSON representation)
     *
     * @param json JSON representation
     */
    public Broker(JsonObject json) {
        BrokerConverter.fromJson(json, this);
    }

    /**
     * @return configuration entries for a resource
     */
    public List<BrokerEntry> getEntries() {
        return entries;
    }

    /**
     * Set the configuration entries for a resource
     *
     * @param entries configuration entries for a resource
     * @return current instance of the class to be fluent
     */
    public Broker setEntries(List<BrokerEntry> entries) {
        this.entries = entries;
        return this;
    }

    /**
     * Convert object to JSON representation
     *
     * @return JSON representation
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        BrokerConverter.toJson(this, json);
        return json;
    }

    @Override
    public String toString() {

        return "Broker{" +
                "entries=" + this.entries +
                "}";
    }
}
