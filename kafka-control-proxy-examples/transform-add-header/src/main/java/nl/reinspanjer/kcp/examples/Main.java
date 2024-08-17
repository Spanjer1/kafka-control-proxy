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

package nl.reinspanjer.kcp.examples;

import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.KafkaControlProxy;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.List;

import static nl.reinspanjer.kcp.control.NodeRegistrator.registerNode;

public class Main {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        registerNode(
                List.of(ApiKeys.PRODUCE), // Register the LogDecisionNode for the given ApiKeys
                new HeaderTransformHead().init(vertx) // TransformHeader
        );
        KafkaControlProxy.deploy(vertx);
    }
}
