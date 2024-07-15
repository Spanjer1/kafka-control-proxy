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

package nl.reinspanjer.kafkacontrolproxy.control;

import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeRegistrator {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRegistrator.class);
    private static final Map<ApiKeys, List<DecisionNode>> decisionNodes = new HashMap<>();
    private static final Map<ApiKeys, List<ObserverNode>> observerNodes = new HashMap<>();
    private static final Map<ApiKeys, List<TransformNode>> transformNodes = new HashMap<>();
    private static final Map<ApiKeys, List<TransformResponseNode>> transformResponseNodes = new HashMap<>();

    public static <T extends Node> void addToHashMap(Map<ApiKeys, List<T>> map, List<ApiKeys> apiKeys, T node) {
        for (ApiKeys apiKey : apiKeys) {
            LOGGER.debug("Register handler [{}] for [{}]", node.getClass(), apiKey);
            if (map.containsKey(apiKey)) {
                map.get(apiKey).add(node);
            } else {
                List<T> reqList = new ArrayList<>();
                reqList.add(node);
                map.put(apiKey, reqList);
            }
        }
    }

    public static void registerNode(List<ApiKeys> apiKeys, DecisionNode node) {
        addToHashMap(decisionNodes, apiKeys, node);
    }

    public static void registerNode(List<ApiKeys> apiKeys, ObserverNode node) {
        addToHashMap(observerNodes, apiKeys, node);
    }

    public static void registerNode(List<ApiKeys> apiKeys, TransformNode node) {
        addToHashMap(transformNodes, apiKeys, node);
    }

    public static void registerNode(List<ApiKeys> apiKeys, TransformResponseNode node) {
        addToHashMap(transformResponseNodes, apiKeys, node);
    }

    public static NodeTemplate getNodes(ApiKeys apiKey) {

        NodeTemplate nodeTemplate = new NodeTemplate();
        nodeTemplate.setDecisionNodes(decisionNodes.get(apiKey));
        nodeTemplate.setObserverNodes(observerNodes.get(apiKey));
        nodeTemplate.setTransformNodes(transformNodes.get(apiKey));
        nodeTemplate.setTransformResponseNodes(transformResponseNodes.get(apiKey));
        return nodeTemplate;

    }

}
