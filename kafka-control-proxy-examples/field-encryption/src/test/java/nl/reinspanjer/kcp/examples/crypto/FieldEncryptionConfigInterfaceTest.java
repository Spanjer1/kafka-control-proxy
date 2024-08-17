/*******************************************************************************
 * Copyright 2024 Rein Spanjer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 ******************************************************************************/

package nl.reinspanjer.kcp.examples.crypto;

import nl.reinspanjer.kcp.examples.config.FieldEncryptionConfigInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldEncryptionConfigInterfaceTest implements FieldEncryptionConfigInterface {

    Map<String, List<RuleTest>> rules = new HashMap<>();
    List<GroupTest> groups = new ArrayList<>();

    public void addRule(String topic, RuleTest rule) {
        if (rules.containsKey(topic)) {
            rules.get(topic).add(rule);
        } else {
            List<RuleTest> ruleList = new ArrayList<>();
            ruleList.add(rule);
            rules.put(topic, ruleList);
        }
    }

    public void addGroup(GroupTest group) {
        groups.add(group);
    }

    @Override
    public Map<String, List<Rule>> topic() {
        Map<String, List<Rule>> castedRules = new HashMap<>();
        rules.forEach((key, value) -> {
            List<Rule> castedValue = List.copyOf(value);
            castedRules.put(key, castedValue);
        });
        return castedRules;
    }

    @Override
    public List<Group> group() {
        return List.copyOf(groups);
    }
}
