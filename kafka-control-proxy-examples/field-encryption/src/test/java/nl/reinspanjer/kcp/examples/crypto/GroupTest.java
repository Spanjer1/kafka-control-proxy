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

import java.util.List;

public class GroupTest implements FieldEncryptionConfigInterface.Group {
    String groupId;
    List<String> user;
    List<String> scope;

    public GroupTest(String groupId, List<String> user, List<String> scope) {
        this.groupId = groupId;
        this.user = user;
        this.scope = scope;
    }

    @Override
    public String groupId() {
        return groupId;
    }

    @Override
    public List<String> user() {
        return user;
    }

    @Override
    public List<String> scope() {
        return scope;
    }
}
