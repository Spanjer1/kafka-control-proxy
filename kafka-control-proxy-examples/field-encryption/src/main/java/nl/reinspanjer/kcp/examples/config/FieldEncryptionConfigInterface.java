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

package nl.reinspanjer.kcp.examples.config;

import io.smallrye.config.ConfigMapping;

import java.util.List;
import java.util.Map;

@ConfigMapping(prefix = "crypto")
interface FieldEncryptionConfigInterface {

    Map<String, Mapping> scope();

    Map<String, List<Users>> group();

    interface Mapping {
        List<String> groups();

        List<String> paths();
    }

    interface Users {
        List<String> users();
    }


}
