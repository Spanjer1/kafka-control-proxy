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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.examples.CryptoControl;
import nl.reinspanjer.kcp.examples.config.FieldEncryptionConfigInterface;
import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CryptoControlTest {

    @Test
    public void encryptFields() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19.95
                        }
                    }
                }
                """;

        String jsonPath = "$.store.book[*].title";

        testJsonPathWithString(jsonPath, jsonString);

    }

    @Test
    public void testDouble() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19.95
                        }
                    }
                }
                """;

        String path = "$.store.bicycle.price";
        testJsonPathWithString(path, jsonString);
    }


    @Test
    public void testInt() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19
                        }
                    }
                }
                """;

        String path = "$.store.bicycle.price";
        testJsonPathWithString(path, jsonString);
    }


    @Test
    public void testObject() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19
                        }
                    }
                }
                """;

        String path = "$.store.bicycle";
        testJsonPathWithString(path, jsonString);
    }


    @Test
    public void testArray() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19
                        }
                    }
                }
                """;

        String path = "$.store.book";
        testJsonPathWithString(path, jsonString);
    }

    @Test
    public void testBoolean() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": true
                        }
                    }
                }
                """;

        String path = "$.store.bicycle.price";
        testJsonPathWithString(path, jsonString);
    }

    @Test
    public void testComplete() throws GeneralSecurityException, JsonProcessingException {
        String jsonString = """
                {
                    "store": {
                        "book": [
                            {"title": "Title 1", "author": "Author 1"},
                            {"title": "Title 2", "author": "Author 2"}
                        ],
                        "bicycle": {
                            "color": "red",
                            "price": 19
                        }
                    }
                }
                """;

        String path = "$.*";
        testJsonPathWithString(path, jsonString);
    }











    private static void testJsonPathWithString(String jsonPath, String jsonString) throws GeneralSecurityException, JsonProcessingException {
        List<FieldEncryptionConfigInterface.Rule> ruleList = List.of(new RuleTest(jsonPath, ""));
        CryptoControl control = new CryptoControl().init(Vertx.vertx());

        String value = control.encryptFields(ruleList, jsonString);
        assertThat(value.replaceAll("\\s+", "")).isNotEqualTo(jsonString.replaceAll("\\s+", ""));

        String decryptedValue = control.decryptFields(ruleList, value);

        assertThat(decryptedValue.replaceAll("\\s+", "")).isEqualTo(jsonString.replaceAll("\\s+", ""));
    }
}
