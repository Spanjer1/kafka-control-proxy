package nl.reinspanjer.kafkacontrolproxy.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link nl.reinspanjer.kafkacontrolproxy.admin.ConfigEntry}.
 * NOTE: This class has been automatically generated from the {@link nl.reinspanjer.kafkacontrolproxy.admin.ConfigEntry} original class using Vert.x codegen.
 */
public class ConfigEntryConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, ConfigEntry obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "default":
          if (member.getValue() instanceof Boolean) {
            obj.setDefault((Boolean)member.getValue());
          }
          break;
        case "name":
          if (member.getValue() instanceof String) {
            obj.setName((String)member.getValue());
          }
          break;
        case "readOnly":
          if (member.getValue() instanceof Boolean) {
            obj.setReadOnly((Boolean)member.getValue());
          }
          break;
        case "sensitive":
          if (member.getValue() instanceof Boolean) {
            obj.setSensitive((Boolean)member.getValue());
          }
          break;
        case "source":
          if (member.getValue() instanceof String) {
            obj.setSource(org.apache.kafka.clients.admin.ConfigEntry.ConfigSource.valueOf((String)member.getValue()));
          }
          break;
        case "synonyms":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<nl.reinspanjer.kafkacontrolproxy.admin.ConfigSynonym> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new nl.reinspanjer.kafkacontrolproxy.admin.ConfigSynonym((io.vertx.core.json.JsonObject)item));
            });
            obj.setSynonyms(list);
          }
          break;
        case "value":
          if (member.getValue() instanceof String) {
            obj.setValue((String)member.getValue());
          }
          break;
      }
    }
  }

   static void toJson(ConfigEntry obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(ConfigEntry obj, java.util.Map<String, Object> json) {
    json.put("default", obj.isDefault());
    if (obj.getName() != null) {
      json.put("name", obj.getName());
    }
    json.put("readOnly", obj.isReadOnly());
    json.put("sensitive", obj.isSensitive());
    if (obj.getSource() != null) {
      json.put("source", obj.getSource().name());
    }
    if (obj.getSynonyms() != null) {
      JsonArray array = new JsonArray();
      obj.getSynonyms().forEach(item -> array.add(item.toJson()));
      json.put("synonyms", array);
    }
    if (obj.getValue() != null) {
      json.put("value", obj.getValue());
    }
  }
}
