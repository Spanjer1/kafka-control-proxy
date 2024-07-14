package nl.reinspanjer.kcp.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link nl.reinspanjer.kcp.admin.BrokerEntry}.
 * NOTE: This class has been automatically generated from the {@link nl.reinspanjer.kcp.admin.BrokerEntry} original class using Vert.x codegen.
 */
public class BrokerEntryConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, BrokerEntry obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "host":
          if (member.getValue() instanceof String) {
            obj.setHost((String)member.getValue());
          }
          break;
        case "id":
          if (member.getValue() instanceof String) {
            obj.setId((String)member.getValue());
          }
          break;
        case "port":
          if (member.getValue() instanceof Number) {
            obj.setPort(((Number)member.getValue()).intValue());
          }
          break;
      }
    }
  }

   static void toJson(BrokerEntry obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(BrokerEntry obj, java.util.Map<String, Object> json) {
    if (obj.getHost() != null) {
      json.put("host", obj.getHost());
    }
    if (obj.getId() != null) {
      json.put("id", obj.getId());
    }
    json.put("port", obj.getPort());
  }
}
