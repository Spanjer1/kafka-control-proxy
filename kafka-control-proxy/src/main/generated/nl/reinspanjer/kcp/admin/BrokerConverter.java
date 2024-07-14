package nl.reinspanjer.kcp.admin;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.impl.JsonUtil;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * Converter and mapper for {@link nl.reinspanjer.kcp.admin.Broker}.
 * NOTE: This class has been automatically generated from the {@link nl.reinspanjer.kcp.admin.Broker} original class using Vert.x codegen.
 */
public class BrokerConverter {


  private static final Base64.Decoder BASE64_DECODER = JsonUtil.BASE64_DECODER;
  private static final Base64.Encoder BASE64_ENCODER = JsonUtil.BASE64_ENCODER;

   static void fromJson(Iterable<java.util.Map.Entry<String, Object>> json, Broker obj) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "entries":
          if (member.getValue() instanceof JsonArray) {
            java.util.ArrayList<nl.reinspanjer.kcp.admin.BrokerEntry> list =  new java.util.ArrayList<>();
            ((Iterable<Object>)member.getValue()).forEach( item -> {
              if (item instanceof JsonObject)
                list.add(new nl.reinspanjer.kcp.admin.BrokerEntry((io.vertx.core.json.JsonObject)item));
            });
            obj.setEntries(list);
          }
          break;
      }
    }
  }

   static void toJson(Broker obj, JsonObject json) {
    toJson(obj, json.getMap());
  }

   static void toJson(Broker obj, java.util.Map<String, Object> json) {
    if (obj.getEntries() != null) {
      JsonArray array = new JsonArray();
      obj.getEntries().forEach(item -> array.add(item.toJson()));
      json.put("entries", array);
    }
  }
}
