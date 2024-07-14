/*
* Copyright 2014 Red Hat, Inc.
*
* Red Hat licenses this file to you under the Apache License, version 2.0
* (the "License"); you may not use this file except in compliance with the
* License. You may obtain a copy of the License at:
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*/

package nl.reinspanjer.kcp.verticles;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.function.Function;
import io.vertx.serviceproxy.ServiceException;
import io.vertx.serviceproxy.ServiceExceptionMessageCodec;
import io.vertx.serviceproxy.ProxyUtils;

import nl.reinspanjer.kcp.verticles.KafkaCacheService;
import nl.reinspanjer.kcp.admin.Config;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.admin.Broker;
import io.vertx.core.Future;
/*
  Generated Proxy code - DO NOT EDIT
  @author Roger the Robot
*/

@SuppressWarnings({"unchecked", "rawtypes"})
public class KafkaCacheServiceVertxEBProxy implements KafkaCacheService {
  private Vertx _vertx;
  private String _address;
  private DeliveryOptions _options;
  private boolean closed;

  public KafkaCacheServiceVertxEBProxy(Vertx vertx, String address) {
    this(vertx, address, null);
  }

  public KafkaCacheServiceVertxEBProxy(Vertx vertx, String address, DeliveryOptions options) {
    this._vertx = vertx;
    this._address = address;
    this._options = options;
    try {
      this._vertx.eventBus().registerDefaultCodec(ServiceException.class, new ServiceExceptionMessageCodec());
    } catch (IllegalStateException ex) {
    }
  }

  @Override
  public Future<Config> getConfig(String topicName){
    if (closed) return io.vertx.core.Future.failedFuture("Proxy is closed");
    JsonObject _json = new JsonObject();
    _json.put("topicName", topicName);

    DeliveryOptions _deliveryOptions = (_options != null) ? new DeliveryOptions(_options) : new DeliveryOptions();
    _deliveryOptions.addHeader("action", "getConfig");
    _deliveryOptions.getHeaders().set("action", "getConfig");
    return _vertx.eventBus().<JsonObject>request(_address, _json, _deliveryOptions).map(msg -> {
      return msg.body() != null ? new nl.reinspanjer.kcp.admin.Config((JsonObject)msg.body()) : null;
    });
  }
  @Override
  public Future<Void> update(){
    if (closed) return io.vertx.core.Future.failedFuture("Proxy is closed");
    JsonObject _json = new JsonObject();

    DeliveryOptions _deliveryOptions = (_options != null) ? new DeliveryOptions(_options) : new DeliveryOptions();
    _deliveryOptions.addHeader("action", "update");
    _deliveryOptions.getHeaders().set("action", "update");
    return _vertx.eventBus().<Void>request(_address, _json, _deliveryOptions).map(msg -> {
      return msg.body();
    });
  }
  @Override
  public Future<Broker> getBrokers(){
    if (closed) return io.vertx.core.Future.failedFuture("Proxy is closed");
    JsonObject _json = new JsonObject();

    DeliveryOptions _deliveryOptions = (_options != null) ? new DeliveryOptions(_options) : new DeliveryOptions();
    _deliveryOptions.addHeader("action", "getBrokers");
    _deliveryOptions.getHeaders().set("action", "getBrokers");
    return _vertx.eventBus().<JsonObject>request(_address, _json, _deliveryOptions).map(msg -> {
      return msg.body() != null ? new nl.reinspanjer.kcp.admin.Broker((JsonObject)msg.body()) : null;
    });
  }
}
