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

package nl.reinspanjer.kcp.control.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import nl.reinspanjer.kcp.config.ApplicationConfig;
import nl.reinspanjer.kcp.config.ProxyConfig;
import nl.reinspanjer.kcp.control.TransformResponseNode;
import nl.reinspanjer.kcp.data.Address;
import nl.reinspanjer.kcp.data.BrokerOriginMap;
import nl.reinspanjer.kcp.request.RequestHeaderAndPayload;
import nl.reinspanjer.kcp.response.ResponseHeaderAndPayload;
import nl.reinspanjer.kcp.utils.LogUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class TransformHosts implements TransformResponseNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformHosts.class);
    private static final ApplicationConfig config = ProxyConfig.build();
    private static final BrokerOriginMap originConfig = BrokerOriginMap.build();
    private Vertx vertx;

    @Override
    public TransformHosts init(Vertx vertx) {
        this.vertx = vertx;
        return this;
    }

    @Override
    public Future<ResponseHeaderAndPayload> response(RequestHeaderAndPayload request, ResponseHeaderAndPayload response) {
        AbstractResponse res = response.response;

        if (request.header.apiKey() == ApiKeys.METADATA) {
            transformMetadataResponse((MetadataResponse) res);
        } else if (request.header.apiKey() == ApiKeys.FIND_COORDINATOR) {
            transformFindCoordinatorResponse(response.header, (FindCoordinatorResponse) res, request.request);
        } else if (request.header.apiKey() == ApiKeys.DESCRIBE_CLUSTER) {
            transformDescribeClusterResponse((DescribeClusterResponse) res);
        }

        return Future.succeededFuture(response);
    }

    private void transformMetadataResponse(MetadataResponse metaDataResponse) {
        MetadataResponseData.MetadataResponseBrokerCollection brokers = new MetadataResponseData.MetadataResponseBrokerCollection();
        metaDataResponse.brokers().forEach(broker -> {
            brokers.add(transformBroker(broker));
        });
        metaDataResponse.data().setBrokers(brokers);
    }

    private void transformFindCoordinatorResponse(ResponseHeader header, FindCoordinatorResponse findCoordinatorResponse, AbstractRequest req) {
        if (findCoordinatorResponse.hasError()) {
            LOGGER.error("Error in {}: {}", ApiKeys.FIND_COORDINATOR, findCoordinatorResponse.data().errorMessage());
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            ByteBuffer buffer = RequestUtils.serialize(header.data(), header.headerVersion(), findCoordinatorResponse.data(), req.version());
            LogUtils.hexDump("before transformation", buffer);
        }


        FindCoordinatorResponseData data = findCoordinatorResponse.data();
        if (!data.host().isEmpty()) {
            LOGGER.error("Unexpected host in {}: {}", ApiKeys.FIND_COORDINATOR, data.host());
        }

        List<FindCoordinatorResponseData.Coordinator> findCoordinatorResponses = data.coordinators();
        if (findCoordinatorResponses.isEmpty()) {
            LOGGER.error("No coordinators found in {}", ApiKeys.FIND_COORDINATOR);
        }

        findCoordinatorResponses.forEach(coordinator -> {
            Address origin = new Address(coordinator.host(), coordinator.port());
            Address newOrigin = transformAddress(origin);
            if (newOrigin == null) {
                LOGGER.error("Unknown coordinator node seen in {}: {}:{}", ApiKeys.FIND_COORDINATOR, coordinator.host(), coordinator.port());
            }
            LOGGER.info("Transforming coordinator " + origin + " to " + newOrigin);
            coordinator.setHost(newOrigin.getHost());
            coordinator.setPort(newOrigin.getPort());
        });

        if (LOGGER.isTraceEnabled()) {
            ByteBuffer buffer = RequestUtils.serialize(header.data(), header.headerVersion(), findCoordinatorResponse.data(), req.version());
            LogUtils.hexDump("after transformation", buffer);
        }
    }

    private void transformDescribeClusterResponse(DescribeClusterResponse describeClusterResponse) {
        describeClusterResponse.data().brokers().forEach(broker -> {
            Address origin = new Address(broker.host(), broker.port());
            Address newOrigin = transformAddress(origin);
            broker.setHost(newOrigin.getHost());
            broker.setPort(newOrigin.getPort());
            LOGGER.info("Transforming cluster broker " + broker.brokerId() + origin + " to " + newOrigin);
        });
    }

    private MetadataResponseData.MetadataResponseBroker transformBroker(Node node) {
        Address origin = new Address(node.host(), node.port());
        Address newOrigin = transformAddress(origin);
        LOGGER.debug("Transforming broker " + origin + " to " + newOrigin);

        MetadataResponseData.MetadataResponseBroker broker = new MetadataResponseData.MetadataResponseBroker();
        broker.setHost(newOrigin.getHost());
        broker.setPort(newOrigin.getPort());
        broker.setNodeId(node.id());
        broker.setRack(node.rack());
        return broker;
    }

    private Address transformAddress(Address origin) {
        return originConfig.getServerFromBroker(origin);
    }
}
