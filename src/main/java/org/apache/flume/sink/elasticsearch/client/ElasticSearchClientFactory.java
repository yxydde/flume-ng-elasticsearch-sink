/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch.client;

import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;

import java.net.UnknownHostException;

/**
 * Internal ElasticSearch client factory. Responsible for creating instance
 * of ElasticSearch clients.
 */
public class ElasticSearchClientFactory {
    public static final String TransportClient = "transport";
    public static final String RestClient = "rest";

    /**
     * @param clientType  String representation of client type
     * @param hostNames   Array of strings that represents hostnames with ports (hostname:port)
     * @param clusterName Elasticsearch cluster name used only by Transport Client
     * @param serializer  Serializer of flume events to elasticsearch documents
     * @return
     */
    public ElasticSearchClient getClient(String clientType, String[] hostNames,
                                         String clusterName, ElasticSearchEventSerializer serializer) throws NoSuchClientTypeException, UnknownHostException {
        if (clientType.equalsIgnoreCase(TransportClient) && serializer != null) {
            return new ElasticSearchTransportClient(hostNames, clusterName, serializer);
        } else if (clientType.equalsIgnoreCase(RestClient) && serializer != null) {
            return new ElasticSearchRestClient(hostNames, serializer);
        }
        throw new NoSuchClientTypeException();
    }

}
