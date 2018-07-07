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

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class ElasticSearchRestClient implements ElasticSearchClient {

    private static final String INDEX_OPERATION_NAME = "index";
    private static final String INDEX_PARAM = "_index";
    private static final String TYPE_PARAM = "_type";
    private static final String BULK_ENDPOINT = "_bulk";

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    private final ElasticSearchEventSerializer serializer;
    private final RoundRobinList<String> serversList;

    private StringBuilder bulkBuilder;
    private HttpClient httpClient;

    public ElasticSearchRestClient(String[] hostNames,
                                   ElasticSearchEventSerializer serializer) {

        for (int i = 0; i < hostNames.length; ++i) {
            if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
                hostNames[i] = "http://" + hostNames[i];
            }
        }
        this.serializer = serializer;

        serversList = new RoundRobinList<String>(Arrays.asList(hostNames));
        httpClient = HttpClients.createDefault();
        bulkBuilder = new StringBuilder();
    }


    @Override
    public void configure(Context context) {
    }

    @Override
    public void close() {
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param event            Flume Event
     * @param indexNameBuilder Index name builder which generates name of index to feed
     * @param indexType        Name of type of document which will be sent to the elasticsearch cluster
     * @throws Exception
     */
    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType) throws Exception {
        JSONObject content = serializer.getContent(event);
        Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
        Map<String, String> indexParameters = new HashMap<String, String>();
        indexParameters.put(INDEX_PARAM, indexNameBuilder.getIndexName(event));
        indexParameters.put(TYPE_PARAM, indexType);
        parameters.put(INDEX_OPERATION_NAME, indexParameters);

        synchronized (bulkBuilder) {
            bulkBuilder.append(JSONObject.toJSON(parameters));
            bulkBuilder.append("\n");
            bulkBuilder.append(content.toJSONString());
            bulkBuilder.append("\n");
        }
    }

    @Override
    public void execute() throws Exception {
        int statusCode = 0, triesCount = 0;
        HttpResponse response = null;
        String entity;
        synchronized (bulkBuilder) {
            entity = bulkBuilder.toString();
            bulkBuilder = new StringBuilder();
        }

        while (statusCode != HttpStatus.SC_OK && triesCount < serversList.size()) {
            triesCount++;
            String host = serversList.get();
            String url = host + "/" + BULK_ENDPOINT;
            logger.debug("Call Rest Api:" + url);
            logger.debug("Post Entity:" + entity);
            HttpPost httpRequest = new HttpPost(url);
            httpRequest.setHeader("Content-Type", "application/x-ndjson");
            httpRequest.setEntity(new StringEntity(entity));
            response = httpClient.execute(httpRequest);
            statusCode = response.getStatusLine().getStatusCode();
            logger.debug("Status code from elasticsearch: " + statusCode);
            if (response.getEntity() != null) {
                logger.debug("Status message from elasticsearch: " +
                        EntityUtils.toString(response.getEntity(), "UTF-8"));
            }
        }

        if (statusCode != HttpStatus.SC_OK) {
            if (response.getEntity() != null) {
                throw new EventDeliveryException(EntityUtils.toString(response.getEntity(), "UTF-8"));
            } else {
                throw new EventDeliveryException("Elasticsearch status code was: " + statusCode);
            }
        }
    }
}
