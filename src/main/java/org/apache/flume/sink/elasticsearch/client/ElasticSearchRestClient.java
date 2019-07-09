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
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class ElasticSearchRestClient implements ElasticSearchClient {

    private static final String INDEX_OPERATION_NAME = "index";
    private static final String INDEX_OPERATION = "index";
    private static final String UPDATE_OPERATION_NAME = "update";
    private static final String UPSERT_OPERATION = "upsert";
    private static final String OPERATION = "operation";
    private static final String INDEX_PARAM = "_index";
    private static final String BULK_ENDPOINT = "/_bulk";
    private static final String DOCUMENT_ID = "_id";

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    private final ElasticSearchEventSerializer serializer;
    private final RestClient restClient;

    private StringBuilder bulkBuilder;

    private String docIdName;
    private String operation;

    public ElasticSearchRestClient(String[] hostNames,
                                   ElasticSearchEventSerializer serializer) {

        ;

        HttpHost[] hosts = new HttpHost[hostNames.length];
        for (int i = 0; i < hostNames.length; ++i) {
            if (!hostNames[i].contains("http://") && !hostNames[i].contains("https://")) {
                hosts[i] = HttpHost.create("http://" + hostNames[i]);
            }
        }
        this.restClient = RestClient.builder(hosts).build();
        this.serializer = serializer;
        this.docIdName = serializer.getDocmentIdName();
        bulkBuilder = new StringBuilder();
    }


    @Override
    public void configure(Context context) {
        operation = context.getString(OPERATION, UPSERT_OPERATION);
    }

    @Override
    public void close() {
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param event            Flume Event
     * @param indexNameBuilder Index name builder which generates name of index to feed
     * @throws Exception
     */
    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder) throws Exception {
        JSONObject content = serializer.getContent(event);
        if (content == null) {
            return;
        }
        String indexName = indexNameBuilder.getIndexName(event);
        String eventOp = event.getHeaders().getOrDefault("operation", operation);
        if (INDEX_OPERATION.equals(eventOp)) {
            index(content, indexName);
        } else if (UPSERT_OPERATION.equals(eventOp)) {
            upsert(content, indexName);
        }
    }

    public void index(JSONObject content, String indexName) {
        if (content == null) {
            return;
        }
        logger.debug("Index Doc: " + content);
        Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
        Map<String, String> indexParameters = new HashMap<String, String>();
        indexParameters.put(INDEX_PARAM, indexName);
        if (docIdName != null && !"".equals(docIdName.trim())) {
            String id = content.getString(docIdName);
            if (id == null || "".equals(id.trim())) {
                logger.warn("doc _id is empty: " + content);
                return;
            }
            indexParameters.put(DOCUMENT_ID, id);
        }
        parameters.put(INDEX_OPERATION_NAME, indexParameters);

        synchronized (bulkBuilder) {
            bulkBuilder.append(JSONObject.toJSON(parameters));
            bulkBuilder.append("\n");
            bulkBuilder.append(content.toJSONString());
            bulkBuilder.append("\n");
        }
    }

    public void upsert(JSONObject content, String indexName) {
        if (content == null) {
            return;
        }
        logger.debug("Update Doc: " + content);
        Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
        Map<String, String> indexParameters = new HashMap<String, String>();
        indexParameters.put(INDEX_PARAM, indexName);
        if (docIdName != null && !"".equals(docIdName.trim())) {
            String id = content.getString(docIdName);
            if (id == null || "".equals(id.trim())) {
                logger.warn("doc _id is empty: " + content);
                return;
            }
            indexParameters.put(DOCUMENT_ID, id);
        }
        parameters.put(UPDATE_OPERATION_NAME, indexParameters);
        synchronized (bulkBuilder) {
            bulkBuilder.append(JSONObject.toJSON(parameters));
            bulkBuilder.append("\n");
            JSONObject doc = new JSONObject();
            doc.put("doc", content);
            doc.put("doc_as_upsert", true);
            bulkBuilder.append(doc.toJSONString());
            bulkBuilder.append("\n");
        }
    }

    @Override
    public void execute() throws Exception {

        String entity;
        synchronized (bulkBuilder) {
            entity = bulkBuilder.toString();
            bulkBuilder = new StringBuilder();
        }

        try {
            Request request = new Request("POST", BULK_ENDPOINT);
            request.setJsonEntity(entity);
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.debug("RestClient Response: " + responseBody);
        } catch (ResponseException e) {
            logger.error("Request Entity: " + entity, e);
            throw new EventDeliveryException(e.fillInStackTrace());
        } catch (IOException e) {
            logger.error("Request Entity: " + entity, e);
            throw new EventDeliveryException(e.fillInStackTrace());
        }

    }
}
