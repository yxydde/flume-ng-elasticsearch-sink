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
package org.apache.flume.sink.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields</p>
 * <p>
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchJSONSerializer implements
        ElasticSearchEventSerializer {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchJSONSerializer.class);


    private static String DOCUMENT_ID = "documentIdName";
    private static String ADD_TIMESTAMP = "addTimeStamp";
    private static String TIMESTAMP_FORMAT = "timeStampFromat";
    private static String USE_SHA1_DOC_ID = "useSHA1DocumnetID";
    private static String HEADERS_TO_SAVE = "headersToSave";

    private boolean addTimeStamp;
    private DateFormat dateFormat;
    private String timeStampFormat;

    private boolean useSHA1DocumnetID;

    private List<String> saveHeaders;

    private String docIdName;


    @Override
    public void configure(Context context) {
        addTimeStamp = context.getBoolean(ADD_TIMESTAMP, false);
        useSHA1DocumnetID = context.getBoolean(USE_SHA1_DOC_ID, false);
        timeStampFormat = context.getString(TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        if (addTimeStamp) {
            dateFormat = new SimpleDateFormat(timeStampFormat);
        }

        String headersToSave = context.getString(HEADERS_TO_SAVE, null);
        if (headersToSave != null && !"".equals(headersToSave.trim())) {
            String[] array = headersToSave.split(",");
            saveHeaders = new ArrayList<String>();
            for (String header : array) {
                saveHeaders.add(header.trim());
            }
        }

        docIdName = context.getString(DOCUMENT_ID, null);
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }

    @Override
    public JSONObject getContent(Event event) throws IOException {
        JSONObject content = null;
        String raw = new String(event.getBody(), "UTF-8");
        try {
            content = JSONObject.parseObject(raw);
            if (useSHA1DocumnetID) {
                content.put(docIdName, DigestUtils.sha1Hex(raw));
            }
            if (addTimeStamp) {
                content.put("@timestamp", dateFormat.format(new Date()));
            }
            if (saveHeaders != null && saveHeaders.size() > 0) {
                for (String header : saveHeaders) {
                    content.put(header, event.getHeaders().get(header));
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e.fillInStackTrace());
            logger.error(raw);
        }
        return content;
    }

    public String getDocmentIdName() {
        return docIdName;
    }
}
