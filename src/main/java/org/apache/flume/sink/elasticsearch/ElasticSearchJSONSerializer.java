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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;

import java.io.IOException;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields</p>
 * <p>
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchJSONSerializer implements
        ElasticSearchEventSerializer {

    @Override
    public void configure(Context context) {
        // NO-OP...
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }

    @Override
    public JSONObject getContent(Event event) throws IOException {
        String raw = new String(event.getBody(), charset);
        return JSONObject.parseObject(raw);
    }

}
