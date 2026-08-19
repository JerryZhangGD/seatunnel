/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.batchjson;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
public class BatchJsonTransformConfig implements Serializable {

    public static final String PLUGIN_NAME = "BatchJson";

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of input rows in each output JSON object.");

    public static final Option<Map<String, Object>> JSON_TEMPLATE =
            Options.key("json.template")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The output JSON template containing exactly one ${batch.data} placeholder.");

    public static final Option<List<Map<String, String>>> BATCH_FIELDS =
            Options.key("batch.fields")
                    .type(new TypeReference<List<Map<String, String>>>() {})
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "Optional source, target and type mappings for fields inside ${batch.data}.");

    private final int batchSize;
    private final Map<String, Object> jsonTemplate;
    private final List<Map<String, String>> batchFields;

    private BatchJsonTransformConfig(
            int batchSize,
            Map<String, Object> jsonTemplate,
            List<Map<String, String>> batchFields) {
        this.batchSize = batchSize;
        this.jsonTemplate = jsonTemplate;
        this.batchFields = batchFields;
    }

    public static BatchJsonTransformConfig of(ReadonlyConfig config) {
        int batchSize = config.get(BATCH_SIZE);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Option 'batch.size' must be greater than 0.");
        }

        Map<String, Object> configuredTemplate = config.get(JSON_TEMPLATE);
        if (configuredTemplate == null || configuredTemplate.isEmpty()) {
            throw new IllegalArgumentException("Option 'json.template' must not be empty.");
        }

        List<Map<String, String>> configuredFields = config.get(BATCH_FIELDS);
        List<Map<String, String>> batchFields = new ArrayList<>(configuredFields.size());
        for (Map<String, String> field : configuredFields) {
            batchFields.add(new LinkedHashMap<>(field));
        }

        return new BatchJsonTransformConfig(
                batchSize, new LinkedHashMap<>(configuredTemplate), batchFields);
    }
}
