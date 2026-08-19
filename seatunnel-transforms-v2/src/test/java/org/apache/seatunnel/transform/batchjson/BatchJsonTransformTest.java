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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class BatchJsonTransformTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void shouldBatchRowsAndSerializeExpectedJson() throws Exception {
        BatchJsonTransform transform =
                new BatchJsonTransform(exampleConfig(), createInputCatalogTable());

        Assertions.assertTrue(transform.flatMap(row(1, "Zhang San", 25)).isEmpty());
        Assertions.assertTrue(transform.flatMap(row(2, "Li Si", 30)).isEmpty());
        List<SeaTunnelRow> output = transform.flatMap(row(3, "Wang Wu", 28));

        Assertions.assertEquals(1, output.size());
        SeaTunnelRowType outputType = transform.getProducedCatalogTable().getSeaTunnelRowType();
        Assertions.assertArrayEquals(
                new String[] {"data", "status", "metadata", "request_info"},
                outputType.getFieldNames());
        Assertions.assertTrue(outputType.getFieldType(0) instanceof ArrayType);

        String json = serialize(outputType, output.get(0));
        JsonNode root = OBJECT_MAPPER.readTree(json);

        Assertions.assertEquals(3, root.get("data").size());
        Assertions.assertEquals(1, root.get("data").get(0).get("id").asInt());
        Assertions.assertEquals("Zhang San", root.get("data").get(0).get("name").asText());
        Assertions.assertEquals(30, root.get("data").get(1).get("age").asInt());
        Assertions.assertEquals("Wang Wu", root.get("data").get(2).get("name").asText());
        Assertions.assertEquals("active", root.get("status").asText());
        Assertions.assertEquals("mysql", root.get("metadata").get("source").asText());
        Assertions.assertEquals("1.0", root.get("metadata").get("version").asText());
        Assertions.assertEquals("a1b2c3d4", root.get("request_info").get("request_id").asText());
        Assertions.assertEquals(
                "2026-08-19 10:00:00", root.get("request_info").get("timestamp").asText());
    }

    @Test
    void shouldPlaceBatchDataAtNestedTemplateLocation() throws Exception {
        ReadonlyConfig nestedConfig =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "batch.size = 2\n"
                                        + "json.template {\n"
                                        + "  status = active\n"
                                        + "  response {\n"
                                        + "    metadata { source = mysql }\n"
                                        + "    records = \"${batch.data}\"\n"
                                        + "  }\n"
                                        + "  request_id = a1b2c3d4\n"
                                        + "}"));
        BatchJsonTransform transform =
                new BatchJsonTransform(nestedConfig, createInputCatalogTable());

        Assertions.assertTrue(transform.flatMap(row(1, "one", 1)).isEmpty());
        SeaTunnelRow output = transform.flatMap(row(2, "two", 2)).get(0);
        SeaTunnelRowType outputType = transform.getProducedCatalogTable().getSeaTunnelRowType();
        Assertions.assertArrayEquals(
                new String[] {"status", "response", "request_id"}, outputType.getFieldNames());

        JsonNode root = OBJECT_MAPPER.readTree(serialize(outputType, output));
        Assertions.assertEquals("active", root.get("status").asText());
        Assertions.assertEquals(
                "mysql", root.get("response").get("metadata").get("source").asText());
        Assertions.assertEquals(2, root.get("response").get("records").size());
        Assertions.assertEquals(1, root.get("response").get("records").get(0).get("id").asInt());
        Assertions.assertEquals("a1b2c3d4", root.get("request_id").asText());
    }

    @Test
    void shouldRenameSelectAndConvertBatchDataFields() throws Exception {
        ReadonlyConfig mappedConfig =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "batch.size = 2\n"
                                        + "batch.fields = [\n"
                                        + "  { source = id, target = user_id, type = STRING },\n"
                                        + "  { source = name, target = full_name },\n"
                                        + "  { source = age, target = years, type = BIGINT }\n"
                                        + "]\n"
                                        + "json.template { data = \"${batch.data}\" }"));
        BatchJsonTransform transform =
                new BatchJsonTransform(mappedConfig, createInputCatalogTable());

        Assertions.assertTrue(transform.flatMap(row(1, "one", 20)).isEmpty());
        SeaTunnelRow output = transform.flatMap(row(2, "two", 30)).get(0);
        SeaTunnelRowType outputType = transform.getProducedCatalogTable().getSeaTunnelRowType();
        ArrayType<?, ?> dataType = (ArrayType<?, ?>) outputType.getFieldType(0);
        SeaTunnelRowType itemType = (SeaTunnelRowType) dataType.getElementType();

        Assertions.assertArrayEquals(
                new String[] {"user_id", "full_name", "years"}, itemType.getFieldNames());
        Assertions.assertEquals(BasicType.STRING_TYPE, itemType.getFieldType(0));
        Assertions.assertEquals(BasicType.STRING_TYPE, itemType.getFieldType(1));
        Assertions.assertEquals(BasicType.LONG_TYPE, itemType.getFieldType(2));

        JsonNode root = OBJECT_MAPPER.readTree(serialize(outputType, output));
        Assertions.assertEquals("1", root.get("data").get(0).get("user_id").asText());
        Assertions.assertEquals("two", root.get("data").get(1).get("full_name").asText());
        Assertions.assertEquals(30L, root.get("data").get(1).get("years").asLong());
        Assertions.assertNull(root.get("data").get(0).get("name"));
    }

    @Test
    void shouldThrowClearErrorWhenBatchFieldConversionFails() {
        ReadonlyConfig mappedConfig =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "batch.size = 1\n"
                                        + "batch.fields = ["
                                        + "  { source = name, target = user_id, type = INT }"
                                        + "]\n"
                                        + "json.template { data = \"${batch.data}\" }"));
        BatchJsonTransform transform =
                new BatchJsonTransform(mappedConfig, createInputCatalogTable());

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> transform.flatMap(row(1, "not-a-number", 20)));

        Assertions.assertTrue(error.getMessage().contains("source field 'name'"));
        Assertions.assertTrue(error.getMessage().contains("output field 'user_id'"));
        Assertions.assertTrue(error.getMessage().contains("as INT"));
        Assertions.assertTrue(error.getMessage().contains("value='not-a-number'"));
    }

    @Test
    void shouldStartANewBatchAfterEmission() {
        BatchJsonTransform transform =
                new BatchJsonTransform(exampleConfig(), createInputCatalogTable());

        transform.flatMap(row(1, "one", 1));
        transform.flatMap(row(2, "two", 2));
        transform.flatMap(row(3, "three", 3));
        Assertions.assertTrue(transform.flatMap(row(4, "four", 4)).isEmpty());
        Assertions.assertTrue(transform.flatMap(row(5, "five", 5)).isEmpty());

        List<SeaTunnelRow> secondBatch = transform.flatMap(row(6, "six", 6));
        SeaTunnelRow[] data = (SeaTunnelRow[]) secondBatch.get(0).getField(0);
        Assertions.assertEquals(3, data.length);
        Assertions.assertEquals(4, data[0].getField(0));
        Assertions.assertEquals(6, data[2].getField(0));
    }

    @Test
    void shouldFlushRemainingRowsWhenInputFinishes() {
        BatchJsonTransform transform =
                new BatchJsonTransform(exampleConfig(), createInputCatalogTable());

        transform.flatMap(row(1, "one", 1));
        transform.flatMap(row(2, "two", 2));

        List<SeaTunnelRow> output = transform.finish();
        Assertions.assertEquals(1, output.size());
        SeaTunnelRow[] data = (SeaTunnelRow[]) output.get(0).getField(0);
        Assertions.assertEquals(2, data.length);
        Assertions.assertEquals(1, data[0].getField(0));
        Assertions.assertEquals(2, data[1].getField(0));
        Assertions.assertTrue(transform.finish().isEmpty());
    }

    @Test
    void shouldRestoreBufferedRowsFromCheckpoint() {
        BatchJsonTransform original =
                new BatchJsonTransform(exampleConfig(), createInputCatalogTable());
        original.flatMap(row(1, "one", 1));
        byte[] state = original.snapshotState(1L);

        BatchJsonTransform restored =
                new BatchJsonTransform(exampleConfig(), createInputCatalogTable());
        restored.restoreState(state);
        Assertions.assertTrue(restored.flatMap(row(2, "two", 2)).isEmpty());
        List<SeaTunnelRow> output = restored.flatMap(row(3, "three", 3));

        SeaTunnelRow[] data = (SeaTunnelRow[]) output.get(0).getField(0);
        Assertions.assertEquals(3, data.length);
        Assertions.assertEquals(1, data[0].getField(0));
        Assertions.assertEquals(2, data[1].getField(0));
        Assertions.assertEquals(3, data[2].getField(0));
    }

    @Test
    void shouldRestoreMultiCatalogTransformState() {
        BatchJsonMultiCatalogTransform original =
                new BatchJsonMultiCatalogTransform(
                        Collections.singletonList(createInputCatalogTable()), exampleConfig());
        original.flatMap(row(1, "one", 1));
        byte[] state = original.snapshotState(1L);

        BatchJsonMultiCatalogTransform restored =
                new BatchJsonMultiCatalogTransform(
                        Collections.singletonList(createInputCatalogTable()), exampleConfig());
        restored.restoreState(state);
        restored.flatMap(row(2, "two", 2));
        List<SeaTunnelRow> output = restored.finish();

        SeaTunnelRow[] data = (SeaTunnelRow[]) output.get(0).getField(0);
        Assertions.assertEquals(2, data.length);
        Assertions.assertEquals(1, data[0].getField(0));
        Assertions.assertEquals(2, data[1].getField(0));
    }

    @Test
    void shouldRejectInvalidConfiguration() {
        Map<String, Object> invalidSize = new LinkedHashMap<>();
        invalidSize.put(BatchJsonTransformConfig.BATCH_SIZE.key(), 0);
        IllegalArgumentException sizeError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BatchJsonTransform(
                                        ReadonlyConfig.fromMap(invalidSize),
                                        createInputCatalogTable()));
        Assertions.assertTrue(sizeError.getMessage().contains("batch.size"));

        Map<String, Object> missingPlaceholder = new LinkedHashMap<>();
        missingPlaceholder.put(BatchJsonTransformConfig.BATCH_SIZE.key(), 3);
        missingPlaceholder.put(
                BatchJsonTransformConfig.JSON_TEMPLATE.key(),
                Collections.singletonMap("payload", "fixed"));
        IllegalArgumentException missingPlaceholderError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BatchJsonTransform(
                                        ReadonlyConfig.fromMap(missingPlaceholder),
                                        createInputCatalogTable()));
        Assertions.assertTrue(missingPlaceholderError.getMessage().contains("exactly one"));

        Map<String, Object> duplicatePlaceholder = new LinkedHashMap<>();
        duplicatePlaceholder.put(BatchJsonTransformConfig.BATCH_SIZE.key(), 3);
        Map<String, Object> duplicateTemplate = new LinkedHashMap<>();
        duplicateTemplate.put("first", JsonTemplateValue.BATCH_DATA_PLACEHOLDER);
        duplicateTemplate.put("second", JsonTemplateValue.BATCH_DATA_PLACEHOLDER);
        duplicatePlaceholder.put(BatchJsonTransformConfig.JSON_TEMPLATE.key(), duplicateTemplate);
        IllegalArgumentException duplicatePlaceholderError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BatchJsonTransform(
                                        ReadonlyConfig.fromMap(duplicatePlaceholder),
                                        createInputCatalogTable()));
        Assertions.assertTrue(duplicatePlaceholderError.getMessage().contains("found 2"));

        Map<String, Object> unknownSource = new LinkedHashMap<>();
        unknownSource.put(BatchJsonTransformConfig.BATCH_SIZE.key(), 3);
        unknownSource.put(
                BatchJsonTransformConfig.JSON_TEMPLATE.key(),
                Collections.singletonMap("data", JsonTemplateValue.BATCH_DATA_PLACEHOLDER));
        unknownSource.put(
                BatchJsonTransformConfig.BATCH_FIELDS.key(),
                Collections.singletonList(fieldMapping("missing_field", "output_field", "STRING")));
        IllegalArgumentException unknownSourceError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BatchJsonTransform(
                                        ReadonlyConfig.fromMap(unknownSource),
                                        createInputCatalogTable()));
        Assertions.assertTrue(unknownSourceError.getMessage().contains("missing_field"));

        Map<String, Object> duplicateTarget = new LinkedHashMap<>();
        duplicateTarget.put(BatchJsonTransformConfig.BATCH_SIZE.key(), 3);
        duplicateTarget.put(
                BatchJsonTransformConfig.JSON_TEMPLATE.key(),
                Collections.singletonMap("data", JsonTemplateValue.BATCH_DATA_PLACEHOLDER));
        List<Map<String, String>> duplicateTargetFields = new ArrayList<>();
        duplicateTargetFields.add(fieldMapping("id", "same_name", null));
        duplicateTargetFields.add(fieldMapping("name", "same_name", null));
        duplicateTarget.put(BatchJsonTransformConfig.BATCH_FIELDS.key(), duplicateTargetFields);
        IllegalArgumentException duplicateTargetError =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new BatchJsonTransform(
                                        ReadonlyConfig.fromMap(duplicateTarget),
                                        createInputCatalogTable()));
        Assertions.assertTrue(duplicateTargetError.getMessage().contains("same_name"));
    }

    @Test
    void factoryShouldExposePluginOptions() {
        BatchJsonTransformFactory factory = new BatchJsonTransformFactory();
        Assertions.assertEquals("BatchJson", factory.factoryIdentifier());
        Assertions.assertTrue(
                factory.optionRule().getRequiredOptions().stream()
                        .anyMatch(
                                option ->
                                        option.getOptions()
                                                .contains(BatchJsonTransformConfig.BATCH_SIZE)));
        Assertions.assertTrue(
                factory.optionRule().getRequiredOptions().stream()
                        .anyMatch(
                                option ->
                                        option.getOptions()
                                                .contains(BatchJsonTransformConfig.JSON_TEMPLATE)));
        Assertions.assertTrue(
                factory.optionRule().getOptionalOptions().stream()
                        .anyMatch(option -> option.equals(BatchJsonTransformConfig.BATCH_FIELDS)));
    }

    private ReadonlyConfig exampleConfig() {
        return ReadonlyConfig.fromConfig(
                ConfigFactory.parseString(
                        "batch.size = 3\n"
                                + "json.template {\n"
                                + "  data = \"${batch.data}\"\n"
                                + "  status = active\n"
                                + "  metadata { source = mysql, version = \"1.0\" }\n"
                                + "  request_info { request_id = a1b2c3d4, timestamp = \"2026-08-19 10:00:00\" }\n"
                                + "}"));
    }

    private String serialize(SeaTunnelRowType outputType, SeaTunnelRow row) {
        return new String(
                new JsonSerializationSchema(outputType).serialize(row), StandardCharsets.UTF_8);
    }

    private CatalogTable createInputCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0L, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0L, false, null, null))
                        .column(PhysicalColumn.of("age", BasicType.INT_TYPE, 0L, false, null, null))
                        .build(),
                new LinkedHashMap<>(),
                new ArrayList<>(),
                "");
    }

    private SeaTunnelRow row(int id, String name, int age) {
        return new SeaTunnelRow(new Object[] {id, name, age});
    }

    private Map<String, String> fieldMapping(String source, String target, String type) {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("source", source);
        mapping.put("target", target);
        if (type != null) {
            mapping.put("type", type);
        }
        return mapping;
    }
}
