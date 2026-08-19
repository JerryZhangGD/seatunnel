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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Getter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
final class JsonTemplateValue implements Serializable {

    static final String BATCH_DATA_PLACEHOLDER = "${batch.data}";

    private final ValueKind kind;
    private final SeaTunnelDataType<?> dataType;
    private final Object constantValue;
    private final String[] fieldNames;
    private final List<JsonTemplateValue> children;

    private JsonTemplateValue(
            ValueKind kind,
            SeaTunnelDataType<?> dataType,
            Object constantValue,
            String[] fieldNames,
            List<JsonTemplateValue> children) {
        this.kind = kind;
        this.dataType = dataType;
        this.constantValue = constantValue;
        this.fieldNames = fieldNames;
        this.children = children;
    }

    static JsonTemplateValue fromTemplate(
            Map<String, Object> template, SeaTunnelRowType inputRowType) {
        int[] placeholderCount = new int[1];
        JsonTemplateValue result =
                fromMap(template, "json.template", inputRowType, placeholderCount);
        if (placeholderCount[0] != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option 'json.template' must contain exactly one string placeholder '%s', but found %d.",
                            BATCH_DATA_PLACEHOLDER, placeholderCount[0]));
        }
        return result;
    }

    SeaTunnelRow resolveObject(SeaTunnelRow[] batchData) {
        return (SeaTunnelRow) resolve(batchData);
    }

    Object resolve(SeaTunnelRow[] batchData) {
        switch (kind) {
            case BATCH_DATA:
                return batchData;
            case OBJECT:
                return new SeaTunnelRow(
                        children.stream().map(child -> child.resolve(batchData)).toArray());
            case ARRAY:
                return children.stream().map(child -> child.resolve(batchData)).toArray();
            case CONSTANT:
            default:
                return constantValue;
        }
    }

    private static JsonTemplateValue from(
            Object value, String path, SeaTunnelRowType inputRowType, int[] placeholderCount) {
        if (BATCH_DATA_PLACEHOLDER.equals(value)) {
            placeholderCount[0]++;
            return new JsonTemplateValue(
                    ValueKind.BATCH_DATA,
                    new ArrayType<>(SeaTunnelRow[].class, inputRowType),
                    null,
                    null,
                    null);
        }
        if (value == null) {
            return constant(BasicType.STRING_TYPE, null);
        }
        if (value instanceof String || value instanceof Character) {
            return constant(BasicType.STRING_TYPE, value.toString());
        }
        if (value instanceof Boolean) {
            return constant(BasicType.BOOLEAN_TYPE, value);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return constant(BasicType.INT_TYPE, ((Number) value).intValue());
        }
        if (value instanceof Long) {
            return constant(BasicType.LONG_TYPE, value);
        }
        if (value instanceof Float || value instanceof Double) {
            return constant(BasicType.DOUBLE_TYPE, ((Number) value).doubleValue());
        }
        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            return constant(new DecimalType(decimal.precision(), decimal.scale()), decimal);
        }
        if (value instanceof Map) {
            return fromMap((Map<?, ?>) value, path, inputRowType, placeholderCount);
        }
        if (value instanceof List) {
            return fromList((List<?>) value, path, inputRowType, placeholderCount);
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported JSON template value at '%s': %s.",
                        path, value.getClass().getName()));
    }

    private static JsonTemplateValue fromMap(
            Map<?, ?> map, String path, SeaTunnelRowType inputRowType, int[] placeholderCount) {
        List<String> fieldNames = new ArrayList<>(map.size());
        List<JsonTemplateValue> children = new ArrayList<>(map.size());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String) || ((String) entry.getKey()).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("JSON object field names at '%s' must not be blank.", path));
            }
            String fieldName = (String) entry.getKey();
            fieldNames.add(fieldName);
            children.add(
                    from(entry.getValue(), path + "." + fieldName, inputRowType, placeholderCount));
        }

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        children.stream()
                                .map(JsonTemplateValue::getDataType)
                                .toArray(SeaTunnelDataType<?>[]::new));
        return new JsonTemplateValue(
                ValueKind.OBJECT, rowType, null, fieldNames.toArray(new String[0]), children);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static JsonTemplateValue fromList(
            List<?> list, String path, SeaTunnelRowType inputRowType, int[] placeholderCount) {
        List<JsonTemplateValue> children = new ArrayList<>(list.size());
        SeaTunnelDataType<?> elementType = null;
        for (int i = 0; i < list.size(); i++) {
            JsonTemplateValue child =
                    from(list.get(i), path + "[" + i + "]", inputRowType, placeholderCount);
            children.add(child);
            if (child.getConstantValue() == null && child.getKind() == ValueKind.CONSTANT) {
                continue;
            }
            if (elementType == null) {
                elementType = child.getDataType();
            } else if (!elementType.equals(child.getDataType())) {
                throw new IllegalArgumentException(
                        String.format(
                                "All values in JSON template array '%s' must have the same type.",
                                path));
            }
        }
        if (elementType == null) {
            elementType = BasicType.STRING_TYPE;
        }
        return new JsonTemplateValue(
                ValueKind.ARRAY, new ArrayType(Object[].class, elementType), null, null, children);
    }

    private static JsonTemplateValue constant(SeaTunnelDataType<?> type, Object value) {
        return new JsonTemplateValue(ValueKind.CONSTANT, type, value, null, null);
    }

    private enum ValueKind {
        CONSTANT,
        OBJECT,
        ARRAY,
        BATCH_DATA
    }
}
