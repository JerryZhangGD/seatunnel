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

import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Maps and converts fields before rows are inserted into the ${batch.data} array. */
final class BatchDataFieldMapper implements Serializable {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String TYPE = "type";

    private final SeaTunnelRowType outputRowType;
    private final List<FieldMapping> mappings;
    private final boolean identity;

    private BatchDataFieldMapper(
            SeaTunnelRowType outputRowType, List<FieldMapping> mappings, boolean identity) {
        this.outputRowType = outputRowType;
        this.mappings = mappings;
        this.identity = identity;
    }

    static BatchDataFieldMapper of(
            SeaTunnelRowType inputRowType, List<Map<String, String>> configuredFields) {
        if (configuredFields.isEmpty()) {
            return new BatchDataFieldMapper(inputRowType, new ArrayList<>(), true);
        }

        String[] outputNames = new String[configuredFields.size()];
        SeaTunnelDataType<?>[] outputTypes = new SeaTunnelDataType<?>[configuredFields.size()];
        List<FieldMapping> mappings = new ArrayList<>(configuredFields.size());
        Set<String> targetNames = new HashSet<>();

        for (int i = 0; i < configuredFields.size(); i++) {
            Map<String, String> field = configuredFields.get(i);
            String configPath = "batch.fields[" + i + "]";
            validateKeys(field, configPath);

            String source = required(field.get(SOURCE), configPath + ".source");
            int sourceIndex;
            try {
                sourceIndex = inputRowType.indexOf(source);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Option '%s.source' refers to unknown input field '%s'.",
                                configPath, source),
                        e);
            }

            String target = optional(field.get(TARGET), source);
            if (!targetNames.add(target)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Option 'batch.fields' contains duplicate output field '%s'.",
                                target));
            }

            SeaTunnelDataType<?> sourceType = inputRowType.getFieldType(sourceIndex);
            String configuredType = field.get(TYPE);
            SeaTunnelDataType<?> targetType =
                    configuredType == null || configuredType.trim().isEmpty()
                            ? sourceType
                            : parseType(target, configuredType, configPath);

            outputNames[i] = target;
            outputTypes[i] = targetType;
            mappings.add(new FieldMapping(source, sourceIndex, target, sourceType, targetType));
        }

        return new BatchDataFieldMapper(
                new SeaTunnelRowType(outputNames, outputTypes), mappings, false);
    }

    SeaTunnelRowType getOutputRowType() {
        return outputRowType;
    }

    SeaTunnelRow map(SeaTunnelRow inputRow) {
        if (identity) {
            return inputRow.copy();
        }

        Object[] fields = new Object[mappings.size()];
        for (int i = 0; i < mappings.size(); i++) {
            FieldMapping mapping = mappings.get(i);
            Object value = inputRow.getField(mapping.sourceIndex);
            fields[i] = mapping.convert(value);
        }

        SeaTunnelRow outputRow = new SeaTunnelRow(fields);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        outputRow.setOptions(inputRow.getOptions());
        return outputRow;
    }

    private static void validateKeys(Map<String, String> field, String configPath) {
        for (String key : field.keySet()) {
            if (!SOURCE.equals(key) && !TARGET.equals(key) && !TYPE.equals(key)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown option '%s.%s'; supported keys are source, target and type.",
                                configPath, key));
            }
        }
    }

    private static String required(String value, String path) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Option '" + path + "' must not be blank.");
        }
        return value.trim();
    }

    private static String optional(String value, String defaultValue) {
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private static SeaTunnelDataType<?> parseType(
            String fieldName, String configuredType, String configPath) {
        try {
            SeaTunnelDataType<?> type =
                    SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                            fieldName, configuredType);
            validateTargetType(type, configPath);
            return type;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s.type' has invalid target type '%s'.",
                            configPath, configuredType),
                    e);
        }
    }

    private static void validateTargetType(SeaTunnelDataType<?> type, String configPath) {
        switch (type.getSqlType()) {
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTES:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Option '%s.type' only supports scalar target types, but was '%s'.",
                                configPath, type));
        }
    }

    private static final class FieldMapping implements Serializable {
        private final String source;
        private final int sourceIndex;
        private final String target;
        private final SeaTunnelDataType<?> sourceType;
        private final SeaTunnelDataType<?> targetType;

        private FieldMapping(
                String source,
                int sourceIndex,
                String target,
                SeaTunnelDataType<?> sourceType,
                SeaTunnelDataType<?> targetType) {
            this.source = source;
            this.sourceIndex = sourceIndex;
            this.target = target;
            this.sourceType = sourceType;
            this.targetType = targetType;
        }

        private Object convert(Object value) {
            if (value == null || sourceType.equals(targetType)) {
                return value;
            }
            try {
                return convertValue(value, targetType);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "BatchJson failed to convert source field '%s' to output field '%s' as %s; value='%s' (%s).",
                                source, target, targetType, value, value.getClass().getName()),
                        e);
            }
        }
    }

    private static Object convertValue(Object value, SeaTunnelDataType<?> targetType) {
        switch (targetType.getSqlType()) {
            case STRING:
                return value.toString();
            case BOOLEAN:
                return toBoolean(value);
            case TINYINT:
                return decimal(value).byteValueExact();
            case SMALLINT:
                return decimal(value).shortValueExact();
            case INT:
                return decimal(value).intValueExact();
            case BIGINT:
                return decimal(value).longValueExact();
            case FLOAT:
                return finiteFloat(value);
            case DOUBLE:
                return finiteDouble(value);
            case DECIMAL:
                return toDecimal(value, (DecimalType) targetType);
            case BYTES:
                return value instanceof byte[]
                        ? value
                        : value.toString().getBytes(StandardCharsets.UTF_8);
            case DATE:
                return toLocalDate(value);
            case TIME:
                return toLocalTime(value);
            case TIMESTAMP:
                return toLocalDateTime(value);
            case TIMESTAMP_TZ:
                return toOffsetDateTime(value);
            default:
                throw new IllegalArgumentException("Unsupported target type " + targetType);
        }
    }

    private static BigDecimal decimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return new BigDecimal(value.toString().trim());
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String text = value.toString().trim().toLowerCase(Locale.ROOT);
        if ("true".equals(text) || "1".equals(text)) {
            return true;
        }
        if ("false".equals(text) || "0".equals(text)) {
            return false;
        }
        throw new IllegalArgumentException("Expected true, false, 1 or 0");
    }

    private static float finiteFloat(Object value) {
        float result = Float.parseFloat(value.toString().trim());
        if (!Float.isFinite(result)) {
            throw new IllegalArgumentException("Non-finite FLOAT value");
        }
        return result;
    }

    private static double finiteDouble(Object value) {
        double result = Double.parseDouble(value.toString().trim());
        if (!Double.isFinite(result)) {
            throw new IllegalArgumentException("Non-finite DOUBLE value");
        }
        return result;
    }

    private static BigDecimal toDecimal(Object value, DecimalType type) {
        BigDecimal result = decimal(value).setScale(type.getScale(), RoundingMode.UNNECESSARY);
        if (result.precision() > type.getPrecision()) {
            throw new ArithmeticException(
                    String.format(
                            "Decimal precision %d exceeds target precision %d",
                            result.precision(), type.getPrecision()));
        }
        return result;
    }

    private static LocalDate toLocalDate(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        return LocalDate.parse(value.toString().trim());
    }

    private static LocalTime toLocalTime(Object value) {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalTime();
        }
        return LocalTime.parse(value.toString().trim());
    }

    private static LocalDateTime toLocalDateTime(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay();
        }
        if (value instanceof Number) {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(((Number) value).longValue()), ZoneId.systemDefault());
        }
        return LocalDateTime.parse(value.toString().trim().replace(' ', 'T'));
    }

    private static OffsetDateTime toOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        return OffsetDateTime.parse(value.toString().trim().replace(' ', 'T'));
    }
}
