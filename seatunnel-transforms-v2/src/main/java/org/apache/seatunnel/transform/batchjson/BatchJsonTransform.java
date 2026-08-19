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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelStatefulTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class BatchJsonTransform extends AbstractCatalogSupportFlatMapTransform
        implements SeaTunnelStatefulTransform<SeaTunnelRow> {

    private final int batchSize;
    private final BatchDataFieldMapper batchDataFieldMapper;
    private final JsonTemplateValue jsonTemplate;
    private final SeaTunnelRowType outputRowType;
    private final List<SeaTunnelRow> bufferedRows;

    public BatchJsonTransform(ReadonlyConfig config, CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        BatchJsonTransformConfig transformConfig = BatchJsonTransformConfig.of(config);
        this.batchSize = transformConfig.getBatchSize();
        this.batchDataFieldMapper =
                BatchDataFieldMapper.of(
                        inputCatalogTable.getSeaTunnelRowType(), transformConfig.getBatchFields());
        this.jsonTemplate =
                JsonTemplateValue.fromTemplate(
                        transformConfig.getJsonTemplate(), batchDataFieldMapper.getOutputRowType());
        this.outputRowType = (SeaTunnelRowType) jsonTemplate.getDataType();
        this.bufferedRows = new ArrayList<>(batchSize);
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return BatchJsonTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected synchronized List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        bufferedRows.add(batchDataFieldMapper.map(inputRow));
        if (bufferedRows.size() < batchSize) {
            return Collections.emptyList();
        }

        return drainBufferedRows();
    }

    private List<SeaTunnelRow> drainBufferedRows() {
        if (bufferedRows.isEmpty()) {
            return Collections.emptyList();
        }
        SeaTunnelRow[] batch = bufferedRows.toArray(new SeaTunnelRow[0]);
        SeaTunnelRow outputRow = jsonTemplate.resolveObject(batch);
        outputRow.setTableId(batch[batch.length - 1].getTableId());
        bufferedRows.clear();
        return Collections.singletonList(outputRow);
    }

    @Override
    public synchronized List<SeaTunnelRow> finish() {
        return drainBufferedRows();
    }

    @Override
    public synchronized byte[] snapshotState(long checkpointId) {
        return SerializationUtils.serialize(new ArrayList<>(bufferedRows));
    }

    @Override
    public synchronized void restoreState(byte[] state) {
        List<SeaTunnelRow> restoredRows = SerializationUtils.deserialize(state);
        if (restoredRows.size() >= batchSize) {
            throw new IllegalStateException(
                    String.format(
                            "BatchJson restored %d buffered rows, but batch.size is %d",
                            restoredRows.size(), batchSize));
        }
        bufferedRows.clear();
        bufferedRows.addAll(restoredRows);
    }

    @Override
    protected TableSchema transformTableSchema() {
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < outputRowType.getTotalFields(); i++) {
            SeaTunnelDataType<?> fieldType = outputRowType.getFieldType(i);
            builder.column(
                    PhysicalColumn.of(
                            outputRowType.getFieldName(i),
                            fieldType,
                            0L,
                            true,
                            null,
                            "Batch JSON template field"));
        }
        return builder.build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public synchronized void close() {
        if (!bufferedRows.isEmpty()) {
            log.warn(
                    "BatchJson discarded {} remaining row(s) because the number of rows did not reach batch.size={}",
                    bufferedRows.size(),
                    batchSize);
            bufferedRows.clear();
        }
    }
}
