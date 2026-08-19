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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelStatefulTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogFlatMapTransform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchJsonMultiCatalogTransform extends AbstractMultiCatalogFlatMapTransform
        implements SeaTunnelStatefulTransform<SeaTunnelRow> {

    public BatchJsonMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public String getPluginName() {
        return BatchJsonTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelFlatMapTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return new BatchJsonTransform(config, inputCatalogTable);
    }

    @Override
    public List<SeaTunnelRow> finish() {
        List<SeaTunnelRow> outputRows = new ArrayList<>();
        for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
            if (transform instanceof SeaTunnelStatefulTransform) {
                outputRows.addAll(((SeaTunnelStatefulTransform<SeaTunnelRow>) transform).finish());
            }
        }
        return outputRows;
    }

    @Override
    public byte[] snapshotState(long checkpointId) {
        HashMap<String, byte[]> states = new HashMap<>();
        for (Map.Entry<String, SeaTunnelTransform<SeaTunnelRow>> entry : transformMap.entrySet()) {
            if (entry.getValue() instanceof SeaTunnelStatefulTransform) {
                states.put(
                        entry.getKey(),
                        ((SeaTunnelStatefulTransform<SeaTunnelRow>) entry.getValue())
                                .snapshotState(checkpointId));
            }
        }
        return SerializationUtils.serialize(states);
    }

    @Override
    public void restoreState(byte[] state) {
        Map<String, byte[]> states = SerializationUtils.deserialize(state);
        for (Map.Entry<String, byte[]> entry : states.entrySet()) {
            SeaTunnelTransform<SeaTunnelRow> transform = transformMap.get(entry.getKey());
            if (!(transform instanceof SeaTunnelStatefulTransform)) {
                throw new IllegalStateException(
                        "Cannot restore BatchJson state for unknown table " + entry.getKey());
            }
            ((SeaTunnelStatefulTransform<SeaTunnelRow>) transform).restoreState(entry.getValue());
        }
    }

    @Override
    public void close() {
        for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
            transform.close();
        }
    }
}
