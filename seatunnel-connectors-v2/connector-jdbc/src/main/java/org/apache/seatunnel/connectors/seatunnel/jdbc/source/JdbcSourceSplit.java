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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {
    private final TablePath tablePath;
    private final String splitId;
    private final String splitQuery;
    private final String splitKeyName;
    private final SeaTunnelDataType splitKeyType;
    private final Object splitStart;
    private final Object splitEnd;
    private Integer splitFetchSize;

    public JdbcSourceSplit(TablePath tablePath, String splitId, String splitQuery, String splitKeyName, SeaTunnelDataType splitKeyType, Object splitStart, Object splitEnd) {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.splitQuery = splitQuery;
        this.splitKeyName = splitKeyName;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
