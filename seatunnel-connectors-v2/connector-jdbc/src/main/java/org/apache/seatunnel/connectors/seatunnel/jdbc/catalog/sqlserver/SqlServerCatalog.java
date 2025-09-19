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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlserverTypeMapper;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    public static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT tbl.name AS table_name,\n"
                    + "       col.name AS column_name,\n"
                    + "       ext.value AS comment,\n"
                    + "       col.column_id AS column_id,\n"
                    + "       types.name AS type,\n"
                    + "       col.max_length AS max_length,\n"
                    + "       col.precision AS precision,\n"
                    + "       col.scale AS scale,\n"
                    + "       col.is_nullable AS is_nullable,\n"
                    + "       def.definition AS default_value\n"
                    + "FROM sys.tables tbl\n"
                    + "    INNER JOIN sys.columns col ON tbl.object_id = col.object_id\n"
                    + "    LEFT JOIN sys.types types ON col.system_type_id = types.user_type_id\n"
                    + "    LEFT JOIN sys.extended_properties ext ON ext.major_id = col.object_id AND ext.minor_id = col.column_id\n"
                    + "    LEFT JOIN sys.default_constraints def ON col.default_object_id = def.object_id AND ext.minor_id = col.column_id AND ext.name = 'MS_Description'\n"
                    + "WHERE schema_name(tbl.schema_id) = '%s' %s\n"
                    + "ORDER BY tbl.name, col.column_id";

    public SqlServerCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + "  where name = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + "  and  TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT NAME FROM sys.databases";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT TABLE_SCHEMA, TABLE_NAME FROM "
                + databaseName
                + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        String tableSql =
                StringUtils.isNotEmpty(tablePath.getTableName())
                        ? "AND tbl.name = '" + tablePath.getTableName() + "'"
                        : "";

        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tableSql);
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String dataType = resultSet.getString("type");
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getBoolean("is_nullable");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision((long) precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return SqlServerTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return SqlServerCreateTableSqlBuilder.builder(tablePath, table, createIndex)
                .build(tablePath, table);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getFullName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE %s", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE %s;", databaseName);
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SqlserverTypeMapper());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select TOP 1 * from %s ;", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format("TRUNCATE TABLE  %s", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        Connection conn = getConnection(dbUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<String> comment = getTableComment(metaData, tablePath);
            Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
            List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
            TableSchema.Builder tableSchemaBuilder =
                    buildColumnsReturnTablaSchemaBuilder(tablePath, conn);
            // add primary key
            primaryKey.ifPresent(tableSchemaBuilder::primaryKey);
            // add constraint key
            constraintKeys.forEach(tableSchemaBuilder::constraintKey);
            TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
            return CatalogTable.of(
                    tableIdentifier,
                    tableSchemaBuilder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    comment.orElse(""),
                    catalogName);

        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }
}
