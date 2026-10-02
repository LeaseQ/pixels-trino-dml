/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.pixelsdb.pixels.core.TypeDescription;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

/** Connector-internal row identifier used by Trino's row-change analyzer. */
public final class PixelsRowIdColumnHandle extends PixelsColumnHandle
{
    @JsonCreator
    public PixelsRowIdColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnAlias") String columnAlias,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("typeCategory") TypeDescription.Category typeCategory,
            @JsonProperty("columnComment") String columnComment,
            @JsonProperty("logicalOrdinal") int logicalOrdinal)
    {
        super(connectorId, schemaName, tableName, columnName, columnAlias, columnType,
                typeCategory, columnComment, logicalOrdinal);
    }

    PixelsRowIdColumnHandle(PixelsColumnHandle primaryKeyColumn)
    {
        this(primaryKeyColumn.getConnectorId(), primaryKeyColumn.getSchemaName(),
                primaryKeyColumn.getTableName(), primaryKeyColumn.getColumnName(),
                primaryKeyColumn.getColumnAlias(), primaryKeyColumn.getColumnType(),
                primaryKeyColumn.getTypeCategory(), primaryKeyColumn.getColumnComment(),
                primaryKeyColumn.getLogicalOrdinal());
    }

    @Override
    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder().setName("$row_id").setType(getColumnType()).build();
    }
}
