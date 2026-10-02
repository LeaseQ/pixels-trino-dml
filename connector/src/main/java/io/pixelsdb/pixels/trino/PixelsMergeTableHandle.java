/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Worker-side state for an index-backed Trino MERGE/UPDATE. */
public final class PixelsMergeTableHandle implements ConnectorMergeTableHandle
{
    private final PixelsTableHandle tableHandle;
    private final long tableId;
    private final long indexId;
    private final List<PixelsColumnHandle> primaryKeyColumns;
    private final List<PixelsColumnHandle> updatedColumns;

    @JsonCreator
    public PixelsMergeTableHandle(
            @JsonProperty("tableHandle") PixelsTableHandle tableHandle,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("indexId") long indexId,
            @JsonProperty("primaryKeyColumns") List<PixelsColumnHandle> primaryKeyColumns,
            @JsonProperty("updatedColumns") List<PixelsColumnHandle> updatedColumns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableId = tableId;
        this.indexId = indexId;
        this.primaryKeyColumns = List.copyOf(requireNonNull(primaryKeyColumns, "primaryKeyColumns is null"));
        this.updatedColumns = List.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));
    }

    @Override
    @JsonProperty
    public PixelsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public long getIndexId()
    {
        return indexId;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }
}
