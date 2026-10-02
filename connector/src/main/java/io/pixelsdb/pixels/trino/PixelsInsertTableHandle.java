/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Coordinator-side state for an INSERT into an existing Pixels table.
 *
 * <p>The target table and the ordered write columns are kept together so the
 * page sink can build a Pixels row batch without relying on planner state that
 * is not available on the worker.</p>
 */
public final class PixelsInsertTableHandle implements ConnectorInsertTableHandle
{
    private final PixelsTableHandle tableHandle;
    private final List<PixelsColumnHandle> columns;

    @JsonCreator
    public PixelsInsertTableHandle(
            @JsonProperty("tableHandle") PixelsTableHandle tableHandle,
            @JsonProperty("columns") List<PixelsColumnHandle> columns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public PixelsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getColumns()
    {
        return columns;
    }
}
