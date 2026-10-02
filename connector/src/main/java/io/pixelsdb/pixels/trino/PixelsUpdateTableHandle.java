/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Constant;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Coordinator-side state for a constant-column UPDATE.
 *
 * <p>Pixels UPDATE is a tag/index operation. It must not be represented as a
 * file mutation, so the handle retains the original scan constraint together
 * with the constant assignments until the tag index writer consumes it.</p>
 */
public final class PixelsUpdateTableHandle implements ConnectorTableHandle
{
    private final PixelsTableHandle tableHandle;
    private final Map<PixelsColumnHandle, Constant> assignments;
    private final List<PixelsColumnHandle> primaryKeyColumns;

    public PixelsUpdateTableHandle(PixelsTableHandle tableHandle, Map<PixelsColumnHandle, Constant> assignments)
    {
        this(tableHandle, assignments, List.of());
    }

    @JsonCreator
    public PixelsUpdateTableHandle(
            @JsonProperty("tableHandle") PixelsTableHandle tableHandle,
            @JsonProperty("assignments") Map<PixelsColumnHandle, Constant> assignments,
            @JsonProperty("primaryKeyColumns") List<PixelsColumnHandle> primaryKeyColumns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.assignments = Map.copyOf(requireNonNull(assignments, "assignments is null"));
        this.primaryKeyColumns = List.copyOf(requireNonNull(primaryKeyColumns, "primaryKeyColumns is null"));
    }

    @JsonProperty
    public PixelsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Map<PixelsColumnHandle, Constant> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }
}
