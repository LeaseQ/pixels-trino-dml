/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Constant;

import java.nio.charset.StandardCharsets;
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
    private final Map<String, String> assignments;
    private final List<PixelsColumnHandle> primaryKeyColumns;

    public PixelsUpdateTableHandle(PixelsTableHandle tableHandle, Map<PixelsColumnHandle, Constant> assignments)
    {
        this(tableHandle, serializeAssignments(assignments), List.of());
    }

    public static PixelsUpdateTableHandle withPrimaryKeyColumns(
            PixelsTableHandle tableHandle,
            Map<PixelsColumnHandle, Constant> assignments,
            List<PixelsColumnHandle> primaryKeyColumns)
    {
        return new PixelsUpdateTableHandle(tableHandle, serializeAssignments(assignments), primaryKeyColumns);
    }

    @JsonCreator
    public PixelsUpdateTableHandle(
            @JsonProperty("tableHandle") PixelsTableHandle tableHandle,
            @JsonProperty("assignments") Map<String, String> assignments,
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
    public Map<String, String> getAssignments()
    {
        return assignments;
    }

    private static Map<String, String> serializeAssignments(
            Map<PixelsColumnHandle, Constant> assignments)
    {
        return assignments.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                entry -> entry.getKey().getColumnName(),
                entry -> toSqlString(entry.getValue().getValue())));
    }

    private static String toSqlString(Object value)
    {
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    @JsonProperty
    public List<PixelsColumnHandle> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }
}
