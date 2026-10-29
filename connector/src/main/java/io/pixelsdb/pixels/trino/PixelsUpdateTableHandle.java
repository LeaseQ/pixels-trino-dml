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
    private final List<PixelsUpdateAssignment> assignments;
    private final List<PixelsColumnHandle> primaryKeyColumns;

    public PixelsUpdateTableHandle(PixelsTableHandle tableHandle, Map<PixelsColumnHandle, ?> assignments)
    {
        this(tableHandle, normalizeAssignments(assignments), List.of());
    }

    public PixelsUpdateTableHandle(
            PixelsTableHandle tableHandle,
            Map<PixelsColumnHandle, ?> assignments,
            List<PixelsColumnHandle> primaryKeyColumns)
    {
        this(tableHandle, normalizeAssignments(assignments), primaryKeyColumns);
    }

    @JsonCreator
    public PixelsUpdateTableHandle(
            @JsonProperty("tableHandle") PixelsTableHandle tableHandle,
            @JsonProperty("assignments") List<PixelsUpdateAssignment> assignments,
            @JsonProperty("primaryKeyColumns") List<PixelsColumnHandle> primaryKeyColumns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.assignments = List.copyOf(requireNonNull(assignments, "assignments is null"));
        this.primaryKeyColumns = List.copyOf(requireNonNull(primaryKeyColumns, "primaryKeyColumns is null"));
    }

    @JsonProperty
    public PixelsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public List<PixelsUpdateAssignment> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getPrimaryKeyColumns()
    {
        return primaryKeyColumns;
    }

    private static List<PixelsUpdateAssignment> normalizeAssignments(Map<PixelsColumnHandle, ?> assignments)
    {
        List<PixelsUpdateAssignment> normalized = new java.util.ArrayList<>();
        for (Map.Entry<PixelsColumnHandle, ?> entry : requireNonNull(assignments, "assignments is null").entrySet()) {
            Object value = requireNonNull(entry.getValue(), "assignment value is null");
            if (value instanceof Constant constant) {
                value = constant.getValue();
            }
            String text = value instanceof Slice slice ? slice.toStringUtf8() : String.valueOf(value);
            normalized.add(new PixelsUpdateAssignment(entry.getKey(), text));
        }
        return normalized;
    }

    public static final class PixelsUpdateAssignment
    {
        private final PixelsColumnHandle column;
        private final String value;

        @JsonCreator
        public PixelsUpdateAssignment(
                @JsonProperty("column") PixelsColumnHandle column,
                @JsonProperty("value") String value)
        {
            this.column = requireNonNull(column, "column is null");
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public PixelsColumnHandle getColumn() { return column; }

        @JsonProperty
        public String getValue() { return value; }
    }
}
