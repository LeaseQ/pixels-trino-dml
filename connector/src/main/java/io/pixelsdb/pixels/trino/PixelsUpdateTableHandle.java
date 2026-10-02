/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Constant;

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

    public PixelsUpdateTableHandle(PixelsTableHandle tableHandle, Map<PixelsColumnHandle, Constant> assignments)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.assignments = Map.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public PixelsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public Map<PixelsColumnHandle, Constant> getAssignments()
    {
        return assignments;
    }
}
