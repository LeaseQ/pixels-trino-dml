/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.planner.plan.logical.Table;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PixelsDmlContractTest
{
    @Test
    void insertHandleCarriesTheTargetTableAndWriteColumns()
    {
        PixelsTableHandle table = tableHandle();
        PixelsColumnHandle label = column("label", 1);

        PixelsInsertTableHandle insert = new PixelsInsertTableHandle(table, List.of(label));

        assertSame(table, insert.getTableHandle());
        assertEquals(List.of(label), insert.getColumns());
    }

    @Test
    void updateHandleCarriesConstantAssignmentsWithoutChangingTheBaseTableHandle()
    {
        PixelsTableHandle table = tableHandle();
        PixelsColumnHandle label = column("label", 1);
        Constant value = new Constant("vip", VarcharType.VARCHAR);

        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(table, Map.of(label, value));

        assertSame(table, update.getTableHandle());
        assertEquals(Map.of(label, value), update.getAssignments());
        assertTrue(update instanceof ConnectorTableHandle);
    }

    private static PixelsTableHandle tableHandle()
    {
        return new PixelsTableHandle(
                "pixels",
                "default",
                "users",
                "users_alias",
                List.of(),
                TupleDomain.all(),
                Table.TableType.BASE,
                null,
                null,
                Storage.Scheme.file,
                List.of("file:///tmp/users"));
    }

    private static PixelsColumnHandle column(String name, int ordinal)
    {
        return new PixelsColumnHandle(
                "pixels",
                "default",
                "users",
                name,
                name,
                VarcharType.VARCHAR,
                TypeDescription.Category.STRING,
                "",
                ordinal);
    }
}
