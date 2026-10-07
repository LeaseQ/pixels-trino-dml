/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PixelsMergeSinkTest
{
    @Test
    void recordsEveryUpdateRowFromOneMergePage()
    {
        PixelsColumnHandle id = column("id", 0);
        PixelsColumnHandle label = column("label", 1);
        RecordingTagIndex index = new RecordingTagIndex();
        PixelsMergeSink sink = new PixelsMergeSink(mergeHandle(id, label), index);

        sink.storeMergedRows(updatePage(11, 22));

        assertEquals(2, index.entries.size());
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("1"),
                index.entries.get(0).tag);
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("11"),
                index.entries.get(0).primaryKey);
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("1"),
                index.entries.get(1).tag);
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("22"),
                index.entries.get(1).primaryKey);
    }

    @Test
    void rejectsDeleteRowsInsteadOfSilentlyIgnoringThem()
    {
        PixelsColumnHandle id = column("id", 0);
        PixelsColumnHandle label = column("label", 1);
        PixelsMergeSink sink = new PixelsMergeSink(mergeHandle(id, label), new RecordingTagIndex());
        Page deletePage = updatePageWithOperation((byte) ConnectorMergeSink.DELETE_OPERATION_NUMBER, 11);

        assertThrows(TrinoException.class, () -> sink.storeMergedRows(deletePage));
    }

    private static PixelsMergeTableHandle mergeHandle(PixelsColumnHandle id, PixelsColumnHandle label)
    {
        PixelsTableHandle table = new PixelsTableHandle(
                "pixels", "default", "users", "users_alias", List.of(id, label), TupleDomain.all(),
                io.pixelsdb.pixels.planner.plan.logical.Table.TableType.BASE,
                null, null, Storage.Scheme.file, List.of("file:///tmp/users"));
        return new PixelsMergeTableHandle(table, 22L, 11L, List.of(id), List.of(label));
    }

    private static Page updatePage(long... primaryKeys)
    {
        byte[] operations = new byte[primaryKeys.length];
        java.util.Arrays.fill(operations, (byte) ConnectorMergeSink.UPDATE_OPERATION_NUMBER);
        return page(primaryKeys, operations);
    }

    private static Page updatePageWithOperation(byte operation, long primaryKey)
    {
        return page(new long[] {primaryKey}, new byte[] {operation});
    }

    private static Page page(long[] primaryKeys, byte[] operations)
    {
        long[] tags = new long[primaryKeys.length];
        java.util.Arrays.fill(tags, 1L);
        int[] mergeCases = new int[primaryKeys.length];
        return new Page(
                new LongArrayBlock(primaryKeys.length, Optional.empty(), primaryKeys),
                new LongArrayBlock(tags.length, Optional.empty(), tags),
                new ByteArrayBlock(operations.length, Optional.empty(), operations),
                new IntArrayBlock(mergeCases.length, Optional.empty(), mergeCases),
                new LongArrayBlock(primaryKeys.length, Optional.empty(), primaryKeys));
    }

    private static PixelsColumnHandle column(String name, int ordinal)
    {
        return new PixelsColumnHandle(
                "pixels", "default", "users", name, name, BigintType.BIGINT, LONG, "", ordinal);
    }

    private static final class RecordingTagIndex implements PixelsMergeSink.TagIndex
    {
        private final List<Entry> entries = new ArrayList<>();

        @Override
        public void append(byte[] tag, List<byte[]> primaryKeys)
        {
            for (byte[] primaryKey : primaryKeys)
            {
                entries.add(new Entry(tag.clone(), primaryKey.clone()));
            }
        }

        @Override
        public void close() {}

        private record Entry(byte[] tag, byte[] primaryKey) {}
    }
}
