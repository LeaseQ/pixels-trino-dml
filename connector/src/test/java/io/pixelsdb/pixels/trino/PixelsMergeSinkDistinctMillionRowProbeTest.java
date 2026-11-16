/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.physical.Storage;
import io.trino.spi.Page;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Manually selected probe for the worker-side distinct-tag merge fallback. */
class PixelsMergeSinkDistinctMillionRowProbeTest
{
    private static final int DEFAULT_ROWS = 1_000_000;
    private static final int PAGE_SIZE = 10_000;

    @Test
    void batchesOneMillionDistinctMergeRowsWithoutTinyFlushes()
    {
        int rows = Integer.getInteger("pixels.merge.distinct.probe.rows", DEFAULT_ROWS);
        PixelsColumnHandle id = column("id", 0);
        PixelsColumnHandle label = column("label", 1);
        CountingTagIndex index = new CountingTagIndex();
        PixelsMergeSink sink = new PixelsMergeSink(mergeHandle(id, label), index);

        long start = System.nanoTime();
        for (int offset = 0; offset < rows; offset += PAGE_SIZE)
        {
            int size = Math.min(PAGE_SIZE, rows - offset);
            sink.storeMergedRows(updatePage(offset, size));
        }
        sink.finish();

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        int expectedBatches = (rows + PixelsMergeSink.MAX_PENDING_TAGS - 1) /
                PixelsMergeSink.MAX_PENDING_TAGS;
        assertEquals(rows, index.rows);
        assertEquals(expectedBatches, index.batchCount);
        System.out.printf("[PIXELS_UPDATE_DISTINCT_MERGE_PROBE] rows=%d batches=%d " +
                        "elapsed_ms=%d encoded_rows=%d%n",
                rows, index.batchCount, elapsedMs, index.rows);
    }

    private static PixelsMergeTableHandle mergeHandle(PixelsColumnHandle id, PixelsColumnHandle label)
    {
        PixelsTableHandle table = new PixelsTableHandle(
                "pixels", "default", "users", "users_alias", List.of(id, label), TupleDomain.all(),
                io.pixelsdb.pixels.planner.plan.logical.Table.TableType.BASE,
                null, null, Storage.Scheme.file, List.of("file:///tmp/users"));
        return new PixelsMergeTableHandle(table, 22L, 11L, List.of(id), List.of(label));
    }

    private static Page updatePage(int offset, int size)
    {
        long[] primaryKeys = new long[size];
        long[] tags = new long[size];
        byte[] operations = new byte[size];
        java.util.Arrays.fill(operations, (byte) ConnectorMergeSink.UPDATE_OPERATION_NUMBER);
        for (int i = 0; i < size; i++)
        {
            primaryKeys[i] = offset + i;
            tags[i] = offset + i + 17L;
        }
        int[] mergeCases = new int[size];
        return new Page(
                new LongArrayBlock(size, Optional.empty(), primaryKeys),
                new LongArrayBlock(size, Optional.empty(), tags),
                new ByteArrayBlock(size, Optional.empty(), operations),
                new IntArrayBlock(size, Optional.empty(), mergeCases),
                new LongArrayBlock(size, Optional.empty(), primaryKeys));
    }

    private static PixelsColumnHandle column(String name, int ordinal)
    {
        return new PixelsColumnHandle(
                "pixels", "default", "users", name, name, BigintType.BIGINT, LONG, "", ordinal);
    }

    private static final class CountingTagIndex implements PixelsMergeSink.TagIndex
    {
        private long rows;
        private int batchCount;

        @Override
        public void append(byte[] tag, List<byte[]> primaryKeys)
        {
            rows += primaryKeys.size();
        }

        @Override
        public void appendBatch(long transactionTimestamp, List<PixelsMergeSink.TagUpdate> updates)
        {
            batchCount++;
            PixelsMergeSink.TagIndex.super.appendBatch(transactionTimestamp, updates);
        }

        @Override
        public void close() {}
    }
}
