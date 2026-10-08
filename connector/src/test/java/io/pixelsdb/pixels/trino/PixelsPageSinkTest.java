/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PixelsPageSinkTest
{
    @Test
    void appendFlushesRowsAndAddsTheCommitTimestamp()
    {
        RecordingWriter writer = new RecordingWriter();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        VectorizedRowBatch batch = schema.createRowBatchWithHiddenColumn(2);
        PixelsPageSink sink = new PixelsPageSink(
                writer, batch, List.of(column("id")), 1234L);

        sink.appendPage(new Page(new LongArrayBlock(
                2, Optional.empty(), new long[] {11L, 22L})));
        sink.finish().join();

        assertEquals(List.of(List.of(11L, 22L)), writer.values);
        assertEquals(List.of(List.of(1234L, 1234L)), writer.timestamps);
        assertEquals(1, writer.closeCount);
        assertEquals(0, batch.size);
        assertFalse(writer.aborted);
    }

    @Test
    void abortStopsTheWriterWithoutFinishingIt()
    {
        RecordingWriter writer = new RecordingWriter();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        PixelsPageSink sink = new PixelsPageSink(
                writer,
                schema.createRowBatchWithHiddenColumn(2),
                List.of(column("id")),
                1234L);

        sink.abort();

        assertEquals(1, writer.abortCount);
        assertEquals(0, writer.closeCount);
    }

    @Test
    void finishReturnsAFragmentForTheCoordinator()
    {
        RecordingWriter writer = new RecordingWriter();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        PixelsPageSink sink = new PixelsPageSink(
                writer,
                schema.createRowBatchWithHiddenColumn(2),
                List.of(column("id")),
                1234L,
                42L,
                "writer_file.pxl");

        sink.appendPage(new Page(new LongArrayBlock(
                1, Optional.empty(), new long[] {11L})));
        var fragments = sink.finish().join();

        assertEquals(1, fragments.size());
        PixelsInsertFragment.Decoded decoded = PixelsInsertFragment.decode(fragments.iterator().next());
        assertEquals(42L, decoded.pathId());
        assertEquals(1, decoded.numRowGroups());
        assertEquals("writer_file.pxl", decoded.fileName());
    }

    @Test
    void recordsPrimaryKeysAtTheirWrittenRowGroupOffsets()
    {
        RecordingWriter writer = new RecordingWriter();
        RecordingInsertIndex index = new RecordingInsertIndex();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        PixelsPageSink sink = new PixelsPageSink(
                writer, schema.createRowBatchWithHiddenColumn(2), List.of(column("id")),
                1234L, 42L, "writer_file.pxl", index);

        sink.appendPage(new Page(new LongArrayBlock(
                3, Optional.empty(), new long[] {11L, 22L, 33L})));
        sink.finish().join();

        assertEquals(List.of("0:0:11", "0:1:22", "1:0:33"), index.locations);
        assertEquals(1, index.finishCount);
    }

    @Test
    void finishFailureAbortsWriterIndexAndMetadataCleanup()
    {
        RecordingWriter writer = new RecordingWriter(true);
        RecordingInsertIndex index = new RecordingInsertIndex();
        List<Boolean> cleanupCalls = new ArrayList<>();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        PixelsPageSink sink = new PixelsPageSink(
                writer, schema.createRowBatchWithHiddenColumn(2), List.of(column("id")),
                1234L, 42L, "writer_file.pxl", index, () -> cleanupCalls.add(true));

        sink.appendPage(new Page(new LongArrayBlock(
                1, Optional.empty(), new long[] {11L})));

        assertThrows(TrinoException.class, sink::finish);

        assertEquals(1, writer.abortCount);
        assertEquals(1, index.abortCount);
        assertEquals(List.of(true), cleanupCalls);
    }

    @Test
    void runtimeFinishFailureAlsoAbortsWriterIndexAndMetadataCleanup()
    {
        RecordingWriter writer = new RecordingWriter();
        RecordingInsertIndex index = new RecordingInsertIndex(true);
        List<Boolean> cleanupCalls = new ArrayList<>();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long"));
        PixelsPageSink sink = new PixelsPageSink(
                writer, schema.createRowBatchWithHiddenColumn(2), List.of(column("id")),
                1234L, 42L, "writer_file.pxl", index, () -> cleanupCalls.add(true));

        sink.appendPage(new Page(new LongArrayBlock(
                1, Optional.empty(), new long[] {11L})));

        assertThrows(RuntimeException.class, sink::finish);

        assertEquals(1, writer.abortCount);
        assertEquals(1, index.abortCount);
        assertEquals(List.of(true), cleanupCalls);
    }

    private static PixelsColumnHandle column(String name)
    {
        return new PixelsColumnHandle(
                "pixels", "default", "users", name, name,
                BigintType.BIGINT, LONG, "", 0);
    }

    private static final class RecordingWriter implements PixelsWriter
    {
        private final List<List<Long>> values = new ArrayList<>();
        private final List<List<Long>> timestamps = new ArrayList<>();
        private int closeCount;
        private int abortCount;
        private boolean aborted;
        private final boolean failOnClose;

        private RecordingWriter()
        {
            this(false);
        }

        private RecordingWriter(boolean failOnClose)
        {
            this.failOnClose = failOnClose;
        }

        @Override
        public boolean addRowBatch(VectorizedRowBatch rowBatch)
        {
            LongColumnVector valueVector = (LongColumnVector) rowBatch.cols[0];
            LongColumnVector timestampVector = (LongColumnVector) rowBatch.cols[1];
            List<Long> batchValues = new ArrayList<>();
            List<Long> batchTimestamps = new ArrayList<>();
            for (int i = 0; i < rowBatch.size; i++)
            {
                batchValues.add(valueVector.vector[i]);
                batchTimestamps.add(timestampVector.vector[i]);
            }
            values.add(batchValues);
            timestamps.add(batchTimestamps);
            return true;
        }

        @Override
        public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeDescription getSchema()
        {
            return TypeDescription.createSchemaFromStrings(
                    List.of("id"), List.of("long"));
        }

        @Override
        public int getNumRowGroup()
        {
            return values.size();
        }

        @Override
        public int getNumWriteRequests()
        {
            return values.size();
        }

        @Override
        public long getCompletedBytes()
        {
            return 0L;
        }

        @Override
        public void close() throws IOException
        {
            closeCount++;
            if (failOnClose)
            {
                throw new IOException("writer close failed");
            }
        }

        @Override
        public void abort() throws IOException
        {
            aborted = true;
            abortCount++;
        }
    }

    private static final class RecordingInsertIndex implements PixelsPageSink.InsertIndexWriter
    {
        private final List<String> locations = new ArrayList<>();
        private int finishCount;
        private int abortCount;
        private final boolean failOnFinish;

        private RecordingInsertIndex()
        {
            this(false);
        }

        private RecordingInsertIndex(boolean failOnFinish)
        {
            this.failOnFinish = failOnFinish;
        }

        @Override
        public void appendBatch(VectorizedRowBatch batch, int rowGroupId, int firstRowOffset)
        {
            LongColumnVector ids = (LongColumnVector) batch.cols[0];
            for (int position = 0; position < batch.size; position++)
            {
                locations.add(rowGroupId + ":" + (firstRowOffset + position) + ":" + ids.vector[position]);
            }
        }

        @Override
        public void finish()
        {
            finishCount++;
            if (failOnFinish)
            {
                throw new IllegalStateException("index finish failed");
            }
        }

        @Override
        public void abort()
        {
            abortCount++;
        }
    }
}
