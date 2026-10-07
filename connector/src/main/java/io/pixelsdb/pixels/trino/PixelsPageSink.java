/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.BooleanColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.LongDecimalColumnVector;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_DATA_TYPE_ERROR;
import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR;
import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_WRITER_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Converts Trino pages into the row batches consumed by a Pixels writer.
 *
 * <p>The writer is deliberately injected here.  The conversion and lifecycle
 * rules can therefore be tested without a storage service, while the provider
 * remains responsible for choosing the physical output path.</p>
 */
final class PixelsPageSink
        implements ConnectorPageSink
{
    interface InsertIndexWriter
    {
        void appendBatch(VectorizedRowBatch batch, int rowGroupId, int firstRowOffset) throws IOException;

        void finish() throws IOException;

        void abort() throws IOException;
    }

    private final PixelsWriter writer;
    private final VectorizedRowBatch rowBatch;
    private final List<PixelsColumnHandle> columns;
    private final long commitTimestamp;
    private final long targetPathId;
    private final String fileName;
    private final InsertIndexWriter indexWriter;
    private final Runnable abortCleanup;
    private int currentRowGroupOffset;
    private boolean finished;
    private boolean aborted;

    PixelsPageSink(
            PixelsWriter writer,
            VectorizedRowBatch rowBatch,
            List<PixelsColumnHandle> columns,
            long commitTimestamp)
    {
        this(writer, rowBatch, columns, commitTimestamp, -1L, null);
    }

    PixelsPageSink(
            PixelsWriter writer,
            VectorizedRowBatch rowBatch,
            List<PixelsColumnHandle> columns,
            long commitTimestamp,
            long targetPathId,
            String fileName)
    {
        this(writer, rowBatch, columns, commitTimestamp, targetPathId, fileName, null);
    }

    PixelsPageSink(
            PixelsWriter writer,
            VectorizedRowBatch rowBatch,
            List<PixelsColumnHandle> columns,
            long commitTimestamp,
            long targetPathId,
            String fileName,
            InsertIndexWriter indexWriter)
    {
        this(writer, rowBatch, columns, commitTimestamp, targetPathId, fileName,
                indexWriter, () -> {});
    }

    PixelsPageSink(
            PixelsWriter writer,
            VectorizedRowBatch rowBatch,
            List<PixelsColumnHandle> columns,
            long commitTimestamp,
            long targetPathId,
            String fileName,
            InsertIndexWriter indexWriter,
            Runnable abortCleanup)
    {
        this.writer = requireNonNull(writer, "writer is null");
        this.rowBatch = requireNonNull(rowBatch, "rowBatch is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.commitTimestamp = commitTimestamp;
        this.targetPathId = targetPathId;
        this.fileName = fileName;
        this.indexWriter = indexWriter;
        this.abortCleanup = requireNonNull(abortCleanup, "abortCleanup is null");
        if (rowBatch.cols.length != columns.size() + 1) {
            throw new IllegalArgumentException("row batch must contain one hidden timestamp column");
        }
    }

    @Override
    public synchronized CompletableFuture<?> appendPage(Page page)
    {
        requireOpen();
        requireNonNull(page, "page is null");
        if (page.getChannelCount() != columns.size()) {
            throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                    "Pixels INSERT page has " + page.getChannelCount() +
                            " columns, expected " + columns.size());
        }

        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (rowBatch.isFull()) {
                    flush();
                }
                for (int channel = 0; channel < columns.size(); channel++) {
                    appendValue(
                            rowBatch.cols[channel],
                            columns.get(channel),
                            page.getBlock(channel),
                            position);
                }
                ((LongColumnVector) rowBatch.cols[rowBatch.cols.length - 1]).add(commitTimestamp);
                rowBatch.size++;
            }
        }
        catch (IOException e) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "failed to flush Pixels INSERT page", e);
        }
        catch (RuntimeException e) {
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(PIXELS_DATA_TYPE_ERROR,
                    "failed to convert a Trino value to a Pixels value", e);
        }
        return NOT_BLOCKED;
    }

    @Override
    public synchronized CompletableFuture<Collection<io.airlift.slice.Slice>> finish()
    {
        if (finished) {
            return CompletableFuture.completedFuture(List.of());
        }
        if (aborted) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "Pixels INSERT page sink was aborted");
        }
        try {
            flush();
            writer.close();
            if (indexWriter != null) {
                indexWriter.finish();
            }
            finished = true;
            if (targetPathId >= 0 && fileName != null) {
                return CompletableFuture.completedFuture(List.of(
                        PixelsInsertFragment.encode(targetPathId, fileName, writer.getNumRowGroup())));
            }
            return CompletableFuture.completedFuture(List.of());
        }
        catch (IOException e) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "failed to close Pixels INSERT writer", e);
        }
    }

    @Override
    public synchronized void abort()
    {
        if (finished || aborted) {
            return;
        }
        try {
            try {
                writer.abort();
            }
            finally {
                try {
                    if (indexWriter != null) {
                        indexWriter.abort();
                    }
                }
                finally {
                    abortCleanup.run();
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "failed to abort Pixels INSERT writer", e);
        }
        finally {
            aborted = true;
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return writer.getCompletedBytes();
    }

    private void flush() throws IOException
    {
        if (rowBatch.isEmpty()) {
            return;
        }
        int rowGroupId = writer.getNumRowGroup();
        int batchSize = rowBatch.size;
        writer.addRowBatch(rowBatch);
        if (indexWriter != null) {
            indexWriter.appendBatch(rowBatch, rowGroupId, currentRowGroupOffset);
        }
        currentRowGroupOffset = writer.getNumRowGroup() > rowGroupId
                ? 0
                : currentRowGroupOffset + batchSize;
        rowBatch.reset();
    }

    private void requireOpen()
    {
        if (finished) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "Pixels INSERT page sink is already finished");
        }
        if (aborted) {
            throw new TrinoException(PIXELS_WRITER_ERROR, "Pixels INSERT page sink was aborted");
        }
    }

    private static void appendValue(ColumnVector target, PixelsColumnHandle column, Block block, int position)
    {
        if (block.isNull(position)) {
            target.addNull();
            return;
        }

        TypeDescription.Category category = column.getTypeCategory();
        switch (category) {
            case BOOLEAN:
                ((BooleanColumnVector) target).add(column.getColumnType().getBoolean(block, position));
                break;
            case BYTE:
                ((ByteColumnVector) target).add((byte) column.getColumnType().getLong(block, position));
                break;
            case SHORT:
                ((ShortColumnVector) target).add((short) column.getColumnType().getLong(block, position));
                break;
            case INT:
                ((IntColumnVector) target).add((int) column.getColumnType().getLong(block, position));
                break;
            case LONG:
                ((LongColumnVector) target).add(column.getColumnType().getLong(block, position));
                break;
            case FLOAT:
                ((FloatColumnVector) target).add((float) column.getColumnType().getDouble(block, position));
                break;
            case DOUBLE:
                ((DoubleColumnVector) target).add(column.getColumnType().getDouble(block, position));
                break;
            case DATE:
                ((DateColumnVector) target).add((int) column.getColumnType().getLong(block, position));
                break;
            case TIME:
                // Trino TIME is picoseconds-of-day; Pixels files store milliseconds-of-day.
                ((TimeColumnVector) target).add((int) (column.getColumnType().getLong(block, position) / 1_000_000_000L));
                break;
            case TIMESTAMP:
                ((TimestampColumnVector) target).add(column.getColumnType().getLong(block, position));
                break;
            case DECIMAL:
                appendDecimal(target, column, block, position);
                break;
            case STRING:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                Slice slice = column.getColumnType().getSlice(block, position);
                ((BinaryColumnVector) target).add(slice.getBytes());
                break;
            default:
                throw new TrinoException(PIXELS_DATA_TYPE_ERROR,
                        "Pixels INSERT does not support column type " + category + " yet");
        }
    }

    private static void appendDecimal(ColumnVector target, PixelsColumnHandle column, Block block, int position)
    {
        DecimalType type = (DecimalType) column.getColumnType();
        if (type.isShort()) {
            ((DecimalColumnVector) target).add(ByteBuffer.allocate(Long.BYTES)
                    .putLong(type.getLong(block, position)).array());
        }
        else {
            Int128 value = (Int128) type.getObject(block, position);
            ((LongDecimalColumnVector) target).add(value.toBigEndianBytes());
        }
    }
}
