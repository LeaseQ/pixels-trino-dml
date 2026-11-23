/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.log.Logger;
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
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.io.IOException;
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
    private static final Logger logger = Logger.get(PixelsPageSink.class);

    interface InsertIndexWriter
    {
        void appendBatch(VectorizedRowBatch batch, int rowGroupId, int firstRowOffset) throws IOException;

        void finish() throws IOException;

        void abort() throws IOException;

        default long getMinRowId()
        {
            return 0L;
        }

        default long getMaxRowId()
        {
            return 0L;
        }
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
    private long writtenRows;
    private long flushCount;
    private long writeNanos;
    private long indexNanos;
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
            int position = 0;
            while (position < page.getPositionCount()) {
                if (rowBatch.isFull()) {
                    flush();
                }
                int rows = Math.min(page.getPositionCount() - position, rowBatch.freeSlots());
                for (int channel = 0; channel < columns.size(); channel++) {
                    appendColumnValues(rowBatch.cols[channel], columns.get(channel),
                            page.getBlock(channel), position, rows);
                }
                LongColumnVector hidden = (LongColumnVector) rowBatch.cols[rowBatch.cols.length - 1];
                for (int i = 0; i < rows; i++) {
                    hidden.add(commitTimestamp);
                }
                rowBatch.size += rows;
                position += rows;
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
            logger.info("[PIXELS_INSERT_SINK_METRIC] rows=%s flush_count=%s write_ms=%.3f index_ms=%.3f",
                    writtenRows,
                    flushCount,
                    writeNanos / 1_000_000.0,
                    indexNanos / 1_000_000.0);
            if (targetPathId >= 0 && fileName != null) {
                return CompletableFuture.completedFuture(List.of(
                        PixelsInsertFragment.encode(targetPathId, fileName, writer.getNumRowGroup(),
                                indexWriter == null ? 0L : indexWriter.getMinRowId(),
                                indexWriter == null ? 0L : indexWriter.getMaxRowId())));
            }
            return CompletableFuture.completedFuture(List.of());
        }
        catch (IOException e) {
            try {
                abort();
            }
            catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw new TrinoException(PIXELS_WRITER_ERROR, "failed to close Pixels INSERT writer", e);
        }
        catch (RuntimeException e) {
            try {
                abort();
            }
            catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
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
        long writeStart = System.nanoTime();
        writer.addRowBatch(rowBatch);
        writeNanos += System.nanoTime() - writeStart;
        if (indexWriter != null) {
            long indexStart = System.nanoTime();
            indexWriter.appendBatch(rowBatch, rowGroupId, currentRowGroupOffset);
            indexNanos += System.nanoTime() - indexStart;
        }
        writtenRows += batchSize;
        flushCount++;
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
                appendSlice((BinaryColumnVector) target, slice);
                break;
            default:
                throw new TrinoException(PIXELS_DATA_TYPE_ERROR,
                        "Pixels INSERT does not support column type " + category + " yet");
        }
    }

    private static void appendColumnValues(ColumnVector target, PixelsColumnHandle column,
            Block block, int offset, int length)
    {
        if (block instanceof RunLengthEncodedBlock) {
            appendRepeated(target, column, ((RunLengthEncodedBlock) block).getValue(), length);
            return;
        }
        Type type = column.getColumnType();
        switch (column.getTypeCategory()) {
            case BOOLEAN:
                appendBooleanValues((BooleanColumnVector) target, type, block, offset, length);
                break;
            case BYTE:
                appendByteValues((ByteColumnVector) target, type, block, offset, length);
                break;
            case SHORT:
                appendShortValues((ShortColumnVector) target, type, block, offset, length);
                break;
            case INT:
                appendIntValues((IntColumnVector) target, type, block, offset, length);
                break;
            case LONG:
                appendLongValues((LongColumnVector) target, type, block, offset, length);
                break;
            case FLOAT:
                appendFloatValues((FloatColumnVector) target, type, block, offset, length);
                break;
            case DOUBLE:
                appendDoubleValues((DoubleColumnVector) target, type, block, offset, length);
                break;
            case DATE:
                appendDateValues((DateColumnVector) target, type, block, offset, length);
                break;
            case TIME:
                appendTimeValues((TimeColumnVector) target, type, block, offset, length);
                break;
            case TIMESTAMP:
                appendTimestampValues((TimestampColumnVector) target, type, block, offset, length);
                break;
            case DECIMAL:
                appendDecimalValues(target, (DecimalType) type, block, offset, length);
                break;
            case STRING:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                appendSliceValues((BinaryColumnVector) target, type, block, offset, length);
                break;
            default:
                for (int i = 0; i < length; i++) {
                    appendValue(target, column, block, offset + i);
                }
        }
    }

    private static void appendBooleanValues(BooleanColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add(type.getBoolean(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add(type.getBoolean(block, i));
            }
        }
    }

    private static void appendByteValues(ByteColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((byte) type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((byte) type.getLong(block, i));
            }
        }
    }

    private static void appendShortValues(ShortColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((short) type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((short) type.getLong(block, i));
            }
        }
    }

    private static void appendIntValues(IntColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((int) type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((int) type.getLong(block, i));
            }
        }
    }

    private static void appendLongValues(LongColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add(type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add(type.getLong(block, i));
            }
        }
    }

    private static void appendFloatValues(FloatColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((float) type.getDouble(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((float) type.getDouble(block, i));
            }
        }
    }

    private static void appendDoubleValues(DoubleColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add(type.getDouble(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add(type.getDouble(block, i));
            }
        }
    }

    private static void appendDateValues(DateColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((int) type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((int) type.getLong(block, i));
            }
        }
    }

    private static void appendTimeValues(TimeColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add((int) (type.getLong(block, i) / 1_000_000_000L));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add((int) (type.getLong(block, i) / 1_000_000_000L));
            }
        }
    }

    private static void appendTimestampValues(TimestampColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    target.add(type.getLong(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                target.add(type.getLong(block, i));
            }
        }
    }

    private static void appendDecimalValues(ColumnVector target, DecimalType type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (type.isShort()) {
            DecimalColumnVector decimalTarget = (DecimalColumnVector) target;
            if (block.mayHaveNull()) {
                for (int i = offset; i < end; i++) {
                    if (block.isNull(i)) {
                        decimalTarget.addNull();
                    }
                    else {
                        decimalTarget.add(type.getLong(block, i));
                    }
                }
            }
            else {
                for (int i = offset; i < end; i++) {
                    decimalTarget.add(type.getLong(block, i));
                }
            }
        }
        else {
            LongDecimalColumnVector decimalTarget = (LongDecimalColumnVector) target;
            if (block.mayHaveNull()) {
                for (int i = offset; i < end; i++) {
                    if (block.isNull(i)) {
                        decimalTarget.addNull();
                    }
                    else {
                        Int128 value = (Int128) type.getObject(block, i);
                        decimalTarget.add(value.getHigh(), value.getLow());
                    }
                }
            }
            else {
                for (int i = offset; i < end; i++) {
                    Int128 value = (Int128) type.getObject(block, i);
                    decimalTarget.add(value.getHigh(), value.getLow());
                }
            }
        }
    }

    private static void appendSliceValues(BinaryColumnVector target, Type type, Block block,
            int offset, int length)
    {
        int end = offset + length;
        if (block.mayHaveNull()) {
            for (int i = offset; i < end; i++) {
                if (block.isNull(i)) {
                    target.addNull();
                }
                else {
                    appendSlice(target, type.getSlice(block, i));
                }
            }
        }
        else {
            for (int i = offset; i < end; i++) {
                appendSlice(target, type.getSlice(block, i));
            }
        }
    }

    private static void appendRepeated(ColumnVector target, PixelsColumnHandle column,
            Block valueBlock, int length)
    {
        if (valueBlock.isNull(0)) {
            for (int i = 0; i < length; i++) {
                target.addNull();
            }
            return;
        }

        Type type = column.getColumnType();
        switch (column.getTypeCategory()) {
            case BOOLEAN:
                boolean booleanValue = type.getBoolean(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((BooleanColumnVector) target).add(booleanValue);
                }
                break;
            case BYTE:
                byte byteValue = (byte) type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((ByteColumnVector) target).add(byteValue);
                }
                break;
            case SHORT:
                short shortValue = (short) type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((ShortColumnVector) target).add(shortValue);
                }
                break;
            case INT:
                int intValue = (int) type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((IntColumnVector) target).add(intValue);
                }
                break;
            case LONG:
                long longValue = type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((LongColumnVector) target).add(longValue);
                }
                break;
            case FLOAT:
                float floatValue = (float) type.getDouble(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((FloatColumnVector) target).add(floatValue);
                }
                break;
            case DOUBLE:
                double doubleValue = type.getDouble(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((DoubleColumnVector) target).add(doubleValue);
                }
                break;
            case DATE:
                int dateValue = (int) type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((DateColumnVector) target).add(dateValue);
                }
                break;
            case TIME:
                int timeValue = (int) (type.getLong(valueBlock, 0) / 1_000_000_000L);
                for (int i = 0; i < length; i++) {
                    ((TimeColumnVector) target).add(timeValue);
                }
                break;
            case TIMESTAMP:
                long timestampValue = type.getLong(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    ((TimestampColumnVector) target).add(timestampValue);
                }
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                if (decimalType.isShort()) {
                    long decimalValue = decimalType.getLong(valueBlock, 0);
                    for (int i = 0; i < length; i++) {
                        ((DecimalColumnVector) target).add(decimalValue);
                    }
                }
                else {
                    Int128 decimalValue = (Int128) decimalType.getObject(valueBlock, 0);
                    for (int i = 0; i < length; i++) {
                        ((LongDecimalColumnVector) target).add(decimalValue.getHigh(), decimalValue.getLow());
                    }
                }
                break;
            case STRING:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                Slice slice = type.getSlice(valueBlock, 0);
                for (int i = 0; i < length; i++) {
                    appendSlice((BinaryColumnVector) target, slice);
                }
                break;
            default:
                throw new TrinoException(PIXELS_DATA_TYPE_ERROR,
                        "Pixels INSERT does not support column type " + column.getTypeCategory() + " yet");
        }
    }

    /**
     * Keep heap-backed Trino slices by reference.  The page sink writes the
     * batch synchronously before returning from appendPage, and the vector
     * itself retains the backing array until the batch is flushed or reset.
     * Direct/non-heap slices use the existing copying fallback.
     */
    private static void appendSlice(BinaryColumnVector target, Slice slice)
    {
        try {
            target.setRef(target.getWriteIndex(), slice.byteArray(),
                    slice.byteArrayOffset(), slice.length());
        }
        catch (UnsupportedOperationException e) {
            target.add(slice.getBytes());
        }
    }

    private static void appendDecimal(ColumnVector target, PixelsColumnHandle column, Block block, int position)
    {
        DecimalType type = (DecimalType) column.getColumnType();
        if (type.isShort()) {
            ((DecimalColumnVector) target).add(type.getLong(block, position));
        }
        else {
            Int128 value = (Int128) type.getObject(block, position);
            ((LongDecimalColumnVector) target).add(value.getHigh(), value.getLow());
        }
    }
}
