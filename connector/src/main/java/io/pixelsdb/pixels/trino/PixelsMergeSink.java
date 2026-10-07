/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.rocksdb.PixelsTagIndex;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.type.Type;
import io.trino.spi.type.TinyintType;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_INVERTED_INDEX_ERROR;
import static java.util.Objects.requireNonNull;

/** Writes Trino UPDATE rows to the Pixels tag index without changing Pixels files. */
final class PixelsMergeSink implements ConnectorMergeSink
{
    private final PixelsMergeTableHandle handle;
    private final TagIndex tagIndex;
    private boolean closed;

    PixelsMergeSink(PixelsMergeTableHandle handle) throws RocksDBException, SinglePointIndexException, IOException
    {
        this(handle, new RocksTagIndex(handle));
    }

    PixelsMergeSink(PixelsMergeTableHandle handle, TagIndex tagIndex)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.tagIndex = requireNonNull(tagIndex, "tagIndex is null");
    }

    @Override
    public void storeMergedRows(Page page)
    {
        requireNonNull(page, "page is null");
        List<PixelsColumnHandle> dataColumns = handle.getTableHandle().getColumns();
        if (page.getChannelCount() < dataColumns.size() + 3)
        {
            throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                    "Pixels UPDATE merge page has an unexpected channel count: " + page.getChannelCount());
        }
        if (handle.getPrimaryKeyColumns().size() != 1)
        {
            throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                    "Pixels UPDATE currently supports one primary-key column per merge row");
        }
        PixelsColumnHandle primaryKeyColumn = handle.getPrimaryKeyColumns().get(0);
        int operationChannel = dataColumns.size();
        int rowIdChannel = dataColumns.size() + 2;
        Block operationBlock = page.getBlock(operationChannel);
        Block rowIdBlock = page.getBlock(rowIdChannel);
        for (int position = 0; position < page.getPositionCount(); position++)
        {
            if (operationBlock.isNull(position))
            {
                throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                        "Pixels merge operation must not be null");
            }
            if (TinyintType.TINYINT.getLong(operationBlock, position) != UPDATE_OPERATION_NUMBER)
            {
                throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                        "Pixels merge sink supports UPDATE only; DELETE and INSERT are not supported");
            }
            byte[] primaryKey = encodeValue(primaryKeyColumn, rowIdBlock, position);
            for (PixelsColumnHandle updatedColumn : handle.getUpdatedColumns())
            {
                int dataIndex = dataColumns.indexOf(updatedColumn);
                if (dataIndex < 0)
                {
                    throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                            "updated column is not present in the merge page: " + updatedColumn.getColumnName());
                }
                try
                {
                    tagIndex.append(encodeValue(updatedColumn, page.getBlock(dataIndex), position),
                            List.of(primaryKey));
                }
                catch (RocksDBException e)
                {
                    throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                            "failed to append Pixels UPDATE tag index", e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        closeIndex();
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public void abort()
    {
        closeIndex();
    }

    private void closeIndex()
    {
        if (!closed)
        {
            try
            {
                tagIndex.close();
            }
            catch (IOException e)
            {
                throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                        "failed to close Pixels UPDATE tag index", e);
            }
            closed = true;
        }
    }

    private static byte[] encodeValue(PixelsColumnHandle column, Block block, int position)
    {
        if (block.isNull(position))
        {
            throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                    "null values are not supported in Pixels UPDATE tags or primary keys");
        }
        Type type = column.getColumnType();
        String value;
        if (column.getTypeCategory() == TypeDescription.Category.VARCHAR ||
                column.getTypeCategory() == TypeDescription.Category.STRING ||
                column.getTypeCategory() == TypeDescription.Category.VARBINARY ||
                column.getTypeCategory() == TypeDescription.Category.BINARY)
        {
            Slice slice = type.getSlice(block, position);
            value = slice.toStringUtf8();
        }
        else if (column.getTypeCategory() == TypeDescription.Category.BOOLEAN)
        {
            value = Boolean.toString(type.getBoolean(block, position));
        }
        else
        {
            value = String.valueOf(type.getLong(block, position));
        }
        return TypeDescription.fromString(type.getDisplayName()).convertSqlStringToByte(value);
    }

    interface TagIndex
    {
        void append(byte[] tag, List<byte[]> primaryKeys) throws RocksDBException;

        void close() throws IOException;
    }

    private static final class RocksTagIndex implements TagIndex
    {
        private final PixelsTagIndex delegate;

        private RocksTagIndex(PixelsMergeTableHandle handle)
                throws RocksDBException, SinglePointIndexException, IOException
        {
            this.delegate = new PixelsTagIndex(handle.getTableId(), handle.getIndexId(), 0);
        }

        @Override
        public void append(byte[] tag, List<byte[]> primaryKeys) throws RocksDBException
        {
            delegate.append(tag, primaryKeys);
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }
    }
}
