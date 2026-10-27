/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.type.Type;
import io.trino.spi.type.TinyintType;

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

    PixelsMergeSink(PixelsMergeTableHandle handle)
    {
        this(handle, new RpcTagIndex(handle));
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
                tagIndex.append(encodeValue(updatedColumn, page.getBlock(dataIndex), position),
                        List.of(primaryKey));
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
            tagIndex.close();
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
        void append(byte[] tag, List<byte[]> primaryKeys);

        void close();
    }

    private static final class RpcTagIndex implements TagIndex
    {
        private final PixelsMergeTableHandle handle;
        private final IndexService indexService;

        private RpcTagIndex(PixelsMergeTableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.rpc);
        }

        @Override
        public void append(byte[] tag, List<byte[]> primaryKeys)
        {
            try
            {
                List<ByteString> encodedPrimaryKeys = primaryKeys.stream()
                        .map(ByteString::copyFrom)
                        .toList();
                indexService.appendTagIndexEntries(handle.getTableId(), handle.getIndexId(), List.of(
                        IndexProto.TagIndexUpdate.newBuilder()
                                .setTag(ByteString.copyFrom(tag))
                                .addAllPrimaryKeys(encodedPrimaryKeys)
                                .build()));
            }
            catch (IndexException e)
            {
                throw new TrinoException(PIXELS_INVERTED_INDEX_ERROR,
                        "failed to append Pixels UPDATE tag index through IndexServer", e);
            }
        }

        @Override
        public void close()
        {
        }
    }
}
