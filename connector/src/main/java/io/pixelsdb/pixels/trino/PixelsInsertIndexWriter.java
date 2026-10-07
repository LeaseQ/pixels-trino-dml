/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.PrimaryKeyBytes;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.index.IndexProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

/** Writes each completed INSERT batch to the primary and main indexes. */
final class PixelsInsertIndexWriter implements PixelsPageSink.InsertIndexWriter
{
    private static final int ROW_ID_BATCH_SIZE = 1024;

    private final IndexService indexService;
    private final RowIdAllocator rowIdAllocator;
    private final long tableId;
    private final long indexId;
    private final long fileId;
    private final long timestamp;
    private final List<TypeDescription> primaryKeyTypes;
    private final int[] primaryKeyOrdinals;
    private final ToIntFunction<ByteString> bucketIdResolver;
    private final Map<Integer, List<IndexProto.IndexKey>> insertedKeysByBucket = new HashMap<>();
    private boolean hasEntries;
    private boolean finished;
    private boolean aborted;

    PixelsInsertIndexWriter(IndexService indexService, long tableId, long indexId, long fileId,
            long timestamp, List<PixelsColumnHandle> columns, List<Integer> primaryKeyOrdinals)
    {
        this(indexService, tableId, indexId, fileId, timestamp, columns, primaryKeyOrdinals,
                IndexUtils::getBucketIdFromByteBuffer);
    }

    PixelsInsertIndexWriter(IndexService indexService, long tableId, long indexId, long fileId,
            long timestamp, List<PixelsColumnHandle> columns, List<Integer> primaryKeyOrdinals,
            ToIntFunction<ByteString> bucketIdResolver)
    {
        this.indexService = requireNonNull(indexService, "indexService is null");
        this.bucketIdResolver = requireNonNull(bucketIdResolver, "bucketIdResolver is null");
        this.tableId = tableId;
        this.indexId = indexId;
        this.fileId = fileId;
        this.timestamp = timestamp;
        requireNonNull(columns, "columns is null");
        requireNonNull(primaryKeyOrdinals, "primaryKeyOrdinals is null");
        if (primaryKeyOrdinals.isEmpty()) {
            throw new IllegalArgumentException("primary key has no columns");
        }
        this.primaryKeyTypes = new ArrayList<>(primaryKeyOrdinals.size());
        this.primaryKeyOrdinals = new int[primaryKeyOrdinals.size()];
        for (int keyPosition = 0; keyPosition < primaryKeyOrdinals.size(); keyPosition++) {
            int ordinal = primaryKeyOrdinals.get(keyPosition);
            if (ordinal < 0 || ordinal >= columns.size()) {
                throw new IllegalArgumentException("primary key column is absent from the INSERT columns");
            }
            this.primaryKeyOrdinals[keyPosition] = ordinal;
            this.primaryKeyTypes.add(TypeDescription.fromString(
                    columns.get(ordinal).getColumnType().getDisplayName()));
        }
        this.rowIdAllocator = new RowIdAllocator(tableId, ROW_ID_BATCH_SIZE, indexService);
    }

    @Override
    public void appendBatch(VectorizedRowBatch batch, int rowGroupId, int firstRowOffset) throws IOException
    {
        Map<Integer, List<IndexProto.PrimaryIndexEntry>> byBucket = new HashMap<>();
        try {
            for (int position = 0; position < batch.size; position++) {
                ByteString key = ByteString.copyFrom(PrimaryKeyBytes.fromColumnVectors(
                        primaryKeyTypes, batch.cols, primaryKeyOrdinals, position));
                int bucket = bucketIdResolver.applyAsInt(key);
                IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                        .setIndexKey(IndexProto.IndexKey.newBuilder()
                                .setTableId(tableId).setIndexId(indexId)
                                .setKey(key).setTimestamp(timestamp))
                        .setRowId(rowIdAllocator.getRowId())
                        .setRowLocation(IndexProto.RowLocation.newBuilder()
                                .setFileId(fileId).setRgId(rowGroupId)
                                .setRgRowOffset(firstRowOffset + position))
                        .build();
                byBucket.computeIfAbsent(bucket, ignored -> new ArrayList<>()).add(entry);
            }
            for (Map.Entry<Integer, List<IndexProto.PrimaryIndexEntry>> bucket : byBucket.entrySet()) {
                IndexOption option = IndexOption.builder().vNodeId(bucket.getKey()).build();
                if (!indexService.putPrimaryIndexEntries(tableId, indexId, bucket.getValue(), option)) {
                    throw new IOException("failed to write Pixels INSERT primary index entries");
                }
                insertedKeysByBucket.computeIfAbsent(bucket.getKey(), ignored -> new ArrayList<>())
                        .addAll(bucket.getValue().stream().map(IndexProto.PrimaryIndexEntry::getIndexKey).toList());
                hasEntries = true;
            }
        }
        catch (IndexException | RuntimeException e) {
            throw new IOException("failed to index Pixels INSERT rows", e);
        }
    }

    @Override
    public void finish() throws IOException
    {
        if (!hasEntries) {
            return;
        }
        try {
            if (!indexService.flushIndexEntriesOfFile(tableId, indexId, fileId, true, new IndexOption())) {
                throw new IOException("failed to flush Pixels INSERT main index");
            }
            finished = true;
        }
        catch (IndexException e) {
            throw new IOException("failed to flush Pixels INSERT main index", e);
        }
    }

    @Override
    public void abort() throws IOException
    {
        if (aborted || finished) {
            return;
        }
        try {
            for (Map.Entry<Integer, List<IndexProto.IndexKey>> bucket : insertedKeysByBucket.entrySet()) {
                if (bucket.getValue().isEmpty()) {
                    continue;
                }
                IndexOption option = IndexOption.builder().vNodeId(bucket.getKey()).build();
                // Use a later version so the delete creates a tombstone instead of
                // overwriting the INSERT version before purge can recover its row id.
                List<IndexProto.IndexKey> cleanupKeys = bucket.getValue().stream()
                        .map(PixelsInsertIndexWriter::createAbortKey)
                        .toList();
                indexService.deletePrimaryIndexEntries(tableId, indexId, cleanupKeys, option);
                // Purge one key at a time. The current purge RPC removes a main-index
                // row range between the first and last returned row id; row ids from
                // one bucket are not guaranteed to be contiguous.
                for (IndexProto.IndexKey cleanupKey : cleanupKeys) {
                    if (!indexService.purgeIndexEntries(tableId, indexId,
                            List.of(cleanupKey), true, option)) {
                        throw new IOException("failed to purge Pixels INSERT index entries");
                    }
                }
            }
            insertedKeysByBucket.clear();
            aborted = true;
        }
        catch (IndexException | RuntimeException e) {
            throw new IOException("failed to remove Pixels INSERT index entries", e);
        }
    }

    private static IndexProto.IndexKey createAbortKey(IndexProto.IndexKey key)
    {
        if (key.getTimestamp() == Long.MAX_VALUE) {
            throw new IllegalStateException("cannot create an abort version after Long.MAX_VALUE");
        }
        return key.toBuilder().setTimestamp(key.getTimestamp() + 1).build();
    }
}
