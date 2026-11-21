/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.google.protobuf.ByteString;
import io.airlift.log.Logger;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

/** Writes each completed INSERT batch to the primary and main indexes. */
final class PixelsInsertIndexWriter implements PixelsPageSink.InsertIndexWriter
{
    private static final Logger logger = Logger.get(PixelsInsertIndexWriter.class);

    /** Reduce row-id allocation RPCs for large INSERT sinks. */
    private static final int ROW_ID_BATCH_SIZE = 65_536;
    /** Keep one INSERT index RPC bounded without returning to row-group-sized batches. */
    static final int MAX_PENDING_ENTRIES_PER_BUCKET = 65_536;
    /** Experimental opt-in parallelism for independent vnode primary-index RPCs. */
    static final String PRIMARY_FLUSH_PARALLELISM_PROPERTY =
            "pixels.insert.index.flush.parallelism";
    /** Experimental opt-in single-request batching across vnode primary-index writes. */
    static final String GROUPED_PRIMARY_WRITE_PROPERTY =
            "pixels.insert.index.grouped.primary.write";
    /** Keep grouped requests below the daemon's default 64 MiB inbound RPC limit. */
    static final String GROUPED_PRIMARY_WRITE_MAX_ENTRIES_PROPERTY =
            "pixels.insert.index.grouped.primary.write.max.entries";
    static final int DEFAULT_GROUPED_PRIMARY_WRITE_MAX_ENTRIES = 200_000;
    private final IndexService indexService;
    private final RowIdAllocator rowIdAllocator;
    private final long tableId;
    private final long indexId;
    private final long fileId;
    private final long timestamp;
    private final List<TypeDescription> primaryKeyTypes;
    private final int[] primaryKeyOrdinals;
    private final ToIntFunction<ByteString> bucketIdResolver;
    private final Map<Integer, List<IndexProto.PrimaryIndexEntry>> pendingEntriesByBucket = new HashMap<>();
    private final Map<Integer, List<IndexProto.IndexKey>> insertedKeysByBucket = new HashMap<>();
    private long minRowId = Long.MAX_VALUE;
    private long maxRowId = -1L;
    private long appendNanos;
    private long primaryPutNanos;
    private long primaryPutWallNanos;
    private long mainFlushNanos;
    private int primaryRpcCount;
    private long indexedRows;
    private boolean hasEntries;
    private boolean hadStagedBucketFlush;
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
        long start = System.nanoTime();
        try {
            for (int position = 0; position < batch.size; position++) {
                ByteString key = ByteString.copyFrom(PrimaryKeyBytes.fromColumnVectors(
                        primaryKeyTypes, batch.cols, primaryKeyOrdinals, position));
                long rowId = rowIdAllocator.getRowId();
                minRowId = Math.min(minRowId, rowId);
                maxRowId = Math.max(maxRowId, rowId);
                int bucket = bucketIdResolver.applyAsInt(key);
                IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                        .setIndexKey(IndexProto.IndexKey.newBuilder()
                                .setTableId(tableId).setIndexId(indexId)
                                .setKey(key).setTimestamp(timestamp))
                        .setRowId(rowId)
                        .setRowLocation(IndexProto.RowLocation.newBuilder()
                                .setFileId(fileId).setRgId(rowGroupId)
                                .setRgRowOffset(firstRowOffset + position))
                        .build();
                List<IndexProto.PrimaryIndexEntry> pendingEntries =
                        pendingEntriesByBucket.computeIfAbsent(bucket, ignored -> new ArrayList<>());
                pendingEntries.add(entry);
                if (pendingEntries.size() >= MAX_PENDING_ENTRIES_PER_BUCKET)
                {
                    hadStagedBucketFlush = true;
                    flushPendingBucket(bucket);
                }
            }
            hasEntries = hasEntries || batch.size > 0;
            indexedRows += batch.size;
        }
        catch (IndexException | RuntimeException e) {
            throw new IOException("failed to index Pixels INSERT rows", e);
        }
        finally {
            appendNanos += System.nanoTime() - start;
        }
    }

    @Override
    public long getMinRowId()
    {
        return hasEntries ? minRowId : 0L;
    }

    @Override
    public long getMaxRowId()
    {
        return hasEntries ? maxRowId : 0L;
    }

    @Override
    public void finish() throws IOException
    {
        if (!hasEntries) {
            return;
        }
        try {
            long primaryStart = System.nanoTime();
            flushPendingBuckets();
            primaryPutWallNanos += System.nanoTime() - primaryStart;
            long flushStart = System.nanoTime();
            if (!indexService.flushIndexEntriesOfFile(tableId, indexId, fileId, true, new IndexOption())) {
                throw new IOException("failed to flush Pixels INSERT main index");
            }
            mainFlushNanos += System.nanoTime() - flushStart;
            finished = true;
            logger.info("[PIXELS_INSERT_INDEX_METRIC] file_id=%s rows=%s primary_rpc_count=%s primary_flush_parallelism=%s append_ms=%.3f primary_put_ms=%.3f primary_put_wall_ms=%.3f main_flush_ms=%.3f",
                    fileId,
                    indexedRows,
                    primaryRpcCount,
                    effectivePrimaryFlushParallelism(),
                    appendNanos / 1_000_000.0,
                    primaryPutNanos / 1_000_000.0,
                    primaryPutWallNanos / 1_000_000.0,
                    mainFlushNanos / 1_000_000.0);
        }
        catch (IndexException e) {
            throw new IOException("failed to flush Pixels INSERT main index", e);
        }
    }

    private void flushPendingBuckets() throws IOException, IndexException
    {
        List<Integer> buckets = List.copyOf(pendingEntriesByBucket.keySet());
        if (buckets.isEmpty()) {
            return;
        }
        if (!hadStagedBucketFlush && Boolean.getBoolean(GROUPED_PRIMARY_WRITE_PROPERTY)) {
            flushPendingBucketsGrouped(buckets);
            return;
        }
        // A large INSERT may have already flushed the first batch of a bucket
        // while appendBatch was still producing rows. Keep later batches
        // serialized until mixed serial/parallel writes prove fully visible.
        int parallelism = hadStagedBucketFlush
                ? 1
                : Math.min(configuredPrimaryFlushParallelism(), buckets.size());
        if (parallelism <= 1) {
            for (Integer bucket : buckets) {
                flushPendingBucket(bucket);
            }
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        List<Future<BucketWriteResult>> futures = new ArrayList<>(buckets.size());
        try {
            for (Integer bucket : buckets) {
                List<IndexProto.PrimaryIndexEntry> entries = List.copyOf(pendingEntriesByBucket.get(bucket));
                futures.add(executor.submit(() -> writePendingBucket(bucket, entries)));
            }

            List<BucketWriteResult> results = new ArrayList<>(futures.size());
            IOException failure = null;
            for (Future<BucketWriteResult> future : futures) {
                try {
                    results.add(future.get());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failure = new IOException("interrupted while flushing Pixels INSERT primary index entries", e);
                }
                catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException) {
                        failure = (IOException) cause;
                    }
                    else if (cause instanceof IndexException) {
                        failure = new IOException("failed to flush Pixels INSERT primary index entries", cause);
                    }
                    else if (cause instanceof RuntimeException) {
                        failure = new IOException("failed to flush Pixels INSERT primary index entries", cause);
                    }
                    else {
                        failure = new IOException("failed to flush Pixels INSERT primary index entries", cause);
                    }
                }
            }
            // Record every completed write before propagating a peer failure so abort()
            // can remove entries that were already accepted by the index service.
            for (BucketWriteResult result : results) {
                recordCompletedBucket(result);
            }
            if (failure != null) {
                throw failure;
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    private void flushPendingBucketsGrouped(List<Integer> buckets) throws IOException, IndexException
    {
        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();
        for (Integer bucket : buckets) {
            entries.addAll(pendingEntriesByBucket.get(bucket));
        }
        int maxEntries = configuredGroupedPrimaryWriteMaxEntries();
        for (int offset = 0; offset < entries.size(); offset += maxEntries) {
            int end = Math.min(entries.size(), offset + maxEntries);
            List<IndexProto.PrimaryIndexEntry> chunk = entries.subList(offset, end);
            long start = System.nanoTime();
            if (!indexService.putPrimaryIndexEntries(tableId, indexId, chunk,
                    IndexOption.builder().vNodeId(-1).build())) {
                throw new IOException("failed to write grouped Pixels INSERT primary index entries");
            }
            primaryPutNanos += System.nanoTime() - start;
            primaryRpcCount++;
            recordCompletedGroupedEntries(chunk);
        }
        for (Integer bucket : buckets) {
            pendingEntriesByBucket.remove(bucket);
        }
    }

    private void recordCompletedGroupedEntries(List<IndexProto.PrimaryIndexEntry> entries)
    {
        for (IndexProto.PrimaryIndexEntry entry : entries) {
            int bucket = bucketIdResolver.applyAsInt(entry.getIndexKey().getKey());
            insertedKeysByBucket.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                    .add(entry.getIndexKey());
        }
    }

    private int configuredGroupedPrimaryWriteMaxEntries()
    {
        return Math.max(1, Integer.getInteger(GROUPED_PRIMARY_WRITE_MAX_ENTRIES_PROPERTY,
                DEFAULT_GROUPED_PRIMARY_WRITE_MAX_ENTRIES));
    }

    private int configuredPrimaryFlushParallelism()
    {
        return Math.max(1, Integer.getInteger(PRIMARY_FLUSH_PARALLELISM_PROPERTY, 1));
    }

    private int effectivePrimaryFlushParallelism()
    {
        return hadStagedBucketFlush ? 1 : configuredPrimaryFlushParallelism();
    }

    private void flushPendingBucket(int bucket) throws IOException, IndexException
    {
        List<IndexProto.PrimaryIndexEntry> entries = pendingEntriesByBucket.get(bucket);
        if (entries == null || entries.isEmpty())
        {
            return;
        }
        recordCompletedBucket(writePendingBucket(bucket, entries));
    }

    private BucketWriteResult writePendingBucket(int bucket,
            List<IndexProto.PrimaryIndexEntry> entries) throws IOException, IndexException
    {
        IndexOption option = IndexOption.builder().vNodeId(bucket).build();
        long start = System.nanoTime();
        if (!indexService.putPrimaryIndexEntries(tableId, indexId, entries, option)) {
            throw new IOException("failed to write Pixels INSERT primary index entries");
        }
        return new BucketWriteResult(bucket, entries, System.nanoTime() - start);
    }

    private void recordCompletedBucket(BucketWriteResult result)
    {
        primaryPutNanos += result.elapsedNanos;
        primaryRpcCount++;
        int bucket = result.bucket;
        List<IndexProto.PrimaryIndexEntry> entries = result.entries;
        pendingEntriesByBucket.remove(bucket);
        insertedKeysByBucket.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                .addAll(entries.stream().map(IndexProto.PrimaryIndexEntry::getIndexKey).toList());
    }

    private static final class BucketWriteResult
    {
        private final int bucket;
        private final List<IndexProto.PrimaryIndexEntry> entries;
        private final long elapsedNanos;

        private BucketWriteResult(int bucket, List<IndexProto.PrimaryIndexEntry> entries, long elapsedNanos)
        {
            this.bucket = bucket;
            this.entries = entries;
            this.elapsedNanos = elapsedNanos;
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
            pendingEntriesByBucket.clear();
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
