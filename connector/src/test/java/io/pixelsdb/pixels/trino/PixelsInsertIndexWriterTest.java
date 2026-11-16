/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.index.IndexProto;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PixelsInsertIndexWriterTest
{
    @Test
    void writesPrimaryKeyAndPhysicalPositionTogether() throws Exception
    {
        List<IndexProto.PrimaryIndexEntry> written = new ArrayList<>();
        List<Long> flushedFiles = new ArrayList<>();
        IndexService service = (IndexService) Proxy.newProxyInstance(
                IndexService.class.getClassLoader(), new Class<?>[] {IndexService.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allocateRowIdBatch")) {
                        return IndexProto.RowIdBatch.newBuilder().setRowIdStart(100).setLength(16).build();
                    }
                    if (method.getName().equals("putPrimaryIndexEntries")) {
                        written.addAll((List<IndexProto.PrimaryIndexEntry>) args[2]);
                        return true;
                    }
                    if (method.getName().equals("flushIndexEntriesOfFile")) {
                        flushedFiles.add((Long) args[2]);
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        PixelsInsertIndexWriter index = new PixelsInsertIndexWriter(
                service, 5L, 6L, 99L, 1234L,
                List.of(new PixelsColumnHandle("pixels", "default", "users", "id", "id",
                        BigintType.BIGINT, LONG, "", 0)),
                List.of(0),
                ignored -> 0);
        VectorizedRowBatch batch = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long")).createRowBatchWithHiddenColumn(2);
        ((LongColumnVector) batch.cols[0]).add(11L);
        ((LongColumnVector) batch.cols[0]).add(22L);
        batch.size = 2;

        index.appendBatch(batch, 3, 7);
        index.finish();

        assertEquals(2, written.size());
        assertEquals(List.of(100L, 101L), written.stream().map(IndexProto.PrimaryIndexEntry::getRowId).toList());
        assertEquals(List.of(7, 8), written.stream()
                .map(entry -> entry.getRowLocation().getRgRowOffset()).toList());
        assertEquals(List.of(3, 3), written.stream()
                .map(entry -> entry.getRowLocation().getRgId()).toList());
        assertEquals(List.of(99L, 99L), written.stream()
                .map(entry -> entry.getRowLocation().getFileId()).toList());
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("11"),
                written.get(0).getIndexKey().getKey().toByteArray());
        assertEquals(List.of(99L), flushedFiles);
    }

    @Test
    void abortRemovesPrimaryAndMainIndexEntries() throws Exception
    {
        List<IndexProto.IndexKey> deletedKeys = new ArrayList<>();
        List<IndexProto.IndexKey> purgedKeys = new ArrayList<>();
        List<List<IndexProto.IndexKey>> purgedBatches = new ArrayList<>();
        int[] deleteCalls = {0};
        int[] purgeCalls = {0};
        IndexService service = (IndexService) Proxy.newProxyInstance(
                IndexService.class.getClassLoader(), new Class<?>[] {IndexService.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allocateRowIdBatch")) {
                        return IndexProto.RowIdBatch.newBuilder().setRowIdStart(100).setLength(16).build();
                    }
                    if (method.getName().equals("putPrimaryIndexEntries")) {
                        return true;
                    }
                    if (method.getName().equals("flushIndexEntriesOfFile")) {
                        return false;
                    }
                    if (method.getName().equals("deletePrimaryIndexEntries")) {
                        deleteCalls[0]++;
                        deletedKeys.addAll((List<IndexProto.IndexKey>) args[2]);
                        return List.of();
                    }
                    if (method.getName().equals("purgeIndexEntries")) {
                        purgeCalls[0]++;
                        List<IndexProto.IndexKey> keys = (List<IndexProto.IndexKey>) args[2];
                        purgedKeys.addAll(keys);
                        purgedBatches.add(List.copyOf(keys));
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        PixelsInsertIndexWriter index = new PixelsInsertIndexWriter(
                service, 5L, 6L, 99L, 1234L,
                List.of(new PixelsColumnHandle("pixels", "default", "users", "id", "id",
                        BigintType.BIGINT, LONG, "", 0)),
                List.of(0),
                ignored -> 0);
        VectorizedRowBatch batch = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long")).createRowBatchWithHiddenColumn(2);
        ((LongColumnVector) batch.cols[0]).add(11L);
        ((LongColumnVector) batch.cols[0]).add(22L);
        batch.size = 2;

        index.appendBatch(batch, 3, 7);
        assertThrows(java.io.IOException.class, index::finish);
        index.abort();
        index.abort();

        assertEquals(1, deleteCalls[0]);
        assertEquals(2, purgeCalls[0]);
        assertEquals(List.of(
                IndexProto.IndexKey.newBuilder().setTableId(5L).setIndexId(6L)
                        .setKey(com.google.protobuf.ByteString.copyFrom(
                                TypeDescription.fromString("long").convertSqlStringToByte("11")))
                        .setTimestamp(1235L).build(),
                IndexProto.IndexKey.newBuilder().setTableId(5L).setIndexId(6L)
                        .setKey(com.google.protobuf.ByteString.copyFrom(
                                TypeDescription.fromString("long").convertSqlStringToByte("22")))
                        .setTimestamp(1235L).build()), deletedKeys);
        assertEquals(deletedKeys, purgedKeys);
        assertEquals(List.of(List.of(deletedKeys.get(0)), List.of(deletedKeys.get(1))), purgedBatches);
    }

    @Test
    void batchesPrimaryIndexWritesAcrossRowGroupsUntilFinish() throws Exception
    {
        int[] putCalls = {0};
        List<Integer> batchSizes = new ArrayList<>();
        IndexService service = (IndexService) Proxy.newProxyInstance(
                IndexService.class.getClassLoader(), new Class<?>[] {IndexService.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allocateRowIdBatch")) {
                        return IndexProto.RowIdBatch.newBuilder().setRowIdStart(100).setLength(16).build();
                    }
                    if (method.getName().equals("putPrimaryIndexEntries")) {
                        putCalls[0]++;
                        batchSizes.add(((List<IndexProto.PrimaryIndexEntry>) args[2]).size());
                        return true;
                    }
                    if (method.getName().equals("flushIndexEntriesOfFile")) {
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        PixelsInsertIndexWriter index = new PixelsInsertIndexWriter(
                service, 5L, 6L, 99L, 1234L,
                List.of(new PixelsColumnHandle("pixels", "default", "users", "id", "id",
                        BigintType.BIGINT, LONG, "", 0)),
                List.of(0),
                ignored -> 0);

        index.appendBatch(batch(11L), 0, 0);
        index.appendBatch(batch(22L), 1, 0);

        assertEquals(0, putCalls[0]);
        index.finish();

        assertEquals(1, putCalls[0]);
        assertEquals(List.of(2), batchSizes);
    }

    @Test
    void allocatesRowIdsInLargeBatches() throws Exception
    {
        int[] allocationCalls = {0};
        List<Integer> requestedBatchSizes = new ArrayList<>();
        IndexService service = (IndexService) Proxy.newProxyInstance(
                IndexService.class.getClassLoader(), new Class<?>[] {IndexService.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allocateRowIdBatch")) {
                        allocationCalls[0]++;
                        requestedBatchSizes.add((Integer) args[1]);
                        return IndexProto.RowIdBatch.newBuilder().setRowIdStart(100).setLength(65536).build();
                    }
                    if (method.getName().equals("putPrimaryIndexEntries") ||
                            method.getName().equals("flushIndexEntriesOfFile")) {
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        PixelsInsertIndexWriter index = new PixelsInsertIndexWriter(
                service, 5L, 6L, 99L, 1234L,
                List.of(new PixelsColumnHandle("pixels", "default", "users", "id", "id",
                        BigintType.BIGINT, LONG, "", 0)),
                List.of(0),
                ignored -> 0);
        VectorizedRowBatch batch = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long")).createRowBatchWithHiddenColumn(4096);
        for (int row = 0; row < 2049; row++) {
            ((LongColumnVector) batch.cols[0]).add(row);
        }
        batch.size = 2049;

        index.appendBatch(batch, 0, 0);
        index.finish();

        assertEquals(1, allocationCalls[0]);
        assertEquals(List.of(65536), requestedBatchSizes);
    }

    @Test
    void boundsPendingPrimaryIndexEntriesPerBucket() throws Exception
    {
        int[] putCalls = {0};
        List<Integer> batchSizes = new ArrayList<>();
        IndexService service = (IndexService) Proxy.newProxyInstance(
                IndexService.class.getClassLoader(), new Class<?>[] {IndexService.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("allocateRowIdBatch")) {
                        return IndexProto.RowIdBatch.newBuilder().setRowIdStart(100).setLength(131072).build();
                    }
                    if (method.getName().equals("putPrimaryIndexEntries")) {
                        putCalls[0]++;
                        batchSizes.add(((List<IndexProto.PrimaryIndexEntry>) args[2]).size());
                        return true;
                    }
                    if (method.getName().equals("flushIndexEntriesOfFile")) {
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        PixelsInsertIndexWriter index = new PixelsInsertIndexWriter(
                service, 5L, 6L, 99L, 1234L,
                List.of(new PixelsColumnHandle("pixels", "default", "users", "id", "id",
                        BigintType.BIGINT, LONG, "", 0)),
                List.of(0),
                ignored -> 0);
        VectorizedRowBatch batch = batch(33L);
        for (int row = 0; row < PixelsInsertIndexWriter.MAX_PENDING_ENTRIES_PER_BUCKET + 1; row++) {
            index.appendBatch(batch, row, 0);
        }

        assertEquals(1, putCalls[0]);
        assertEquals(List.of(PixelsInsertIndexWriter.MAX_PENDING_ENTRIES_PER_BUCKET), batchSizes);
        index.finish();

        assertEquals(2, putCalls[0]);
        assertEquals(List.of(PixelsInsertIndexWriter.MAX_PENDING_ENTRIES_PER_BUCKET, 1), batchSizes);
    }

    private static VectorizedRowBatch batch(long value)
    {
        VectorizedRowBatch batch = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("long")).createRowBatchWithHiddenColumn(2);
        ((LongColumnVector) batch.cols[0]).add(value);
        batch.size = 1;
        return batch;
    }
}
