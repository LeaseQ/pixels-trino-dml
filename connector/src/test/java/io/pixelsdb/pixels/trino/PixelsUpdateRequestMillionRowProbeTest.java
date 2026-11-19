/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static io.pixelsdb.pixels.core.TypeDescription.Category.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Manually selected probe for exact UPDATE request construction. */
class PixelsUpdateRequestMillionRowProbeTest
{
    @Test
    void buildsTheConfiguredNumberOfExactUpdateKeysIntoProtobuf()
    {
        int rows = Integer.getInteger("pixels.update.request.probe.rows", 100_000);
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        Domain keys = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, rows - 1L, true)), false);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(id, keys))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        ProtobufTagIndex index = new ProtobufTagIndex();

        System.out.printf("[PIXELS_PROTOBUF_RUNTIME] generated=%s protobuf=%s methods=%s%n",
                IndexProto.class.getProtectionDomain().getCodeSource().getLocation(),
                GeneratedMessageV3.class.getProtectionDomain().getCodeSource().getLocation(),
                Arrays.stream(GeneratedMessageV3.class.getDeclaredMethods())
                        .map(method -> method.getName())
                        .distinct()
                        .sorted()
                        .collect(Collectors.joining(",")));
        long start = System.nanoTime();
        PixelsUpdateExecutor.Result result = PixelsUpdateExecutor.executeWithMetrics(
                update, List.of(id), index);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(rows, result.updatedRows());
        assertEquals(1, index.batchCount);
        System.out.printf("[PIXELS_UPDATE_REQUEST_PROBE] rows=%d elapsed_ms=%d encoding_ms=%.3f " +
                        "protobuf_build_ms=%.3f request_bytes=%d%n",
                rows, elapsedMs, result.encodingNanos() / 1_000_000.0,
                index.buildNanos / 1_000_000.0, index.requestBytes);
    }

    @Test
    void measuresRepeatedPrimaryKeyEncodingAcrossAssignments()
    {
        int rows = Integer.getInteger("pixels.update.request.probe.rows", 100_000);
        long[] primaryKeyCounts = new long[] {1, 2, 4, 8};
        for (long assignmentCount : primaryKeyCounts)
        {
            PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
            List<PixelsColumnHandle> columns = new ArrayList<>();
            columns.add(id);
            Map<PixelsColumnHandle, Constant> assignments = new LinkedHashMap<>();
            for (int ordinal = 1; ordinal <= assignmentCount; ordinal++)
            {
                PixelsColumnHandle label = column("label_" + ordinal, VarcharType.VARCHAR, STRING, ordinal);
                columns.add(label);
                assignments.put(label, new Constant("vip" + ordinal, VarcharType.VARCHAR));
            }
            Domain keys = Domain.create(ValueSet.ofRanges(
                    Range.range(BigintType.BIGINT, 0L, true, rows - 1L, true)), false);
            PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                    tableHandle(columns, TupleDomain.withColumnDomains(Map.of(id, keys))), assignments);
            ProtobufTagIndex index = new ProtobufTagIndex();
            PixelsUpdateExecutor.Result result = PixelsUpdateExecutor.executeWithMetrics(
                    update, List.of(id), index);
            assertEquals(rows, result.updatedRows());
            assertEquals(1, index.batchCount);
            System.out.printf("[PIXELS_UPDATE_REQUEST_WIDTH_PROBE] rows=%d assignments=%d " +
                            "request_bytes=%d build_ms=%.3f%n",
                    rows, assignmentCount, index.requestBytes, index.buildNanos / 1_000_000.0);
        }
    }

    private static PixelsTableHandle tableHandle(
            List<PixelsColumnHandle> columns,
            TupleDomain<PixelsColumnHandle> constraint)
    {
        return new PixelsTableHandle(
                "pixels", "default", "users", "users_alias", columns, constraint,
                io.pixelsdb.pixels.planner.plan.logical.Table.TableType.BASE,
                null, null, Storage.Scheme.file, List.of("file:///tmp/users"));
    }

    private static PixelsColumnHandle column(
            String name,
            io.trino.spi.type.Type type,
            TypeDescription.Category category,
            int ordinal)
    {
        return new PixelsColumnHandle(
                "pixels", "default", "users", name, name, type, category, "", ordinal);
    }

    private static final class ProtobufTagIndex implements PixelsUpdateExecutor.TagIndex
    {
        private int batchCount;
        private long buildNanos;
        private int requestBytes;

        @Override
        public void append(List<PixelsUpdateExecutor.TagUpdate> updates)
        {
            long start = System.nanoTime();
            List<IndexProto.TagIndexUpdate> requests = new ArrayList<>(updates.size());
            for (PixelsUpdateExecutor.TagUpdate update : updates)
            {
                IndexProto.TagIndexUpdate.Builder builder = IndexProto.TagIndexUpdate.newBuilder()
                        .setTag(ByteString.copyFrom(update.tag()))
                        .setTransactionTimestamp(1L);
                for (byte[] primaryKey : update.primaryKeys())
                {
                    builder.addPrimaryKeys(ByteString.copyFrom(primaryKey));
                }
                requests.add(builder.build());
            }
            IndexProto.AppendTagIndexEntriesRequest request =
                    IndexProto.AppendTagIndexEntriesRequest.newBuilder()
                            .setTableId(22L)
                            .setIndexId(11L)
                            .addAllUpdates(requests)
                            .build();
            requestBytes += request.getSerializedSize();
            buildNanos += System.nanoTime() - start;
            batchCount++;
        }
    }
}
