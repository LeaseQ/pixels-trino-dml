/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.TypeDescription;
import io.trino.spi.connector.SchemaTableName;
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
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static io.pixelsdb.pixels.core.TypeDescription.Category.LONG;
import static io.pixelsdb.pixels.core.TypeDescription.Category.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PixelsUpdateExecutorTest
{
    @Test
    void writesTheEncodedPrimaryKeyForAConstantTagAssignment()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(
                        id, Domain.singleValue(BigintType.BIGINT, 7L)))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        SinglePointIndex primaryIndex = primaryIndex();
        RecordingTagIndex index = new RecordingTagIndex();

        long updated = PixelsUpdateExecutor.execute(update, primaryIndex, index);

        assertEquals(1L, updated);
        assertEquals(1, index.entries.size());
        assertArrayEquals("vip".getBytes(), index.entries.get(0).tag);
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("7"),
                index.entries.get(0).primaryKeys.get(0));
    }

    @Test
    void writesAllPrimaryKeysForAnInPredicate()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(
                        id, Domain.multipleValues(BigintType.BIGINT, List.of(7L, 9L, 11L))))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        RecordingTagIndex index = new RecordingTagIndex();

        assertEquals(3L, PixelsUpdateExecutor.execute(update, primaryIndex(), index));
        assertEquals(3, index.entries.get(0).primaryKeys.size());
        assertArrayEquals(TypeDescription.fromString("long").convertSqlStringToByte("11"),
                index.entries.get(0).primaryKeys.get(2));
    }

    @Test
    void acceptsAnExactPrimaryKeyBatchLargerThanTheLegacyTenThousandLimit()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        List<Long> primaryKeys = LongStream.range(0, 10_001).boxed().toList();
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(
                        id, Domain.multipleValues(BigintType.BIGINT, primaryKeys)))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        RecordingTagIndex index = new RecordingTagIndex();

        assertEquals(10_001L, PixelsUpdateExecutor.execute(update, primaryIndex(), index));
        assertEquals(1, index.batchCount);
        assertEquals(10_001, index.entries.get(0).primaryKeys.size());
    }

    @Test
    void keepsTheMillionKeyRangeOnTheExactUpdatePath()
    {
        Domain millionKeys = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, 999_999L, true)), false);
        Domain overLimit = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, 1_000_000L, true)), false);

        assertTrue(PixelsUpdateExecutor.isFinitePrimaryKeyDomain(millionKeys));
        assertFalse(PixelsUpdateExecutor.isFinitePrimaryKeyDomain(overLimit));
    }

    @Test
    void rejectsACompositePrimaryKeyProductAboveTheExactLimit()
    {
        PixelsColumnHandle first = column("first", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle second = column("second", BigintType.BIGINT, LONG, 1);
        Domain firstKeys = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, 999L, true)), false);
        Domain secondKeys = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, 1_000L, true)), false);

        assertFalse(PixelsUpdateExecutor.isFinitePrimaryKeyConstraint(
                List.of(first, second), Map.of(first, firstKeys, second, secondKeys)));
    }

    @Test
    void onlyMarksFinitePrimaryKeyPredicatesAsConnectorUpdatePushdown()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        Domain key = Domain.singleValue(BigintType.BIGINT, 7L);

        assertTrue(PixelsMetadata.isExactUpdateEligible(List.of(id), Map.of(id, key)));
        assertFalse(PixelsMetadata.isExactUpdateEligible(List.of(id), Map.of(id,
                Domain.all(BigintType.BIGINT))));
    }

    @Test
    void encodesTheOneMillionKeyTargetWithoutFallingBack()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        Domain millionKeys = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, 0L, true, 999_999L, true)), false);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(id, millionKeys))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        CountingTagIndex index = new CountingTagIndex();

        assertEquals(1_000_000L, PixelsUpdateExecutor.execute(update, primaryIndex(), index));
        assertEquals(1, index.batchCount);
        assertEquals(1_000_000L, index.primaryKeyCount);
    }

    @Test
    void handlesTheMaximumLongPrimaryKeyWithoutOverflowingTheRangeLoop()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        Domain maximumKey = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, Long.MAX_VALUE, true, Long.MAX_VALUE, true)), false);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(id, maximumKey))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        CountingTagIndex index = new CountingTagIndex();

        assertEquals(1L, PixelsUpdateExecutor.execute(update, primaryIndex(), index));
        assertEquals(1L, index.primaryKeyCount);

        Domain minimumToNegative = Domain.create(ValueSet.ofRanges(
                Range.range(BigintType.BIGINT, Long.MIN_VALUE, true, -1L, true)), false);
        PixelsUpdateTableHandle overLimit = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.withColumnDomains(Map.of(id, minimumToNegative))),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));
        assertFalse(PixelsUpdateExecutor.isFinitePrimaryKeyDomain(minimumToNegative));
        assertThrows(IllegalArgumentException.class,
                () -> PixelsUpdateExecutor.execute(overLimit, primaryIndex(), new CountingTagIndex()));
    }

    @Test
    void batchesMultipleAssignmentsFromOneStatement()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle first = column("first", VarcharType.VARCHAR, STRING, 1);
        PixelsColumnHandle second = column("second", VarcharType.VARCHAR, STRING, 2);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, first, second), TupleDomain.withColumnDomains(Map.of(
                        id, Domain.singleValue(BigintType.BIGINT, 7L)))),
                Map.of(first, new Constant("A", VarcharType.VARCHAR),
                        second, new Constant("B", VarcharType.VARCHAR)));
        RecordingTagIndex index = new RecordingTagIndex();

        assertEquals(1L, PixelsUpdateExecutor.execute(update, primaryIndex(), index));
        assertEquals(1, index.batchCount);
        assertEquals(2, index.entries.size());
    }

    @Test
    void rejectsAnUpdateThatCouldMatchMoreThanOnePrimaryKey()
    {
        PixelsColumnHandle id = column("id", BigintType.BIGINT, LONG, 0);
        PixelsColumnHandle label = column("label", VarcharType.VARCHAR, STRING, 1);
        PixelsUpdateTableHandle update = new PixelsUpdateTableHandle(
                tableHandle(List.of(id, label), TupleDomain.all()),
                Map.of(label, new Constant("vip", VarcharType.VARCHAR)));

        assertThrows(IllegalArgumentException.class,
                () -> PixelsUpdateExecutor.execute(update, primaryIndex(), new RecordingTagIndex()));
    }

    private static SinglePointIndex primaryIndex()
    {
        KeyColumns keyColumns = new KeyColumns();
        keyColumns.addKeyColumnIds(0);
        SinglePointIndex index = new SinglePointIndex();
        index.setId(11L);
        index.setTableId(22L);
        index.setKeyColumns(keyColumns);
        return index;
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

    private static final class RecordingTagIndex implements PixelsUpdateExecutor.TagIndex
    {
        private final List<Entry> entries = new ArrayList<>();
        private int batchCount;

        @Override
        public void append(List<PixelsUpdateExecutor.TagUpdate> updates)
        {
            batchCount++;
            updates.forEach(update -> entries.add(new Entry(update.tag(), update.primaryKeys())));
        }

        private record Entry(byte[] tag, List<byte[]> primaryKeys) {}
    }

    private static final class CountingTagIndex implements PixelsUpdateExecutor.TagIndex
    {
        private int batchCount;
        private long primaryKeyCount;

        @Override
        public void append(List<PixelsUpdateExecutor.TagUpdate> updates)
        {
            batchCount++;
            primaryKeyCount = updates.stream()
                    .mapToLong(update -> update.primaryKeys().size())
                    .sum();
        }
    }
}
