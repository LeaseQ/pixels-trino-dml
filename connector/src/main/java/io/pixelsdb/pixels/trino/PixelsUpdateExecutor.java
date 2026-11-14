/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.core.TypeDescription;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.pixelsdb.pixels.core.utils.PrimaryKeyBytes.concat;
import static java.util.Objects.requireNonNull;

/** Executes the deliberately narrow, index-only Pixels UPDATE contract. */
final class PixelsUpdateExecutor
{
    private static final int MAX_INDEX_UPDATE_KEYS = 10_000;

    private PixelsUpdateExecutor() {}

    interface TagIndex
    {
        void append(List<TagUpdate> updates);
    }

    record TagUpdate(byte[] tag, List<byte[]> primaryKeys) {}

    record Result(long updatedRows, long encodingNanos) {}

    static long execute(
            PixelsUpdateTableHandle update,
            SinglePointIndex primaryIndex,
            TagIndex tagIndex)
    {
        requireNonNull(update, "update is null");
        requireNonNull(primaryIndex, "primaryIndex is null");
        return executeWithMetrics(update, resolvePrimaryKeyColumns(update, primaryIndex), tagIndex).updatedRows();
    }

    static Result executeWithMetrics(
            PixelsUpdateTableHandle update,
            List<PixelsColumnHandle> primaryKeyColumns,
            TagIndex tagIndex)
    {
        requireNonNull(update, "update is null");
        requireNonNull(primaryKeyColumns, "primaryKeyColumns is null");
        requireNonNull(tagIndex, "tagIndex is null");

        TupleDomain<PixelsColumnHandle> constraint = update.getTableHandle().getConstraint();
        if (constraint.isNone()) {
            return new Result(0L, 0L);
        }
        Map<PixelsColumnHandle, Domain> domains = constraint.getDomains()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Pixels UPDATE requires primary-key predicates"));
        if (domains.keySet().stream().anyMatch(column -> !primaryKeyColumns.contains(column))) {
            throw new IllegalArgumentException(
                    "Pixels UPDATE index path only supports predicates on primary-key columns");
        }
        long encodingStart = System.nanoTime();
        List<byte[]> primaryKeys = encodePrimaryKeys(primaryKeyColumns, domains);

        List<TagUpdate> tagUpdates = new ArrayList<>(update.getAssignments().size());
        for (PixelsUpdateTableHandle.PixelsUpdateAssignment assignment : update.getAssignments()) {
            String columnName = assignment.getColumn().getColumnName();
            PixelsColumnHandle column = update.getTableHandle().getColumns().stream()
                    .filter(item -> item.getColumnName().equalsIgnoreCase(columnName))
                    .findFirst()
                    .orElseThrow(() -> new TrinoException(
                            io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_DATA_TYPE_ERROR,
                            "UPDATE column is not present in the table handle: " + columnName));
            byte[] tag = encodeAssignment(column, assignment.getValue());
            tagUpdates.add(new TagUpdate(tag, primaryKeys));
        }
        long encodingNanos = System.nanoTime() - encodingStart;
        tagIndex.append(tagUpdates);
        return new Result(primaryKeys.size(), encodingNanos);
    }

    private static List<PixelsColumnHandle> resolvePrimaryKeyColumns(
            PixelsUpdateTableHandle update,
            SinglePointIndex primaryIndex)
    {
        KeyColumns keyColumns = primaryIndex.getKeyColumns();
        if (keyColumns == null || keyColumns.getKeyColumnIds().isEmpty()) {
            throw new IllegalArgumentException("Pixels UPDATE requires a primary index");
        }
        List<PixelsColumnHandle> primaryKeyColumns = update.getPrimaryKeyColumns();
        Map<Integer, PixelsColumnHandle> columnsByOrdinal = new HashMap<>();
        for (PixelsColumnHandle column : update.getTableHandle().getColumns()) {
            columnsByOrdinal.put(column.getLogicalOrdinal(), column);
        }

        if (!primaryKeyColumns.isEmpty()) {
            return primaryKeyColumns;
        }
        primaryKeyColumns = new ArrayList<>();
        for (int keyIndex = 0; keyIndex < keyColumns.getKeyColumnIds().size(); keyIndex++) {
            Integer keyColumnId = keyColumns.getKeyColumnIds().get(keyIndex);
            PixelsColumnHandle column = columnsByOrdinal.getOrDefault(keyColumnId, columnsByOrdinal.get(keyColumnId - 1));
            if (column == null) {
                throw new IllegalArgumentException("primary-key column is not present in the table handle: " + keyColumnId);
            }
            primaryKeyColumns.add(column);
        }
        return primaryKeyColumns;
    }

    private static List<byte[]> encodePrimaryKeys(
            List<PixelsColumnHandle> primaryKeyColumns,
            Map<PixelsColumnHandle, Domain> domains)
    {
        long keyCount = 1;
        for (PixelsColumnHandle column : primaryKeyColumns) {
            Domain domain = domains.get(column);
            List<Object> values = valuesForDomain(domain);
            if (domain == null || domain.isNullAllowed()) {
                throw new IllegalArgumentException(
                        "Pixels UPDATE requires finite non-null primary-key values for " + column.getColumnName());
            }
            keyCount *= values.size();
            if (keyCount > MAX_INDEX_UPDATE_KEYS) {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large: " + keyCount);
            }
        }
        List<byte[]> result = new ArrayList<>((int) keyCount);
        appendPrimaryKeys(primaryKeyColumns, domains, 0, new ArrayList<>(), result);
        return result;
    }

    private static void appendPrimaryKeys(
            List<PixelsColumnHandle> columns,
            Map<PixelsColumnHandle, Domain> domains,
            int columnIndex,
            List<byte[]> parts,
            List<byte[]> result)
    {
        if (columnIndex == columns.size()) {
            result.add(concat(parts.toArray(byte[][]::new)));
            return;
        }
        PixelsColumnHandle column = columns.get(columnIndex);
        for (Object value : valuesForDomain(domains.get(column))) {
            List<byte[]> next = new ArrayList<>(parts);
            next.add(TypeDescription.fromString(column.getColumnType().getDisplayName())
                    .convertSqlStringToByte(toSqlString(value)));
            appendPrimaryKeys(columns, domains, columnIndex + 1, next, result);
        }
    }

    private static List<Object> valuesForDomain(Domain domain)
    {
        if (domain == null || domain.isNullAllowed()) {
            throw new IllegalArgumentException("Pixels UPDATE requires finite non-null primary-key values");
        }
        if (domain.getValues().isDiscreteSet()) {
            return domain.getValues().getDiscreteSet();
        }
        if (domain.getType().getJavaType() != long.class) {
            throw new IllegalArgumentException("Pixels UPDATE only expands finite integer ranges");
        }
        List<Object> values = new ArrayList<>();
        for (io.trino.spi.predicate.Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isLowUnbounded() || range.isHighUnbounded() ||
                    !range.isLowInclusive() || !range.isHighInclusive()) {
                throw new IllegalArgumentException("Pixels UPDATE requires bounded inclusive primary-key ranges");
            }
            long low = ((Number) range.getLowBoundedValue()).longValue();
            long high = ((Number) range.getHighBoundedValue()).longValue();
            if (high < low || high - low + 1 > MAX_INDEX_UPDATE_KEYS - values.size()) {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large");
            }
            for (long value = low; value <= high; value++) {
                values.add(value);
            }
        }
        return values;
    }

    private static byte[] encodeAssignment(PixelsColumnHandle column, String value)
    {
        return TypeDescription.fromString(column.getColumnType().getDisplayName())
                .convertSqlStringToByte(value);
    }

    private static String toSqlString(Object value)
    {
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        return String.valueOf(value);
    }
}
