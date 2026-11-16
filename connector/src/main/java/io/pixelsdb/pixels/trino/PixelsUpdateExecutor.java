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
    /**
     * Keep exact-key updates on the index path up to the target batch size.
     * The old 10k guard forced larger predicates back to a full merge scan.
     */
    static final int MAX_INDEX_UPDATE_KEYS = 1_000_000;

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
        if (primaryKeyColumns.size() == 1)
        {
            List<byte[]> optimized = encodeSingleLongPrimaryKeyDomain(
                    primaryKeyColumns.get(0), domains.get(primaryKeyColumns.get(0)));
            if (optimized != null)
            {
                return optimized;
            }
        }
        long keyCount = 1;
        List<List<byte[]>> encodedValues = new ArrayList<>(primaryKeyColumns.size());
        for (PixelsColumnHandle column : primaryKeyColumns) {
            Domain domain = domains.get(column);
            List<Object> values = valuesForDomain(domain);
            if (domain == null || domain.isNullAllowed()) {
                throw new IllegalArgumentException(
                        "Pixels UPDATE requires finite non-null primary-key values for " + column.getColumnName());
            }
            if (values.size() > MAX_INDEX_UPDATE_KEYS / keyCount) {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large");
            }
            keyCount *= values.size();

            TypeDescription type = TypeDescription.fromString(column.getColumnType().getDisplayName());
            List<byte[]> encodedColumnValues = new ArrayList<>(values.size());
            for (Object value : values) {
                encodedColumnValues.add(type.convertSqlStringToByte(toSqlString(value)));
            }
            encodedValues.add(encodedColumnValues);
        }
        List<byte[]> result = new ArrayList<>((int) keyCount);
        appendPrimaryKeys(encodedValues, 0, new byte[encodedValues.size()][], result);
        return result;
    }

    private static List<byte[]> encodeSingleLongPrimaryKeyDomain(
            PixelsColumnHandle column, Domain domain)
    {
        if (column.getTypeCategory() != TypeDescription.Category.LONG ||
                domain == null || domain.isNullAllowed() ||
                domain.getType().getJavaType() != long.class)
        {
            return null;
        }
        if (domain.getValues().isDiscreteSet())
        {
            List<Object> values = domain.getValues().getDiscreteSet();
            if (values.size() > MAX_INDEX_UPDATE_KEYS)
            {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large");
            }
            List<byte[]> result = new ArrayList<>(values.size());
            for (Object value : values)
            {
                if (!(value instanceof Number number))
                {
                    return null;
                }
                result.add(encodeLong(number.longValue()));
            }
            return result;
        }

        long keyCount = 0;
        for (io.trino.spi.predicate.Range range : domain.getValues().getRanges().getOrderedRanges())
        {
            if (range.isLowUnbounded() || range.isHighUnbounded() ||
                    !range.isLowInclusive() || !range.isHighInclusive())
            {
                return null;
            }
            long low = ((Number) range.getLowBoundedValue()).longValue();
            long high = ((Number) range.getHighBoundedValue()).longValue();
            long remaining = MAX_INDEX_UPDATE_KEYS - keyCount;
            if (rangeExceedsLimit(low, high, remaining))
            {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large");
            }
            keyCount += rangeLength(low, high);
        }
        List<byte[]> result = new ArrayList<>((int) keyCount);
        for (io.trino.spi.predicate.Range range : domain.getValues().getRanges().getOrderedRanges())
        {
            long low = ((Number) range.getLowBoundedValue()).longValue();
            long high = ((Number) range.getHighBoundedValue()).longValue();
            for (long value = low;; value++)
            {
                result.add(encodeLong(value));
                if (value == high)
                {
                    break;
                }
            }
        }
        return result;
    }

    private static byte[] encodeLong(long value)
    {
        return new byte[] {
                (byte) (value >>> 56),
                (byte) (value >>> 48),
                (byte) (value >>> 40),
                (byte) (value >>> 32),
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    private static void appendPrimaryKeys(
            List<List<byte[]>> encodedValues,
            int columnIndex,
            byte[][] parts,
            List<byte[]> result)
    {
        if (columnIndex == encodedValues.size()) {
            result.add(concat(parts));
            return;
        }
        for (byte[] value : encodedValues.get(columnIndex)) {
            parts[columnIndex] = value;
            appendPrimaryKeys(encodedValues, columnIndex + 1, parts, result);
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
            long remaining = MAX_INDEX_UPDATE_KEYS - values.size();
            if (rangeExceedsLimit(low, high, remaining)) {
                throw new IllegalArgumentException("Pixels UPDATE primary-key batch is too large");
            }
            for (long value = low;; value++) {
                values.add(value);
                if (value == high) {
                    break;
                }
            }
        }
        return values;
    }

    static boolean isFinitePrimaryKeyDomain(Domain domain)
    {
        return finitePrimaryKeyCardinality(domain) > 0;
    }

    /**
     * Returns whether the Cartesian product of all primary-key domains stays
     * on the exact UPDATE path.  Checking each column independently is not
     * sufficient for composite keys: 1,000 values in each of two columns
     * already produces one million encoded keys.
     */
    static boolean isFinitePrimaryKeyConstraint(
            List<PixelsColumnHandle> primaryKeyColumns,
            Map<PixelsColumnHandle, Domain> domains)
    {
        requireNonNull(primaryKeyColumns, "primaryKeyColumns is null");
        requireNonNull(domains, "domains is null");
        if (primaryKeyColumns.isEmpty() || domains.size() != primaryKeyColumns.size()) {
            return false;
        }
        long keyCount = 1;
        for (PixelsColumnHandle column : primaryKeyColumns) {
            long cardinality = finitePrimaryKeyCardinality(domains.get(column));
            if (cardinality <= 0 || keyCount > MAX_INDEX_UPDATE_KEYS / cardinality) {
                return false;
            }
            keyCount *= cardinality;
        }
        return true;
    }

    static long finitePrimaryKeyCardinality(Domain domain)
    {
        if (domain == null || domain.isNullAllowed()) {
            return -1;
        }
        if (domain.getValues().isDiscreteSet()) {
            long count = domain.getValues().getDiscreteSet().size();
            return count > 0 && count <= MAX_INDEX_UPDATE_KEYS ? count : -1;
        }
        if (domain.getType().getJavaType() != long.class) {
            return -1;
        }
        long count = 0;
        for (io.trino.spi.predicate.Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isLowUnbounded() || range.isHighUnbounded() ||
                    !range.isLowInclusive() || !range.isHighInclusive()) {
                return -1;
            }
            long low = ((Number) range.getLowBoundedValue()).longValue();
            long high = ((Number) range.getHighBoundedValue()).longValue();
            long remaining = MAX_INDEX_UPDATE_KEYS - count;
            if (rangeExceedsLimit(low, high, remaining)) {
                return -1;
            }
            count += rangeLength(low, high);
        }
        return count;
    }

    private static boolean rangeExceedsLimit(long low, long high, long limit)
    {
        if (high < low || limit <= 0) {
            return true;
        }
        if (low < 0 && high >= 0) {
            // Avoid overflowing high - low for ranges crossing zero.  Once
            // the non-negative side is at least the limit, the range is
            // already too large; otherwise compare the negative magnitude
            // against the remaining budget.
            if (high >= limit) {
                return true;
            }
            return low < -(limit - 1 - high);
        }
        return high - low >= limit;
    }

    private static long rangeLength(long low, long high)
    {
        if (low < 0 && high >= 0) {
            return high + 1 - low;
        }
        return high - low + 1;
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
