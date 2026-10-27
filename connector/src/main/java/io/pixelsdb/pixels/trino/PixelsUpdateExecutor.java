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
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.pixelsdb.pixels.core.utils.PrimaryKeyBytes.concat;
import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_DATA_TYPE_ERROR;
import static java.util.Objects.requireNonNull;

/** Executes the deliberately narrow, index-only Pixels UPDATE contract. */
final class PixelsUpdateExecutor
{
    private PixelsUpdateExecutor() {}

    interface TagIndex
    {
        void append(byte[] tag, List<byte[]> primaryKeys);
    }

    static long execute(
            PixelsUpdateTableHandle update,
            SinglePointIndex primaryIndex,
            TagIndex tagIndex)
    {
        requireNonNull(update, "update is null");
        requireNonNull(primaryIndex, "primaryIndex is null");
        requireNonNull(tagIndex, "tagIndex is null");

        TupleDomain<PixelsColumnHandle> constraint = update.getTableHandle().getConstraint();
        if (constraint.isNone()) {
            return 0L;
        }
        Map<PixelsColumnHandle, NullableValue> fixedValues = TupleDomain.extractFixedValues(constraint)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Pixels UPDATE requires equality predicates for every primary-key column"));
        byte[] primaryKey = encodePrimaryKey(update, primaryIndex, fixedValues);

        for (Map.Entry<PixelsColumnHandle, Constant> assignment : update.getAssignments().entrySet()) {
            byte[] tag = encodeConstant(assignment.getKey(), assignment.getValue());
            tagIndex.append(tag, List.of(primaryKey));
        }
        return 1L;
    }

    private static byte[] encodePrimaryKey(
            PixelsUpdateTableHandle update,
            SinglePointIndex primaryIndex,
            Map<PixelsColumnHandle, NullableValue> fixedValues)
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

        List<byte[]> encodedParts = new ArrayList<>();
        for (int keyIndex = 0; keyIndex < keyColumns.getKeyColumnIds().size(); keyIndex++) {
            Integer keyColumnId = keyColumns.getKeyColumnIds().get(keyIndex);
            PixelsColumnHandle column = primaryKeyColumns.isEmpty()
                    ? columnsByOrdinal.getOrDefault(keyColumnId, columnsByOrdinal.get(keyColumnId - 1))
                    : primaryKeyColumns.get(keyIndex);
            if (column == null) {
                throw new IllegalArgumentException("primary-key column is not present in the table handle: " + keyColumnId);
            }
            NullableValue value = fixedValues.get(column);
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException(
                        "Pixels UPDATE requires a non-null equality predicate for primary-key column " +
                                column.getColumnName());
            }
            encodedParts.add(TypeDescription.fromString(column.getColumnType().getDisplayName())
                    .convertSqlStringToByte(toSqlString(value.getValue())));
        }
        return concat(encodedParts.toArray(byte[][]::new));
    }

    private static byte[] encodeConstant(PixelsColumnHandle column, Constant constant)
    {
        if (!column.getColumnType().equals(constant.getType())) {
            throw new TrinoException(PIXELS_DATA_TYPE_ERROR,
                    "UPDATE value type does not match column " + column.getColumnName());
        }
        return TypeDescription.fromString(column.getColumnType().getDisplayName())
                .convertSqlStringToByte(toSqlString(constant.getValue()));
    }

    private static String toSqlString(Object value)
    {
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }
}
