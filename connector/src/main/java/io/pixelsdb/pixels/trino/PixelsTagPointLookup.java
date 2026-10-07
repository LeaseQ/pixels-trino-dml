/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** Helpers for turning an exact tag predicate into index-backed scan inputs. */
final class PixelsTagPointLookup
{
    private PixelsTagPointLookup() {}

    static Optional<byte[]> extractTagValue(
            TupleDomain<PixelsColumnHandle> constraint, PixelsColumnHandle tagColumn)
    {
        requireNonNull(constraint, "constraint is null");
        requireNonNull(tagColumn, "tagColumn is null");
        if (constraint.isNone() || constraint.getDomains().isEmpty()) {
            return Optional.empty();
        }
        Domain domain = constraint.getDomains().get().get(tagColumn);
        if (domain == null || !domain.isSingleValue() || domain.isNullAllowed()) {
            return Optional.empty();
        }
        Object value = domain.getSingleValue();
        String sqlValue = value instanceof Slice slice ? slice.toStringUtf8() : String.valueOf(value);
        return Optional.of(TypeDescription.fromString(tagColumn.getColumnType().getDisplayName())
                .convertSqlStringToByte(sqlValue));
    }

    static Map<Long, Map<Integer, List<Integer>>> groupLocations(
            List<IndexProto.RowLocation> locations)
    {
        requireNonNull(locations, "locations is null");
        Map<Long, Map<Integer, List<Integer>>> grouped = new LinkedHashMap<>();
        for (IndexProto.RowLocation location : locations) {
            grouped.computeIfAbsent(location.getFileId(), ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(location.getRgId(), ignored -> new ArrayList<>())
                    .add(location.getRgRowOffset());
        }
        return grouped;
    }
}
