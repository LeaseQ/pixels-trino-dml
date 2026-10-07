/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slices;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class PixelsTagPointLookupTest
{
    @Test
    void extractsConfiguredTagEqualityAndEncodesItForPixelsTagIndex()
    {
        PixelsColumnHandle tag = column("tag");
        TupleDomain<PixelsColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                tag, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("ready"))));

        byte[] encoded = PixelsTagPointLookup.extractTagValue(constraint, tag)
                .orElseThrow();

        assertArrayEquals(TypeDescription.fromString(tag.getColumnType().getDisplayName())
                .convertSqlStringToByte("ready"), encoded);
    }

    @Test
    void groupsResolvedLocationsByFileAndRowGroup()
    {
        List<IndexProto.RowLocation> locations = List.of(
                location(10, 2, 4),
                location(10, 2, 7),
                location(10, 4, 1),
                location(11, 0, 3));

        assertEquals(Map.of(10L, Map.of(2, List.of(4, 7), 4, List.of(1)),
                11L, Map.of(0, List.of(3))),
                PixelsTagPointLookup.groupLocations(locations));
    }

    private static PixelsColumnHandle column(String name)
    {
        return new PixelsColumnHandle("pixels", "test", "orders", name, name,
                VarcharType.VARCHAR, TypeDescription.Category.STRING, "", 0);
    }

    private static IndexProto.RowLocation location(long fileId, int rgId, int offset)
    {
        return IndexProto.RowLocation.newBuilder()
                .setFileId(fileId)
                .setRgId(rgId)
                .setRgRowOffset(offset)
                .build();
    }
}
