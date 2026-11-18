/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/** The coordinator/worker contract for a completed INSERT file. */
final class PixelsInsertFragment
{
    private static final String PREFIX = "pixels-insert-v2";
    private static final String LEGACY_PREFIX = "pixels-insert-v1";
    private static final String SEPARATOR = "\t";
    private static final Pattern FORMAT = Pattern.compile("^(?:" + PREFIX + "|" + LEGACY_PREFIX + ")\\t[0-9]+\\t[0-9]+(?:\\t[0-9]+\\t[0-9]+)?\\t.+$");

    private PixelsInsertFragment() {}

    static Slice encode(long pathId, String fileName, int numRowGroups, long minRowId, long maxRowId)
    {
        requireNonNull(fileName, "fileName is null");
        if (minRowId < 0 || maxRowId < minRowId) {
            throw new IllegalArgumentException("invalid row-id range");
        }
        if (fileName.contains(SEPARATOR) || fileName.contains("\n")) {
            throw new IllegalArgumentException("file name contains an unsupported separator");
        }
        return Slices.utf8Slice(PREFIX + SEPARATOR + pathId + SEPARATOR + numRowGroups + SEPARATOR +
                minRowId + SEPARATOR + maxRowId + SEPARATOR + fileName);
    }

    static Decoded decode(Slice fragment)
    {
        String value = requireNonNull(fragment, "fragment is null").toStringUtf8();
        if (!FORMAT.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid Pixels INSERT fragment");
        }
        String[] parts = value.split(SEPARATOR, 6);
        if (LEGACY_PREFIX.equals(parts[0])) {
            return new Decoded(Long.parseLong(parts[1]), Integer.parseInt(parts[2]), 0L, 0L, parts[3]);
        }
        return new Decoded(Long.parseLong(parts[1]), Integer.parseInt(parts[2]),
                Long.parseLong(parts[3]), Long.parseLong(parts[4]), parts[5]);
    }

    record Decoded(long pathId, int numRowGroups, long minRowId, long maxRowId, String fileName) {}
}
