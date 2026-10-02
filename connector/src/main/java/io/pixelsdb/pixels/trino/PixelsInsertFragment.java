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
    private static final String PREFIX = "pixels-insert-v1";
    private static final String SEPARATOR = "\t";
    private static final Pattern FORMAT = Pattern.compile("^" + PREFIX + "\\t[0-9]+\\t[0-9]+\\t.+$");

    private PixelsInsertFragment() {}

    static Slice encode(long pathId, String fileName, int numRowGroups)
    {
        requireNonNull(fileName, "fileName is null");
        if (fileName.contains(SEPARATOR) || fileName.contains("\n")) {
            throw new IllegalArgumentException("file name contains an unsupported separator");
        }
        return Slices.utf8Slice(PREFIX + SEPARATOR + pathId + SEPARATOR + numRowGroups + SEPARATOR + fileName);
    }

    static Decoded decode(Slice fragment)
    {
        String value = requireNonNull(fragment, "fragment is null").toStringUtf8();
        if (!FORMAT.matcher(value).matches()) {
            throw new IllegalArgumentException("invalid Pixels INSERT fragment");
        }
        String[] parts = value.split(SEPARATOR, 4);
        return new Decoded(Long.parseLong(parts[1]), Integer.parseInt(parts[2]), parts[3]);
    }

    record Decoded(long pathId, int numRowGroups, String fileName) {}
}
