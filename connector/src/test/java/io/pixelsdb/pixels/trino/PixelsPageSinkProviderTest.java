/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Permission;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PixelsPageSinkProviderTest
{
    @Test
    void insertTargetSelectionUsesWritableOrderedPathsOnly()
    {
        Layout readOnly = layout(Permission.READ_ONLY, 1L, "file:///tmp/read-only-ordered");
        Layout writable = layout(Permission.READ_WRITE, 2L, "file:///tmp/writable-ordered");

        assertEquals("file:///tmp/writable-ordered",
                PixelsPageSinkProvider.selectWritableOrderedPath(List.of(readOnly, writable), 0).uri());
    }

    @Test
    void insertTargetSelectionRoundRobinsWritableOrderedPaths()
    {
        Layout writable = new Layout();
        writable.setPermission(Permission.READ_WRITE);
        Path first = path(1L, "file:///tmp/ordered-a");
        Path second = path(2L, "file:///tmp/ordered-b");
        writable.setOrderedPaths(List.of(first, second));

        assertEquals("file:///tmp/ordered-b",
                PixelsPageSinkProvider.selectWritableOrderedPath(List.of(writable), 1).uri());
    }

    private static Layout layout(Permission permission, long pathId, String uri)
    {
        Layout layout = new Layout();
        layout.setPermission(permission);
        layout.setOrderedPaths(List.of(path(pathId, uri)));
        return layout;
    }

    private static Path path(long id, String uri)
    {
        Path path = new Path();
        path.setId(id);
        path.setUri(uri);
        path.setType(Path.Type.ORDERED);
        return path;
    }
}
