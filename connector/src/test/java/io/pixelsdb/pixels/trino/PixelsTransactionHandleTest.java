/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import io.pixelsdb.pixels.common.turbo.ExecutorType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PixelsTransactionHandleTest
{
    @Test
    void tracksWhetherMetadataEnteredAWritePath()
    {
        PixelsTransactionHandle handle = new PixelsTransactionHandle(
                11L, 22L, false, ExecutorType.MPP);

        assertFalse(handle.hasWriteOperation());

        handle.markWriteOperation();

        assertTrue(handle.hasWriteOperation());
    }
}
