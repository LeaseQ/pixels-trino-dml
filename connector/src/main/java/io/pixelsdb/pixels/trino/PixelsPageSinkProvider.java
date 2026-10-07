/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.trino;

import com.google.inject.Inject;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.trino.exception.PixelsErrorCode;
import io.pixelsdb.pixels.trino.impl.PixelsMetadataProxy;
import io.pixelsdb.pixels.trino.impl.PixelsTrinoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_METADATA_ERROR;
import static io.pixelsdb.pixels.trino.exception.PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR;
import static java.util.Objects.requireNonNull;

/** Creates direct Pixels writers for Trino INSERT workers. */
public final class PixelsPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PixelsMetadataProxy metadataProxy;
    private final PixelsTrinoConfig config;

    @Inject
    public PixelsPageSinkProvider(PixelsMetadataProxy metadataProxy, PixelsTrinoConfig config)
    {
        this.metadataProxy = requireNonNull(metadataProxy, "metadataProxy is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                "Pixels INSERT only supports writing to an existing table");
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        if (!(mergeTableHandle instanceof PixelsMergeTableHandle pixelsMergeTableHandle))
        {
            throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                    "Pixels UPDATE received an invalid merge handle");
        }
        try
        {
            return new PixelsMergeSink(pixelsMergeTableHandle);
        }
        catch (Exception e)
        {
            throw new TrinoException(PixelsErrorCode.PIXELS_INVERTED_INDEX_ERROR,
                    "failed to create Pixels UPDATE merge sink", e);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        if (!(transactionHandle instanceof PixelsTransactionHandle pixelsTransactionHandle)) {
            throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                    "Pixels INSERT received an invalid transaction handle");
        }
        if (!(insertTableHandle instanceof PixelsInsertTableHandle pixelsInsertTableHandle)) {
            throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                    "Pixels INSERT received an invalid insert handle");
        }

        PixelsTableHandle table = pixelsInsertTableHandle.getTableHandle();
        List<PixelsColumnHandle> columns = pixelsInsertTableHandle.getColumns();
        if (columns.isEmpty() || table.getStoragePaths().isEmpty()) {
            throw new TrinoException(PIXELS_SQL_EXECUTE_ERROR,
                    "Pixels INSERT has no target columns or storage path");
        }

        TargetPath targetPath = selectTargetPath(table, pageSinkId.getId());
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                columns.stream().map(PixelsColumnHandle::getColumnName).toList(),
                columns.stream().map(column -> column.getColumnType().getDisplayName()).toList());
        ConfigFactory pixelsConfig = config.getConfigFactory();
        String fileName = PixelsFileNameUtils.buildSingleFileName(
                NetUtils.getLocalHostName() + "_" + pageSinkId.getId());
        String filePath = targetPath.uri().endsWith("/")
                ? targetPath.uri() + fileName
                : targetPath.uri() + "/" + fileName;
        MetadataService metadataService = metadataProxy.getMetadataService();
        long fileId = -1;
        PixelsWriter writer = null;
        try {
            io.pixelsdb.pixels.common.metadata.domain.Table metadataTable = metadataService.getTable(
                    table.getSchemaName(), table.getTableName());
            io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex primaryIndex =
                    metadataService.getPrimaryIndex(metadataTable.getId());
            List<Integer> keyOrdinals = primaryIndex == null ? List.of() :
                    resolvePrimaryKeyOrdinals(metadataService, table, columns, primaryIndex);
            File metadataFile = new File();
            metadataFile.setName(fileName);
            metadataFile.setType(File.Type.TEMPORARY_INGEST);
            metadataFile.setNumRowGroup(1);
            metadataFile.setPathId(targetPath.pathId());
            metadataService.addFiles(List.of(metadataFile));
            fileId = metadataService.getFileId(filePath);
            long registeredFileId = fileId;
            Storage storage = StorageFactory.Instance().getStorage(targetPath.uri());
            writer = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setHasHiddenColumn(true)
                    .setPixelStride(Integer.parseInt(pixelsConfig.getProperty("pixel.stride")))
                    .setRowGroupSize(Integer.parseInt(pixelsConfig.getProperty("row.group.size")))
                    .setStorage(storage)
                    .setPath(filePath)
                    .setBlockSize(Long.parseLong(pixelsConfig.getProperty("block.size")))
                    .setReplication(Short.parseShort(pixelsConfig.getProperty("block.replication")))
                    .setBlockPadding(Boolean.parseBoolean(pixelsConfig.getProperty("block.padding")))
                    .setEncodingLevel(EncodingLevel.EL2)
                    .setNullsPadding(false)
                    .setCompressionBlockSize(Integer.parseInt(pixelsConfig.getProperty("compression.block.size")))
                    .build();
            PixelsPageSink.InsertIndexWriter indexWriter = null;
            if (primaryIndex != null) {
                String configuredMode = pixelsConfig.getProperty("dml.insert.index.service");
                IndexServiceProvider.ServiceMode serviceMode = configuredMode == null || configuredMode.isBlank()
                        ? IndexServiceProvider.ServiceMode.rpc
                        : IndexServiceProvider.ServiceMode.from(configuredMode);
                indexWriter = new PixelsInsertIndexWriter(
                        IndexServiceProvider.getService(serviceMode), metadataTable.getId(),
                        primaryIndex.getId(), fileId, pixelsTransactionHandle.getTimestamp(),
                        columns, keyOrdinals);
            }
            return new PixelsPageSink(
                    writer,
                    schema.createRowBatchWithHiddenColumn(PixelsTrinoConfig.getBatchSize()),
                    columns,
                    pixelsTransactionHandle.getTimestamp(),
                    targetPath.pathId(),
                    fileName,
                    indexWriter,
                    () -> {
                        try {
                            metadataService.deleteFiles(List.of(registeredFileId));
                        }
                        catch (MetadataException e) {
                            throw new TrinoException(PIXELS_METADATA_ERROR,
                                    "failed to remove aborted Pixels INSERT file", e);
                        }
                    });
        }
        catch (IOException | MetadataException | RuntimeException e) {
            if (writer != null) {
                try {
                    writer.abort();
                }
                catch (IOException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            if (fileId > 0) {
                try {
                    metadataService.deleteFiles(List.of(fileId));
                }
                catch (MetadataException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw new TrinoException(PixelsErrorCode.PIXELS_WRITER_OPEN_ERROR,
                    "failed to create Pixels INSERT writer for " + filePath, e);
        }
    }

    private static List<Integer> resolvePrimaryKeyOrdinals(
            MetadataService metadataService,
            PixelsTableHandle table,
            List<PixelsColumnHandle> insertColumns,
            io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex primaryIndex)
            throws MetadataException
    {
        List<Column> tableColumns = metadataService.getColumns(
                table.getSchemaName(), table.getTableName(), false);
        List<Integer> ordinals = new ArrayList<>();
        for (Integer columnId : primaryIndex.getKeyColumns().getKeyColumnIds()) {
            Column keyColumn = tableColumns.stream()
                    .filter(column -> column.getId() == columnId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Pixels INSERT primary-key column is absent from metadata: " + columnId));
            int ordinal = -1;
            for (int i = 0; i < insertColumns.size(); i++) {
                if (insertColumns.get(i).getColumnName().equalsIgnoreCase(keyColumn.getName())) {
                    ordinal = i;
                    break;
                }
            }
            if (ordinal < 0) {
                throw new IllegalArgumentException(
                        "Pixels INSERT must provide primary-key column " + keyColumn.getName());
            }
            ordinals.add(ordinal);
        }
        return ordinals;
    }

    private TargetPath selectTargetPath(PixelsTableHandle table, long pageSinkId)
    {
        try {
            return selectWritableOrderedPath(
                    metadataProxy.getDataLayouts(table.getSchemaName(), table.getTableName()), pageSinkId);
        }
        catch (MetadataException e) {
            throw new TrinoException(PIXELS_METADATA_ERROR, "failed to resolve Pixels INSERT target path", e);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(PIXELS_METADATA_ERROR,
                    "Pixels INSERT has no writable ordered path for table " +
                            table.getSchemaName() + "." + table.getTableName(), e);
        }
    }

    static TargetPath selectWritableOrderedPath(List<Layout> layouts, long pageSinkId)
    {
        List<TargetPath> writableOrderedPaths = new ArrayList<>();
        for (Layout layout : layouts) {
            if (!layout.isWritable()) {
                continue;
            }
            for (Path path : layout.getOrderedPaths()) {
                writableOrderedPaths.add(new TargetPath(path.getId(), path.getUri()));
            }
        }
        if (writableOrderedPaths.isEmpty()) {
            throw new IllegalArgumentException("no writable ordered path");
        }
        return writableOrderedPaths.get(Math.floorMod(pageSinkId, writableOrderedPaths.size()));
    }

    static record TargetPath(long pathId, String uri) {}
}
