// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const rowsource_bridge = @import("rowsource_bridge.zig");
pub const catalog_binding = @import("catalog_binding.zig");
pub const object_snapshot = @import("object_snapshot.zig");
pub const iceberg_metadata = @import("iceberg_metadata.zig");
pub const iceberg_avro = @import("iceberg_avro.zig");
pub const iceberg_inventory = @import("iceberg_inventory.zig");

pub const Format = types.Format;
pub const ColumnChunk = types.ColumnChunk;
pub const RowGroup = types.RowGroup;
pub const FileEntry = types.FileEntry;
pub const DeletedRowGroup = types.DeletedRowGroup;
pub const Inventory = types.Inventory;
pub const ExternalCredentialRef = catalog_binding.CredentialRef;
pub const ExternalSnapshotMode = catalog_binding.SnapshotMode;
pub const ExternalWritePolicy = catalog_binding.WritePolicy;
pub const ExternalTableBinding = catalog_binding.Binding;
pub const ListedExternalObject = object_snapshot.ListedObject;
pub const ObjectStorageSnapshotRequest = object_snapshot.ObjectStorageSnapshotRequest;
pub const IcebergMetadataPlan = iceberg_metadata.Plan;
pub const IcebergSnapshotRef = iceberg_metadata.SnapshotRef;
pub const IcebergManifestContent = iceberg_avro.ManifestContent;
pub const IcebergManifestListEntry = iceberg_avro.ManifestListEntry;
pub const IcebergManifestList = iceberg_avro.ManifestList;
pub const IcebergManifestEntryStatus = iceberg_avro.ManifestEntryStatus;
pub const IcebergDataFileContent = iceberg_avro.DataFileContent;
pub const IcebergDataFileEntry = iceberg_avro.DataFileEntry;
pub const IcebergDataManifest = iceberg_avro.DataManifest;
pub const IcebergInventoryRequest = iceberg_inventory.InventoryRequest;
pub const IcebergDecodedManifest = iceberg_inventory.DecodedManifest;
pub const IcebergSnapshotInventoryRequest = iceberg_inventory.SnapshotInventoryRequest;
pub const InventoryDecodeLimits = codec.DecodeLimits;
pub const freeInventory = types.freeInventory;
pub const encodeInventoryAlloc = codec.encodeAlloc;
pub const decodeInventoryAlloc = codec.decodeAlloc;
pub const decodeInventoryWithLimitsAlloc = codec.decodeAllocWithLimits;
pub const sourceKindForExternalFormat = catalog_binding.sourceKindForFormat;
pub const manifestFormatForExternalFormat = catalog_binding.manifestFormatForExternalFormat;
pub const externalTableBindingFromRuntimeExternalBaseSource = catalog_binding.bindingFromRuntimeExternalBaseSource;
pub const isParquetDataObject = object_snapshot.isParquetDataObject;
pub const planParquetPrefixInventoryAlloc = object_snapshot.planParquetPrefixInventoryAlloc;
pub const planParquetPrefixInventoryFromObjectStorageAlloc = object_snapshot.planParquetPrefixInventoryFromObjectStorageAlloc;
pub const ObjectListingPaginationGuard = object_snapshot.PaginationGuard;
pub const defaultMaxObjectListingPages = object_snapshot.default_max_listing_pages;
pub const defaultMaxObjectListingObjects = object_snapshot.default_max_listing_objects;
pub const parseIcebergMetadataPlanAlloc = iceberg_metadata.parseMetadataPlanAlloc;
pub const parseIcebergManifestListAlloc = iceberg_avro.parseManifestListAlloc;
pub const parseIcebergDataManifestAlloc = iceberg_avro.parseDataManifestAlloc;
pub const planIcebergInventoryFromDataFilesAlloc = iceberg_inventory.planInventoryFromDataFilesAlloc;
pub const planIcebergInventoryFromSnapshotManifestsAlloc = iceberg_inventory.planInventoryFromSnapshotManifestsAlloc;
pub const bindingFromInventory = rowsource_bridge.bindingFromInventory;
pub const rowRefForInventoryRow = rowsource_bridge.rowRefForInventoryRow;
pub const validateBatchAgainstInventory = rowsource_bridge.validateBatchAgainstInventory;

test "serverless external source module compiles" {
    _ = types;
    _ = codec;
    _ = rowsource_bridge;
    _ = catalog_binding;
    _ = object_snapshot;
    _ = iceberg_metadata;
    _ = iceberg_avro;
    _ = iceberg_inventory;
    _ = Format;
    _ = ColumnChunk;
    _ = Inventory;
    _ = ExternalTableBinding;
    _ = ListedExternalObject;
    _ = ObjectStorageSnapshotRequest;
    _ = IcebergMetadataPlan;
    _ = IcebergManifestList;
    _ = IcebergDataManifest;
    _ = encodeInventoryAlloc;
    _ = decodeInventoryAlloc;
    _ = sourceKindForExternalFormat;
    _ = manifestFormatForExternalFormat;
    _ = externalTableBindingFromRuntimeExternalBaseSource;
    _ = isParquetDataObject;
    _ = planParquetPrefixInventoryAlloc;
    _ = planParquetPrefixInventoryFromObjectStorageAlloc;
    _ = ObjectListingPaginationGuard;
    _ = parseIcebergMetadataPlanAlloc;
    _ = parseIcebergManifestListAlloc;
    _ = parseIcebergDataManifestAlloc;
    _ = planIcebergInventoryFromDataFilesAlloc;
    _ = planIcebergInventoryFromSnapshotManifestsAlloc;
    _ = bindingFromInventory;
    _ = rowRefForInventoryRow;
    _ = validateBatchAgainstInventory;
}
