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

//! Object-storage backed Iceberg snapshot planning.
//!
//! This stitches the Iceberg pieces together: read table metadata, read the
//! pinned manifest list, read each data manifest, and produce an Antfly external
//! source inventory that the existing Parquet footer discovery path can enrich.

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source = @import("../external_source/types.zig");
const iceberg_avro = @import("../external_source/iceberg_avro.zig");
const external_binding = @import("../external_source/catalog_binding.zig");
const iceberg_inventory = @import("../external_source/iceberg_inventory.zig");
const iceberg_metadata = @import("../external_source/iceberg_metadata.zig");
const lake_iceberg_deletes = @import("lake_iceberg_deletes.zig");
const lake_object_reader = @import("lake_object_reader.zig");
const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
const lake_range_io = @import("lake_range_io.zig");
const object_storage = @import("../../storage/object_storage.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const SnapshotReadRequest = struct {
    client: object_storage.ObjectStorage,
    source_id: []const u8,
    metadata_uri: []const u8,
    requested_snapshot_id: ?[]const u8 = null,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache = null,

    pub fn validate(self: SnapshotReadRequest) !void {
        if (self.source_id.len == 0) return error.InvalidIcebergSnapshotRead;
        if (self.metadata_uri.len == 0) return error.InvalidIcebergSnapshotRead;
    }
};

pub const IcebergDeleteFile = struct {
    content: iceberg_avro.DataFileContent,
    file_path: []u8,
    file_format: []u8,
    snapshot_id: i64,
    data_sequence_number: i64,
    file_sequence_number: i64,
    equality_ids: []i32 = &.{},
    equality_columns: [][]u8 = &.{},
    record_count: u64,
    file_size_in_bytes: u64,

    pub fn deinit(self: *IcebergDeleteFile, alloc: Allocator) void {
        alloc.free(self.file_path);
        alloc.free(self.file_format);
        if (self.equality_ids.len > 0) alloc.free(self.equality_ids);
        for (self.equality_columns) |column| alloc.free(column);
        if (self.equality_columns.len > 0) alloc.free(self.equality_columns);
        self.* = undefined;
    }

    pub fn validate(self: IcebergDeleteFile) !void {
        switch (self.content) {
            .position_deletes, .equality_deletes => {},
            .data => return error.InvalidIcebergDeleteManifest,
        }
        if (self.file_path.len == 0) return error.InvalidIcebergDeleteManifest;
        if (self.file_format.len == 0) return error.InvalidIcebergDeleteManifest;
        if (!std.ascii.eqlIgnoreCase(self.file_format, "PARQUET")) return error.UnsupportedIcebergDataFileFormat;
        if (self.record_count == 0) return error.InvalidIcebergDeleteManifest;
        if (self.file_size_in_bytes == 0) return error.InvalidIcebergDeleteManifest;
        if (self.content == .equality_deletes and self.equality_ids.len == 0) return error.InvalidIcebergDeleteManifest;
        if (self.equality_columns.len != 0 and self.equality_columns.len != self.equality_ids.len) {
            return error.InvalidIcebergDeleteManifest;
        }
        for (self.equality_ids, 0..) |field_id, idx| {
            if (field_id < 0) return error.InvalidIcebergDeleteManifest;
            for (self.equality_ids[0..idx]) |previous| {
                if (previous == field_id) return error.InvalidIcebergDeleteManifest;
            }
        }
        for (self.equality_columns, 0..) |column, idx| {
            if (column.len == 0) return error.InvalidIcebergDeleteManifest;
            for (self.equality_columns[0..idx]) |previous| {
                if (std.mem.eql(u8, previous, column)) return error.InvalidIcebergDeleteManifest;
            }
        }
    }
};

pub const IcebergDeletePlan = struct {
    files: []IcebergDeleteFile,

    pub fn deinit(self: *IcebergDeletePlan, alloc: Allocator) void {
        for (self.files) |*file| file.deinit(alloc);
        if (self.files.len > 0) alloc.free(self.files);
        self.* = undefined;
    }

    pub fn activeFileCount(self: IcebergDeletePlan) usize {
        return self.files.len;
    }

    pub fn activePositionDeleteFileCount(self: IcebergDeletePlan) usize {
        var count: usize = 0;
        for (self.files) |file| {
            if (file.content == .position_deletes) count += 1;
        }
        return count;
    }

    pub fn activeEqualityDeleteFileCount(self: IcebergDeletePlan) usize {
        var count: usize = 0;
        for (self.files) |file| {
            if (file.content == .equality_deletes) count += 1;
        }
        return count;
    }

    pub fn validate(self: IcebergDeletePlan) !void {
        for (self.files) |file| try file.validate();
    }
};

pub const SnapshotWithDeletePlan = struct {
    inventory: external_source.Inventory,
    delete_plan: IcebergDeletePlan = .{ .files = &.{} },

    pub fn deinit(self: *SnapshotWithDeletePlan, alloc: Allocator) void {
        self.inventory.deinit(alloc);
        self.delete_plan.deinit(alloc);
        self.* = undefined;
    }
};

pub const PositionDeleteRowRefsReadRequest = struct {
    reader: lake_parquet_rowgroup.ObjectRangeReader,
    client: ?object_storage.ObjectStorage = null,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache = null,
    data_inventory: external_source.Inventory,
    delete_plan: IcebergDeletePlan,
    coalesce_options: lake_range_io.CoalesceOptions = .{},
    footer_probe_bytes: u64 = 64 * 1024,
};

pub const DeleteRowRefsReadRequest = PositionDeleteRowRefsReadRequest;

pub fn readSnapshotInventoryAlloc(
    alloc: Allocator,
    request: SnapshotReadRequest,
) !external_source.Inventory {
    try request.validate();
    var client = request.client;
    client.allocator = alloc;

    const metadata_bytes = try readFullObjectAlloc(alloc, &client, request.cache, request.metadata_uri, .iceberg_metadata, null);
    defer alloc.free(metadata_bytes);
    var metadata_plan = try iceberg_metadata.parseMetadataPlanAlloc(
        alloc,
        request.metadata_uri,
        metadata_bytes,
        request.requested_snapshot_id,
    );
    defer metadata_plan.deinit(alloc);

    const current_snapshot = metadata_plan.currentSnapshot();
    const manifest_list_bytes = try readFullObjectAlloc(alloc, &client, request.cache, current_snapshot.manifest_list_uri, .iceberg_metadata, null);
    defer alloc.free(manifest_list_bytes);
    var manifest_list = try iceberg_avro.parseManifestListAlloc(alloc, manifest_list_bytes);
    defer manifest_list.deinit(alloc);

    const decoded_manifests = try alloc.alloc(iceberg_inventory.DecodedManifest, manifest_list.entries.len);
    defer alloc.free(decoded_manifests);
    var initialized: usize = 0;
    defer {
        for (decoded_manifests[0..initialized]) |*decoded| {
            decoded.manifest.deinit(alloc);
        }
    }

    for (manifest_list.entries) |manifest_entry| {
        switch (manifest_entry.content) {
            .data => {
                const manifest_bytes = try readFullObjectAlloc(
                    alloc,
                    &client,
                    request.cache,
                    manifest_entry.manifest_path,
                    .iceberg_metadata,
                    manifest_entry.manifest_length,
                );
                defer alloc.free(manifest_bytes);
                decoded_manifests[initialized] = .{
                    .manifest_path = manifest_entry.manifest_path,
                    .manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes),
                };
                initialized += 1;
            },
            .deletes => {
                const active_deletes = try readDeleteManifestHasActiveDeletesAlloc(
                    alloc,
                    &client,
                    request.cache,
                    manifest_entry,
                );
                if (active_deletes) return error.UnsupportedIcebergDeletes;
            },
        }
    }

    return try iceberg_inventory.planInventoryFromSnapshotManifestsAlloc(alloc, .{
        .source_id = request.source_id,
        .metadata_plan = metadata_plan,
        .manifest_list = manifest_list,
        .data_manifests = decoded_manifests[0..initialized],
    });
}

pub fn readSnapshotInventoryAndDeletePlanAlloc(
    alloc: Allocator,
    request: SnapshotReadRequest,
) !SnapshotWithDeletePlan {
    try request.validate();
    var client = request.client;
    client.allocator = alloc;

    const metadata_bytes = try readFullObjectAlloc(alloc, &client, request.cache, request.metadata_uri, .iceberg_metadata, null);
    defer alloc.free(metadata_bytes);
    var metadata_plan = try iceberg_metadata.parseMetadataPlanAlloc(
        alloc,
        request.metadata_uri,
        metadata_bytes,
        request.requested_snapshot_id,
    );
    defer metadata_plan.deinit(alloc);

    const current_snapshot = metadata_plan.currentSnapshot();
    const manifest_list_bytes = try readFullObjectAlloc(alloc, &client, request.cache, current_snapshot.manifest_list_uri, .iceberg_metadata, null);
    defer alloc.free(manifest_list_bytes);
    var manifest_list = try iceberg_avro.parseManifestListAlloc(alloc, manifest_list_bytes);
    defer manifest_list.deinit(alloc);

    const decoded_manifests = try alloc.alloc(iceberg_inventory.DecodedManifest, manifest_list.entries.len);
    defer alloc.free(decoded_manifests);
    var initialized: usize = 0;
    defer {
        for (decoded_manifests[0..initialized]) |*decoded| {
            decoded.manifest.deinit(alloc);
        }
    }

    var delete_files = std.ArrayListUnmanaged(IcebergDeleteFile).empty;
    defer delete_files.deinit(alloc);
    errdefer deinitDeleteFileItems(alloc, delete_files.items);

    for (manifest_list.entries) |manifest_entry| {
        switch (manifest_entry.content) {
            .data => {
                const manifest_bytes = try readFullObjectAlloc(
                    alloc,
                    &client,
                    request.cache,
                    manifest_entry.manifest_path,
                    .iceberg_metadata,
                    manifest_entry.manifest_length,
                );
                defer alloc.free(manifest_bytes);
                decoded_manifests[initialized] = .{
                    .manifest_path = manifest_entry.manifest_path,
                    .manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes),
                };
                initialized += 1;
            },
            .deletes => {
                const manifest_bytes = try readFullObjectAlloc(
                    alloc,
                    &client,
                    request.cache,
                    manifest_entry.manifest_path,
                    .iceberg_delete_metadata,
                    manifest_entry.manifest_length,
                );
                defer alloc.free(manifest_bytes);

                var delete_manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes);
                defer delete_manifest.deinit(alloc);
                try appendActiveDeleteFilesFromManifestAlloc(alloc, &delete_files, manifest_entry, delete_manifest, metadata_plan);
            },
        }
    }

    var inventory = try planInventoryFromDecodedDataManifestsAllowingDeletesAlloc(
        alloc,
        request.source_id,
        metadata_plan,
        manifest_list,
        decoded_manifests[0..initialized],
    );
    errdefer inventory.deinit(alloc);

    const files = try delete_files.toOwnedSlice(alloc);
    var delete_plan = IcebergDeletePlan{ .files = files };
    errdefer delete_plan.deinit(alloc);
    try delete_plan.validate();

    return .{
        .inventory = inventory,
        .delete_plan = delete_plan,
    };
}

pub fn readSnapshotDeletePlanAlloc(
    alloc: Allocator,
    request: SnapshotReadRequest,
) !IcebergDeletePlan {
    try request.validate();
    var client = request.client;
    client.allocator = alloc;

    const metadata_bytes = try readFullObjectAlloc(alloc, &client, request.cache, request.metadata_uri, .iceberg_metadata, null);
    defer alloc.free(metadata_bytes);
    var metadata_plan = try iceberg_metadata.parseMetadataPlanAlloc(
        alloc,
        request.metadata_uri,
        metadata_bytes,
        request.requested_snapshot_id,
    );
    defer metadata_plan.deinit(alloc);

    const current_snapshot = metadata_plan.currentSnapshot();
    const manifest_list_bytes = try readFullObjectAlloc(alloc, &client, request.cache, current_snapshot.manifest_list_uri, .iceberg_metadata, null);
    defer alloc.free(manifest_list_bytes);
    var manifest_list = try iceberg_avro.parseManifestListAlloc(alloc, manifest_list_bytes);
    defer manifest_list.deinit(alloc);

    var delete_files = std.ArrayListUnmanaged(IcebergDeleteFile).empty;
    defer delete_files.deinit(alloc);
    errdefer deinitDeleteFileItems(alloc, delete_files.items);

    for (manifest_list.entries) |manifest_entry| {
        if (manifest_entry.content != .deletes) continue;
        const manifest_bytes = try readFullObjectAlloc(
            alloc,
            &client,
            request.cache,
            manifest_entry.manifest_path,
            .iceberg_delete_metadata,
            manifest_entry.manifest_length,
        );
        defer alloc.free(manifest_bytes);

        var delete_manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes);
        defer delete_manifest.deinit(alloc);
        try appendActiveDeleteFilesFromManifestAlloc(alloc, &delete_files, manifest_entry, delete_manifest, metadata_plan);
    }

    const files = try delete_files.toOwnedSlice(alloc);
    var plan = IcebergDeletePlan{ .files = files };
    errdefer plan.deinit(alloc);
    try plan.validate();
    return plan;
}

pub fn readPositionDeleteRowRefsAlloc(
    alloc: Allocator,
    request: PositionDeleteRowRefsReadRequest,
) ![]rowsource.RowRef {
    try request.data_inventory.validate();
    if (request.data_inventory.format != .iceberg) return error.InvalidIcebergPositionDeleteInventory;
    try request.delete_plan.validate();
    if (request.delete_plan.activePositionDeleteFileCount() == 0) return try alloc.alloc(rowsource.RowRef, 0);

    var delete_inventory = try positionDeleteFilesInventoryAlloc(alloc, request.data_inventory, request.delete_plan, request.client);
    defer delete_inventory.deinit(alloc);

    const columns = [_][]const u8{ "file_path", "pos" };
    var discovered = if (request.cache) |cache|
        try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
            alloc,
            request.reader,
            cache,
            delete_inventory,
            &columns,
            request.footer_probe_bytes,
        )
    else
        try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
            alloc,
            request.reader,
            delete_inventory,
            &columns,
            request.footer_probe_bytes,
        );
    defer discovered.deinit(alloc);

    var delete_rows = try lake_parquet_rowgroup.querySupportedI64ObjectRangeRowsAlloc(alloc, .{
        .reader = request.reader,
        .cache = request.cache,
        .inventory = discovered.inventory,
        .projected_columns = &columns,
        .coalesce_options = request.coalesce_options,
    });
    defer delete_rows.deinit(alloc);

    return try lake_iceberg_deletes.positionDeleteScanResultToRowRefsAlloc(
        alloc,
        request.data_inventory,
        delete_rows,
        .{},
    );
}

pub fn readDeleteRowRefsAlloc(
    alloc: Allocator,
    request: DeleteRowRefsReadRequest,
) ![]rowsource.RowRef {
    try request.data_inventory.validate();
    if (request.data_inventory.format != .iceberg) return error.InvalidIcebergPositionDeleteInventory;
    try request.delete_plan.validate();

    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer out.deinit(alloc);

    const position_refs = try readPositionDeleteRowRefsAlloc(alloc, request);
    defer alloc.free(position_refs);
    for (position_refs) |row_ref| {
        if (!rowRefsContain(out.items, row_ref)) try out.append(alloc, row_ref);
    }

    const equality_refs = try readEqualityDeleteRowRefsAlloc(alloc, request);
    defer alloc.free(equality_refs);
    for (equality_refs) |row_ref| {
        if (!rowRefsContain(out.items, row_ref)) try out.append(alloc, row_ref);
    }

    return try out.toOwnedSlice(alloc);
}

fn readEqualityDeleteRowRefsAlloc(
    alloc: Allocator,
    request: DeleteRowRefsReadRequest,
) ![]rowsource.RowRef {
    if (request.delete_plan.activeEqualityDeleteFileCount() == 0) return try alloc.alloc(rowsource.RowRef, 0);

    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer out.deinit(alloc);

    for (request.delete_plan.files) |delete_file| {
        if (delete_file.content != .equality_deletes) continue;
        if (delete_file.equality_columns.len == 0) return error.UnsupportedIcebergDeletes;

        var delete_inventory = try equalityDeleteFileInventoryAlloc(alloc, request.data_inventory, delete_file, request.client);
        defer delete_inventory.deinit(alloc);

        const equality_columns: []const []const u8 = delete_file.equality_columns;
        var discovered_delete = if (request.cache) |cache|
            try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
                alloc,
                request.reader,
                cache,
                delete_inventory,
                equality_columns,
                request.footer_probe_bytes,
            )
        else
            try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
                alloc,
                request.reader,
                delete_inventory,
                equality_columns,
                request.footer_probe_bytes,
            );
        defer discovered_delete.deinit(alloc);

        var delete_rows = try lake_parquet_rowgroup.querySupportedI64ObjectRangeRowsAlloc(alloc, .{
            .reader = request.reader,
            .cache = request.cache,
            .inventory = discovered_delete.inventory,
            .projected_columns = equality_columns,
            .coalesce_options = request.coalesce_options,
        });
        defer delete_rows.deinit(alloc);
        if (delete_rows.rows.len == 0) continue;

        var discovered_data: ?lake_parquet_rowgroup.DiscoveredObjectRangeRowGroupPlan = null;
        defer if (discovered_data) |*discovered| discovered.deinit(alloc);
        if (!inventoryHasRowGroupMetadata(request.data_inventory)) {
            discovered_data = if (request.cache) |cache|
                try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
                    alloc,
                    request.reader,
                    cache,
                    request.data_inventory,
                    equality_columns,
                    request.footer_probe_bytes,
                )
            else
                try lake_parquet_rowgroup.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(
                    alloc,
                    request.reader,
                    request.data_inventory,
                    equality_columns,
                    request.footer_probe_bytes,
                );
        }
        const data_inventory = if (discovered_data) |discovered| discovered.inventory else request.data_inventory;

        var data_rows = try lake_parquet_rowgroup.querySupportedI64ObjectRangeRowsAlloc(alloc, .{
            .binding = bindingForPinnedIcebergInventory(data_inventory),
            .reader = request.reader,
            .cache = request.cache,
            .inventory = data_inventory,
            .projected_columns = equality_columns,
            .coalesce_options = request.coalesce_options,
        });
        defer data_rows.deinit(alloc);

        const matched_refs = try lake_iceberg_deletes.equalityDeleteScanResultsToRowRefsAlloc(
            alloc,
            data_rows,
            delete_rows,
            equality_columns,
        );
        defer alloc.free(matched_refs);

        for (matched_refs) |row_ref| {
            const stable_ref = try rebindRowRefToInventory(request.data_inventory, row_ref);
            if (!try equalityDeleteAppliesToRowRef(request.data_inventory, delete_file, stable_ref)) continue;
            if (!rowRefsContain(out.items, stable_ref)) try out.append(alloc, stable_ref);
        }
    }

    return try out.toOwnedSlice(alloc);
}

fn planInventoryFromDecodedDataManifestsAllowingDeletesAlloc(
    alloc: Allocator,
    source_id: []const u8,
    metadata_plan: iceberg_metadata.Plan,
    manifest_list: iceberg_avro.ManifestList,
    data_manifests: []const iceberg_inventory.DecodedManifest,
) !external_source.Inventory {
    var total_entries: usize = 0;
    for (manifest_list.entries) |manifest_entry| {
        if (manifest_entry.content != .data) continue;
        const decoded = try decodedManifestForPath(data_manifests, manifest_entry.manifest_path);
        try validateDataManifestSummary(manifest_entry, decoded.manifest);
        total_entries += decoded.manifest.entries.len;
    }
    if (total_entries == 0) return error.EmptyIcebergInventory;

    const data_files = try alloc.alloc(iceberg_avro.DataFileEntry, total_entries);
    defer alloc.free(data_files);
    var next_file: usize = 0;
    for (manifest_list.entries) |manifest_entry| {
        if (manifest_entry.content != .data) continue;
        const decoded = try decodedManifestForPath(data_manifests, manifest_entry.manifest_path);
        for (decoded.manifest.entries) |entry| {
            data_files[next_file] = entry;
            next_file += 1;
        }
    }

    return try iceberg_inventory.planInventoryFromDataFilesAlloc(alloc, .{
        .source_id = source_id,
        .source_uri = metadata_plan.location,
        .snapshot_id = metadata_plan.current_snapshot_id,
        .schema_fingerprint = metadata_plan.schema_fingerprint,
        .data_files = data_files,
    });
}

fn decodedManifestForPath(
    decoded_manifests: []const iceberg_inventory.DecodedManifest,
    manifest_path: []const u8,
) !iceberg_inventory.DecodedManifest {
    var found: ?iceberg_inventory.DecodedManifest = null;
    for (decoded_manifests) |decoded| {
        if (!std.mem.eql(u8, decoded.manifest_path, manifest_path)) continue;
        if (found != null) return error.DuplicateIcebergManifest;
        found = decoded;
    }
    return found orelse error.MissingIcebergManifest;
}

fn validateDataManifestSummary(
    manifest_entry: iceberg_avro.ManifestListEntry,
    manifest: iceberg_avro.DataManifest,
) !void {
    if (!hasManifestSummary(manifest_entry)) return;

    var added_files: u32 = 0;
    var existing_files: u32 = 0;
    var deleted_files: u32 = 0;
    var added_rows: u64 = 0;
    var existing_rows: u64 = 0;
    var deleted_rows: u64 = 0;
    for (manifest.entries) |entry| {
        if (entry.content != .data) return error.InvalidIcebergDataManifest;
        switch (entry.status) {
            .added => {
                added_files += 1;
                added_rows += entry.record_count;
            },
            .existing => {
                existing_files += 1;
                existing_rows += entry.record_count;
            },
            .deleted => {
                deleted_files += 1;
                deleted_rows += entry.record_count;
            },
        }
    }

    if (added_files != manifest_entry.added_files_count) return error.IcebergManifestSummaryMismatch;
    if (existing_files != manifest_entry.existing_files_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_files != manifest_entry.deleted_files_count) return error.IcebergManifestSummaryMismatch;
    if (added_rows != manifest_entry.added_rows_count) return error.IcebergManifestSummaryMismatch;
    if (existing_rows != manifest_entry.existing_rows_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_rows != manifest_entry.deleted_rows_count) return error.IcebergManifestSummaryMismatch;
}

fn positionDeleteFilesInventoryAlloc(
    alloc: Allocator,
    data_inventory: external_source.Inventory,
    delete_plan: IcebergDeletePlan,
    maybe_client: ?object_storage.ObjectStorage,
) !external_source.Inventory {
    const file_count = delete_plan.activePositionDeleteFileCount();
    if (file_count == 0) return error.InvalidIcebergDeleteManifest;

    const files = try alloc.alloc(external_source.FileEntry, file_count);
    errdefer alloc.free(files);
    var initialized: usize = 0;
    errdefer {
        for (files[0..initialized]) |*file| file.deinit(alloc);
    }

    for (delete_plan.files) |delete_file| {
        if (delete_file.content != .position_deletes) continue;
        files[initialized] = try positionDeleteFileEntryAlloc(alloc, data_inventory, delete_file, maybe_client);
        initialized += 1;
    }

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, data_inventory.source_id),
        .source_uri = try alloc.dupe(u8, data_inventory.source_uri),
        .snapshot_id = try alloc.dupe(u8, data_inventory.snapshot_id),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-position-delete:v1:file_path-pos"),
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn equalityDeleteFileInventoryAlloc(
    alloc: Allocator,
    data_inventory: external_source.Inventory,
    delete_file: IcebergDeleteFile,
    maybe_client: ?object_storage.ObjectStorage,
) !external_source.Inventory {
    const files = try alloc.alloc(external_source.FileEntry, 1);
    errdefer alloc.free(files);
    var initialized = false;
    errdefer if (initialized) files[0].deinit(alloc);

    files[0] = try deleteFileEntryAlloc(alloc, data_inventory, delete_file, maybe_client, "iceberg-equality-delete");
    initialized = true;

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, data_inventory.source_id),
        .source_uri = try alloc.dupe(u8, data_inventory.source_uri),
        .snapshot_id = try alloc.dupe(u8, data_inventory.snapshot_id),
        .schema_fingerprint = try equalityDeleteSchemaFingerprintAlloc(alloc, delete_file.equality_columns),
        .files = files,
    };
    errdefer inventory.deinit(alloc);
    try inventory.validate();
    return inventory;
}

fn positionDeleteFileEntryAlloc(
    alloc: Allocator,
    data_inventory: external_source.Inventory,
    delete_file: IcebergDeleteFile,
    maybe_client: ?object_storage.ObjectStorage,
) !external_source.FileEntry {
    return try deleteFileEntryAlloc(alloc, data_inventory, delete_file, maybe_client, "iceberg-position-delete");
}

fn deleteFileEntryAlloc(
    alloc: Allocator,
    data_inventory: external_source.Inventory,
    delete_file: IcebergDeleteFile,
    maybe_client: ?object_storage.ObjectStorage,
    kind: []const u8,
) !external_source.FileEntry {
    const file_id = try alloc.dupe(u8, delete_file.file_path);
    errdefer alloc.free(file_id);
    const object_uri = try alloc.dupe(u8, delete_file.file_path);
    errdefer alloc.free(object_uri);
    var etag: []u8 = &.{};
    errdefer if (etag.len > 0) alloc.free(etag);
    var version_id: []u8 = &.{};
    errdefer if (version_id.len > 0) alloc.free(version_id);

    if (maybe_client) |source_client| {
        var client = source_client;
        client.allocator = alloc;
        const location = try lake_range_io.objectLocationForUri(delete_file.file_path);
        var meta = try client.statObject(location.bucket, location.key);
        defer meta.deinit(alloc);
        if (meta.content_length != delete_file.file_size_in_bytes) return error.IcebergManifestLengthMismatch;
        if (meta.etag) |value| etag = try alloc.dupe(u8, value);
        if (meta.version_id) |value| version_id = try alloc.dupe(u8, value);
        if (etag.len == 0 and version_id.len == 0) {
            version_id = try std.fmt.allocPrint(
                alloc,
                "object-stat:v1:uri={s}:len={d}",
                .{ delete_file.file_path, meta.content_length },
            );
        }
    } else {
        version_id = try std.fmt.allocPrint(
            alloc,
            "{s}:v1:snapshot={s}:data-seq={d}:file-seq={d}:path={s}",
            .{ kind, data_inventory.snapshot_id, delete_file.data_sequence_number, delete_file.file_sequence_number, delete_file.file_path },
        );
    }

    return .{
        .file_id = file_id,
        .object_uri = object_uri,
        .etag = etag,
        .version_id = version_id,
        .byte_len = delete_file.file_size_in_bytes,
        .row_count = delete_file.record_count,
        .row_groups = &.{},
    };
}

fn equalityDeleteSchemaFingerprintAlloc(alloc: Allocator, columns: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "iceberg-equality-delete:v1");
    for (columns) |column| {
        try out.append(alloc, ':');
        try out.appendSlice(alloc, column);
    }
    return try out.toOwnedSlice(alloc);
}

fn bindingForPinnedIcebergInventory(inventory: external_source.Inventory) external_binding.Binding {
    return .{
        .table_id = inventory.source_id,
        .format = .iceberg,
        .source_uri = inventory.source_uri,
        .snapshot_mode = .{ .snapshot_id = inventory.snapshot_id },
        .schema_fingerprint = inventory.schema_fingerprint,
        .write_policy = .read_only,
    };
}

fn inventoryHasRowGroupMetadata(inventory: external_source.Inventory) bool {
    for (inventory.files) |file| {
        if (file.row_groups.len != 0) return true;
    }
    return false;
}

fn rebindRowRefToInventory(inventory: external_source.Inventory, row_ref: rowsource.RowRef) !rowsource.RowRef {
    const external = switch (row_ref) {
        .external => |external| external,
        else => return row_ref,
    };
    const file = inventory.fileById(external.file_id) orelse return error.ExternalSourceFileNotFound;
    return .{ .external = .{
        .source_id = inventory.source_id,
        .snapshot_id = inventory.snapshot_id,
        .file_id = file.file_id,
        .row_group_ordinal = external.row_group_ordinal,
        .row_ordinal = external.row_ordinal,
    } };
}

fn equalityDeleteAppliesToRowRef(
    inventory: external_source.Inventory,
    delete_file: IcebergDeleteFile,
    row_ref: rowsource.RowRef,
) !bool {
    const external = switch (row_ref) {
        .external => |external| external,
        else => return false,
    };
    const file = inventory.fileById(external.file_id) orelse return error.ExternalSourceFileNotFound;
    const data_sequence = file.data_sequence_number orelse try icebergDataSequenceFromFileVersion(file.version_id);
    return data_sequence < delete_file.data_sequence_number;
}

fn icebergDataSequenceFromFileVersion(version_id: []const u8) !i64 {
    const marker = "data_seq=";
    const start = std.mem.indexOf(u8, version_id, marker) orelse return error.UnsupportedIcebergDeletes;
    const value_start = start + marker.len;
    var value_end = value_start;
    while (value_end < version_id.len and version_id[value_end] != ':') : (value_end += 1) {}
    if (value_end == value_start) return error.UnsupportedIcebergDeletes;
    return try std.fmt.parseInt(i64, version_id[value_start..value_end], 10);
}

fn rowRefsContain(haystack: []const rowsource.RowRef, needle: rowsource.RowRef) bool {
    for (haystack) |candidate| {
        if (rowRefsEqual(candidate, needle)) return true;
    }
    return false;
}

fn rowRefsEqual(a: rowsource.RowRef, b: rowsource.RowRef) bool {
    return switch (a) {
        .external => |left| switch (b) {
            .external => |right| std.mem.eql(u8, left.source_id, right.source_id) and
                std.mem.eql(u8, left.snapshot_id, right.snapshot_id) and
                std.mem.eql(u8, left.file_id, right.file_id) and
                left.row_group_ordinal == right.row_group_ordinal and
                left.row_ordinal == right.row_ordinal,
            else => false,
        },
        .serverless => |left| switch (b) {
            .serverless => |right| std.mem.eql(u8, left.fragment_id, right.fragment_id) and left.row_ordinal == right.row_ordinal,
            else => false,
        },
        .relational_key => |left| switch (b) {
            .relational_key => |right| std.mem.eql(u8, left, right),
            else => false,
        },
    };
}

fn readDeleteManifestHasActiveDeletesAlloc(
    alloc: Allocator,
    client: *object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    manifest_entry: iceberg_avro.ManifestListEntry,
) !bool {
    const manifest_bytes = try readFullObjectAlloc(
        alloc,
        client,
        cache,
        manifest_entry.manifest_path,
        .iceberg_delete_metadata,
        manifest_entry.manifest_length,
    );
    defer alloc.free(manifest_bytes);

    var delete_manifest = try iceberg_avro.parseDataManifestAlloc(alloc, manifest_bytes);
    defer delete_manifest.deinit(alloc);
    return try validateDeleteManifestAndHasActiveDeletes(manifest_entry, delete_manifest);
}

fn appendActiveDeleteFilesFromManifestAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(IcebergDeleteFile),
    manifest_entry: iceberg_avro.ManifestListEntry,
    manifest: iceberg_avro.DataManifest,
    metadata_plan: iceberg_metadata.Plan,
) !void {
    var added_files: u32 = 0;
    var existing_files: u32 = 0;
    var deleted_files: u32 = 0;
    var added_rows: u64 = 0;
    var existing_rows: u64 = 0;
    var deleted_rows: u64 = 0;

    for (manifest.entries) |entry| {
        switch (entry.content) {
            .data => return error.InvalidIcebergDeleteManifest,
            .position_deletes, .equality_deletes => {},
        }
        switch (entry.status) {
            .added => {
                added_files += 1;
                added_rows += entry.record_count;
                try appendDeleteFileFromEntryAlloc(alloc, out, entry, metadata_plan);
            },
            .existing => {
                existing_files += 1;
                existing_rows += entry.record_count;
                try appendDeleteFileFromEntryAlloc(alloc, out, entry, metadata_plan);
            },
            .deleted => {
                deleted_files += 1;
                deleted_rows += entry.record_count;
            },
        }
    }

    if (!hasManifestSummary(manifest_entry)) return;
    if (added_files != manifest_entry.added_files_count) return error.IcebergManifestSummaryMismatch;
    if (existing_files != manifest_entry.existing_files_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_files != manifest_entry.deleted_files_count) return error.IcebergManifestSummaryMismatch;
    if (added_rows != manifest_entry.added_rows_count) return error.IcebergManifestSummaryMismatch;
    if (existing_rows != manifest_entry.existing_rows_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_rows != manifest_entry.deleted_rows_count) return error.IcebergManifestSummaryMismatch;
}

fn appendDeleteFileFromEntryAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(IcebergDeleteFile),
    entry: iceberg_avro.DataFileEntry,
    metadata_plan: iceberg_metadata.Plan,
) !void {
    const file_path = try alloc.dupe(u8, entry.file_path);
    errdefer alloc.free(file_path);
    const file_format = try alloc.dupe(u8, entry.file_format);
    errdefer alloc.free(file_format);
    const equality_ids = try cloneI32SliceAlloc(alloc, entry.equality_ids);
    errdefer if (equality_ids.len > 0) alloc.free(equality_ids);
    const equality_columns = try cloneEqualityColumnNamesAlloc(alloc, entry.equality_ids, metadata_plan);
    errdefer freeEqualityColumnNames(alloc, equality_columns);

    const delete_file = IcebergDeleteFile{
        .content = entry.content,
        .file_path = file_path,
        .file_format = file_format,
        .snapshot_id = entry.snapshot_id,
        .data_sequence_number = entry.data_sequence_number,
        .file_sequence_number = entry.file_sequence_number,
        .equality_ids = equality_ids,
        .equality_columns = equality_columns,
        .record_count = entry.record_count,
        .file_size_in_bytes = entry.file_size_in_bytes,
    };
    try delete_file.validate();
    try out.append(alloc, delete_file);
}

fn cloneI32SliceAlloc(alloc: Allocator, source: []const i32) ![]i32 {
    if (source.len == 0) return &.{};
    return try alloc.dupe(i32, source);
}

fn cloneEqualityColumnNamesAlloc(
    alloc: Allocator,
    equality_ids: []const i32,
    metadata_plan: iceberg_metadata.Plan,
) ![][]u8 {
    if (equality_ids.len == 0 or metadata_plan.schema_fields.len == 0) return &.{};
    const columns = try alloc.alloc([]u8, equality_ids.len);
    errdefer alloc.free(columns);
    var initialized: usize = 0;
    errdefer {
        for (columns[0..initialized]) |column| alloc.free(column);
    }
    for (equality_ids, 0..) |field_id, idx| {
        const column_name = metadata_plan.fieldNameForId(field_id) orelse return error.UnsupportedIcebergDeletes;
        columns[idx] = try alloc.dupe(u8, column_name);
        initialized += 1;
    }
    return columns;
}

fn freeEqualityColumnNames(alloc: Allocator, columns: [][]u8) void {
    for (columns) |column| alloc.free(column);
    if (columns.len > 0) alloc.free(columns);
}

fn deinitDeleteFileItems(alloc: Allocator, files: []IcebergDeleteFile) void {
    for (files) |*file| file.deinit(alloc);
}

fn validateDeleteManifestAndHasActiveDeletes(
    manifest_entry: iceberg_avro.ManifestListEntry,
    manifest: iceberg_avro.DataManifest,
) !bool {
    var added_files: u32 = 0;
    var existing_files: u32 = 0;
    var deleted_files: u32 = 0;
    var added_rows: u64 = 0;
    var existing_rows: u64 = 0;
    var deleted_rows: u64 = 0;
    for (manifest.entries) |entry| {
        switch (entry.content) {
            .data => return error.InvalidIcebergDeleteManifest,
            .position_deletes, .equality_deletes => {},
        }
        switch (entry.status) {
            .added => {
                added_files += 1;
                added_rows += entry.record_count;
            },
            .existing => {
                existing_files += 1;
                existing_rows += entry.record_count;
            },
            .deleted => {
                deleted_files += 1;
                deleted_rows += entry.record_count;
            },
        }
    }
    const has_active_deletes = added_files != 0 or existing_files != 0 or added_rows != 0 or existing_rows != 0;
    if (!hasManifestSummary(manifest_entry)) return has_active_deletes;
    if (added_files != manifest_entry.added_files_count) return error.IcebergManifestSummaryMismatch;
    if (existing_files != manifest_entry.existing_files_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_files != manifest_entry.deleted_files_count) return error.IcebergManifestSummaryMismatch;
    if (added_rows != manifest_entry.added_rows_count) return error.IcebergManifestSummaryMismatch;
    if (existing_rows != manifest_entry.existing_rows_count) return error.IcebergManifestSummaryMismatch;
    if (deleted_rows != manifest_entry.deleted_rows_count) return error.IcebergManifestSummaryMismatch;
    return has_active_deletes;
}

fn hasManifestSummary(manifest_entry: iceberg_avro.ManifestListEntry) bool {
    return manifest_entry.added_files_count != 0 or
        manifest_entry.existing_files_count != 0 or
        manifest_entry.deleted_files_count != 0 or
        manifest_entry.added_rows_count != 0 or
        manifest_entry.existing_rows_count != 0 or
        manifest_entry.deleted_rows_count != 0;
}

fn readFullObjectAlloc(
    alloc: Allocator,
    client: *object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    uri: []const u8,
    purpose: lake_range_io.RangePurpose,
    expected_byte_len: ?u64,
) ![]u8 {
    const location = try lake_range_io.objectLocationForUri(uri);
    var meta = try client.statObject(location.bucket, location.key);
    defer meta.deinit(alloc);
    if (expected_byte_len) |expected| {
        if (meta.content_length != expected) return error.IcebergManifestLengthMismatch;
    }
    const version = try objectVersionForMetadataAlloc(alloc, meta, uri);
    defer {
        if (version.etag.len > 0) alloc.free(@constCast(version.etag));
        if (version.version_id.len > 0) alloc.free(@constCast(version.version_id));
    }
    const object = try lake_range_io.objectRefForIcebergUri(uri, meta.content_length, version);
    const read = switch (purpose) {
        .iceberg_metadata => try lake_range_io.planIcebergManifestListRead(object),
        .iceberg_delete_metadata => try lake_range_io.planIcebergManifestRead(object, .deletes),
        else => return error.InvalidIcebergSnapshotRead,
    };
    return try readMaybeCachedIcebergRangeAlloc(alloc, client.*, cache, read);
}

fn readMaybeCachedIcebergRangeAlloc(
    alloc: Allocator,
    client: object_storage.ObjectStorage,
    cache: ?*lake_parquet_rowgroup.ObjectRangeCache,
    read: lake_range_io.RangeRead,
) ![]u8 {
    var range_reader = lake_object_reader.ObjectStorageRangeReader.init(client);
    const parquet_reader = range_reader.parquetReader();
    if (cache) |range_cache| {
        return try range_cache.readAlloc(alloc, parquet_reader, read);
    }
    return try parquet_reader.readPlannedAlloc(alloc, read);
}

pub fn pinInventoryDataFileObjectVersions(
    alloc: Allocator,
    source_client: object_storage.ObjectStorage,
    inventory: *external_source.Inventory,
) !void {
    if (inventory.format != .iceberg) return error.InvalidIcebergSnapshotRead;
    var client = source_client;
    client.allocator = alloc;
    for (inventory.files) |*file| {
        const location = try lake_range_io.objectLocationForUri(file.object_uri);
        var meta = try client.statObject(location.bucket, location.key);
        defer meta.deinit(alloc);
        if (meta.content_length != file.byte_len) return error.IcebergManifestLengthMismatch;

        const etag: []u8 = if (meta.etag) |value| try alloc.dupe(u8, value) else &.{};
        errdefer if (etag.len > 0) alloc.free(etag);
        const version_id: []u8 = if (meta.version_id) |value| try alloc.dupe(u8, value) else if (etag.len == 0) blk: {
            break :blk try std.fmt.allocPrint(
                alloc,
                "object-stat:v1:uri={s}:len={d}",
                .{ file.object_uri, meta.content_length },
            );
        } else &.{};
        errdefer if (version_id.len > 0) alloc.free(version_id);

        if (file.etag.len > 0) alloc.free(file.etag);
        if (file.version_id.len > 0) alloc.free(file.version_id);
        file.etag = etag;
        file.version_id = version_id;
    }
    try inventory.validate();
}

fn objectVersionForMetadataAlloc(
    alloc: Allocator,
    meta: object_storage.ObjectMetadata,
    uri: []const u8,
) !lake_range_io.ObjectVersion {
    const etag = if (meta.etag) |etag| try alloc.dupe(u8, etag) else &.{};
    errdefer if (etag.len > 0) alloc.free(etag);
    const version_id = if (meta.version_id) |version_id| try alloc.dupe(u8, version_id) else if (etag.len == 0) blk: {
        break :blk try std.fmt.allocPrint(
            alloc,
            "object-stat:v1:uri={s}:len={d}",
            .{ uri, meta.content_length },
        );
    } else &.{};
    errdefer if (version_id.len > 0) alloc.free(version_id);
    const version = lake_range_io.ObjectVersion{
        .etag = etag,
        .version_id = version_id,
    };
    try version.validate();
    return version;
}

test "iceberg snapshot reader plans inventory from object storage metadata and manifests" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var inventory = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, inventory.format);
    try std.testing.expectEqualStrings("events", inventory.source_id);
    try std.testing.expectEqualStrings("s3://bucket/t", inventory.source_uri);
    try std.testing.expectEqualStrings("12", inventory.snapshot_id);
    try std.testing.expectEqualStrings("iceberg-schema:7", inventory.schema_fingerprint);
    try std.testing.expectEqual(@as(usize, 2), inventory.files.len);
    try std.testing.expectEqualStrings("s3://bucket/t/data/a.parquet", inventory.files[0].file_id);
    try std.testing.expectEqualStrings("s3://bucket/t/data/b.parquet", inventory.files[1].file_id);
}

test "iceberg snapshot reader reuses cached metadata and manifest ranges" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);

    var first = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer first.deinit(alloc);
    const first_stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), first_stats.hits);
    try std.testing.expectEqual(@as(usize, 3), first_stats.misses);
    try std.testing.expect(first_stats.stored_bytes > 0);
    try std.testing.expectEqual(@as(usize, 3), first_stats.lane(.metadata).misses);
    try std.testing.expect(first_stats.lane(.metadata).stored_bytes > 0);

    var second = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer second.deinit(alloc);
    const second_stats = cache.statsSnapshot();
    try std.testing.expectEqual(first_stats.misses, second_stats.misses);
    try std.testing.expectEqual(@as(usize, 3), second_stats.hits);
    try std.testing.expectEqual(@as(usize, 3), second_stats.lane(.metadata).hits);
    try std.testing.expectEqualStrings(first.snapshot_id, second.snapshot_id);
    try std.testing.expectEqual(first.files.len, second.files.len);
}

test "iceberg snapshot reader applies object range cache metadata lane admission" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{
        .policy = lake_parquet_rowgroup.ObjectRangeCachePolicy.withLaneLimit(.metadata, 1),
    };
    defer cache.deinit(alloc);

    var inventory = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer inventory.deinit(alloc);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 3), stats.misses);
    try std.testing.expectEqual(@as(usize, 3), stats.lane(.metadata).misses);
    try std.testing.expectEqual(@as(usize, 0), stats.stored_bytes);
    try std.testing.expect(stats.rejected_bytes >= testMetadataJson().len + manifest_list.items.len + data_manifest.items.len);
    try std.testing.expectEqual(stats.rejected_bytes, stats.lane(.metadata).rejected_bytes);
}

test "iceberg snapshot reader rejects corrupted cached metadata range lengths" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);

    var meta = try client.statObject("bucket", "t/metadata/v1.metadata.json");
    defer meta.deinit(alloc);
    const version = try objectVersionForMetadataAlloc(alloc, meta, "s3://bucket/t/metadata/v1.metadata.json");
    defer {
        if (version.etag.len > 0) alloc.free(@constCast(version.etag));
        if (version.version_id.len > 0) alloc.free(@constCast(version.version_id));
    }
    const object = try lake_range_io.objectRefForIcebergUri(
        "s3://bucket/t/metadata/v1.metadata.json",
        meta.content_length,
        version,
    );
    const read = try lake_range_io.planIcebergManifestListRead(object);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);
    const cache_key = try read.cacheKeyAlloc(alloc);
    const corrupt_bytes = try alloc.dupe(u8, "{}");
    cache.entries.put(alloc, cache_key, .{
        .bytes = corrupt_bytes,
        .checksum = [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
    }) catch |err| {
        alloc.free(cache_key);
        alloc.free(corrupt_bytes);
        return err;
    };
    cache.stats.stored_bytes += corrupt_bytes.len;

    try std.testing.expectError(error.InvalidLakeRangeRead, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    }));
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 0), stats.misses);
}

test "iceberg snapshot reader rejects manifest length mismatches" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var manifest_list = try buildManifestListFixture(alloc, data_manifest.items.len + 1);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);

    try std.testing.expectError(error.IcebergManifestLengthMismatch, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    }));
}

test "iceberg snapshot reader reads delete manifest metadata before failing closed" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var delete_manifest = try buildDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDeleteManifestListFixture(alloc, delete_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);

    try std.testing.expectError(error.UnsupportedIcebergDeletes, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    }));
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.hits);
    try std.testing.expectEqual(@as(usize, 3), stats.misses);
    try std.testing.expectEqual(@as(usize, 3), stats.lane(.metadata).misses);
    try std.testing.expect(stats.stored_bytes >= testMetadataJson().len + manifest_list.items.len + delete_manifest.items.len);
}

test "iceberg snapshot reader plans active delete files" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var delete_manifest = try buildDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDeleteManifestListFixture(alloc, delete_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var plan = try readSnapshotDeletePlanAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.activeFileCount());
    try std.testing.expectEqual(@as(usize, 1), plan.activePositionDeleteFileCount());
    try std.testing.expectEqual(@as(usize, 0), plan.activeEqualityDeleteFileCount());
    try std.testing.expectEqual(iceberg_avro.DataFileContent.position_deletes, plan.files[0].content);
    try std.testing.expectEqualStrings("s3://bucket/t/deletes/pos-a.parquet", plan.files[0].file_path);
    try std.testing.expectEqualStrings("PARQUET", plan.files[0].file_format);
    try std.testing.expectEqual(@as(u64, 1), plan.files[0].record_count);
    try std.testing.expectEqual(@as(u64, 1024), plan.files[0].file_size_in_bytes);
}

test "iceberg snapshot reader plans equality delete files with equality ids" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJsonWithSchema(), .{});
    defer metadata_file.deinit(alloc);
    var delete_manifest = try buildEqualityDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDeleteManifestListFixture(alloc, delete_manifest.items.len);
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var plan = try readSnapshotDeletePlanAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.activeFileCount());
    try std.testing.expectEqual(@as(usize, 0), plan.activePositionDeleteFileCount());
    try std.testing.expectEqual(@as(usize, 1), plan.activeEqualityDeleteFileCount());
    try std.testing.expectEqual(iceberg_avro.DataFileContent.equality_deletes, plan.files[0].content);
    try std.testing.expectEqualStrings("s3://bucket/t/deletes/eq-a.parquet", plan.files[0].file_path);
    try std.testing.expectEqualStrings("PARQUET", plan.files[0].file_format);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2 }, plan.files[0].equality_ids);
    try std.testing.expectEqual(@as(usize, 2), plan.files[0].equality_columns.len);
    try std.testing.expectEqualStrings("tenant_id", plan.files[0].equality_columns[0]);
    try std.testing.expectEqualStrings("amount", plan.files[0].equality_columns[1]);
    try std.testing.expectEqual(@as(u64, 1), plan.files[0].record_count);
    try std.testing.expectEqual(@as(u64, 1024), plan.files[0].file_size_in_bytes);
}

test "iceberg snapshot reader scans position delete parquet files into row refs" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    const data_file_path = "s3://bucket/t/data/a.parquet";
    const delete_file_path = "s3://bucket/t/deletes/pos-a.parquet";
    const pos_columns = [_]lake_parquet_rowgroup.TestPlainI64Column{.{
        .column_id = "pos",
        .values = &[_]i64{ 0, 2 },
    }};
    const file_path_columns = [_]lake_parquet_rowgroup.TestPlainByteArrayColumn{.{
        .column_id = "file_path",
        .values = &[_][]const u8{ data_file_path, data_file_path },
    }};
    const delete_object = try lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(
        alloc,
        &pos_columns,
        &file_path_columns,
    );
    defer alloc.free(delete_object);
    var delete_put = try client.putObject("bucket", "t/deletes/pos-a.parquet", delete_object, .{});
    defer delete_put.deinit(alloc);

    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, data_file_path),
        .object_uri = try alloc.dupe(u8, data_file_path),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12"),
        .byte_len = 4096,
        .row_count = 3,
        .row_groups = try alloc.dupe(external_source.RowGroup, &[_]external_source.RowGroup{
            .{ .ordinal = 0, .row_count = 2 },
            .{ .ordinal = 1, .row_count = 1 },
        }),
    };

    var plan = IcebergDeletePlan{ .files = try alloc.alloc(IcebergDeleteFile, 1) };
    defer plan.deinit(alloc);
    plan.files[0] = .{
        .content = .position_deletes,
        .file_path = try alloc.dupe(u8, delete_file_path),
        .file_format = try alloc.dupe(u8, "PARQUET"),
        .snapshot_id = 12,
        .data_sequence_number = 42,
        .file_sequence_number = 43,
        .record_count = 2,
        .file_size_in_bytes = delete_object.len,
    };

    var range_reader = lake_object_reader.ObjectStorageRangeReader.init(client);
    const refs = try readPositionDeleteRowRefsAlloc(alloc, .{
        .reader = range_reader.parquetReader(),
        .client = client,
        .data_inventory = inventory,
        .delete_plan = plan,
    });
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);
    try std.testing.expectEqualStrings("events", refs[0].external.source_id);
    try std.testing.expectEqualStrings("12", refs[0].external.snapshot_id);
    try std.testing.expectEqualStrings(data_file_path, refs[0].external.file_id);
    try std.testing.expectEqual(@as(u32, 0), refs[0].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), refs[0].external.row_ordinal);
    try std.testing.expectEqual(@as(u32, 1), refs[1].external.row_group_ordinal);
    try std.testing.expectEqual(@as(u64, 0), refs[1].external.row_ordinal);
}

test "iceberg snapshot reader discovers data footers for equality deletes" {
    const alloc = std.testing.allocator;

    const data_file_path = "s3://bucket/t/data/a.parquet";
    const data_columns = [_]lake_parquet_rowgroup.TestPlainI64Column{.{
        .column_id = "amount",
        .values = &[_]i64{ 10, 20, 10 },
        .field_id = 1,
    }};
    const data_object = try lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(alloc, &data_columns);
    defer alloc.free(data_object);

    const delete_file_path = "s3://bucket/t/deletes/eq-a.parquet";
    const delete_columns = [_]lake_parquet_rowgroup.TestPlainI64Column{.{
        .column_id = "amount",
        .values = &[_]i64{10},
    }};
    const delete_object = try lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(alloc, &delete_columns);
    defer alloc.free(delete_object);

    const MemoryRangeReader = struct {
        data_body: []const u8,
        delete_body: []const u8,

        fn reader(self: *@This()) lake_parquet_rowgroup.ObjectRangeReader {
            return .{
                .ctx = self,
                .read_range_alloc = readRangeAlloc,
            };
        }

        fn readRangeAlloc(
            ctx: *anyopaque,
            a: Allocator,
            bucket: []const u8,
            key: []const u8,
            offset: u64,
            len: usize,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!std.mem.eql(u8, bucket, "bucket")) return error.ObjectNotFound;
            const body = if (std.mem.eql(u8, key, "t/data/a.parquet"))
                self.data_body
            else if (std.mem.eql(u8, key, "t/deletes/eq-a.parquet"))
                self.delete_body
            else
                return error.ObjectNotFound;
            const start: usize = std.math.cast(usize, offset) orelse return error.InvalidLakeRangeRead;
            if (start > body.len or len > body.len - start) return error.InvalidLakeRangeRead;
            return try a.dupe(u8, body[start..][0..len]);
        }
    };
    var range_reader = MemoryRangeReader{
        .data_body = data_object,
        .delete_body = delete_object,
    };

    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/t"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, data_file_path),
        .object_uri = try alloc.dupe(u8, data_file_path),
        .version_id = try alloc.dupe(u8, "iceberg:v1:data_seq=5:file_seq=6"),
        .byte_len = data_object.len,
        .row_count = 3,
        .data_sequence_number = 5,
        .row_groups = &.{},
    };

    var plan = IcebergDeletePlan{ .files = try alloc.alloc(IcebergDeleteFile, 1) };
    defer plan.deinit(alloc);
    plan.files[0] = .{
        .content = .equality_deletes,
        .file_path = try alloc.dupe(u8, delete_file_path),
        .file_format = try alloc.dupe(u8, "PARQUET"),
        .snapshot_id = 12,
        .data_sequence_number = 7,
        .file_sequence_number = 8,
        .equality_ids = try alloc.dupe(i32, &[_]i32{1}),
        .equality_columns = try alloc.alloc([]u8, 1),
        .record_count = 1,
        .file_size_in_bytes = delete_object.len,
    };
    plan.files[0].equality_columns[0] = try alloc.dupe(u8, "amount");

    const refs = try readDeleteRowRefsAlloc(alloc, .{
        .reader = range_reader.reader(),
        .data_inventory = inventory,
        .delete_plan = plan,
    });
    defer alloc.free(refs);

    try std.testing.expectEqual(@as(usize, 2), refs.len);
    try std.testing.expectEqualStrings(data_file_path, refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 0), refs[0].external.row_ordinal);
    try std.testing.expectEqualStrings(data_file_path, refs[1].external.file_id);
    try std.testing.expectEqual(@as(u64, 2), refs[1].external.row_ordinal);
}

test "iceberg snapshot reader ignores inactive delete manifests" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var delete_manifest = try buildInactiveDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDataAndInactiveDeleteManifestListFixture(
        alloc,
        data_manifest.items.len,
        delete_manifest.items.len,
    );
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var data_manifest_put = try client.putObject("bucket", "t/metadata/m-a.avro", data_manifest.items, .{});
    defer data_manifest_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var cache = lake_parquet_rowgroup.ObjectRangeCache{};
    defer cache.deinit(alloc);

    var inventory = try readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
        .cache = &cache,
    });
    defer inventory.deinit(alloc);

    try std.testing.expectEqual(external_source.Format.iceberg, inventory.format);
    try std.testing.expectEqual(@as(usize, 2), inventory.files.len);
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(usize, 4), stats.misses);
    try std.testing.expectEqual(@as(usize, 4), stats.lane(.metadata).misses);
}

test "iceberg snapshot reader delete plan ignores inactive delete files" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");

    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);
    var data_manifest = try buildDataManifestFixture(alloc);
    defer data_manifest.deinit(alloc);
    var delete_manifest = try buildInactiveDeleteManifestFixture(alloc);
    defer delete_manifest.deinit(alloc);
    var manifest_list = try buildDataAndInactiveDeleteManifestListFixture(
        alloc,
        data_manifest.items.len,
        delete_manifest.items.len,
    );
    defer manifest_list.deinit(alloc);
    var manifest_list_put = try client.putObject("bucket", "t/metadata/snap-12.avro", manifest_list.items, .{});
    defer manifest_list_put.deinit(alloc);
    var delete_manifest_put = try client.putObject("bucket", "t/metadata/d-a.avro", delete_manifest.items, .{});
    defer delete_manifest_put.deinit(alloc);

    var plan = try readSnapshotDeletePlanAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "12",
    });
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), plan.activeFileCount());
    try std.testing.expectEqual(@as(usize, 0), plan.activePositionDeleteFileCount());
    try std.testing.expectEqual(@as(usize, 0), plan.activeEqualityDeleteFileCount());
}

test "iceberg snapshot reader rejects requested snapshot mismatch" {
    const alloc = std.testing.allocator;
    var memory = object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var metadata_file = try client.putObject("bucket", "t/metadata/v1.metadata.json", testMetadataJson(), .{});
    defer metadata_file.deinit(alloc);

    try std.testing.expectError(error.IcebergSnapshotMismatch, readSnapshotInventoryAlloc(alloc, .{
        .client = client,
        .source_id = "events",
        .metadata_uri = "s3://bucket/t/metadata/v1.metadata.json",
        .requested_snapshot_id = "11",
    }));
}

fn testMetadataJson() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/t",
    \\  "current-schema-id": 7,
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [
    \\    {
    \\      "snapshot-id": 12,
    \\      "sequence-number": 42,
    \\      "timestamp-ms": 1700000000000,
    \\      "manifest-list": "s3://bucket/t/metadata/snap-12.avro"
    \\    }
    \\  ]
    \\}
    ;
}

fn testMetadataJsonWithSchema() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/t",
    \\  "current-schema-id": 7,
    \\  "schemas": [
    \\    {
    \\      "schema-id": 7,
    \\      "fields": [
    \\        {"id": 1, "name": "tenant_id", "required": true, "type": "string"},
    \\        {"id": 2, "name": "amount", "required": false, "type": "long"}
    \\      ]
    \\    }
    \\  ],
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [
    \\    {
    \\      "snapshot-id": 12,
    \\      "sequence-number": 42,
    \\      "timestamp-ms": 1700000000000,
    \\      "manifest-list": "s3://bucket/t/metadata/snap-12.avro"
    \\    }
    \\  ]
    \\}
    ;
}

fn buildManifestListFixture(alloc: Allocator, manifest_length: usize) !std.ArrayListUnmanaged(u8) {
    return try buildOneManifestListFixture(alloc, "s3://bucket/t/metadata/m-a.avro", .data, manifest_length, .{
        .added_files = 0,
        .existing_files = 0,
        .deleted_files = 0,
        .added_rows = 0,
        .existing_rows = 0,
        .deleted_rows = 0,
    });
}

fn buildDeleteManifestListFixture(alloc: Allocator, manifest_length: usize) !std.ArrayListUnmanaged(u8) {
    return try buildOneManifestListFixture(alloc, "s3://bucket/t/metadata/d-a.avro", .deletes, manifest_length, .{
        .added_files = 1,
        .existing_files = 0,
        .deleted_files = 0,
        .added_rows = 1,
        .existing_rows = 0,
        .deleted_rows = 0,
    });
}

fn buildDataAndInactiveDeleteManifestListFixture(
    alloc: Allocator,
    data_manifest_length: usize,
    delete_manifest_length: usize,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, manifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendManifestListRecord(alloc, &block, "s3://bucket/t/metadata/m-a.avro", .data, data_manifest_length, .{
        .added_files = 0,
        .existing_files = 0,
        .deleted_files = 0,
        .added_rows = 0,
        .existing_rows = 0,
        .deleted_rows = 0,
    });
    try appendManifestListRecord(alloc, &block, "s3://bucket/t/metadata/d-a.avro", .deletes, delete_manifest_length, .{
        .added_files = 0,
        .existing_files = 0,
        .deleted_files = 1,
        .added_rows = 0,
        .existing_rows = 0,
        .deleted_rows = 1,
    });

    try appendAvroBlock(alloc, &out, block.items, 2, "0123456789abcdef");
    return out;
}

const ManifestSummaryFixture = struct {
    added_files: i64,
    existing_files: i64,
    deleted_files: i64,
    added_rows: i64,
    existing_rows: i64,
    deleted_rows: i64,
};

fn buildOneManifestListFixture(
    alloc: Allocator,
    manifest_path: []const u8,
    content: iceberg_avro.ManifestContent,
    manifest_length: usize,
    summary: ManifestSummaryFixture,
) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, manifestListSchema(), "0123456789abcdef");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendManifestListRecord(alloc, &block, manifest_path, content, manifest_length, summary);

    try appendAvroBlock(alloc, &out, block.items, 1, "0123456789abcdef");
    return out;
}

fn appendManifestListRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    manifest_path: []const u8,
    content: iceberg_avro.ManifestContent,
    manifest_length: usize,
    summary: ManifestSummaryFixture,
) !void {
    try appendString(alloc, out, manifest_path);
    try appendLong(alloc, out, @intCast(manifest_length));
    try appendLong(alloc, out, 0);
    try appendLong(alloc, out, @intFromEnum(content));
    try appendLong(alloc, out, 42);
    try appendLong(alloc, out, summary.added_files);
    try appendLong(alloc, out, summary.existing_files);
    try appendLong(alloc, out, summary.deleted_files);
    try appendLong(alloc, out, summary.added_rows);
    try appendLong(alloc, out, summary.existing_rows);
    try appendLong(alloc, out, summary.deleted_rows);
}

fn buildDataManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, dataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .added, .data, "s3://bucket/t/data/a.parquet", 3, 4096);
    try appendDataManifestRecord(alloc, &block, .existing, .data, "s3://bucket/t/data/b.parquet", 2, 2048);

    try appendAvroBlock(alloc, &out, block.items, 2, "fedcba9876543210");
    return out;
}

fn buildDeleteManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, dataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .added, .position_deletes, "s3://bucket/t/deletes/pos-a.parquet", 1, 1024);

    try appendAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn buildEqualityDeleteManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, equalityDeleteManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecordWithEqualityIds(
        alloc,
        &block,
        .added,
        "s3://bucket/t/deletes/eq-a.parquet",
        1,
        1024,
        &[_]i32{ 1, 2 },
    );

    try appendAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn buildInactiveDeleteManifestFixture(alloc: Allocator) !std.ArrayListUnmanaged(u8) {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendAvroHeader(alloc, &out, dataManifestSchema(), "fedcba9876543210");

    var block = std.ArrayListUnmanaged(u8).empty;
    defer block.deinit(alloc);
    try appendDataManifestRecord(alloc, &block, .deleted, .position_deletes, "s3://bucket/t/deletes/pos-a.parquet", 1, 1024);

    try appendAvroBlock(alloc, &out, block.items, 1, "fedcba9876543210");
    return out;
}

fn manifestListSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_file","fields":[
    \\{"name":"manifest_path","type":"string"},
    \\{"name":"manifest_length","type":"long"},
    \\{"name":"partition_spec_id","type":"int"},
    \\{"name":"content","type":"int"},
    \\{"name":"sequence_number","type":"long"},
    \\{"name":"added_files_count","type":"int"},
    \\{"name":"existing_files_count","type":"int"},
    \\{"name":"deleted_files_count","type":"int"},
    \\{"name":"added_rows_count","type":"long"},
    \\{"name":"existing_rows_count","type":"long"},
    \\{"name":"deleted_rows_count","type":"long"}]}
    ;
}

fn dataManifestSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"}]}}]}
    ;
}

fn equalityDeleteManifestSchema() []const u8 {
    return
    \\{"type":"record","name":"manifest_entry","fields":[
    \\{"name":"status","type":"int"},
    \\{"name":"snapshot_id","type":["null","long"]},
    \\{"name":"data_sequence_number","type":["null","long"]},
    \\{"name":"file_sequence_number","type":["null","long"]},
    \\{"name":"data_file","type":{"type":"record","name":"data_file","fields":[
    \\{"name":"content","type":"int"},
    \\{"name":"file_path","type":"string"},
    \\{"name":"file_format","type":"string"},
    \\{"name":"record_count","type":"long"},
    \\{"name":"file_size_in_bytes","type":"long"},
    \\{"name":"equality_ids","type":{"type":"array","items":"int"}}]}}]}
    ;
}

fn appendDataManifestRecord(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: iceberg_avro.ManifestEntryStatus,
    content: iceberg_avro.DataFileContent,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
) !void {
    try appendLong(alloc, out, @intFromEnum(status));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 12);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 42);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 43);
    try appendLong(alloc, out, @intFromEnum(content));
    try appendString(alloc, out, file_path);
    try appendString(alloc, out, "PARQUET");
    try appendLong(alloc, out, record_count);
    try appendLong(alloc, out, file_size_in_bytes);
}

fn appendDataManifestRecordWithEqualityIds(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: iceberg_avro.ManifestEntryStatus,
    file_path: []const u8,
    record_count: i64,
    file_size_in_bytes: i64,
    equality_ids: []const i32,
) !void {
    try appendLong(alloc, out, @intFromEnum(status));
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 12);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 42);
    try appendLong(alloc, out, 1);
    try appendLong(alloc, out, 43);
    try appendLong(alloc, out, @intFromEnum(iceberg_avro.DataFileContent.equality_deletes));
    try appendString(alloc, out, file_path);
    try appendString(alloc, out, "PARQUET");
    try appendLong(alloc, out, record_count);
    try appendLong(alloc, out, file_size_in_bytes);
    try appendArrayInts(alloc, out, equality_ids);
}

fn appendArrayInts(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), values: []const i32) !void {
    if (values.len > 0) {
        try appendLong(alloc, out, @intCast(values.len));
        for (values) |value| try appendLong(alloc, out, value);
    }
    try appendLong(alloc, out, 0);
}

fn appendAvroHeader(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: []const u8,
    sync: []const u8,
) !void {
    try out.appendSlice(alloc, "Obj\x01");
    try appendLong(alloc, out, 2);
    try appendString(alloc, out, "avro.schema");
    try appendBytes(alloc, out, schema);
    try appendString(alloc, out, "avro.codec");
    try appendBytes(alloc, out, "null");
    try appendLong(alloc, out, 0);
    try out.appendSlice(alloc, sync);
}

fn appendAvroBlock(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    block: []const u8,
    count: i64,
    sync: []const u8,
) !void {
    try appendLong(alloc, out, count);
    try appendLong(alloc, out, @intCast(block.len));
    try out.appendSlice(alloc, block);
    try out.appendSlice(alloc, sync);
}

fn appendBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    try appendLong(alloc, out, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn appendString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    try appendBytes(alloc, out, text);
}

fn appendLong(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: i64) !void {
    const raw = encodeZigzag(value);
    var remaining = raw;
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @intCast(remaining & 0x7f)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @intCast(remaining));
}

fn encodeZigzag(value: i64) u64 {
    if (value >= 0) return @as(u64, @intCast(value)) << 1;
    const magnitude: u64 = @intCast(-(value + 1));
    return (magnitude << 1) | 1;
}
