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

const std = @import("std");
const builtin = @import("builtin");
const platform_time = @import("antfly_platform").time;

const Allocator = std.mem.Allocator;
const backup_codec = @import("../backup_codec.zig");
const backups_api = @import("../../api/backups.zig");
const connection = @import("connection.zig");
const db_mod = @import("../db/db.zig");
const db_types = @import("../db/types.zig");
const group_ids = @import("../../common/group_ids.zig");
const internal_keys = @import("../internal_keys.zig");
const portable_backup = @import("../portable_backup.zig");
const query_api = @import("../../api/query.zig");
const tables_api = @import("../../api/tables.zig");
const table_writes = @import("../../api/table_writes.zig");
const fs_paths = @import("../../common/fs_paths.zig");

pub const max_afb_file_bytes: usize = 16 * 1024 * 1024 * 1024;

const LiteDb = connection.Connection;
var portable_generation_nonce: u64 = 0;
const portable_generation_lease_magic = "antfly-lite-portable-generation-v1\n";
var test_fail_published_file_directory_sync: std.atomic.Value(bool) = .init(false);

const PreparedGenerationAdoption = struct {
    live: *@import("backend.zig").Handle,
    prepared: *@import("backend.zig").Handle,

    fn adopt(context: PreparedGenerationAdoption) !db_mod.DB.PortableGenerationPublication {
        return switch (try context.live.replaceWithPreparedGeneration(context.prepared)) {
            .complete => .complete,
            .durability_unknown => .durability_unknown,
        };
    }
};

/// Confirms that a previously renamed Lite file is crash-durable. A failure
/// occurs after the rename commit point and must therefore be translated by
/// callers to `DurabilityOutcomeUnknown`, never to a normal retryable error.
pub fn confirmPublishedFileDurability(io: std.Io, path: []const u8) !void {
    if (builtin.is_test and test_fail_published_file_directory_sync.swap(false, .acq_rel)) {
        return error.InjectedPublishedFileDirectorySyncFailure;
    }
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

pub fn failNextPublishedFileDirectorySyncForTest() void {
    std.debug.assert(builtin.is_test);
    test_fail_published_file_directory_sync.store(true, .release);
}

pub fn isImportTargetEmpty(allocator: Allocator, db: *db_mod.DB) !bool {
    return try db.isPortableImportTargetEmpty(allocator);
}

pub fn finalizeRestoredLiteDb(allocator: Allocator, db: *db_mod.DB) !void {
    _ = try db.rebuildDenseIndexesForTargetCoverage(allocator);
    _ = try db.rebuildSparseIndexesForTargetCoverage(allocator);
    try db.rebuildGraphIndexesForTargetCoverage(allocator);
    _ = try db.replayGeneratedEnrichmentsFromStoredDocs(allocator);
    try db.runUntilIdle();
    try db.sync(true);
    try db.syncIndexes(true);
}

/// Populates a disposable Lite generation. All archive writes are bounded by
/// portable block size; callers must delete the file if this returns an error.
pub fn populateUnpublishedLiteDb(allocator: Allocator, db: *db_mod.DB, backup: []const u8) !void {
    try db.importPortableIntoUnpublishedEmpty(allocator, backup, connection.embeddedRootIdentity());
    try finalizeRestoredLiteDb(allocator, db);
}

/// File-backed population path for bounded-memory CLI restores.
pub fn populateUnpublishedLiteDbFromPortableFile(
    allocator: Allocator,
    db: *db_mod.DB,
    io: std.Io,
    file: std.Io.File,
    file_size: u64,
) !void {
    try db.importPortableFileIntoUnpublishedEmpty(
        allocator,
        io,
        file,
        file_size,
        connection.embeddedRootIdentity(),
    );
    try finalizeRestoredLiteDb(allocator, db);
}

/// Builds and finalizes a complete sibling generation, then atomically adopts
/// it into an already-open, strictly pristine Lite DB. All expensive/failure-
/// prone index and enrichment construction precedes the publication boundary.
pub fn importPortableIntoLiteDb(
    allocator: Allocator,
    db: *db_mod.DB,
    lite_backend: *@import("backend.zig").Handle,
    backup: []const u8,
) !void {
    if (!(try db.isPortableImportTargetEmpty(allocator))) return error.LiteImportTargetNotEmpty;
    if (lite_backend.engine != .native_single_file) return error.UnsupportedOperation;

    const target_path = lite_backend.native_docstore.?.file.path;
    const nonce = @atomicRmw(u64, &portable_generation_nonce, .Add, 1, .monotonic);
    const tmp_path = try std.fmt.allocPrint(
        allocator,
        "{s}.portable-restore-{d}-{x}.aflite",
        .{ target_path, platform_time.monotonicNs(), nonce },
    );
    defer allocator.free(tmp_path);

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    scavengePortableRestoreGenerations(allocator, io, target_path) catch |err| {
        std.log.warn("Lite portable restore scavenging failed target={s} class={s}", .{ target_path, @errorName(err) });
    };
    deleteFileIfExists(io, tmp_path) catch {};
    const tmp_lock_path = try std.fmt.allocPrint(allocator, "{s}.lock", .{tmp_path});
    defer allocator.free(tmp_lock_path);
    defer deleteFileIfExists(io, tmp_lock_path) catch {};
    errdefer deleteFileIfExists(io, tmp_path) catch {};

    var prepared = try LiteDb.createWithOptions(allocator, tmp_path, true, .{
        .fsync = !lite_backend.native_docstore.?.file.no_sync,
        .writer_lock_marker = portable_generation_lease_magic,
    });
    var prepared_live = true;
    errdefer if (prepared_live) prepared.close();
    try populateUnpublishedLiteDb(allocator, &prepared.db, backup);
    var prepared_runtime = try db.preparePortableRuntimeMetadata(
        prepared.db.core.store,
        connection.embeddedRootIdentity(),
    );
    defer prepared_runtime.deinit();

    // Quiesce the prepared runtime before moving its native file descriptor.
    // Finalization above already synced both primary and derived state.
    prepared.db.close();
    var prepared_backend = prepared.backend;
    prepared_live = false;
    prepared = undefined;
    defer prepared_backend.deinit();

    try db.adoptPreparedPortableGenerationIfEmpty(
        allocator,
        &prepared_runtime,
        PreparedGenerationAdoption{
            .live = lite_backend,
            .prepared = &prepared_backend,
        },
        PreparedGenerationAdoption.adopt,
    );
}

pub const StagedRestore = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    table_name: []const u8,

    pub fn deinit(self: *StagedRestore, allocator: Allocator) void {
        allocator.free(self.backup_id);
        allocator.free(self.location);
        allocator.free(self.snapshot_path);
        allocator.free(self.table_name);
        self.* = undefined;
    }
};

const PortableManifestMetadata = struct {
    schema_json: []const u8,
    indexes_json: []const u8,

    fn deinit(self: *PortableManifestMetadata, allocator: Allocator) void {
        allocator.free(self.schema_json);
        allocator.free(self.indexes_json);
        self.* = undefined;
    }
};

pub fn defaultBackupIdAlloc(allocator: Allocator, path: []const u8) ![]u8 {
    const base = std.fs.path.basename(path);
    const stem = if (std.mem.endsWith(u8, base, ".aflite"))
        base[0 .. base.len - ".aflite".len]
    else if (std.mem.endsWith(u8, base, ".afb"))
        base[0 .. base.len - ".afb".len]
    else
        base;
    var out = try std.ArrayListUnmanaged(u8).initCapacity(allocator, "lite-".len + stem.len);
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "lite-");
    for (stem) |byte| {
        try out.append(allocator, if (std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_') byte else '-');
    }
    return try out.toOwnedSlice(allocator);
}

pub fn stageInputRestoreBackup(
    allocator: Allocator,
    input_path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    if (std.mem.endsWith(u8, input_path, ".aflite")) {
        return try stageAfliteRestoreBackup(allocator, input_path, table_name, backup_id, location_uri);
    }
    if (std.mem.endsWith(u8, input_path, ".afb")) {
        return try stageAfbRestoreBackup(allocator, input_path, table_name, backup_id, location_uri);
    }
    return error.InvalidArguments;
}

pub fn stageAfliteRestoreBackup(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    if (!std.mem.endsWith(u8, path, ".aflite")) return error.InvalidArguments;
    var location = try backups_api.openBackupLocation(allocator, location_uri);
    defer location.deinit(allocator);

    var lite = try LiteDb.open(allocator, path, .query_readonly);
    defer lite.close();

    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);
    try portable_backup.exportPortable(allocator, lite.db.core.store, &portable);
    try portable_backup.validatePortable(allocator, portable.items);

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}.afb", .{backup_id});
    errdefer allocator.free(snapshot_path);
    const manifest = try promoteManifest(allocator, &lite.db, table_name, backup_id, snapshot_path, portable.items);
    defer freeManifest(allocator, manifest);

    try backups_api.writeFileToLocation(allocator, &location, snapshot_path, portable.items, "application/vnd.antfly.backup");
    try backups_api.writeManifestToLocation(allocator, &location, &manifest);

    return .{
        .backup_id = try allocator.dupe(u8, backup_id),
        .location = try allocator.dupe(u8, location_uri),
        .snapshot_path = snapshot_path,
        .table_name = try allocator.dupe(u8, table_name),
    };
}

pub fn stageAfbRestoreBackup(
    allocator: Allocator,
    path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location_uri: []const u8,
) !StagedRestore {
    if (!std.mem.endsWith(u8, path, ".afb")) return error.InvalidArguments;
    var location = try backups_api.openBackupLocation(allocator, location_uri);
    defer location.deinit(allocator);

    const portable = try readFileAlloc(allocator, path, max_afb_file_bytes);
    defer allocator.free(portable);
    try portable_backup.validatePortable(allocator, portable);

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}.afb", .{backup_id});
    errdefer allocator.free(snapshot_path);
    const manifest = try portableFileManifest(allocator, portable, table_name, backup_id, snapshot_path);
    defer freeManifest(allocator, manifest);

    try backups_api.writeFileToLocation(allocator, &location, snapshot_path, portable, "application/vnd.antfly.backup");
    try backups_api.writeManifestToLocation(allocator, &location, &manifest);

    return .{
        .backup_id = try allocator.dupe(u8, backup_id),
        .location = try allocator.dupe(u8, location_uri),
        .snapshot_path = snapshot_path,
        .table_name = try allocator.dupe(u8, table_name),
    };
}

fn promoteManifest(
    allocator: Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
    portable: []const u8,
) !backups_api.TableBackupManifest {
    const schema_json = (try db.getSchemaJson(allocator)) orelse try allocator.dupe(u8, "{}");
    errdefer allocator.free(schema_json);
    const indexes_json = try indexesObjectJson(allocator, db);
    errdefer allocator.free(indexes_json);
    return try manifestFromParts(allocator, table_name, backup_id, snapshot_path, portable, schema_json, indexes_json, "Promoted from Antfly Lite");
}

fn portableFileManifest(
    allocator: Allocator,
    portable: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
) !backups_api.TableBackupManifest {
    const metadata = try portableManifestMetadataAlloc(allocator, portable);
    const schema_json = metadata.schema_json;
    errdefer allocator.free(schema_json);
    const indexes_json = metadata.indexes_json;
    errdefer allocator.free(indexes_json);
    return try manifestFromParts(allocator, table_name, backup_id, snapshot_path, portable, schema_json, indexes_json, "Restored from portable Antfly backup");
}

fn portableManifestMetadataAlloc(allocator: Allocator, portable: []const u8) !PortableManifestMetadata {
    var schema_json = try allocator.dupe(u8, "{}");
    errdefer allocator.free(schema_json);
    var raw_indexes_json: ?[]u8 = null;
    defer if (raw_indexes_json) |value| allocator.free(value);
    var raw_enrichments_json: ?[]u8 = null;
    defer if (raw_enrichments_json) |value| allocator.free(value);

    var reader = backup_codec.SliceReader.init(portable);
    _ = try reader.readHeader();
    while (reader.pos < reader.data.len) {
        const block = try reader.readBlock(allocator);
        defer allocator.free(block.payload);
        if (block.block_type != .metadata_batch) continue;

        const entries = try backup_codec.decodeKeyValueBatch(allocator, block.payload);
        defer freeKeyValueEntries(allocator, entries);
        for (entries) |entry| {
            if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:schema_json")) {
                allocator.free(schema_json);
                schema_json = try allocator.dupe(u8, entry.value);
            } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:indexes")) {
                if (raw_indexes_json) |value| allocator.free(value);
                raw_indexes_json = try allocator.dupe(u8, entry.value);
            } else if (std.mem.eql(u8, entry.key, "\x00\x00__metadata__:enrichments")) {
                if (raw_enrichments_json) |value| allocator.free(value);
                raw_enrichments_json = try allocator.dupe(u8, entry.value);
            }
        }
    }

    const indexes_json = try tableIndexesJsonFromPortableMetadata(allocator, raw_indexes_json, raw_enrichments_json);
    errdefer allocator.free(indexes_json);

    return .{
        .schema_json = schema_json,
        .indexes_json = indexes_json,
    };
}

fn freeKeyValueEntries(allocator: Allocator, entries: []backup_codec.KeyValueEntry) void {
    for (entries) |entry| {
        allocator.free(entry.key);
        allocator.free(entry.value);
    }
    allocator.free(entries);
}

fn tableIndexesJsonFromPortableMetadata(
    allocator: Allocator,
    raw_indexes_json: ?[]const u8,
    raw_enrichments_json: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.append(allocator, '{');
    var first = true;

    if (raw_indexes_json) |indexes_json| {
        if (std.mem.startsWith(u8, indexes_json, "AIDX")) {
            try appendBinaryIndexCatalogFields(allocator, &out, &first, indexes_json);
        } else {
            var parsed = try std.json.parseFromSlice(std.json.Value, allocator, indexes_json, .{});
            defer parsed.deinit();
            switch (parsed.value) {
                .object => |object| {
                    var it = object.iterator();
                    while (it.next()) |entry| {
                        if (raw_enrichments_json != null and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                        try appendJsonObjectField(allocator, &out, &first, entry.key_ptr.*, entry.value_ptr.*);
                    }
                },
                .array => |array| {
                    for (array.items) |item| {
                        const object = switch (item) {
                            .object => |object| object,
                            else => return error.InvalidBackupRequest,
                        };
                        const name_value = object.get("name") orelse return error.InvalidBackupRequest;
                        if (name_value != .string or name_value.string.len == 0) return error.InvalidBackupRequest;
                        try appendJsonObjectField(allocator, &out, &first, name_value.string, item);
                    }
                },
                else => return error.InvalidBackupRequest,
            }
        }
    }

    if (raw_enrichments_json) |enrichments_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, enrichments_json, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidBackupRequest;
        try appendJsonObjectField(allocator, &out, &first, "enrichments", parsed.value);
    }

    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn appendBinaryIndexCatalogFields(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    data: []const u8,
) !void {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "AIDX")) return error.InvalidBackupRequest;

    var pos: usize = 4;
    const version = try readCatalogU32(data, &pos);
    if (version != 1 and version != 2) return error.InvalidBackupRequest;
    const count = try readCatalogU32(data, &pos);

    for (0..count) |_| {
        const name = try readCatalogString(data, &pos);
        if (pos >= data.len) return error.InvalidBackupRequest;
        const kind_value = data[pos];
        pos += 1;
        const kind = indexKindFromCatalogByte(kind_value) orelse return error.InvalidBackupRequest;
        const config_json = try readCatalogString(data, &pos);
        const coverage_generation = if (version >= 2)
            try readCatalogU64(data, &pos)
        else
            internal_keys.derivedCoverageGeneration(config_json);

        if (!first.*) try out.append(allocator, ',');
        first.* = false;
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ":{\"name\":");
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ",\"kind\":");
        try appendJsonString(allocator, out, @tagName(kind));
        try out.appendSlice(allocator, ",\"config_json\":");
        try appendJsonString(allocator, out, config_json);
        try out.appendSlice(allocator, ",\"coverage_generation\":");
        try appendJsonU64(allocator, out, coverage_generation);
        try out.append(allocator, '}');
    }
}

fn indexKindFromCatalogByte(value: u8) ?db_types.IndexKind {
    return switch (value) {
        @intFromEnum(db_types.IndexKind.full_text) => .full_text,
        @intFromEnum(db_types.IndexKind.dense_vector) => .dense_vector,
        @intFromEnum(db_types.IndexKind.sparse_vector) => .sparse_vector,
        @intFromEnum(db_types.IndexKind.graph) => .graph,
        @intFromEnum(db_types.IndexKind.algebraic) => .algebraic,
        else => null,
    };
}

fn readCatalogU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return value;
}

fn readCatalogU64(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return value;
}

fn readCatalogString(data: []const u8, pos: *usize) ![]const u8 {
    const len = try readCatalogU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidBackupRequest;
    const value = data[pos.* .. pos.* + len];
    pos.* += len;
    return value;
}

fn appendJsonObjectField(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: std.json.Value,
) !void {
    if (!first.*) try out.append(allocator, ',');
    first.* = false;
    try appendJsonString(allocator, out, name);
    try out.append(allocator, ':');
    const encoded = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

fn manifestFromParts(
    allocator: Allocator,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
    portable: []const u8,
    schema_json: []const u8,
    indexes_json: []const u8,
    description: []const u8,
) !backups_api.TableBackupManifest {
    const shard = try allocator.alloc(backups_api.ShardSnapshot, 1);
    errdefer allocator.free(shard);
    shard[0] = .{
        .group_id = group_ids.dataGroupIdFromHash(1),
        .start_key = try allocator.dupe(u8, ""),
        .end_key = null,
        .snapshot_path = try allocator.dupe(u8, snapshot_path),
    };
    errdefer shard[0].deinit(allocator);
    var integrity = try backups_api.portableBytesIntegrityAlloc(allocator, portable);
    shard[0].artifact_size_bytes = integrity.size_bytes;
    shard[0].artifact_sha256 = integrity.sha256;
    integrity = undefined;

    return .{
        .format = .portable,
        .backup_id = try allocator.dupe(u8, backup_id),
        .table_name = try allocator.dupe(u8, table_name),
        .description = try allocator.dupe(u8, description),
        .schema_json = schema_json,
        .read_schema_json = try allocator.dupe(u8, ""),
        .indexes_json = indexes_json,
        .replication_sources_json = try allocator.dupe(u8, "[]"),
        .shards = shard,
    };
}

fn freeManifest(allocator: Allocator, manifest: backups_api.TableBackupManifest) void {
    var owned = manifest;
    owned.deinit(allocator);
}

fn indexesObjectJson(allocator: Allocator, db: *db_mod.DB) ![]u8 {
    const configs = try db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, configs);
    const enrichments = try db.listEnrichments(allocator);
    defer db_types.freeEnrichmentConfigs(allocator, enrichments);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.append(allocator, '{');
    var first = true;
    for (configs) |cfg| {
        if (!first) try out.append(allocator, ',');
        first = false;
        try appendJsonString(allocator, &out, cfg.name);
        try out.append(allocator, ':');
        const encoded = try std.json.Stringify.valueAlloc(allocator, cfg, .{});
        defer allocator.free(encoded);
        try out.appendSlice(allocator, encoded);
    }
    if (enrichments.len > 0) {
        if (!first) try out.append(allocator, ',');
        first = false;
        try appendJsonString(allocator, &out, "enrichments");
        try out.append(allocator, ':');
        const encoded = try std.json.Stringify.valueAlloc(allocator, enrichments, .{});
        defer allocator.free(encoded);
        try out.appendSlice(allocator, encoded);
    }
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn appendJsonString(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(escaped);
    try out.appendSlice(allocator, escaped);
}

fn appendJsonU64(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: u64,
) !void {
    const encoded = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

fn readFileAlloc(allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    if (!std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    }
    var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size > max_bytes or size > std.math.maxInt(usize)) return error.FileTooBig;
    var buf: [8192]u8 = undefined;
    var reader = file.reader(io, &buf);
    return try reader.interface.readAlloc(allocator, @intCast(size));
}

fn searchJson(allocator: Allocator, db: *db_mod.DB, body: []const u8) ![]u8 {
    var owned = try query_api.parsePublicQueryRequest(
        allocator,
        null,
        "docs",
        body,
    );
    defer owned.deinit(allocator);

    var result = try db.search(allocator, owned.req);
    defer result.deinit();

    var response = try query_api.encodeQueryResponses(
        allocator,
        "docs",
        owned.req,
        .{},
        result,
    );
    defer response.deinit(allocator);
    return try allocator.dupe(u8, response.json);
}

fn fileExists(io: std.Io, path: []const u8) bool {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.accessAbsolute(io, path, .{}) catch return false;
    } else {
        std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    }
    return true;
}

fn deleteFileIfExists(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.deleteFileAbsolute(io, path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    } else {
        std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

fn scavengePortableRestoreGenerations(
    allocator: Allocator,
    io: std.Io,
    target_path: []const u8,
) !void {
    const parent = std.fs.path.dirname(target_path) orelse ".";
    const target_name = std.fs.path.basename(target_path);
    const prefix = try std.fmt.allocPrint(allocator, "{s}.portable-restore-", .{target_name});
    defer allocator.free(prefix);

    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var iterator = dir.iterateAssumeFirstIteration();
    while (try iterator.next(io)) |entry| {
        // Scan leases rather than data files. The writer records the lease
        // marker before creating the native generation, so this also reaps a
        // process that died in that otherwise easy-to-miss interval.
        if (entry.kind != .file or
            !std.mem.startsWith(u8, entry.name, prefix) or
            !std.mem.endsWith(u8, entry.name, ".aflite.lock")) continue;

        const lease_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ parent, entry.name });
        defer allocator.free(lease_path);
        const candidate_name = entry.name[0 .. entry.name.len - ".lock".len];
        const candidate = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ parent, candidate_name });
        defer allocator.free(candidate);
        var lease = std.Io.Dir.cwd().openFile(io, lease_path, .{
            .mode = .read_write,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.AccessDenied, error.FileBusy, error.WouldBlock, error.FileLocksUnsupported, error.FileNotFound => continue,
            else => return err,
        };
        var lease_open = true;
        defer if (lease_open) lease.close(io);

        const stat = lease.stat(io) catch continue;
        if (stat.size != portable_generation_lease_magic.len) continue;
        var magic: [portable_generation_lease_magic.len]u8 = undefined;
        _ = lease.readPositionalAll(io, &magic, 0) catch continue;
        if (!std.mem.eql(u8, &magic, portable_generation_lease_magic)) continue;

        try deleteFileIfExists(io, candidate);
        lease.close(io);
        lease_open = false;
        try deleteFileIfExists(io, lease_path);
        std.log.info("removed abandoned Lite portable generation path={s}", .{candidate});
    }
}

fn freeBackupShards(allocator: Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(allocator);
    allocator.free(@constCast(shards));
}

test "lite restore staging writer close syncs unsynced batch before readonly reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-staging-close-sync.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var lite = try LiteDb.create(allocator, path, true);
        defer lite.close();

        try lite.db.batch(.{
            .writes = &.{.{
                .key = "doc:restore-staging-close-sync",
                .value = "{\"title\":\"restore staging close persists\"}",
            }},
            .sync_level = .propose,
        });
    }

    {
        var reopened = try LiteDb.open(allocator, path, .query_readonly);
        defer reopened.close();

        var result = (try reopened.db.lookup(allocator, "doc:restore-staging-close-sync", .{})) orelse return error.MissingRestoredLiteDocument;
        defer result.deinit(allocator);
        try std.testing.expect(std.mem.indexOf(u8, result.json, "\"restore staging close persists\"") != null);
    }
}

test "lite portable generation scavenging reaps stale data and lease-only crashes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(parent);
    const target = try std.fmt.allocPrint(allocator, "{s}/target.aflite", .{parent});
    defer allocator.free(target);
    const stale_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-stale.aflite", .{target});
    defer allocator.free(stale_data);
    const stale_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{stale_data});
    defer allocator.free(stale_lock);
    const lease_only_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-lease-only.aflite", .{target});
    defer allocator.free(lease_only_data);
    const lease_only_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{lease_only_data});
    defer allocator.free(lease_only_lock);
    const active_data = try std.fmt.allocPrint(allocator, "{s}.portable-restore-active.aflite", .{target});
    defer allocator.free(active_data);
    const active_lock = try std.fmt.allocPrint(allocator, "{s}.lock", .{active_data});
    defer allocator.free(active_lock);

    const io = std.testing.io;
    {
        var data = try std.Io.Dir.cwd().createFile(io, stale_data, .{ .truncate = true });
        data.close(io);
        var lease = try std.Io.Dir.cwd().createFile(io, stale_lock, .{ .read = true, .truncate = true });
        try lease.writePositionalAll(io, portable_generation_lease_magic, 0);
        lease.close(io);
    }
    {
        var lease = try std.Io.Dir.cwd().createFile(io, lease_only_lock, .{ .read = true, .truncate = true });
        try lease.writePositionalAll(io, portable_generation_lease_magic, 0);
        lease.close(io);
    }
    var active_lease = try std.Io.Dir.cwd().createFile(io, active_lock, .{
        .read = true,
        .truncate = true,
        .lock = .exclusive,
        .lock_nonblocking = true,
    });
    defer active_lease.close(io);
    try active_lease.writePositionalAll(io, portable_generation_lease_magic, 0);

    try scavengePortableRestoreGenerations(allocator, io, target);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, stale_data, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, stale_lock, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, lease_only_lock, .{}));
    try std.Io.Dir.cwd().access(io, active_lock, .{});
}

test "lite portable publication never reports a retryable failure after adoption" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/post-adopt-source.aflite", .{tmp.sub_path});
    defer allocator.free(source_path);
    const target_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/post-adopt-target.aflite", .{tmp.sub_path});
    defer allocator.free(target_path);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);

    {
        var source = try LiteDb.create(allocator, source_path, true);
        defer source.close();
        var index = try std.json.parseFromSlice(
            db_types.IndexConfig,
            allocator,
            "{\"name\":\"published_ft\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
            .{ .ignore_unknown_fields = true },
        );
        defer index.deinit();
        try source.db.addIndex(index.value);
        try source.db.batch(.{ .writes = &.{.{
            .key = "doc:published",
            .value = "{\"title\":\"published\"}",
        }} });
        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    var target = try LiteDb.create(allocator, target_path, true);
    defer target.close();
    db_mod.failNextPortableIndexActivationForTest();
    try importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items);

    const degraded = target.db.pendingWorkStats();
    try std.testing.expect(degraded.portable_runtime_activation_pending);
    try std.testing.expectEqual(@as(u64, 1), degraded.portable_runtime_activation_attempts);
    try std.testing.expectError(error.PortableRuntimeActivationPending, target.db.listIndexes(allocator));
    try std.testing.expectError(error.PortableRuntimeActivationPending, target.db.batch(.{ .writes = &.{.{
        .key = "doc:blocked-until-runtime-ready",
        .value = "{\"title\":\"blocked\"}",
    }} }));

    try target.db.runUntilIdle();
    const recovered_runtime = target.db.pendingWorkStats();
    try std.testing.expect(!recovered_runtime.portable_runtime_activation_pending);
    try std.testing.expectEqual(@as(u64, 2), recovered_runtime.portable_runtime_activation_attempts);
    const indexes = try target.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings("published_ft", indexes[0].name);

    var restored = (try target.db.lookup(allocator, "doc:published", .{})) orelse return error.MissingPublishedDocument;
    defer restored.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "published") != null);
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &target.db)));
}

test "lite portable publication reports durability unknown after adopting runtime state" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/durability-source.aflite", .{tmp.sub_path});
    defer allocator.free(source_path);
    const target_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/durability-target.aflite", .{tmp.sub_path});
    defer allocator.free(target_path);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);

    {
        var source = try LiteDb.create(allocator, source_path, true);
        defer source.close();
        var index = try std.json.parseFromSlice(
            db_types.IndexConfig,
            allocator,
            "{\"name\":\"durable_ft\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
            .{ .ignore_unknown_fields = true },
        );
        defer index.deinit();
        try source.db.addIndex(index.value);
        try source.db.batch(.{ .writes = &.{.{
            .key = "doc:published-unknown",
            .value = "{\"title\":\"published unknown\"}",
        }} });
        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    var target = try LiteDb.create(allocator, target_path, true);
    defer target.close();
    target.backend.native_docstore.?.file.failNextGenerationDirectorySyncForTest();
    try std.testing.expectError(
        error.DurabilityOutcomeUnknown,
        importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items),
    );

    // The error is explicitly non-retryable: the live storage and runtime
    // both adopted the committed generation before it was returned.
    try std.testing.expectError(
        error.LiteImportTargetNotEmpty,
        importPortableIntoLiteDb(allocator, &target.db, &target.backend, portable.items),
    );
    var restored = (try target.db.lookup(allocator, "doc:published-unknown", .{})) orelse
        return error.MissingPublishedDocument;
    defer restored.deinit(allocator);
    const indexes = try target.db.listIndexes(allocator);
    defer db_types.freeIndexConfigs(allocator, indexes);
    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqualStrings("durable_ft", indexes[0].name);
}

test "lite portable import emptiness rejects tombstones and durable transactions" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tombstone_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-tombstone.aflite", .{tmp.sub_path});
    defer allocator.free(tombstone_path);
    var tombstone_db = try LiteDb.create(allocator, tombstone_path, true);
    defer tombstone_db.close();
    try tombstone_db.db.batch(.{ .writes = &.{.{
        .key = "doc:deleted",
        .value = "{\"title\":\"deleted\"}",
    }} });
    try tombstone_db.db.batch(.{ .deletes = &.{"doc:deleted"} });
    try std.testing.expectEqual(@as(u64, 0), try tombstone_db.db.primaryDocCount(allocator));
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &tombstone_db.db)));

    const transaction_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/import-transaction.aflite", .{tmp.sub_path});
    defer allocator.free(transaction_path);
    var transaction_db = try LiteDb.create(allocator, transaction_path, true);
    defer transaction_db.close();
    const txn_id: db_types.TxnId = .{ 0x69, 0x6d, 0x70, 0x6f, 0x72, 0x74, 0x2d, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0, 0, 1 };
    _ = try transaction_db.db.beginTransactionWithId(txn_id, 8_000);
    try std.testing.expect(!(try isImportTargetEmpty(allocator, &transaction_db.db)));
}

test "lite restore staging preserves portable afb schema index and enrichment metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/portable-metadata-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const afb_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/portable-metadata.afb", .{tmp.sub_path});
    defer allocator.free(afb_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/portable-metadata-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json = "{\"name\":\"body_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128,\"chunk_overlap\":16}";
    const index_json = "{\"name\":\"ft_body\",\"kind\":\"full_text\",\"config_json\":\"{\\\"chunk_name\\\":\\\"body_chunks_v1\\\"}\"}";

    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(allocator);
    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
            .ignore_unknown_fields = true,
        });
        defer index.deinit();
        try source.db.addIndex(index.value);

        try portable_backup.exportPortable(allocator, source.db.core.store, &portable);
    }

    {
        var file = try std.Io.Dir.cwd().createFile(io, afb_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [8192]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(portable.items);
        try writer.end();
        try file.sync(io);
    }

    var staged = try stageAfbRestoreBackup(allocator, afb_path, "docs", "portable-metadata-test", location);
    defer staged.deinit(allocator);

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, "portable-metadata-test");
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"body_chunks_v1\"") != null);

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, allocator, manifest.indexes_json, .{});
    defer parsed_indexes.deinit();
    try std.testing.expect(parsed_indexes.value == .object);
    const ft_body = parsed_indexes.value.object.get("ft_body") orelse return error.TestExpectedEqual;
    try std.testing.expect(ft_body == .object);
    try std.testing.expect(ft_body.object.get("coverage_generation") != null);
    const enrichments = parsed_indexes.value.object.get("enrichments") orelse return error.TestExpectedEqual;
    try std.testing.expect(enrichments == .array);
    try std.testing.expectEqual(@as(usize, 1), enrichments.array.items.len);
}

test "lite restore staging preflights afb before publishing staged files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const malformed_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/malformed.afb", .{tmp.sub_path});
    defer allocator.free(malformed_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/malformed-staging-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);
    const staged_afb_path = try std.fmt.allocPrint(allocator, "{s}/bad-input.afb", .{backup_root});
    defer allocator.free(staged_afb_path);
    const staged_manifest_path = try backups_api.metadataPath(allocator, backup_root, "bad-input");
    defer allocator.free(staged_manifest_path);

    var malformed = std.ArrayList(u8).empty;
    defer malformed.deinit(allocator);
    try backup_codec.writeHeader(&malformed, allocator, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = [_]u8{0} ** 16,
        .table_count = 1,
        .shard_count = 1,
    });
    const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
    try backup_codec.writeBlock(&malformed, allocator, .document_batch, &malformed_doc_payload);
    {
        var file = try std.Io.Dir.cwd().createFile(io, malformed_path, .{ .truncate = true });
        defer file.close(io);
        try file.writePositionalAll(io, malformed.items, 0);
        try file.sync(io);
    }

    try std.testing.expectError(
        error.Truncated,
        stageAfbRestoreBackup(allocator, malformed_path, "docs", "bad-input", location),
    );
    try std.testing.expect(!fileExists(io, staged_afb_path));
    try std.testing.expect(!fileExists(io, staged_manifest_path));
}

test "lite restore staging accepts aflite input for normal restore" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/normal-restore-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const default_backup_id = try defaultBackupIdAlloc(allocator, src_path);
    defer allocator.free(default_backup_id);
    try std.testing.expectEqualStrings("lite-normal-restore-src", default_backup_id);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json = "{\"name\":\"restore_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":256,\"chunk_overlap\":32}";
    const index_jsons = [_][]const u8{
        "{\"name\":\"restore_ft_body\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        "{\"name\":\"restore_dense\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":2,\\\"metric\\\":\\\"l2_squared\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"restore_sparse\",\"kind\":\"sparse_vector\",\"config_json\":\"{\\\"field\\\":\\\"sparse_embedding\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"restore_graph\",\"kind\":\"graph\",\"config_json\":\"{}\"}",
    };

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        for (index_jsons) |index_json| {
            var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
                .ignore_unknown_fields = true,
            });
            defer index.deinit();
            try source.db.addIndex(index.value);
        }

        try source.db.batch(.{
            .writes = &.{
                .{
                    .key = "doc:restore:a",
                    .value = "{\"title\":\"restore alpha\",\"body\":\"direct aflite restore hybrid alpha\",\"_embeddings\":{\"restore_dense\":[1,0],\"restore_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"restore_graph\":{\"links\":[{\"target\":\"doc:restore:c\",\"weight\":1.0}]}}}",
                },
                .{
                    .key = "doc:restore:b",
                    .value = "{\"title\":\"restore beta\",\"body\":\"direct aflite restore hybrid beta\",\"_embeddings\":{\"restore_dense\":[0,1],\"restore_sparse\":{\"indices\":[99],\"values\":[2.0]}}}",
                },
                .{
                    .key = "doc:restore:c",
                    .value = "{\"title\":\"restore graph target\"}",
                },
            },
            .sync_level = .full_index,
        });
        try source.db.runUntilIdle();
    }

    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", default_backup_id, location);
    defer staged.deinit(allocator);

    try std.testing.expectEqualStrings(default_backup_id, staged.backup_id);
    try std.testing.expectEqualStrings(location, staged.location);
    try std.testing.expectEqualStrings("docs", staged.table_name);
    try std.testing.expectEqualStrings("lite-normal-restore-src.afb", staged.snapshot_path);

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, default_backup_id);
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings(default_backup_id, manifest.backup_id);
    try std.testing.expectEqualStrings(staged.snapshot_path, manifest.shards[0].snapshot_path);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_sparse\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_chunks_v1\"") != null);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/{s}.afb", .{ backup_root, default_backup_id });
    defer allocator.free(afb_path);
    const portable = try readFileAlloc(allocator, afb_path, max_afb_file_bytes);
    defer allocator.free(portable);
    try std.testing.expect(portable.len > 0);

    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-roundtrip.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    {
        var restored = try LiteDb.create(allocator, restored_path, true);
        defer restored.close();
        try populateUnpublishedLiteDb(allocator, &restored.db, portable);

        const dense = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"restore_dense\":[1,0]},\"indexes\":[\"restore_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:restore:a\"") != null);

        const sparse = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"restore_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"restore_sparse\"],\"limit\":1}",
        );
        defer allocator.free(sparse);
        try std.testing.expect(std.mem.indexOf(u8, sparse, "\"doc:restore:a\"") != null);

        const graph = try searchJson(
            allocator,
            &restored.db,
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"restore_graph\",\"start_nodes\":{\"keys\":[\"doc:restore:a\"]},\"params\":{\"edge_types\":[\"links\"]}}},\"limit\":10}",
        );
        defer allocator.free(graph);
        try std.testing.expect(std.mem.indexOf(u8, graph, "\"doc:restore:c\"") != null);

        const hybrid = try searchJson(
            allocator,
            &restored.db,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"restore_dense\":[1,0]},\"indexes\":[\"restore_dense\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer allocator.free(hybrid);
        try std.testing.expect(std.mem.indexOf(u8, hybrid, "\"doc:restore:a\"") != null);
    }
}

test "lite restore staging exports stable aflite data while writer has open transaction" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-active-writer.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-restore-active-writer-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/normal-restore-active-writer-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    var source = try LiteDb.create(allocator, src_path, true);
    defer source.close();

    try source.db.batch(.{
        .writes = &.{.{
            .key = "doc:restore-committed",
            .value = "{\"title\":\"normal restore committed\"}",
        }},
        .sync_level = .write,
    });

    const txn_id: db_types.TxnId = .{ 0x6c, 0x69, 0x74, 0x65, 0x2d, 0x72, 0x65, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2d, 0, 0, 1 };
    _ = try source.db.beginTransactionWithId(txn_id, 7_000);
    try source.db.writeTransaction(txn_id, .{
        .writes = &.{.{
            .key = "doc:restore-pending",
            .value = "{\"title\":\"normal restore pending\"}",
        }},
    });

    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", "active-writer", location);
    defer staged.deinit(allocator);
    try source.db.abortTransaction(txn_id, 7_001);

    try std.testing.expectEqualStrings("active-writer.afb", staged.snapshot_path);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/active-writer.afb", .{backup_root});
    defer allocator.free(afb_path);
    const portable = try readFileAlloc(allocator, afb_path, max_afb_file_bytes);
    defer allocator.free(portable);
    try portable_backup.validatePortable(allocator, portable);

    var restored = try LiteDb.create(allocator, restored_path, true);
    defer restored.close();
    try populateUnpublishedLiteDb(allocator, &restored.db, portable);

    var committed = (try restored.db.lookup(allocator, "doc:restore-committed", .{})) orelse return error.MissingRestoredCommittedDoc;
    defer committed.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, committed.json, "\"normal restore committed\"") != null);

    try std.testing.expectEqual(@as(?db_types.LookupResult, null), try restored.db.lookup(allocator, "doc:restore-pending", .{}));
}

test "lite portable backup roundtrips through normal table backup APIs" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-normal-lite-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const normal_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/normal-middle-db", .{tmp.sub_path});
    defer allocator.free(normal_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-normal-lite-restored.aflite", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/lite-normal-lite-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json: []const u8 =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    const enrichment_json: []const u8 = "{\"name\":\"roundtrip_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128,\"chunk_overlap\":16}";
    const index_jsons = [_][]const u8{
        "{\"name\":\"roundtrip_ft_body\",\"kind\":\"full_text\",\"config_json\":\"{}\"}",
        "{\"name\":\"roundtrip_dense\",\"kind\":\"dense_vector\",\"config_json\":\"{\\\"field\\\":\\\"embedding\\\",\\\"dims\\\":2,\\\"metric\\\":\\\"l2_squared\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"roundtrip_sparse\",\"kind\":\"sparse_vector\",\"config_json\":\"{\\\"field\\\":\\\"sparse_embedding\\\",\\\"external\\\":true}\"}",
        "{\"name\":\"roundtrip_graph\",\"kind\":\"graph\",\"config_json\":\"{}\"}",
    };

    {
        var source = try LiteDb.create(allocator, src_path, true);
        defer source.close();

        try source.db.setSchemaJson(allocator, schema_json);

        var enrichment = try std.json.parseFromSlice(db_types.EnrichmentConfig, allocator, enrichment_json, .{
            .ignore_unknown_fields = true,
        });
        defer enrichment.deinit();
        try source.db.addEnrichment(enrichment.value);

        for (index_jsons) |index_json| {
            var index = try std.json.parseFromSlice(db_types.IndexConfig, allocator, index_json, .{
                .ignore_unknown_fields = true,
            });
            defer index.deinit();
            try source.db.addIndex(index.value);
        }

        try source.db.batch(.{
            .writes = &.{
                .{
                    .key = "doc:roundtrip:a",
                    .value = "{\"title\":\"normal hop alpha\",\"body\":\"lite normal lite hybrid alpha\",\"_embeddings\":{\"roundtrip_dense\":[1,0],\"roundtrip_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"_edges\":{\"roundtrip_graph\":{\"links\":[{\"target\":\"doc:roundtrip:c\",\"weight\":1.0}]}}}",
                },
                .{
                    .key = "doc:roundtrip:b",
                    .value = "{\"title\":\"normal hop beta\",\"body\":\"lite normal lite hybrid beta\",\"_embeddings\":{\"roundtrip_dense\":[0,1],\"roundtrip_sparse\":{\"indices\":[99],\"values\":[2.0]}}}",
                },
                .{
                    .key = "doc:roundtrip:c",
                    .value = "{\"title\":\"normal hop graph target\"}",
                },
            },
            .sync_level = .full_index,
        });
        try source.db.runUntilIdle();
    }

    var staged = try stageInputRestoreBackup(allocator, src_path, "docs", "lite-normal-lite-in", location);
    defer staged.deinit(allocator);

    var backup_location = try backups_api.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var lite_manifest = try backups_api.readManifestFromLocation(allocator, &backup_location, "lite-normal-lite-in");
    defer lite_manifest.deinit(allocator);
    try std.testing.expectEqualStrings(schema_json, lite_manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, lite_manifest.indexes_json, "\"roundtrip_dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, lite_manifest.indexes_json, "\"roundtrip_chunks_v1\"") != null);

    var normal_db = try db_mod.DB.open(allocator, normal_path, .{});
    defer normal_db.close();
    var normal_source = table_writes.BoundTableWriteSource.init("docs", &normal_db);
    _ = try normal_source.source().restoreTable(allocator, "docs", .{
        .backup_root = backup_root,
        .manifest = &lite_manifest,
        .artifact_backup_id = lite_manifest.backup_id,
        .source_location = location,
    });
    _ = try normal_db.rebuildDenseIndexesForTargetCoverage(allocator);
    _ = try normal_db.rebuildSparseIndexesForTargetCoverage(allocator);
    try normal_db.rebuildGraphIndexesForTargetCoverage(allocator);
    _ = try normal_db.replayGeneratedEnrichmentsFromStoredDocs(allocator);
    try normal_db.runUntilIdle();

    {
        const dense = try searchJson(
            allocator,
            &normal_db,
            "{\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:roundtrip:a\"") != null);
    }

    const normal_shards = (try normal_source.source().backupTable(allocator, "docs", .{
        .backup_root = backup_root,
        .backup_id = "lite-normal-lite-out",
        .format = .portable,
    })).?;
    defer freeBackupShards(allocator, normal_shards);
    try std.testing.expectEqual(@as(usize, 1), normal_shards.len);
    try std.testing.expectEqualStrings("lite-normal-lite-out.afb", normal_shards[0].snapshot_path);

    const table_schema_json = try allocator.dupe(u8, schema_json);
    defer allocator.free(table_schema_json);
    const table_indexes_json = try allocator.dupe(u8, lite_manifest.indexes_json);
    defer allocator.free(table_indexes_json);
    const table = tables_api.deriveTableRecord("docs", .{
        .schema_json = table_schema_json,
        .indexes_json = table_indexes_json,
    });
    var normal_manifest = try backups_api.createManifest(allocator, "lite-normal-lite-out", .portable, &table, normal_shards);
    defer normal_manifest.deinit(allocator);
    try std.testing.expectEqualStrings(schema_json, normal_manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, normal_manifest.indexes_json, "\"roundtrip_graph\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, normal_manifest.indexes_json, "\"roundtrip_chunks_v1\"") != null);

    const normal_afb_path = try std.fmt.allocPrint(allocator, "{s}/lite-normal-lite-out.afb", .{backup_root});
    defer allocator.free(normal_afb_path);
    const normal_portable = try readFileAlloc(allocator, normal_afb_path, max_afb_file_bytes);
    defer allocator.free(normal_portable);
    try portable_backup.validatePortable(allocator, normal_portable);

    {
        var restored = try LiteDb.create(allocator, restored_path, true);
        defer restored.close();

        try populateUnpublishedLiteDb(allocator, &restored.db, normal_portable);

        const schema = (try restored.db.getSchemaJson(allocator)) orelse return error.MissingLiteSchemaJson;
        defer allocator.free(schema);
        try std.testing.expectEqualStrings(schema_json, schema);

        const indexes = try restored.db.listIndexes(allocator);
        defer db_types.freeIndexConfigs(allocator, indexes);
        try std.testing.expectEqual(@as(usize, 4), indexes.len);

        const enrichments = try restored.db.listEnrichments(allocator);
        defer db_types.freeEnrichmentConfigs(allocator, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("roundtrip_chunks_v1", enrichments[0].name);

        const dense = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"limit\":1}",
        );
        defer allocator.free(dense);
        try std.testing.expect(std.mem.indexOf(u8, dense, "\"doc:roundtrip:a\"") != null);

        const sparse = try searchJson(
            allocator,
            &restored.db,
            "{\"embeddings\":{\"roundtrip_sparse\":{\"indices\":[7,42],\"values\":[1.5,0.5]}},\"indexes\":[\"roundtrip_sparse\"],\"limit\":1}",
        );
        defer allocator.free(sparse);
        try std.testing.expect(std.mem.indexOf(u8, sparse, "\"doc:roundtrip:a\"") != null);

        const graph = try searchJson(
            allocator,
            &restored.db,
            "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"roundtrip_graph\",\"start_nodes\":{\"keys\":[\"doc:roundtrip:a\"]},\"params\":{\"edge_types\":[\"links\"]}}},\"limit\":10}",
        );
        defer allocator.free(graph);
        try std.testing.expect(std.mem.indexOf(u8, graph, "\"doc:roundtrip:c\"") != null);

        const hybrid = try searchJson(
            allocator,
            &restored.db,
            "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"hybrid alpha\"}},\"embeddings\":{\"roundtrip_dense\":[1,0]},\"indexes\":[\"roundtrip_dense\"],\"merge_config\":{\"strategy\":\"rrf\"},\"limit\":3}",
        );
        defer allocator.free(hybrid);
        try std.testing.expect(std.mem.indexOf(u8, hybrid, "\"doc:roundtrip:a\"") != null);
    }
}
