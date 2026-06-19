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
const antfly = @import("antfly-zig");

const Allocator = std.mem.Allocator;
const db_mod = antfly.db;
const db_types = db_mod.types;
const backups_api = antfly.public_api.backups;
const portable_backup = antfly.portable_backup;
const group_ids = antfly.common.group_ids;

pub const max_afb_file_bytes: usize = 16 * 1024 * 1024 * 1024;

const LiteDb = struct {
    backend: antfly.lite.backend.Handle,
    db: db_mod.DB,

    fn open(allocator: Allocator, path: []const u8, open_mode: db_mod.OpenOptions.OpenMode) !LiteDb {
        var backend = try antfly.lite.backend.Handle.open(allocator, path, .{
            .read_only = open_mode == .query_readonly or open_mode == .status_only,
        });
        errdefer backend.deinit();

        var opts = db_mod.OpenOptions{
            .open_mode = open_mode,
            .external_derived_checkpoints = false,
        };
        try backend.configureDbOpenOptions(&opts);

        return .{
            .backend = backend,
            .db = try db_mod.DB.open(allocator, path, opts),
        };
    }

    fn close(self: *LiteDb) void {
        self.db.close();
        self.backend.deinit();
        self.* = undefined;
    }
};

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

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}.afb", .{backup_id});
    errdefer allocator.free(snapshot_path);
    try backups_api.writeFileToLocation(allocator, &location, snapshot_path, portable.items, "application/vnd.antfly.backup");

    const manifest = try promoteManifest(allocator, &lite.db, table_name, backup_id, snapshot_path);
    defer freeManifest(allocator, manifest);
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

    const snapshot_path = try std.fmt.allocPrint(allocator, "{s}.afb", .{backup_id});
    errdefer allocator.free(snapshot_path);
    try backups_api.writeFileToLocation(allocator, &location, snapshot_path, portable, "application/vnd.antfly.backup");

    const manifest = try portableFileManifest(allocator, table_name, backup_id, snapshot_path);
    defer freeManifest(allocator, manifest);
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
) !backups_api.TableBackupManifest {
    const schema_json = (try db.getSchemaJson(allocator)) orelse try allocator.dupe(u8, "{}");
    errdefer allocator.free(schema_json);
    const indexes_json = try indexesObjectJson(allocator, db);
    errdefer allocator.free(indexes_json);
    return try manifestFromParts(allocator, table_name, backup_id, snapshot_path, schema_json, indexes_json, "Promoted from Antfly Lite");
}

fn portableFileManifest(
    allocator: Allocator,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
) !backups_api.TableBackupManifest {
    const schema_json = try allocator.dupe(u8, "{}");
    errdefer allocator.free(schema_json);
    const indexes_json = try allocator.dupe(u8, "{}");
    errdefer allocator.free(indexes_json);
    return try manifestFromParts(allocator, table_name, backup_id, snapshot_path, schema_json, indexes_json, "Restored from portable Antfly backup");
}

fn manifestFromParts(
    allocator: Allocator,
    table_name: []const u8,
    backup_id: []const u8,
    snapshot_path: []const u8,
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

    return .{
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

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.append(allocator, '{');
    for (configs, 0..) |cfg, i| {
        if (i > 0) try out.append(allocator, ',');
        try appendJsonString(allocator, &out, cfg.name);
        try out.append(allocator, ':');
        const encoded = try std.json.Stringify.valueAlloc(allocator, cfg, .{});
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
