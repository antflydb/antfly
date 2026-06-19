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
const backup_codec = antfly.backup_codec;
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

    const manifest = try portableFileManifest(allocator, portable, table_name, backup_id, snapshot_path);
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
    return try manifestFromParts(allocator, table_name, backup_id, snapshot_path, schema_json, indexes_json, "Restored from portable Antfly backup");
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
    if (version != 1) return error.InvalidBackupRequest;
    const count = try readCatalogU32(data, &pos);

    for (0..count) |_| {
        const name = try readCatalogString(data, &pos);
        if (pos >= data.len) return error.InvalidBackupRequest;
        const kind_value = data[pos];
        pos += 1;
        const kind = indexKindFromCatalogByte(kind_value) orelse return error.InvalidBackupRequest;
        const config_json = try readCatalogString(data, &pos);

        if (!first.*) try out.append(allocator, ',');
        first.* = false;
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ":{\"name\":");
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ",\"kind\":");
        try appendJsonString(allocator, out, @tagName(kind));
        try out.appendSlice(allocator, ",\"config_json\":");
        try appendJsonString(allocator, out, config_json);
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
        var source = try LiteDb.open(allocator, src_path, .writer);
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
    try std.testing.expect(parsed_indexes.value.object.get("ft_body") != null);
    const enrichments = parsed_indexes.value.object.get("enrichments") orelse return error.TestExpectedEqual;
    try std.testing.expect(enrichments == .array);
    try std.testing.expectEqual(@as(usize, 1), enrichments.array.items.len);
}
