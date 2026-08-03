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

const coverage_policy_mod = @import("../coverage_policy.zig");
const db_mod = @import("../../storage/db/mod.zig");
const lsm_backend = @import("../../storage/lsm_backend/mod.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");

pub fn parseIndexKind(value: std.json.Value) !db_mod.types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse return .full_text;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (indexConfigTypeIsMetadataOnlyScalar(type_value.string)) return error.UnsupportedCreateTableRequest;
    if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
    if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
    if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
    if (std.mem.eql(u8, type_value.string, "embeddings")) {
        const sparse = if (value.object.get("sparse")) |sparse_value| switch (sparse_value) {
            .bool => sparse_value.bool,
            else => return error.InvalidCreateTableRequest,
        } else false;
        return if (sparse) .sparse_vector else .dense_vector;
    }
    return error.UnsupportedCreateTableRequest;
}

pub fn isReservedTableIndexMetadataEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "resolvers") or
        std.mem.eql(u8, name, "enrichments") or
        std.mem.eql(u8, name, "typed_paths");
}

fn indexConfigTypeIsMetadataOnlyScalar(type_name: []const u8) bool {
    return std.mem.eql(u8, type_name, "scalar") or
        std.mem.eql(u8, type_name, "path") or
        std.mem.eql(u8, type_name, "secondary") or
        std.mem.eql(u8, type_name, "keyword") or
        std.mem.eql(u8, type_name, "numeric") or
        std.mem.eql(u8, type_name, "boolean") or
        std.mem.eql(u8, type_name, "datetime") or
        std.mem.eql(u8, type_name, "term");
}

pub fn parseIndexConfig(alloc: std.mem.Allocator, index_name: []const u8, index_json: []const u8) !db_mod.types.IndexConfig {
    return try parseIndexConfigWithOptions(alloc, index_name, index_json, .{});
}

pub fn parseIndexConfigWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    index_json: []const u8,
    options: managed_embedder.InitOptions,
) !db_mod.types.IndexConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    const kind = try parseIndexKind(parsed.value);
    const config_json = try extractIndexConfigJsonWithOptions(alloc, index_name, parsed.value, options);
    errdefer alloc.free(config_json);
    return .{
        .name = try alloc.dupe(u8, index_name),
        .kind = kind,
        .config_json = config_json,
        .coverage_generation = coverage_policy_mod.incarnation(parsed.value) orelse 0,
    };
}

pub fn validateIndexConfig(alloc: std.mem.Allocator, index_name: []const u8, index_json: []const u8) !void {
    return try validateIndexConfigWithOptions(alloc, index_name, index_json, .{});
}

pub fn validateIndexConfigWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    index_json: []const u8,
    options: managed_embedder.InitOptions,
) !void {
    const cfg = try parseIndexConfigWithOptions(alloc, index_name, index_json, options);
    defer {
        alloc.free(cfg.name);
        alloc.free(cfg.config_json);
    }
    if (cfg.kind == .algebraic) {
        var parsed = std.json.parseFromSlice(db_mod.algebraic.index.Config, alloc, cfg.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return error.InvalidCreateTableRequest;
        defer parsed.deinit();
        db_mod.algebraic.index.validateConfig(parsed.value) catch return error.InvalidCreateTableRequest;
    }
}

pub fn extractIndexConfigJson(alloc: std.mem.Allocator, index_name: []const u8, value: std.json.Value) ![]u8 {
    return try extractIndexConfigJsonWithOptions(alloc, index_name, value, .{});
}

pub fn extractIndexConfigJsonWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
    options: managed_embedder.InitOptions,
) ![]u8 {
    if (value != .object) return try alloc.dupe(u8, "{}");
    const kind = try parseIndexKind(value);
    switch (kind) {
        .dense_vector, .sparse_vector => return try managed_embedder.translateEmbeddingsIndexConfigJsonWithOptions(alloc, index_name, value, options),
        else => {},
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "type") or
            std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, "description") or
            std.mem.eql(u8, entry.key_ptr.*, "validation") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments") or
            (kind == .full_text and fullTextCatalogOnlyIndexConfigField(entry.key_ptr.*)))
        {
            continue;
        }
        if (std.mem.eql(u8, entry.key_ptr.*, "version") and kind != .algebraic) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn fullTextCatalogOnlyIndexConfigField(field: []const u8) bool {
    return std.mem.eql(u8, field, "field") or
        std.mem.eql(u8, field, "path") or
        std.mem.eql(u8, field, "fields") or
        std.mem.eql(u8, field, "paths") or
        std.mem.eql(u8, field, "scalar_field") or
        std.mem.eql(u8, field, "scalar_path") or
        std.mem.eql(u8, field, "scalar_fields") or
        std.mem.eql(u8, field, "scalar_paths") or
        std.mem.eql(u8, field, "indexed_scalar_field") or
        std.mem.eql(u8, field, "indexed_scalar_path") or
        std.mem.eql(u8, field, "indexed_scalar_fields") or
        std.mem.eql(u8, field, "indexed_scalar_paths");
}

pub fn normalizeManagedEmbeddingIndexDimensionJsonWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    index_json: []const u8,
    options: managed_embedder.InitOptions,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    if (try managed_embedder.normalizeEmbeddingsIndexDimensionJsonWithOptions(alloc, index_name, parsed.value, options)) |normalized| {
        return normalized;
    }
    return try alloc.dupe(u8, index_json);
}

pub fn normalizeManagedEmbeddingIndexDimensionsJsonWithOptions(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    options: managed_embedder.InitOptions,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        if (try managed_embedder.normalizeEmbeddingsIndexDimensionJsonWithOptions(alloc, entry.key_ptr.*, entry.value_ptr.*, options)) |normalized| {
            defer alloc.free(normalized);
            try out.appendSlice(alloc, normalized);
        } else {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub const StartupConfiguredIndex = struct {
    name: []u8 = &.{},
    kind: db_mod.types.IndexKind = .dense_vector,
    algebraic_schema_version: u32 = 0,
    algebraic_capability_fingerprint: ?[]u8 = null,
    algebraic_capability_lifecycle_status: ?[]u8 = null,
    algebraic_capability_change_added_fields: u32 = 0,
    algebraic_capability_change_removed_fields: u32 = 0,
    algebraic_capability_change_changed_type_fields: u32 = 0,
    algebraic_skipped_dynamic_fields: u32 = 0,
    algebraic_skipped_complex_fields: u32 = 0,
    algebraic_skipped_unbounded_fields: u32 = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(self.name);
        if (self.algebraic_capability_fingerprint) |value| alloc.free(value);
        if (self.algebraic_capability_lifecycle_status) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn populateStats(self: *const @This(), alloc: std.mem.Allocator, stats: *db_mod.types.DBIndexStats) !void {
        if (self.kind != .algebraic) return;
        stats.algebraic_schema_version = self.algebraic_schema_version;
        stats.algebraic_capability_change_added_fields = self.algebraic_capability_change_added_fields;
        stats.algebraic_capability_change_removed_fields = self.algebraic_capability_change_removed_fields;
        stats.algebraic_capability_change_changed_type_fields = self.algebraic_capability_change_changed_type_fields;
        stats.algebraic_skipped_dynamic_fields = self.algebraic_skipped_dynamic_fields;
        stats.algebraic_skipped_complex_fields = self.algebraic_skipped_complex_fields;
        stats.algebraic_skipped_unbounded_fields = self.algebraic_skipped_unbounded_fields;
        if (self.algebraic_capability_fingerprint) |value| {
            stats.algebraic_capability_fingerprint = try alloc.dupe(u8, value);
        }
        if (self.algebraic_capability_lifecycle_status) |value| {
            stats.algebraic_capability_lifecycle_status = try alloc.dupe(u8, value);
        }
    }
};

pub const StartupConfiguredIndexes = struct {
    items: []StartupConfiguredIndex,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        alloc.free(self.items);
        self.* = undefined;
    }

    pub fn populateConfiguredCounts(self: *const @This(), stats: *db_mod.types.StartupCatchUpStats) void {
        for (self.items) |item| incrementStartupConfiguredIndexCounts(stats, item.kind);
    }

    pub fn accumulateRetention(
        self: *const @This(),
        storage: lsm_backend.Storage,
        alloc: std.mem.Allocator,
        table_path: []const u8,
        stats: *db_mod.types.StartupCatchUpStats,
    ) !void {
        for (self.items) |item| {
            const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ table_path, item.name });
            defer alloc.free(index_path);
            const main_retention = try lsm_backend.wal.snapshotRetention(storage, alloc, index_path);
            const replay_retention = try lsm_backend.wal.snapshotReplayRetention(storage, alloc, index_path);
            stats.wal_retained_segments += main_retention.segments + replay_retention.segments;
            stats.wal_retained_bytes += main_retention.bytes + replay_retention.bytes;
        }
    }
};

pub fn parseStartupConfiguredIndexes(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !StartupConfiguredIndexes {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    const array_form = object.get("indexes");
    const index_count = try startupConfiguredStorageIndexCount(parsed.value);
    const items = try alloc.alloc(StartupConfiguredIndex, index_count);
    errdefer {
        for (items[0..index_count]) |*item| item.deinit(alloc);
        alloc.free(items);
    }
    @memset(items, .{});
    var initialized: usize = 0;

    if (array_form) |value| {
        const array_items = switch (value) {
            .array => value.array.items,
            else => return error.InvalidCreateTableRequest,
        };
        for (array_items) |item| {
            if (item != .object) return error.InvalidCreateTableRequest;
            const name_value = item.object.get("name") orelse return error.InvalidCreateTableRequest;
            if (name_value != .string) return error.InvalidCreateTableRequest;
            const kind = try parseIndexKind(item);
            var configured = StartupConfiguredIndex{
                .name = try alloc.dupe(u8, name_value.string),
                .kind = kind,
            };
            errdefer configured.deinit(alloc);
            try populateStartupAlgebraicCapability(alloc, &configured, item);
            items[initialized] = configured;
            initialized += 1;
        }
        return .{ .items = items };
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (isReservedTableIndexMetadataEntry(entry.key_ptr.*)) continue;
        const kind = try parseIndexKind(entry.value_ptr.*);
        var configured = StartupConfiguredIndex{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .kind = kind,
        };
        errdefer configured.deinit(alloc);
        try populateStartupAlgebraicCapability(alloc, &configured, entry.value_ptr.*);
        items[initialized] = configured;
        initialized += 1;
    }
    return .{ .items = items };
}

fn startupConfiguredStorageIndexCount(root: std.json.Value) !usize {
    const object = switch (root) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    if (object.get("indexes")) |array_value| {
        const array_items = switch (array_value) {
            .array => array_value.array.items,
            else => return error.InvalidCreateTableRequest,
        };
        var count: usize = 0;
        for (array_items) |item| {
            if (item != .object) return error.InvalidCreateTableRequest;
            count += 1;
        }
        return count;
    }

    var count: usize = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (isReservedTableIndexMetadataEntry(entry.key_ptr.*)) continue;
        count += 1;
    }
    return count;
}

fn startupAlgebraicField(value: std.json.Value, field: []const u8) ?std.json.Value {
    const object = switch (value) {
        .object => |object| object,
        else => return null,
    };
    if (object.get(field)) |direct| return direct;
    const config = object.get("config") orelse return null;
    const config_object = switch (config) {
        .object => |config_object| config_object,
        else => return null,
    };
    return config_object.get(field);
}

fn startupAlgebraicString(value: std.json.Value, field: []const u8) ?[]const u8 {
    const field_value = startupAlgebraicField(value, field) orelse return null;
    return switch (field_value) {
        .string => |string| string,
        else => null,
    };
}

fn startupAlgebraicU32(value: std.json.Value, field: []const u8) ?u32 {
    const field_value = startupAlgebraicField(value, field) orelse return null;
    return switch (field_value) {
        .integer => |integer| if (integer >= 0 and integer <= std.math.maxInt(u32)) @intCast(integer) else null,
        else => null,
    };
}

fn populateStartupAlgebraicCapability(
    alloc: std.mem.Allocator,
    item: *StartupConfiguredIndex,
    value: std.json.Value,
) !void {
    if (item.kind != .algebraic) return;
    item.algebraic_schema_version = startupAlgebraicU32(value, "schema_version") orelse 0;
    item.algebraic_capability_change_added_fields = startupAlgebraicU32(value, "capability_change_added_fields") orelse 0;
    item.algebraic_capability_change_removed_fields = startupAlgebraicU32(value, "capability_change_removed_fields") orelse 0;
    item.algebraic_capability_change_changed_type_fields = startupAlgebraicU32(value, "capability_change_changed_type_fields") orelse 0;
    item.algebraic_skipped_dynamic_fields = startupAlgebraicU32(value, "skipped_dynamic_fields") orelse 0;
    item.algebraic_skipped_complex_fields = startupAlgebraicU32(value, "skipped_complex_fields") orelse 0;
    item.algebraic_skipped_unbounded_fields = startupAlgebraicU32(value, "skipped_unbounded_fields") orelse 0;
    if (startupAlgebraicString(value, "capability_fingerprint")) |fingerprint| {
        if (fingerprint.len > 0) item.algebraic_capability_fingerprint = try alloc.dupe(u8, fingerprint);
    }
    if (startupAlgebraicString(value, "capability_lifecycle_status")) |status| {
        if (status.len > 0) item.algebraic_capability_lifecycle_status = try alloc.dupe(u8, status);
    }
}

fn incrementStartupConfiguredIndexCounts(
    stats: *db_mod.types.StartupCatchUpStats,
    kind: db_mod.types.IndexKind,
) void {
    stats.configured_indexes += 1;
    switch (kind) {
        .dense_vector => stats.configured_dense_indexes += 1,
        .sparse_vector => stats.configured_sparse_indexes += 1,
        .full_text => stats.configured_full_text_indexes += 1,
        .graph => stats.configured_graph_indexes += 1,
        .algebraic => {},
    }
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

test "table write index parser extracts expanded algebraic capability config" {
    const alloc = std.testing.allocator;
    const cfg = try parseIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","version":1,"table":"sales","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[]}
    );
    defer {
        alloc.free(cfg.name);
        alloc.free(cfg.config_json);
    }

    try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, cfg.kind);
    try std.testing.expectEqualStrings("sales_rollup", cfg.name);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"version\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"materializations\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"type\":\"algebraic\"") == null);
}

test "table write index validation checks expanded algebraic config semantics" {
    const alloc = std.testing.allocator;

    try validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[]}
    );

    try std.testing.expectError(error.InvalidCreateTableRequest, validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[{"name":"sum_by_customer","op":"median","group_by":["customer"],"measure":"amount"}]}
    ));

    try std.testing.expectError(error.InvalidCreateTableRequest, validateIndexConfig(alloc, "sales_rollup",
        \\{"type":"algebraic","table":"sales","schema_version":1,"capability_fingerprint":"sales:v1","group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[{"name":"sum_by_customer","op":"sum","group_by":["missing"],"measure":"amount"}]}
    ));
}

test "table write index parser trusts normalized managed embedding dimension without provider probe" {
    try validateIndexConfig(std.testing.allocator, "semantic_idx",
        \\{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"}}
    );
}

test "table write index parser keeps full text field metadata out of storage config" {
    const alloc = std.testing.allocator;
    const cfg = try parseIndexConfig(alloc, "category_fts",
        \\{"type":"full_text","field":"category","fields":["title","body"],"analyzer":"standard"}
    );
    defer {
        alloc.free(cfg.name);
        alloc.free(cfg.config_json);
    }

    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, cfg.kind);
    try std.testing.expectEqualStrings("category_fts", cfg.name);
    try std.testing.expectEqualStrings("{\"analyzer\":\"standard\"}", cfg.config_json);
}

test "table write startup configured index parser supports object and array forms" {
    const alloc = std.testing.allocator;

    var object_form = try parseStartupConfiguredIndexes(
        alloc,
        "{\"vec\":{\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},\"fts\":{\"type\":\"full_text\"},\"alg\":{\"type\":\"algebraic\",\"schema_version\":42,\"capability_fingerprint\":\"cap:v1\",\"capability_lifecycle_status\":\"rebuild_required\",\"capability_change_added_fields\":1,\"capability_change_removed_fields\":2,\"capability_change_changed_type_fields\":3,\"skipped_dynamic_fields\":4,\"skipped_complex_fields\":5,\"skipped_unbounded_fields\":6,\"materializations\":[]},\"resolvers\":{}}",
    );
    defer object_form.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), object_form.items.len);
    try std.testing.expectEqualStrings("vec", object_form.items[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, object_form.items[0].kind);
    try std.testing.expectEqualStrings("fts", object_form.items[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, object_form.items[1].kind);
    try std.testing.expectEqualStrings("alg", object_form.items[2].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.algebraic, object_form.items[2].kind);
    try std.testing.expectEqual(@as(u32, 42), object_form.items[2].algebraic_schema_version);
    try std.testing.expectEqualStrings("cap:v1", object_form.items[2].algebraic_capability_fingerprint.?);
    try std.testing.expectEqualStrings("rebuild_required", object_form.items[2].algebraic_capability_lifecycle_status.?);
    try std.testing.expectEqual(@as(u32, 1), object_form.items[2].algebraic_capability_change_added_fields);
    try std.testing.expectEqual(@as(u32, 2), object_form.items[2].algebraic_capability_change_removed_fields);
    try std.testing.expectEqual(@as(u32, 3), object_form.items[2].algebraic_capability_change_changed_type_fields);
    try std.testing.expectEqual(@as(u32, 4), object_form.items[2].algebraic_skipped_dynamic_fields);
    try std.testing.expectEqual(@as(u32, 5), object_form.items[2].algebraic_skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 6), object_form.items[2].algebraic_skipped_unbounded_fields);

    var stats: db_mod.types.StartupCatchUpStats = .{};
    object_form.populateConfiguredCounts(&stats);
    try std.testing.expectEqual(@as(u64, 3), stats.configured_indexes);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_dense_indexes);
    try std.testing.expectEqual(@as(u64, 1), stats.configured_full_text_indexes);

    var array_form = try parseStartupConfiguredIndexes(
        alloc,
        "{\"indexes\":[{\"name\":\"vec\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":768}},{\"name\":\"fts\",\"type\":\"full_text\",\"config\":{}}]}",
    );
    defer array_form.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), array_form.items.len);
    try std.testing.expectEqualStrings("vec", array_form.items[0].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.dense_vector, array_form.items[0].kind);
    try std.testing.expectEqualStrings("fts", array_form.items[1].name);
    try std.testing.expectEqual(db_mod.types.IndexKind.full_text, array_form.items[1].kind);
}

test "table write index validation rejects scalar path metadata as an index" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedCreateTableRequest, validateIndexConfig(alloc, "score_idx",
        \\{"type":"numeric","path":"metrics.score"}
    ));
    try std.testing.expectError(error.UnsupportedCreateTableRequest, validateIndexConfig(alloc, "status_idx",
        \\{"type":"keyword","fields":["status","metadata.plan"]}
    ));
    try std.testing.expectError(error.UnsupportedCreateTableRequest, parseIndexConfig(alloc, "score_idx",
        \\{"type":"numeric","path":"metrics.score"}
    ));
}
