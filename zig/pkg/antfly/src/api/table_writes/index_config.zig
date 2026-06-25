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

const db_mod = @import("../../storage/db/mod.zig");
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
