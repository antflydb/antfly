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

//! Table index parsing and validation, independent of storage ownership.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");
const algebraic = @import("../storage/db/algebraic/mod.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const coverage_policy_mod = @import("coverage_policy.zig");
const index_manager = @import("../storage/db/catalog/index_manager.zig");
const internal_keys = @import("../storage/internal_keys.zig");

pub fn parseIndexKind(value: std.json.Value) !db_types.IndexKind {
    if (value != .object) return .full_text;
    const type_value = value.object.get("type") orelse return .full_text;
    if (type_value != .string) return error.InvalidCreateTableRequest;
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

pub fn parseIndexConfig(alloc: std.mem.Allocator, index_name: []const u8, index_json: []const u8) !db_types.IndexConfig {
    return try parseIndexConfigWithOptions(alloc, index_name, index_json, .{});
}

pub fn parseIndexConfigWithOptions(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    index_json: []const u8,
    options: managed_embedder.InitOptions,
) !db_types.IndexConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    const kind = try parseIndexKind(parsed.value);
    const config_json = try extractIndexConfigJsonWithOptions(alloc, index_name, parsed.value, options);
    errdefer alloc.free(config_json);
    const configured_incarnation = coverage_policy_mod.incarnation(parsed.value);
    return .{
        .name = try alloc.dupe(u8, index_name),
        .kind = kind,
        .config_json = config_json,
        // v0.2 configs may predate the private catalog incarnation. Derive a
        // deterministic cross-shard fallback so rolling upgrades fence stale
        // same-name observations without assigning unrelated local generations.
        .coverage_generation = configured_incarnation orelse internal_keys.derivedCoverageGeneration(config_json),
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
        var parsed = std.json.parseFromSlice(algebraic.index.Config, alloc, cfg.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return error.InvalidCreateTableRequest;
        defer parsed.deinit();
        algebraic.index.validateConfig(parsed.value) catch return error.InvalidCreateTableRequest;
    } else if (cfg.kind == .graph) {
        try validateGraphConfig(alloc, cfg.config_json);
    }
}

fn validateGraphConfig(alloc: std.mem.Allocator, config_json: []const u8) !void {
    index_manager.validateGraphConfig(alloc, config_json) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCreateTableRequest,
    };
}

/// Validate the runtime semantics of every graph index in a table definition.
/// This is intentionally graph-only: managed embedding normalization may need
/// provider I/O and algebraic validation needs the expanded table schema, while
/// graph parsing is pure and must complete before any catalog mutation.
pub fn validateGraphIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const indexes = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var it = indexes.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidCreateTableRequest;
        if (try parseIndexKind(entry.value_ptr.*) != .graph) continue;
        const config_json = try extractIndexConfigJson(alloc, entry.key_ptr.*, entry.value_ptr.*);
        defer alloc.free(config_json);
        try validateGraphConfig(alloc, config_json);
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
            std.mem.eql(u8, entry.key_ptr.*, coverage_policy_mod.incarnation_field) or
            std.mem.eql(u8, entry.key_ptr.*, coverage_policy_mod.legacy_coverage_incarnation_field))
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
pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

test "graph index validation runs the runtime parser before catalog admission" {
    const alloc = std.testing.allocator;

    try validateIndexConfig(
        alloc,
        "relations_graph",
        "{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"source\":{\"type\":\"field\",\"value\":\"relations\"}}}",
    );

    const invalid_configs = [_][]const u8{
        "{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[0]\"}}",
        "{\"type\":\"graph\",\"edge_types\":[{\"name\":\"mentions\",\"topology\":\"dag\"}]}",
        "{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\",\"nodes\":{\"source\":\"{{ _doc.value.tenant }}\"}}}",
        "{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\"}}",
        "{\"type\":\"graph\",\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"source\":{\"type\":\"document\",\"value\":\"relations\"}}}",
    };
    for (invalid_configs) |config| {
        try std.testing.expectError(
            error.InvalidCreateTableRequest,
            validateIndexConfig(alloc, "relations_graph", config),
        );
    }
}

test "table graph validation rejects runtime-invalid configs before catalog admission" {
    const alloc = std.testing.allocator;
    try validateGraphIndexesJson(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"relations_graph\":{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\"}}}",
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validateGraphIndexesJson(
            alloc,
            "{\"relations_graph\":{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[0]\"}}}",
        ),
    );
}
