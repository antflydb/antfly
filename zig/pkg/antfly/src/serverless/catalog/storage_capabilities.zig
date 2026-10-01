// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! The artifact-serving engine does not own native typed-row transactions or
//! constraint claims. Reject declarations before catalog publication, and old
//! unsupported definitions before writes, rather than acknowledging unenforced
//! relational semantics. Compiled storage-owner deployments use native DB APIs.
const std = @import("std");

pub fn requireSchema(alloc: std.mem.Allocator, bytes: []const u8) !void {
    if (bytes.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableSchema;
    if (parsed.value.object.get("storage_mode")) |mode| {
        if (mode != .string) return error.InvalidTableSchema;
        if (std.mem.eql(u8, mode.string, "relational")) {
            // Existing Parquet/Iceberg read-only sources have typed schemas but
            // do not promise native mutable-row/index/constraint execution.
            if (!isReadOnlyExternal(parsed.value)) return error.RelationalStorageUnavailable;
        } else if (!std.mem.eql(u8, mode.string, "document")) return error.InvalidTableSchema;
    }
    for ([_][]const u8{ "relational_indexes", "checks", "unique_constraints", "foreign_keys", "column_defaults", "generated_columns" }) |name| {
        if (parsed.value.object.get(name)) |declarations| {
            if (declarations == .null) continue;
            if (declarations != .array) return error.InvalidTableSchema;
            if (declarations.array.items.len != 0) return error.RelationalStorageUnavailable;
        }
    }
}

fn isReadOnlyExternal(root: std.json.Value) bool {
    const source = root.object.get("base_source") orelse return false;
    if (source != .object) return false;
    const kind = source.object.get("kind") orelse return false;
    const policy = source.object.get("write_policy") orelse return false;
    return kind == .string and std.mem.eql(u8, kind.string, "external") and
        policy == .string and std.mem.eql(u8, policy.string, "read_only");
}

pub fn requireDefinition(alloc: std.mem.Allocator, schema: []const u8, read_schema: []const u8, indexes: []const u8) !void {
    try requireSchema(alloc, schema);
    try requireSchema(alloc, read_schema);
    if (indexes.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTableIndexMetadata;
    for (parsed.value.object.values()) |index| {
        if (index != .object) return error.InvalidTableIndexMetadata;
        if (index.object.get("type")) |kind| {
            if (kind != .string) return error.InvalidTableIndexMetadata;
            if (std.mem.eql(u8, kind.string, "relational")) return error.RelationalStorageUnavailable;
        }
    }
}

test "serverless capabilities reject unenforced relational semantics before publication" {
    const alloc = std.testing.allocator;
    try requireDefinition(alloc, "", "{}", "{\"graph\":{\"type\":\"graph\"}}");
    try requireSchema(alloc, "{\"storage_mode\":\"document\",\"checks\":[],\"document_schemas\":{}}");
    try requireSchema(alloc, "{\"storage_mode\":\"relational\",\"base_source\":{\"kind\":\"external\",\"write_policy\":\"read_only\"}}");
    try std.testing.expectError(error.RelationalStorageUnavailable, requireSchema(alloc, "{\"storage_mode\":\"relational\",\"base_source\":{\"kind\":\"external\",\"write_policy\":\"overlay\"}}"));
    try std.testing.expectError(error.RelationalStorageUnavailable, requireSchema(alloc, "{\"storage_mode\":\"relational\"}"));
    for ([_][]const u8{ "checks", "unique_constraints", "foreign_keys", "relational_indexes", "column_defaults", "generated_columns" }) |field| {
        const bytes = try std.fmt.allocPrint(alloc, "{{\"{s}\":[{{}}]}}", .{field});
        defer alloc.free(bytes);
        try std.testing.expectError(error.RelationalStorageUnavailable, requireSchema(alloc, bytes));
    }
    try std.testing.expectError(error.RelationalStorageUnavailable, requireSchema(alloc,
        \\{"storage_mode":"relational","base_source":{"kind":"external","write_policy":"read_only"},"generated_columns":[{"column":"computed","expression":{"op":"literal","type":"integer","value":1}}]}
    ));
    try std.testing.expectError(error.RelationalStorageUnavailable, requireDefinition(alloc, "{}", "{\"storage_mode\":\"relational\"}", "{}"));
    try std.testing.expectError(error.RelationalStorageUnavailable, requireDefinition(alloc, "{}", "{}", "{\"ordered\":{\"type\":\"relational\"}}"));
    try std.testing.expectError(error.InvalidTableSchema, requireSchema(alloc, "{\"storage_mode\":false}"));
}
