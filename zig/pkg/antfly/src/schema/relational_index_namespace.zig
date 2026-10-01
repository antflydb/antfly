// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy at
// https://www.antfly.io/licensing/ELv2-license.

//! One public index name resolves to exactly one durable owner. Relational
//! definitions live only in schema.relational_indexes, never artifact configs.
const std = @import("std");

pub fn validate(alloc: std.mem.Allocator, schema_json: []const u8, indexes_json: []const u8) !void {
    var artifact = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len == 0) "{}" else indexes_json, .{});
    defer artifact.deinit();
    if (artifact.value != .object) return error.InvalidSchemaUpdateRequest;
    var iterator = artifact.value.object.iterator();
    while (iterator.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        if (entry.value_ptr.object.get("type")) |kind| {
            if (kind == .string and std.mem.eql(u8, kind.string, "relational")) return error.InvalidSchemaUpdateRequest;
        }
    }
    if (schema_json.len == 0) return;
    var schema = try std.json.parseFromSlice(struct { relational_indexes: []const struct { name: []const u8 } = &.{} }, alloc, schema_json, .{ .ignore_unknown_fields = true });
    defer schema.deinit();
    for (schema.value.relational_indexes) |definition| {
        if (artifact.value.object.contains(definition.name)) return error.InvalidSchemaUpdateRequest;
    }
}

test "relational mutation namespace rejects dual owners and misplaced physical declarations" {
    const a = std.testing.allocator;
    try validate(a, "{\"relational_indexes\":[{\"name\":\"row_idx\"}]}", "{\"text\":{\"type\":\"full_text\"}}");
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(a, "{\"relational_indexes\":[{\"name\":\"text\"}]}", "{\"text\":{\"type\":\"full_text\"}}"));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, validate(a, "{}", "{\"row_idx\":{\"type\":\"relational\"}}"));
}
