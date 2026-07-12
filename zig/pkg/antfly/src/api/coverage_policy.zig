// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

pub const Policy = enum {
    strict,
    partial,
    best_effort,
    external,
};

pub fn parse(value: std.json.Value) !Policy {
    if (value != .string) return error.InvalidCoveragePolicy;
    if (std.mem.eql(u8, value.string, "strict")) return .strict;
    if (std.mem.eql(u8, value.string, "partial")) return .partial;
    if (std.mem.eql(u8, value.string, "best_effort")) return .best_effort;
    return error.InvalidCoveragePolicy;
}

pub fn validateIndexConfig(value: std.json.Value) !void {
    if (value != .object) return error.InvalidIndexConfig;
    // These experimental spellings were never part of the public schema.
    if (value.object.get("coverage") != null or value.object.get("partial") != null or value.object.get("applies_when") != null) {
        return error.InvalidCoveragePolicy;
    }

    const configured = value.object.get("coverage_policy") orelse return;
    const index_type = value.object.get("type") orelse return error.InvalidCoveragePolicy;
    if (index_type != .string) return error.InvalidIndexConfig;
    const embeddings = std.mem.eql(u8, index_type.string, "embeddings");
    if (!embeddings) return error.InvalidCoveragePolicy;
    if (value.object.get("external")) |external| {
        if (external == .bool and external.bool) return error.InvalidCoveragePolicy;
    }
    _ = try parse(configured);
}

test "coverage policy accepts only the public embeddings contract" {
    const alloc = std.testing.allocator;
    var valid = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\"}", .{});
    defer valid.deinit();
    try validateIndexConfig(valid.value);

    var invalid = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"coverage\":\"partial\"}", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, validateIndexConfig(invalid.value));

    var external = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"external\":true,\"coverage_policy\":\"partial\"}", .{});
    defer external.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, validateIndexConfig(external.value));

    var wrong_kind = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"full_text\",\"coverage_policy\":\"partial\"}", .{});
    defer wrong_kind.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, validateIndexConfig(wrong_kind.value));

    var unsupported_eligibility = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\",\"applies_when\":{\"exists\":\"image_url\"}}", .{});
    defer unsupported_eligibility.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, validateIndexConfig(unsupported_eligibility.value));
}
