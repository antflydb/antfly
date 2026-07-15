// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const coverage_identity = @import("../storage/coverage_identity.zig");

pub const incarnation_field = "_coverage_incarnation";

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
    return try validateIndexConfigWithPrivateFields(value, false);
}

pub fn validateStoredIndexConfig(value: std.json.Value) !void {
    return try validateIndexConfigWithPrivateFields(value, true);
}

fn validateIndexConfigWithPrivateFields(value: std.json.Value, allow_incarnation: bool) !void {
    if (value != .object) return error.InvalidIndexConfig;
    const stored_incarnation = value.object.get(incarnation_field);
    if (stored_incarnation != null and !allow_incarnation) return error.InvalidIndexConfig;
    if (stored_incarnation) |raw| {
        if (raw != .integer or raw.integer <= 0) return error.InvalidIndexConfig;
    }
    // These experimental spellings were never part of the public schema.
    if (value.object.get("coverage") != null or value.object.get("partial") != null or value.object.get("applies_when") != null) {
        return error.InvalidCoveragePolicy;
    }

    const configured = value.object.get("coverage_policy");
    const index_type = value.object.get("type") orelse {
        if (configured != null or stored_incarnation != null) return error.InvalidCoveragePolicy;
        return;
    };
    if (index_type != .string) return error.InvalidIndexConfig;
    const embeddings = std.mem.eql(u8, index_type.string, "embeddings");
    if ((configured != null or stored_incarnation != null) and !embeddings) return error.InvalidCoveragePolicy;
    if (configured == null) return;
    if (value.object.get("external")) |external| {
        if (external == .bool and external.bool) return error.InvalidCoveragePolicy;
    }
    _ = try parse(configured.?);
}

fn newIncarnation(io: std.Io) !i64 {
    return @intCast(try coverage_identity.generate(io));
}

pub fn withFreshIncarnationAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidIndexConfig;
    try validateIndexConfig(value);
    const index_type = value.object.get("type") orelse return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    if (index_type != .string or !std.mem.eql(u8, index_type.string, "embeddings")) {
        return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    }

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try withIncarnationAlloc(alloc, value, @intCast(try newIncarnation(io_impl.io())));
}

pub fn withIncarnationAlloc(alloc: std.mem.Allocator, value: std.json.Value, coverage_incarnation: u64) ![]u8 {
    if (value != .object or !coverage_identity.isValid(coverage_incarnation)) return error.InvalidIndexConfig;
    try validateIndexConfig(value);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var cloned = try std.json.parseFromSlice(
        std.json.Value,
        arena,
        try std.fmt.allocPrint(arena, "{f}", .{std.json.fmt(value, .{})}),
        .{},
    );
    defer cloned.deinit();
    const index_type = cloned.value.object.get("type") orelse return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cloned.value, .{})});
    if (index_type != .string or !std.mem.eql(u8, index_type.string, "embeddings")) {
        return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cloned.value, .{})});
    }
    try cloned.value.object.put(arena, incarnation_field, .{ .integer = @intCast(coverage_incarnation) });
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(cloned.value, .{})});
}

/// Canonicalizes trusted catalog input while preserving an incarnation already
/// assigned by the authoritative metadata mutation. Public request validation
/// must call validateIndexConfig before this internal replay path.
pub fn withIncarnationIfMissingAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidIndexConfig;
    try validateStoredIndexConfig(value);
    if (value.object.get(incarnation_field)) |raw| {
        _ = raw;
        return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    }
    return try withFreshIncarnationAlloc(alloc, value);
}

pub fn incarnation(value: std.json.Value) ?u64 {
    if (value != .object) return null;
    const raw = value.object.get(incarnation_field) orelse return null;
    if (raw != .integer or raw.integer <= 0) return null;
    return @intCast(raw.integer);
}

pub fn withMissingIncarnationsAlloc(alloc: std.mem.Allocator, indexes_json: []const u8) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidIndexConfig;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const config = entry.value_ptr;
        if (config.* != .object or config.object.get(incarnation_field) != null) continue;
        const index_type = config.object.get("type") orelse continue;
        if (index_type != .string or !std.mem.eql(u8, index_type.string, "embeddings")) continue;
        try config.object.put(arena, incarnation_field, .{ .integer = try newIncarnation(io_impl.io()) });
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
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

    var reserved = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"_coverage_incarnation\":42}", .{});
    defer reserved.deinit();
    try std.testing.expectError(error.InvalidIndexConfig, validateIndexConfig(reserved.value));
}

test "coverage policy assigns persistent private incarnations only to embeddings" {
    const alloc = std.testing.allocator;

    var embeddings = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\"}", .{});
    defer embeddings.deinit();
    const first_json = try withFreshIncarnationAlloc(alloc, embeddings.value);
    defer alloc.free(first_json);
    const second_json = try withFreshIncarnationAlloc(alloc, embeddings.value);
    defer alloc.free(second_json);

    var first = try std.json.parseFromSlice(std.json.Value, alloc, first_json, .{});
    defer first.deinit();
    var second = try std.json.parseFromSlice(std.json.Value, alloc, second_json, .{});
    defer second.deinit();
    const first_incarnation = incarnation(first.value) orelse return error.TestUnexpectedResult;
    const second_incarnation = incarnation(second.value) orelse return error.TestUnexpectedResult;
    try std.testing.expect(first_incarnation != second_incarnation);

    const indexes = try withMissingIncarnationsAlloc(alloc, "{\"visual\":{\"type\":\"embeddings\"},\"title\":{\"type\":\"full_text\"}}");
    defer alloc.free(indexes);
    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, indexes, .{});
    defer parsed_indexes.deinit();
    try std.testing.expect(incarnation(parsed_indexes.value.object.get("visual").?) != null);
    try std.testing.expect(incarnation(parsed_indexes.value.object.get("title").?) == null);

    const replayed_json = try withIncarnationIfMissingAlloc(alloc, first.value);
    defer alloc.free(replayed_json);
    var replayed = try std.json.parseFromSlice(std.json.Value, alloc, replayed_json, .{});
    defer replayed.deinit();
    try std.testing.expectEqual(first_incarnation, incarnation(replayed.value).?);

    var invalid_stored = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"full_text\",\"_coverage_incarnation\":42}", .{});
    defer invalid_stored.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, withIncarnationIfMissingAlloc(alloc, invalid_stored.value));
}
