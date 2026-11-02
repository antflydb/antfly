// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const coverage_identity = @import("../storage/coverage_identity.zig");

/// Private catalog identity for the desired index incarnation. It is assigned
/// by the metadata mutation path, stripped from every public response, and
/// shared by all shards so same-name replacements cannot reuse stale runtime
/// observations.
pub const incarnation_field = "_index_incarnation";
/// v0.2 persisted this embeddings-only spelling before incarnation fencing was
/// generalized to full-text and graph indexes. It remains readable so stored
/// metadata upgrades in place, but new writes always use `incarnation_field`.
pub const legacy_coverage_incarnation_field = "_coverage_incarnation";

pub const Policy = enum {
    strict,
    partial,
    best_effort,
    external,
};

// `std.fmt.allocPrint(alloc, "{f}", ...)` goes through
// `std.Io.Writer.Allocating`, which starts from a tiny capacity, grows the
// buffer while writing, and then shrinks it back down in `toOwnedSlice` by
// attempting a `remap` and falling back to alloc+copy+free when that remap
// fails. The resize/remap outcome for that final shrink is not something
// `std.testing.checkAllAllocationFailures` holds constant across its
// repeated passes, so the exact number of allocator calls a grow-then-shrink
// stringify performs is not guaranteed to match run to run, which can trip
// `error.NondeterministicMemoryUsage` for callers that assign incarnations
// through this module even though the function itself is pure. Measure the
// exact output length first with a throwaway `Discarding` writer, then
// allocate precisely once and format directly into that buffer with a
// `Writer.fixed`, so there is never a grow-then-shrink step.
fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    var measure_buf: [256]u8 = undefined;
    var discarding: std.Io.Writer.Discarding = .init(&measure_buf);
    discarding.writer.print("{f}", .{std.json.fmt(value, .{})}) catch |err| switch (err) {
        // Discarding.drain never reports a short write.
        error.WriteFailed => unreachable,
    };
    const len = std.math.cast(usize, discarding.fullCount()) orelse return error.OutOfMemory;
    const out = try alloc.alloc(u8, len);
    errdefer alloc.free(out);
    var fixed_writer = std.Io.Writer.fixed(out);
    fixed_writer.print("{f}", .{std.json.fmt(value, .{})}) catch |err| switch (err) {
        // The buffer was sized exactly for this output by the measuring pass above.
        error.WriteFailed => unreachable,
    };
    std.debug.assert(fixed_writer.end == len);
    return out;
}

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
    const legacy_incarnation = value.object.get(legacy_coverage_incarnation_field);
    if ((stored_incarnation != null or legacy_incarnation != null) and !allow_incarnation) return error.InvalidIndexConfig;
    if (stored_incarnation) |raw| {
        if (raw != .integer or raw.integer <= 0) return error.InvalidIndexConfig;
    }
    if (legacy_incarnation) |raw| {
        if (raw != .integer or raw.integer <= 0) return error.InvalidIndexConfig;
        if (stored_incarnation) |current| {
            if (current.integer != raw.integer) return error.InvalidIndexConfig;
        }
    }
    // These experimental spellings were never part of the public schema.
    if (value.object.get("coverage") != null or value.object.get("partial") != null or value.object.get("applies_when") != null) {
        return error.InvalidCoveragePolicy;
    }

    const configured = value.object.get("coverage_policy");
    const index_type = value.object.get("type") orelse {
        if (configured != null or stored_incarnation != null or legacy_incarnation != null) return error.InvalidCoveragePolicy;
        return;
    };
    if (index_type != .string) return error.InvalidIndexConfig;
    const embeddings = std.mem.eql(u8, index_type.string, "embeddings");
    if (configured != null and !embeddings) return error.InvalidCoveragePolicy;
    if (legacy_incarnation != null and !embeddings) return error.InvalidCoveragePolicy;
    if (configured == null) return;
    if (value.object.get("external")) |external| {
        if (external == .bool and external.bool) return error.InvalidCoveragePolicy;
    }
    _ = try parse(configured.?);
}

fn newIncarnation(io: std.Io) !i64 {
    return @intCast(try coverage_identity.generate(io));
}

pub fn withFreshIncarnationAllocWithIo(alloc: std.mem.Allocator, io: std.Io, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidIndexConfig;
    try validateIndexConfig(value);
    _ = value.object.get("type") orelse return try stringifyJsonAlloc(alloc, value);
    return try withIncarnationAlloc(alloc, value, @intCast(try newIncarnation(io)));
}

pub fn withFreshIncarnationAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try withFreshIncarnationAllocWithIo(alloc, std.Io.Threaded.global_single_threaded.io(), value);
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
    _ = cloned.value.object.get("type") orelse return try stringifyJsonAlloc(alloc, cloned.value);
    try cloned.value.object.put(arena, incarnation_field, .{ .integer = @intCast(coverage_incarnation) });
    return try stringifyJsonAlloc(alloc, cloned.value);
}

/// Canonicalizes trusted catalog input while preserving an incarnation already
/// assigned by the authoritative metadata mutation. Public request validation
/// must call validateIndexConfig before this internal replay path.
pub fn withIncarnationIfMissingAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    if (value != .object) return error.InvalidIndexConfig;
    try validateStoredIndexConfig(value);
    if (incarnation(value) != null) return try stringifyJsonAlloc(alloc, value);
    return try withFreshIncarnationAlloc(alloc, value);
}

pub fn incarnation(value: std.json.Value) ?u64 {
    if (value != .object) return null;
    const raw = value.object.get(incarnation_field) orelse
        value.object.get(legacy_coverage_incarnation_field) orelse return null;
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
        _ = config.object.get("type") orelse continue;
        const assigned = if (config.object.get(legacy_coverage_incarnation_field)) |legacy| switch (legacy) {
            .integer => |value| if (value > 0) value else return error.InvalidIndexConfig,
            else => return error.InvalidIndexConfig,
        } else try newIncarnation(io_impl.io());
        try config.object.put(arena, incarnation_field, .{ .integer = assigned });
    }
    return try stringifyJsonAlloc(alloc, parsed.value);
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

    var reserved = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"embeddings\",\"_index_incarnation\":42}", .{});
    defer reserved.deinit();
    try std.testing.expectError(error.InvalidIndexConfig, validateIndexConfig(reserved.value));
}

test "index configs receive persistent private incarnations across index kinds" {
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
    try std.testing.expect(incarnation(parsed_indexes.value.object.get("title").?) != null);

    const replayed_json = try withIncarnationIfMissingAlloc(alloc, first.value);
    defer alloc.free(replayed_json);
    var replayed = try std.json.parseFromSlice(std.json.Value, alloc, replayed_json, .{});
    defer replayed.deinit();
    try std.testing.expectEqual(first_incarnation, incarnation(replayed.value).?);

    var invalid_stored = try std.json.parseFromSlice(std.json.Value, alloc, "{\"type\":\"full_text\",\"_coverage_incarnation\":42}", .{});
    defer invalid_stored.deinit();
    try std.testing.expectError(error.InvalidCoveragePolicy, withIncarnationIfMissingAlloc(alloc, invalid_stored.value));
}
