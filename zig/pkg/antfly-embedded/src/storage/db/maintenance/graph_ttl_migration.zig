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

//! Bounded, transactional relocation of the legacy mixed expiry/promotion
//! family. Reader epoch 4 excludes writers that could append old expiry keys.
const std = @import("std");
const missingAsNull = @import("../../backend_types.zig").missingAsNull;
const erased = @import("../../backend_erased.zig");
const keys = @import("../../internal_keys.zig");
const expiration = @import("../graph_edge_ttl_expiration.zig");
const catalog = @import("../catalog/resolver_catalog.zig");
const A = std.mem.Allocator;

pub const progress_key = "\x00\x00__metadata__:graph_expiration_namespace_v2";
pub const Limits = struct { keys: usize = 4096, bytes: usize = 4 * 1024 * 1024 };
const Move = struct { old: []const u8, key: []const u8, value: []const u8 };

fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return try missingAsNull(txn.get(key));
}
fn same(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}
fn promotionKey(a: A, key: []const u8) !bool {
    const prefix = &keys.promoted_keys_state_prefix;
    if (!std.mem.startsWith(u8, key, prefix)) return false;
    const end = keys.findComponentTerminator(key, prefix.len) orelse return false;
    if (end + 2 != key.len) return false;
    const resolution = try keys.decodeBodyAlloc(a, key[prefix.len..end]);
    defer a.free(resolution);
    return keys.isResolutionArtifactKey(resolution);
}
fn validate(a: A, key: []const u8, value: []const u8) !void {
    const deadline = try expiration.deadlineFromKey(key);
    const due = try expiration.decodeDue(value);
    const expected = switch (due) {
        .direct => |direct| blk: {
            if (direct.deadline_ns != deadline) return error.InvalidGraphTtlCandidate;
            break :blk try expiration.directIndexKeyAlloc(a, deadline, direct.artifact_key);
        },
        .source => |source| blk: {
            if (source.deadline_ns != deadline) return error.InvalidGraphTtlCandidate;
            const contender = try keys.graphGlobalEdgeContenderKeyAlloc(a, source.index_name, source.generation, source.edge_key, source.source_priority, source.state_key);
            defer a.free(contender);
            break :blk try expiration.indexKeyAlloc(a, deadline, contender);
        },
    };
    defer a.free(expected);
    if (!std.mem.eql(u8, expected, key)) return error.InvalidGraphTtlCandidate;
}

/// true means another bounded page is needed. Progress and relocations commit
/// together. A changed progress record causes a fresh read, never a replay of
/// a stale transaction; ambiguous commit replies retain their original error.
pub fn advance(allocator: A, store: *erased.Store, limits: Limits) !bool {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var moves: std.ArrayList(Move) = .empty;
    var after: std.ArrayList(u8) = .empty;
    var observed: ?[]const u8 = null;
    var more = false;
    {
        var read = try store.beginReadWithBlockCacheAdmission(.transient);
        defer read.abort();
        if (try catalog.readAuthoritativeReaderVersion(&read, catalog.supported_reader_version) < catalog.graph_expiration_namespace_reader_version)
            return error.UnsupportedDatabaseReaderVersion;
        if (try optionalGet(&read, progress_key)) |raw| {
            if (std.mem.eql(u8, raw, &.{0})) return false;
            if (raw.len < 2 or raw[0] != 1 or !std.mem.startsWith(u8, raw[1..], &keys.legacy_graph_edge_expiration_index_prefix)) return error.InvalidGraphTtlCandidate;
            observed = try a.dupe(u8, raw);
        }
        var cursor = try read.openCursor();
        defer cursor.close();
        const lower = if (observed) |raw| raw[1..] else &keys.legacy_graph_edge_expiration_index_prefix;
        var entry = try cursor.seekAtOrAfter(lower);
        if (entry) |row| if (observed != null and std.mem.eql(u8, row.key, lower)) {
            entry = try cursor.next();
        };
        var count: usize = 0;
        var bytes: usize = 0;
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, &keys.legacy_graph_edge_expiration_index_prefix)) break;
            const size = std.math.add(usize, row.key.len, row.value.len) catch return error.ResourceLimitExceeded;
            // One indivisible entry may exceed the byte target. Always
            // advance it once; a large promotion value must not pin the cursor.
            if (count >= @max(1, limits.keys) or (count != 0 and size > @max(1, limits.bytes) -| bytes)) {
                more = true;
                break;
            }
            count += 1;
            bytes += size;
            if (try promotionKey(a, row.key)) {
                // A pre-existing exact key collision cannot be guessed away.
                // Preserve it and report the ambiguity instead of dropping an
                // expiry record or redirecting promotion state.
                if (expiration.decodeDue(row.value)) |_| return error.InvalidGraphTtlCandidate else |_| {}
            } else {
                const key = try std.mem.concat(a, u8, &.{ &keys.graph_edge_expiration_index_prefix, row.key[keys.legacy_graph_edge_expiration_index_prefix.len..] });
                try validate(a, key, row.value);
                try moves.append(a, .{ .old = try a.dupe(u8, row.key), .key = key, .value = try a.dupe(u8, row.value) });
            }
            after.clearRetainingCapacity();
            try after.appendSlice(a, row.key);
        }
    }
    const progress: []const u8 = if (more) try std.mem.concat(a, u8, &.{ &.{1}, after.items }) else &.{0};
    var write = try store.beginWrite();
    var committed = false;
    defer if (!committed) write.abort();
    if (try catalog.readAuthoritativeReaderVersion(&write, catalog.supported_reader_version) < catalog.graph_expiration_namespace_reader_version)
        return error.UnsupportedDatabaseReaderVersion;
    if (!same(observed, try optionalGet(&write, progress_key))) return true;
    for (moves.items) |move| {
        const current = (try optionalGet(&write, move.old)) orelse continue;
        if (!std.mem.eql(u8, current, move.value)) return error.CatalogGenerationChanged;
        if (try optionalGet(&write, move.key)) |newer| {
            // A current writer may already own a newer digest at this exact
            // deadline/source key. Validate and preserve that publication.
            try validate(a, move.key, newer);
        } else try write.put(move.key, move.value);
        try write.delete(move.old);
    }
    try write.put(progress_key, progress);
    try write.commit();
    committed = true;
    return more;
}

test "graph ttl migration relocates bounded pages and preserves promotion and newer expiry state" {
    const a = std.testing.allocator;
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "migration" });
    defer store.deinit();
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const artifact = try keys.graphEdgeArtifactKeyAlloc(scratch, "doc", "g", "links", "other");
    const resolution = try keys.resolutionArtifactKeyAlloc(scratch, "doc", "resolution");
    var promotion: std.ArrayList(u8) = .empty;
    try promotion.appendSlice(scratch, &keys.promoted_keys_state_prefix);
    try keys.appendEncodedComponent(&promotion, scratch, resolution);
    var seed = try store.beginWrite();
    try seed.put(catalog.minimum_reader_key, "4");
    try seed.put(promotion.items, "preserve promotion");
    try seed.put(&.{ keys.replay_namespace, 0xff, 0x55 }, "neighbor");
    for ([_]u64{ 100, 200 }) |deadline| {
        const key = try expiration.directIndexKeyAlloc(scratch, deadline, artifact);
        const old = try scratch.dupe(u8, key);
        @memcpy(old[0..keys.legacy_graph_edge_expiration_index_prefix.len], &keys.legacy_graph_edge_expiration_index_prefix);
        const value = try expiration.encodeDirectAlloc(scratch, .{ .index_name = "g", .generation = 7, .artifact_key = artifact, .deadline_ns = deadline, .artifact_digest = @splat(3) });
        try seed.put(old, value);
        if (deadline == 200) {
            const newer = try expiration.encodeDirectAlloc(scratch, .{ .index_name = "g", .generation = 7, .artifact_key = artifact, .deadline_ns = deadline, .artifact_digest = @splat(9) });
            try seed.put(key, newer);
        }
    }
    try seed.commit();
    var pages: usize = 1;
    while (try advance(a, &store, .{ .keys = 1, .bytes = 1 })) : (pages += 1) {
        try std.testing.expect(pages < 8);
    }
    try std.testing.expectEqual(@as(usize, 3), pages);
    try std.testing.expect(!try advance(a, &store, .{ .keys = 1 }));
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("preserve promotion", try read.get(promotion.items));
    try std.testing.expectEqualStrings("neighbor", try read.get(&.{ keys.replay_namespace, 0xff, 0x55 }));
    for ([_]u64{ 100, 200 }) |deadline| {
        const key = try expiration.directIndexKeyAlloc(scratch, deadline, artifact);
        const old = try scratch.dupe(u8, key);
        @memcpy(old[0..keys.legacy_graph_edge_expiration_index_prefix.len], &keys.legacy_graph_edge_expiration_index_prefix);
        try std.testing.expectError(error.NotFound, read.get(old));
        const due = try expiration.decodeDue(try read.get(key));
        try std.testing.expectEqualSlices(u8, &(@as([32]u8, @splat(if (deadline == 200) 9 else 3))), &due.direct.artifact_digest);
    }
}

test "graph ttl migration requires its reader fence and preserves corrupt legacy pages" {
    const a = std.testing.allocator;
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "migration" });
    defer store.deinit();
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, advance(a, &store, .{}));
    {
        var reservation = try store.beginWrite();
        try reservation.put(catalog.minimum_reader_key, "4");
        try reservation.put(catalog.replacement_reader_base_key, "3");
        try reservation.commit();
    }
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, advance(a, &store, .{}));
    var write = try store.beginWrite();
    try write.delete(catalog.replacement_reader_base_key);
    try write.put(catalog.minimum_reader_key, "4");
    const bad = keys.legacy_graph_edge_expiration_index_prefix ++ "bad";
    try write.put(bad, "bad");
    try write.commit();
    try std.testing.expectError(error.InvalidGraphTtlCandidate, advance(a, &store, .{}));
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("bad", try read.get(bad));
    try std.testing.expectError(error.NotFound, read.get(progress_key));
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, catalog.readMinimumReaderVersion(&read, 3));
}
