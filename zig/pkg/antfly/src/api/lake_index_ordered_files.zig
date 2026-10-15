// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Paged file-slot dictionaries. Serving resolves reached slots/names/public
//! IDs only. Maintenance explicitly materializes slots under its build budget.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Roots = struct { slots: ?tree.Ref = null, names: ?tree.Ref = null, public: ?tree.Ref = null, count: u32 = 0 };
pub const Record = struct { file: []const u8, fingerprint: [32]u8 };
pub fn slotKey(slot: u32) [4]u8 {
    var key: [4]u8 = undefined;
    std.mem.writeInt(u32, &key, slot, .big);
    return key;
}
fn nameKey(name: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(name, &digest, .{});
    return digest;
}
pub fn validate(roots: Roots) !void {
    for ([_]?tree.Ref{ roots.slots, roots.names, roots.public }) |ref| {
        if ((ref == null) != (roots.count == 0)) return error.InvalidNativeLakeRowIndex;
        if (ref) |value| {
            try value.validate();
            if (value.records != roots.count) return error.InvalidNativeLakeRowIndex;
        }
    }
}
fn lookup(a: A, store: tree.Store, root: ?tree.Ref, key: []const u8) !?[]u8 {
    var cursor = try tree.Cursor.init(a, store, root, key, null);
    defer cursor.deinit();
    const record = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, record.key, key)) return null;
    return try a.dupe(u8, record.value);
}
pub fn file(a: A, store: tree.Store, roots: Roots, slot: u32) !Record {
    if (slot >= roots.count) return error.InvalidNativeLakeRowIndex;
    const bytes = (try lookup(a, store, roots.slots, &slotKey(slot))) orelse return error.InvalidNativeLakeRowIndex;
    defer a.free(bytes);
    const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
    if (record.file.len == 0) return error.InvalidNativeLakeRowIndex;
    return record;
}
pub fn named(a: A, store: tree.Store, roots: Roots, name: []const u8) !?u32 {
    const bytes = (try lookup(a, store, roots.names, &nameKey(name))) orelse return null;
    defer a.free(bytes);
    const result = try std.json.parseFromSlice(struct { file: []const u8, slot: u32 }, a, bytes, .{});
    defer result.deinit();
    if (!std.mem.eql(u8, result.value.file, name) or result.value.slot >= roots.count) return error.InvalidNativeLakeRowIndex;
    return result.value.slot;
}
pub fn publicSlot(a: A, store: tree.Store, roots: Roots, digest: [32]u8) !?u32 {
    const bytes = (try lookup(a, store, roots.public, &digest)) orelse return null;
    defer a.free(bytes);
    if (bytes.len != 4) return error.InvalidNativeLakeRowIndex;
    const slot = std.mem.readInt(u32, bytes[0..4], .big);
    if (slot >= roots.count) return error.InvalidNativeLakeRowIndex;
    return slot;
}
pub const Materialized = struct { files: []const []const u8, fingerprints: []const [32]u8 };
pub fn materialize(a: A, store: tree.Store, roots: Roots) !Materialized {
    const files = try a.alloc([]const u8, roots.count);
    const fingerprints = try a.alloc([32]u8, roots.count);
    var cursor = try tree.Cursor.init(a, store, roots.slots, "", null);
    defer cursor.deinit();
    for (files, fingerprints, 0..) |*name, *digest, i| {
        const record = (try cursor.next()) orelse return error.InvalidNativeLakeRowIndex;
        if (!std.mem.eql(u8, record.key, &slotKey(@intCast(i)))) return error.InvalidNativeLakeRowIndex;
        const value = try std.json.parseFromSliceLeaky(Record, a, record.value, .{ .allocate = .alloc_always });
        if (value.file.len == 0) return error.InvalidNativeLakeRowIndex;
        name.* = value.file;
        digest.* = value.fingerprint;
    }
    if (try cursor.next() != null) return error.InvalidNativeLakeRowIndex;
    return .{ .files = files, .fingerprints = fingerprints };
}
const Batch = struct {
    arena: std.heap.ArenaAllocator,
    changes: std.ArrayList(tree.Mutation) = .empty,
    fn add(self: *@This(), key: []const u8, value: ?[]const u8) !void {
        const a = self.arena.allocator();
        try self.changes.append(a, .{ .key = try a.dupe(u8, key), .value = if (value) |bytes| try a.dupe(u8, bytes) else null });
    }
    fn flush(self: *@This(), store: tree.Store, root: *?tree.Ref) !void {
        if (self.changes.items.len == 0) return;
        std.mem.sort(tree.Mutation, self.changes.items, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        root.* = try tree.apply(self.arena.allocator(), store, root.*, self.changes.items);
        _ = self.arena.reset(.retain_capacity);
        self.changes = .empty;
    }
};
pub const Previous = struct { roots: Roots, files: []const []const u8, fingerprints: []const [32]u8, same_snapshot: bool };
pub fn publish(a: A, store: tree.Store, source: []const u8, snapshot: []const u8, files: []const []const u8, fingerprints: []const [32]u8, previous: ?Previous) !Roots {
    if (files.len > std.math.maxInt(u32) or (fingerprints.len != 0 and fingerprints.len != files.len)) return error.InvalidNativeLakeRowIndex;
    var result: Roots = .{ .count = @intCast(files.len) };
    const prior = if (previous) |p| if (p.files.len <= files.len and p.files.len == p.roots.count and p.fingerprints.len == p.files.len) p else null else null;
    if (prior) |p| {
        result.slots = p.roots.slots;
        result.names = p.roots.names;
        if (p.same_snapshot) result.public = p.roots.public;
    }
    var slots: Batch = .{ .arena = .init(a) };
    defer slots.arena.deinit();
    var names: Batch = .{ .arena = .init(a) };
    defer names.arena.deinit();
    var public: Batch = .{ .arena = .init(a) };
    defer public.arena.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (files, 0..) |name, i| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const fingerprint = if (fingerprints.len != 0) fingerprints[i] else @as([32]u8, @splat(0));
        const old_name = if (prior) |p| if (i < p.files.len) p.files[i] else null else null;
        const same_name = if (old_name) |old| std.mem.eql(u8, name, old) else false;
        const same_hash = if (prior) |p| if (i < p.fingerprints.len) std.mem.eql(u8, &fingerprint, &p.fingerprints[i]) else false else false;
        const slot: u32 = @intCast(i);
        if (!same_name or !same_hash) try slots.add(&slotKey(slot), try std.json.Stringify.valueAlloc(sa, Record{ .file = name, .fingerprint = fingerprint }, .{}));
        if (!same_name) {
            if (old_name) |old| try names.add(&nameKey(old), null);
            try names.add(&nameKey(name), try std.json.Stringify.valueAlloc(sa, .{ .file = name, .slot = slot }, .{}));
        }
        if (prior == null or !prior.?.same_snapshot or !same_name) {
            if (prior != null and prior.?.same_snapshot) if (old_name) |old| try public.add(&local.storage_rowsource_identity.fileDigest(source, snapshot, old), null);
            try public.add(&local.storage_rowsource_identity.fileDigest(source, snapshot, name), &slotKey(slot));
        }
        if (slots.changes.items.len >= 64 or names.changes.items.len >= 64 or public.changes.items.len >= 64) {
            try slots.flush(store, &result.slots);
            try names.flush(store, &result.names);
            try public.flush(store, &result.public);
        }
    }
    try slots.flush(store, &result.slots);
    try names.flush(store, &result.names);
    try public.flush(store, &result.public);
    try validate(result);
    return result;
}

test "external lake metadata navigation ordered dictionaries exceed legacy file limits and copy only changed pages" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const names = try ca.alloc([]const u8, 17000);
    const hashes = try ca.alloc([32]u8, names.len);
    for (names, hashes, 0..) |*name, *hash, i| {
        name.* = try std.fmt.allocPrint(ca, "file-{d:0>5}", .{i});
        hash.* = @splat(1);
    }
    const first = try publish(a, memory.store(), "source", "snapshot", names, hashes, null);
    try std.testing.expectEqual(@as(u32, 17000), first.count);
    const before = memory.reads;
    const record = try file(ca, memory.store(), first, 16999);
    try std.testing.expectEqualStrings("file-16999", record.file);
    try std.testing.expect(memory.reads - before < 10);
    try std.testing.expectEqual(@as(?u32, 16999), try named(ca, memory.store(), first, record.file));
    try std.testing.expectEqual(@as(?u32, 16999), try publicSlot(ca, memory.store(), first, local.storage_rowsource_identity.fileDigest("source", "snapshot", record.file)));
    const prior_hashes = try ca.dupe([32]u8, hashes);
    hashes[16999] = @splat(2);
    const writes = memory.writes;
    const next = try publish(a, memory.store(), "source", "snapshot", names, hashes, .{ .roots = first, .files = names, .fingerprints = prior_hashes, .same_snapshot = true });
    try std.testing.expectEqualDeep(first.names, next.names);
    try std.testing.expectEqualDeep(first.public, next.public);
    try std.testing.expect(memory.writes - writes < 10);
    try std.testing.expectEqualDeep(@as([32]u8, @splat(2)), (try file(ca, memory.store(), next, 16999)).fingerprint);
    try std.testing.expectEqualDeep(@as([32]u8, @splat(1)), (try file(ca, memory.store(), first, 16999)).fingerprint);
    const expanded = try materialize(ca, memory.store(), next);
    for (names, expanded.files) |expected, actual| try std.testing.expectEqualStrings(expected, actual);
    try std.testing.expectEqualDeep(hashes, expanded.fingerprints);
}
