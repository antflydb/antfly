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
pub const Public = struct { root: ?tree.Ref, count: u32, source: []const u8, snapshot: []const u8 };
pub const Roots = struct {
    slots: ?tree.Ref = null,
    names: ?tree.Ref = null,
    public: ?tree.Ref = null,
    count: u32 = 0,
    // Shared publication navigation maps public digests to names, independently
    // of each index's stable slots and retired-slot history.
    public_names: bool = false,
    public_count: u32 = 0,
};
pub const Options = struct { shared: ?Public = null, manager: ?*local.sql_spill.Manager = null };
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
    for ([_]?tree.Ref{ roots.slots, roots.names, roots.public }, 0..) |ref, index| {
        const count = if (index == 2 and roots.public_names) roots.public_count else roots.count;
        if ((ref == null) != (count == 0)) return error.InvalidNativeLakeRowIndex;
        if (ref) |value| {
            try value.validate();
            if (value.records != count) return error.InvalidNativeLakeRowIndex;
        }
    }
    if (!roots.public_names and roots.public_count != 0) return error.InvalidNativeLakeRowIndex;
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
    if (roots.public_names) return (try named(a, store, roots, bytes)) orelse error.InvalidNativeLakeRowIndex;
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
    return publishWithOptions(a, store, source, snapshot, files, fingerprints, previous, .{});
}
// New trees are bulk-built from bounded spill runs; path copying is reserved
// for edits to existing trees. All ordered indexes borrow one public directory.
pub fn publishWithOptions(a: A, store: tree.Store, source: []const u8, snapshot: []const u8, files: []const []const u8, fingerprints: []const [32]u8, previous: ?Previous, options: Options) !Roots {
    if (options.shared) |shared| if (!std.mem.eql(u8, source, shared.source) or !std.mem.eql(u8, snapshot, shared.snapshot)) return error.ExternalLakeSnapshotMismatch;

    if (files.len > std.math.maxInt(u32) or (fingerprints.len != 0 and fingerprints.len != files.len)) return error.InvalidNativeLakeRowIndex;
    var result: Roots = .{ .count = @intCast(files.len) };
    const prior = if (previous) |p| if (p.files.len <= files.len and p.files.len == p.roots.count and p.fingerprints.len == p.files.len) p else null else null;
    if (prior == null) return bulk(a, store, source, snapshot, files, fingerprints, options);
    if (prior) |p| {
        result.slots = p.roots.slots;
        result.names = p.roots.names;
        if (p.same_snapshot and !p.roots.public_names and options.shared == null) result.public = p.roots.public;
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
        if (options.shared == null and prior.?.same_snapshot and !prior.?.roots.public_names and !same_name) {
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
    if (options.shared) |shared| {
        result.public = shared.root;
        result.public_names = true;
        result.public_count = shared.count;
    } else if (prior.?.roots.public_names or !prior.?.same_snapshot) result.public = try bulkHashes(a, store, source, snapshot, files, true, options.manager);
    try validate(result);
    return result;
}

fn bulk(a: A, store: tree.Store, source: []const u8, snapshot: []const u8, files: []const []const u8, fingerprints: []const [32]u8, options: Options) !Roots {
    var result: Roots = .{ .count = @intCast(files.len) };
    var slots: tree.SortedBuilder = .{ .alloc = a, .store = store };
    defer slots.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (files, 0..) |name, i| {
        _ = scratch.reset(.retain_capacity);
        const fingerprint = if (fingerprints.len == 0) @as([32]u8, @splat(0)) else fingerprints[i];
        try slots.add(&slotKey(@intCast(i)), try std.json.Stringify.valueAlloc(scratch.allocator(), Record{ .file = name, .fingerprint = fingerprint }, .{}));
    }
    result.slots = try slots.finish();
    result.names = try bulkHashes(a, store, source, snapshot, files, false, options.manager);
    if (options.shared) |shared| {
        result.public = shared.root;
        result.public_names = true;
        result.public_count = shared.count;
    } else result.public = try bulkHashes(a, store, source, snapshot, files, true, options.manager);
    try validate(result);
    return result;
}
fn bulkHashes(a: A, store: tree.Store, source: []const u8, snapshot: []const u8, files: []const []const u8, public: bool, manager: ?*local.sql_spill.Manager) !?tree.Ref {
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = store };
    defer builder.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    if (manager) |managed| {
        var sort = local.sql_spill.Sort.init(a, managed, &.{.{}}, 512 * 1024);
        sort.run_limit = 4;
        sort.encoded_bytes = true;
        defer sort.deinit();
        for (files, 0..) |name, i| {
            try managed.check();
            _ = scratch.reset(.retain_capacity);
            const key = if (public) local.storage_rowsource_identity.fileDigest(source, snapshot, name) else nameKey(name);
            const value = if (public) &slotKey(@intCast(i)) else try std.json.Stringify.valueAlloc(scratch.allocator(), .{ .file = name, .slot = @as(u32, @intCast(i)) }, .{});
            try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = value })}, .ordinal = 0 });
        }
        while (true) {
            _ = scratch.reset(.retain_capacity);
            const row = (try sort.next(scratch.allocator())) orelse break;
            try builder.add(row.keys[0].value.string, row.values[0].value.string);
        }
    } else {
        const keys = try a.alloc([32]u8, files.len);
        defer a.free(keys);
        const order = try a.alloc(u32, files.len);
        defer a.free(order);
        for (files, keys, order, 0..) |name, *key, *slot, i| {
            key.* = if (public) local.storage_rowsource_identity.fileDigest(source, snapshot, name) else nameKey(name);
            slot.* = @intCast(i);
        }
        std.mem.sort(u32, order, keys, struct {
            fn less(hashes: [][32]u8, l: u32, r: u32) bool {
                return std.mem.lessThan(u8, &hashes[l], &hashes[r]);
            }
        }.less);
        for (order) |i| {
            _ = scratch.reset(.retain_capacity);
            const value = if (public) &slotKey(i) else try std.json.Stringify.valueAlloc(scratch.allocator(), .{ .file = files[i], .slot = i }, .{});
            try builder.add(&keys[i], value);
        }
    }
    return builder.finish();
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
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const first = try publishWithOptions(a, memory.store(), "source", "snapshot", names, hashes, null, .{ .manager = &manager });
    // Fresh dictionaries write packed pages once, rather than path-copying a
    // growing tree for each mutation batch.
    try std.testing.expect(memory.writes < 256);
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

test "external lake metadata navigation ordered indexes share public names across stable slot histories" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Make = struct {
        fn directory(alloc: A, store: tree.Store, snapshot: []const u8) !Public {
            const names = [_][]const u8{ "a", "b" };
            const first = local.storage_rowsource_identity.fileDigest("source", snapshot, names[0]);
            const second = local.storage_rowsource_identity.fileDigest("source", snapshot, names[1]);
            const order: [2]usize = if (std.mem.lessThan(u8, &first, &second)) .{ 0, 1 } else .{ 1, 0 };
            var builder: tree.SortedBuilder = .{ .alloc = alloc, .store = store };
            defer builder.deinit();
            for (order) |i| try builder.add(&local.storage_rowsource_identity.fileDigest("source", snapshot, names[i]), names[i]);
            return .{ .root = try builder.finish(), .count = 2, .source = "source", .snapshot = snapshot };
        }
    };
    const shared = try Make.directory(a, memory.store(), "first");
    const left_names: []const []const u8 = &.{ "retired", "b", "a" };
    const right_names: []const []const u8 = &.{ "a", "b" };
    const hashes: [3][32]u8 = @splat(@splat(1));
    const left = try publishWithOptions(a, memory.store(), "source", "first", left_names, &hashes, null, .{ .shared = shared });
    const right = try publishWithOptions(a, memory.store(), "source", "first", right_names, hashes[0..2], null, .{ .shared = shared });
    try std.testing.expectEqualDeep(left.public, right.public);
    try std.testing.expectEqual(@as(u32, 2), left.public_count);
    try std.testing.expectEqual(@as(u32, 3), left.count);
    const digest = local.storage_rowsource_identity.fileDigest("source", "first", "a");
    try std.testing.expectEqual(@as(?u32, 2), try publicSlot(ca, memory.store(), left, digest));
    try std.testing.expectEqual(@as(?u32, 0), try publicSlot(ca, memory.store(), right, digest));
    try std.testing.expectEqual(@as(?u32, null), try publicSlot(ca, memory.store(), left, local.storage_rowsource_identity.fileDigest("source", "first", "retired")));
    const next_shared = try Make.directory(a, memory.store(), "second");
    const writes = memory.writes;
    const next = try publishWithOptions(a, memory.store(), "source", "second", left_names, &hashes, .{ .roots = left, .files = left_names, .fingerprints = &hashes, .same_snapshot = false }, .{ .shared = next_shared });
    try std.testing.expectEqual(writes, memory.writes);
    try std.testing.expectEqualDeep(left.names, next.names);
    try std.testing.expectEqualDeep(left.slots, next.slots);
    try std.testing.expectEqual(@as(?u32, null), try publicSlot(ca, memory.store(), next, digest));
    try std.testing.expectEqual(@as(?u32, 2), try publicSlot(ca, memory.store(), next, local.storage_rowsource_identity.fileDigest("source", "second", "a")));
    try std.testing.expectEqual(@as(?u32, 2), try publicSlot(ca, memory.store(), left, digest));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, publishWithOptions(a, memory.store(), "source", "second", left_names, &hashes, null, .{ .shared = shared }));
}
