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

//! A term-first hierarchy in two ordinary authenticated page trees. Branches
//! describe hash buckets and conservative identity/score ranges. Leaves retain
//! exact immutable segment contributions. No embedded child refs need special GC.
const std = @import("std");
const local = @import("antfly_local_sources");
const stats = @import("lake_index_text_statistics.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Roots = struct { branches: ?tree.Ref = null, leaves: ?tree.Ref = null };
pub fn identity(artifact_id: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(artifact_id, &digest, .{});
    return digest;
}
/// Escaping makes term boundaries unambiguous, including zero bytes. Leave
/// room for segment identities and an exclusive upper bound in every tree key.
pub fn prefixAlloc(a: A, term_key: []const u8) ![]u8 {
    var length: usize = 2;
    for (term_key) |byte| length += if (byte == 0) @as(usize, 2) else 1;
    if (length > tree.max_key_bytes - 33) return error.NativeLakeTermDirectoryKeyTooLarge;
    const out = try a.alloc(u8, length);
    var i: usize = 0;
    for (term_key) |byte| {
        out[i] = byte;
        i += 1;
        if (byte == 0) {
            out[i] = 255;
            i += 1;
        }
    }
    out[i] = 0;
    out[i + 1] = 0;
    return out;
}
pub const Branch = struct {
    pub const max_bytes = 64 + stats.Summary.max_bytes;
    first: [32]u8,
    last: [32]u8,
    envelope: stats.Summary,
    pub fn encode(self: Branch, out: *[max_bytes]u8) []const u8 {
        @memcpy(out[0..32], &self.first);
        @memcpy(out[32..64], &self.last);
        var scratch: [stats.Summary.max_bytes]u8 = undefined;
        const value = self.envelope.encode(&scratch);
        @memcpy(out[64..][0..value.len], value);
        return out[0 .. 64 + value.len];
    }
    pub fn decode(bytes: []const u8) !Branch {
        if (bytes.len < 76) return error.InvalidNativeLakeTermDirectory;
        const first: [32]u8 = bytes[0..32].*;
        const last: [32]u8 = bytes[32..64].*;
        if (first[0] != last[0] or std.mem.order(u8, &first, &last) == .gt) return error.InvalidNativeLakeTermDirectory;
        return .{ .first = first, .last = last, .envelope = try stats.Summary.decode(bytes[64..]) };
    }
};

pub const Builder = struct {
    a: A,
    changes: local.sql_spill.Sort,
    branches: local.sql_spill.Sort,
    ordinal: u64 = 0,
    disabled: bool = false,
    pub fn init(a: A, manager: *local.sql_spill.Manager) Builder {
        return .{ .a = a, .changes = local.sql_spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024), .branches = local.sql_spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024) };
    }
    pub fn deinit(self: *Builder) void {
        self.changes.deinit();
        self.branches.deinit();
    }
    pub fn add(self: *Builder, store: tree.Store, root: ?tree.Ref, segment: [32]u8, sign: i64) !void {
        if (sign != 1 and sign != -1) return error.InvalidNativeLakeTermDirectory;
        if (self.disabled) return;
        var cursor = try tree.Cursor.init(self.a, store, root, "", null);
        defer cursor.deinit();
        while (try cursor.next()) |record| try self.addRecord(record, segment, sign);
    }
    pub fn addRecord(self: *Builder, record: tree.Cursor.Record, segment: [32]u8, sign: i64) !void {
        if (sign != 1 and sign != -1) return error.InvalidNativeLakeTermDirectory;
        if (self.disabled) return;
        _ = try stats.Summary.decode(record.value);
        const prefix = prefixAlloc(self.a, record.key) catch |err| {
            // Preserve legacy accepted terms through the exact reader path.
            if (err == error.NativeLakeTermDirectoryKeyTooLarge) {
                self.disabled = true;
                return;
            }
            return err;
        };
        defer self.a.free(prefix);
        const key = try self.a.alloc(u8, prefix.len + 32);
        defer self.a.free(key);
        @memcpy(key[0..prefix.len], prefix);
        @memcpy(key[prefix.len..], &segment);
        try self.changes.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{ local.sql_scalar.Datum.fromJson(.{ .integer = sign }), local.sql_scalar.Datum.fromJson(.{ .string = record.value }) }, .ordinal = self.ordinal });
        self.ordinal += 1;
    }

    fn queueBranch(self: *Builder, key: []const u8, branch: ?Branch) !void {
        var buffer: [Branch.max_bytes]u8 = undefined;
        try self.branches.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{if (branch) |value| local.sql_scalar.Datum.fromJson(.{ .string = value.encode(&buffer) }) else local.sql_scalar.Datum.fromJson(.null)}, .ordinal = self.ordinal });
    }
    const Changes = struct {
        owner: *Builder,
        scratch: std.heap.ArenaAllocator,
        row: ?local.sql_operators.Row = null,
        key: ?[]u8 = null,
        value: [stats.Summary.max_bytes]u8 = undefined,
        sign: i64 = 0,
        fn deinit(self: *@This()) void {
            self.scratch.deinit();
            if (self.key) |key| self.owner.a.free(key);
        }
        pub fn next(self: *@This()) !?tree.Mutation {
            while (true) {
                if (self.key) |key| self.owner.a.free(key);
                self.key = null;
                if (self.row == null) self.row = try self.owner.changes.next(self.scratch.allocator());
                const first = self.row orelse return null;
                self.key = try self.owner.a.dupe(u8, first.keys[0].value.string);
                self.sign = 0;
                var length: usize = 0;
                while (self.row) |row| {
                    if (!std.mem.eql(u8, self.key.?, row.keys[0].value.string)) break;
                    self.sign = std.math.add(i64, self.sign, row.values[0].value.integer) catch return error.InvalidNativeLakeTermDirectory;
                    if (row.values[0].value.integer > 0) {
                        const encoded = (try stats.Summary.decode(row.values[1].value.string)).encode(&self.value);
                        length = encoded.len;
                    }
                    _ = self.scratch.reset(.retain_capacity);
                    self.row = try self.owner.changes.next(self.scratch.allocator());
                }
                if (self.sign == 0) continue;
                if (self.sign != 1 and self.sign != -1) return error.InvalidNativeLakeTermDirectory;
                return .{ .key = self.key.?, .value = if (self.sign == 1) self.value[0..length] else null };
            }
        }
    };
    pub fn finish(self: *Builder, store: tree.Store, prior: Roots) !Roots {
        if (self.disabled) return .{};
        try self.changes.finish();
        var changes: Changes = .{ .owner = self, .scratch = std.heap.ArenaAllocator.init(self.a) };
        defer changes.deinit();
        var roots = prior;
        if (prior.leaves == null) {
            const Initial = struct {
                changes: *Changes,
                key: ?[]u8 = null,
                branch: ?Branch = null,
                fn flush(source: *@This()) !void {
                    if (source.key) |key| try source.changes.owner.queueBranch(key, source.branch);
                }
                pub fn next(source: *@This()) !?tree.Cursor.Record {
                    const row = (try source.changes.next()) orelse {
                        try source.flush();
                        if (source.key) |key| source.changes.owner.a.free(key);
                        source.key = null;
                        return null;
                    };
                    const value = row.value orelse return error.InvalidNativeLakeTermDirectory;
                    const branch_key = row.key[0 .. row.key.len - 31];
                    const id: [32]u8 = row.key[row.key.len - 32 ..][0..32].*;
                    const summary = try stats.Summary.decode(value);
                    if (source.key == null or !std.mem.eql(u8, source.key.?, branch_key)) {
                        try source.flush();
                        if (source.key) |key| source.changes.owner.a.free(key);
                        source.key = null;
                        source.key = try source.changes.owner.a.dupe(u8, branch_key);
                        source.branch = .{ .first = id, .last = id, .envelope = summary };
                    } else {
                        source.branch.?.last = id;
                        source.branch.?.envelope.merge(summary);
                    }
                    return .{ .key = row.key, .value = value };
                }
            };
            var initial: Initial = .{ .changes = &changes };
            defer if (initial.key) |key| self.a.free(key);
            roots.leaves = try tree.buildSorted(self.a, store, &initial);
        } else {
            var batch = std.heap.ArenaAllocator.init(self.a);
            defer batch.deinit();
            var mutations: std.ArrayListUnmanaged(tree.Mutation) = .empty;
            var bytes: usize = 0;
            while (try changes.next()) |change| {
                const a = batch.allocator();
                const key = try a.dupe(u8, change.key);
                const value = if (change.value) |value| try a.dupe(u8, value) else null;
                try mutations.append(a, .{ .key = key, .value = value });
                bytes += key.len + (if (value) |v| v.len else 0) + @sizeOf(tree.Mutation);
                // Branch keys are sorted again and deduplicated after all leaf
                // mutations, so each affected bucket is recomputed once.
                try self.queueBranch(key[0 .. key.len - 31], null);
                if (mutations.items.len >= 4096 or bytes >= 2 * 1024 * 1024) {
                    roots.leaves = try tree.apply(self.a, store, roots.leaves, mutations.items);
                    _ = batch.reset(.free_all);
                    mutations = .empty;
                    bytes = 0;
                }
            }
            if (mutations.items.len != 0) roots.leaves = try tree.apply(self.a, store, roots.leaves, mutations.items);
        }
        try self.branches.finish();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        var batch = std.heap.ArenaAllocator.init(self.a);
        defer batch.deinit();
        var previous: ?[]u8 = null;
        defer if (previous) |key| self.a.free(key);
        var mutations: std.ArrayListUnmanaged(tree.Mutation) = .empty;
        var bytes: usize = 0;
        var cache: tree.Cache = .{ .alloc = self.a, .underlying = store };
        defer cache.deinit();
        while (true) {
            _ = arena.reset(.retain_capacity);
            const row = (try self.branches.next(arena.allocator())) orelse break;
            const key = row.keys[0].value.string;
            if (previous) |old| if (std.mem.eql(u8, old, key)) continue;
            if (previous) |old| self.a.free(old);
            previous = null;
            previous = try self.a.dupe(u8, key);
            var branch: ?Branch = null;
            if (prior.leaves == null) {
                branch = try Branch.decode(row.values[0].value.string);
            } else {
                const end = try arena.allocator().dupe(u8, key);
                // Last byte is the bucket. 255 uses the term prefix's final
                // zero terminator as carry; this is still below the next term.
                var i = end.len;
                while (i > 0) {
                    i -= 1;
                    if (end[i] != 255) {
                        end[i] += 1;
                        break;
                    }
                    end[i] = 0;
                }
                var cursor = try tree.Cursor.init(self.a, cache.store(), roots.leaves, key, end);
                defer cursor.deinit();
                while (try cursor.next()) |record| {
                    if (record.key.len != key.len + 31 or !std.mem.startsWith(u8, record.key, key)) return error.InvalidNativeLakeTermDirectory;
                    const id: [32]u8 = record.key[record.key.len - 32 ..][0..32].*;
                    const summary = try stats.Summary.decode(record.value);
                    if (branch) |*value| {
                        value.last = id;
                        value.envelope.merge(summary);
                    } else branch = .{ .first = id, .last = id, .envelope = summary };
                }
            }
            const a = batch.allocator();
            var buffer: [Branch.max_bytes]u8 = undefined;
            const value = if (branch) |value| try a.dupe(u8, value.encode(&buffer)) else null;
            try mutations.append(a, .{ .key = try a.dupe(u8, key), .value = value });
            bytes += key.len + Branch.max_bytes + @sizeOf(tree.Mutation);
            if (mutations.items.len >= 4096 or bytes >= 2 * 1024 * 1024) {
                roots.branches = try tree.apply(self.a, cache.store(), roots.branches, mutations.items);
                _ = batch.reset(.free_all);
                mutations = .empty;
                bytes = 0;
            }
        }
        if (mutations.items.len != 0) roots.branches = try tree.apply(self.a, cache.store(), roots.branches, mutations.items);
        return roots;
    }
};

test "external lake text statistics term directory replaces removes and preserves unchanged contributions" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Source = struct {
        done: bool = false,
        value: [stats.Summary.max_bytes]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.done) return null;
            self.done = true;
            const summary: stats.Summary = .{ .frequency = 2, .max_frequency = 3, .min_norm = 10 };
            return .{ .key = "\x00\x00\x00\x04bodyrare", .value = summary.encode(&self.value) };
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var source: Source = .{};
    const contribution = try tree.buildSorted(a, memory.store(), &source);
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Source.checkpoint };
    defer manager.deinit();
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    var ids: [3][32]u8 = @splat(@splat(255));
    for (&ids, 0..) |*id, index| id[31] = @intCast(index + 1); // One bucket, including the 255 upper-bound carry.
    for (ids[0..2]) |id| try initial.add(memory.store(), contribution, id, 1);
    const prior = try initial.finish(memory.store(), .{});
    try std.testing.expectEqual(@as(u64, 2), prior.leaves.?.records);
    try std.testing.expectEqual(@as(u64, 1), prior.branches.?.records);
    var unchanged = Builder.init(a, &manager);
    defer unchanged.deinit();
    const same = try unchanged.finish(memory.store(), prior);
    try std.testing.expect(same.leaves.?.eql(prior.leaves.?));
    try std.testing.expect(same.branches.?.eql(prior.branches.?));
    var update = Builder.init(a, &manager);
    defer update.deinit();
    try update.add(memory.store(), contribution, ids[0], -1);
    try update.add(memory.store(), contribution, ids[2], 1);
    const current = try update.finish(memory.store(), prior);
    const prefix = try prefixAlloc(a, "\x00\x00\x00\x04bodyrare");
    defer a.free(prefix);
    const key = try a.alloc(u8, prefix.len + 32);
    defer a.free(key);
    @memcpy(key[0..prefix.len], prefix);
    for (ids, 0..) |id, index| {
        @memcpy(key[prefix.len..], &id);
        var cursor = try tree.Cursor.init(a, memory.store(), current.leaves, key, null);
        defer cursor.deinit();
        const found = try cursor.next();
        const exists = if (found) |record| std.mem.eql(u8, record.key, key) else false;
        try std.testing.expectEqual(index != 0, exists);
    }
    var remove = Builder.init(a, &manager);
    defer remove.deinit();
    for (ids[1..]) |id| try remove.add(memory.store(), contribution, id, -1);
    const empty = try remove.finish(memory.store(), current);
    try std.testing.expect(empty.leaves == null and empty.branches == null);
}
