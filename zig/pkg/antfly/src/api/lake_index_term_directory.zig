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
pub const Roots = struct { version: u8 = 1, branches: ?tree.Ref = null, leaves: ?tree.Ref = null };
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
    pub const max_bytes = 72 + stats.Summary.max_bytes;
    first: [32]u8,
    last: [32]u8,
    envelope: stats.Summary,
    count: u64 = 1,
    pub fn encode(self: Branch, out: *[max_bytes]u8) []const u8 {
        @memcpy(out[0..32], &self.first);
        @memcpy(out[32..64], &self.last);
        var scratch: [stats.Summary.max_bytes]u8 = undefined;
        const value = self.envelope.encode(&scratch);
        std.mem.writeInt(u64, out[64..72], self.count, .little);
        @memcpy(out[72..][0..value.len], value);
        return out[0 .. 72 + value.len];
    }
    pub fn decode(bytes: []const u8) !Branch {
        if (bytes.len < 89) return error.InvalidNativeLakeTermDirectory;
        const first: [32]u8 = bytes[0..32].*;
        const last: [32]u8 = bytes[32..64].*;
        const count = std.mem.readInt(u64, bytes[64..72], .little);
        if (count == 0 or std.mem.order(u8, &first, &last) == .gt) return error.InvalidNativeLakeTermDirectory;
        return .{ .first = first, .last = last, .count = count, .envelope = try stats.Summary.decode(bytes[72..]) };
    }
    pub fn decodeLegacy(bytes: []const u8) !Branch {
        if (bytes.len < 76) return error.InvalidNativeLakeTermDirectory;
        const first: [32]u8 = bytes[0..32].*;
        const last: [32]u8 = bytes[32..64].*;
        if (first[0] != last[0] or std.mem.order(u8, &first, &last) == .gt) return error.InvalidNativeLakeTermDirectory;
        return .{ .first = first, .last = last, .count = std.math.maxInt(u64), .envelope = try stats.Summary.decode(bytes[64..]) };
    }
    pub fn merge(self: *Branch, other: Branch) !void {
        if (std.mem.order(u8, &other.first, &self.first) == .lt) self.first = other.first;
        if (std.mem.order(u8, &other.last, &self.last) == .gt) self.last = other.last;
        self.count = std.math.add(u64, self.count, other.count) catch return error.InvalidNativeLakeTermDirectory;
        self.envelope.merge(other.envelope);
    }
};

/// The same bounded raw envelope serves bucket and page reductions. Interior
/// page reductions can span terms/buckets; range folding uses only covered pages.
pub fn reduce(a: A, entries: []const tree.Entry, height: u8) ![]u8 {
    var result: ?Branch = null;
    for (entries) |entry| {
        const contribution: Branch = if (height == 0) blk: {
            if (entry.key.len < 34) return error.InvalidNativeLakeTermDirectory;
            const id: [32]u8 = entry.key[entry.key.len - 32 ..][0..32].*;
            break :blk .{ .first = id, .last = id, .envelope = try stats.Summary.decode(entry.value) };
        } else blk: {
            const value = try Branch.decode(entry.aggregate);
            if (value.count != entry.child.?.records) return error.InvalidNativeLakeTermDirectory;
            break :blk value;
        };
        if (result) |*value| try value.merge(contribution) else result = contribution;
    }
    var buffer: [Branch.max_bytes]u8 = undefined;
    return a.dupe(u8, (result orelse return error.InvalidNativeLakeTermDirectory).encode(&buffer));
}

pub const Builder = struct {
    a: A,
    changes: local.sql_spill.Sort,
    ordinal: u64 = 0,
    disabled: bool = false,
    strategy: stats.Builder.Strategy = .auto,
    selected: stats.Builder.Strategy = .auto,
    distinct_changes: u64 = 0,
    reduced_records: u64 = 0,
    aggregate_nodes: u64 = 0,
    durable: ?@import("lake_index_delta_runs.zig").Forest = null,
    pub fn init(a: A, manager: *local.sql_spill.Manager) Builder {
        return .{ .a = a, .changes = local.sql_spill.Sort.init(a, manager, &.{.{}}, 4 * 1024 * 1024) };
    }
    pub fn deinit(self: *Builder) void {
        self.changes.deinit();
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
        self.changes.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{ local.sql_scalar.Datum.fromJson(.{ .integer = sign }), local.sql_scalar.Datum.fromJson(.{ .string = record.value }) }, .ordinal = self.ordinal }) catch |err| {
            if (err == error.SqlProgramLimitExceeded) return self.disable();
            return err;
        };
        self.ordinal += 1;
        if (self.durable != null and (self.changes.total >= 8192 or self.changes.manager.live_bytes >= 32 * 1024 * 1024)) self.flush() catch |err| {
            if (err == error.SqlProgramLimitExceeded) return self.disable();
            return err;
        };
    }

    const Changes = struct {
        owner: *Builder,
        scratch: std.heap.ArenaAllocator,
        row: ?local.sql_operators.Row = null,
        cursor: ?*tree.Cursor = null,
        key: ?[]u8 = null,
        value: [stats.Summary.max_bytes]u8 = undefined,
        sign: i64 = 0,
        fn deinit(self: *@This()) void {
            self.scratch.deinit();
            if (self.key) |key| self.owner.a.free(key);
        }
        pub fn next(self: *@This()) !?tree.Mutation {
            if (self.cursor) |cursor| {
                const record = try cursor.next() orelse return null;
                if (record.value.len < 8) return error.InvalidNativeLakeTermDirectory;
                self.sign = std.mem.readInt(i64, record.value[0..8], .big);
                if (self.sign != 1 and self.sign != -1) return error.InvalidNativeLakeTermDirectory;
                return .{ .key = record.key, .value = if (self.sign == 1) record.value[8..] else null };
            }
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
    pub fn enableDurable(self: *Builder, store: tree.Store) void {
        self.durable = .{ .a = self.a, .store = store, .policy = .contribution };
        self.changes.shared_runs = true;
    }
    fn disable(self: *Builder) void {
        self.disabled = true;
        const manager = self.changes.manager;
        self.changes.deinit();
        self.changes = local.sql_spill.Sort.init(self.a, manager, &.{.{}}, 4 * 1024 * 1024);
        self.durable = null;
    }
    pub fn flush(self: *Builder) !void {
        if (self.changes.total == 0) return;
        const forest = if (self.durable) |*value| value else return error.InvalidNativeLakeDeltaRun;
        try self.changes.finish();
        var changes: Changes = .{ .owner = self, .scratch = .init(self.a) };
        defer changes.deinit();
        const Source = struct {
            changes: *Changes,
            value: [8 + stats.Summary.max_bytes]u8 = undefined,
            pub fn next(source: *@This()) !?tree.Cursor.Record {
                const record = try source.changes.next() orelse return null;
                std.mem.writeInt(i64, source.value[0..8], source.changes.sign, .big);
                const payload = record.value orelse "";
                @memcpy(source.value[8..][0..payload.len], payload);
                return .{ .key = record.key, .value = source.value[0 .. 8 + payload.len] };
            }
        };
        var source: Source = .{ .changes = &changes };
        try forest.add(&source);
        const manager = self.changes.manager;
        const memory_bytes = self.changes.memory_bytes;
        self.changes.deinit();
        self.changes = local.sql_spill.Sort.init(self.a, manager, &.{.{}}, memory_bytes);
        self.changes.shared_runs = true;
    }
    pub fn finish(self: *Builder, store: tree.Store, prior: Roots) !Roots {
        return self.finishImpl(store, prior) catch |err| {
            // Optional acceleration must fail complete: no partial directory
            // can be advertised as proof of term absence or score bounds.
            if (err == error.SqlProgramLimitExceeded) {
                self.disable();
                return .{ .version = 2 };
            }
            return err;
        };
    }
    fn finishImpl(self: *Builder, store: tree.Store, prior: Roots) !Roots {
        if (self.disabled) return .{ .version = 2 };
        var delta_cursor: ?tree.Cursor = null;
        defer if (delta_cursor) |*value| value.deinit();
        var run: ?tree.Ref = null;
        if (self.durable) |*forest| {
            try self.flush();
            run = try forest.finish();
            delta_cursor = try tree.Cursor.init(self.a, store, run, "", null);
        } else try self.changes.finish();
        var changes: Changes = .{ .owner = self, .cursor = if (delta_cursor) |*value| value else null, .scratch = std.heap.ArenaAllocator.init(self.a) };
        defer changes.deinit();
        var roots = prior;
        roots.version = 2;
        var leaf_store = store;
        leaf_store.reduce = reduce;
        if (prior.leaves != null) {
            if (prior.version != 2) return error.InvalidNativeLakeTermDirectory;
            while (try changes.next()) |_| self.distinct_changes += 1;
            if (self.distinct_changes == 0) return prior;
            changes.row = null;
            _ = changes.scratch.reset(.free_all);
            if (delta_cursor) |*value| {
                value.deinit();
                value.* = try tree.Cursor.init(self.a, store, run, "", null);
            } else try self.changes.rewind();
            self.selected = if (self.strategy == .auto) (if (self.distinct_changes >= @max(4096, prior.leaves.?.records / 4)) .merge else .cow) else self.strategy;
        }
        if (prior.leaves == null) {
            const Initial = struct {
                changes: *Changes,
                branches: *tree.SortedBuilder,
                key: ?[]u8 = null,
                branch: ?Branch = null,
                fn flush(source: *@This()) !void {
                    var buffer: [Branch.max_bytes]u8 = undefined;
                    if (source.key) |key| try source.branches.add(key, source.branch.?.encode(&buffer));
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
                        try source.branch.?.merge(.{ .first = id, .last = id, .envelope = summary });
                    }
                    return .{ .key = row.key, .value = value };
                }
            };
            // Leaf order already groups each term/bucket. Emit reductions
            // directly into a bounded page builder, with no second disk sort.
            var branch_builder: tree.SortedBuilder = .{ .alloc = self.a, .store = store };
            defer branch_builder.deinit();
            var initial: Initial = .{ .changes = &changes, .branches = &branch_builder };
            defer if (initial.key) |key| self.a.free(key);
            roots.leaves = try tree.buildSorted(self.a, leaf_store, &initial);
            roots.branches = try branch_builder.finish();
            return roots;
        } else if (self.selected == .merge) {
            const Merge = struct {
                old: *tree.Cursor,
                changes: *Changes,
                left: ?tree.Cursor.Record = null,
                right: ?tree.Mutation = null,
                advance_left: bool = true,
                advance_right: bool = true,
                pub fn next(source: *@This()) !?tree.Cursor.Record {
                    while (true) {
                        if (source.advance_left) source.left = try source.old.next();
                        if (source.advance_right) source.right = try source.changes.next();
                        source.advance_left = false;
                        source.advance_right = false;
                        if (source.left == null and source.right == null) return null;
                        const order: std.math.Order = if (source.left == null) .gt else if (source.right == null) .lt else std.mem.order(u8, source.left.?.key, source.right.?.key);
                        source.advance_left = order != .gt;
                        source.advance_right = order != .lt;
                        if (order == .lt) return source.left;
                        if (source.right.?.value) |value| return .{ .key = source.right.?.key, .value = value };
                    }
                }
            };
            var cursor = try tree.Cursor.init(self.a, store, prior.leaves, "", null);
            defer cursor.deinit();
            var merged: Merge = .{ .old = &cursor, .changes = &changes };
            roots.leaves = try tree.buildSorted(self.a, leaf_store, &merged);
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
                if (mutations.items.len >= 4096 or bytes >= 2 * 1024 * 1024) {
                    roots.leaves = try tree.apply(self.a, leaf_store, roots.leaves, mutations.items);
                    _ = batch.reset(.free_all);
                    mutations = .empty;
                    bytes = 0;
                }
            }
            if (mutations.items.len != 0) roots.leaves = try tree.apply(self.a, leaf_store, roots.leaves, mutations.items);
        }
        // Replay sorted changes after updating leaves. Adjacent segment IDs
        // share a bucket, so deduplicate before reducing or allocating mutations.
        changes.row = null;
        _ = changes.scratch.reset(.free_all);
        if (delta_cursor) |*value| {
            value.deinit();
            value.* = try tree.Cursor.init(self.a, store, run, "", null);
        } else try self.changes.rewind();
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
            const change = (try changes.next()) orelse break;
            const key = change.key[0 .. change.key.len - 31];
            if (previous) |old| if (std.mem.eql(u8, old, key)) continue;
            if (previous) |old| self.a.free(old);
            previous = null;
            previous = try self.a.dupe(u8, key);
            var branch: ?Branch = null;
            {
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
                const Fold = struct {
                    owner: *Builder,
                    value: ?Branch = null,
                    fn add(fold: *@This(), value: Branch) !void {
                        if (fold.value) |*old| try old.merge(value) else fold.value = value;
                    }
                    pub fn aggregate(fold: *@This(), bytes_: []const u8, records: u64) !void {
                        const value = try Branch.decode(bytes_);
                        if (value.count != records) return error.InvalidNativeLakeTermDirectory;
                        fold.owner.aggregate_nodes += 1;
                        try fold.add(value);
                    }
                    pub fn record(fold: *@This(), key_: []const u8, value_: []const u8) !void {
                        if (key_.len < 34) return error.InvalidNativeLakeTermDirectory;
                        const id: [32]u8 = key_[key_.len - 32 ..][0..32].*;
                        fold.owner.reduced_records += 1;
                        try fold.add(.{ .first = id, .last = id, .envelope = try stats.Summary.decode(value_) });
                    }
                };
                var fold: Fold = .{ .owner = self };
                try tree.reduceRange(self.a, cache.store(), roots.leaves, key, end, &fold);
                branch = fold.value;
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

test "external lake text statistics subtree reductions bound small updates and dense directory merges agree" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const key = "\x00\x00\x00\x04bodycommon";
    const summary: stats.Summary = .{ .frequency = 2, .max_frequency = 3, .min_norm = 10 };
    var buffer: [stats.Summary.max_bytes]u8 = undefined;
    const record: tree.Cursor.Record = .{ .key = key, .value = summary.encode(&buffer) };
    var initial = Builder.init(a, &manager);
    defer initial.deinit();
    for (0..10000) |i| {
        var id: [32]u8 = @splat(255);
        std.mem.writeInt(u64, id[24..32], i, .big);
        try initial.addRecord(record, id, 1);
    }
    const prior = try initial.finish(memory.store(), .{});
    var cow = Builder.init(a, &manager);
    defer cow.deinit();
    cow.strategy = .cow;
    var dense = Builder.init(a, &manager);
    defer dense.deinit();
    dense.strategy = .merge;
    for ([_]*Builder{ &cow, &dense }) |builder| {
        var id: [32]u8 = @splat(255);
        std.mem.writeInt(u64, id[24..32], 5000, .big);
        try builder.addRecord(record, id, -1);
        std.mem.writeInt(u64, id[24..32], 10000, .big);
        try builder.addRecord(record, id, 1);
    }
    const current = try cow.finish(memory.store(), prior);
    const merged = try dense.finish(memory.store(), prior);
    try std.testing.expect(cow.aggregate_nodes > 0);
    try std.testing.expect(cow.reduced_records < 1000);
    try std.testing.expectEqual(@as(u64, 2), cow.distinct_changes);
    var left = try tree.Cursor.init(a, memory.store(), current.leaves, "", null);
    defer left.deinit();
    var right = try tree.Cursor.init(a, memory.store(), merged.leaves, "", null);
    defer right.deinit();
    while (try left.next()) |row| {
        const other = (try right.next()).?;
        try std.testing.expectEqualSlices(u8, row.key, other.key);
        try std.testing.expectEqualSlices(u8, row.value, other.value);
    }
    try std.testing.expect((try right.next()) == null);
    var branch_cursor = try tree.Cursor.init(a, memory.store(), current.branches, "", null);
    defer branch_cursor.deinit();
    const branch = try Branch.decode((try branch_cursor.next()).?.value);
    try std.testing.expectEqual(@as(u64, 10000), branch.count);
    try std.testing.expectEqual(@as(u8, 255), branch.first[0]);
}

test "external lake text statistics durable directory bounds temporary disk and disables exhausted acceleration" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check, .max_bytes = 256 * 1024, .compression = .none };
    defer manager.deinit();
    var builder = Builder.init(a, &manager);
    defer builder.deinit();
    builder.enableDurable(memory.store());
    builder.changes.memory_bytes = 32 * 1024;
    var buffer: [stats.Summary.max_bytes]u8 = undefined;
    const summary = (stats.Summary{ .frequency = 1, .max_frequency = 1, .min_norm = 2 }).encode(&buffer);
    for (0..4096) |index| {
        var id: [32]u8 = @splat(0);
        std.mem.writeInt(u64, id[24..32], index, .big);
        try builder.addRecord(.{ .key = "common", .value = summary }, id, 1);
        if (index % 64 == 63) try builder.flush();
    }
    const roots = try builder.finish(memory.store(), .{});
    try std.testing.expect(!builder.disabled);
    try std.testing.expectEqual(@as(u64, 4096), roots.leaves.?.records);
    try std.testing.expect(manager.peak_bytes <= manager.max_bytes);
    try std.testing.expect(manager.written_bytes > manager.max_bytes);
    var constrained = Builder.init(a, &manager);
    defer constrained.deinit();
    constrained.changes.memory_bytes = 0;
    manager.max_bytes = 0;
    try constrained.addRecord(.{ .key = "common", .value = summary }, @splat(1), 1);
    const fallback = try constrained.finish(memory.store(), roots);
    try std.testing.expect(constrained.disabled);
    try std.testing.expect(fallback.leaves == null and fallback.branches == null);
}
