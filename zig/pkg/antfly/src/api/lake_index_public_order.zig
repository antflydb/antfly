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

//! Public-ID ordered seeks over native tuple/file counts and physical rows.
//! A tie group visits only participating files and the requested row window.
//! Snapshot-bound public digests are computed once; retained row pages remain
//! reusable when a new snapshot changes the digest permutation of the files.
const std = @import("std");
const local = @import("antfly_local_sources");
const ordered = @import("lake_index_ordered_rows.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Cursor = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    group_arena: std.heap.ArenaAllocator,
    file_arena: std.heap.ArenaAllocator,
    directory: ?tree.Cursor = null,
    pending: ?tree.Cursor.Record = null,
    reader: *ordered.Reader,
    navigation: ?*Navigation = null,
    digests: []const [32]u8,
    group_digests: std.AutoHashMapUnmanaged(u32, [32]u8) = .empty,
    boundary_digest: ?[32]u8 = null,
    lower: []const u8,
    upper: ?[]const u8,
    boundary: ?[]const u8,
    boundary_file: ?u32 = null,
    boundary_coordinate: [12]u8 = @splat(0),
    reverse: bool,
    reverse_ids: bool,
    group: []const u8 = "",
    files: []const u32 = &.{},
    file_position: usize = 0,
    rows: ?tree.Cursor = null,
    exhausted: bool = false,
    /// One statement-owned cache amortizes seeks across distinct tie groups.
    /// It retains immutable pages only, with the original query's read context.
    const Navigation = struct {
        reader: *ordered.Reader,
        context: ?local.serverless_query_lake_read_context.Context,
        cache: tree.Cache,
        fn check(raw: *anyopaque) !void {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            if (owner.context) |context| try context.ensureActive();
            try owner.reader.pages.cancellation.check();
        }
        fn get(raw: *anyopaque, a_: A, ref: tree.Ref) ![]u8 {
            const owner: *@This() = @ptrCast(@alignCast(raw));
            try check(raw);
            const backing = owner.reader.pages.store();
            return backing.get(backing.ptr, a_, ref);
        }
        fn put(_: *anyopaque, _: tree.Ref, _: []const u8) !void {
            return error.InvalidNativeLakeRowIndex;
        }
    };
    pub fn init(a: A, reader: *ordered.Reader, lower: []const u8, upper: ?[]const u8, boundary: ?[]const u8, id: ?[]const u8, id_descending: bool, before: bool) !Cursor {
        var result: Cursor = .{ .a = a, .arena = .init(a), .group_arena = .init(a), .file_arena = .init(a), .reader = reader, .digests = &.{}, .lower = undefined, .upper = null, .boundary = null, .reverse = before, .reverse_ids = id_descending != before };
        errdefer result.deinit();
        const navigation = try a.create(Navigation);
        navigation.* = .{ .reader = reader, .context = if (reader.cached) |cached| cached.context else null, .cache = .{ .alloc = a, .underlying = .{ .domain = reader.root.domain, .ptr = navigation, .get = Navigation.get, .put = Navigation.put, .check = Navigation.check } } };
        result.navigation = navigation;
        const ca = result.arena.allocator();
        result.lower = try ca.dupe(u8, lower);
        result.upper = if (upper) |end| try ca.dupe(u8, end) else null;
        if (reader.root.file_dictionary == null and reader.root.public_digests.len != reader.root.files.len) return error.InvalidNativeLakeRowIndex;
        const digests = reader.root.public_digests;
        result.digests = digests;
        if (boundary) |prefix| {
            const value = id orelse return error.InvalidRelationalIndexBound;
            if (value.len != 96 or !std.mem.startsWith(u8, value, "lake1:") or value[70] != ':' or value[79] != ':') return error.InvalidRelationalIndexBound;
            for (value, 0..) |byte, i| if (i >= 6 and i != 70 and i != 79 and !((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidRelationalIndexBound;
            const group = std.fmt.parseUnsigned(u32, value[71..79], 16) catch return error.InvalidRelationalIndexBound;
            const row = std.fmt.parseUnsigned(u64, value[80..96], 16) catch return error.InvalidRelationalIndexBound;
            var digest_bytes: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest_bytes, value[6..70]) catch return error.InvalidRelationalIndexBound;
            result.boundary_file = try reader.publicSlot(ca, digest_bytes);
            result.boundary_digest = digest_bytes;
            if (result.boundary_file == null) return error.InvalidRelationalIndexBound;
            result.boundary = try ca.dupe(u8, prefix);
            std.mem.writeInt(u32, result.boundary_coordinate[0..4], group, .big);
            std.mem.writeInt(u64, result.boundary_coordinate[4..12], row, .big);
            if (before) {
                if (try orderedSuccessor(ca, prefix)) |end| {
                    if (result.upper == null or std.mem.order(u8, end, result.upper.?) == .lt) result.upper = end;
                }
            } else if (std.mem.order(u8, prefix, result.lower) == .gt) result.lower = result.boundary.?;
        }
        if (result.upper) |end| if (std.mem.order(u8, result.lower, end) != .lt) {
            result.exhausted = true;
        };
        if (!result.exhausted) result.directory = if (before)
            try tree.Cursor.initReverse(a, result.navigation.?.cache.store(), reader.root.ties, result.lower, result.upper)
        else
            try tree.Cursor.init(a, result.navigation.?.cache.store(), reader.root.ties, result.lower, result.upper);
        return result;
    }
    pub fn deinit(self: *Cursor) void {
        if (self.rows) |*rows| rows.deinit();
        self.group_arena.deinit();
        if (self.navigation) |navigation| {
            navigation.cache.deinit();
            self.a.destroy(navigation);
            self.navigation = null;
        }
        self.file_arena.deinit();
        if (self.directory) |*directory| directory.deinit();
        self.arena.deinit();
    }
    fn loadGroup(self: *Cursor) !bool {
        if (self.exhausted) return false;
        _ = self.group_arena.reset(.retain_capacity);
        self.group_digests = .empty;
        const ca = self.group_arena.allocator();
        const first = (self.pending orelse try self.directory.?.next()) orelse {
            self.exhausted = true;
            return false;
        };
        self.pending = null;
        if (first.key.len < 4) return error.InvalidNativeLakeRowIndex;
        self.group = try ca.dupe(u8, first.key[0 .. first.key.len - 4]);
        var cache_key: ?[32]u8 = null;
        if (self.reader.cached) |cached| {
            try cached.context.ensureActive();
            try self.reader.pages.cancellation.check();
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update("antfly.native-public-tie-files.v1");
            hash.update(&cached.scope);
            hash.update(&self.reader.root.domain);
            hash.update(&self.reader.root.fingerprint);
            const root = self.reader.root.ties orelse return error.InvalidNativeLakeRowIndex;
            // JSON includes the complete immutable tree identity, independent of
            // pointer addresses. Length framing keeps each identity component exact.
            const identity = try std.json.Stringify.valueAlloc(ca, root, .{});
            for ([_][]const u8{ identity, self.reader.root.source, self.reader.root.snapshot, self.group }) |part| {
                var length: [8]u8 = undefined;
                std.mem.writeInt(u64, &length, part.len, .little);
                hash.update(&length);
                hash.update(part);
            }
            var key: [32]u8 = undefined;
            hash.final(&key);
            cache_key = key;
            if (cached.cache.decoded.lookup(key)) |lease| {
                defer lease.release();
                const value: *const GroupFiles = @ptrCast(@alignCast(lease.item.payload.extension));
                self.files = try ca.dupe(u32, value.files);
                // Only a warm group needs a seek. A cold scan keeps its existing
                // directory pages and the first record of the following group.
                self.directory.?.deinit();
                self.directory = null;
                self.directory = if (self.reverse)
                    try tree.Cursor.initReverse(self.a, self.navigation.?.cache.store(), self.reader.root.ties, self.lower, self.group)
                else if (try orderedSuccessor(ca, self.group)) |end|
                    try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.ties, end, self.upper)
                else blk: {
                    self.exhausted = true;
                    break :blk null;
                };
                try self.prepareDigests(ca);
                self.seekBoundary();
                return true;
            }
        }
        self.files = try self.readGroup(ca, first);
        // Small groups already fit in a directory page: reseeking a cached one
        // would cost more reads than sequential traversal. Reserve residency
        // for groups whose encoded records span pages.
        if (self.files.len > tree.target_page_bytes / (self.group.len + 32)) if (self.reader.cached) |cached| {
            const Loader = struct {
                files: []const u32,
                fn load(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
                    const ctx: *@This() = @ptrCast(@alignCast(raw));
                    const alloc = item.arena.allocator();
                    const value = try alloc.create(GroupFiles);
                    value.files = try alloc.dupe(u32, ctx.files);
                    item.payload = .{ .extension = value };
                }
            };
            var loader: Loader = .{ .files = self.files };
            const Check = struct {
                parent: local.serverless_query_lake_read_context.Context,
                token: @import("antfly_cancellation").CancellationToken,
                fn check(raw: *anyopaque) !void {
                    const ctx: *@This() = @ptrCast(@alignCast(raw));
                    try ctx.parent.ensureActive();
                    try ctx.token.check();
                }
            };
            var check: Check = .{ .parent = cached.context, .token = self.reader.pages.cancellation };
            var context = cached.context;
            context.checkpoint = .{ .ptr = &check, .check = Check.check };
            const lease = try cached.cache.decoded.acquire(cache_key.?, 512 * 1024, context, .{ .ptr = &loader, .load = Loader.load });
            lease.release();
        };
        self.seekBoundary();
        return true;
    }
    const GroupFiles = struct { files: []const u32 };
    fn readGroup(self: *Cursor, a: A, first: tree.Cursor.Record) ![]const u32 {
        var files: std.ArrayList(u32) = .empty;
        errdefer files.deinit(a);
        var record_next: ?tree.Cursor.Record = first;
        while (record_next) |record| {
            if (files.items.len % 256 == 0) {
                try self.reader.pages.cancellation.check();
                if (self.reader.cached) |cached| try cached.context.ensureActive();
            }
            if (record.key.len < 4) return error.InvalidNativeLakeRowIndex;
            if (record.key.len != self.group.len + 4 or !std.mem.eql(u8, record.key[0..self.group.len], self.group)) {
                self.pending = record;
                break;
            }
            if (record.value.len != 8 or std.mem.readInt(u64, record.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
            const slot = std.mem.readInt(u32, record.key[record.key.len - 4 ..][0..4], .big);
            if (slot >= self.reader.root.fileCount() or files.items.len >= self.reader.root.fileCount()) return error.InvalidNativeLakeRowIndex;
            try files.append(a, slot);
            record_next = try self.directory.?.next();
        }
        self.files = files.items;
        try self.prepareDigests(a);
        const Order = struct {
            digests: *const std.AutoHashMapUnmanaged(u32, [32]u8),
            fn less(order: @This(), x: u32, y: u32) bool {
                return std.mem.order(u8, &order.digests.get(x).?, &order.digests.get(y).?) == .lt;
            }
        };
        std.mem.sort(u32, files.items, Order{ .digests = &self.group_digests }, Order.less);
        return files.toOwnedSlice(a);
    }
    fn prepareDigests(self: *Cursor, a: A) !void {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        for (self.files) |slot| {
            _ = scratch.reset(.retain_capacity);
            try self.group_digests.put(a, slot, try self.reader.publicDigest(scratch.allocator(), slot));
        }
    }
    fn seekBoundary(self: *Cursor) void {
        self.file_position = 0;
        const boundary = self.boundary orelse return;
        if (!std.mem.eql(u8, self.group, boundary)) return;
        const digest = self.boundary_digest.?;
        var lo: usize = 0;
        var hi = self.files.len;
        // Lower bound in traversal order, including the boundary file itself:
        // its row-coordinate seek decides which rows remain admissible.
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const pos = if (self.reverse_ids) self.files.len - mid - 1 else mid;
            const relation = std.mem.order(u8, &self.group_digests.get(self.files[pos]).?, &digest);
            if (relation == (if (self.reverse_ids) std.math.Order.gt else .lt)) lo = mid + 1 else hi = mid;
        }
        self.file_position = lo;
    }
    fn openFile(self: *Cursor, slot: u32) !bool {
        _ = self.file_arena.reset(.retain_capacity);
        const ca = self.file_arena.allocator();
        var file: [4]u8 = undefined;
        std.mem.writeInt(u32, &file, slot, .big);
        var lower: []const u8 = try std.mem.concat(ca, u8, &.{ self.group, &file });
        var upper = try orderedSuccessor(ca, lower);
        if (self.boundary) |boundary| if (std.mem.eql(u8, self.group, boundary)) {
            const relation = std.mem.order(u8, &self.group_digests.get(slot).?, &self.boundary_digest.?);
            if (relation == (if (self.reverse_ids) std.math.Order.gt else .lt)) return false;
            if (relation == .eq) {
                const key = try std.mem.concat(ca, u8, &.{ lower, &self.boundary_coordinate });
                if (self.reverse_ids) upper = key else lower = (try orderedSuccessor(ca, key)) orelse return false;
            }
        };
        self.rows = if (self.reverse_ids)
            try tree.Cursor.initReverse(self.a, self.navigation.?.cache.store(), self.reader.root.page, lower, upper)
        else
            try tree.Cursor.init(self.a, self.navigation.?.cache.store(), self.reader.root.page, lower, upper);
        return true;
    }
    pub fn next(self: *Cursor, a: A, count: usize) ![]const local.storage_rowsource_types.RowRef {
        if (count == 0 or count > 65536) return error.InvalidNativeLakeRowIndex;
        var result: std.ArrayList(local.storage_rowsource_types.RowRef) = .empty;
        errdefer result.deinit(a);
        while (result.items.len < count) {
            if (self.rows) |*rows| {
                if (try rows.next()) |record| {
                    try result.append(a, try self.reader.decode(a, record));
                    continue;
                }
                rows.deinit();
                self.rows = null;
            }
            if (self.file_position < self.files.len) {
                const position = if (self.reverse_ids) self.files.len - self.file_position - 1 else self.file_position;
                const slot = self.files[position];
                self.file_position += 1;
                _ = try self.openFile(slot);
                continue;
            }
            if (!try self.loadGroup()) break;
        }
        return result.toOwnedSlice(a);
    }
};
fn orderedSuccessor(a: A, prefix: []const u8) !?[]const u8 {
    var end = prefix.len;
    while (end != 0) {
        end -= 1;
        if (prefix[end] == 255) continue;
        const result = try a.dupe(u8, prefix[0 .. end + 1]);
        result[end] += 1;
        return result;
    }
    return null;
}
