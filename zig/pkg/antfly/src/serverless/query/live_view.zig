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

//! One captured published generation plus a bounded committed WAL tail.
//! Published readers/leases remain caller-owned; this view never reconstructs
//! the base namespace or changes its cutoff when another writer commits.
const std = @import("std");
const A = std.mem.Allocator;
const WalStore = @import("../wal/store.zig").WalStore;
const records = @import("../wal/types.zig");
const visibility = @import("../visibility.zig");
const Overlay = @import("live_overlay.zig").Overlay;

/// A complete native-normalized interval installed by an exclusive commit owner.
/// This is caller-admitted data, never a proof or an untrusted client option.
pub const AdmittedTail = struct {
    scope: [32]u8,
    start_lsn: []const u8,
    end_lsn: []const u8,
    mutations: []const @import("../api/types.zig").DocumentMutation,
};

pub const View = struct {
    overlay: Overlay,
    token: visibility.Token,
    published_lsn: u64,
    point_id: ?[]const u8 = null,

    pub const Options = struct {
        required: ?visibility.Token = null,
        admitted_tail: ?AdmittedTail = null,
        /// A resumed page retains its original cutoff, even if HEAD advances.
        cutoff: ?u64 = null,
        max_records: usize = 4096,
        body_resolver: ?Overlay.BodyResolver = null,
        max_bytes: usize = 8 * 1024 * 1024,
        cancellation: ?@import("objectstore").CancellationToken = null,
        state_cache: ?*@import("read_state_cache.zig").Cache = null,
        cache_allocator: ?A = null,
        point_id: ?[]const u8 = null,
    };
    pub fn open(alloc: A, wal: *WalStore, incarnation: []const u8, published_lsn: u64, options: Options) !View {
        if (options.cancellation) |token| try token.check();
        if (options.admitted_tail) |tail| {
            const first = try std.fmt.parseInt(u64, tail.start_lsn, 10);
            const end = try std.fmt.parseInt(u64, tail.end_lsn, 10);
            if (first == 0 or end < first or end - first + 1 != tail.mutations.len or tail.mutations.len > 128 or !std.mem.eql(u8, &tail.scope, &visibility.Token.init(incarnation, 0).scope)) return error.InvalidAdmittedTail;
            const latest = try wal.latestLsn(incarnation);
            const cutoff = options.cutoff orelse latest;
            if (cutoff > latest) return error.WalVisibilityUnavailable;
            _ = try visibility.Cut.capture(incarnation, published_lsn, cutoff, options.required);
            if (end >= cutoff and first <= published_lsn +| 1 and published_lsn <= cutoff) {
                var arena = std.heap.ArenaAllocator.init(alloc);
                defer arena.deinit();
                const skip: usize = if (published_lsn < first) 0 else @intCast(@min(published_lsn - first, tail.mutations.len - 1) + 1);
                const count: usize = @intCast(cutoff - published_lsn);
                const selected = tail.mutations[@min(skip, tail.mutations.len)..][0..count];
                if (selected.len > options.max_records) return error.WalReadBudgetExceeded;
                const owned = try arena.allocator().alloc(records.Record, selected.len);
                var bytes: usize = 0;
                for (selected, owned, 0..) |mutation, *record, i| {
                    if ((mutation.kind != .upsert and mutation.kind != .delete) or (mutation.kind == .upsert and mutation.body == null) or (mutation.kind == .delete and mutation.body != null)) return error.InvalidAdmittedTail;
                    const payload = try @import("../api/codec.zig").encodeMutationAlloc(arena.allocator(), mutation);
                    bytes = try std.math.add(usize, bytes, payload.len);
                    if (bytes > options.max_bytes) return error.WalReadBudgetExceeded;
                    record.* = .{ .lsn = published_lsn + i + 1, .timestamp_ns = 0, .payload = payload };
                }
                return .{ .overlay = try Overlay.initWithResolver(alloc, owned, published_lsn, cutoff, options.max_bytes, options.body_resolver), .token = visibility.Token.init(incarnation, cutoff), .published_lsn = published_lsn };
            }
        }
        const snapshot = if (options.state_cache != null) try wal.readSnapshot(incarnation) else null;
        const latest = if (snapshot) |value| value.latest_lsn else try wal.latestLsn(incarnation);
        const cutoff = options.cutoff orelse latest;
        if (cutoff > latest) return error.WalVisibilityUnavailable;
        const cut = try visibility.Cut.capture(incarnation, published_lsn, cutoff, options.required);
        if (options.state_cache) |cache| if (snapshot) |state| {
            if (cutoff == state.latest_lsn) if (try cache.prepare(alloc, incarnation, published_lsn, state, options.max_records, options.max_bytes, options.body_resolver, wal, options.cancellation)) |prepared| {
                var overlay = prepared;
                errdefer overlay.deinit();
                if (options.cache_allocator) |persistent| cache.remember(persistent, incarnation, published_lsn, state, &overlay) catch |err| switch (err) {
                    error.OutOfMemory => {},
                    else => return err,
                };
                return .{ .overlay = overlay, .token = visibility.Token.init(incarnation, cutoff), .published_lsn = published_lsn };
            };
        };
        // Tiny hot tails are cheaper to materialize once and reuse than to
        // independently seek the document projection on every cache miss. The
        // probe is bounded and only its explicit capacity signal selects the
        // point path; integrity, expiry and transport failures still fail.
        const warm_tail: ?[]records.Record = if (options.point_id != null and options.state_cache != null and snapshot != null and snapshot.?.small_tail and cutoff == snapshot.?.latest_lsn and cutoff > published_lsn and cutoff - published_lsn <= @min(options.max_records, 16) and options.state_cache.?.canWarm(incarnation, published_lsn, snapshot.?, @min(options.max_bytes, 64 * 1024)))
            wal.readRangeAlloc(incarnation, .{ .start_lsn = published_lsn + 1, .end_lsn = cutoff, .max_records = @min(options.max_records, 16), .max_bytes = @min(options.max_bytes, 64 * 1024), .cancellation = options.cancellation }) catch |err| switch (err) {
                error.WalReadBudgetExceeded => blk: {
                    options.state_cache.?.noteOversized(incarnation, published_lsn, snapshot.?, @min(options.max_bytes, 64 * 1024));
                    break :blk null;
                },
                else => return err,
            }
        else
            null;
        if (warm_tail == null) if (options.point_id) |id| if (cut.visible_lsn > cut.published_lsn) {
            const point = try wal.readPointAlloc(incarnation, id, .{ .start_lsn = cut.published_lsn + 1, .end_lsn = cut.visible_lsn, .max_records = options.max_records, .max_bytes = options.max_bytes, .cancellation = options.cancellation });
            if (point.record) |record| {
                defer {
                    wal.allocator.free(record.payload);
                    if (record.operation_id) |op| wal.allocator.free(op);
                }
                if (!point.indexed or record.lsn <= published_lsn or record.lsn > cutoff) return error.InvalidWal;
                const overlay = try Overlay.initWithResolver(alloc, &.{record}, record.lsn - 1, record.lsn, options.max_bytes, options.body_resolver);
                return .{ .overlay = overlay, .token = visibility.Token.init(incarnation, cutoff), .published_lsn = published_lsn, .point_id = id };
            } else if (point.indexed) {
                const overlay = try Overlay.initWithResolver(alloc, &.{}, cutoff, cutoff, options.max_bytes, options.body_resolver);
                return .{ .overlay = overlay, .token = visibility.Token.init(incarnation, cutoff), .published_lsn = published_lsn, .point_id = id };
            }
        };
        const tail = if (warm_tail) |value| value else if (cut.visible_lsn == cut.published_lsn)
            try alloc.alloc(records.Record, 0)
        else blk: {
            // WalStore owns its allocator. Give the returned slice the same
            // allocator while the overlay adopts independent caller ownership.
            break :blk try wal.readRangeAlloc(incarnation, .{
                .start_lsn = cut.published_lsn + 1,
                .end_lsn = cut.visible_lsn,
                .max_records = options.max_records,
                .max_bytes = options.max_bytes,
                .cancellation = options.cancellation,
            });
        };
        defer records.freeRecords(if (cut.visible_lsn == cut.published_lsn) alloc else wal.allocator, tail);
        var overlay = try Overlay.initWithResolver(alloc, tail, cut.published_lsn, cut.visible_lsn, options.max_bytes, options.body_resolver);
        errdefer overlay.deinit();
        if (options.state_cache) |cache| if (snapshot) |state| {
            if (cutoff == state.latest_lsn) if (options.cache_allocator) |persistent| cache.remember(persistent, incarnation, published_lsn, state, &overlay) catch |err| switch (err) {
                error.OutOfMemory => {},
                else => return err,
            };
        };
        return .{ .overlay = overlay, .token = visibility.Token.init(incarnation, cutoff), .published_lsn = published_lsn };
    }
    pub fn deinit(self: *View) void {
        self.overlay.deinit();
        self.* = undefined;
    }

    /// Return an owned current body. The provider performs an authenticated
    /// point read from the caller's pinned base only when the tail is silent.
    pub fn lookupAlloc(self: *View, id: []const u8, base: anytype) !?[]u8 {
        if (self.point_id) |selected| if (!std.mem.eql(u8, selected, id)) return error.PointReadIdentityMismatch;
        return switch (try self.overlay.lookup(id)) {
            .deleted => null,
            .present => |body| try self.overlay.alloc.dupe(u8, body),
            .unchanged => try base.lookupAlloc(self.overlay.alloc, id),
        };
    }
};

test "serverless live view fences cross-instance reads before publication and preserves resumed cutoffs" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const Indexed = @import("../wal/indexed_object_store.zig").IndexedObjectStore;
    var writer = try Indexed.init(a, client, "db", "scope");
    defer writer.deinit();
    var reader = try Indexed.init(a, client, "db", "scope");
    defer reader.deinit();
    var wal = reader.walStore();
    const codec = @import("../api/codec.zig");
    const first = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "a", .body = "first" });
    defer a.free(first);
    _ = try writer.appendIdempotent("incarnation", 1, first, "op-1");
    const token = visibility.Token.init("incarnation", 1);
    var view = try View.open(a, &wal, "incarnation", 0, .{ .required = token });
    defer view.deinit();
    const Base = struct {
        fn lookupAlloc(_: @This(), _: A, _: []const u8) !?[]u8 {
            return error.UnexpectedBaseRead;
        }
    };
    const body = (try view.lookupAlloc("a", Base{})).?;
    defer a.free(body);
    try std.testing.expectEqualStrings("first", body);
    const second = try codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "a", .body = "second" });
    defer a.free(second);
    _ = try writer.appendIdempotent("incarnation", 2, second, "op-2");
    var resumed = try View.open(a, &wal, "incarnation", 0, .{ .cutoff = 1, .required = token });
    defer resumed.deinit();
    const old = (try resumed.lookupAlloc("a", Base{})).?;
    defer a.free(old);
    try std.testing.expectEqualStrings("first", old);
    try std.testing.expectError(error.VisibilityIncarnationMismatch, View.open(a, &wal, "other-incarnation", 0, .{ .required = token }));
    try std.testing.expectError(error.WalVisibilityUnavailable, View.open(a, &wal, "incarnation", 0, .{ .required = visibility.Token.init("incarnation", 3) }));
    try std.testing.expectError(error.WalReadBudgetExceeded, View.open(a, &wal, "incarnation", 0, .{ .max_records = 1 }));
}

test "resident serving shard admitted tail preserves updates tombstones gaps and current fences" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var indexed = try @import("../wal/indexed_object_store.zig").IndexedObjectStore.init(a, client, "db", "admitted");
    defer indexed.deinit();
    var wal = indexed.walStore();
    const Mutation = @import("../api/types.zig").DocumentMutation;
    const mutations = [_]Mutation{ .{ .kind = .upsert, .doc_id = "one", .body = "first" }, .{ .kind = .delete, .doc_id = "one" } };
    for (mutations, 0..) |mutation, i| {
        const payload = try @import("../api/codec.zig").encodeMutationAlloc(a, mutation);
        defer a.free(payload);
        _ = try indexed.appendIdempotent("ns", i + 1, payload, if (i == 0) "one" else "two");
    }
    const Base = struct {
        fn lookupAlloc(_: @This(), alloc: A, _: []const u8) !?[]u8 {
            return try alloc.dupe(u8, "base");
        }
    };
    const tail = AdmittedTail{ .scope = visibility.Token.init("ns", 0).scope, .start_lsn = "1", .end_lsn = "2", .mutations = &mutations };
    var view = try View.open(a, &wal, "ns", 0, .{ .admitted_tail = tail, .required = visibility.Token.init("ns", 2) });
    defer view.deinit();
    try std.testing.expect((try view.lookupAlloc("one", Base{})) == null);
    const unchanged = (try view.lookupAlloc("other", Base{})).?;
    defer a.free(unchanged);
    try std.testing.expectEqualStrings("base", unchanged);
    try std.testing.expectError(error.WalVisibilityUnavailable, View.open(a, &wal, "ns", 0, .{ .admitted_tail = tail, .required = visibility.Token.init("ns", 3) }));
    var wrong = tail;
    wrong.scope = @splat(0);
    try std.testing.expectError(error.InvalidAdmittedTail, View.open(a, &wal, "ns", 0, .{ .admitted_tail = wrong }));
    var gap = tail;
    gap.start_lsn = "2";
    gap.mutations = mutations[1..];
    var fallback = try View.open(a, &wal, "ns", 0, .{ .admitted_tail = gap });
    defer fallback.deinit();
    try std.testing.expect((try fallback.lookupAlloc("one", Base{})) == null);
}
