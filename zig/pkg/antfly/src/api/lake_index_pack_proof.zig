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

//! Query-owned sorted pack proof. Compressed index pages stream into bounded
//! merge runs; the deduplicated 32-byte keys support random frontier range seeks.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const routes = @import("lake_index_source_routes.zig");
const Resolver = @import("lake_index_text_predicate.zig").PhysicalResolver;
const Id = [32]u8;
const A = std.mem.Allocator;
const Self = @This();
budget: local.sql_memory_budget,
manager: local.sql_spill.Manager = undefined,
file: ?local.sql_spill.File = null,
count: u64 = 0,
token: @import("antfly_cancellation").CancellationToken,
fn check(raw: *anyopaque) !void {
    try @as(*Self, @ptrCast(@alignCast(raw))).token.check();
}
pub fn create(root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, resolver: Resolver, json: []const u8, token: @import("antfly_cancellation").CancellationToken) !?*Self {
    if (root.source_routes_version != 1 or cached.context.io == null) return null;
    const self = try std.heap.page_allocator.create(Self);
    self.* = .{ .budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), .limit = 16 * 1024 * 1024 }, .token = token };
    const a = self.budget.allocator();
    self.manager = .{ .alloc = a, .io = cached.context.io.?, .context = self, .checkpoint = check };
    errdefer self.deinit();
    var predicate = (try resolver.selectivePredicate(a, json, std.math.maxInt(usize))) orelse {
        self.deinit();
        return null;
    };
    // Very broad predicates are cheaper as membership probes in text-selected
    // windows. Avoid a complete index census before a selective text query.
    if (root.collection) |collection| if (collection.documents != 0 and predicate.estimatedRows() *| 4 >= @as(u64, collection.documents) * 3) {
        predicate.deinit();
        self.deinit();
        return null;
    };
    defer predicate.deinit();
    var sort = local.sql_spill.Sort.init(a, &self.manager, &.{.{}}, 512 * 1024);
    sort.run_limit = 4;
    defer sort.deinit();
    const Sink = struct {
        sort: *local.sql_spill.Sort,
        pub fn add(sink: *@This(), id: Id) !void {
            try sink.sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &id })}, .values = &.{}, .ordinal = 0 });
        }
    };
    var sink: Sink = .{ .sort = &sort };
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 0;
    var backend = store;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = root.domain, .artifacts = &backend, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var cache: tree.Cache = .{ .alloc = a, .underlying = pages.store(), .max_bytes = 1024 * 1024 };
    defer cache.deinit();
    var window = std.heap.ArenaAllocator.init(a);
    defer window.deinit();
    while (true) {
        try cached.context.ensureActive();
        try token.check();
        _ = window.reset(.free_all);
        const wa = window.allocator();
        var selected = @import("lake_index_physical_set.zig").Set.init(a);
        defer selected.deinit();
        if (predicate.hasBlocks()) {
            const blocks = try predicate.nextBlocks(wa);
            if (blocks.len == 0) break;
            for (blocks) |block| try selected.addBlock(block);
        } else {
            const rows = try predicate.nextPhysical(wa, 1024);
            if (rows.len == 0) break;
            for (rows) |row| {
                if (row != .external) return error.InvalidNativeLakeRowIndex;
                try selected.addRow(row.external.file_id, row.external.row_group_ordinal, row.external.row_ordinal);
            }
        }
        try routes.stream(a, cache.store(), root, store, cached, token, &selected, &sink);
    }
    try self.finish(&sort);
    return self;
}
fn finish(self: *Self, sort: *local.sql_spill.Sort) !void {
    var window = std.heap.ArenaAllocator.init(self.budget.allocator());
    defer window.deinit();
    self.file = try self.manager.create();
    var previous: ?Id = null;
    while (true) {
        _ = window.reset(.free_all);
        const row = (try sort.next(window.allocator())) orelse break;
        const bytes = row.keys[0].value.string;
        if (bytes.len != 32) return error.InvalidNativeLakeTextCorpus;
        const id = bytes[0..32].*;
        if (previous != null and std.mem.eql(u8, &id, &previous.?)) continue;
        try self.file.?.writeRaw(self.count * 32, &id);
        self.count += 1;
        previous = id;
    }
    try self.file.?.seal();
}
pub fn deinit(self: *Self) void {
    if (self.file) |*file| file.close();
    self.manager.deinit();
    std.debug.assert(self.budget.live == 0);
    std.heap.page_allocator.destroy(self);
}
fn at(self: *Self, index: u64) !Id {
    var id: Id = undefined;
    try self.file.?.readRaw(index * 32, &id);
    return id;
}
fn lowerBound(self: *Self, key: Id, inclusive: bool) !u64 {
    var lower: u64 = 0;
    var upper = self.count;
    while (lower < upper) {
        const mid = lower + (upper - lower) / 2;
        const order = std.mem.order(u8, &try self.at(mid), &key);
        if (order == .lt or (!inclusive and order == .eq)) lower = mid + 1 else upper = mid;
    }
    return lower;
}
pub fn clip(raw: *anyopaque, first: Id, last: Id) !?struct { first: Id, last: Id } {
    const self: *Self = @ptrCast(@alignCast(raw));
    const begin = try self.lowerBound(first, true);
    const end = try self.lowerBound(last, false);
    if (begin >= end) return null;
    return .{ .first = try self.at(begin), .last = try self.at(end - 1) };
}

test "external lake global frontier spilled pack proofs seek beyond flat admission limits" {
    const self = try std.heap.page_allocator.create(Self);
    self.* = .{ .budget = .{ .backing = std.testing.allocator, .limit = 4 * 1024 * 1024 }, .token = .none };
    self.manager = .{ .alloc = self.budget.allocator(), .io = std.testing.io, .context = self, .checkpoint = check };
    defer self.deinit();
    var sort = local.sql_spill.Sort.init(self.budget.allocator(), &self.manager, &.{.{}}, 16 * 1024);
    sort.run_limit = 4;
    defer sort.deinit();
    for (0..3) |_| for (0..10000) |i| {
        var id: Id = @splat(0);
        std.mem.writeInt(u32, id[28..32], @intCast(9999 - i), .big);
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &id })}, .values = &.{}, .ordinal = 0 });
    };
    try self.finish(&sort);
    try std.testing.expectEqual(@as(u64, 10000), self.count);
    try std.testing.expect(self.manager.written_bytes > self.file.?.size);
    var first: Id = @splat(0);
    var last: Id = @splat(0);
    std.mem.writeInt(u32, first[28..32], 7000, .big);
    std.mem.writeInt(u32, last[28..32], 9000, .big);
    const clipped = (try clip(self, first, last)).?;
    try std.testing.expectEqual(first, clipped.first);
    try std.testing.expectEqual(last, clipped.last);
    std.mem.writeInt(u32, first[28..32], 10000, .big);
    try std.testing.expect((try clip(self, first, @splat(255))) == null);
}
