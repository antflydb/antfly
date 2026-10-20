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

//! Exact normalized-key indexes over immutable aggregate blocks. Hash/radix
//! reduction ownership remains independent of the ordered serving forest.
//! Ordered copy-on-write group state, shared by serving and file reductions.
const std = @import("std");
const local = @import("antfly_local_sources");
const directory = @import("lake_index_native_directory.zig");
const partial = local.sql_aggregate_partial;
const operators = local.sql_operators;
const spill = local.sql_spill;
const stores = @import("../serverless/artifacts/store.zig");
pub const tree = directory.tree;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Recipe = local.sql_aggregate_materialization.Recipe;
pub const Index = struct {
    domain: [32]u8,
    page: ?tree.Ref = null,
    pub fn count(self: Index) u64 {
        return if (self.page) |page| page.records else 0;
    }
};
pub fn eligible(recipe: Recipe) bool {
    if (recipe.keys.len == 0 or partial.physicalWidth(recipe.inputs) > 1024) return false;
    for (recipe.inputs) |input| if (!partial.physicalEligible(input.spec)) return false;
    return true;
}
pub fn encode(a: A, input: operators.GroupResult, recipe: Recipe) ![]u8 {
    if (try partial.partialCount(input) != recipe.inputs.len) return error.InvalidNativeAggregateArtifact;
    const cells = try a.alloc(local.sql_scalar.Datum, partial.physicalWidth(recipe.inputs));
    for (recipe.inputs, 0..) |item, slot| {
        const state = try partial.read(a, input, slot, item.spec);
        const physical = try partial.physicalCells(a, state);
        const offset = partial.physicalOffset(recipe.inputs, slot);
        for (partial.physicalColumns(item.spec), 0..) |lane, i| cells[offset + i] = physical[lane];
    }
    // Enumeration ordinals are not state: stable zero avoids unrelated edits.
    return spill.encodeColumnarBlockAlloc(a, &.{.{ .keys = input.keys, .values = cells, .ordinal = 0 }}, 4 * 1024 * 1024);
}
pub fn block(a: A, bytes: []const u8, recipe: Recipe) !spill.ColumnarBlock {
    const result = try spill.decodeColumnarBlockInArena(a, bytes, 4 * 1024 * 1024);
    if (result.count() != 1 or result.keys.len != recipe.keys.len or result.values.len != partial.physicalWidth(recipe.inputs)) return error.InvalidNativeAggregateArtifact;
    return result;
}
const Physical = struct {
    value: spill.ColumnarBlock,
    recipe: Recipe,
    slots: []const u16,
    fn read(raw: *const anyopaque, a: A, slot: usize) !operators.Aggregate {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (slot >= self.slots.len or self.slots[slot] >= self.recipe.inputs.len) return error.InvalidSqlBackendResponse;
        const p = self.slots[slot];
        return partial.readPhysical(a, self.value, 0, partial.physicalOffset(self.recipe.inputs, p), self.recipe.inputs[p].spec);
    }
    fn finish(raw: *const anyopaque, a: A, slot: usize, spec: operators.AggregateSpec) !local.sql_scalar.Datum {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (slot >= self.slots.len or self.slots[slot] >= self.recipe.inputs.len) return error.InvalidSqlBackendResponse;
        const p = self.slots[slot];
        if (!std.meta.eql(spec, self.recipe.inputs[p].spec)) return error.InvalidSqlBackendResponse;
        return partial.finishPhysical(a, self.value, 0, partial.physicalOffset(self.recipe.inputs, p), spec);
    }
};
pub fn row(a: A, value: spill.ColumnarBlock, recipe: Recipe, slots: []const u16, outputs: ?[]const u16, ordinal: u64) !operators.GroupResult {
    const view = try a.create(Physical);
    view.* = .{ .value = value, .recipe = recipe, .slots = slots };
    const cells = try a.alloc(local.sql_scalar.Datum, recipe.keys.len);
    for (cells, 0..) |*cell, i| cell.* = try value.keyCell(0, i);
    return .{ .keys = cells, .aggregates = &.{}, .state_reader = .{ .ptr = view, .count = slots.len, .read = Physical.read, .finish = Physical.finish }, .aggregate_slots = outputs, .ordinal = ordinal };
}
fn merged(a: A, left: ?[]const u8, right: ?[]const u8, recipe: Recipe) !?[]const u8 {
    if (left == null) return right;
    if (right == null) return left;
    const l = try block(a, left.?, recipe);
    const r = try block(a, right.?, recipe);
    const cells = try a.alloc(local.sql_scalar.Datum, partial.physicalWidth(recipe.inputs));
    for (recipe.inputs, 0..) |input, slot| {
        const offset = partial.physicalOffset(recipe.inputs, slot);
        var state = try partial.readPhysical(a, l, 0, offset, input.spec);
        try partial.merge(&state, try partial.readPhysical(a, r, 0, offset, input.spec));
        const physical = try partial.physicalCells(a, state);
        for (partial.physicalColumns(input.spec), 0..) |lane, i| cells[offset + i] = physical[lane];
    }
    const key_cells = try a.alloc(local.sql_scalar.Datum, recipe.keys.len);
    for (key_cells, 0..) |*cell, i| cell.* = try l.keyCell(0, i);
    return try spill.encodeColumnarBlockAlloc(a, &.{.{ .keys = key_cells, .values = cells, .ordinal = 0 }}, 4 * 1024 * 1024);
}
fn same(left: ?tree.Ref, right: ?tree.Ref) bool {
    if (left == null or right == null) return left == null and right == null;
    return left.?.bytes == right.?.bytes and left.?.height == right.?.height and left.?.records == right.?.records and std.mem.eql(u8, &left.?.digest, &right.?.digest);
}
fn differences(a: A, backing: tree.Store, left: ?tree.Ref, right: ?tree.Ref, visitor: anytype) anyerror!void {
    try backing.check(backing.ptr);
    if (same(left, right)) return;
    if (left != null and right != null and left.?.height != 0 and left.?.height == right.?.height) {
        var l = try tree.readPage(a, backing, left.?);
        defer l.deinit(a);
        var r = try tree.readPage(a, backing, right.?);
        defer r.deinit(a);
        const aligned = l.entries.len == r.entries.len and for (l.entries, r.entries) |x, y| {
            if (!std.mem.eql(u8, x.key, y.key)) break false;
        } else true;
        if (aligned) {
            for (l.entries, r.entries) |x, y| try differences(a, backing, x.child, y.child, visitor);
            return;
        }
    }
    var lc = try tree.Cursor.init(a, backing, left, "", null);
    defer lc.deinit();
    var rc = try tree.Cursor.init(a, backing, right, "", null);
    defer rc.deinit();
    var l = try lc.next();
    var r = try rc.next();
    while (l != null or r != null) {
        const order = if (l == null) std.math.Order.gt else if (r == null) std.math.Order.lt else std.mem.order(u8, l.?.key, r.?.key);
        if (order == .eq) {
            if (!std.mem.eql(u8, l.?.value, r.?.value)) try visitor.add(l.?.key);
            l = try lc.next();
            r = try rc.next();
        } else if (order == .lt) {
            try visitor.add(l.?.key);
            l = try lc.next();
        } else {
            try visitor.add(r.?.key);
            r = try rc.next();
        }
    }
}
pub const Metrics = struct { changed_groups: usize = 0, written_bytes: u64 = 0 };
pub const Previous = struct { parent: Index, left: ?Index, right: ?Index };
/// Recompute touched keys from the two current children. This supports extrema
/// removal and exact NUMERIC without subtraction or old-generation chains.
pub fn merge(a: A, store: *stores.ArtifactStore, recipe: Recipe, left: Index, right: Index, previous: ?Previous, token: Token, metrics: ?*Metrics) !Index {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (!std.mem.eql(u8, &scope.domain, &left.domain) or !std.mem.eql(u8, &scope.domain, &right.domain)) return error.InvalidNativeAggregateArtifact;
    if (left.page == null) return right;
    if (right.page == null) return left;
    var access: directory.Access = undefined;
    access.init(store.*, scope.domain, token, null);
    var cache: tree.Cache = .{ .alloc = a, .underlying = access.backing() };
    defer cache.deinit();
    const backing = cache.store();
    const before_writes = access.writes;
    defer if (metrics) |m| {
        m.written_bytes = before_writes - access.writes;
    };
    if (previous) |prior| if (try incremental(a, backing, recipe, left, right, prior, metrics)) |result| return result;
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = backing };
    defer builder.deinit();
    var lc = try tree.Cursor.init(a, backing, left.page, "", null);
    defer lc.deinit();
    var rc = try tree.Cursor.init(a, backing, right.page, "", null);
    defer rc.deinit();
    var l = try lc.next();
    var r = try rc.next();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    while (l != null or r != null) {
        _ = scratch.reset(.free_all);
        const order = if (l == null) std.math.Order.gt else if (r == null) std.math.Order.lt else std.mem.order(u8, l.?.key, r.?.key);
        try builder.add(if (order == .gt) r.?.key else l.?.key, (try merged(scratch.allocator(), if (order != .gt) l.?.value else null, if (order != .lt) r.?.value else null, recipe)).?);
        if (order != .gt) l = try lc.next();
        if (order != .lt) r = try rc.next();
    }
    return .{ .domain = scope.domain, .page = try builder.finish() };
}
fn incremental(a: A, backing: tree.Store, recipe: Recipe, left: Index, right: Index, prior: Previous, metrics: ?*Metrics) !?Index {
    var budget: local.sql_memory_budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 8 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const Visitor = struct {
        a: A,
        keys: std.StringHashMapUnmanaged(void) = .empty,
        fn add(self: *@This(), key: []const u8) !void {
            if (!self.keys.contains(key)) try self.keys.put(self.a, try self.a.dupe(u8, key), {});
        }
    };
    var visitor: Visitor = .{ .a = arena.allocator() };
    differences(a, backing, if (prior.left) |v| v.page else null, left.page, &visitor) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return null;
        return err;
    };
    differences(a, backing, if (prior.right) |v| v.page else null, right.page, &visitor) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return null;
        return err;
    };
    if (metrics) |m| m.changed_groups = visitor.keys.count();
    const edits = arena.allocator().alloc(tree.Mutation, visitor.keys.count()) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return null;
        return err;
    };
    var it = visitor.keys.keyIterator();
    for (edits) |*edit| edit.* = .{ .key = it.next().?.*, .value = null };
    std.mem.sort(tree.Mutation, edits, {}, struct {
        fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
            return std.mem.lessThan(u8, l.key, r.key);
        }
    }.less);
    var root = prior.parent.page;
    var offset: usize = 0;
    while (offset < edits.len) {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const end = @min(edits.len, offset + 32);
        for (edits[offset..end]) |*edit| {
            var lc = try tree.Cursor.init(scratch.allocator(), backing, left.page, edit.key, null);
            defer lc.deinit();
            var rc = try tree.Cursor.init(scratch.allocator(), backing, right.page, edit.key, null);
            defer rc.deinit();
            const l = try lc.next();
            const r = try rc.next();
            const lv = if (l != null and std.mem.eql(u8, l.?.key, edit.key)) l.?.value else null;
            const rv = if (r != null and std.mem.eql(u8, r.?.key, edit.key)) r.?.value else null;
            const value = try merged(scratch.allocator(), lv, rv, recipe);
            edit.value = if (value) |v| try scratch.allocator().dupe(u8, v) else null;
        }
        root = try tree.apply(a, backing, root, edits[offset..end]);
        offset = end;
    }
    return .{ .domain = left.domain, .page = root };
}
