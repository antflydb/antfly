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
pub const record = @import("lake_index_aggregate_record.zig");
const stores = @import("../serverless/artifacts/store.zig");
pub const tree = directory.tree;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Recipe = local.sql_aggregate_materialization.Recipe;
pub const Index = struct {
    domain: [32]u8,
    /// 1: historical one-row columnar blocks; 2: recipe-bound transitions.
    layout: u8 = 1,
    page: ?tree.Ref = null,
    pub fn jsonStringify(self: Index, writer: anytype) !void {
        // Preserve historical interpretation commitments, including JSON shape.
        if (self.layout == 1) return writer.write(.{ .domain = self.domain, .page = self.page });
        return writer.write(.{ .domain = self.domain, .layout = self.layout, .page = self.page });
    }
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
    return record.encode(a, input, recipe);
}
pub const View = union(enum) {
    legacy: spill.ColumnarBlock,
    native: record.View,
    fn keys(self: View, a: A, recipe: Recipe) ![]const local.sql_scalar.Datum {
        if (self == .native) return self.native.keys(a);
        const cells = try a.alloc(local.sql_scalar.Datum, recipe.keys.len);
        for (cells, 0..) |*cell, i| cell.* = try self.legacy.keyCell(0, i);
        return cells;
    }
    fn read(self: View, a: A, recipe: Recipe, slot: usize) !operators.Aggregate {
        return if (self == .native) self.native.read(a, slot) else partial.readPhysical(a, self.legacy, 0, partial.physicalOffset(recipe.inputs, slot), recipe.inputs[slot].spec);
    }
    fn finish(self: View, a: A, recipe: Recipe, slot: usize, spec: operators.AggregateSpec) !local.sql_scalar.Datum {
        return if (self == .native) self.native.finish(a, slot, spec) else partial.finishPhysical(a, self.legacy, 0, partial.physicalOffset(recipe.inputs, slot), spec);
    }
};
pub fn view(a: A, bytes: []const u8, recipe: Recipe, layout: u8, prepared: ?*const record.Layout) !View {
    if (layout == 2) {
        const compiled = prepared orelse new: {
            const value = try a.create(record.Layout);
            value.* = try record.Layout.init(a, recipe);
            break :new value;
        };
        return .{ .native = try record.View.init(bytes, compiled) };
    }
    if (layout != 1) return error.InvalidNativeAggregateArtifact;
    const result = try spill.decodeColumnarBlockInArena(a, bytes, 4 * 1024 * 1024);
    if (result.count() != 1 or result.keys.len != recipe.keys.len or result.values.len != partial.physicalWidth(recipe.inputs)) return error.InvalidNativeAggregateArtifact;
    return .{ .legacy = result };
}
/// Only the oversized-record compatibility publisher needs every physical lane.
pub fn block(a: A, bytes: []const u8, recipe: Recipe) !spill.ColumnarBlock {
    const compiled = try a.create(record.Layout);
    compiled.* = try record.Layout.init(a, recipe);
    const input = try record.View.init(bytes, compiled);
    const cells = try a.alloc(local.sql_scalar.Datum, partial.physicalWidth(recipe.inputs));
    for (recipe.inputs, 0..) |item, slot| {
        const physical = try input.cells(a, slot);
        const offset = partial.physicalOffset(recipe.inputs, slot);
        for (partial.physicalColumns(item.spec), 0..) |lane, i| cells[offset + i] = physical[lane];
    }
    return spill.decodeColumnarBlockInArena(a, try spill.encodeColumnarBlockAlloc(a, &.{.{ .keys = try input.keys(a), .values = cells, .ordinal = 0 }}, 4 * 1024 * 1024), 4 * 1024 * 1024);
}
const Physical = struct {
    value: View,
    recipe: Recipe,
    slots: []const u16,
    fn read(raw: *const anyopaque, a: A, slot: usize) !operators.Aggregate {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (slot >= self.slots.len or self.slots[slot] >= self.recipe.inputs.len) return error.InvalidSqlBackendResponse;
        return self.value.read(a, self.recipe, self.slots[slot]);
    }
    fn finish(raw: *const anyopaque, a: A, slot: usize, spec: operators.AggregateSpec) !local.sql_scalar.Datum {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        if (slot >= self.slots.len or self.slots[slot] >= self.recipe.inputs.len) return error.InvalidSqlBackendResponse;
        const p = self.slots[slot];
        if (!std.meta.eql(spec, self.recipe.inputs[p].spec)) return error.InvalidSqlBackendResponse;
        return self.value.finish(a, self.recipe, p, spec);
    }
};
pub fn row(a: A, value: View, recipe: Recipe, slots: []const u16, outputs: ?[]const u16, ordinal: u64) !operators.GroupResult {
    const reader = try a.create(Physical);
    reader.* = .{ .value = value, .recipe = recipe, .slots = slots };
    return .{ .keys = try value.keys(a, recipe), .aggregates = &.{}, .state_reader = .{ .ptr = reader, .count = slots.len, .read = Physical.read, .finish = Physical.finish }, .aggregate_slots = outputs, .ordinal = ordinal };
}
fn merged(a: A, left: ?[]const u8, right: ?[]const u8, recipe: Recipe, left_layout: u8, right_layout: u8, layout: *const record.Layout) !?[]const u8 {
    if (left == null and right == null) return null;
    if (left == null and right_layout == 2) return right;
    if (right == null and left_layout == 2) return left;
    const l = if (left) |bytes| try view(a, bytes, recipe, left_layout, layout) else null;
    const r = if (right) |bytes| try view(a, bytes, recipe, right_layout, layout) else null;
    const states = try a.alloc(operators.Aggregate, recipe.inputs.len);
    for (states, 0..) |*state, slot| {
        state.* = try (l orelse r.?).read(a, recipe, slot);
        if (l != null and r != null) try partial.merge(state, try r.?.read(a, recipe, slot));
    }
    return try record.encodeWithLayout(a, .{ .keys = try (l orelse r.?).keys(a, recipe), .aggregates = &.{}, .typed_states = states, .ordinal = 0 }, layout.*);
}
fn same(left: ?tree.Ref, right: ?tree.Ref) bool {
    if (left == null or right == null) return left == null and right == null;
    return left.?.bytes == right.?.bytes and left.?.height == right.?.height and left.?.records == right.?.records and std.mem.eql(u8, &left.?.digest, &right.?.digest);
}
fn differences(a: A, backing: tree.Store, left: ?tree.Ref, right: ?tree.Ref, visitor: anytype) anyerror!void {
    return differencesRange(a, backing, left, right, "", null, visitor);
}
/// Zip routing intervals rather than child positions. Splits, merges and a new
/// minimum key may change fences while retaining nearly every child reference.
fn differencesRange(a: A, backing: tree.Store, left: ?tree.Ref, right: ?tree.Ref, lower: []const u8, upper: ?[]const u8, visitor: anytype) anyerror!void {
    try backing.check(backing.ptr);
    if (same(left, right)) return;
    var lp: ?tree.Page = if (left != null and left.?.height != 0) try tree.readPage(a, backing, left.?) else null;
    defer if (lp) |*page| page.deinit(a);
    var rp: ?tree.Page = if (right != null and right.?.height != 0) try tree.readPage(a, backing, right.?) else null;
    defer if (rp) |*page| page.deinit(a);
    if (lp != null or rp != null) {
        var li: usize = 0;
        var ri: usize = 0;
        var begin = lower;
        while (true) {
            if (lp) |page| while (li + 1 < page.entries.len and std.mem.order(u8, page.entries[li + 1].key, begin) != .gt) {
                li += 1;
            };
            if (rp) |page| while (ri + 1 < page.entries.len and std.mem.order(u8, page.entries[ri + 1].key, begin) != .gt) {
                ri += 1;
            };
            var end = upper;
            if (lp) |page| if (li + 1 < page.entries.len) {
                const fence = page.entries[li + 1].key;
                if (end == null or std.mem.order(u8, fence, end.?) == .lt) end = fence;
            };
            if (rp) |page| if (ri + 1 < page.entries.len) {
                const fence = page.entries[ri + 1].key;
                if (end == null or std.mem.order(u8, fence, end.?) == .lt) end = fence;
            };
            try differencesRange(a, backing, if (lp) |page| page.entries[li].child else left, if (rp) |page| page.entries[ri].child else right, begin, end, visitor);
            if (end == null or (upper != null and std.mem.eql(u8, end.?, upper.?))) break;
            begin = end.?;
        }
        return;
    }
    var lc = try tree.Cursor.init(a, backing, left, lower, upper);
    defer lc.deinit();
    var rc = try tree.Cursor.init(a, backing, right, lower, upper);
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
    const layout = try record.Layout.init(a, recipe);
    defer layout.deinit(a);
    const before_writes = access.writes;
    defer if (metrics) |m| {
        m.written_bytes = before_writes - access.writes;
    };
    if (previous) |prior| if (left.layout == 2 and right.layout == 2 and prior.parent.layout == 2 and (prior.left == null or prior.left.?.layout == 2) and (prior.right == null or prior.right.?.layout == 2)) if (try incremental(a, backing, recipe, left, right, prior, metrics, &layout)) |result| return result;
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
        try builder.add(if (order == .gt) r.?.key else l.?.key, (try merged(scratch.allocator(), if (order != .gt) l.?.value else null, if (order != .lt) r.?.value else null, recipe, left.layout, right.layout, &layout)).?);
        if (order != .gt) l = try lc.next();
        if (order != .lt) r = try rc.next();
    }
    return .{ .domain = scope.domain, .layout = 2, .page = try builder.finish() };
}
fn incremental(a: A, backing: tree.Store, recipe: Recipe, left: Index, right: Index, prior: Previous, metrics: ?*Metrics, layout: *const record.Layout) !?Index {
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
            const value = try merged(scratch.allocator(), lv, rv, recipe, left.layout, right.layout, layout);
            edit.value = if (value) |v| try scratch.allocator().dupe(u8, v) else null;
        }
        root = try tree.apply(a, backing, root, edits[offset..end]);
        offset = end;
    }
    return .{ .domain = left.domain, .layout = 2, .page = root };
}

test "external lake aggregate interval differences skip retained pages across fences splits and height changes" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const backing = memory.store();
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = backing };
    defer builder.deinit();
    const value: [250]u8 = @splat(9);
    for (1..12001) |i| try builder.add(&directory.key(i), &value);
    const root = (try builder.finish()).?;
    const Visitor = struct {
        expected: u64,
        count: usize = 0,
        fn add(self: *@This(), key: []const u8) !void {
            try std.testing.expectEqual(self.expected, std.mem.readInt(u64, key[0..8], .big));
            self.count += 1;
        }
    };
    const split_value: [30000]u8 = @splat(7);
    for ([_]struct { key: u64, value: ?[]const u8 }{
        .{ .key = 6001, .value = "changed" },
        .{ .key = 0, .value = "new minimum" },
        .{ .key = 1, .value = null },
        .{ .key = 12001, .value = &value },
        .{ .key = 6001, .value = &split_value },
    }) |change| {
        const changed = try tree.apply(a, backing, root, &.{.{ .key = &directory.key(change.key), .value = change.value }});
        var visitor: Visitor = .{ .expected = change.key };
        var cache: tree.Cache = .{ .alloc = a, .underlying = backing };
        defer cache.deinit();
        const before = memory.reads;
        try differences(a, cache.store(), root, changed, &visitor);
        try std.testing.expectEqual(@as(usize, 1), visitor.count);
        try std.testing.expect(memory.reads - before < 12);
        std.debug.print("aggregate interval edit key={} reads={}\n", .{ change.key, memory.reads - before });
    }
    // The same retained leaf can acquire a taller root after a boundary split.
    var root_page = try tree.readPage(a, backing, root);
    defer root_page.deinit(a);
    const first_leaf = root_page.entries[0].child.?;
    var visitor: Visitor = .{ .expected = 0 };
    var cache: tree.Cache = .{ .alloc = a, .underlying = backing };
    defer cache.deinit();
    // Restrict to the retained leaf's interval so all keys are identical.
    const before = memory.reads;
    try differencesRange(a, cache.store(), first_leaf, root, root_page.entries[0].key, root_page.entries[1].key, &visitor);
    try std.testing.expectEqual(@as(usize, 0), visitor.count);
    try std.testing.expect(memory.reads - before <= 1);
}
