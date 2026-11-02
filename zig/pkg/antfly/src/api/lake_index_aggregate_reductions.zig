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

//! Persistent group reductions. A mutation rereads at most 64 contributions
//! and folds sixteen ordered child summaries at each ancestor. Group size does
//! not determine refresh work, and exact reducer presentation remains stable.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifact = @import("lake_index_aggregate_artifact.zig");
const tree = artifact.state_tree.tree;
const record = artifact.state_tree.record;
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const keys = @import("lake_index_aggregate_keys.zig");
const A = std.mem.Allocator;
pub const version = 2;
pub fn direct(input: local.sql_aggregate_materialization.Input) bool {
    return input.spec.kind == .count or (input.spec.kind == .sum and input.spec.input_type == .integer) or input.spec.kind == .min or input.spec.kind == .max;
}
pub const Projection = struct {
    layout: record.Layout,
    indices: []usize,
    inverse: [256]?u8 = @splat(null),
    pub fn init(a: A, source: *const record.Layout) !Projection {
        var count: usize = 0;
        for (source.recipe.inputs) |input| if (!direct(input)) {
            count += 1;
        };
        const inputs = try a.alloc(local.sql_aggregate_materialization.Input, count);
        errdefer a.free(inputs);
        const indices = try a.alloc(usize, count);
        errdefer a.free(indices);
        var next: usize = 0;
        for (source.recipe.inputs, 0..) |input, index| if (!direct(input)) {
            inputs[next] = input;
            indices[next] = index;
            next += 1;
        };
        var result: Projection = .{ .layout = try record.Layout.init(a, .{ .keys = source.recipe.keys, .inputs = inputs }), .indices = indices };
        for (indices, 0..) |original, slot_| result.inverse[original] = @intCast(slot_);
        return result;
    }
    pub fn deinit(self: Projection, a: A) void {
        self.layout.deinit(a);
        a.free(self.layout.recipe.inputs);
        a.free(self.indices);
    }
    pub fn slot(self: Projection, original: usize) !usize {
        if (original >= self.inverse.len) return error.InvalidNativeAggregateArtifact;
        return self.inverse[original] orelse error.InvalidNativeAggregateArtifact;
    }
};
pub const Dirty = std.StringHashMapUnmanaged(void);
pub fn levels(rows: u64) u8 {
    var last = (rows -| 1) >> 6;
    var depth: u8 = 0;
    while (last != 0) : (last >>= 4) depth += 1;
    return depth;
}
fn key(a: A, group: []const u8, level: u8, block: u64) ![]u8 {
    if (group.len + 9 > tree.max_key_bytes) return error.RelationalIndexKeyTooLarge;
    const bytes = try a.alloc(u8, group.len + 9);
    @memcpy(bytes[0..group.len], group);
    bytes[group.len] = level;
    std.mem.writeInt(u64, bytes[group.len + 1 ..][0..8], block, .big);
    return bytes;
}
pub fn touch(a: A, dirty: *Dirty, group: []const u8, row: u64) !void {
    const encoded = try key(a, group, 0, row >> 6);
    const entry = dirty.getOrPut(a, encoded) catch |err| {
        a.free(encoded);
        return err;
    };
    if (entry.found_existing) a.free(encoded);
}
pub fn clear(a: A, dirty: *Dirty) void {
    var it = dirty.keyIterator();
    while (it.next()) |value| a.free(value.*);
    dirty.deinit(a);
    dirty.* = .empty;
}
pub fn lookup(a: A, storage: tree.Store, root: ?tree.Ref, group: []const u8, row_count: u64, layout: *const record.Layout) !record.View {
    const encoded = try key(a, group, levels(row_count), 0);
    var cursor = try tree.Cursor.init(a, storage, root, encoded, null);
    defer cursor.deinit();
    const found = (try cursor.next()) orelse return error.InvalidNativeAggregateArtifact;
    if (!std.mem.eql(u8, found.key, encoded)) return error.InvalidNativeAggregateArtifact;
    return record.View.init(try a.dupe(u8, found.value), layout);
}
pub const Metrics = struct { leaf_rows: u64 = 0, child_summaries: u64 = 0 };
pub fn repair(a: A, manager: *local.sql_spill.Manager, storage: tree.Store, rows: ?tree.Ref, prior: ?tree.Ref, dirty: *const Dirty, row_count: u64, layout: *const record.Layout) !?tree.Ref {
    return repairMeasured(a, manager, storage, rows, prior, dirty, row_count, layout, null);
}
pub fn repairMeasured(a: A, manager: *local.sql_spill.Manager, storage: tree.Store, rows: ?tree.Ref, prior: ?tree.Ref, dirty: *const Dirty, row_count: u64, layout: *const record.Layout, metrics: ?*Metrics) !?tree.Ref {
    const projection = try Projection.init(a, layout);
    defer projection.deinit(a);
    if (prior == null) return buildFresh(a, manager, storage, rows, row_count, layout, &projection, metrics);
    var cache: tree.Cache = .{ .decoded = true, .alloc = local.sql_memory_budget.reclaimingAllocator(a), .underlying = storage };
    defer cache.deinit();
    const cached = if (storage.decoded_page != null) storage else cache.store();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var pending: Dirty = .empty;
    var initial = dirty.keyIterator();
    while (initial.next()) |encoded| try pending.put(ca, encoded.*, {});
    var root = prior;
    var level: u8 = 0;
    while (level <= levels(row_count)) : (level += 1) {
        var changes = Mutations.init(a, manager, cached);
        defer changes.deinit();
        var parents: Dirty = .empty;
        // Preserve leaf locality across working sets larger than the cache.
        // Only borrowed keys enter this workspace-accounted level directory.
        const ordered = try a.alloc([]const u8, pending.count());
        defer a.free(ordered);
        var it = pending.keyIterator();
        var next: usize = 0;
        while (it.next()) |entry| {
            ordered[next] = entry.*;
            next += 1;
        }
        std.mem.sort([]const u8, ordered, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.less);
        for (ordered) |entry| {
            try manager.check();
            const group = entry[0 .. entry.len - 9];
            const block = std.mem.readInt(u64, entry[entry.len - 8 ..][0..8], .big);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const sa = scratch.allocator();
            var lower: []u8 = undefined;
            var upper: ?[]const u8 = null;
            if (level == 0) {
                lower = try sa.alloc(u8, group.len + 8);
                @memcpy(lower[0..group.len], group);
                const first = block << 6;
                std.mem.writeInt(u64, lower[group.len..][0..8], first, .big);
                upper = try sa.dupe(u8, lower);
                std.mem.writeInt(u64, @constCast(upper.?)[group.len..][0..8], first +| 64, .big);
            } else {
                lower = try key(sa, group, level - 1, block << 4);
                upper = if (block <= (std.math.maxInt(u64) >> 4) - 1) try key(sa, group, level - 1, (block + 1) << 4) else try keys.successor(sa, lower[0 .. lower.len - 8]);
            }
            var cursor = try tree.Cursor.init(sa, cached, if (level == 0) rows else root, lower, upper);
            defer cursor.deinit();
            var merged: ?[]u8 = null;
            while (try cursor.next()) |contribution| {
                if (metrics) |value| {
                    if (level == 0) value.leaf_rows += 1 else value.child_summaries += 1;
                }
                const value = if (level == 0 and projection.indices.len != layout.slots.len) try record.project(sa, contribution.value, layout, &projection.layout, projection.indices) else contribution.value;
                merged = if (merged) |previous| try record.merge(sa, previous, value, &projection.layout) else try sa.dupe(u8, value);
            }
            try changes.addRecord(entry, merged);
            try parents.put(ca, try key(ca, group, level + 1, block >> 4), {});
        }
        root = try changes.finish(root);
        pending = parents;
    }
    return root;
}

/// Fresh cohorts traverse contributions once. A bounded carry stack folds each
/// ordinal block in order; only compact summaries enter the external sort.
fn buildFresh(a: A, manager: *local.sql_spill.Manager, storage: tree.Store, rows: ?tree.Ref, row_count: u64, layout: *const record.Layout, projection: *const Projection, metrics: ?*Metrics) !?tree.Ref {
    const Builder = struct {
        a: A,
        layout: *const record.Layout,
        depth: u8,
        group: []const u8 = "",
        blocks: [17]u64 = @splat(0),
        states: [17]?[]u8 = @splat(null),
        sort: *local.sql_spill.Sort,
        metrics: ?*Metrics,
        fn add(self: *@This(), level: u8, block: u64, value: []const u8) anyerror!void {
            if (self.states[level] != null and self.blocks[level] != block) try self.flush(level);
            const next = if (self.states[level]) |prior| try record.merge(self.a, prior, value, self.layout) else try self.a.dupe(u8, value);
            if (self.states[level]) |prior| self.a.free(prior);
            self.states[level] = next;
            self.blocks[level] = block;
            if (self.metrics) |m| if (level != 0) {
                m.child_summaries += 1;
            };
        }
        fn flush(self: *@This(), level: u8) anyerror!void {
            const value = self.states[level] orelse return;
            const encoded = try key(self.a, self.group, level, self.blocks[level]);
            defer self.a.free(encoded);
            try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = encoded })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = value })}, .ordinal = self.sort.total });
            if (level < self.depth) try self.add(level + 1, self.blocks[level] >> 4, value);
            self.a.free(value);
            self.states[level] = null;
        }
        fn finishGroup(self: *@This()) !void {
            for (0..@as(usize, self.depth) + 1) |level| try self.flush(@intCast(level));
        }
        fn deinit(self: *@This()) void {
            for (self.states) |state| if (state) |value| self.a.free(value);
            self.a.free(self.group);
        }
    };
    var sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
    sort.shared_runs = true;
    sort.encoded_bytes = true;
    defer sort.deinit();
    var builder: Builder = .{ .a = a, .layout = &projection.layout, .depth = levels(row_count), .sort = &sort, .metrics = metrics };
    defer builder.deinit();
    var cursor = try tree.Cursor.init(a, storage, rows, "", null);
    defer cursor.deinit();
    while (try cursor.next()) |row| {
        try manager.check();
        if (row.key.len < 12) return error.InvalidNativeAggregateArtifact;
        const group = row.key[0 .. row.key.len - 8];
        const ordinal = std.mem.readInt(u64, row.key[row.key.len - 8 ..][0..8], .big);
        if (ordinal >= row_count) return error.InvalidNativeAggregateArtifact;
        if (!std.mem.eql(u8, group, builder.group)) {
            try builder.finishGroup();
            const next = try a.dupe(u8, group);
            a.free(builder.group);
            builder.group = next;
        }
        if (metrics) |m| m.leaf_rows += 1;
        const projected = if (projection.indices.len != layout.slots.len) try record.project(a, row.value, layout, &projection.layout, projection.indices) else null;
        defer if (projected) |value| a.free(value);
        try builder.add(0, ordinal >> 6, projected orelse row.value);
    }
    try builder.finishGroup();
    const Source = struct {
        sort: *local.sql_spill.Sort,
        scratch: std.heap.ArenaAllocator,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            _ = self.scratch.reset(.free_all);
            const row = (try self.sort.next(self.scratch.allocator())) orelse return null;
            return .{ .key = row.keys[0].value.string, .value = row.values[0].value.string };
        }
    };
    var source: Source = .{ .sort = &sort, .scratch = .init(a) };
    defer source.scratch.deinit();
    return tree.buildSortedWithOptions(a, storage, &source, .{ .target_bytes = tree.bulk_target_page_bytes });
}

test "external lake exact reduction trees refresh a bounded leaf and retain prior roots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const recipe: local.sql_aggregate_materialization.Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = &.{.{ .spec = .{ .kind = .sum, .input_type = .number, .input_element = .numeric }, .column = .{ .path = "n", .type = .number, .element_type = .numeric, .nullable = true } }} };
    const layout = try record.Layout.init(a, recipe);
    defer layout.deinit(a);
    const group = &[_]u8{ 0, 0, 0, 1, 'g' };
    var rows: tree.SortedBuilder = .{ .alloc = a, .store = memory.store(), .target_bytes = 4096 };
    defer rows.deinit();
    var dirty: Dirty = .empty;
    defer clear(a, &dirty);
    const count = 257;
    var work: local.sql_array_value.Budget = .{};
    const cell = try local.sql_scalar.numericTextLeaky(out, "1.00", &work);
    var state = try local.sql_operators.Aggregate.initTyped(out, .sum, .number, .numeric);
    defer state.deinit();
    try state.update(cell);
    const encoded = try record.encodeWithLayout(out, .{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = "g" })}, .aggregates = &.{}, .typed_states = &.{state}, .ordinal = 0 }, layout);
    for (0..count) |i| {
        var row_key: [13]u8 = undefined;
        @memcpy(row_key[0..5], group);
        std.mem.writeInt(u64, row_key[5..13], i, .big);
        try rows.add(&row_key, encoded);
        try touch(a, &dirty, group, i);
    }
    const before_rows = try rows.finish();
    const before = try repair(a, &manager, memory.store(), before_rows, null, &dirty, count, &layout);
    clear(a, &dirty);
    try touch(a, &dirty, group, 7);
    var update = Mutations.init(a, &manager, memory.store());
    defer update.deinit();
    var row_key: [13]u8 = undefined;
    @memcpy(row_key[0..5], group);
    std.mem.writeInt(u64, row_key[5..13], 7, .big);
    var changed = try local.sql_operators.Aggregate.initTyped(out, .sum, .number, .numeric);
    defer changed.deinit();
    try changed.update(try local.sql_scalar.numericTextLeaky(out, "2.1250", &work));
    try update.addRecord(&row_key, try record.encodeWithLayout(out, .{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = "g" })}, .aggregates = &.{}, .typed_states = &.{changed}, .ordinal = 7 }, layout));
    const after_rows = try update.finish(before_rows);
    var metrics: Metrics = .{};
    const after = try repairMeasured(a, &manager, memory.store(), after_rows, before, &dirty, count, &layout, &metrics);
    try std.testing.expectEqual(@as(u64, 64), metrics.leaf_rows);
    try std.testing.expectEqual(@as(u64, 5), metrics.child_summaries);
    const previous = try lookup(out, memory.store(), before, group, count, &layout);
    const current = try lookup(out, memory.store(), after, group, count, &layout);
    try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(try previous.finish(out, 0, recipe.inputs[0].spec), try local.sql_scalar.numericTextLeaky(out, "257.00", &work)));
    try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(try current.finish(out, 0, recipe.inputs[0].spec), try local.sql_scalar.numericTextLeaky(out, "258.1250", &work)));
}

test "external lake exact reduction dirty keys unwind allocation failures" {
    const Case = struct {
        fn run(a: A) !void {
            var dirty: Dirty = .empty;
            defer clear(a, &dirty);
            try touch(a, &dirty, "group", 7);
            try touch(a, &dirty, "group", 8);
            try std.testing.expectEqual(@as(usize, 1), dirty.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "external lake high cardinality reductions bulk read each contribution page once and cache repairs" {
    for ([_]usize{ 257, 2049 }) |count| try repairLocalityScenario(count);
}
fn repairLocalityScenario(count: usize) !void {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const recipe: local.sql_aggregate_materialization.Recipe = .{ .keys = &.{.{ .path = "k", .type = .string, .nullable = false }}, .inputs = &.{.{ .spec = .{ .kind = .bool_and, .input_type = .boolean }, .column = .{ .path = "n", .type = .boolean, .nullable = true } }} };
    const layout = try record.Layout.init(a, recipe);
    defer layout.deinit(a);
    var rows: tree.SortedBuilder = .{ .alloc = a, .store = memory.store(), .target_bytes = 4096 };
    defer rows.deinit();
    var dirty: Dirty = .empty;
    defer clear(a, &dirty);
    var state = try local.sql_operators.Aggregate.initTyped(out, .bool_and, .boolean, null);
    defer state.deinit();
    try state.update(local.sql_scalar.Datum.json(.{ .bool = true }));
    for (0..count) |i| {
        const group: [6]u8 = .{ 0, 0, 0, 2, @intCast(i >> 8), @truncate(i) };
        const encoded = try record.encodeWithLayout(out, .{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = group[4..] })}, .aggregates = &.{}, .typed_states = &.{state}, .ordinal = i }, layout);
        var row_key: [14]u8 = undefined;
        @memcpy(row_key[0..6], &group);
        std.mem.writeInt(u64, row_key[6..14], i, .big);
        try rows.add(&row_key, encoded);
        try touch(a, &dirty, &group, i);
    }
    const root = try rows.finish();
    const before_reads = memory.reads;
    const input_pages = memory.pages.count();
    const reduced = try repair(a, &manager, memory.store(), root, null, &dirty, count, &layout);
    try std.testing.expectEqual(@as(usize, input_pages), memory.reads - before_reads);
    try std.testing.expectEqual(@as(u64, @intCast(count)) * (@as(u64, levels(count)) + 1), reduced.?.records);
    const before_repair = memory.reads;
    _ = try repair(a, &manager, memory.store(), root, reduced, &dirty, count, &layout);
    try std.testing.expect(memory.reads - before_repair <= input_pages + 16);
    const repair_reads = memory.reads - before_repair;
    var changed_rows: tree.SortedBuilder = .{ .alloc = a, .store = memory.store(), .target_bytes = 4096 };
    defer changed_rows.deinit();
    var false_state = try local.sql_operators.Aggregate.initTyped(out, .bool_and, .boolean, null);
    defer false_state.deinit();
    try false_state.update(local.sql_scalar.Datum.json(.{ .bool = false }));
    for (0..count) |i| {
        const group: [6]u8 = .{ 0, 0, 0, 2, @intCast(i >> 8), @truncate(i) };
        const encoded = try record.encodeWithLayout(out, .{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = group[4..] })}, .aggregates = &.{}, .typed_states = &.{false_state}, .ordinal = i }, layout);
        var row_key: [14]u8 = undefined;
        @memcpy(row_key[0..6], &group);
        std.mem.writeInt(u64, row_key[6..14], i, .big);
        try changed_rows.add(&row_key, encoded);
    }
    const changed_root = try changed_rows.finish();
    const before_change = memory.reads;
    const changed = try repair(a, &manager, memory.store(), changed_root, reduced, &dirty, count, &layout);
    const changed_reads = memory.reads - before_change;
    try std.testing.expect(changed_reads <= input_pages * 2 + 32);
    const expected = try repair(a, &manager, memory.store(), changed_root, null, &dirty, count, &layout);
    try std.testing.expect(try tree.equivalent(a, memory.store(), expected, changed, .{}));
    const first_group = &[_]u8{ 0, 0, 0, 2, 0, 0 };
    const previous = try lookup(out, memory.store(), reduced, first_group, count, &layout);
    const current = try lookup(out, memory.store(), changed, first_group, count, &layout);
    try std.testing.expect((try previous.finish(out, 0, recipe.inputs[0].spec)).value.bool);
    try std.testing.expect(!(try current.finish(out, 0, recipe.inputs[0].spec)).value.bool);
    std.debug.print("bounded reductions: groups={} fresh_reads={} repair_reads={} changed_reads={}\n", .{ count, input_pages, repair_reads, changed_reads });
}
