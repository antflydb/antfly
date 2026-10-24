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
    const entry = try dirty.getOrPut(a, encoded);
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
pub fn repair(a: A, manager: *local.sql_spill.Manager, storage: tree.Store, rows: ?tree.Ref, prior: ?tree.Ref, dirty: *const Dirty, row_count: u64, layout: *const record.Layout, phase: ?*const @import("../serverless/artifacts/store.zig").IoPhase) !?tree.Ref {
    return repairMeasured(a, manager, storage, rows, prior, dirty, row_count, layout, phase, null);
}
pub fn repairMeasured(a: A, manager: *local.sql_spill.Manager, storage: tree.Store, rows: ?tree.Ref, prior: ?tree.Ref, dirty: *const Dirty, row_count: u64, layout: *const record.Layout, phase: ?*const @import("../serverless/artifacts/store.zig").IoPhase, metrics: ?*Metrics) !?tree.Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var pending: Dirty = .empty;
    var initial = dirty.keyIterator();
    while (initial.next()) |encoded| try pending.put(ca, encoded.*, {});
    var root = prior;
    var level: u8 = 0;
    while (level <= levels(row_count)) : (level += 1) {
        var changes = Mutations.init(a, manager, storage);
        defer changes.deinit();
        changes.private_pages.phase = phase;
        var parents: Dirty = .empty;
        var it = pending.keyIterator();
        while (it.next()) |entry| {
            try manager.check();
            const group = entry.*[0 .. entry.*.len - 9];
            const block = std.mem.readInt(u64, entry.*[entry.*.len - 8 ..][0..8], .big);
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
            var cursor = try tree.Cursor.init(sa, storage, if (level == 0) rows else root, lower, upper);
            defer cursor.deinit();
            var merged: ?[]u8 = null;
            while (try cursor.next()) |contribution| {
                if (metrics) |value| {
                    if (level == 0) value.leaf_rows += 1 else value.child_summaries += 1;
                }
                merged = if (merged) |previous| try record.merge(sa, previous, contribution.value, layout) else try sa.dupe(u8, contribution.value);
            }
            try changes.addRecord(entry.*, merged);
            try parents.put(ca, try key(ca, group, level + 1, block >> 4), {});
        }
        root = try changes.finish(root);
        pending = parents;
    }
    return root;
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
    const before = try repair(a, &manager, memory.store(), before_rows, null, &dirty, count, &layout, null);
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
    const after = try repairMeasured(a, &manager, memory.store(), after_rows, before, &dirty, count, &layout, null, &metrics);
    try std.testing.expectEqual(@as(u64, 64), metrics.leaf_rows);
    try std.testing.expectEqual(@as(u64, 5), metrics.child_summaries);
    const previous = try lookup(out, memory.store(), before, group, count, &layout);
    const current = try lookup(out, memory.store(), after, group, count, &layout);
    try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(try previous.finish(out, 0, recipe.inputs[0].spec), try local.sql_scalar.numericTextLeaky(out, "257.00", &work)));
    try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compareDatums(try current.finish(out, 0, recipe.inputs[0].spec), try local.sql_scalar.numericTextLeaky(out, "258.1250", &work)));
}
