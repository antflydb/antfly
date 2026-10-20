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

//! Best-first aggregate result pages over authenticated key fences. Only a
//! bounded native TopK and the reached directory frontier remain resident.
const std = @import("std");
const local = @import("antfly_local_sources");
const keys = @import("lake_index_aggregate_keys.zig");
const operators = local.sql_operators;
const A = std.mem.Allocator;
pub fn next(self: anytype, a: A, maximum: u32) anyerror!?[]const operators.GroupResult {
    const Reader = @TypeOf(self.*);
    if (maximum == 0) return error.InvalidSqlLimit;
    if (self.order_done) return null;
    try self.cancellation.check();
    if (self.cached) |cached| try cached.context.ensureActive();
    var plan = std.heap.ArenaAllocator.init(self.a);
    defer plan.deinit();
    const pa = plan.allocator();
    var range = self.key_range orelse keys.Range{};
    if (self.order_boundary) |boundary| {
        if (self.reverse) {
            if (range.upper == null or std.mem.order(u8, boundary, range.upper.?) == .lt) range.upper = boundary;
        } else {
            const lower = (try keys.successor(pa, boundary)) orelse {
                self.order_done = true;
                return null;
            };
            if (std.mem.order(u8, lower, range.lower) == .gt) range.lower = lower;
        }
    }
    const Owner = struct { reader: *Reader, pending: usize = 1, live: bool = true, borrowed: bool = false };
    const Node = struct { owner: *Owner, kind: union(enum) { root, part: usize, block: usize } = .root, bounds: keys.Bounds };
    const Queue = struct {
        fn compare(reverse: bool, left: Node, right: Node) std.math.Order {
            if (!reverse) return std.mem.order(u8, left.bounds.lower.slice(), right.bounds.lower.slice());
            const l = left.bounds.upper;
            const r = right.bounds.upper;
            if (l == null) return if (r == null) .eq else .lt;
            if (r == null) return .gt;
            return std.mem.order(u8, r.?.slice(), l.?.slice());
        }
    };
    var queue = std.PriorityQueue(Node, bool, Queue.compare).initContext(self.reverse);
    defer queue.deinit(pa);
    var owners: std.ArrayList(*Owner) = .empty;
    defer for (owners.items) |owner| if (owner.live and !owner.borrowed) {
        self.blocks_decoded += owner.reader.blocks_decoded;
        owner.reader.cursor().close(owner.reader);
    };
    const root = try pa.create(Owner);
    root.* = .{ .reader = self, .borrowed = true };
    try owners.append(pa, root);
    try queue.push(pa, .{ .owner = root, .bounds = self.root.bounds.? });
    const orders = try pa.alloc(operators.Order, self.root.recipe.keys.len);
    @memset(orders, .{ .descending = self.reverse, .nulls_first = self.reverse });
    var top = try operators.TopK.initWithSpill(self.a, @min(maximum, 256), orders, self.selection_memory, self.selection_spill);
    defer top.deinit();
    const old_raw = self.raw_states;
    self.raw_states = true;
    defer {
        self.raw_states = old_raw;
        self.discardBlock();
    }
    var page = std.heap.ArenaAllocator.init(self.a);
    defer page.deinit();
    while (queue.pop()) |node| {
        try self.cancellation.check();
        if (self.cached) |cached| try cached.context.ensureActive();
        const owner = node.owner;
        defer {
            owner.pending -= 1;
            if (owner.pending == 0 and owner.live and !owner.borrowed) {
                self.blocks_decoded += owner.reader.blocks_decoded;
                owner.reader.cursor().close(owner.reader);
                owner.live = false;
            }
        }
        if (!range.overlaps(node.bounds)) continue;
        _ = page.reset(.free_all);
        const scratch = page.allocator();
        if (top.external == null and top.count == top.capacity) {
            const worst = try keys.encode(scratch, (try top.scalarRow(0)).keys);
            const done = if (self.reverse) (node.bounds.upper != null and std.mem.order(u8, worst, node.bounds.upper.?.slice()) != .lt) else std.mem.order(u8, worst, node.bounds.lower.slice()) == .lt;
            if (done) break;
        }
        const reader = owner.reader;
        switch (node.kind) {
            .part => |index| {
                const child = try reader.partitionReader(index);
                var adopted = false;
                errdefer if (!adopted) child.cursor().close(child);
                const owned = try pa.create(Owner);
                owned.* = .{ .reader = child };
                try owners.append(pa, owned);
                adopted = true;
                try queue.push(pa, .{ .owner = owned, .bounds = child.root.bounds orelse return error.InvalidNativeAggregateArtifact });
            },
            .block => |index| {
                reader.discardBlock();
                reader.block_index = index;
                reader.raw_states = true;
                const rows = (try Reader.nextRaw(reader, scratch, 256)) orelse return error.InvalidNativeAggregateArtifact;
                if (rows.len > self.selection_rows_left or self.selection_pages_left == 0) return error.SqlProgramLimitExceeded;
                self.selection_rows_left -= rows.len;
                self.selection_pages_left -= 1;
                for (rows) |row| {
                    const encoded = try keys.encode(scratch, row.keys);
                    if (!range.contains(encoded)) continue;
                    try top.add(.{ .keys = row.keys, .values = row.aggregates, .ordinal = row.ordinal });
                }
                reader.discardBlock();
            },
            .root => {
                if (reader.root.partitions.len != 0) {
                    for (reader.root.partitions, 0..) |part, index| {
                        if (!range.overlaps(part.bounds)) continue;
                        if (reader.point_hash) |hash| if (part.bucket != (hash >> partitionShift(reader.root.radix_depth)) % partitionFanout(reader.root.radix_depth)) continue;
                        try queue.push(pa, .{ .owner = owner, .kind = .{ .part = index }, .bounds = part.bounds orelse return error.InvalidNativeAggregateArtifact });
                        owner.pending += 1;
                    }
                } else for (reader.root.blocks, 0..) |block, index| {
                    if (!range.overlaps(block.bounds)) continue;
                    try queue.push(pa, .{ .owner = owner, .kind = .{ .block = index }, .bounds = block.bounds orelse return error.InvalidNativeAggregateArtifact });
                    owner.pending += 1;
                }
            },
        }
    }
    if (top.external == null and top.count == 0) {
        self.order_done = true;
        return null;
    }
    const rows = try top.finishPage(a, 0, top.capacity);
    const result = try a.alloc(operators.GroupResult, rows.len);
    for (result, rows, 0..) |*row, input, index| {
        const key_cells = try a.alloc(local.sql_scalar.Datum, input.keys.len);
        for (key_cells, input.keys) |*cell, value| cell.* = try operators.cloneDatum(a, value);
        const cells = try a.alloc(local.sql_scalar.Datum, input.values.len);
        for (cells, input.values) |*cell, value| cell.* = try operators.cloneDatum(a, value);
        row.* = .{ .keys = key_cells, .aggregates = cells, .ordinal = self.order_emitted + index, .aggregate_slots = self.output_slots };
    }
    const boundary = try keys.encode(self.a, rows[rows.len - 1].keys);
    if (self.order_boundary) |old| self.a.free(old);
    self.order_boundary = boundary;
    // SQL TopK uses ordinals to stabilize equal ORDER BY prefixes. Preserve
    // this provider's complete-key tie order across page sizes and LIMITs.
    self.order_emitted += rows.len;
    return result;
}
fn partitionFanout(depth: u8) usize {
    return if (depth == 0) 64 else 4;
}
fn partitionShift(depth: u8) u6 {
    return if (depth == 0) 0 else @intCast(6 + (depth - 1) * 2);
}
