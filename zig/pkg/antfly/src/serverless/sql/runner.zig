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

//! Bounded typed task execution shared by local dispatch and remote workers.
//! Source owners apply the retained WAL overlay before predicates or filters.
const std = @import("std");
const local = @import("antfly_local_sources");
const plan = @import("plan.zig");
const exchange = @import("exchange.zig");
const state = @import("state.zig");
const operators = local.sql_operators;
const Datum = local.sql_scalar.Datum;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
pub const Stream = struct {
    ptr: *anyopaque,
    /// At most maximum rows; returned cells borrow the supplied page arena.
    next_fn: *const fn (*anyopaque, A, usize) anyerror!?[]const []const Datum,
    close_fn: *const fn (*anyopaque) void,
    pub fn close(self: Stream) void {
        self.close_fn(self.ptr);
    }
};
pub const Source = struct {
    ptr: *anyopaque,
    /// Must validate the capability's tenant/schema/table incarnation/expiry,
    /// retain its base and copied WAL cut, and open exactly this immutable split.
    open: *const fn (*anyopaque, A, plan.Digest, plan.Input, u32, ?*const local.sql_dynamic_filter.Filter, Context) anyerror!Stream,
};
pub const OutputRows = struct {
    arena: std.heap.ArenaAllocator,
    partitions: []std.ArrayList(operators.Row),
    bytes: u64 = 0,
    limit: u64,
    aggregation: ?plan.Aggregation = null,
    grouped: ?*operators.Grouped = null,
    pub fn init(a: A, count: usize, limit: u64) !OutputRows {
        const partitions = try a.alloc(std.ArrayList(operators.Row), count);
        @memset(partitions, .empty);
        return .{ .arena = .init(a), .partitions = partitions, .limit = limit };
    }
    pub fn deinit(self: *OutputRows, a: A) void {
        for (self.partitions) |*partition| partition.deinit(a);
        a.free(self.partitions);
        self.arena.deinit();
        if (self.grouped) |grouped| grouped.deinit();
    }
    pub fn add(self: *OutputRows, a: A, partition: usize, cells: []const Datum) !void {
        if (self.aggregation) |aggregate| {
            var group_cells: [32]Datum = undefined;
            var inputs: [64]Datum = undefined;
            for (aggregate.groups, 0..) |ordinal, i| group_cells[i] = cells[ordinal];
            for (aggregate.inputs, 0..) |ordinal, i| inputs[i] = if (ordinal) |column| cells[column] else Datum.json(.{ .integer = 1 });
            try self.grouped.?.add(group_cells[0..aggregate.groups.len], inputs[0..aggregate.inputs.len]);
            return;
        }
        var cost: u64 = @sizeOf(operators.Row) + cells.len * @sizeOf(Datum);
        for (cells) |cell| cost +|= try operators.datumBytes(cell);
        if (cost > self.limit -| self.bytes) return error.SqlJoinBudgetExceeded;
        const owned = try self.arena.allocator().alloc(Datum, cells.len);
        for (cells, owned) |cell, *out| out.* = try operators.cloneDatum(self.arena.allocator(), cell);
        try self.partitions[partition].append(a, .{ .values = owned, .keys = &.{}, .ordinal = self.partitions[partition].items.len });
        self.bytes += cost;
    }
    pub fn publish(self: *OutputRows, a: A, store: exchange.Store, query: plan.Digest, expires: u64, limit: usize) !state.Output {
        if (self.aggregation != null) {
            const previous = self.aggregation;
            self.aggregation = null;
            defer self.aggregation = previous;
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.free_all);
                const sa = scratch.allocator();
                const partial = (try self.grouped.?.nextPartialResult(sa)) orelse break;
                const cells = try sa.alloc(Datum, partial.keys.len + partial.aggregates.len);
                @memcpy(cells[0..partial.keys.len], partial.keys);
                @memcpy(cells[partial.keys.len..], partial.aggregates);
                try self.add(a, 0, cells);
            }
        }
        const partitions = try a.alloc([]const operators.Row, self.partitions.len);
        defer a.free(partitions);
        for (self.partitions, partitions) |partition, *out| out.* = partition.items;
        return store.put(a, query, expires, partitions, limit);
    }
};
fn keys(cells: []const Datum, ordinals: []const u16, out: *[32]Datum) ![]const Datum {
    if (ordinals.len > out.len) return error.InvalidSqlDistributedPlan;
    for (ordinals, 0..) |ordinal, i| {
        if (ordinal >= cells.len) return error.InvalidSqlBackendResponse;
        out[i] = cells[ordinal];
    }
    return out[0..ordinals.len];
}
fn blockRow(a: A, block: local.sql_spill.ColumnarBlock, row: usize) ![]const Datum {
    const cells = try a.alloc(Datum, block.values.len);
    for (cells, 0..) |*cell, column| cell.* = try block.cell(row, column);
    return cells;
}
pub const Runner = struct {
    a: A,
    source: Source,
    exchange_store: exchange.Store,
    context: Context,
    spill: ?*local.sql_spill.Manager = null,
    pub fn run(self: Runner, query: plan.Digest, specification: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        try self.context.ensureActive();
        return switch (task.kind) {
            .merge_probe_matches, .merge_build_matches => @import("skew_runner.zig").merge(self, query, specification, task, dependencies),
            .tile_pair => @import("skew_runner.zig").pair(self, query, specification, task, dependencies),
            .finalize_probe, .finalize_build => @import("skew_runner.zig").finalize(self, query, specification, task, dependencies),
            .passthrough => blk: {
                if (dependencies.len != 1) return error.InvalidSqlExecutionState;
                var ref = dependencies[0];
                ref.key = try self.a.dupe(u8, ref.key);
                ref.matches = null;
                break :blk ref;
            },
            .scan_build, .map_left, .map_right => self.map(query, specification, task),
            .bucket_join, .broadcast_probe, .reduce => self.join(query, specification, task, dependencies),
            .aggregate => self.merge(query, specification, dependencies, task.finalize),
            .results => error.SqlDistributedJoinUnsupported,
        };
    }
    fn map(self: Runner, query: plan.Digest, spec: plan.Plan, task: plan.Task) !state.Output {
        const input = if (task.kind == .map_left) spec.left else spec.right;
        const ordinals = if (task.kind == .map_left) spec.join.left_keys else spec.join.right_keys;
        if (task.split >= input.splits) return error.InvalidSqlDistributedPlan;
        const stream = try self.source.open(self.source.ptr, self.a, spec.tenant, input, task.split, null, self.context);
        defer stream.close();
        var output = try OutputRows.init(self.a, if (task.kind == .scan_build) 1 else spec.partitions, @min(spec.limits.task_output_bytes, spec.limits.task_memory / 4));
        defer output.deinit(self.a);
        var page = std.heap.ArenaAllocator.init(self.a);
        defer page.deinit();
        var count: u64 = 0;
        while (true) {
            try self.context.ensureActive();
            _ = page.reset(.free_all);
            const rows = (try stream.next_fn(stream.ptr, page.allocator(), 256)) orelse break;
            if (rows.len == 0 or rows.len > 256) return error.InvalidSqlBackendResponse;
            count += rows.len;
            if (count > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
            for (rows) |cells| {
                if (cells.len != input.columns.len) return error.InvalidSqlBackendResponse;
                if (input.filter) |filter| if (!try filter.matches(self.a, cells)) continue;
                var key: [32]Datum = undefined;
                const hashed = try operators.HashJoin.keyHash(try keys(cells, ordinals, &key));
                if (hashed == null and (if (task.kind == .map_left) !spec.join.preservesProbe() else !spec.join.preservesBuild())) continue;
                // NULL keys never match but outer/anti joins must retain them.
                const partition = if (task.kind == .scan_build) 0 else (hashed orelse 0) & (spec.partitions - 1);
                try output.add(self.a, @intCast(partition), cells);
            }
        }
        return output.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
    }
    fn join(self: Runner, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        const broadcast = task.kind == .broadcast_probe;
        const bucketed = task.kind == .bucket_join;
        if ((broadcast and spec.join.preservesBuild()) or task.partition >= spec.partitions) return error.SqlDistributedJoinUnsupported;
        const build_count = spec.right.splits;
        if (!broadcast and !bucketed and self.spill != null and spec.join.kind != .semi and spec.join.kind != .anti) return self.grace(query, spec, task, dependencies);
        if (dependencies.len != (if (bucketed) @as(usize, 0) else build_count + (if (broadcast) @as(usize, 0) else spec.left.splits))) return error.InvalidSqlDistributedPlan;
        const hash = try operators.HashJoin.create(self.a, .{ .bytes = @intCast(spec.limits.task_memory / 2), .rows = @intCast(spec.limits.task_scan_rows), .spill = self.spill });
        defer hash.deinit();
        const columns = try self.a.alloc(local.sql_dynamic_filter.Column, spec.join.left_keys.len);
        defer self.a.free(columns);
        for (columns, spec.join.left_keys) |*column, ordinal| column.* = spec.left.columns[ordinal];
        const filter = if ((broadcast or bucketed) and spec.join.mayFilterProbe()) try local.sql_dynamic_filter.Filter.create(self.a, columns, 64 * 1024) else null;
        defer if (filter) |value| value.close();
        var decode = std.heap.ArenaAllocator.init(self.a);
        defer decode.deinit();
        var block_arena = std.heap.ArenaAllocator.init(self.a);
        defer block_arena.deinit();
        var row_arena = std.heap.ArenaAllocator.init(self.a);
        defer row_arena.deinit();
        var input_rows: u64 = 0;
        if (bucketed) {
            var input = spec.right;
            input.bucket_group = .{ .index = task.partition, .count = spec.partitions };
            const stream = try self.source.open(self.source.ptr, self.a, spec.tenant, input, task.partition, null, self.context);
            defer stream.close();
            while (true) {
                try self.context.ensureActive();
                _ = decode.reset(.free_all);
                const rows = (try stream.next_fn(stream.ptr, decode.allocator(), 256)) orelse break;
                if (rows.len == 0 or rows.len > 256) return error.InvalidSqlBackendResponse;
                for (rows) |cells| {
                    if (cells.len != spec.right.columns.len) return error.InvalidSqlBackendResponse;
                    input_rows += 1;
                    if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    if (spec.right.filter) |filter_| if (!try filter_.matches(self.a, cells)) continue;
                    var key: [32]Datum = undefined;
                    const values = try keys(cells, spec.join.right_keys, &key);
                    if (!spec.join.preservesBuild() and try operators.HashJoin.keyHash(values) == null) continue;
                    try hash.add(cells, values);
                    if (filter) |value| try value.add(values);
                }
            }
        } else for (dependencies[0..build_count]) |ref| {
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.readRange(decode.allocator(), ref, if (broadcast) 0 else task.partition, if (broadcast) 1 else task.partition_end, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                for (0..block.count()) |index| {
                    try self.context.ensureActive();
                    _ = row_arena.reset(.free_all);
                    const cells = try blockRow(row_arena.allocator(), block, index);
                    if (cells.len != spec.right.columns.len) return error.InvalidSqlExchange;
                    input_rows += 1;
                    if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    var key: [32]Datum = undefined;
                    const values = try keys(cells, spec.join.right_keys, &key);
                    if (!spec.join.preservesBuild() and try operators.HashJoin.keyHash(values) == null) continue;
                    try hash.add(cells, values);
                    if (filter) |value| try value.add(values);
                }
            }
        }
        if (filter) |value| value.sealed = true;
        var output = try OutputRows.init(self.a, 1, @min(spec.limits.task_output_bytes, spec.limits.task_memory / 4));
        defer output.deinit(self.a);
        if (spec.aggregate) |aggregate| {
            output.grouped = try operators.Grouped.create(self.a, aggregate.specs, .{ .groups = 65536, .bytes = @intCast(spec.limits.task_memory / 8), .spill = self.spill });
            output.aggregation = aggregate;
            if (aggregate.groups.len == 0) try output.grouped.?.ensureGlobalGroup();
        }
        if (broadcast or bucketed) {
            var input = spec.left;
            if (bucketed) input.bucket_group = .{ .index = task.partition, .count = spec.partitions };
            const stream = try self.source.open(self.source.ptr, self.a, spec.tenant, input, if (bucketed) task.partition else task.split, filter, self.context);
            defer stream.close();
            while (true) {
                try self.context.ensureActive();
                _ = decode.reset(.free_all);
                const rows = (try stream.next_fn(stream.ptr, decode.allocator(), 256)) orelse break;
                if (rows.len == 0 or rows.len > 256) return error.InvalidSqlBackendResponse;
                for (rows) |cells| {
                    input_rows += 1;
                    if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    if (spec.left.filter) |filter_| if (!try filter_.matches(self.a, cells)) continue;
                    try self.probe(spec, hash, cells, &output, &row_arena);
                }
            }
        } else for (dependencies[build_count..]) |ref| {
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.readRange(decode.allocator(), ref, task.partition, task.partition_end, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                for (0..block.count()) |index| {
                    try self.context.ensureActive();
                    _ = row_arena.reset(.free_all);
                    const cells = try blockRow(row_arena.allocator(), block, index);
                    input_rows += 1;
                    if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    // Probe scratch must not invalidate its borrowed input cells.
                    var matches = std.heap.ArenaAllocator.init(self.a);
                    defer matches.deinit();
                    try self.probe(spec, hash, cells, &output, &matches);
                }
            }
        }
        if (spec.join.preservesBuild()) {
            var next: usize = 0;
            while (try hash.unmatched(&next)) |match| {
                try self.context.ensureActive();
                _ = row_arena.reset(.free_all);
                try self.emit(spec, null, try match.materializeValues(row_arena.allocator()), &output, row_arena.allocator());
            }
        }
        return output.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
    }
    fn grace(self: Runner, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        if (dependencies.len != @as(usize, spec.right.splits) + spec.left.splits) return error.InvalidSqlDistributedPlan;
        const join_ = try local.sql_partition_join.Join.create(self.a, self.spill.?, @intCast(spec.limits.task_memory / 2), @intCast(spec.limits.task_scan_rows), spec.limits.task_memory / 8, spec.join.preservesProbe(), spec.join.preservesBuild());
        defer join_.close();
        var decode = std.heap.ArenaAllocator.init(self.a);
        defer decode.deinit();
        var block_arena = std.heap.ArenaAllocator.init(self.a);
        defer block_arena.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var ordinal: usize = 0;
        for (dependencies, 0..) |ref, input| {
            try self.context.ensureActive();
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.readRange(decode.allocator(), ref, task.partition, task.partition_end, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                for (0..block.count()) |row| {
                    _ = scratch.reset(.free_all);
                    const cells = try blockRow(scratch.allocator(), block, row);
                    var key: [32]Datum = undefined;
                    const build = input < spec.right.splits;
                    const values = try keys(cells, if (build) spec.join.right_keys else spec.join.left_keys, &key);
                    if (ordinal >= spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    try join_.add(build, cells, values, ordinal);
                    ordinal += 1;
                }
            }
        }
        var output = try OutputRows.init(self.a, 1, @min(spec.limits.task_output_bytes, spec.limits.task_memory / 4));
        defer output.deinit(self.a);
        if (spec.aggregate) |aggregate| {
            output.grouped = try operators.Grouped.create(self.a, aggregate.specs, .{ .groups = 65536, .bytes = @intCast(spec.limits.task_memory / 8), .spill = self.spill });
            output.aggregation = aggregate;
            if (aggregate.groups.len == 0) try output.grouped.?.ensureGlobalGroup();
        }
        while (try join_.next()) |pair| {
            try self.context.ensureActive();
            _ = scratch.reset(.free_all);
            if (pair.left != null and pair.right != null) {
                if (spec.join.condition) |condition| {
                    const cells = try scratch.allocator().alloc(Datum, pair.left.?.len + pair.right.?.len);
                    @memcpy(cells[0..pair.left.?.len], pair.left.?);
                    @memcpy(cells[pair.left.?.len..], pair.right.?);
                    if (!try condition.matches(self.a, cells)) continue;
                }
                if (pair.match) |match| try join_.accept(match);
            }
            try self.emit(spec, pair.left, pair.right, &output, scratch.allocator());
        }
        return output.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
    }
    fn merge(self: Runner, query: plan.Digest, spec: plan.Plan, pages: []const state.Output, finalize: bool) !state.Output {
        const aggregate = spec.aggregate orelse return error.InvalidSqlDistributedPlan;
        if (pages.len == 0 or pages.len > 32) return error.InvalidSqlDistributedPlan;
        const grouped = try operators.Grouped.create(self.a, aggregate.specs, .{ .groups = 65536, .bytes = @intCast(spec.limits.task_memory / 2), .spill = self.spill });
        defer grouped.deinit();
        if (aggregate.groups.len == 0) try grouped.ensureGlobalGroup();
        var decode = std.heap.ArenaAllocator.init(self.a);
        defer decode.deinit();
        var block_arena = std.heap.ArenaAllocator.init(self.a);
        defer block_arena.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var ordinal: u64 = 0;
        for (pages) |page| {
            try self.context.ensureActive();
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.read(decode.allocator(), page, 0, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                if (block.values.len != aggregate.groups.len + aggregate.specs.len) return error.InvalidSqlExchange;
                for (0..block.count()) |row| {
                    try self.context.ensureActive();
                    _ = scratch.reset(.free_all);
                    const cells = try blockRow(scratch.allocator(), block, row);
                    try grouped.importPartial(cells[0..aggregate.groups.len], cells[aggregate.groups.len..], ordinal);
                    ordinal += 1;
                }
            }
        }
        var output = try OutputRows.init(self.a, 1, @min(spec.limits.task_output_bytes, spec.limits.task_memory / 4));
        defer output.deinit(self.a);
        while (true) {
            try self.context.ensureActive();
            _ = scratch.reset(.free_all);
            const a = scratch.allocator();
            const row = (if (finalize) try grouped.nextResult(a) else try grouped.nextPartialResult(a)) orelse break;
            const cells = try a.alloc(Datum, row.keys.len + row.aggregates.len);
            @memcpy(cells[0..row.keys.len], row.keys);
            @memcpy(cells[row.keys.len..], row.aggregates);
            try output.add(self.a, 0, cells);
        }
        return output.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
    }
    fn probe(self: Runner, spec: plan.Plan, hash: *operators.HashJoin, left: []const Datum, output: *OutputRows, scratch: *std.heap.ArenaAllocator) !void {
        if (left.len != spec.left.columns.len) return error.InvalidSqlBackendResponse;
        var key: [32]Datum = undefined;
        var iterator = try hash.probe(try keys(left, spec.join.left_keys, &key));
        var matched = false;
        while (try iterator.next()) |match| {
            try self.context.ensureActive();
            _ = scratch.reset(.free_all);
            const right = try match.materializeValues(scratch.allocator());
            if (spec.join.condition) |condition| {
                const cells = try scratch.allocator().alloc(Datum, left.len + right.len);
                @memcpy(cells[0..left.len], left);
                @memcpy(cells[left.len..], right);
                if (!try condition.matches(self.a, cells)) continue;
            }
            matched = true;
            try hash.markMatched(match.index);
            if (spec.join.kind == .anti) break;
            try self.emit(spec, left, if (spec.join.kind == .semi) null else right, output, scratch.allocator());
            if (spec.join.kind == .semi) break;
        }
        if (!matched and spec.join.preservesProbe()) {
            _ = scratch.reset(.free_all);
            try self.emit(spec, left, null, output, scratch.allocator());
        }
    }
    pub fn emit(self: Runner, spec: plan.Plan, left: ?[]const Datum, right: ?[]const Datum, output: *OutputRows, a: A) !void {
        const width = spec.left.columns.len + (if (spec.join.kind == .semi or spec.join.kind == .anti) @as(usize, 0) else spec.right.columns.len);
        const cells = try a.alloc(Datum, width);
        @memset(cells, .{});
        if (left) |values| @memcpy(cells[0..values.len], values);
        if (right) |values| @memcpy(cells[spec.left.columns.len..], values);
        if (spec.join.projection.len == 0) return output.add(self.a, 0, cells);
        const projected = try a.alloc(Datum, spec.join.projection.len);
        for (spec.join.projection, projected) |ordinal, *cell| cell.* = cells[ordinal];
        try output.add(self.a, 0, projected);
    }
};
