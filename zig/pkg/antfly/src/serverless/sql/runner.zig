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
    next_batch_fn: ?*const fn (*anyopaque, A, usize) anyerror!?local.sql_execution_batch.Batch = null,
    pub fn nextBatch(self: Stream, a: A, maximum: usize) !?local.sql_execution_batch.Batch {
        if (self.next_batch_fn) |next| return next(self.ptr, a, maximum);
        return .{ .rows = (try self.next_fn(self.ptr, a, maximum)) orelse return null };
    }
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
    partitions: []local.sql_typed_store.Store,
    bytes: u64 = 0,
    limit: u64,
    aggregation: ?plan.Aggregation = null,
    grouped: ?*operators.Grouped = null,
    pub fn init(a: A, count: usize, limit: u64) !OutputRows {
        const partitions = try a.alloc(local.sql_typed_store.Store, count);
        for (partitions) |*partition| partition.* = .init(a);
        return .{ .partitions = partitions, .limit = limit };
    }
    pub fn deinit(self: *OutputRows, a: A) void {
        for (self.partitions) |*partition| partition.deinit();
        a.free(self.partitions);
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
        _ = a;
        _ = try self.partitions[partition].append(cells);
        self.bytes += cost;
    }
    pub fn addBatch(self: *OutputRows, a: A, partition: usize, batch: local.sql_execution_batch.Batch) !void {
        if (self.aggregation != null) {
            for (0..batch.len()) |row| try self.add(a, partition, try batch.row(a, row));
            return;
        }
        const store = &self.partitions[partition];
        const cost = (try store.batchAppendBytes(a, batch, std.math.maxInt(usize))) +| store.appendGrowthBytes(batch.len());
        if (cost > self.limit -| self.bytes) return error.SqlJoinBudgetExceeded;
        try store.appendBatch(batch);
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
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const partitions = try scratch.allocator().alloc(exchange.Store.Input, self.partitions.len);
        for (self.partitions, partitions) |*partition, *out| {
            const ordinals = try scratch.allocator().alloc(u64, partition.len);
            for (ordinals, 0..) |*ordinal, row| ordinal.* = row;
            out.* = .{ .values = .{ .retained = .{ .store = partition, .count = partition.len } }, .ordinals = ordinals };
        }
        return store.putBatches(a, query, expires, partitions, limit);
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
pub fn batchKeys(a: A, batch: local.sql_execution_batch.Batch, ordinals: []const u16) ![]const []const Datum {
    if (ordinals.len > 32) return error.InvalidSqlDistributedPlan;
    const result = try a.alloc([]const Datum, batch.len());
    const cells = try a.alloc(Datum, batch.len() * ordinals.len);
    for (result, 0..) |*row, index| {
        const key = cells[index * ordinals.len ..][0..ordinals.len];
        for (ordinals, key) |column, *value| {
            if (column >= batch.width()) return error.InvalidSqlExchange;
            value.* = try batch.cell(a, index, column);
        }
        row.* = key;
    }
    return result;
}
pub const Runner = struct {
    a: A,
    source: Source,
    exchange_store: exchange.Store,
    context: Context,
    spill: ?*local.sql_spill.Manager = null,
    build_cache: ?*@import("build_cache.zig").Cache = null,
    predicates: ?*@import("expression.zig").Task = null,
    pub fn run(self: Runner, query: plan.Digest, specification: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        try self.context.ensureActive();
        const left_filter = switch (task.kind) {
            .map_left, .bucket_join, .broadcast_probe => specification.left.filter,
            else => null,
        };
        const right_filter = switch (task.kind) {
            .scan_build, .map_right, .bucket_join => specification.right.filter,
            else => null,
        };
        const condition = switch (task.kind) {
            .bucket_join, .broadcast_probe, .reduce, .tile_pair => specification.join.condition,
            else => null,
        };
        var predicates = try @import("expression.zig").Task.init(self.a, self.context, specification.limits.task_memory, left_filter, right_filter, condition);
        defer predicates.deinit();
        var prepared = self;
        prepared.predicates = &predicates;
        return prepared.runPrepared(query, specification, task, dependencies) catch |err| {
            // Residency is optional. A build exceeding its shared cache slot
            // retries under the original task budget, without retaining failure.
            if (prepared.build_cache != null and (err == error.OutOfMemory or err == error.SqlProgramLimitExceeded)) {
                prepared.build_cache = null;
                return prepared.runPrepared(query, specification, task, dependencies);
            }
            return err;
        };
    }
    pub fn predicate(self: Runner, kind: @import("expression.zig").Task.Kind, cells: []const Datum) !bool {
        return (self.predicates orelse return error.InvalidSqlExecutionState).matches(kind, cells);
    }
    fn runPrepared(self: Runner, query: plan.Digest, specification: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        return switch (task.kind) {
            .merge_probe_matches, .merge_build_matches => @import("skew_runner.zig").merge(self, query, specification, task, dependencies),
            .consolidate => self.consolidate(query, specification, task, dependencies),
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
    fn consolidate(self: Runner, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        if (dependencies.len == 0 or dependencies.len > 32) return error.InvalidSqlExecutionState;
        var result = try OutputRows.init(self.a, 1, @min(spec.limits.task_output_bytes, spec.limits.task_memory / 4));
        defer result.deinit(self.a);
        var decode = std.heap.ArenaAllocator.init(self.a);
        defer decode.deinit();
        var blocks = std.heap.ArenaAllocator.init(self.a);
        defer blocks.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (dependencies) |ref| {
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.readRange(decode.allocator(), ref, if (task.stage == 1) task.partition else 0, if (task.stage == 1) task.partition_end else 1, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&blocks, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                try self.context.ensureActive();
                _ = scratch.reset(.free_all);
                try result.addBatch(scratch.allocator(), 0, block.batch());
            }
        }
        return result.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
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
            const batch = (try stream.nextBatch(page.allocator(), 256)) orelse break;
            if (batch.len() == 0 or batch.len() > 256 or batch.width() != input.columns.len) return error.InvalidSqlBackendResponse;
            count += batch.len();
            if (count > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
            const batch_keys = try batchKeys(page.allocator(), batch, ordinals);
            const selections = try page.allocator().alloc(std.ArrayList(usize), output.partitions.len);
            @memset(selections, .empty);
            for (batch_keys, 0..) |key, row| {
                if (input.filter != null) if (!try self.predicate(if (task.kind == .map_left) .left else .right, try batch.row(page.allocator(), row))) continue;
                const hashed = try operators.HashJoin.keyHash(key);
                if (hashed == null and (if (task.kind == .map_left) !spec.join.preservesProbe() else !spec.join.preservesBuild())) continue;
                const partition = if (task.kind == .scan_build) 0 else (hashed orelse 0) & (spec.partitions - 1);
                try selections[@intCast(partition)].append(page.allocator(), row);
            }
            for (selections, 0..) |selection, partition| {
                if (selection.items.len != 0) try output.addBatch(page.allocator(), partition, try batch.select(page.allocator(), selection.items));
            }
        }
        return output.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
    }
    pub fn cachedBuild(self: Runner, query: plan.Digest, spec: plan.Plan, dependencies: []const state.Output, begin: u32, end: u32) !?@import("build_cache.zig").Cache.Lease {
        const cache = self.build_cache orelse return null;
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const identity = try std.json.Stringify.valueAlloc(scratch.allocator(), .{ .query = query, .tenant = spec.tenant, .bucket = self.exchange_store.bucket, .prefix = self.exchange_store.prefix, .keys = spec.join.right_keys, .dependencies = dependencies, .begin = begin, .end = end }, .{});
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(identity, &digest, .{});
        return cache.acquire(digest, @intCast(spec.limits.task_memory / 2), @intCast(spec.limits.task_scan_rows));
    }
    fn join(self: Runner, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
        const broadcast = task.kind == .broadcast_probe;
        const bucketed = task.kind == .bucket_join;
        if ((broadcast and spec.join.preservesBuild()) or task.partition >= spec.partitions) return error.SqlDistributedJoinUnsupported;
        const build_count = task.build_count orelse spec.right.splits;
        if (!broadcast and !bucketed and self.spill != null and spec.join.kind != .semi and spec.join.kind != .anti) return self.grace(query, spec, task, dependencies);
        if (dependencies.len != (if (bucketed) @as(usize, 0) else build_count + (if (broadcast) @as(usize, 0) else (if (task.build_count != null) @as(u32, 1) else spec.left.splits)))) return error.InvalidSqlDistributedPlan;
        var lease = if (broadcast) try self.cachedBuild(query, spec, dependencies[0..build_count], 0, 1) else null;
        defer if (lease) |*value| value.release();
        const hash = if (lease) |value| value.hash() else try operators.HashJoin.create(self.a, .{ .bytes = @intCast(spec.limits.task_memory / 2), .rows = @intCast(spec.limits.task_scan_rows), .spill = self.spill });
        defer if (lease == null) hash.deinit();
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
        var input_rows: u64 = if (lease != null and lease.?.hit) hash.buildKeys().len() else 0;
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
                    if (spec.right.filter != null) if (!try self.predicate(.right, cells)) continue;
                    var key: [32]Datum = undefined;
                    const values = try keys(cells, spec.join.right_keys, &key);
                    if (!spec.join.preservesBuild() and try operators.HashJoin.keyHash(values) == null) continue;
                    try hash.add(cells, values);
                    if (filter) |value| try value.add(values);
                }
            }
        } else if (lease == null or !lease.?.hit) {
            for (dependencies[0..build_count]) |ref| {
                _ = decode.reset(.free_all);
                var data = try self.exchange_store.readRange(decode.allocator(), ref, if (broadcast or task.build_count != null) 0 else task.partition, if (broadcast or task.build_count != null) 1 else task.partition_end, @intCast(spec.limits.task_memory / 4));
                while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                    defer block.deinit();
                    try self.context.ensureActive();
                    _ = row_arena.reset(.free_all);
                    if (block.values.len != spec.right.columns.len) return error.InvalidSqlExchange;
                    input_rows += block.count();
                    if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                    const batch = block.batch();
                    const build_keys = try batchKeys(row_arena.allocator(), batch, spec.join.right_keys);
                    try hash.addBatch(row_arena.allocator(), batch, build_keys);
                    if (filter) |value| for (build_keys) |key| try value.add(key);
                }
            }
        }
        if (lease) |*value| {
            value.complete();
            if (value.hit) {
                if (filter) |target| {
                    const cached_keys = hash.buildKeys();
                    for (0..cached_keys.len()) |row| {
                        _ = row_arena.reset(.free_all);
                        const key = try row_arena.allocator().alloc(Datum, cached_keys.width());
                        for (key, 0..) |*cell, column| cell.* = try cached_keys.cell(row_arena.allocator(), row, column);
                        try target.add(key);
                    }
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
                const batch = (try stream.nextBatch(decode.allocator(), 256)) orelse break;
                if (batch.len() == 0 or batch.len() > 256) return error.InvalidSqlBackendResponse;
                input_rows += batch.len();
                if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                try self.probeBatch(spec, hash, batch, &output, decode.allocator(), true);
            }
        } else for (dependencies[build_count..]) |ref| {
            _ = decode.reset(.free_all);
            var data = try self.exchange_store.readRange(decode.allocator(), ref, if (task.build_count != null) 0 else task.partition, if (task.build_count != null) 1 else task.partition_end, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                try self.context.ensureActive();
                _ = row_arena.reset(.free_all);
                input_rows += block.count();
                if (input_rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
                try self.probeBatch(spec, hash, block.batch(), &output, row_arena.allocator(), false);
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
        if (dependencies.len != (if (task.build_count != null) @as(usize, 2) else @as(usize, spec.right.splits) + spec.left.splits)) return error.InvalidSqlDistributedPlan;
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
            var data = try self.exchange_store.readRange(decode.allocator(), ref, if (task.build_count != null) 0 else task.partition, if (task.build_count != null) 1 else task.partition_end, @intCast(spec.limits.task_memory / 4));
            while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
                defer block.deinit();
                try self.context.ensureActive();
                _ = scratch.reset(.free_all);
                const build = input < (task.build_count orelse spec.right.splits);
                if (block.count() > spec.limits.task_scan_rows -| ordinal) return error.SqlJoinBudgetExceeded;
                const batch = block.batch();
                const batch_keys = try batchKeys(scratch.allocator(), batch, if (build) spec.join.right_keys else spec.join.left_keys);
                try join_.addBatch(build, batch, batch_keys, 0);
                ordinal += block.count();
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
                if (spec.join.condition != null) {
                    const cells = try scratch.allocator().alloc(Datum, pair.left.?.len + pair.right.?.len);
                    @memcpy(cells[0..pair.left.?.len], pair.left.?);
                    @memcpy(cells[pair.left.?.len..], pair.right.?);
                    if (!try self.predicate(.condition, cells)) continue;
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
    fn probeBatch(self: Runner, spec: plan.Plan, hash: *operators.HashJoin, batch: local.sql_execution_batch.Batch, output: *OutputRows, arena: A, apply_filter: bool) !void {
        if (batch.width() != spec.left.columns.len) return error.InvalidSqlBackendResponse;
        const probe_keys = try batchKeys(arena, batch, spec.join.left_keys);
        const probes = try hash.probeBatch(arena, probe_keys);
        var rows = std.heap.ArenaAllocator.init(self.a);
        defer rows.deinit();
        var matches = std.heap.ArenaAllocator.init(self.a);
        defer matches.deinit();
        for (probes, 0..) |*iterator, row| {
            try self.context.ensureActive();
            _ = rows.reset(.free_all);
            const left = try batch.select(rows.allocator(), try rows.allocator().dupe(usize, &.{row}));
            const left_cells = if ((apply_filter and spec.left.filter != null) or spec.join.condition != null) try left.row(rows.allocator(), 0) else &.{};
            if (apply_filter and spec.left.filter != null) if (!try self.predicate(.left, left_cells)) continue;
            var matched = false;
            while (try iterator.next()) |match| {
                try self.context.ensureActive();
                _ = matches.reset(.free_all);
                const right = try match.valuesBatch(matches.allocator());
                if (spec.join.condition != null) {
                    const right_cells = try right.row(matches.allocator(), 0);
                    const cells = try matches.allocator().alloc(Datum, left_cells.len + right_cells.len);
                    @memcpy(cells[0..left_cells.len], left_cells);
                    @memcpy(cells[left_cells.len..], right_cells);
                    if (!try self.predicate(.condition, cells)) continue;
                }
                matched = true;
                if (spec.join.preservesBuild()) try hash.markMatched(match.index);
                if (spec.join.kind == .anti) break;
                try self.emitBatch(spec, left, if (spec.join.kind == .semi) null else right, output, matches.allocator());
                if (spec.join.kind == .semi) break;
            }
            if (!matched and spec.join.preservesProbe()) {
                _ = matches.reset(.free_all);
                try self.emitBatch(spec, left, null, output, matches.allocator());
            }
        }
    }
    pub fn emitBatch(_: Runner, spec: plan.Plan, left: ?local.sql_execution_batch.Batch, right: ?local.sql_execution_batch.Batch, output: *OutputRows, a: A) !void {
        const Batch = local.sql_execution_batch.Batch;
        const View = struct {
            left: ?Batch,
            right: ?Batch,
            left_width: usize,
            projection: []const u16,
            fn column(self: *@This(), ordinal: usize) struct { batch: ?Batch, index: usize } {
                const index = if (self.projection.len == 0) ordinal else self.projection[ordinal];
                return if (index < self.left_width) .{ .batch = self.left, .index = index } else .{ .batch = self.right, .index = index - self.left_width };
            }
            fn cell(raw: *anyopaque, alloc: A, _: usize, ordinal: usize) anyerror!Datum {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const value = self.column(ordinal);
                return if (value.batch) |source| source.cell(alloc, 0, value.index) else .{};
            }
            fn frozen(raw: *anyopaque, _: usize, ordinal: usize) anyerror!?local.sql_execution_batch.FrozenJson {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const value = self.column(ordinal);
                return if (value.batch) |source| source.frozenJson(0, value.index) else null;
            }
            fn json(raw: *anyopaque, _: usize, ordinal: usize) anyerror!?[]const u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const value = self.column(ordinal);
                return if (value.batch) |source| source.encodedJson(0, value.index) else null;
            }
        };
        const width = spec.left.columns.len + (if (spec.join.kind == .semi or spec.join.kind == .anti) @as(usize, 0) else spec.right.columns.len);
        var view: View = .{ .left = left, .right = right, .left_width = spec.left.columns.len, .projection = spec.join.projection };
        try output.addBatch(a, 0, .{ .reader = .{ .ptr = &view, .read = View.cell, .read_frozen = View.frozen, .read_json = View.json, .count = 1, .width = if (spec.join.projection.len == 0) width else spec.join.projection.len } });
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

test "distributed SQL typed join projection forwards encoded JSON without scalar materialization and preserves outer nulls" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    const objects: exchange.Store = .{ .client = memory.client(), .bucket = "queries", .prefix = "tenant" };
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "snapshot", .wal_cut = 0, .cut = "cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 1, .estimated_rows = 1, .splits = 1 };
    var spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = 1000, .left = input, .right = input, .join = .{ .kind = .left, .left_keys = &.{0}, .right_keys = &.{0}, .projection = &.{ 1, 0, 1 } }, .strategy = .shuffle, .partitions = 1, .limits = .{} };
    spec.right.columns = &.{.{ .name = "payload", .type = .json }};
    const Reader = struct {
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) anyerror!Datum {
            return error.UnexpectedMaterialization;
        }
        fn json(_: *anyopaque, _: usize, _: usize) anyerror!?[]const u8 {
            return "{\"x\":[1,null]}";
        }
    };
    var marker: u8 = 0;
    const right: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &marker, .read = Reader.cell, .read_json = Reader.json, .count = 1, .width = 1 } };
    const left: local.sql_execution_batch.Batch = .{ .rows = &.{&.{Datum.json(.{ .integer = 7 })}} };
    const executor: Runner = .{ .a = a, .source = undefined, .exchange_store = objects, .context = .{} };
    var output = try OutputRows.init(a, 1, 1024 * 1024);
    defer output.deinit(a);
    try executor.emitBatch(spec, left, right, &output, sa);
    try executor.emitBatch(spec, left, null, &output, sa);
    const ref = try output.publish(sa, objects, @splat('a'), 1000, 1024 * 1024);
    var data = try objects.read(sa, ref, 0, 1024 * 1024);
    const block = (try data.next(sa, 1024 * 1024)).?;
    defer block.deinit();
    try std.testing.expectEqual(@as(usize, 2), block.count());
    try std.testing.expectEqual(@as(i64, 7), (try block.cell(0, 1)).value.integer);
    try std.testing.expectEqual(@as(usize, 2), (try block.cell(0, 0)).value.object.get("x").?.array.items.len);
    try std.testing.expect((try block.cell(1, 0)).sql_null);
    try std.testing.expect((try block.cell(1, 2)).sql_null);
}
