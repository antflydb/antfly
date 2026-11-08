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

//! Bounded tile joins and distributed match-evidence finalization. Each pair
//! publishes immutable match ordinals separately from its SQL result rows.
const std = @import("std");
const local = @import("antfly_local_sources");
const plan = @import("plan.zig");
const state = @import("state.zig");
const physical = @import("physical.zig");
const runner = @import("runner.zig");
const operators = local.sql_operators;
const Datum = local.sql_scalar.Datum;
const A = std.mem.Allocator;
fn output(self: anytype, spec: plan.Plan) !runner.OutputRows {
    var result = try runner.OutputRows.init(self.a, 1, @min(spec.limits.task_output_bytes / 2, spec.limits.task_memory / 8));
    errdefer result.deinit(self.a);
    if (spec.aggregate) |aggregate| {
        result.grouped = try operators.Grouped.create(self.a, aggregate.specs, .{ .groups = 65536, .bytes = @intCast(spec.limits.task_memory / 8), .spill = self.spill });
        result.aggregation = aggregate;
        if (aggregate.groups.len == 0) try result.grouped.?.ensureGlobalGroup();
    }
    return result;
}
fn contains(bits: []const u8, ordinal: usize) bool {
    return ordinal / 8 < bits.len and bits[ordinal / 8] & (@as(u8, 1) << @as(u3, @truncate(ordinal))) != 0;
}
/// Dependencies contain the left and right accepted map output, omitting empty
/// placeholder tiles. SQL NULL keys remain owned by exactly one original tile.
pub fn pair(self: anytype, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
    const part = spec.skew[task.physical_part];
    const left = task.left_tile;
    const right = task.right_tile;
    if (dependencies.len != @as(usize, @intFromBool(left.map != null)) + @intFromBool(right.map != null)) return error.InvalidSqlExecutionState;
    if (left.rows +| right.rows > spec.limits.task_scan_rows) return error.SqlJoinBudgetExceeded;
    const right_dependency = dependencies[@intFromBool(left.map != null)..];
    var lease = if (right.map != null) try self.cachedBuild(query, spec, right_dependency, @intCast(part.partition), @intCast(part.partition + 1)) else null;
    defer if (lease) |*value| value.release();
    const hash = if (lease) |value| value.hash() else try operators.HashJoin.create(self.a, .{ .bytes = @intCast(spec.limits.task_memory / 2), .rows = @intCast(spec.limits.task_scan_rows) });
    defer if (lease == null) hash.deinit();
    var decode = std.heap.ArenaAllocator.init(self.a);
    defer decode.deinit();
    var block_arena = std.heap.ArenaAllocator.init(self.a);
    defer block_arena.deinit();
    var scratch = std.heap.ArenaAllocator.init(self.a);
    defer scratch.deinit();
    var right_rows: usize = if (lease != null and lease.?.hit) hash.buildKeys().len() else 0;
    if (right.map != null and (lease == null or !lease.?.hit)) {
        var data = try self.exchange_store.read(decode.allocator(), dependencies[@intFromBool(left.map != null)], part.partition, @intCast(spec.limits.task_memory / 4));
        while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
            defer block.deinit();
            try self.context.ensureActive();
            _ = scratch.reset(.free_all);
            const batch = block.batch();
            const build_keys = try runner.batchKeys(scratch.allocator(), batch, spec.join.right_keys);
            hash.addBatch(scratch.allocator(), batch, build_keys) catch |err| return if (lease) |value| value.buildError(err) else err;
            right_rows += block.count();
        }
    }
    if (right_rows != right.rows) return error.InvalidSqlExchange;
    if (lease) |*value| value.complete();
    var result = try output(self, spec);
    defer result.deinit(self.a);
    const right_bits = try self.a.alloc(u8, @intCast((right.rows + 7) / 8));
    defer self.a.free(right_bits);
    @memset(right_bits, 0);
    var proof = try runner.OutputRows.init(self.a, 2, @min(spec.limits.task_output_bytes / 2, spec.limits.task_memory / 8));
    defer proof.deinit(self.a);
    const left_bits = try self.a.alloc(u8, @intCast((left.rows + 7) / 8));
    defer self.a.free(left_bits);
    @memset(left_bits, 0);
    var left_rows: usize = 0;
    if (left.map != null) {
        _ = decode.reset(.free_all);
        var data = try self.exchange_store.read(decode.allocator(), dependencies[0], part.partition, @intCast(spec.limits.task_memory / 4));
        while (try data.nextBlock(&block_arena, @intCast(spec.limits.task_memory / 4))) |block| {
            defer block.deinit();
            if (block.count() > left.rows -| left_rows) return error.InvalidSqlExchange;
            _ = scratch.reset(.free_all);
            try self.probeBatchWitness(spec, hash, block.batch(), &result, scratch.allocator(), false, .{ .left = left_bits, .right = right_bits, .left_begin = left_rows });
            left_rows += block.count();
        }
    }
    if (left_rows != left.rows) return error.InvalidSqlExchange;
    try proof.add(self.a, 0, &.{Datum.json(.{ .string = left_bits })});
    try proof.add(self.a, 1, &.{Datum.json(.{ .string = right_bits })});
    const witness = try proof.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes / 2));
    errdefer self.a.free(witness.key);
    var accepted = try result.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes / 2));
    accepted.matches = witness.witness();
    return accepted;
}
pub fn merge(self: anytype, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
    if (dependencies.len == 0 or dependencies.len > 32) return error.InvalidSqlExecutionState;
    const build = task.kind == .merge_build_matches;
    const tile = if (build) task.right_tile else task.left_tile;
    const bits = try self.a.alloc(u8, @intCast((tile.rows + 7) / 8));
    defer self.a.free(bits);
    @memset(bits, 0);
    try mergeBits(self, spec, dependencies, bits, task.stage == spec.joinStage() + 1, build);
    var result = try runner.OutputRows.init(self.a, 1, spec.limits.task_memory / 8);
    defer result.deinit(self.a);
    try result.add(self.a, 0, &.{Datum.json(.{ .string = bits })});
    return result.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes));
}
fn mergeBits(self: anytype, spec: plan.Plan, dependencies: []const state.Output, bits: []u8, pairs: bool, build: bool) !void {
    var decode = std.heap.ArenaAllocator.init(self.a);
    defer decode.deinit();
    var blocks = std.heap.ArenaAllocator.init(self.a);
    defer blocks.deinit();
    for (dependencies) |dependency| {
        try self.context.ensureActive();
        _ = decode.reset(.free_all);
        const witness = if (pairs) (dependency.matches orelse return error.InvalidSqlExchange).output() else dependency;
        var data = try self.exchange_store.read(decode.allocator(), witness, if (pairs and build) 1 else 0, @intCast(spec.limits.task_memory / 4));
        var witnesses: usize = 0;
        while (try data.nextBlock(&blocks, @intCast(spec.limits.task_memory / 4))) |block| {
            defer block.deinit();
            if (block.values.len != 1 or block.count() != 1 or witnesses != 0) return error.InvalidSqlExchange;
            const value = try block.cell(0, 0);
            if (value.sql_null or value.value != .string or value.value.string.len != bits.len) return error.InvalidSqlExchange;
            for (bits, value.value.string) |*byte, incoming| byte.* |= incoming;
            witnesses += 1;
        }
        if (witnesses != 1) return error.InvalidSqlExchange;
    }
}
/// One map tile owns its unmatched/existence results. OR all accepted pair
/// witnesses before emitting; a delayed/retried pair cannot duplicate output.
pub fn finalize(self: anytype, query: plan.Digest, spec: plan.Plan, task: plan.Task, dependencies: []const state.Output) !state.Output {
    const part = spec.skew[task.physical_part];
    const build = task.kind == .finalize_build;
    const tile = if (build) task.right_tile else task.left_tile;
    const opposite = if (build) part.leftCount() else part.rightCount();
    const levels = plan.Plan.witnessLevels(opposite);
    const count = if (levels == 0) opposite else 1;
    if (dependencies.len != count + @intFromBool(tile.map != null)) return error.InvalidSqlExecutionState;
    const bits = try self.a.alloc(u8, @intCast((tile.rows + 7) / 8));
    defer self.a.free(bits);
    @memset(bits, 0);
    var decode = std.heap.ArenaAllocator.init(self.a);
    defer decode.deinit();
    var blocks = std.heap.ArenaAllocator.init(self.a);
    defer blocks.deinit();
    try mergeBits(self, spec, dependencies[@intFromBool(tile.map != null)..], bits, levels == 0, build);
    var result = try output(self, spec);
    defer result.deinit(self.a);
    if (tile.map != null) {
        _ = decode.reset(.free_all);
        var data = try self.exchange_store.read(decode.allocator(), dependencies[0], part.partition, @intCast(spec.limits.task_memory / 4));
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var ordinal: usize = 0;
        while (try data.nextBlock(&blocks, @intCast(spec.limits.task_memory / 4))) |block| {
            defer block.deinit();
            const batch = block.batch();
            var selected: [256]usize = undefined;
            var selected_count: usize = 0;
            _ = scratch.reset(.free_all);
            for (0..block.count()) |row| {
                try self.context.ensureActive();
                const matched = contains(bits, ordinal);
                ordinal += 1;
                if (matched != (!build and spec.join.kind == .semi)) continue;
                selected[selected_count] = row;
                selected_count += 1;
                if (selected_count == selected.len) {
                    try self.emitUnmatched(spec, &batch, selected[0..selected_count], build, &result, scratch.allocator());
                    selected_count = 0;
                    _ = scratch.reset(.free_all);
                }
            }
            if (selected_count != 0) try self.emitUnmatched(spec, &batch, selected[0..selected_count], build, &result, scratch.allocator());
        }
        if (ordinal != tile.rows) return error.InvalidSqlExchange;
    }
    return result.publish(self.a, self.exchange_store, query, spec.expires_ms, @intCast(spec.limits.task_output_bytes / 2));
}
