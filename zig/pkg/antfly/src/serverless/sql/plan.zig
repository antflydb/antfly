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

//! Versioned worker IR. It contains authorized input capabilities and column
//! ordinals, never process pointers, credentials, or relation_binding.Node.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const physical = @import("physical.zig");
pub const Digest = @import("state.zig").Digest;
pub const Column = local.sql_dynamic_filter.Column;
pub const Bucket = struct { column: u16, count: u32 };
pub const Input = struct {
    bucket: ?Bucket = null,
    /// Resolved only on a bucket task; source validates proof against the cut.
    bucket_group: ?struct { index: u16, count: u16 } = null,
    table_id: u64,
    schema: Digest,
    snapshot: []const u8,
    wal_cut: u64,
    /// Shared retained-cut capability, including copied accepted WAL images.
    /// The host validates its tenant, table incarnation, schema and expiry.
    cut: []const u8,
    columns: []const Column,
    filter: ?@import("expression.zig").Program = null,
    estimated_bytes: u64,
    estimated_rows: u64,
    splits: u32,
};
pub const Join = struct {
    kind: enum { inner, left, right, full, semi, anti } = .inner,
    left_keys: []const u16,
    right_keys: []const u16,
    /// Build is physically right. Output columns remain in logical SQL order.
    projection: []const u16 = &.{},
    condition: ?@import("expression.zig").Program = null,
    pub fn preservesProbe(self: Join) bool {
        return self.kind == .left or self.kind == .full or self.kind == .anti;
    }
    pub fn preservesBuild(self: Join) bool {
        return self.kind == .right or self.kind == .full;
    }
    pub fn mayFilterProbe(self: Join) bool {
        return !self.preservesProbe();
    }
};
pub const Limits = struct {
    task_memory: u64 = 128 * 1024 * 1024,
    task_scan_rows: u64 = 1_000_000,
    task_spill_bytes: u64 = 1024 * 1024 * 1024,
    task_output_bytes: u64 = 16 * 1024 * 1024,
    query_scan_bytes: u64 = 1024 * 1024 * 1024,
    query_exchange_bytes: u64 = 1024 * 1024 * 1024,
    query_requests: u64 = 100_000,
    max_tasks: u32 = 4096,
    max_partitions: u16 = 256,
    concurrency: u16 = 8,
    max_attempts: u16 = 3,
    pub fn validate(self: Limits) !void {
        if (self.task_memory < 1024 * 1024 or self.task_memory > 1024 * 1024 * 1024 or self.task_scan_rows == 0 or self.task_output_bytes < 4096 or self.task_output_bytes > @import("exchange.zig").max_object_bytes or self.max_tasks == 0 or self.max_tasks > 65536 or self.max_partitions == 0 or self.max_partitions > 1024 or self.concurrency == 0 or self.concurrency > 256 or self.max_attempts == 0 or self.max_attempts > @import("state.zig").max_task_attempts) return error.InvalidSqlDistributedPlan;
        if (self.task_scan_rows > std.math.maxInt(u32) or self.query_scan_bytes == 0 or self.query_exchange_bytes == 0 or self.query_requests == 0) return error.InvalidSqlDistributedPlan;
    }
};
pub const Aggregation = struct {
    groups: []const u16 = &.{},
    inputs: []const ?u16,
    specs: []const local.sql_operators.AggregateSpec,
};
pub const Strategy = enum { broadcast, shuffle, bucket };
pub const Plan = struct {
    version: u16 = 2,
    tenant: Digest,
    expires_ms: u64,
    left: Input,
    right: Input,
    join: Join,
    strategy: Strategy,
    partitions: u16,
    limits: Limits,
    aggregate: ?Aggregation = null,
    /// Coordinator-owned physical reducer boundaries; absent in submitted IR.
    reduce_boundaries: []const u16 = &.{},
    skew: []const physical.Part = &.{},
    pub fn reduceCount(self: Plan) u32 {
        return if (self.reduce_boundaries.len == 0) self.partitions else @intCast(self.reduce_boundaries.len - 1);
    }
    pub fn withReducers(self: Plan, boundaries: []const u16) !Plan {
        if (boundaries.len != 0) {
            if (self.strategy != .shuffle or boundaries.len < 2 or boundaries.len > @as(usize, self.partitions) + 1 or boundaries[0] != 0 or boundaries[boundaries.len - 1] != self.partitions) return error.InvalidSqlExecutionState;
            for (boundaries[1..], boundaries[0 .. boundaries.len - 1]) |end, start| if (end <= start) return error.InvalidSqlExecutionState;
        }
        var result = self;
        result.reduce_boundaries = boundaries;
        return result;
    }
    pub fn validate(self: Plan, now_ms: u64) !void {
        if (self.version != 2 or self.expires_ms <= now_ms) return error.SqlQueryExpired;
        const limits = self.limits;
        try limits.validate();
        if (self.partitions == 0 or self.partitions > limits.max_partitions or (self.strategy != .bucket and !std.math.isPowerOfTwo(self.partitions))) return error.InvalidSqlDistributedPlan;
        if (self.left.columns.len == 0 or self.right.columns.len == 0 or self.left.columns.len + self.right.columns.len > 2048 or self.left.splits == 0 or self.right.splits == 0 or self.left.splits > 65536 or self.right.splits > 65536 or self.left.cut.len == 0 or self.right.cut.len == 0 or self.left.cut.len > 8192 or self.right.cut.len > 8192 or self.left.snapshot.len > 256 or self.right.snapshot.len > 256) return error.InvalidSqlDistributedPlan;
        if (self.join.left_keys.len == 0 or self.join.left_keys.len > 32 or self.join.left_keys.len != self.join.right_keys.len) return error.InvalidSqlDistributedPlan;
        for (self.join.left_keys, self.join.right_keys) |left, right| {
            if (left >= self.left.columns.len or right >= self.right.columns.len) return error.InvalidSqlDistributedPlan;
            const l = self.left.columns[left];
            const r = self.right.columns[right];
            // Planner coercions must establish one comparison/hash domain on
            // both inputs before the exchange. Never silently hash raw casts.
            if (l.type != r.type or l.element_type != r.element_type) return error.SqlDistributedJoinUnsupported;
        }
        for (self.join.projection) |index| if (index >= self.left.columns.len + (if (self.join.kind == .semi or self.join.kind == .anti) @as(usize, 0) else self.right.columns.len)) return error.InvalidSqlDistributedPlan;
        if (self.strategy == .broadcast and (self.partitions != 1 or self.join.preservesBuild())) return error.SqlDistributedJoinUnsupported;
        if (self.left.bucket_group != null or self.right.bucket_group != null) return error.InvalidSqlDistributedPlan;
        if (self.strategy == .bucket) {
            const l = self.left.bucket orelse return error.InvalidSqlDistributedPlan;
            const r = self.right.bucket orelse return error.InvalidSqlDistributedPlan;
            if (l.column >= self.left.columns.len or r.column >= self.right.columns.len or l.count == 0 or r.count == 0 or l.count % self.partitions != 0 or r.count % self.partitions != 0) return error.InvalidSqlDistributedPlan;
            const aligned = for (self.join.left_keys, self.join.right_keys) |left, right| {
                if (left == l.column and right == r.column) break true;
            } else false;
            if (!aligned) return error.InvalidSqlDistributedPlan;
            const kind = self.left.columns[l.column].type;
            if (kind != .integer and kind != .string) return error.SqlDistributedJoinUnsupported;
        }
        if (self.left.filter) |filter| {
            try filter.validate(self.left.columns.len);
            if (filter.output.kind != .boolean) return error.InvalidSqlDistributedPlan;
        }
        if (self.right.filter) |filter| {
            try filter.validate(self.right.columns.len);
            if (filter.output.kind != .boolean) return error.InvalidSqlDistributedPlan;
        }
        if (self.join.condition) |condition| {
            try condition.validate(self.left.columns.len + self.right.columns.len);
            if (condition.output.kind != .boolean) return error.InvalidSqlDistributedPlan;
        }
        if (self.aggregate) |aggregate| {
            const width = if (self.join.projection.len != 0) self.join.projection.len else self.left.columns.len + (if (self.join.kind == .semi or self.join.kind == .anti) @as(usize, 0) else self.right.columns.len);
            if (aggregate.groups.len > 32 or aggregate.specs.len == 0 or aggregate.specs.len > 64 or aggregate.specs.len != aggregate.inputs.len) return error.InvalidSqlDistributedPlan;
            for (aggregate.groups) |column| if (column >= width) return error.InvalidSqlDistributedPlan;
            for (aggregate.specs, aggregate.inputs) |spec, input| {
                if (spec.distinct or spec.kind == .pattern_set or (input == null and spec.kind != .count)) return error.SqlDistributedJoinUnsupported;
                if (input) |column| if (column >= width) return error.InvalidSqlDistributedPlan;
                try local.sql_operators.Aggregate.validate(spec.kind, spec.input_type);
            }
        }
        const tasks = try self.taskCount();
        if (tasks > limits.max_tasks or self.left.estimated_bytes +| self.right.estimated_bytes > limits.query_scan_bytes) return error.SqlJoinBudgetExceeded;
        // Admission reserves every attempt and each task's maximum output.
        // Runtime counters still enforce per-task limits on actual input/output.
        if (@as(u64, tasks) *| limits.max_attempts *| limits.task_output_bytes > limits.query_exchange_bytes) return error.SqlJoinBudgetExceeded;
        const requests = if (self.strategy == .bucket) @as(u64, self.partitions) *| 8 else if (self.strategy == .broadcast) @as(u64, self.left.splits) *| 4 +| @as(u64, self.right.splits) *| 3 else (@as(u64, self.left.splits) +| self.right.splits) *| (@as(u64, self.partitions) *| 3 +| 4);
        if (requests *| limits.max_attempts > limits.query_requests) return error.SqlJoinBudgetExceeded;
    }
    pub fn outputStage(self: Plan) u32 {
        return if (self.skew.len != 0) self.joinStage() + 1 + self.matchLevels() else self.joinStage();
    }
    pub fn witnessLevels(count_: u32) u32 {
        if (count_ <= 32) return 0;
        var count = count_;
        var levels: u32 = 0;
        while (count > 1) : (levels += 1) count = (count + 31) / 32;
        return levels;
    }
    pub fn witnessGroups(count_: u32, level: u32) u32 {
        if (level == 0) return count_;
        if (level > witnessLevels(count_)) return 0;
        var count = count_;
        for (0..level) |_| count = (count + 31) / 32;
        return count;
    }
    pub fn matchLevels(self: Plan) u32 {
        var levels: u32 = 0;
        for (self.skew) |part| {
            if (self.join.preservesProbe() or self.join.kind == .semi) levels = @max(levels, witnessLevels(part.rightCount()));
            if (self.join.preservesBuild()) levels = @max(levels, witnessLevels(part.leftCount()));
        }
        return levels;
    }
    pub fn witnessOffset(self: Plan, target: usize, build: bool, level: u32) !u32 {
        var offset: u32 = 0;
        for (self.skew, 0..) |part, index| {
            if (index == target and !build) return offset;
            if (self.join.preservesProbe() or self.join.kind == .semi) offset = try std.math.add(u32, offset, try std.math.mul(u32, part.leftCount(), witnessGroups(part.rightCount(), level)));
            if (index == target) return offset;
            if (self.join.preservesBuild()) offset = try std.math.add(u32, offset, try std.math.mul(u32, part.rightCount(), witnessGroups(part.leftCount(), level)));
        }
        return offset;
    }
    pub fn finalizerCount(self: Plan) !u32 {
        var count = try physical.pairCount(self.skew, self.reduceCount());
        for (self.skew) |part| {
            if (self.join.preservesProbe() or self.join.kind == .semi) count = try std.math.add(u32, count, part.leftCount());
            if (self.join.preservesBuild()) count = try std.math.add(u32, count, part.rightCount());
        }
        return count;
    }
    pub fn withPhysical(self: Plan, boundaries: []const u16, parts: []const physical.Part) !Plan {
        var result = try self.withReducers(boundaries);
        if (parts.len > self.partitions or (parts.len != 0 and (self.strategy != .shuffle or boundaries.len < 2))) return error.InvalidSqlExecutionState;
        for (parts, 0..) |part, index| {
            if (part.partition >= self.partitions or part.leftCount() == 0 or part.rightCount() == 0 or (index != 0 and parts[index - 1].partition >= part.partition)) return error.InvalidSqlExecutionState;
            const isolated = for (boundaries[0 .. boundaries.len - 1], boundaries[1..]) |begin, end| {
                if (begin == part.partition and end == begin + 1) break true;
            } else false;
            if (!isolated) return error.InvalidSqlExecutionState;
            for (part.left) |tile| if (tile.map) |id| {
                if (id < self.right.splits or id >= self.right.splits + self.left.splits or tile.rows == 0 or tile.rows > self.limits.task_scan_rows) return error.InvalidSqlExecutionState;
            };
            for (part.right) |tile| if (tile.map) |id| {
                if (id >= self.right.splits or tile.rows == 0 or tile.rows > self.limits.task_scan_rows) return error.InvalidSqlExecutionState;
            };
        }
        result.skew = parts;
        const count = try result.taskCount();
        if (parts.len != 0 and @as(u64, count) *| result.limits.max_attempts *| 16 > result.limits.query_requests) return error.SqlJoinBudgetExceeded;
        if (count > result.limits.max_tasks or @as(u64, count) *| result.limits.max_attempts *| result.limits.task_output_bytes > result.limits.query_exchange_bytes) return error.SqlJoinBudgetExceeded;
        return result;
    }
    pub fn inputLevels(self: Plan) u32 {
        if (self.strategy == .bucket) return 0;
        var count = if (self.strategy == .broadcast) self.right.splits else @max(self.right.splits, self.left.splits);
        if (count <= 32) return 0;
        var levels: u32 = 0;
        while (count > 1) : (levels += 1) count = (count + 31) / 32;
        return levels;
    }
    pub fn inputGroups(count_: u32, level: u32) u32 {
        var count = count_;
        for (0..level) |_| count = (count + 31) / 32;
        return count;
    }
    pub fn ordinaryCount(self: Plan) u32 {
        return self.reduceCount() - @as(u32, @intCast(self.skew.len));
    }
    pub fn ordinaryReducer(self: Plan, ordinal: u32) !u32 {
        var next = ordinal;
        var part: usize = 0;
        for (0..self.reduceCount()) |index| {
            const partition = if (self.reduce_boundaries.len == 0) @as(u16, @intCast(index)) else self.reduce_boundaries[index];
            if (part < self.skew.len and self.skew[part].partition == partition) {
                part += 1;
                continue;
            }
            if (next == 0) return @intCast(index);
            next -= 1;
        }
        return error.InvalidSqlExecutionState;
    }
    pub fn ordinaryOrdinal(self: Plan, reducer: u32) !u32 {
        const partition = if (self.reduce_boundaries.len == 0) @as(u16, @intCast(reducer)) else self.reduce_boundaries[reducer];
        var removed: u32 = 0;
        for (self.skew) |part| {
            if (part.partition >= partition) break;
            removed += 1;
        }
        return std.math.sub(u32, reducer, removed);
    }
    pub fn joinStage(self: Plan) u32 {
        return if (self.strategy == .bucket) 0 else 1 + self.inputLevels();
    }
    pub fn stageTasks(self: Plan, stage: u32) !u32 {
        const join_stage = self.joinStage();
        if (stage == 0 and join_stage != 0) return if (self.strategy == .broadcast) self.right.splits else std.math.add(u32, self.left.splits, self.right.splits);
        if (stage < join_stage) {
            const groups = inputGroups(self.right.splits, stage) + (if (self.strategy == .broadcast) @as(u32, 0) else inputGroups(self.left.splits, stage));
            return std.math.mul(u32, groups, if (self.strategy == .broadcast) 1 else self.ordinaryCount());
        }
        var count: u32 = if (self.strategy == .broadcast) self.left.splits else if (self.skew.len != 0) try self.finalizerCount() else self.reduceCount();
        if (self.skew.len != 0 and stage == join_stage) return physical.pairCount(self.skew, self.reduceCount());
        if (self.skew.len != 0 and stage > join_stage and stage < self.outputStage()) return self.witnessOffset(self.skew.len, false, stage - join_stage);
        if (stage == self.outputStage()) return count;
        if (self.aggregate == null or stage > 16) return error.InvalidSqlDistributedPlan;
        for (self.outputStage() + 1..stage + 1) |_| count = (count + 31) / 32;
        return count;
    }
    pub fn finalStage(self: Plan) u32 {
        if (self.aggregate == null) return self.outputStage() + 1;
        var count: u32 = if (self.strategy == .broadcast) self.left.splits else if (self.skew.len != 0) self.finalizerCount() catch return 9 else self.reduceCount();
        var stage: u32 = self.outputStage() + 1;
        while (count > 32) {
            count = (count + 31) / 32;
            stage += 1;
        }
        return stage + 1;
    }
    pub fn taskCount(self: Plan) !u32 {
        var count: u32 = 0;
        for (0..self.finalStage()) |stage| count = try std.math.add(u32, count, try self.stageTasks(@intCast(stage)));
        return count;
    }
    pub fn encode(self: Plan, a: A) ![]u8 {
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn digest(self: Plan, a: A) !Digest {
        const bytes = try self.encode(a);
        defer a.free(bytes);
        return local.serverless_external_source_mod.lake_catalog.types.digestHex(bytes);
    }
};
pub const Task = struct {
    stage: u32,
    task: u32,
    kind: enum { consolidate, tile_pair, merge_probe_matches, merge_build_matches, finalize_probe, finalize_build, passthrough, bucket_join, scan_build, broadcast_probe, map_left, map_right, reduce, aggregate, results },
    split: u32 = 0,
    build: bool = false,
    reducer: u32 = 0,
    build_count: ?u32 = null,
    partition: u16 = 0,
    partition_end: u16 = 1,
    left_tile: physical.Tile = .{},
    right_tile: physical.Tile = .{},
    physical_part: u16 = 0,
    tile_left: u32 = 0,
    tile_right: u32 = 0,
    dependent: u32 = 0,
    witness_group: u32 = 0,
    finalize: bool = false,
};
/// No row payload travels through the dispatcher. The worker loads the durable
/// plan and accepted dependency records using these deterministic identities.
pub const Ticket = struct {
    tenant: Digest,
    query: @import("state.zig").Query,
    stage: u32,
    task: u32,
    /// Durable claim epoch rotates lease-expired replay, including after a
    /// coordinator restart or endpoint quarantine expiry. Not task identity.
    attempt: u64 = 0,
    pub fn sameTask(self: Ticket, other: Ticket) bool {
        return self.stage == other.stage and self.task == other.task and std.meta.eql(self.query, other.query) and std.meta.eql(self.tenant, other.tenant);
    }
};
/// Parquet compression does not bound the resident typed rows or hash metadata.
/// Include the fixed per-row representation before selecting a build strategy.
pub fn estimatedRetained(input: Input) u64 {
    const fixed = @sizeOf(local.sql_operators.Row) + input.columns.len * @sizeOf(local.sql_scalar.Datum);
    return input.estimated_bytes +| input.estimated_rows *| fixed;
}
/// Compare admissible physical orientations while preserving logical ordinals.
/// Remapped projection and instruction storage belongs to the caller's allocator.
pub fn choosePhysical(a: A, left: Input, right: Input, tenant: Digest, expires_ms: u64, join: Join, limits: Limits, now_ms: u64) !Plan {
    const forward = choose(left, right, tenant, expires_ms, join, limits, now_ms) catch |err| switch (err) {
        error.SqlJoinBudgetExceeded => null,
        else => return err,
    };
    if (join.kind == .semi or join.kind == .anti) return forward orelse error.SqlJoinBudgetExceeded;
    var reversed = join;
    reversed.left_keys = join.right_keys;
    reversed.right_keys = join.left_keys;
    reversed.kind = switch (join.kind) {
        .left => .right,
        .right => .left,
        else => join.kind,
    };
    const backward = choose(right, left, tenant, expires_ms, reversed, limits, now_ms) catch |err| switch (err) {
        error.SqlJoinBudgetExceeded => null,
        else => return err,
    };
    var selected = backward orelse return forward orelse error.SqlJoinBudgetExceeded;
    if (forward) |original| {
        const original_cost = orientationCost(original);
        const swapped_cost = orientationCost(selected);
        if (original_cost < swapped_cost or (original_cost == swapped_cost and estimatedRetained(original.right) <= estimatedRetained(selected.right))) return original;
    }
    const projection = try a.alloc(u16, if (join.projection.len == 0) left.columns.len + right.columns.len else join.projection.len);
    errdefer a.free(projection);
    for (projection, 0..) |*ordinal, index| ordinal.* = swappedOrdinal(if (join.projection.len == 0) @intCast(index) else join.projection[index], left.columns.len, right.columns.len);
    selected.join.projection = projection;
    var instructions: ?[]local.sql_scalar.Instruction = null;
    errdefer if (instructions) |owned| a.free(owned);
    if (join.condition) |condition| {
        const owned = try a.dupe(local.sql_scalar.Instruction, condition.instructions);
        instructions = owned;
        for (owned) |*instruction| if (instruction.operation == .column) {
            instruction.operation.column = swappedOrdinal(@intCast(instruction.operation.column), left.columns.len, right.columns.len);
        };
        var remapped = condition;
        remapped.instructions = owned;
        selected.join.condition = remapped;
    }
    try selected.validate(now_ms);
    return selected;
}
fn swappedOrdinal(ordinal: u16, left_width: usize, right_width: usize) u16 {
    return @intCast(if (ordinal < left_width) right_width + ordinal else ordinal - left_width);
}
fn orientationCost(p: Plan) u128 {
    // Source scans are common. Broadcast transports the build per probe split;
    // shuffle writes and reads both inputs. Ties prefer smaller build residency.
    return switch (p.strategy) {
        .bucket => 0,
        .broadcast => @as(u128, estimatedRetained(p.right)) * p.left.splits,
        .shuffle => 2 * (@as(u128, estimatedRetained(p.left)) + estimatedRetained(p.right)),
    };
}

pub fn choose(left: Input, right: Input, tenant: Digest, expires_ms: u64, join: Join, limits: Limits, now_ms: u64) !Plan {
    if (left.bucket) |l| if (right.bucket) |r| {
        const aligned = for (join.left_keys, join.right_keys) |lk, rk| {
            if (lk == l.column and rk == r.column) break true;
        } else false;
        if (aligned and l.count > 0 and r.count > 0) {
            const gcd = std.math.gcd(l.count, r.count);
            var groups: u32 = @min(gcd, limits.max_partitions);
            while (groups > 0) : (groups -= 1) {
                if (gcd % groups != 0) continue;
                if (estimatedRetained(right) / groups > limits.task_memory / 8 or (left.estimated_rows +| right.estimated_rows) / groups > limits.task_scan_rows / 2) break;
                const result: Plan = .{ .tenant = tenant, .expires_ms = expires_ms, .left = left, .right = right, .join = join, .strategy = .bucket, .partitions = @intCast(groups), .limits = limits };
                if (result.validate(now_ms)) |_| return result else |err| {
                    if (err != error.SqlJoinBudgetExceeded) return err;
                }
            }
        }
    };
    const broadcast = !join.preservesBuild() and estimatedRetained(right) <= @min(64 * 1024 * 1024, limits.task_memory / 8) and right.estimated_rows <= limits.task_scan_rows / 2;
    var partitions: u16 = 1;
    if (!broadcast) while (((estimatedRetained(left) +| estimatedRetained(right)) / partitions > limits.task_memory / 8 or (left.estimated_rows +| right.estimated_rows) / partitions > limits.task_scan_rows / 2) and partitions < limits.max_partitions) {
        if (partitions > limits.max_partitions / 2) break;
        partitions = std.math.mul(u16, partitions, 2) catch return error.SqlJoinBudgetExceeded;
    };
    const result: Plan = .{ .tenant = tenant, .expires_ms = expires_ms, .left = left, .right = right, .join = join, .strategy = if (broadcast) .broadcast else .shuffle, .partitions = partitions, .limits = limits };
    try result.validate(now_ms);
    return result;
}

test "distributed SQL plan preserves join semantics and admits retry and exchange costs" {
    const column: Column = .{ .name = "id", .type = .integer };
    const input: Input = .{ .table_id = 1, .schema = @splat('a'), .snapshot = "1", .wal_cut = 7, .cut = "authorized-retained-input", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 10, .splits = 1 };
    const join: Join = .{ .left_keys = &.{0}, .right_keys = &.{0} };
    var selected = try choose(input, input, @splat('b'), 10000, join, .{}, 1);
    try std.testing.expectEqual(Strategy.broadcast, selected.strategy);
    selected.join.kind = .left;
    try std.testing.expect(!selected.join.mayFilterProbe());
    selected.join.kind = .full;
    try std.testing.expectError(error.SqlDistributedJoinUnsupported, selected.validate(1));
    selected.strategy = .shuffle;
    try selected.validate(1);
    var larger = input;
    larger.estimated_bytes = 256 * 1024 * 1024;
    var capped = selected.limits;
    capped.max_partitions = 3;
    const bounded = try choose(input, larger, @splat('b'), 10000, selected.join, capped, 1);
    try std.testing.expectEqual(@as(u16, 2), bounded.partitions);
    var compressed = input;
    compressed.estimated_rows = 1_000_000;
    const resident = try choose(input, compressed, @splat('b'), 10000, join, .{}, 1);
    try std.testing.expectEqual(Strategy.shuffle, resident.strategy);
    try std.testing.expect(estimatedRetained(compressed) / resident.partitions <= resident.limits.task_memory / 8);
    try std.testing.expectError(error.InvalidSqlExecutionState, selected.withPhysical(&.{}, &.{.{ .partition = 0, .left = &.{.{}}, .right = &.{.{}} }}));
    selected.limits.query_exchange_bytes = 1;
    try std.testing.expectError(error.SqlJoinBudgetExceeded, selected.validate(1));
}

test "distributed SQL physical orientation selects small builds and remaps logical ordinals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const small: Input = .{ .table_id = 1, .schema = @splat('a'), .snapshot = "1", .wal_cut = 0, .cut = "cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 1024, .estimated_rows = 10, .splits = 1 };
    var big = small;
    big.table_id = 2;
    big.columns = &.{ .{ .name = "id", .type = .integer }, .{ .name = "extra", .type = .integer } };
    big.estimated_bytes = 64 * 1024 * 1024;
    big.estimated_rows = 1_000_000;
    big.splits = 64;
    const condition: @import("expression.zig").Program = .{ .instructions = &.{
        .{ .type = .{ .kind = .integer }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .integer }, .operation = .{ .column = 2 } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .eq, .left = 0, .right = 1 } } },
    }, .root = 2, .output = .{ .kind = .boolean } };
    const join: Join = .{ .left_keys = &.{0}, .right_keys = &.{0}, .condition = condition };
    const Ownership = struct {
        fn check(allocator: A, l: Input, r: Input, j: Join) !void {
            const result = try choosePhysical(allocator, l, r, @splat('b'), 10000, j, .{ .task_output_bytes = 4096 }, 1);
            defer allocator.free(result.join.projection);
            defer allocator.free(result.join.condition.?.instructions);
            try std.testing.expectEqual(@as(u64, 1), result.right.table_id);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Ownership.check, .{ small, big, join });
    const chosen = try choosePhysical(a, small, big, @splat('b'), 10000, join, .{ .task_output_bytes = 4096 }, 1);
    try std.testing.expectEqual(Strategy.broadcast, chosen.strategy);
    try std.testing.expectEqual(@as(u32, 65), try chosen.taskCount());
    const constrained = try choosePhysical(a, small, big, @splat('b'), 10000, join, .{ .task_output_bytes = 4096, .max_tasks = 100 }, 1);
    try std.testing.expectEqual(Strategy.broadcast, constrained.strategy);
    const equivalent = try choosePhysical(a, big, small, @splat('b'), 10000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .task_output_bytes = 4096 }, 1);
    try std.testing.expectEqual(chosen.strategy, equivalent.strategy);
    try std.testing.expectEqual(try chosen.taskCount(), try equivalent.taskCount());
    try std.testing.expectEqual(@as(u64, 1), chosen.right.table_id);
    try std.testing.expectEqualSlices(u16, &.{ 2, 0, 1 }, chosen.join.projection);
    try std.testing.expectEqual(@as(u32, 2), chosen.join.condition.?.instructions[0].operation.column);
    try std.testing.expectEqual(@as(u32, 1), chosen.join.condition.?.instructions[1].operation.column);
    try std.testing.expectEqual(@as(u32, 0), condition.instructions[0].operation.column);
    var right_join = join;
    right_join.kind = .right;
    right_join.projection = &.{ 2, 0, 2 };
    const outer = try choosePhysical(a, small, big, @splat('b'), 10000, right_join, .{ .task_output_bytes = 4096 }, 1);
    try std.testing.expectEqual(Strategy.broadcast, outer.strategy);
    try std.testing.expectEqual(@as(@FieldType(Join, "kind"), .left), outer.join.kind);
    try std.testing.expectEqualSlices(u16, &.{ 1, 2, 1 }, outer.join.projection);
    var semi = join;
    semi.kind = .semi;
    const existence = try choosePhysical(a, small, big, @splat('b'), 10000, semi, .{ .task_output_bytes = 4096 }, 1);
    try std.testing.expectEqual(@as(u64, 1), existence.left.table_id);
    try std.testing.expectEqual(Strategy.shuffle, existence.strategy);
}
