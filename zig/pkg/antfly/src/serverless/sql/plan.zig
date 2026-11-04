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
pub const Digest = @import("state.zig").Digest;
pub const Column = local.sql_dynamic_filter.Column;
pub const Input = struct {
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
};
pub const Aggregation = struct {
    groups: []const u16 = &.{},
    inputs: []const ?u16,
    specs: []const local.sql_operators.AggregateSpec,
};
pub const Strategy = enum { broadcast, shuffle };
pub const Plan = struct {
    version: u16 = 1,
    tenant: Digest,
    expires_ms: u64,
    left: Input,
    right: Input,
    join: Join,
    strategy: Strategy,
    partitions: u16,
    limits: Limits,
    aggregate: ?Aggregation = null,
    pub fn validate(self: Plan, now_ms: u64) !void {
        if (self.version != 1 or self.expires_ms <= now_ms) return error.SqlQueryExpired;
        const limits = self.limits;
        if (limits.task_memory < 1024 * 1024 or limits.task_memory > 1024 * 1024 * 1024 or limits.task_scan_rows == 0 or limits.task_output_bytes < 4096 or limits.task_output_bytes > @import("exchange.zig").max_object_bytes or limits.max_tasks == 0 or limits.max_tasks > 65536 or limits.max_partitions == 0 or limits.max_partitions > 1024 or limits.concurrency == 0 or limits.concurrency > 256 or limits.max_attempts == 0 or limits.max_attempts > 16) return error.InvalidSqlDistributedPlan;
        if (self.partitions == 0 or self.partitions > limits.max_partitions or !std.math.isPowerOfTwo(self.partitions)) return error.InvalidSqlDistributedPlan;
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
        const requests = if (self.strategy == .broadcast) @as(u64, self.left.splits) *| 4 +| @as(u64, self.right.splits) *| 3 else (@as(u64, self.left.splits) +| self.right.splits) *| (@as(u64, self.partitions) *| 3 +| 4);
        if (requests *| limits.max_attempts > limits.query_requests) return error.SqlJoinBudgetExceeded;
    }
    pub fn stageTasks(self: Plan, stage: u32) !u32 {
        if (stage == 0) return if (self.strategy == .broadcast) self.right.splits else std.math.add(u32, self.left.splits, self.right.splits);
        var count: u32 = if (self.strategy == .broadcast) self.left.splits else self.partitions;
        if (stage == 1) return count;
        if (self.aggregate == null or stage > 8) return error.InvalidSqlDistributedPlan;
        for (2..stage + 1) |_| count = (count + 31) / 32;
        return count;
    }
    pub fn finalStage(self: Plan) u32 {
        if (self.aggregate == null) return 2;
        var count: u32 = if (self.strategy == .broadcast) self.left.splits else self.partitions;
        var stage: u32 = 2;
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
    kind: enum { scan_build, broadcast_probe, map_left, map_right, reduce, aggregate, results },
    split: u32 = 0,
    partition: u16 = 0,
    finalize: bool = false,
};
/// No row payload travels through the dispatcher. The worker loads the durable
/// plan and accepted dependency records using these deterministic identities.
pub const Ticket = struct { tenant: Digest, query: Digest, stage: u32, task: u32 };
pub fn choose(left: Input, right: Input, tenant: Digest, expires_ms: u64, join: Join, limits: Limits, now_ms: u64) !Plan {
    const broadcast = !join.preservesBuild() and right.estimated_bytes <= @min(64 * 1024 * 1024, limits.task_memory / 4);
    var partitions: u16 = 1;
    if (!broadcast) while (@as(u64, partitions) *| (limits.task_memory / 2) < right.estimated_bytes and partitions < limits.max_partitions) {
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
    selected.limits.query_exchange_bytes = 1;
    try std.testing.expectError(error.SqlJoinBudgetExceeded, selected.validate(1));
}
