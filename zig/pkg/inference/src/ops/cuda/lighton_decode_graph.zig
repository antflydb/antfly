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

//! Epoch-local owner for a bounded multi-token decoder graph. The caller holds
//! mutex across preflight, real execution, and the final ID read. No borrowed
//! host page or tensor descriptor is retained by this owner.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");
const memory = @import("../../runtime/tier/memory.zig");
const plan_mod = @import("execution_arena.zig");
pub const max_rows = 128;

/// The scheduler may rotate the same cohort between claims. Capture uses a
/// stable sequence order, while publication retains the original claim order.
pub fn canonicalOrder(sequences: []const u64, order: []usize) !void {
    if (sequences.len < 2 or sequences.len > max_rows or order.len != sequences.len) return error.InvalidShape;
    for (sequences, 0..) |sequence, i| {
        if (sequence == 0) return error.InvalidPagedKvBatch;
        order[i] = i;
        var j = i;
        while (j > 0 and sequences[order[j - 1]] > sequence) : (j -= 1) {
            order[j] = order[j - 1];
        }
        if (j > 0 and sequences[order[j - 1]] == sequence) return error.InvalidPagedKvBatch;
        order[j] = i;
    }
}

test "LightOnOCR3 CUDA graph canonical cohorts restore both token columns after scheduler rotation" {
    // The two claims name the same states in different scheduler orders.
    // Canonical decoder rows and each original caller's IDs must agree.
    const claims = [_][4]u64{ .{ 43, 7, 29, 11 }, .{ 11, 43, 7, 29 } };
    for (claims) |sequences| {
        var order: [4]usize = undefined;
        try canonicalOrder(&sequences, &order);
        var canonical: [4]u64 = undefined;
        var published: [2][4]u64 = undefined;
        for (order, 0..) |original, row| {
            canonical[row] = sequences[original];
            published[0][original] = sequences[original] * 10;
            published[1][original] = sequences[original] * 10 + 1;
        }
        try std.testing.expectEqualSlices(u64, &.{ 7, 11, 29, 43 }, &canonical);
        for (sequences, 0..) |sequence, original| {
            try std.testing.expectEqual(sequence * 10, published[0][original]);
            try std.testing.expectEqual(sequence * 10 + 1, published[1][original]);
        }
    }
    var order: [4]usize = undefined;
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 29, 7, 43 }, &order));
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 0, 29, 43 }, &order));
}
pub const max_steps: usize = 8;
// Bank + positions, with two possible 256-byte alignment gaps and progress.
pub const max_metadata_read_words = (max_steps + 3) * max_rows + 128;

/// Graph depth shares the existing bounded continuation policy. Never reserve
/// or execute more tokens than the smallest remaining request allowance.
pub fn boundedSteps(requested: usize, remaining: usize) ?usize {
    if (remaining < 2) return null;
    return @min(std.math.clamp(requested, 2, max_steps), remaining);
}

/// Restore every token column after canonical scheduler-row ordering.
pub fn restoreTokenColumns(tokens: []u32, order: []const usize) !void {
    if (order.len == 0 or order.len > max_rows or tokens.len == 0 or tokens.len % order.len != 0 or tokens.len / order.len > max_steps) return error.InvalidPagedKvBatch;
    var seen = std.StaticBitSet(max_rows).empty;
    for (order) |original| {
        if (original >= order.len or seen.isSet(original)) return error.InvalidPagedKvBatch;
        seen.set(original);
    }
    var restored: [max_rows]u32 = undefined;
    var offset: usize = 0;
    while (offset < tokens.len) : (offset += order.len) {
        for (order, 0..) |original, row| restored[original] = tokens[offset + row];
        @memcpy(tokens[offset..][0..order.len], restored[0..order.len]);
    }
}

pub const MetadataLayout = struct {
    bank_offset: usize,
    positions_offset: usize,
    progress_offset: usize,
    bytes: usize,

    pub fn init(rows: usize, hidden: usize, steps: usize) !MetadataLayout {
        if (rows < 2 or rows > max_rows or (hidden != 1024 and hidden != 2560) or steps < 2 or steps > max_steps) return error.InvalidShape;
        const input_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, hidden), 4);
        const bank_offset = std.mem.alignForward(usize, input_bytes, 256);
        const positions_offset = bank_offset + std.mem.alignForward(usize, rows * steps * 4, 256);
        const progress_offset = std.mem.alignForward(usize, positions_offset + rows * 3 * 4, 256);
        return .{ .bank_offset = bank_offset, .positions_offset = positions_offset, .progress_offset = progress_offset, .bytes = progress_offset + 256 };
    }
};

pub const Phase = enum { warm, learn, capture, replay, disabled };
pub const Module = struct {
    mutex: std.atomic.Mutex = .unlocked,
    epoch_id: u64 = 0,
    disabled_epoch: u64 = 0,
    key: []u64 = &.{},
    plan: ?*plan_mod.Plan = null,
    phase: Phase = .warm,
    metadata: buffer.DeviceBuffer = .{},
    arena: buffer.DeviceBuffer = .{},
    metadata_admission: ?memory.AdmissionLease = null,
    arena_admission: ?memory.AdmissionLease = null,
    graph: driver.CUgraphExec = null,
    rows: usize = 0,
    hidden: usize = 0,
    vocab: usize = 0,
    steps: usize = 2,
    // Wide logits live outside the bounded temporary arena. All dependent
    // steps reuse this independently admitted output after their argmax.
    head: buffer.DeviceBuffer = .{},
    head_admission: ?memory.AdmissionLease = null,
    future_tokens: usize = 0,
    expected_attention_calls: usize = 0,
    expected_ragged_packets: usize = 0,
    bank_offset: usize = 0,
    positions_offset: usize = 0,
    progress_offset: usize = 0,
    base_positions: [3 * max_rows]u32 = @splat(0),
    next_positions: [3 * max_rows]u32 = @splat(0),
    progress: u32 = 0,
    capture_step: u32 = 0,
    active: bool = false,
    captures: u64 = 0,
    replays: u64 = 0,
    wide_replays: u64 = 0,
    max_replay_rows: usize = 0,
    invalidations: u64 = 0,
    admission_denials: u64 = 0,
    weight_declines_logged: usize = 0,
    state_declines_logged: usize = 0,
    resets_logged: usize = 0,

    pub fn clear(self: *Module, ctx: *context.CudaContext) !void {
        // Disable before waiting. A failed drain must never leave a replayable
        // graph whose inputs are about to retire through a page callback.
        self.phase = .disabled;
        self.active = false;
        if (self.head.ptr != 0 or self.metadata.ptr != 0 or self.arena.ptr != 0 or self.graph != null)
            try ctx.synchronize();
        if (self.graph != null) ctx.destroyGraphExec(self.graph);
        self.graph = null;
        self.head.free(ctx);
        self.arena.free(ctx);
        self.metadata.free(ctx);
        if (self.plan) |plan| std.heap.c_allocator.destroy(plan);
        self.plan = null;
        if (self.key.len != 0) std.heap.c_allocator.free(self.key);
        self.key = &.{};
        if (self.head_admission) |*lease| lease.release();
        self.head_admission = null;
        if (self.arena_admission) |*lease| lease.release();
        self.arena_admission = null;
        if (self.metadata_admission) |*lease| lease.release();
        self.metadata_admission = null;
        self.rows = 0;
        self.hidden = 0;
        self.vocab = 0;
        self.progress = 0;
        self.invalidations += 1;
    }
    pub fn invalidate(self: *Module, ctx: *context.CudaContext) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.clear(ctx) catch {};
    }
    pub fn matches(self: *const Module, key: []const u64, host_positions: []const u32) bool {
        return self.key.len != 0 and std.mem.eql(u64, self.key, key) and
            host_positions.len == 3 * self.rows and
            std.mem.eql(u32, self.next_positions[0..host_positions.len], host_positions);
    }
    pub fn reset(self: *Module, ctx: *context.CudaContext, key: []const u64, host_positions: []const u32, rows: usize, hidden: usize, vocab: usize, steps: usize) !void {
        try self.clear(ctx);
        if (rows < 2 or rows > max_rows or host_positions.len != rows * 3 or key.len == 0 or vocab == 0)
            return error.InvalidShape;
        const layout = try MetadataLayout.init(rows, hidden, steps);
        const bytes = layout.bytes;
        var lease: ?memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy|
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                .backend_scratch_bytes = bytes,
                .host_scratch_bytes = @sizeOf(plan_mod.Plan) + key.len * @sizeOf(u64),
            }, policy.check_live_memory);
        errdefer if (lease) |*value| value.release();
        const owned_key = try std.heap.c_allocator.dupe(u64, key);
        errdefer std.heap.c_allocator.free(owned_key);
        const plan = try std.heap.c_allocator.create(plan_mod.Plan);
        errdefer std.heap.c_allocator.destroy(plan);
        plan.* = .{};
        var device = try buffer.DeviceBuffer.alloc(ctx, bytes);
        errdefer device.free(ctx);
        var head_lease: ?memory.AdmissionLease = null;
        errdefer if (head_lease) |*value| value.release();
        var head = buffer.DeviceBuffer{};
        if (rows > 64) {
            const head_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, vocab), 4);
            if (head_bytes > plan_mod.limit) return error.InvalidShape;
            if (ctx.video_cache_budget.admission) |policy|
                head_lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                    .backend_scratch_bytes = head_bytes,
                }, policy.check_live_memory);
            head = try buffer.DeviceBuffer.alloc(ctx, head_bytes);
        }
        self.head = head;
        self.head_admission = head_lease;
        self.key = owned_key;
        self.plan = plan;
        self.metadata = device;
        self.metadata_admission = lease;
        self.rows = rows;
        self.hidden = hidden;
        self.vocab = vocab;
        self.steps = steps;
        self.bank_offset = layout.bank_offset;
        self.positions_offset = layout.positions_offset;
        self.progress_offset = layout.progress_offset;
        @memcpy(self.base_positions[0..host_positions.len], host_positions);
        @memcpy(self.next_positions[0..host_positions.len], host_positions);
        self.phase = .warm;
    }
    pub fn reserveArena(self: *Module, ctx: *context.CudaContext) !void {
        if (self.arena.ptr != 0) return;
        const plan = self.plan orelse return error.InvalidState;
        if (!plan.valid or plan.peak == 0 or plan.peak > plan_mod.limit)
            return error.InvalidExecutionPlan;
        var lease: ?memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy|
            lease = try policy.controller.tryAcquire(.gpu, policy.limits, .{
                .backend_scratch_bytes = plan.peak,
            }, policy.check_live_memory);
        errdefer if (lease) |*value| value.release();
        self.arena = try buffer.DeviceBuffer.alloc(ctx, plan.peak);
        self.arena_admission = lease;
    }
    pub fn input(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr, .len = self.rows * self.hidden * 4, .borrowed = true };
    }
    pub fn bank(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.bank_offset, .len = self.rows * self.steps * 4, .borrowed = true };
    }
    pub fn positions(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.positions_offset, .len = self.rows * 3 * 4, .borrowed = true };
    }
    pub fn counter(self: *const Module) buffer.DeviceBuffer {
        return .{ .ptr = self.metadata.ptr + self.progress_offset, .len = 4, .borrowed = true };
    }
    pub fn noteSteps(self: *Module) !void {
        const next = try std.math.add(u32, self.progress, @intCast(self.steps));
        for (self.next_positions[0 .. self.rows * 3]) |*position|
            position.* = try std.math.add(u32, position.*, @intCast(self.steps));
        self.progress = next;
    }
};

test "LightOn CUDA graph depth bounds every token bank and scheduler column" {
    try std.testing.expect(boundedSteps(4, 1) == null);
    try std.testing.expectEqual(@as(?usize, 3), boundedSteps(4, 3));
    try std.testing.expectEqual(@as(?usize, 8), boundedSteps(100, 100));
    for ([_]usize{ 2, 3, 4, 8 }) |steps| {
        const layout = try MetadataLayout.init(128, 2560, steps);
        try std.testing.expect(layout.bank_offset + 128 * steps * 4 <= layout.positions_offset);
        try std.testing.expect((layout.progress_offset + 4 - layout.bank_offset) / 4 <= max_metadata_read_words);
        try std.testing.expect(layout.positions_offset + 128 * 3 * 4 <= layout.progress_offset);
        try std.testing.expectEqual(@as(usize, 0), layout.bytes % 256);
        var module = Module{ .rows = 4, .steps = steps };
        try module.noteSteps();
        try module.noteSteps();
        try std.testing.expectEqual(@as(u32, @intCast(2 * steps)), module.progress);
        try std.testing.expectEqual(@as(u32, @intCast(2 * steps)), module.next_positions[11]);
        var tokens: [max_steps * 4]u32 = undefined;
        for (0..steps) |step| for (0..4) |row| {
            tokens[step * 4 + row] = @intCast(step * 10 + row);
        };
        try restoreTokenColumns(tokens[0 .. steps * 4], &.{ 2, 0, 3, 1 });
        for (0..steps) |step| try std.testing.expectEqualSlices(u32, &.{ @intCast(step * 10 + 1), @intCast(step * 10 + 3), @intCast(step * 10), @intCast(step * 10 + 2) }, tokens[step * 4 ..][0..4]);
    }
    try std.testing.expectError(error.InvalidShape, MetadataLayout.init(4, 2560, 9));
    var tokens = [_]u32{ 0, 1, 2, 3 };
    try std.testing.expectError(error.InvalidPagedKvBatch, restoreTokenColumns(&tokens, &.{ 0, 1, 1, 3 }));
}
