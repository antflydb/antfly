// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Test-driver contracts for continuously active, persistent native KV owners.
//! A short diagnostic and a formal four-hour observation share raw records;
//! duration and resource authority are independently checked by the collector.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const allocations = @import("host_allocation_tracker.zig");

pub const contexts = [_]usize{ 8192, 32768 };
pub const steps_per_batch = 64;
pub const formal_seconds = 4 * 60 * 60;

pub const Request = struct {
    encoding: checkpoint.Encoding,
    duration_seconds: u32 = formal_seconds,

    pub fn validate(self: Request) !void {
        if (self.encoding == .f32 or self.duration_seconds == 0 or self.duration_seconds > formal_seconds)
            return error.InvalidNativeSoakRequest;
    }
};

pub const Activity = struct {
    owner_id: u64,
    revision: u64,
    logical_tokens: u64,
    attention_calls: usize,
    donor_appends: usize,
    live_bytes: u64,
    prepared_layer_blocks: u64,
};

pub const Batch = struct {
    index: usize,
    steps: [2]usize,
    start: [2]Activity,
    finish: [2]Activity,
    active_ns: u64,
    elapsed_ns: u64,
    output_tokens: [2]usize,

    pub fn validate(self: Batch, layers: usize, donors: usize) !void {
        if (layers == 0 or donors == 0 or self.steps[0] != steps_per_batch / 2 or
            self.steps[1] != steps_per_batch / 2 or self.active_ns == 0 or self.elapsed_ns < self.active_ns or
            self.start[0].owner_id == self.start[1].owner_id)
            return error.InvalidNativeSoakProgress;
        for (self.start, self.finish, self.steps, contexts) |before, after, count, context| {
            if (before.owner_id == 0 or before.owner_id != after.owner_id or before.live_bytes == 0 or
                before.live_bytes != after.live_bytes or
                (before.logical_tokens != context - 256 and !(context == 8192 and before.logical_tokens == context - 64)) or
                after.logical_tokens != before.logical_tokens or after.revision <= before.revision or
                after.attention_calls != before.attention_calls + count * layers or
                after.donor_appends != before.donor_appends + count * donors or
                after.prepared_layer_blocks != before.prepared_layer_blocks + count * layers)
                return error.InvalidNativeSoakProgress;
        }
    }
};

pub const Report = struct {
    collection_complete: bool = false,
    production_ready: bool = false,
    release_authority: bool = false,
    diagnostic_only: bool,
    load_generation: u64,
    batches: usize,
    forward_steps: usize,
    active_ns: u64,
    window_elapsed_ns: u64,
    journal_sha256: [32]u8,
    output_tokens: [2]usize,
    initial: [2]Activity,
    final: [2]Activity,
    prefix_sha256_before: [2][32]u8,
    prefix_sha256_after: [2][32]u8,
    runtime_kv_bytes_before: u64,
    runtime_kv_bytes_after: u64,
    release: [2]allocations.RequestAllocationOwner.ReleaseReceipt,
};

test "attention_match Metal persistent soak requires real activity on both unchanged owners" {
    try (Request{ .encoding = .f16 }).validate();
    try (Request{ .encoding = .rotated4, .duration_seconds = 1 }).validate();
    try std.testing.expectError(error.InvalidNativeSoakRequest, (Request{ .encoding = .f32 }).validate());
    try std.testing.expectError(error.InvalidNativeSoakRequest, (Request{ .encoding = .fp8, .duration_seconds = 0 }).validate());
    const first: [2]Activity = .{
        .{ .owner_id = 1, .revision = 1, .logical_tokens = 7936, .attention_calls = 100, .donor_appends = 50, .live_bytes = 4096, .prepared_layer_blocks = 100 },
        .{ .owner_id = 2, .revision = 1, .logical_tokens = 32512, .attention_calls = 100, .donor_appends = 50, .live_bytes = 8192, .prepared_layer_blocks = 100 },
    };
    var last = first;
    for (&last) |*value| {
        value.revision += 64;
        value.attention_calls += 32 * 35;
        value.donor_appends += 32 * 15;
        value.prepared_layer_blocks += 32 * 35;
    }
    const batch: Batch = .{ .index = 0, .steps = .{ 32, 32 }, .start = first, .finish = last, .active_ns = 900, .elapsed_ns = 1000, .output_tokens = .{ 12, 13 } };
    try batch.validate(35, 15);
    var changed = batch;
    changed.finish[1].owner_id = 3;
    try std.testing.expectError(error.InvalidNativeSoakProgress, changed.validate(35, 15));
    changed = batch;
    changed.finish[0].attention_calls -= 1;
    try std.testing.expectError(error.InvalidNativeSoakProgress, changed.validate(35, 15));
    changed = batch;
    changed.finish[0].live_bytes += 4096;
    try std.testing.expectError(error.InvalidNativeSoakProgress, changed.validate(35, 15));
}
