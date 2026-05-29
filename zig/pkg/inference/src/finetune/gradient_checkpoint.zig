// Copyright 2026 Antfly, Inc.
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

// Minimal architecture-agnostic gradient checkpointing interface.
// Trainers register checkpoint boundaries, record per-segment inputs at
// forward time, and invoke a user-supplied recompute callback during
// backward to reconstruct segment outputs without storing activations.

const std = @import("std");

pub const SegmentToken = struct {
    start_layer: u32,
    end_layer: u32,
    input: []const f32,
    total_tokens: usize,
    hidden_size: usize,
    user_data: ?*anyopaque = null,
};

pub const RecomputeFn = *const fn (
    ctx: *anyopaque,
    segment: SegmentToken,
    out_hidden: []f32,
) anyerror!void;

pub const CheckpointPolicy = struct {
    every_n_layers: u32 = 0,
    explicit_boundaries: []const u32 = &.{},
};

pub fn planBoundaries(
    allocator: std.mem.Allocator,
    num_layers: u32,
    policy: CheckpointPolicy,
) ![]u32 {
    if (policy.explicit_boundaries.len > 0) {
        const out = try allocator.alloc(u32, policy.explicit_boundaries.len);
        @memcpy(out, policy.explicit_boundaries);
        return out;
    }
    if (policy.every_n_layers == 0 or num_layers == 0) {
        return try allocator.alloc(u32, 0);
    }
    const step = policy.every_n_layers;
    const count: usize = (@as(usize, num_layers) + step - 1) / step;
    const out = try allocator.alloc(u32, count);
    var i: usize = 0;
    var layer: u32 = 0;
    while (layer < num_layers) : (layer += step) {
        out[i] = layer;
        i += 1;
    }
    return out;
}

fn countBoundaries(num_layers: u32, policy: CheckpointPolicy) u32 {
    if (policy.explicit_boundaries.len > 0) {
        return @intCast(policy.explicit_boundaries.len);
    }
    if (policy.every_n_layers == 0 or num_layers == 0) return 0;
    const step = policy.every_n_layers;
    return (num_layers + step - 1) / step;
}

pub const ActivationMemoryEstimate = struct {
    full_bytes: u64,
    checkpointed_bytes: u64,
    recompute_fraction: f32,
};

pub fn estimateActivationMemory(
    num_layers: u32,
    hidden_size: u32,
    batch_size: u32,
    seq_len: u32,
    bytes_per_element: u32,
    policy: CheckpointPolicy,
) ActivationMemoryEstimate {
    const per_layer: u64 =
        @as(u64, batch_size) *
        @as(u64, seq_len) *
        @as(u64, hidden_size) *
        @as(u64, bytes_per_element);
    const full_bytes: u64 = @as(u64, num_layers) * per_layer;

    const no_checkpoint =
        policy.every_n_layers == 0 and policy.explicit_boundaries.len == 0;
    if (no_checkpoint) {
        return .{
            .full_bytes = full_bytes,
            .checkpointed_bytes = full_bytes,
            .recompute_fraction = 0.0,
        };
    }

    const num_boundaries = countBoundaries(num_layers, policy);
    const checkpointed_bytes: u64 = @as(u64, num_boundaries) * per_layer;

    const recompute_fraction: f32 = if (num_layers == 0)
        0.0
    else blk: {
        const layers_f: f32 = @floatFromInt(num_layers);
        const boundaries_f: f32 = @floatFromInt(num_boundaries);
        const diff = layers_f - boundaries_f;
        break :blk diff / layers_f;
    };

    return .{
        .full_bytes = full_bytes,
        .checkpointed_bytes = checkpointed_bytes,
        .recompute_fraction = recompute_fraction,
    };
}

pub const CheckpointHarness = struct {
    allocator: std.mem.Allocator,
    segments: std.ArrayList(SegmentToken),
    recompute_ctx: *anyopaque,
    recompute_fn: RecomputeFn,

    pub fn init(
        allocator: std.mem.Allocator,
        recompute_ctx: *anyopaque,
        recompute_fn: RecomputeFn,
    ) CheckpointHarness {
        return .{
            .allocator = allocator,
            .segments = .empty,
            .recompute_ctx = recompute_ctx,
            .recompute_fn = recompute_fn,
        };
    }

    pub fn deinit(self: *CheckpointHarness) void {
        self.segments.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn recordSegment(
        self: *CheckpointHarness,
        start_layer: u32,
        end_layer: u32,
        input: []const f32,
        total_tokens: usize,
        hidden_size: usize,
        user_data: ?*anyopaque,
    ) !u32 {
        const idx: u32 = @intCast(self.segments.items.len);
        try self.segments.append(self.allocator, .{
            .start_layer = start_layer,
            .end_layer = end_layer,
            .input = input,
            .total_tokens = total_tokens,
            .hidden_size = hidden_size,
            .user_data = user_data,
        });
        return idx;
    }

    pub fn recomputeSegment(
        self: *const CheckpointHarness,
        segment_idx: u32,
        out_hidden: []f32,
    ) !void {
        if (segment_idx >= self.segments.items.len) return error.InvalidSegmentIndex;
        const segment = self.segments.items[segment_idx];
        try self.recompute_fn(self.recompute_ctx, segment, out_hidden);
    }

    pub fn reset(self: *CheckpointHarness) void {
        self.segments.clearRetainingCapacity();
    }
};

// ---------------- tests ----------------

const testing = std.testing;

test "planBoundaries every_n_layers=4 across 24 layers" {
    const alloc = testing.allocator;
    const boundaries = try planBoundaries(alloc, 24, .{ .every_n_layers = 4 });
    defer alloc.free(boundaries);
    const expected = [_]u32{ 0, 4, 8, 12, 16, 20 };
    try testing.expectEqual(expected.len, boundaries.len);
    for (expected, 0..) |v, i| try testing.expectEqual(v, boundaries[i]);
}

test "planBoundaries explicit boundaries are duped" {
    const alloc = testing.allocator;
    const explicit = [_]u32{ 0, 10, 20 };
    const boundaries = try planBoundaries(alloc, 24, .{
        .explicit_boundaries = &explicit,
    });
    defer alloc.free(boundaries);
    try testing.expectEqual(@as(usize, 3), boundaries.len);
    try testing.expectEqual(@as(u32, 0), boundaries[0]);
    try testing.expectEqual(@as(u32, 10), boundaries[1]);
    try testing.expectEqual(@as(u32, 20), boundaries[2]);
    // Ensure it's a copy: mutating the result must not affect the source.
    boundaries[0] = 99;
    try testing.expectEqual(@as(u32, 0), explicit[0]);
}

test "planBoundaries disabled returns empty" {
    const alloc = testing.allocator;
    const boundaries = try planBoundaries(alloc, 24, .{});
    defer alloc.free(boundaries);
    try testing.expectEqual(@as(usize, 0), boundaries.len);
}

test "estimateActivationMemory 24x768 seq512 batch8 every4" {
    const est = estimateActivationMemory(24, 768, 8, 512, 4, .{ .every_n_layers = 4 });
    const per_layer: u64 = 8 * 512 * 768 * 4;
    try testing.expectEqual(@as(u64, 24) * per_layer, est.full_bytes);
    try testing.expectEqual(@as(u64, 6) * per_layer, est.checkpointed_bytes);
    // 24 layers, 6 boundaries -> (24 - 6) / 24 = 0.75
    try testing.expectApproxEqAbs(@as(f32, 0.75), est.recompute_fraction, 1e-6);
}

test "estimateActivationMemory no checkpointing matches full" {
    const est = estimateActivationMemory(12, 512, 4, 256, 4, .{});
    const per_layer: u64 = 4 * 256 * 512 * 4;
    try testing.expectEqual(@as(u64, 12) * per_layer, est.full_bytes);
    try testing.expectEqual(est.full_bytes, est.checkpointed_bytes);
    try testing.expectEqual(@as(f32, 0.0), est.recompute_fraction);
}

const MockCtx = struct {
    last_start: u32 = 0,
    last_end: u32 = 0,
    last_input_ptr: ?[*]const f32 = null,
    last_out_len: usize = 0,
    call_count: u32 = 0,
};

fn mockRecompute(
    ctx: *anyopaque,
    segment: SegmentToken,
    out_hidden: []f32,
) anyerror!void {
    const m: *MockCtx = @ptrCast(@alignCast(ctx));
    m.last_start = segment.start_layer;
    m.last_end = segment.end_layer;
    m.last_input_ptr = segment.input.ptr;
    m.last_out_len = out_hidden.len;
    m.call_count += 1;
    // Trivial "forward": copy input to output.
    const n = @min(segment.input.len, out_hidden.len);
    @memcpy(out_hidden[0..n], segment.input[0..n]);
}

test "CheckpointHarness records and recomputes segment by index" {
    const alloc = testing.allocator;
    var mock = MockCtx{};
    var harness = CheckpointHarness.init(alloc, @ptrCast(&mock), mockRecompute);
    defer harness.deinit();

    const in0 = [_]f32{ 1, 2, 3, 4 };
    const in1 = [_]f32{ 5, 6, 7, 8 };
    const in2 = [_]f32{ 9, 10, 11, 12 };

    const idx0 = try harness.recordSegment(0, 4, &in0, 1, 4, null);
    const idx1 = try harness.recordSegment(4, 8, &in1, 1, 4, null);
    const idx2 = try harness.recordSegment(8, 12, &in2, 1, 4, null);
    try testing.expectEqual(@as(u32, 0), idx0);
    try testing.expectEqual(@as(u32, 1), idx1);
    try testing.expectEqual(@as(u32, 2), idx2);

    var out: [4]f32 = undefined;
    try harness.recomputeSegment(1, &out);

    try testing.expectEqual(@as(u32, 1), mock.call_count);
    try testing.expectEqual(@as(u32, 4), mock.last_start);
    try testing.expectEqual(@as(u32, 8), mock.last_end);
    try testing.expectEqual(@as(usize, 4), mock.last_out_len);
    try testing.expectEqual(@as(?[*]const f32, in1[0..].ptr), mock.last_input_ptr);
    try testing.expectEqual(@as(f32, 5), out[0]);
    try testing.expectEqual(@as(f32, 8), out[3]);
}

test "CheckpointHarness reset restarts indexing at zero" {
    const alloc = testing.allocator;
    var mock = MockCtx{};
    var harness = CheckpointHarness.init(alloc, @ptrCast(&mock), mockRecompute);
    defer harness.deinit();

    const in0 = [_]f32{ 1, 2 };
    _ = try harness.recordSegment(0, 2, &in0, 1, 2, null);
    _ = try harness.recordSegment(2, 4, &in0, 1, 2, null);
    try testing.expectEqual(@as(usize, 2), harness.segments.items.len);

    harness.reset();
    try testing.expectEqual(@as(usize, 0), harness.segments.items.len);

    const in1 = [_]f32{ 3, 4 };
    const idx = try harness.recordSegment(0, 2, &in1, 1, 2, null);
    try testing.expectEqual(@as(u32, 0), idx);

    // After reset, recomputing out-of-range should fail.
    var out: [2]f32 = undefined;
    try testing.expectError(error.InvalidSegmentIndex, harness.recomputeSegment(5, &out));
}
