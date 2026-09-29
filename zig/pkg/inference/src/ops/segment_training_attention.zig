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

//! CPU flash-style segment training attention: the graph-facing wrapper
//! (control decoding, admission) around `inference_linalg`'s
//! `segmentTrainingAttentionForwardHost`/`BackwardHost`. Those already tile
//! Q/K/V and never materialize a `[tokens, tokens]` tensor; this module only
//! decodes the physical i32 control leaf (replay limbs, logical positions,
//! `laya_tree`-style ranges) and admits the resulting work before calling in.
//!
//! Not an inference fallback: this implements the dedicated v1 graph
//! contract (`fused_segment_training_attention_v1`/`_backward_v1`).
//! Graph/backend integration is separate (`native_compute.zig`).
const std = @import("std");
const linalg = @import("inference_linalg");
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const Attrs = @import("ml").graph.SegmentTrainingAttentionAttrs;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const gib = 1024 * mib;

pub const Limits = struct {
    max_tensor_bytes: usize = gib,
    /// Bounds forward+backward score-tile work (proportional to the keys
    /// each query can see, not `seq_len^2`), not FLOPs or allocation bytes.
    max_work_items: u64 = 4 * 1024 * 1024 * 1024,
};

pub const Options = struct {
    limits: Limits = .{},
    control: ?ExecutionControl = null,
    io: ?std.Io = null,

    fn check(self: Options) !void {
        if (self.control) |control| try control.check();
        if (self.io) |io| try io.checkCancel();
    }
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidSegmentTrainingAttentionShape;
}

/// Admits shape and a work bound proportional to `batch*heads*seq_len*avg
/// visible keys`; since visibility is data-dependent (ranges live in the
/// control leaf), this conservatively assumes every query may see every key
/// in its row unless `window` bounds it below `seq_len`.
pub fn plan(attrs: Attrs, limits: Limits) !void {
    const layout = try attrs.layout();
    if (limits.max_tensor_bytes == 0 or limits.max_tensor_bytes > gib * 8 or limits.max_work_items == 0 or limits.max_work_items > 1 << 42)
        return error.InvalidSegmentTrainingAttentionLimit;
    const batch_tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidSegmentTrainingAttentionShape;
    const hidden = std.math.cast(usize, layout.hidden) orelse return error.InvalidSegmentTrainingAttentionShape;
    const qkv_elements = try mul(3, try mul(batch_tokens, hidden));
    const control_elements = std.math.cast(usize, layout.control_elements) orelse return error.InvalidSegmentTrainingAttentionShape;
    for ([_]usize{ try mul(qkv_elements, 4), try mul(control_elements, 4) }) |bytes|
        if (bytes > limits.max_tensor_bytes) return error.SegmentTrainingAttentionTensorLimitExceeded;
    const seq_len: u64 = attrs.seq_len;
    const visible_keys = @min(seq_len, @as(u64, attrs.window) *| 2 +| 1);
    const forward_work = std.math.mul(u64, try std.math.mul(u64, attrs.batch, attrs.num_heads), try std.math.mul(u64, seq_len, visible_keys)) catch return error.InvalidSegmentTrainingAttentionShape;
    // Backward makes one recompute-forward pass plus one gradient pass.
    const total_work = std.math.mul(u64, forward_work, 3) catch return error.InvalidSegmentTrainingAttentionShape;
    if (total_work > limits.max_work_items) return error.SegmentTrainingAttentionWorkLimitExceeded;
}

pub const Replay = struct { seed: u64, micro_batch: u64, replica: u64 };

fn decodeU64(words: []align(1) const i32) u64 {
    return @as(u64, @as(u32, @bitCast(words[0]))) | (@as(u64, @as(u32, @bitCast(words[1]))) << 32);
}

fn mix(value: u64) u64 {
    var x = value +% 0x9e3779b97f4a7c15;
    x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
    x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
    return x ^ (x >> 31);
}

/// Combines the replay limbs and the op's dropout stream id into the single
/// seed `linalg.segmentAttentionDropoutKeep` mixes with `(batch, head,
/// query, key)`. The same seed on forward and backward reproduces the same
/// mask without persisting one.
pub fn dropoutSeed(attrs: Attrs, replay: Replay) u64 {
    return mix(replay.seed) ^ mix(replay.micro_batch) ^ mix(replay.replica +% 0x7265706c696361) ^ mix(attrs.dropout_stream_id);
}

pub const ControlView = struct {
    replay: Replay,
    /// Whether dropout applies at all this call, independent of the graph-time
    /// `attrs.dropout_probability`: the trainer's `predict`/eval path binds
    /// this to `0` (see `finetune/laya/training.zig`'s `inputs`, `training`
    /// param) so a program built once with nonzero head dropout still turns
    /// it off outside actual training steps -- the fused op has no separate
    /// runtime mask tensor the way the dense path's `drop()` does, so this
    /// flag is the only way eval can silence it without rebuilding the graph.
    apply_dropout: bool,
    /// `[batch*seq_len]`, borrowed from the physical control leaf.
    positions: []align(1) const i32,
    /// `[batch*seq_len*6]`, borrowed and range-checked against `seq_len`.
    /// Physical control is i32 throughout; values are non-negative here.
    ranges: []align(1) const i32,
};

/// Allocation-free, strict decoder shared by the CPU op and Metal's
/// host-bridged execution. Returned slices borrow the immutable physical
/// i32 input; `ranges` values are checked to be `<= seq_len` and each
/// `start, end` pair ordered (`laya_tree`'s half-open convention, empty
/// when equal).
pub fn validateControl(attrs: Attrs, words: []align(1) const i32, options: Options) !ControlView {
    const layout = try attrs.layout();
    const count = std.math.cast(usize, layout.control_elements) orelse return error.InvalidSegmentTrainingAttentionControl;
    const tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidSegmentTrainingAttentionControl;
    if (words.len != count) return error.InvalidSegmentTrainingAttentionControl;
    const positions = words[7..][0..tokens];
    const range_words = words[7 + tokens ..];
    if (range_words.len != tokens * 6) return error.InvalidSegmentTrainingAttentionControl;
    for (range_words, 0..) |bound, i| {
        if (i % 4096 == 0) try options.check();
        if (bound < 0 or bound > attrs.seq_len) return error.InvalidSegmentTrainingAttentionControl;
        // Each `start, end` pair must be ordered (empty when equal).
        if (i % 2 == 1 and range_words[i - 1] > bound) return error.InvalidSegmentTrainingAttentionControl;
    }
    const replay = Replay{ .seed = decodeU64(words[0..2]), .micro_batch = decodeU64(words[2..4]), .replica = decodeU64(words[4..6]) };
    return .{ .replay = replay, .apply_dropout = words[6] != 0, .positions = positions, .ranges = range_words };
}

/// `inference_linalg`'s kernels take naturally aligned `[]const i32`/`[]const
/// u32`, but the physical control leaf is `align(1)` (it may be a raw
/// device-mapped or unaligned host view, as in `deberta_training_attention`).
/// Copies the two views out; O(batch*seq_len), negligible next to Q/K/V.
const HostControl = struct {
    positions: []i32,
    ranges: []u32,

    fn init(a: Allocator, view: ControlView) !HostControl {
        const positions = try a.alloc(i32, view.positions.len);
        errdefer a.free(positions);
        for (positions, view.positions) |*dst, src| dst.* = src;
        const ranges = try a.alloc(u32, view.ranges.len);
        errdefer a.free(ranges);
        for (ranges, view.ranges) |*dst, src| dst.* = @intCast(src);
        return .{ .positions = positions, .ranges = ranges };
    }
    fn deinit(self: *HostControl, a: Allocator) void {
        a.free(self.positions);
        a.free(self.ranges);
    }
};

pub fn forward(a: Allocator, attrs: Attrs, qkv: []const f32, control: []align(1) const i32, options: Options) ![]f32 {
    try plan(attrs, options.limits);
    const layout = try attrs.layout();
    const view = try validateControl(attrs, control, options);
    var host = try HostControl.init(a, view);
    defer host.deinit(a);
    const batch_tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidSegmentTrainingAttentionShape;
    const hidden = std.math.cast(usize, layout.hidden) orelse return error.InvalidSegmentTrainingAttentionShape;
    if (qkv.len != 3 * batch_tokens * hidden) return error.InvalidSegmentTrainingAttentionShape;
    const q = qkv[0 .. batch_tokens * hidden];
    const k = qkv[batch_tokens * hidden .. 2 * batch_tokens * hidden];
    const v = qkv[2 * batch_tokens * hidden .. 3 * batch_tokens * hidden];
    const result = try linalg.segmentTrainingAttentionForwardHost(
        a,
        q,
        k,
        v,
        host.ranges,
        host.positions,
        attrs.window,
        if (view.apply_dropout) attrs.dropout_probability else 0,
        dropoutSeed(attrs, view.replay),
        attrs.batch,
        attrs.seq_len,
        attrs.num_heads,
        attrs.head_dim,
    );
    a.free(result.row_max);
    a.free(result.row_sum);
    return result.output;
}

/// Packed gradient order is dQ,dK,dV, matching the forward `qkv` order.
pub fn backward(a: Allocator, attrs: Attrs, qkv: []const f32, control: []align(1) const i32, dout: []const f32, options: Options) ![]f32 {
    try plan(attrs, options.limits);
    const layout = try attrs.layout();
    const view = try validateControl(attrs, control, options);
    var host = try HostControl.init(a, view);
    defer host.deinit(a);
    const batch_tokens = std.math.cast(usize, layout.batch_tokens) orelse return error.InvalidSegmentTrainingAttentionShape;
    const hidden = std.math.cast(usize, layout.hidden) orelse return error.InvalidSegmentTrainingAttentionShape;
    if (qkv.len != 3 * batch_tokens * hidden or dout.len != batch_tokens * hidden) return error.InvalidSegmentTrainingAttentionShape;
    const q = qkv[0 .. batch_tokens * hidden];
    const k = qkv[batch_tokens * hidden .. 2 * batch_tokens * hidden];
    const v = qkv[2 * batch_tokens * hidden .. 3 * batch_tokens * hidden];
    var grad = try linalg.segmentTrainingAttentionBackwardHost(
        a,
        q,
        k,
        v,
        dout,
        host.ranges,
        host.positions,
        attrs.window,
        if (view.apply_dropout) attrs.dropout_probability else 0,
        dropoutSeed(attrs, view.replay),
        attrs.batch,
        attrs.seq_len,
        attrs.num_heads,
        attrs.head_dim,
    );
    defer grad.deinit(a);
    const gradient = try a.alloc(f32, 3 * batch_tokens * hidden);
    errdefer a.free(gradient);
    @memcpy(gradient[0 .. batch_tokens * hidden], grad.dq);
    @memcpy(gradient[batch_tokens * hidden .. 2 * batch_tokens * hidden], grad.dk);
    @memcpy(gradient[2 * batch_tokens * hidden .. 3 * batch_tokens * hidden], grad.dv);
    return gradient;
}

test "plan admits an 8k global row within default limits, and enforces the bound above it" {
    const wide = Attrs{ .batch = 1, .seq_len = 8192, .num_heads = 16, .head_dim = 64, .dropout_probability = 0, .dropout_stream_id = 0 };
    try plan(wide, .{});
    var narrow_window = wide;
    narrow_window.window = 64;
    try plan(narrow_window, .{ .max_work_items = 1024 * 1024 * 1024 });
    var larger_batch = wide;
    larger_batch.batch = 8;
    try std.testing.expectError(error.SegmentTrainingAttentionWorkLimitExceeded, plan(larger_batch, .{}));
}

test "validateControl rejects out-of-range bounds and wrong lengths" {
    const attrs = Attrs{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 2, .dropout_probability = 0, .dropout_stream_id = 0 };
    var words = [_]i32{0} ** (7 + 4 + 24);
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionControl, validateControl(attrs, words[0..1], .{}));
    words[7 + 4] = 5; // out of range: > seq_len
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionControl, validateControl(attrs, &words, .{}));
    words[7 + 4] = 3; // start after end
    words[7 + 5] = 1;
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionControl, validateControl(attrs, &words, .{}));
    words[7 + 5] = 3; // empty is fine
    _ = try validateControl(attrs, &words, .{});
}

test "forward and backward reject a qkv or dout of the wrong length" {
    const a = std.testing.allocator;
    const attrs = Attrs{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 2, .dropout_probability = 0, .dropout_stream_id = 0 };
    var words = [_]i32{0} ** (7 + 4 + 24);
    for (0..4) |t| words[7 + 4 + t * 6 + 1] = 4;
    const short = [_]f32{0} ** (3 * 4 * 2 - 1);
    const long = [_]f32{0} ** (3 * 4 * 2 + 2);
    const dout = [_]f32{0} ** (4 * 2);
    for ([_][]const f32{ &short, &long }) |qkv| {
        try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, forward(a, attrs, qkv, &words, .{}));
        try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, backward(a, attrs, qkv, &words, &dout, .{}));
    }
    const exact = [_]f32{0} ** (3 * 4 * 2);
    a.free(try forward(a, attrs, &exact, &words, .{}));
    try std.testing.expectError(error.InvalidSegmentTrainingAttentionShape, backward(a, attrs, &exact, &words, dout[0..7], .{}));
}

test "validateControl decodes apply_dropout from word 6" {
    const attrs = Attrs{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 2, .dropout_probability = 0.1, .dropout_stream_id = 0 };
    var words = [_]i32{0} ** (7 + 4 + 24);
    const off = try validateControl(attrs, &words, .{});
    try std.testing.expect(!off.apply_dropout);
    words[6] = 1;
    const on = try validateControl(attrs, &words, .{});
    try std.testing.expect(on.apply_dropout);
}
