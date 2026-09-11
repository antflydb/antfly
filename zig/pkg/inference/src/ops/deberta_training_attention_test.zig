// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Independent scalar and finite-difference checks for the replay-tiled CPU
//! primitive. Pinned Python fixtures are a separate numerical qualification.
const std = @import("std");
const attention = @import("deberta_training_attention.zig");
const encoder = @import("../finetune/gliner_boundary_encoder_graph.zig");
const relative = @import("../models/deberta.zig");
const Shape = @import("ml").graph.Shape;
const Allocator = std.mem.Allocator;

const replay = attention.Replay{ .seed = 0xfedcba9876543210, .micro_batch = 0x100000002, .replica = 0x8000000000000003 };

fn storeInteger(output: []i32, value: u64) void {
    output[0] = @bitCast(@as(u32, @truncate(value)));
    output[1] = @bitCast(@as(u32, @truncate(value >> 32)));
}

const Fixture = struct {
    allocator: Allocator,
    attrs: attention.Attrs,
    qkv: []f32,
    relative: []f32,
    control: []i32,
    dout: []f32,
    // Reference dropout comes from the existing materialized encoder binder,
    // independently of the new primitive's counter/control implementation.
    dropout: []f32,

    fn init(a: Allocator, sequence: u32, probability: f32) !Fixture {
        const attrs = attention.Attrs{ .batch = 2, .seq_len = sequence, .num_heads = 2, .head_dim = 3, .relative_rows = 512, .dropout_probability = probability, .dropout_stream_id = (@as(u64, 7) << 32) | 3 };
        const p = try attention.plan(attrs, .{});
        const qkv = try a.alloc(f32, p.qkv_elements);
        errdefer a.free(qkv);
        const r = try a.alloc(f32, p.relative_elements);
        errdefer a.free(r);
        const control = try a.alloc(i32, p.control_elements);
        errdefer a.free(control);
        const dout = try a.alloc(f32, p.output_elements);
        errdefer a.free(dout);
        const dropout = try a.alloc(f32, @intCast(p.forward_work_items));
        errdefer a.free(dropout);
        for (qkv, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.61 + 0.1) * 0.3;
        for (r, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i)) * 0.37 + 0.2) * 0.21;
        for (dout, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.23 + 0.3) * 0.41;
        storeInteger(control[0..2], replay.seed);
        storeInteger(control[2..4], replay.micro_batch);
        storeInteger(control[4..6], replay.replica);
        for (control[6..][0..p.batch_tokens], 0..) |*value, i| value.* = @intFromBool(i < sequence and i % 3 != 1);
        for (control[6 + p.batch_tokens ..], 0..) |*value, i| value.* = @intCast(relative.relativePositionBucket(@as(i64, @intCast(i)) - (@as(i64, sequence) - 1), 256, 512));
        try encoder.fillDropout(.{ .node = 0, .site = .{ .kind = .attention_probabilities, .layer = 7 }, .shape = Shape.init(.f32, &.{ attrs.batch * attrs.num_heads, sequence, sequence }), .probability = probability }, .{ .seed = replay.seed, .micro_batch = replay.micro_batch, .replica = replay.replica }, dropout);
        return .{ .allocator = a, .attrs = attrs, .qkv = qkv, .relative = r, .control = control, .dout = dout, .dropout = dropout };
    }

    fn deinit(self: *Fixture) void {
        self.allocator.free(self.qkv);
        self.allocator.free(self.relative);
        self.allocator.free(self.control);
        self.allocator.free(self.dout);
        self.allocator.free(self.dropout);
        self.* = undefined;
    }
};

const Reference = struct {
    allocator: Allocator,
    output: []f32,
    gradient: []f32,

    fn deinit(self: *Reference) void {
        self.allocator.free(self.output);
        self.allocator.free(self.gradient);
        self.* = undefined;
    }
};

/// Scalar f64 accumulation, no GEMM, tiling, online-softmax helper or new RNG.
/// Both outputs and all five derivatives are computed directly from one
/// query's full probability distribution. The test reference alone uses a
/// materialized dropout mask from the already qualified encoder binder.
fn scalar(a: Allocator, fixture: Fixture) !Reference {
    const attrs = fixture.attrs;
    const batch: usize = attrs.batch;
    const sequence: usize = attrs.seq_len;
    const heads: usize = attrs.num_heads;
    const dim: usize = attrs.head_dim;
    const hidden = heads * dim;
    const elements = batch * sequence * hidden;
    const relative_elements = @as(usize, attrs.relative_rows) * hidden;
    const output = try a.alloc(f32, elements);
    errdefer a.free(output);
    const gradient = try a.alloc(f32, 3 * elements + 2 * relative_elements);
    errdefer a.free(gradient);
    const wide = try a.alloc(f64, gradient.len);
    defer a.free(wide);
    @memset(wide, 0);
    const scores = try a.alloc(f64, sequence);
    defer a.free(scores);
    const probabilities = try a.alloc(f64, sequence);
    defer a.free(probabilities);
    const dp = try a.alloc(f64, sequence);
    defer a.free(dp);
    const scale = @sqrt(@as(f64, @floatFromInt(dim * 3)));
    for (0..batch) |b| {
        for (0..heads) |h| {
            for (0..sequence) |q| {
                const qoff = (b * sequence + q) * hidden + h * dim;
                var maximum: f64 = -std.math.inf(f64);
                for (0..sequence) |k| {
                    const koff = (b * sequence + k) * hidden + h * dim;
                    const bucket: usize = @intCast(fixture.control[6 + batch * sequence + q + sequence - 1 - k]);
                    const roff = bucket * hidden + h * dim;
                    var c2c: f64 = 0;
                    var c2p: f64 = 0;
                    var p2c: f64 = 0;
                    for (0..dim) |d| {
                        c2c += @as(f64, fixture.qkv[qoff + d]) * fixture.qkv[elements + koff + d] / scale;
                        c2p += @as(f64, fixture.qkv[qoff + d]) * fixture.relative[relative_elements + roff + d];
                        p2c += @as(f64, fixture.qkv[elements + koff + d]) * fixture.relative[roff + d];
                    }
                    scores[k] = if (fixture.control[6 + b * sequence + q] != 0 and fixture.control[6 + b * sequence + k] != 0) c2c + c2p / scale + p2c / scale else -std.math.floatMax(f32);
                    maximum = @max(maximum, scores[k]);
                }
                var denominator: f64 = 0;
                for (scores, probabilities) |value, *probability| {
                    probability.* = @exp(value - maximum);
                    denominator += probability.*;
                }
                var delta: f64 = 0;
                for (0..sequence) |k| {
                    probabilities[k] /= denominator;
                    const keep = fixture.dropout[((b * heads + h) * sequence + q) * sequence + k];
                    const koff = (b * sequence + k) * hidden + h * dim;
                    var dot: f64 = 0;
                    for (0..dim) |d| dot += @as(f64, fixture.dout[qoff + d]) * fixture.qkv[2 * elements + koff + d];
                    dp[k] = keep * dot;
                    delta += probabilities[k] * dp[k];
                }
                for (0..dim) |d| {
                    var value: f64 = 0;
                    for (0..sequence) |k| {
                        const koff = (b * sequence + k) * hidden + h * dim;
                        const keep = fixture.dropout[((b * heads + h) * sequence + q) * sequence + k];
                        value += probabilities[k] * keep * fixture.qkv[2 * elements + koff + d];
                    }
                    output[qoff + d] = @floatCast(value);
                }
                for (0..sequence) |k| {
                    const koff = (b * sequence + k) * hidden + h * dim;
                    const bucket: usize = @intCast(fixture.control[6 + batch * sequence + q + sequence - 1 - k]);
                    const roff = bucket * hidden + h * dim;
                    const keep = fixture.dropout[((b * heads + h) * sequence + q) * sequence + k];
                    const ds = if (fixture.control[6 + b * sequence + q] != 0 and fixture.control[6 + b * sequence + k] != 0) probabilities[k] * (dp[k] - delta) else 0;
                    for (0..dim) |d| {
                        wide[qoff + d] += ds * (@as(f64, fixture.qkv[elements + koff + d]) + fixture.relative[relative_elements + roff + d]) / scale;
                        wide[elements + koff + d] += ds * (@as(f64, fixture.qkv[qoff + d]) + fixture.relative[roff + d]) / scale;
                        wide[2 * elements + koff + d] += probabilities[k] * keep * fixture.dout[qoff + d];
                        wide[3 * elements + roff + d] += ds * fixture.qkv[elements + koff + d] / scale;
                        wide[3 * elements + relative_elements + roff + d] += ds * fixture.qkv[qoff + d] / scale;
                    }
                }
            }
        }
    }
    for (gradient, wide) |*value, expected| value.* = @floatCast(expected);
    return .{ .allocator = a, .output = output, .gradient = gradient };
}

fn expectClose(expected: []const f32, actual: []const f32, absolute: f32, relative_tolerance: f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |want, got, i| {
        if (!std.math.isFinite(got) or @abs(want - got) > absolute + relative_tolerance * @abs(want)) {
            std.debug.print("replay training attention element {d}: expected {d}, actual {d}\n", .{ i, want, got });
            return error.TestExpectedApproxEqAbs;
        }
    }
}

test "replay DeBERTa training attention scalar five VJPs tails and masked rows" {
    const a = std.testing.allocator;
    for ([_]u32{ 1, 7, 17, 131 }) |sequence| {
        for ([_]f32{ 0, 0.1, 0.125 }) |probability| {
            var fixture = try Fixture.init(a, sequence, probability);
            defer fixture.deinit();
            var expected = try scalar(a, fixture);
            defer expected.deinit();
            for ([_][2]usize{ .{ 3, 2 }, .{ 16, 7 } }) |tiles| {
                const options = attention.Options{ .limits = .{ .query_tile = tiles[0], .key_tile = tiles[1] } };
                const output = try attention.forward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, options);
                defer a.free(output);
                const gradient = try attention.backward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.dout, options);
                defer a.free(gradient);
                try expectClose(expected.output, output, 2e-5, 3e-5);
                try expectClose(expected.gradient, gradient, 2e-5, 3e-5);
                // Fully masked second batch has zero Q/K VJP. Its arbitrary
                // upstream cotangent still reaches V through uniform rows.
                const one_batch = @as(usize, sequence) * fixture.attrs.num_heads * fixture.attrs.head_dim;
                for (gradient[one_batch .. 2 * one_batch]) |value| try std.testing.expectEqual(@as(f32, 0), value);
                for (gradient[3 * one_batch .. 4 * one_batch]) |value| try std.testing.expectEqual(@as(f32, 0), value);
                var v_nonzero = false;
                for (gradient[5 * one_batch .. 6 * one_batch]) |value| v_nonzero = v_nonzero or value != 0;
                try std.testing.expect(v_nonzero);
            }
        }
    }
}

test "replay DeBERTa training attention exact counter masks and high limbs" {
    const a = std.testing.allocator;
    for ([_]f32{ 0, 0.1, 0.125 }) |probability| {
        var fixture = try Fixture.init(a, 17, probability);
        defer fixture.deinit();
        const drop = try attention.Dropout.init(fixture.attrs, replay);
        for (fixture.dropout, 0..) |expected, i| try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(drop.value(i))));
        var changed_attrs = fixture.attrs;
        changed_attrs.dropout_stream_id ^= @as(u64, 1) << 40;
        const other = try attention.Dropout.init(changed_attrs, replay);
        if (probability > 0) {
            var difference = false;
            for (fixture.dropout, 0..) |value, i| difference = difference or value != other.value(i);
            try std.testing.expect(difference);
        }
        // The decoder must preserve seed high bits as i32 payload bits.
        var first = try scalar(a, fixture);
        defer first.deinit();
        fixture.control[1] ^= @as(i32, @bitCast(@as(u32, 0x80000000)));
        const output = try attention.forward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, .{});
        defer a.free(output);
        if (probability > 0) try std.testing.expect(!std.mem.eql(f32, first.output, output));
    }
}

fn objective(a: Allocator, fixture: Fixture, options: attention.Options) !f64 {
    const output = try attention.forward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, options);
    defer a.free(output);
    var value: f64 = 0;
    for (output, fixture.dout) |x, dy| value += @as(f64, x) * dy;
    return value;
}

test "replay DeBERTa training attention independent central finite differences" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, 5, 0.125);
    defer fixture.deinit();
    // Repeated edge bucket IDs exercise both table reductions independently.
    const p = try attention.plan(fixture.attrs, .{});
    for (fixture.control[6 + p.batch_tokens ..], 0..) |*bucket, i| bucket.* = if (i % 2 == 0) 0 else 511;
    const options = attention.Options{ .limits = .{ .query_tile = 3, .key_tile = 2 } };
    const gradient = try attention.backward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.dout, options);
    defer a.free(gradient);
    const indices = [_]usize{ 0, 2, p.output_elements + 1, 2 * p.output_elements + 3, p.qkv_elements, p.qkv_elements + 511 * p.hidden + 2, p.qkv_elements + p.relative_elements / 2 + 1, p.qkv_elements + p.relative_elements / 2 + 511 * p.hidden };
    for (indices) |index| {
        const value = if (index < p.qkv_elements) &fixture.qkv[index] else &fixture.relative[index - p.qkv_elements];
        const original = value.*;
        value.* = original + 0.002;
        const plus_x = value.*;
        const plus = try objective(a, fixture, options);
        value.* = original - 0.002;
        const minus_x = value.*;
        const minus = try objective(a, fixture, options);
        value.* = original;
        const numerical: f32 = @floatCast((plus - minus) / (@as(f64, plus_x) - minus_x));
        try std.testing.expectApproxEqAbs(numerical, gradient[index], 8e-5);
    }
}

test "replay DeBERTa training attention plan has no quadratic allocation" {
    const base = attention.Attrs{ .batch = 1, .seq_len = 512, .num_heads = 1, .head_dim = 64, .relative_rows = 512, .dropout_probability = 0.1, .dropout_stream_id = 3 };
    const short = try attention.plan(base, .{});
    var long_attrs = base;
    long_attrs.seq_len = 4096;
    const long = try attention.plan(long_attrs, .{});
    long_attrs.seq_len = 16384;
    const maximum = try attention.plan(long_attrs, .{});
    try std.testing.expectEqual(short.scratch_bytes, long.scratch_bytes);
    try std.testing.expectEqual(long.scratch_bytes, maximum.scratch_bytes);
    try std.testing.expect(short.scratch_bytes < 1024 * 1024);
    try std.testing.expectEqual(@as(usize, (3 * 16384 + 1024) * 64 * 4), maximum.gradient_bytes);
    try std.testing.expectEqual(@as(usize, (6 + 16384 + 2 * 16384 - 1) * 4), maximum.control_bytes);
    try std.testing.expectEqual(@as(u64, 16384) * 16384, maximum.forward_work_items);
    try std.testing.expectEqual(2 * maximum.forward_work_items, maximum.backward_work_items);
    try std.testing.expectError(error.DebertaTrainingAttentionScratchLimitExceeded, attention.plan(base, .{ .max_scratch_bytes = short.scratch_bytes - 1 }));
    try std.testing.expectError(error.DebertaTrainingAttentionTensorLimitExceeded, attention.plan(base, .{ .max_tensor_bytes = short.gradient_bytes - 1 }));
    try std.testing.expectError(error.DebertaTrainingAttentionWorkLimitExceeded, attention.plan(base, .{ .max_work_items = 3 * short.forward_work_items - 1 }));
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionTile, attention.plan(base, .{ .key_tile = 513 }));
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionLimit, attention.plan(base, .{ .max_tensor_bytes = 1024 * 1024 * 1024 + 1 }));
    var invalid = base;
    invalid.dropout_probability = std.math.nan(f32);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, attention.plan(invalid, .{}));
    invalid = base;
    invalid.batch = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, attention.plan(invalid, .{}));
}

test "replay DeBERTa training attention malformed control alias and finite admission" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, 5, 0.1);
    defer fixture.deinit();
    const p = try attention.plan(fixture.attrs, .{});
    const output = try a.alloc(f32, p.output_elements);
    defer a.free(output);
    @memset(output, 19);
    const scratch = try a.alloc(f32, p.scratch_elements);
    defer a.free(scratch);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionControl, attention.forwardInto(fixture.attrs, fixture.qkv, fixture.relative, fixture.control[0 .. fixture.control.len - 1], output, scratch, .{}));
    fixture.control[6] = 2;
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionControl, attention.forwardInto(fixture.attrs, fixture.qkv, fixture.relative, fixture.control, output, scratch, .{}));
    fixture.control[6] = 1;
    const last = fixture.control.len - 1;
    const bucket = fixture.control[last];
    for ([_]i32{ -1, 512 }) |bad| {
        fixture.control[last] = bad;
        try std.testing.expectError(error.InvalidDebertaTrainingAttentionControl, attention.forwardInto(fixture.attrs, fixture.qkv, fixture.relative, fixture.control, output, scratch, .{}));
    }
    fixture.control[last] = bucket;
    for (output) |value| try std.testing.expectEqual(@as(f32, 19), value);
    try std.testing.expectError(error.AliasedDebertaTrainingAttentionStorage, attention.forwardInto(fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.qkv[0..p.output_elements], scratch, .{}));
    const original = fixture.qkv[0];
    fixture.qkv[0] = std.math.inf(f32);
    try std.testing.expectError(error.NonFiniteDebertaTrainingAttention, attention.forward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, .{}));
    fixture.qkv[0] = original;
    const result = try attention.forward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, .{});
    defer a.free(result);
    try std.testing.expectEqual(p.output_elements, result.len);
}

test "replay DeBERTa training attention owned OOM cancellation exact scratch and retry" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, 7, 0.125);
    defer fixture.deinit();
    const options = attention.Options{ .limits = .{ .query_tile = 3, .key_tile = 2 } };
    const Exercise = struct {
        fn run(allocator: Allocator, input: Fixture, opts: attention.Options) !void {
            const result = try attention.forward(allocator, input.attrs, input.qkv, input.relative, input.control, opts);
            defer allocator.free(result);
            const gradient = try attention.backward(allocator, input.attrs, input.qkv, input.relative, input.control, input.dout, opts);
            defer allocator.free(gradient);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{ fixture, options });
    const Cancel = struct {
        checks: usize = 0,
        stop: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks >= self.stop) return error.Cancelled;
        }
    };
    // Cover validation, after an allocation, and during replay/GEMM progress.
    const original = try a.dupe(f32, fixture.qkv);
    defer a.free(original);
    for ([_]usize{ 1, 20, 70, 150 }) |stop| {
        var state = Cancel{ .stop = stop };
        var cancelled = options;
        cancelled.control = .{ .ptr = &state, .check_fn = Cancel.check };
        try std.testing.expectError(error.Cancelled, attention.backward(a, fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.dout, cancelled));
        try std.testing.expectEqualSlices(f32, original, fixture.qkv);
    }
    const p = try attention.plan(fixture.attrs, options.limits);
    const storage = try a.alignedAlloc(u8, .fromByteUnits(@alignOf(f32)), p.backward_owned_bytes);
    defer a.free(storage);
    var bounded = std.heap.FixedBufferAllocator.init(storage);
    const gradient = try attention.backward(bounded.allocator(), fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.dout, options);
    try std.testing.expectEqual(p.gradient_elements, gradient.len);
    // Only the owned output remains; the exact scratch allocation was freed.
    try std.testing.expectEqual(p.gradient_bytes, bounded.end_index);
    bounded.allocator().free(gradient);
    try std.testing.expectEqual(@as(usize, 0), bounded.end_index);
    const retry = try attention.backward(bounded.allocator(), fixture.attrs, fixture.qkv, fixture.relative, fixture.control, fixture.dout, options);
    bounded.allocator().free(retry);
    try std.testing.expectEqual(@as(usize, 0), bounded.end_index);
}

fn backendCase(a: Allocator, fixture: Fixture) !void {
    const native_compute = @import("native_compute.zig");
    const tensors = @import("../backends/tensor.zig");
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native_compute.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const p = try attention.plan(fixture.attrs, .{});
    const layout = try fixture.attrs.layout();
    // A genuine non-contiguous QKV view exercises the checked transactional
    // materialization path, including its ownership failures below.
    const swapped = try a.alloc(f32, fixture.qkv.len);
    defer a.free(swapped);
    const rows: usize = @intCast(layout.qkv_rows);
    for (0..rows) |row| for (0..p.hidden) |column| {
        swapped[column * rows + row] = fixture.qkv[row * p.hidden + column];
    };
    const qkv_raw = try cb.fromFloat32Shape(swapped, &.{ @intCast(p.hidden), @intCast(rows) });
    defer cb.free(qkv_raw);
    const qkv = try cb.primTranspose(qkv_raw, &.{ 1, 0 }, &.{ @intCast(p.hidden), @intCast(rows) });
    defer cb.free(qkv);
    const r = try cb.fromFloat32Shape(fixture.relative, &.{ @intCast(layout.relative_packed_rows), @intCast(p.hidden) });
    defer cb.free(r);
    const dout = try cb.fromFloat32Shape(fixture.dout, &.{ @intCast(p.batch_tokens), @intCast(p.hidden) });
    defer cb.free(dout);
    // Physical i32 bytes deliberately start at an unaligned address. The
    // primitive must retain their high limbs without a copy through F32.
    const bytes = try a.alignedAlloc(u8, .fromByteUnits(4), p.control_bytes + 1);
    defer a.free(bytes);
    @memcpy(bytes[1..], std.mem.sliceAsBytes(fixture.control));
    const control_shape = [_]i64{@intCast(p.control_elements)};
    const words = try compute.importOwnedStaticTensor(tensors.Tensor{ .allocator = a, .data = bytes[1..], .dtype = .i32, .name = "", .shape = &control_shape, .owns_shape = false, .owns_data = false });
    defer cb.free(words);
    const output = try cb.debertaTrainingAttentionV1(qkv, r, words, fixture.attrs);
    defer cb.free(output);
    const gradient = try cb.debertaTrainingAttentionBackwardV1(qkv, r, words, dout, fixture.attrs);
    defer cb.free(gradient);
    const actual = try cb.toFloat32(output, a);
    defer a.free(actual);
    const actual_gradient = try cb.toFloat32(gradient, a);
    defer a.free(actual_gradient);
    var expected = try scalar(a, fixture);
    defer expected.deinit();
    try expectClose(expected.output, actual, 2e-5, 3e-5);
    try expectClose(expected.gradient, actual_gradient, 2e-5, 3e-5);
    const shape = try cb.tensorShape(gradient, a);
    defer a.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ layout.gradient_rows, layout.hidden }, shape);
}

test "replay DeBERTa training attention native hooks retain physical i32 and views through OOM" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, 5, 0.125);
    defer fixture.deinit();
    try std.testing.checkAllAllocationFailures(a, backendCase, .{fixture});
}

test "replay DeBERTa training attention native hooks reject unsupported types and absent profile" {
    const native_compute = @import("native_compute.zig");
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, 7, 0.1);
    defer fixture.deinit();
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native_compute.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const p = try attention.plan(fixture.attrs, .{});
    const layout = try fixture.attrs.layout();
    const qkv = try cb.fromFloat32Shape(fixture.qkv, &.{ @intCast(layout.qkv_rows), @intCast(layout.hidden) });
    defer cb.free(qkv);
    const r = try cb.fromFloat32Shape(fixture.relative, &.{ @intCast(layout.relative_packed_rows), @intCast(layout.hidden) });
    defer cb.free(r);
    const dout = try cb.fromFloat32Shape(fixture.dout, &.{ @intCast(layout.batch_tokens), @intCast(layout.hidden) });
    defer cb.free(dout);
    const words = (try cb.fromInt32Shape(fixture.control, &.{@intCast(p.control_elements)})).?;
    defer cb.free(words);
    const float_values = try a.alloc(f32, p.control_elements);
    defer a.free(float_values);
    @memset(float_values, 0);
    const wrong_type = try cb.fromFloat32Shape(float_values, &.{@intCast(p.control_elements)});
    defer cb.free(wrong_type);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionDType, cb.debertaTrainingAttentionV1(qkv, r, wrong_type, fixture.attrs));
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionDType, cb.debertaTrainingAttentionBackwardV1(qkv, r, wrong_type, dout, fixture.attrs));
    const wrong_shape = try cb.primReshape(qkv, &.{@intCast(p.qkv_elements)});
    defer cb.free(wrong_shape);
    try std.testing.expectError(error.InvalidDebertaTrainingAttentionShape, cb.debertaTrainingAttentionV1(wrong_shape, r, words, fixture.attrs));
    var absent_vtable = native_compute.vtable_impl;
    absent_vtable.debertaTrainingAttentionV1 = null;
    absent_vtable.debertaTrainingAttentionBackwardV1 = null;
    var unavailable = cb;
    unavailable.vtable = &absent_vtable;
    try std.testing.expectError(error.DebertaTrainingAttentionProfileUnavailable, unavailable.debertaTrainingAttentionV1(qkv, r, words, fixture.attrs));
    try std.testing.expectError(error.DebertaTrainingAttentionProfileUnavailable, unavailable.debertaTrainingAttentionBackwardV1(qkv, r, words, dout, fixture.attrs));
    const Cancel = struct {
        count: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count >= 40) return error.Cancelled;
        }
    };
    var cancellation = Cancel{};
    cb.execution_control = .{ .ptr = &cancellation, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, cb.debertaTrainingAttentionBackwardV1(qkv, r, words, dout, fixture.attrs));
    cb.execution_control = null;
    const retry = try cb.debertaTrainingAttentionBackwardV1(qkv, r, words, dout, fixture.attrs);
    cb.free(retry);
}
