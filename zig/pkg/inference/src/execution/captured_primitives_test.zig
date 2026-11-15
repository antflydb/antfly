// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ops = @import("../ops/ops.zig");
const capture = @import("capture.zig");
const prepared = @import("captured_forward.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Kind = enum { conv1d, conv2d, grid_grouped, grid_depthwise, sdpa_token, sdpa_heads, sdpa_bias, causal, cross, relative, gqa, marker_head, interleaved_pairs };
const Fixture = struct {
    kind: Kind,
    weights: @import("../ops/native_compute.zig").WeightStore,
    fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return @import("../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
    }
    fn import(_: *anyopaque, cb: *const ops.ComputeBackend, input: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
        const native: *@import("../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
        return native.importHostTensor(input);
    }
    fn tensor(cb: *const ops.ComputeBackend, input: Tensor) !ops.CT {
        var shape: [8]i32 = undefined;
        if (input.shape.len > shape.len) return error.UnsupportedShape;
        for (input.shape, 0..) |dim, axis| shape[axis] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
        return cb.fromFloat32Shape(input.asFloat32(), shape[0..input.shape.len]);
    }
    fn forward(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const x = try tensor(cb, inputs[0].tensor);
        defer cb.free(x);
        const result = if (self.kind == .interleaved_pairs) paired: {
            const wa = try cb.getWeight("wa");
            defer cb.free(wa);
            const wb = try cb.getWeight("wb");
            defer cb.free(wb);
            const wc = try cb.getWeight("wc");
            defer cb.free(wc);
            const wd = try cb.getWeight("wd");
            defer cb.free(wd);
            const first = try cb.linearNoBiasPair(x, wa, wb, 2, 4, 3);
            defer cb.free(first.first);
            defer cb.free(first.second);
            const second = try cb.linearNoBiasPair(x, wc, wd, 2, 4, 3);
            defer cb.free(second.first);
            defer cb.free(second.second);
            const first_sum = try cb.add(first.first, second.first);
            defer cb.free(first_sum);
            const second_sum = try cb.add(first.second, second.second);
            defer cb.free(second_sum);
            break :paired try cb.add(first_sum, second_sum);
        } else if (self.kind == .marker_head) (try @import("../architectures/gliner_decision_head.zig").forwardCt(cb, allocator, x, inputs[1].tensor.asInt64(), inputs[2].tensor.asInt64(), 2, 3, 2, 2)).logits else switch (self.kind) {
            .conv1d, .conv2d, .grid_grouped, .grid_depthwise => convolution: {
                const weight = try cb.getWeight("weight");
                defer cb.free(weight);
                const bias = try cb.getWeight("bias");
                defer cb.free(bias);
                break :convolution switch (self.kind) {
                    .conv1d => try cb.conv1d(x, weight, bias, 2, 2, 4, 5, 3, 2, 1),
                    .conv2d => try cb.conv2d(x, weight, bias, 2, 4, 4, 3, 4, 3, 3, 1, 1, 1, 1, 2),
                    else => try cb.tokenGridConv2d(x, weight, bias, 2, 4, 4, 3, 4, 3, 3, 1, 1, 1, 1, if (self.kind == .grid_depthwise) 4 else 2),
                };
            },
            else => attention: {
                const key = try tensor(cb, inputs[1].tensor);
                defer cb.free(key);
                const value = try tensor(cb, inputs[2].tensor);
                defer cb.free(value);
                if (self.kind == .gqa) break :attention try cb.gqaCausalAttention(x, key, value, null, 1, 3, 4, 2, 2);
                if (self.kind == .causal) break :attention try cb.causalSelfAttention(x, key, value, null, 2, 3, 2, 2);
                if (self.kind == .relative) {
                    const qr = try tensor(cb, inputs[3].tensor);
                    defer cb.free(qr);
                    const kr = try tensor(cb, inputs[4].tensor);
                    defer cb.free(kr);
                    break :attention try cb.disentangledRelativeAttention(x, key, value, qr, kr, inputs[5].tensor.asInt64(), 2, 3, 2, 2);
                }
                const mask = inputs[3].tensor.asInt64();
                if (self.kind == .cross) break :attention try cb.crossAttention(x, key, value, mask, 2, 2, 3, 2, 2);
                const bias = if (self.kind == .sdpa_bias) try tensor(cb, inputs[4].tensor) else null;
                defer if (bias) |ct| cb.free(ct);
                break :attention try cb.scaledDotProductAttention(x, key, value, mask, bias, 2, 3, 2, 2);
            },
        };
        errdefer cb.free(result);
        const outputs = try allocator.alloc(capture.Output, 1);
        outputs[0] = .{ .name = "result", .value = result };
        return outputs;
    }
    fn putWeight(self: *@This(), name: []const u8, dims: []const i64) !void {
        return self.putWeightShift(name, dims, 1);
    }
    fn putWeightShift(self: *@This(), name: []const u8, dims: []const i64, shift: i32) !void {
        const a = self.weights.allocator;
        var value = try numbers(a, name, dims, shift);
        errdefer value.deinit();
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        try self.weights.resident_weights.put(a, key, .{ .tensor = value, .quantized = false, .quantized_storage = null });
    }
};
fn numbers(a: std.mem.Allocator, name: []const u8, dims: []const i64, shift: i32) !Tensor {
    var count: usize = 1;
    for (dims) |dim| count = try std.math.mul(usize, count, @intCast(dim));
    const data = try a.alloc(f32, count);
    defer a.free(data);
    for (data, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5 + shift)) / 7;
    return Tensor.initFloat32(a, name, dims, data);
}
fn qualify(kind: Kind) !void {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var source = Fixture{ .kind = kind, .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    var inputs: std.ArrayList(capture.Input) = .empty;
    defer inputs.deinit(a);
    defer for (inputs.items) |*input| input.tensor.deinit();
    const shape: []const i64 = switch (kind) {
        .conv1d => &.{ 2, 2, 5 },
        .conv2d => &.{ 2, 4, 3, 4 },
        .grid_grouped, .grid_depthwise => &.{ 24, 4 },
        .sdpa_heads => &.{ 4, 3, 2 },
        .cross => &.{ 4, 4 },
        .gqa => &.{ 3, 8 },
        .marker_head => &.{ 6, 2 },
        .interleaved_pairs => &.{ 2, 4 },
        else => &.{ 6, 4 },
    };
    try inputs.append(a, .{ .tensor = try numbers(a, "x", shape, 0) });
    switch (kind) {
        .conv1d => {
            try source.putWeight("weight", &.{ 4, 2, 3 });
            try source.putWeight("bias", &.{4});
        },
        .conv2d, .grid_grouped => {
            try source.putWeight("weight", &.{ 4, 2, 3, 3 });
            try source.putWeight("bias", &.{4});
        },
        .grid_depthwise => {
            try source.putWeight("weight", &.{ 4, 1, 3, 3 });
            try source.putWeight("bias", &.{4});
        },
        .interleaved_pairs => {
            try source.putWeightShift("wa", &.{ 3, 4 }, 1);
            try source.putWeightShift("wb", &.{ 3, 4 }, 2);
            try source.putWeightShift("wc", &.{ 3, 4 }, 3);
            try source.putWeightShift("wd", &.{ 3, 4 }, 4);
        },
        .marker_head => {
            try source.putWeight("classifier.0.weight", &.{ 4, 2 });
            try source.putWeight("classifier.0.bias", &.{4});
            try source.putWeight("classifier.2.weight", &.{ 1, 4 });
            try source.putWeight("classifier.2.bias", &.{1});
            try inputs.append(a, .{ .tensor = try Tensor.initInt64(a, "decision_marker_positions", &.{ 2, 2 }, &.{ 2, 0, 1, 0 }) });
            try inputs.append(a, .{ .tensor = try Tensor.initInt64(a, "decision_marker_mask", &.{ 2, 2 }, &.{ 1, 0, 1, 1 }) });
        },
        else => {
            const key_shape: []const i64 = if (kind == .cross) &.{ 6, 4 } else if (kind == .gqa) &.{ 3, 4 } else shape;
            try inputs.append(a, .{ .tensor = try numbers(a, "key", key_shape, 1) });
            try inputs.append(a, .{ .tensor = try numbers(a, "value", key_shape, 2) });
            if (kind == .relative) {
                try inputs.append(a, .{ .tensor = try numbers(a, "query_relative", &.{ 5, 4 }, -1) });
                try inputs.append(a, .{ .tensor = try numbers(a, "key_relative", &.{ 5, 4 }, 3) });
                try inputs.append(a, .{ .tensor = try Tensor.initInt64(a, "attention_mask", &.{ 2, 3 }, &.{ 1, 0, 1, 0, 0, 0 }) });
            } else if (kind != .causal and kind != .gqa) try inputs.append(a, .{ .tensor = try Tensor.initInt64(a, "attention_mask", &.{ 2, 3 }, &.{ 1, 0, 1, 1, 1, 0 }) });
            if (kind == .sdpa_bias) try inputs.append(a, .{ .tensor = try numbers(a, "bias", &.{ 2, 3, 3 }, -1) });
        },
    }
    var reference = try Fixture.create(&source, a);
    defer reference.deinit();
    var cache = prepared.Cache{};
    defer cache.deinit(a);
    const dynamic_mask = switch (kind) {
        .relative, .sdpa_heads, .sdpa_token, .sdpa_bias, .cross => true,
        else => false,
    };
    for (0..if (kind == .marker_head or dynamic_mask) @as(usize, 3) else 1) |iteration| {
        if (dynamic_mask and iteration != 0) {
            const input = &inputs.items[if (kind == .relative) @as(usize, 5) else 3].tensor;
            const mask: []i64 = @alignCast(std.mem.bytesAsSlice(i64, input.data));
            @memcpy(mask, if (iteration == 1) &[_]i64{ 1, 1, 1, 1, 1, 1 } else &[_]i64{ 0, 1, 1, 1, 0, 1 });
        }
        if (kind == .marker_head and iteration == 1) @as([*]i64, @ptrCast(@alignCast(inputs.items[1].tensor.data.ptr)))[0] = 1;
        const expected = try Fixture.forward(&source, &reference, a, inputs.items);
        defer a.free(expected);
        defer for (expected) |output| reference.free(output.value);
        const values = try reference.toFloat32(expected[0].value, a);
        defer a.free(values);
        const actual = try cache.run(a, .{ .context = &source, .create = Fixture.create, .import = Fixture.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } }, .{ .context = &source, .fingerprint = @backingInt(kind), .output_names = &.{"result"}, .run = Fixture.forward }, inputs.items, a, null);
        defer a.free(actual);
        defer for (actual) |*output| output.deinit();
        try std.testing.expectEqual(values.len, actual[0].asFloat32().len);
        for (values, actual[0].asFloat32()) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
    try std.testing.expectEqual(@as(u64, 1), cache.entries.stats.builds);
}

test "captured primitive PJRT biased strided conv1d matches native" {
    try qualify(.conv1d);
}
test "captured primitive PJRT grouped conv2d matches native" {
    try qualify(.conv2d);
}
test "captured primitive PJRT grouped token-grid convolution matches native" {
    try qualify(.grid_grouped);
}
test "captured primitive PJRT depthwise token-grid convolution matches native" {
    try qualify(.grid_depthwise);
}
test "captured primitive PJRT masked token-major attention matches native" {
    try qualify(.sdpa_token);
}
test "captured primitive PJRT masked head-major attention matches native" {
    try qualify(.sdpa_heads);
}
test "captured primitive PJRT shared-head biased attention matches native" {
    try qualify(.sdpa_bias);
}
test "captured primitive PJRT causal attention matches native" {
    try qualify(.causal);
}
test "captured primitive PJRT cross attention matches native" {
    try qualify(.cross);
}

test "captured primitive PJRT relative attention preserves both terms and empty masked batches" {
    try qualify(.relative);
}

test "captured primitive PJRT grouped-query attention repeats KV heads without matrix projection" {
    try qualify(.gqa);
}

test "captured primitive PJRT marker classifier keeps row indices dynamic across cache hits" {
    try qualify(.marker_head);
}

test "captured primitive PJRT interleaved linear pairs preserve producer-specific secondary results" {
    try qualify(.interleaved_pairs);
}
