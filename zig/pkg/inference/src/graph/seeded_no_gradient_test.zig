// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const staged = @import("staged_training.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const fixture = @import("resident_training_fixture.zig");
const metal = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Allocator = std.mem.Allocator;
const identity = seeded.StepIdentity{ .binding = @splat(17), .optimizer_step = 3, .microbatch = 5 };
const decisions = [_]u8{23} ** 32;
const shape = ml.Shape.init(.f32, &.{2});

const Graph = struct {
    graph: ml.Graph,
    x: ml.NodeId,
    gate: ml.NodeId,
    dormant: ml.NodeId,
    first: ml.NodeId,
    seed: ml.autodiff.Seed,

    fn init(a: Allocator) !Graph {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const x = try b.parameter("frozen_features", shape);
        const gate = try b.parameter("selected_candidates", shape);
        const dormant = try b.parameter("dormant_adapter", shape);
        const first = try b.mul(x, x);
        const output = try b.mul(first, gate);
        const seed = try b.parameter("cotangent", shape);
        return .{ .graph = graph, .x = x, .gate = gate, .dormant = dormant, .first = first, .seed = .{ .output = output, .cotangent = seed } };
    }
};

fn construction(a: Allocator, execution: seeded.Execution) !void {
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &.{graph.dormant}, .{ .execution = execution, .allow_no_gradients = true });
    defer session.deinit();
    try std.testing.expectEqual(@as(usize, 0), session.gradient_parameters.len);
    try std.testing.expectEqual(@as(usize, 0), session.backward.graph.nodeCount());
    try std.testing.expectEqual(@as(usize, 0), session.backward.graph.constant_pool.items.len);
    if (session.executionAdmission()) |admission| {
        try std.testing.expectEqual(@as(usize, 1), admission.programs);
        try std.testing.expectEqual(@as(usize, 0), admission.backward_output_bytes);
        try std.testing.expect(admission.retained_capture_bytes > 0);
    }
}

test "seeded no-gradient sessions are opt in and preserve strict parameter validation with allocation cleanup" {
    var graph = try Graph.init(std.testing.allocator);
    defer graph.graph.deinit();
    for ([_]seeded.Execution{ .native, .resident_metal }) |execution| {
        try std.testing.expectError(error.DisconnectedGradientParameter, seeded.Session.init(std.testing.allocator, &graph.graph, &.{graph.seed}, &.{graph.dormant}, .{ .execution = execution }));
        try std.testing.expectError(error.DisconnectedGradientParameter, seeded.Session.init(std.testing.allocator, &graph.graph, &.{graph.seed}, &.{graph.dormant}, .{ .execution = execution, .allow_no_gradients = true, .gradient = .{ .require_all_gradients = true } }));
        try std.testing.checkAllAllocationFailures(std.testing.allocator, construction, .{execution});
    }
}

fn upload(cb: *const ops.ComputeBackend, values: []const f32) !ops.CT {
    if (cb.kind() == .metal) return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = &.{2} } }, .{});
    return cb.fromFloat32Shape(values, &.{2});
}

fn finish(a: Allocator, cb: *const ops.ComputeBackend, tape: *seeded.Tape, cotangent: ops.CT, failure: usize) !void {
    const values = if (cb.kind() == .metal) download: {
        const result = try a.alloc(f32, 2);
        errdefer a.free(result);
        try cb.glinerBoundaryDownload(try tape.logits(0), result);
        break :download result;
    } else try cb.toFloat32(try tape.logits(0), a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 12, 80 }, values);
    try std.testing.expectError(error.TrainingTapeStillLive, tape.session.advanceParameterEpoch());
    try tape.sealDecisions(decisions);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    if (failure == 1) {
        var stale = tape.identity;
        stale.microbatch += 1;
        try std.testing.expectError(error.TrainingTapeIdentityMismatch, tape.backward(stale, decisions, 0, &.{cotangent}, null));
    } else if (failure == 2) {
        try std.testing.expectError(error.Cancelled, tape.backward(tape.identity, decisions, 0, &.{cotangent}, .{ .check_fn = Cancel.check }));
    } else {
        const before = metal_tensor.memoryStatsSnapshot();
        var result = try tape.backward(tape.identity, decisions, 0, &.{cotangent}, null);
        defer result.deinit(cb);
        try std.testing.expectEqual(@as(usize, 0), result.parameter_ids.len);
        try std.testing.expectEqual(@as(usize, 0), result.gradients.outputs.len);
        try std.testing.expectEqual(@as(f32, 0), result.loss);
        if (cb.kind() == .metal) {
            const after = metal_tensor.memoryStatsSnapshot();
            try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
            try std.testing.expectEqual(before.to_host_calls, after.to_host_calls);
            try std.testing.expectEqual(@as(usize, 12), result.control_readback_bytes);
        }
    }
    try std.testing.expect(tape.consumed);
    try std.testing.expect(!tape.session.active_tape);
    try tape.session.advanceParameterEpoch();
}

fn exercise(a: Allocator, cb: *const ops.ComputeBackend, execution: seeded.Execution) !void {
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    const x = try upload(cb, &.{ 2, 4 });
    defer cb.free(x);
    const gate = try upload(cb, &.{ 3, 5 });
    defer cb.free(gate);
    const cotangent = try upload(cb, &.{ 1, 1 });
    defer cb.free(cotangent);
    const options = seeded.Options{ .execution = execution, .allow_no_gradients = true };
    // Cover an empty selected intersection and a disconnected declared leaf,
    // both before and after a retained proposal boundary.
    for (0..3) |failure| {
        var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &.{}, options);
        defer session.deinit();
        var tape = try session.forward(cb, &.{ .{ .node_id = graph.x, .value = x }, .{ .node_id = graph.gate, .value = gate } }, identity, null);
        defer tape.deinit();
        try finish(a, cb, &tape, cotangent, failure);
        var stages = try staged.Session.init(a, &graph.graph, &.{graph.seed}, &.{graph.dormant}, .{ .deferred_parameters = &.{graph.gate}, .outputs = &.{graph.first} }, options);
        defer stages.deinit();
        var prefix = try stages.forwardPrefix(cb, &.{.{ .node_id = graph.x, .value = x }}, identity, null);
        defer prefix.deinit();
        var suffix = try prefix.forwardSuffix(identity, @splat(27), &.{.{ .node_id = graph.gate, .value = gate }}, null);
        defer suffix.deinit();
        try finish(a, cb, &suffix, cotangent, failure);
    }
}

fn cpu(a: Allocator) !void {
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    try exercise(a, &cb, .native);
}

test "seeded no-gradient CPU direct and staged tapes consume ownership on success cancellation identity failure and OOM" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cpu, .{});
}

test "seeded no-gradient Metal direct and staged tapes preserve resident ownership and finite cotangent checks" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    try exercise(a, &cb, .resident_metal);
}
