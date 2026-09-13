// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const options = @import("build_options");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const staged = @import("staged_training.zig");
const multi = @import("multi_stage_training.zig");
const fixture = @import("resident_training_fixture.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const interpreter = @import("interpreter.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Allocator = std.mem.Allocator;

const Graph = struct {
    graph: ml.Graph,
    x: ml.NodeId,
    weight: ml.NodeId,
    dropout: ml.NodeId,
    pool: ml.NodeId,
    relations: ml.NodeId,
    first: ml.NodeId,
    second: ml.NodeId,
    seed: ml.autodiff.Seed,

    fn init(a: Allocator) !Graph {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const x = try b.parameter("x", ml.Shape.init(.f32, &.{ 3, 2 }));
        const weight = try b.parameter("weight", ml.Shape.init(.f32, &.{2}));
        const dropout = try b.parameter("dropout", ml.Shape.init(.f32, &.{ 3, 2 }));
        const pool = try b.parameter("pool", ml.Shape.init(.i32, &.{3}));
        const relations = try b.parameter("relations", ml.Shape.init(.i32, &.{2}));
        const first = try b.mul(try b.mul(x, weight), dropout);
        const candidate = try b.gather(first, pool, ml.Shape.init(.f32, &.{ 3, 2 }));
        const second = try b.mul(candidate, candidate);
        const selected = try b.gather(second, relations, ml.Shape.init(.f32, &.{ 2, 2 }));
        const output = try b.reduceSum(selected, &.{0});
        const cotangent = try b.parameter("cotangent", graph.node(output).output_shape);
        try graph.markOutput(output);
        return .{ .graph = graph, .x = x, .weight = weight, .dropout = dropout, .pool = pool, .relations = relations, .first = first, .second = second, .seed = .{ .output = output, .cotangent = cotangent } };
    }
};
const execution_options = seeded.Options{ .execution = .resident_metal, .gradient = .{ .require_all_gradients = true } };
const identity = seeded.StepIdentity{ .binding = .{0x19} ** 32, .optimizer_step = 4, .microbatch = 7 };
const decisions = [_]u8{0x51} ** 32;

fn constructionCheck(a: Allocator, profile: usize) !void {
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    const wrt = [_]ml.NodeId{ graph.x, graph.weight };
    switch (profile) {
        0 => {
            var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, execution_options);
            defer session.deinit();
            try std.testing.expectEqual(@as(usize, 2), session.executionAdmission().?.programs);
        },
        1 => {
            var session = try staged.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, .{ .deferred_parameters = &.{ graph.pool, graph.relations }, .outputs = &.{graph.first} }, execution_options);
            defer session.deinit();
            try std.testing.expectEqual(@as(usize, 3), session.base.executionAdmission().?.programs);
        },
        2 => {
            var session = try multi.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, &.{
                .{ .deferred_parameters = &.{graph.pool}, .outputs = &.{graph.first} },
                .{ .deferred_parameters = &.{graph.relations}, .outputs = &.{graph.second} },
            }, execution_options);
            defer session.deinit();
            try std.testing.expectEqual(@as(usize, 4), session.base.executionAdmission().?.programs);
        },
        else => unreachable,
    }
}

test "resident session direct and stage construction release every allocation failure" {
    for (0..3) |profile| try std.testing.checkAllAllocationFailures(std.testing.allocator, constructionCheck, .{profile});
}

test "resident session explicit backend profile and combined retained admission reject before execution" {
    const a = std.testing.allocator;
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    const wrt = [_]ml.NodeId{ graph.x, graph.weight };
    var ordinary = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, .{});
    defer ordinary.deinit();
    try std.testing.expect(ordinary.executionAdmission() == null);
    var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, execution_options);
    defer session.deinit();
    const admission = session.executionAdmission().?;
    try std.testing.expect(admission.retained_capture_bytes > 0);
    try std.testing.expect(admission.device_upper_bound_bytes > admission.persistent_binding_bytes + admission.retained_capture_bytes);
    var limits = execution_options;
    limits.resident.max_device_bytes = admission.device_upper_bound_bytes - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, limits));
    limits = execution_options;
    limits.resident.max_compile_bytes = admission.compile_upper_bound_bytes - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, limits));
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    try ordinary.validateBackend(&cb);
    try std.testing.expectError(error.UnsupportedSeededTrainingBackend, session.forward(&cb, &.{}, identity, null));
}

const Inputs = struct {
    ids: [5]ml.NodeId,
    values: [5]?ops.CT = @splat(null),

    fn init(cb: *const ops.ComputeBackend, graph: *const Graph) !Inputs {
        var result = Inputs{ .ids = .{ graph.x, graph.weight, graph.dropout, graph.pool, graph.relations } };
        errdefer result.deinit(cb);
        result.values[0] = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 2, 3, 4, 5, 6 }, .shape = &.{ 3, 2 } } }, .{});
        result.values[1] = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 2, 3 }, .shape = &.{2} } }, .{});
        result.values[2] = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 0, 1, 1, 1, 1 }, .shape = &.{ 3, 2 } } }, .{});
        result.values[3] = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &.{ 2, 0, 2 }, .shape = &.{3} } }, .{});
        result.values[4] = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &.{ 1, 0 }, .shape = &.{2} } }, .{});
        return result;
    }

    fn release(self: *Inputs, cb: *const ops.ComputeBackend, begin: usize, end: usize) void {
        for (self.values[begin..end]) |*value| {
            if (value.*) |owned| cb.free(owned);
            value.* = null;
        }
    }

    fn deinit(self: *Inputs, cb: *const ops.ComputeBackend) void {
        self.release(cb, 0, self.values.len);
    }

    fn runtime(self: *const Inputs, buffer: *[5]interpreter.RuntimeInput, begin: usize, end: usize) []const interpreter.RuntimeInput {
        for (begin..end, buffer[0 .. end - begin]) |index, *out| out.* = .{ .node_id = self.ids[index], .value = self.values[index].? };
        return buffer[0 .. end - begin];
    }
};

fn finish(a: Allocator, cb: *const ops.ComputeBackend, tape: *seeded.Tape, wrt: []const ml.NodeId) !void {
    try fixture.expectValues(a, cb, try tape.logits(0), &.{ 104, 324 }, 0, 0, "staged logits");
    try std.testing.expectError(error.TrainingTapeStillLive, tape.session.advanceParameterEpoch());
    try tape.sealDecisions(decisions);
    const seed = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 0.5, 2 }, .shape = &.{ 1, 2 } } }, .{});
    defer cb.free(seed);
    const before = metal_tensor.memoryStatsSnapshot();
    var backward = try tape.backward(tape.identity, decisions, 700, &.{seed}, null);
    defer backward.deinit(cb);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(@as(usize, 12), backward.control_readback_bytes);
    try std.testing.expectEqualSlices(ml.NodeId, wrt, backward.parameter_ids);
    try fixture.expectValues(a, cb, backward.gradients.outputs[0], &.{ 4, 0, 0, 0, 20, 216 }, 0, 0, "staged x gradient");
    try fixture.expectValues(a, cb, backward.gradients.outputs[1], &.{ 52, 432 }, 0, 0, "staged weight gradient");
    try tape.session.advanceParameterEpoch();
}

test "resident session Metal direct two and three stages retain caller bindings through backward" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    const wrt = [_]ml.NodeId{ graph.x, graph.weight };
    for (0..3) |profile| {
        errdefer std.debug.print("resident staged session profile{d}\n", .{profile});
        var inputs = try Inputs.init(&cb, &graph);
        defer inputs.deinit(&cb);
        var buffer: [5]interpreter.RuntimeInput = undefined;
        switch (profile) {
            0 => {
                var ordinary = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, .{});
                defer ordinary.deinit();
                try std.testing.expectError(error.UnsupportedSeededTrainingBackend, ordinary.forward(&cb, inputs.runtime(&buffer, 0, 5), identity, null));
                var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, execution_options);
                defer session.deinit();
                var tape = try session.forward(&cb, inputs.runtime(&buffer, 0, 5), identity, null);
                defer tape.deinit();
                inputs.release(&cb, 0, 5);
                try finish(a, &cb, &tape, &wrt);
            },
            1 => {
                var session = try staged.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, .{ .deferred_parameters = &.{ graph.pool, graph.relations }, .outputs = &.{graph.first} }, execution_options);
                defer session.deinit();
                var prefix = try session.forwardPrefix(&cb, inputs.runtime(&buffer, 0, 3), identity, null);
                defer prefix.deinit();
                inputs.release(&cb, 0, 3);
                try fixture.expectValues(a, &cb, try prefix.logits(0), &.{ 2, 0, 6, 12, 10, 18 }, 0, 0, "prefix dropout");
                var tape = try prefix.forwardSuffix(identity, .{0x24} ** 32, inputs.runtime(&buffer, 3, 5), null);
                defer tape.deinit();
                inputs.release(&cb, 3, 5);
                try finish(a, &cb, &tape, &wrt);
            },
            2 => {
                var session = try multi.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, &.{
                    .{ .deferred_parameters = &.{graph.pool}, .outputs = &.{graph.first} },
                    .{ .deferred_parameters = &.{graph.relations}, .outputs = &.{graph.second} },
                }, execution_options);
                defer session.deinit();
                var stages = try session.forward(&cb, inputs.runtime(&buffer, 0, 3), identity, null);
                defer stages.deinit();
                inputs.release(&cb, 0, 3);
                try stages.advance(identity, .{0x24} ** 32, inputs.runtime(&buffer, 3, 4), null);
                inputs.release(&cb, 3, 4);
                try fixture.expectValues(a, &cb, try stages.logits(0), &.{ 100, 324, 4, 0, 100, 324 }, 0, 0, "candidate scores");
                try stages.advance(identity, .{0x47} ** 32, inputs.runtime(&buffer, 4, 5), null);
                inputs.release(&cb, 4, 5);
                var tape = try stages.finish();
                defer tape.deinit();
                try finish(a, &cb, &tape, &wrt);
            },
            else => unreachable,
        }
    }
}

test "resident session Metal rejects nonfinite cotangents stale decisions and cancellation then recovers" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var graph = try Graph.init(a);
    defer graph.graph.deinit();
    const wrt = [_]ml.NodeId{ graph.x, graph.weight };
    var session = try seeded.Session.init(a, &graph.graph, &.{graph.seed}, &wrt, execution_options);
    defer session.deinit();
    var inputs = try Inputs.init(&cb, &graph);
    defer inputs.deinit(&cb);
    var buffer: [5]interpreter.RuntimeInput = undefined;
    const seed = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 0.5, 2 }, .shape = &.{ 1, 2 } } }, .{});
    defer cb.free(seed);
    const nonfinite = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ std.math.nan(f32), 2 }, .shape = &.{ 1, 2 } } }, .{});
    defer cb.free(nonfinite);
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    for (0..4) |failure| {
        var tape = try session.forward(&cb, inputs.runtime(&buffer, 0, 5), identity, null);
        defer tape.deinit();
        try tape.sealDecisions(decisions);
        switch (failure) {
            0 => try std.testing.expectError(error.InvalidTrainingCotangent, tape.backward(identity, decisions, 1, &.{nonfinite}, null)),
            1 => try std.testing.expectError(error.TrainingDecisionIdentityMismatch, tape.backward(identity, .{0x37} ** 32, 1, &.{seed}, null)),
            2 => {
                var stale = identity;
                stale.optimizer_step += 1;
                try std.testing.expectError(error.TrainingTapeIdentityMismatch, tape.backward(stale, decisions, 1, &.{seed}, null));
            },
            3 => try std.testing.expectError(error.Cancelled, tape.backward(identity, decisions, 1, &.{seed}, .{ .check_fn = Cancel.check })),
            else => unreachable,
        }
        try std.testing.expect(!session.active_tape);
    }
    var recovered = try session.forward(&cb, inputs.runtime(&buffer, 0, 5), identity, null);
    defer recovered.deinit();
    try finish(a, &cb, &recovered, &wrt);
}
