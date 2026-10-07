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
const identity = seeded.StepIdentity{ .binding = @splat(0x19), .optimizer_step = 4, .microbatch = 7 };
const decisions = @as([32]u8, @splat(0x51));

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
    for (0..3) |profile| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, constructionCheck, .{profile});
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

    pub fn deinit(self: *Inputs, cb: *const ops.ComputeBackend) void {
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
                var tape = try prefix.forwardSuffix(identity, @splat(0x24), inputs.runtime(&buffer, 3, 5), null);
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
                try stages.advance(identity, @splat(0x24), inputs.runtime(&buffer, 3, 4), null);
                inputs.release(&cb, 3, 4);
                try fixture.expectValues(a, &cb, try stages.logits(0), &.{ 100, 324, 4, 0, 100, 324 }, 0, 0, "candidate scores");
                try stages.advance(identity, @splat(0x47), inputs.runtime(&buffer, 4, 5), null);
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
            1 => try std.testing.expectError(error.TrainingDecisionIdentityMismatch, tape.backward(identity, @splat(0x37), 1, &.{seed}, null)),
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

test "resident session Metal batched matmul with transposed right operand matches the interpreter" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try fixture.Device.init(a);
    defer device.deinit();
    const metal = device.backend.computeBackend();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cpu = compute.computeBackend();
    const Case = struct { batch: i64, rows: i64, cols: i64, depth: i64 };
    var failed = false;
    for ([_]Case{
        .{ .batch = 8, .rows = 56, .cols = 56, .depth = 16 },
        .{ .batch = 8, .rows = 64, .cols = 64, .depth = 16 },
        .{ .batch = 8, .rows = 56, .cols = 56, .depth = 64 },
        .{ .batch = 1, .rows = 56, .cols = 56, .depth = 16 },
        .{ .batch = 8, .rows = 56, .cols = 56, .depth = 2 },
        .{ .batch = 8, .rows = 32, .cols = 32, .depth = 16 },
        .{ .batch = 2, .rows = 7, .cols = 5, .depth = 3 },
    }) |case| {
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var b = ml.Builder.init(&graph);
        const left = try b.parameter("left", ml.Shape.init(.f32, &.{ case.batch, case.rows, case.depth }));
        const right = try b.parameter("right", ml.Shape.init(.f32, &.{ case.batch, case.cols, case.depth }));
        const product = try b.matmul3DTransB(left, right);
        const cotangent = try b.parameter("__cotangent", graph.node(product).output_shape);
        try graph.markOutput(product);
        const sizes = [_]usize{ @intCast(case.batch * case.rows * case.depth), @intCast(case.batch * case.cols * case.depth) };
        var values: [2][]f32 = undefined;
        for (&values, sizes, 0..) |*out, size, which| {
            out.* = try a.alloc(f32, size);
            for (out.*, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i * 7 + which * 3 + 1)) * 0.37);
        }
        defer for (values) |v| a.free(v);
        const dims = [2][3]i32{ .{ @intCast(case.batch), @intCast(case.rows), @intCast(case.depth) }, .{ @intCast(case.batch), @intCast(case.cols), @intCast(case.depth) } };
        const cpu_inputs = [_]interpreter.RuntimeInput{ .{ .node_id = left, .value = try cpu.fromFloat32Shape(values[0], &dims[0]) }, .{ .node_id = right, .value = try cpu.fromFloat32Shape(values[1], &dims[1]) } };
        defer for (cpu_inputs) |input| cpu.free(input.value);
        var reference = try interpreter.execute(a, &graph, &cpu, .{ .runtime_inputs = &cpu_inputs });
        defer reference.deinit(&cpu);
        const metal_inputs = [_]interpreter.RuntimeInput{ .{ .node_id = left, .value = try metal.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values[0], .shape = &dims[0] } }, .{}) }, .{ .node_id = right, .value = try metal.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values[1], .shape = &dims[1] } }, .{}) } };
        defer for (metal_inputs) |input| metal.free(input.value);
        var session = try seeded.Session.init(a, &graph, &.{.{ .output = product, .cotangent = cotangent }}, &.{ left, right }, execution_options);
        defer session.deinit();
        var tape = try session.forward(&metal, &metal_inputs, identity, null);
        defer tape.deinit();
        const want = try cpu.toFloat32(reference.outputs[0], a);
        defer a.free(want);
        const got = try metal.toFloat32(try tape.logits(0), a);
        defer a.free(got);
        var worst: f32 = 0;
        for (want, got) |w, g| worst = @max(worst, @abs(w - g));
        std.debug.print("matmul3DTransB batch={d} rows={d} cols={d} depth={d}: max error {d}\n", .{ case.batch, case.rows, case.cols, case.depth, worst });
        failed = failed or worst > 1e-3;
    }
    try std.testing.expect(!failed);
}
