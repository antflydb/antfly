// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exhaust the small session-owned runtime allocation surface while reusing
//! admitted graphs and device inputs. Primitive/device-buffer allocation
//! failures are tested separately; this targets tape lease transfers.
const std = @import("std");
const options = @import("build_options");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const staged = @import("staged_training.zig");
const multi = @import("multi_stage_training.zig");
const interpreter = @import("interpreter.zig");
const fixture = @import("resident_training_fixture.zig");
const ops = @import("../ops/ops.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const Allocator = std.mem.Allocator;
const identity = seeded.StepIdentity{ .binding = .{0x42} ** 32, .optimizer_step = 0, .microbatch = 0 };
const decisions = [_]u8{0x31} ** 32;

const Session = union(enum) {
    direct: seeded.Session,
    staged: staged.Session,
    multi: multi.Session,
    fn base(self: *Session) *seeded.Session {
        return switch (self.*) {
            .direct => |*value| value,
            .staged => |*value| &value.base,
            .multi => |*value| &value.base,
        };
    }
    fn deinit(self: *Session) void {
        switch (self.*) {
            inline else => |*value| value.deinit(),
        }
    }
};

const Harness = struct {
    session: *Session,
    cb: *const ops.ComputeBackend,
    inputs: [3]interpreter.RuntimeInput,
    cotangent: ops.CT,
};

fn backward(cb: *const ops.ComputeBackend, tape: *seeded.Tape, cotangent: ops.CT) !void {
    try tape.sealDecisions(decisions);
    var gradients = try tape.backward(tape.identity, decisions, 28, &.{cotangent}, null);
    defer gradients.deinit(cb);
    try std.testing.expectEqual(@as(usize, 1), gradients.gradients.outputs.len);
}

fn attempt(a: Allocator, harness: Harness) !void {
    const base = harness.session.base();
    const original = base.allocator;
    const original_execution = base.resident.?.allocator;
    base.allocator = a;
    base.resident.?.allocator = a;
    // Compiled graph ownership and Metal buffers use their original owners.
    // Runtime input vectors/capture arrays/gradient results use the failing
    // allocator and must all be released before restoring those owners.
    defer {
        std.debug.assert(!base.active_tape);
        base.allocator = original;
        base.resident.?.allocator = original_execution;
    }
    switch (harness.session.*) {
        .direct => |*session| {
            var tape = try session.forward(harness.cb, &harness.inputs, identity, null);
            defer tape.deinit();
            try backward(harness.cb, &tape, harness.cotangent);
        },
        .staged => |*session| {
            var prefix = try session.forwardPrefix(harness.cb, harness.inputs[0..1], identity, null);
            defer prefix.deinit();
            var tape = try prefix.forwardSuffix(identity, .{0x18} ** 32, harness.inputs[1..], null);
            defer tape.deinit();
            try backward(harness.cb, &tape, harness.cotangent);
        },
        .multi => |*session| {
            var stages = try session.forward(harness.cb, harness.inputs[0..1], identity, null);
            defer stages.deinit();
            try stages.advance(identity, .{0x18} ** 32, harness.inputs[1..2], null);
            try stages.advance(identity, .{0x39} ** 32, harness.inputs[2..3], null);
            var tape = try stages.finish();
            defer tape.deinit();
            try backward(harness.cb, &tape, harness.cotangent);
        },
    }
}

test "resident session Metal runtime allocation failures preserve direct and staged binding leases" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try fixture.Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const x = try builder.parameter("x", ml.Shape.init(.f32, &.{3}));
    const pool = try builder.parameter("pool", ml.Shape.init(.i32, &.{2}));
    const multiplier = try builder.parameter("multiplier", ml.Shape.init(.f32, &.{2}));
    const first = try builder.mul(x, x);
    const second = try builder.gather(first, pool, ml.Shape.init(.f32, &.{2}));
    const output = try builder.mul(second, multiplier);
    const seed = ml.autodiff.Seed{ .output = output, .cotangent = try builder.parameter("cotangent", ml.Shape.init(.f32, &.{2})) };
    try graph.markOutput(output);
    const input_x = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 2, 3 }, .shape = &.{3} } }, .{});
    defer cb.free(input_x);
    const input_pool = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &.{ 2, 0 }, .shape = &.{2} } }, .{});
    defer cb.free(input_pool);
    const input_multiplier = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 4, 5 }, .shape = &.{2} } }, .{});
    defer cb.free(input_multiplier);
    const cotangent = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 0.5, 2 }, .shape = &.{2} } }, .{});
    defer cb.free(cotangent);
    const settings = seeded.Options{ .execution = .resident_metal, .gradient = .{ .require_all_gradients = true } };
    for (0..3) |profile| {
        errdefer std.debug.print("resident session runtime OOM profile{d}\n", .{profile});
        var session: Session = switch (profile) {
            0 => .{ .direct = try seeded.Session.init(a, &graph, &.{seed}, &.{x}, settings) },
            1 => .{ .staged = try staged.Session.init(a, &graph, &.{seed}, &.{x}, .{ .deferred_parameters = &.{ pool, multiplier }, .outputs = &.{first} }, settings) },
            2 => .{ .multi = try multi.Session.init(a, &graph, &.{seed}, &.{x}, &.{
                .{ .deferred_parameters = &.{pool}, .outputs = &.{first} },
                .{ .deferred_parameters = &.{multiplier}, .outputs = &.{second} },
            }, settings) },
            else => unreachable,
        };
        defer session.deinit();
        const harness = Harness{ .session = &session, .cb = &cb, .inputs = .{ .{ .node_id = x, .value = input_x }, .{ .node_id = pool, .value = input_pool }, .{ .node_id = multiplier, .value = input_multiplier } }, .cotangent = cotangent };
        try std.testing.checkAllAllocationFailures(a, attempt, .{harness});
        try std.testing.expect(!session.base().active_tape);
        try fixture.expectValues(a, &cb, input_x, &.{ 1, 2, 3 }, 0, 0, "OOM original input");
        try fixture.expectValues(a, &cb, input_multiplier, &.{ 4, 5 }, 0, 0, "OOM deferred input");
        // Recovery exercises the same admitted programs after every failed
        // ownership transfer, without rebuilding or re-uploading parameters.
        try attempt(a, harness);
    }
}
