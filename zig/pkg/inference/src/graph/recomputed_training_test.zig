// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ml = @import("ml").graph;
const recomputed = @import("recomputed_training.zig");
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;
const Id = ml.NodeId;
const identity = seeded.StepIdentity{ .binding = @splat(31), .optimizer_step = 2, .microbatch = 5 };
const decisions: [32]u8 = @splat(53);

const Example = struct {
    graph: ml.Graph,
    x: Id,
    embedding: Id,
    relative: Id,
    shared: Id,
    head: Id,
    unused: Id,
    masks: [2]Id,
    prelude: [2]Id,
    layer1_inputs: [2]Id,
    layer1_output: [1]Id,
    layer2_inputs: [2]Id,
    layer2_output: [1]Id,
    output: Id,
    seed: Id,

    fn init(a: A) !Example {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const shape = ml.Shape.init(.f32, &.{2});
        const x = try b.parameter("tokens", shape);
        const embedding = try b.parameter("encoder.embedding", shape);
        const relative = try b.parameter("encoder.relative", shape);
        const shared = try b.parameter("encoder.shared.weight", shape);
        const head = try b.parameter("head.weight", shape);
        const unused = try b.parameter("inactive.adapter", shape);
        const mask0 = try b.parameter("dropout.layer.0", shape);
        const mask1 = try b.parameter("dropout.layer.1", shape);
        const e0 = try b.mul(x, embedding);
        const r = try b.mul(relative, relative);
        const e1 = try b.mul(try b.geluExact(try b.add(try b.mul(e0, shared), r)), mask0);
        const pre2 = try b.add(try b.mul(e1, shared), r);
        const e2 = try b.mul(try b.mul(pre2, pre2), mask1);
        const output = try b.mul(e2, head);
        const seed = try b.parameter("head.cotangent", shape);
        try graph.markOutput(output);
        return .{ .graph = graph, .x = x, .embedding = embedding, .relative = relative, .shared = shared, .head = head, .unused = unused, .masks = .{ mask0, mask1 }, .prelude = .{ e0, r }, .layer1_inputs = .{ e0, r }, .layer1_output = .{e1}, .layer2_inputs = .{ e1, r }, .layer2_output = .{e2}, .output = output, .seed = seed };
    }
    fn regions(self: *const Example) [3]recomputed.RegionSpec {
        return .{ .{ .key = 10, .inputs = &.{}, .outputs = &self.prelude }, .{ .key = 20, .inputs = &self.layer1_inputs, .outputs = &self.layer1_output }, .{ .key = 30, .inputs = &self.layer2_inputs, .outputs = &self.layer2_output } };
    }
    fn selected(self: *const Example) [4]Id {
        return .{ self.embedding, self.relative, self.shared, self.unused };
    }
    fn replayInputs(self: *const Example) [2]recomputed.ReplayInput {
        return .{ .{ .node = self.masks[0], .key = @splat(1) }, .{ .node = self.masks[1], .key = @splat(2) } };
    }
};

const Recipe = struct {
    fingerprint: [32]u8 = @splat(77),
    calls: [2]usize = @splat(0),
    reject: bool = false,
    fail_call: ?usize = null,
    fn validate(raw: ?*const anyopaque, expected: [32]u8) !void {
        const self: *const Recipe = @ptrCast(@alignCast(raw.?));
        if (self.reject or !std.mem.eql(u8, &self.fingerprint, &expected)) return error.ReplayRecipeChanged;
    }
    fn materialize(raw: ?*const anyopaque, _: A, cb: *const ops.ComputeBackend, input: recomputed.ReplayInput, shape: ml.Shape, step: seeded.StepIdentity, control: ?Control) !ops.CT {
        const self: *Recipe = @ptrCast(@alignCast(@constCast(raw.?)));
        if (control) |active| try active.check();
        try std.testing.expect(std.meta.eql(step, identity));
        try std.testing.expect(std.meta.eql(shape, ml.Shape.init(.f32, &.{2})));
        const index: usize = input.key[0] - 1;
        if (index >= 2) return error.UnexpectedRecipe;
        self.calls[index] += 1;
        if (self.fail_call) |at| if (self.calls[0] + self.calls[1] == at) return error.ReplayUploadRejected;
        return cb.fromFloat32Shape(if (index == 0) &.{ 1.25, 0 } else &.{ 0.75, 1.5 }, &.{2});
    }
    fn source(self: *Recipe) recomputed.ReplaySource {
        return .{ .context = self, .fingerprint = self.fingerprint, .validate = validate, .materialize = materialize };
    }
};

fn expectTensor(cb: *const ops.ComputeBackend, a: A, expected: ops.CT, actual: ops.CT) !void {
    const lhs = try cb.toFloat32(expected, a);
    defer a.free(lhs);
    const rhs = try cb.toFloat32(actual, a);
    defer a.free(rhs);
    try std.testing.expectEqual(lhs.len, rhs.len);
    for (lhs, rhs) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
}
fn gradient(result: seeded.BackwardResult, id: Id) !ops.CT {
    const index = std.mem.indexOfScalar(Id, result.parameter_ids, id) orelse return error.MissingExpectedGradient;
    return result.gradients.outputs[index];
}

fn comparison(a: A) !void {
    var example = try Example.init(a);
    defer example.graph.deinit();
    const specs = example.regions();
    const selected = example.selected();
    const recipes = example.replayInputs();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .managed_parameters = selected[0..3], .replay_inputs = &recipes }, .{});
    defer plan.deinit();
    try std.testing.expect(plan.needsBackward());
    try std.testing.expect(plan.requiresInput(example.embedding));
    try std.testing.expect(!plan.requiresInput(example.masks[0]));
    try std.testing.expect(!plan.requiresInput(example.head));
    try std.testing.expectEqual(@as(usize, 6), plan.inputDescriptors().len);
    var cut = try plan.cutHead(a, &.{example.output});
    defer cut.deinit();
    var head_builder = ml.Builder.init(&cut.graph);
    const head_seed = try head_builder.parameter("head.seed", ml.Shape.init(.f32, &.{2}));
    const final_id = cut.id_map[example.layer2_output[0]];
    const head_id = cut.id_map[example.head];
    var head = try seeded.Session.init(a, &cut.graph, &.{.{ .output = cut.id_map[example.output], .cotangent = head_seed }}, &.{ final_id, head_id }, .{});
    defer head.deinit();
    _ = try plan.sealAdmission(.{ .fixed_host_bytes = 24, .head_tape_bytes = head.tape_bytes, .head_local_bytes = 4096, .head_gradient_bytes = 16, .optimizer_transaction_backend_bytes = 1024 });
    var full = try seeded.Session.init(a, &example.graph, &.{.{ .output = example.output, .cotangent = example.seed }}, &.{ example.embedding, example.relative, example.shared, example.head }, .{});
    defer full.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const ids = [_]Id{ example.x, example.embedding, example.relative, example.shared, example.head, example.masks[0], example.masks[1] };
    const data = [_][2]f32{ .{ 0.8, -0.4 }, .{ 0.5, 0.3 }, .{ -0.2, 0.6 }, .{ 0.7, -0.25 }, .{ 1.1, -0.75 }, .{ 1.25, 0 }, .{ 0.75, 1.5 } };
    var bindings: [ids.len]interpreter.RuntimeInput = undefined;
    var created: usize = 0;
    defer for (bindings[0..created]) |input| cb.free(input.value);
    for (ids, data, &bindings) |id, values, *input| {
        input.* = .{ .node_id = id, .value = try cb.fromFloat32Shape(&values, &.{2}) };
        created += 1;
    }
    const dy = try cb.fromFloat32Shape(&.{ 0.6, -0.3 }, &.{2});
    defer cb.free(dy);
    var original = try full.forward(&cb, &bindings, identity, null);
    defer original.deinit();
    var recipe = Recipe{};
    var tape = try plan.forward(&cb, bindings[0..4], recipe.source(), identity, null);
    defer tape.deinit();
    try std.testing.expectEqualSlices(usize, &.{ 1, 1 }, &recipe.calls);
    try std.testing.expectError(error.TrainingTapeStillLive, plan.advanceParameterEpoch());
    var head_tape = try head.forward(&cb, &.{ .{ .node_id = final_id, .value = try tape.finalOutput() }, .{ .node_id = head_id, .value = bindings[4].value } }, identity, null);
    defer head_tape.deinit();
    try expectTensor(&cb, a, try original.logits(0), try head_tape.logits(0));
    try head_tape.sealDecisions(decisions);
    try original.sealDecisions(decisions);
    try tape.sealDecisions(decisions);
    var head_result = try head_tape.backward(identity, decisions, 1.25, &.{dy}, null);
    defer head_result.deinit(&cb);
    var result = try tape.backward(identity, decisions, 1.25, try gradient(head_result, final_id), null);
    defer result.deinit(&cb);
    var reference = try original.backward(identity, decisions, 1.25, &.{dy}, null);
    defer reference.deinit(&cb);
    try std.testing.expectEqualSlices(usize, &.{ 2, 2 }, &recipe.calls);
    try std.testing.expectEqualSlices(Id, selected[0..3], result.parameter_ids);
    for (selected[0..3]) |id| try expectTensor(&cb, a, try gradient(reference, id), try gradient(result, id));
    try expectTensor(&cb, a, try gradient(reference, example.head), try gradient(head_result, head_id));
    try std.testing.expect(!plan.active_tape);
    try plan.advanceParameterEpoch();
    try std.testing.expectEqual(@as(u64, 1), plan.parameter_epoch);
}

test "recomputed training two encoder layers share relative cotangents and preserve a single detached head" {
    try comparison(std.testing.allocator);
}

fn construction(a: A) !void {
    var example = try Example.init(a);
    defer example.graph.deinit();
    const specs = example.regions();
    const selected = example.selected();
    const recipes = example.replayInputs();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .managed_parameters = selected[0..3], .replay_inputs = &recipes }, .{});
    defer plan.deinit();
    _ = try plan.sealAdmission(.{ .fixed_host_bytes = 24, .optimizer_transaction_backend_bytes = 1024 });
}

test "recomputed training compilation has bounded ownership at every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, construction, .{});
}

test "recomputed training admission charges checkpoints compilation head and complete optimizer before forward" {
    const a = std.testing.allocator;
    var example = try Example.init(a);
    defer example.graph.deinit();
    const specs = example.regions();
    const selected = example.selected();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .managed_parameters = selected[0..3] }, .{});
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 32), plan.regional_admission.checkpoint_bytes);
    try std.testing.expectEqual(@as(usize, 24), plan.regional_admission.managed_parameter_bytes);
    try std.testing.expectEqual(@as(usize, 24), plan.regional_admission.immutable_binding_bytes);
    try std.testing.expect(plan.regional_admission.work > 0);
    try std.testing.expect(plan.regional_admission.largest_region_tape_bytes > 0);
    try std.testing.expectError(error.InvalidRecomputeAdmission, plan.sealAdmission(.{ .fixed_host_bytes = 23 }));
    try std.testing.expectError(error.RecomputeLimitExceeded, plan.sealAdmission(.{ .fixed_host_bytes = 24, .optimizer_transaction_backend_bytes = std.math.maxInt(usize) }));
    try std.testing.expect(plan.admission == null);
    const admitted = try plan.sealAdmission(.{ .fixed_host_bytes = 24, .head_tape_bytes = 1000, .head_local_bytes = 100_000, .head_gradient_bytes = 64, .head_compile_bytes = 100, .head_work = 200, .optimizer_transaction_backend_bytes = 512, .optimizer_transaction_host_bytes = 128, .optimizer_transaction_work = 300 });
    try std.testing.expectEqual(plan.regional_admission.work + 500, admitted.work);
    try std.testing.expectEqual(plan.regional_admission.compile_upper_bound_bytes + 100, admitted.compile_upper_bound_bytes);
    try std.testing.expect(admitted.backend_upper_bound_bytes >= 101_576);
    try std.testing.expectError(error.RecomputeAdmissionAlreadySealed, plan.sealAdmission(.{}));
    try std.testing.expectError(error.RecomputeLimitExceeded, recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{}, .{ .limits = .{ .max_checkpoint_bytes = 31 } }));
    try std.testing.expectError(error.RecomputeLimitExceeded, recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{}, .{ .limits = .{ .max_plan_host_bytes = 1 } }));
    var malformed = specs;
    malformed[2].inputs = &.{example.layer1_output[0]};
    try std.testing.expectError(error.UndeclaredRecomputeDependency, recomputed.Plan.init(a, &example.graph, &malformed, example.layer2_output[0], &selected, .{}, .{}));
    try std.testing.expectError(error.UndeclaredRecomputeDependency, plan.cutHead(a, &.{ example.output, example.prelude[0] }));
}

fn cycle(plan: *recomputed.Plan, cb: *const ops.ComputeBackend, bindings: []const interpreter.RuntimeInput, dy: ops.CT, recipe: *Recipe) !void {
    var tape = try plan.forward(cb, bindings, recipe.source(), identity, null);
    defer tape.deinit();
    try tape.sealDecisions(decisions);
    var result = try tape.backward(identity, decisions, 1, dy, null);
    defer result.deinit(cb);
    try std.testing.expectEqual(@as(usize, 3), result.parameter_ids.len);
}

fn runtimeFailure(fail_offset: ?usize) !usize {
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const a = failure.allocator();
    var example = try Example.init(a);
    defer example.graph.deinit();
    const specs = example.regions();
    const selected = example.selected();
    const recipes = example.replayInputs();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .replay_inputs = &recipes }, .{});
    defer plan.deinit();
    _ = try plan.sealAdmission(.{});
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var bindings: [4]interpreter.RuntimeInput = undefined;
    var created: usize = 0;
    defer for (bindings[0..created]) |input| cb.free(input.value);
    for ([_]Id{ example.x, example.embedding, example.relative, example.shared }, &bindings) |id, *input| {
        input.* = .{ .node_id = id, .value = try cb.fromFloat32Shape(&.{ 0.2, 0.7 }, &.{2}) };
        created += 1;
    }
    const dy = try cb.fromFloat32Shape(&.{ 0.5, -0.3 }, &.{2});
    defer cb.free(dy);
    const base_live = plan.budget.live;
    const start = failure.alloc_index;
    if (fail_offset) |offset| failure.fail_index = start + offset;
    var recipe = Recipe{};
    cycle(&plan, &cb, &bindings, dy, &recipe) catch |err| {
        // Only the real backing allocator failed. A declared budget denial
        // would be a different contract and must not be inferred from OOM.
        try std.testing.expect(fail_offset != null and failure.has_induced_failure);
        try std.testing.expectEqual(error.OutOfMemory, err);
    };
    const allocations = failure.alloc_index - start;
    failure.fail_index = std.math.maxInt(usize);
    try std.testing.expect(!plan.active_tape);
    try std.testing.expectEqual(@as(u64, 0), plan.parameter_epoch);
    try std.testing.expectEqual(base_live, plan.budget.live);
    for (bindings) |input| {
        const data = try cb.toFloat32(input.value, a);
        defer a.free(data);
        try std.testing.expectEqualSlices(f32, &.{ 0.2, 0.7 }, data);
    }
    var retry = Recipe{};
    try cycle(&plan, &cb, &bindings, dy, &retry);
    try std.testing.expectEqualSlices(usize, &.{ 2, 2 }, &retry.calls);
    return allocations;
}

test "recomputed training runtime allocation failure consumes tapes preserves inputs and retries" {
    const allocations = try runtimeFailure(null);
    try std.testing.expect(allocations > 50 and allocations < 4096);
    for (0..allocations) |index| _ = try runtimeFailure(index);
}

const Cancellation = struct {
    cancelled: bool = false,
    fn check(raw: ?*anyopaque) !void {
        const self: *const Cancellation = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }
    fn control(self: *Cancellation) Control {
        return .{ .ptr = self, .check_fn = check };
    }
};

test "recomputed training seals identity recipes and both controls with no-gradient cleanup" {
    const a = std.testing.allocator;
    var example = try Example.init(a);
    defer example.graph.deinit();
    const specs = example.regions();
    const selected = example.selected();
    const recipes = example.replayInputs();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .replay_inputs = &recipes }, .{});
    defer plan.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var recipe = Recipe{};
    try std.testing.expectError(error.RecomputeAdmissionNotSealed, plan.forward(&cb, &.{}, recipe.source(), identity, null));
    try std.testing.expectEqualSlices(usize, &.{ 0, 0 }, &recipe.calls);
    _ = try plan.sealAdmission(.{});
    try std.testing.expectError(error.MissingRecomputeBinding, plan.forward(&cb, &.{}, recipe.source(), identity, null));
    const value = try cb.fromFloat32Shape(&.{ 0.2, 0.7 }, &.{2});
    defer cb.free(value);
    const bindings = [_]interpreter.RuntimeInput{ .{ .node_id = example.x, .value = value }, .{ .node_id = example.embedding, .value = value }, .{ .node_id = example.relative, .value = value }, .{ .node_id = example.shared, .value = value } };
    var original = Cancellation{};
    var request = Cancellation{};
    cb.execution_control = original.control();
    original.cancelled = true;
    try std.testing.expectError(error.Cancelled, plan.forward(&cb, &bindings, recipe.source(), identity, request.control()));
    original.cancelled = false;
    request.cancelled = true;
    try std.testing.expectError(error.Cancelled, plan.forward(&cb, &bindings, recipe.source(), identity, request.control()));
    request.cancelled = false;
    const base_live = plan.budget.live;
    const declared = plan.budget.limit;
    plan.budget.limit = base_live;
    try std.testing.expectError(error.RecomputeLimitExceeded, plan.forward(&cb, &bindings, recipe.source(), identity, null));
    try std.testing.expectEqual(base_live, plan.budget.live);
    try std.testing.expect(!plan.active_tape);
    plan.budget.limit = declared;
    plan.parameter_epoch = 1;
    try std.testing.expectError(error.TrainingTapeIdentityMismatch, plan.forward(&cb, &bindings, recipe.source(), identity, null));
    plan.parameter_epoch = 0;
    for (0..5) |scenario| {
        var tape = try plan.forward(&cb, &bindings, recipe.source(), identity, request.control());
        defer tape.deinit();
        try tape.sealDecisions(decisions);
        switch (scenario) {
            0 => {
                var wrong = identity;
                wrong.microbatch += 1;
                try std.testing.expectError(error.TrainingTapeIdentityMismatch, tape.backward(wrong, decisions, 1, value, request.control()));
            },
            1 => try std.testing.expectError(error.TrainingDecisionIdentityMismatch, tape.backward(identity, @splat(9), 1, value, request.control())),
            2 => {
                recipe.reject = true;
                try std.testing.expectError(error.ReplayRecipeChanged, tape.backward(identity, decisions, 1, value, request.control()));
                recipe.reject = false;
            },
            3 => {
                original.cancelled = true;
                try std.testing.expectError(error.Cancelled, tape.backward(identity, decisions, 1, value, request.control()));
                original.cancelled = false;
            },
            4 => {
                const before = recipe.calls;
                var absent = try tape.backward(identity, decisions, 1, null, request.control());
                defer absent.deinit(&cb);
                try std.testing.expectEqual(@as(usize, 0), absent.parameter_ids.len);
                try std.testing.expectEqualSlices(usize, &before, &recipe.calls);
            },
            else => unreachable,
        }
        try std.testing.expect(!plan.active_tape);
        try std.testing.expectEqual(base_live, plan.budget.live);
    }
    // Failing the second initial region upload owns and releases the first
    // checkpoint and mask; a later identical step can still run to completion.
    recipe = .{ .fail_call = 2 };
    try std.testing.expectError(error.ReplayUploadRejected, plan.forward(&cb, &bindings, recipe.source(), identity, null));
    try std.testing.expectEqual(base_live, plan.budget.live);
    recipe = .{};
    try cycle(&plan, &cb, &bindings, value, &recipe);
}

test "recomputed training replay summary counts region occurrences and excludes absent reverse passes" {
    const a = std.testing.allocator;
    var example = try Example.init(a);
    defer example.graph.deinit();
    // One immutable logical mask is deliberately reused by two regions.
    example.graph.nodes.items[example.layer2_output[0]].inputs[1] = example.masks[0];
    const specs = example.regions();
    const selected = example.selected();
    const recipes = example.replayInputs();
    var plan = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &selected, .{ .replay_inputs = recipes[0..1] }, .{});
    defer plan.deinit();
    const summary = try plan.replaySummary();
    try std.testing.expectEqual(@as(usize, 3), summary.initial_regions);
    try std.testing.expectEqual(@as(usize, 3), summary.replay_regions);
    try std.testing.expectEqual(@as(usize, 2), summary.initial_calls);
    try std.testing.expectEqual(@as(usize, 2), summary.replay_calls);
    try std.testing.expectEqual(@as(usize, 16), summary.initial_upload_bytes);
    try std.testing.expectEqual(@as(usize, 16), summary.replay_upload_bytes);
    try std.testing.expectEqual(@as(usize, 8), summary.largest_input_bytes);
    try std.testing.expectEqual(@as(usize, 8), summary.largest_live_region_bytes);
    try std.testing.expectEqual(@as(usize, 15), summary.source_validation_calls_upper_bound);
    var frozen = try recomputed.Plan.init(a, &example.graph, &specs, example.layer2_output[0], &.{}, .{ .replay_inputs = recipes[0..1] }, .{});
    defer frozen.deinit();
    const no_reverse = try frozen.replaySummary();
    try std.testing.expectEqual(@as(usize, 2), no_reverse.initial_calls);
    try std.testing.expectEqual(@as(usize, 0), no_reverse.replay_calls);
    try std.testing.expectEqual(@as(usize, 0), no_reverse.replay_regions);
    try std.testing.expectEqual(@as(usize, 8), no_reverse.source_validation_calls_upper_bound);
}

test "recomputed training auxiliary stopped and predicate parameters remain absent instead of zero" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{2});
    const x = try b.parameter("x", shape);
    const weight = try b.parameter("weight", shape);
    const stopped = try b.parameter("stopped", shape);
    const unused = try b.parameter("auxiliary.only", shape);
    const predicate = try b.parameter("predicate", shape);
    const e0 = try b.mul(x, x);
    const r0 = try b.mul(stopped, stopped);
    const orphan = try b.mul(unused, unused);
    const scaled = try b.mul(e0, weight);
    const choice = try graph.addNode(.{ .op = .where_select, .output_shape = shape, .inputs = .{ predicate, scaled, e0, ml.null_node }, .num_inputs = 3 });
    const final = try b.add(choice, try b.stopGradient(r0));
    const seed = try b.parameter("seed", shape);
    const selected = [_]Id{ x, weight, stopped, unused, predicate };
    const specs = [_]recomputed.RegionSpec{
        .{ .key = 0, .inputs = &.{}, .outputs = &.{ e0, r0, orphan } },
        .{ .key = 1, .inputs = &.{ e0, r0 }, .outputs = &.{final} },
    };
    var plan = try recomputed.Plan.init(a, &graph, &specs, final, &selected, .{}, .{});
    defer plan.deinit();
    _ = try plan.sealAdmission(.{});
    try std.testing.expectEqualSlices(Id, &.{ x, weight }, plan.parameters);
    var full = try seeded.Session.init(a, &graph, &.{.{ .output = final, .cotangent = seed }}, &selected, .{ .gradient = .{ .require_all_gradients = false } });
    defer full.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    const data = [_][2]f32{ .{ 0.2, 0.7 }, .{ 1, 2 }, .{ -3, 0.5 }, .{ 9, 9 }, .{ 1, 0 } };
    var bindings: [5]interpreter.RuntimeInput = undefined;
    var created: usize = 0;
    defer for (bindings[0..created]) |input| cb.free(input.value);
    for (selected, data, &bindings) |id, values, *input| {
        input.* = .{ .node_id = id, .value = try cb.fromFloat32Shape(&values, &.{2}) };
        created += 1;
    }
    const dy = try cb.fromFloat32Shape(&.{ 0.25, -0.75 }, &.{2});
    defer cb.free(dy);
    var tape = try plan.forward(&cb, &bindings, .{}, identity, null);
    defer tape.deinit();
    // Whole-graph execution cannot bind a leaf that is absent from its live
    // forward. Keep that inactive slot in wrt, just as the normal trainer does.
    var reference_inputs: [4]interpreter.RuntimeInput = undefined;
    var index: usize = 0;
    for (bindings) |input| if (input.node_id != unused) {
        reference_inputs[index] = input;
        index += 1;
    };
    var reference_tape = try full.forward(&cb, &reference_inputs, identity, null);
    defer reference_tape.deinit();
    try expectTensor(&cb, a, try reference_tape.logits(0), try tape.finalOutput());
    try tape.sealDecisions(decisions);
    try reference_tape.sealDecisions(decisions);
    var actual = try tape.backward(identity, decisions, 1, dy, null);
    defer actual.deinit(&cb);
    var expected = try reference_tape.backward(identity, decisions, 1, &.{dy}, null);
    defer expected.deinit(&cb);
    try std.testing.expectEqualSlices(Id, expected.parameter_ids, actual.parameter_ids);
    try std.testing.expectEqualSlices(Id, &.{ x, weight }, actual.parameter_ids);
    for (actual.parameter_ids) |id| try expectTensor(&cb, a, try gradient(expected, id), try gradient(actual, id));
}
