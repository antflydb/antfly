// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Join the independently differentiated head and encoder regions in the
//! original graph's optimizer namespace. Inputs remain borrowed and immutable;
//! every returned tensor has its own lease or owned sum. No tensor payload is
//! downloaded, and an absent selected gradient never becomes an implicit zero.
const std = @import("std");
const ml = @import("ml").graph;
const graph = @import("gliner_boundary_recomputed_graph.zig");
const seeded = @import("../graph/seeded_training.zig");
const resident = @import("../graph/resident_training_execution.zig");
const ops = @import("../ops/ops.zig");
const primitive_ops = @import("../ops/resident_training_ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const Shape = ml.Shape;
const nil = ml.null_node;
const max_selected: usize = 65536;

pub const Admission = struct {
    parameters: usize = 0,
    shared_parameters: usize = 0,
    /// Both input results remain live until merge returns. These diagnostic
    /// bytes are ALREADY charged by head gradients and regional accumulators.
    head_gradient_bytes: usize = 0,
    encoder_gradient_bytes: usize = 0,
    borrowed_gradient_bytes: usize = 0,
    result_tensor_bytes: usize = 0,
    shared_sum_bytes: usize = 0,
    native_lease_copy_bytes: usize = 0,
    native_operand_scratch_bytes: usize = 0,
    /// Additional logical buffers only: every possible shared sum, native
    /// lease-copy fallback, and two largest native add operand materializations.
    /// The pending output is counted while both borrowed inputs remain live.
    backend_upper_bound_bytes: usize = 0,
    /// Routing, result vectors and checked tensor-shape metadata. Opaque
    /// backend handle/driver owners remain under the enclosing owner budgets.
    host_upper_bound_bytes: usize = 0,
    total_work: u64 = 0,
};

fn add(left: usize, right: usize) !usize {
    return std.math.add(usize, left, right) catch error.RecomputeLimitExceeded;
}
fn mul(left: usize, right: usize) !usize {
    return std.math.mul(usize, left, right) catch error.RecomputeLimitExceeded;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn bytes(shape: Shape, primitive: primitive_ops.Limits) !usize {
    if (shape.dtype != .f32 or shape.rank_ > 8) return error.InvalidRecomputeGradient;
    for (shape.bounds[0..shape.rank_]) |bound| if (bound != 0) return error.InvalidRecomputeGradient;
    return mul(try primitive_ops.shapeElements(i64, shape.dims[0..shape.rank_], primitive), 4);
}
fn equalShape(left: Shape, right: Shape) bool {
    return left.rank_ <= 8 and right.rank_ <= 8 and left.eq(right) and
        std.mem.eql(i64, left.bounds[0..left.rank_], right.bounds[0..right.rank_]);
}
fn validateBuilt(built: *const graph.Built) !void {
    const source = built.regional.source;
    if (source.nodeCount() == 0 or source.nodeCount() > 1_000_000 or
        built.head.graph.nodeCount() > 1_000_000 + 16384 or
        built.head.id_map.len != source.nodeCount() or built.original_ids.len > built.head.graph.nodeCount() or
        built.parameters.len > max_selected + 1 or built.regional.parameters.len > max_selected or
        built.regional.parameters.len != built.regional.parameter_shapes.len or
        built.regional.final_slot >= built.regional.checkpoints.len or
        built.final_hidden >= built.original_ids.len or built.head.graph.node(built.final_hidden).op != .parameter)
        return error.InvalidRecomputeGradient;
    const boundary = built.regional.checkpoints[built.regional.final_slot];
    if (boundary.node >= source.nodeCount() or source.node(boundary.node).op == .parameter or
        built.original_ids[built.final_hidden] != boundary.node or built.head.id_map[boundary.node] != built.final_hidden or
        !equalShape(boundary.shape, source.node(boundary.node).output_shape) or
        !equalShape(boundary.shape, built.head.graph.node(built.final_hidden).output_shape)) return error.InvalidRecomputeGradient;
}
fn originalForHead(built: *const graph.Built, local: Id) !Id {
    if (local == built.final_hidden or local >= built.original_ids.len or built.head.graph.node(local).op != .parameter)
        return error.InvalidRecomputeGradient;
    const original = built.original_ids[local];
    if (original >= built.regional.source.nodeCount() or built.regional.source.node(original).op != .parameter or
        built.head.id_map[original] != local or
        !equalShape(built.head.graph.node(local).output_shape, built.regional.source.node(original).output_shape))
        return error.InvalidRecomputeGradient;
    return original;
}
fn validateResult(result: *const seeded.BackwardResult, maximum: usize) !void {
    if (result.parameter_ids.len != result.gradients.outputs.len or result.parameter_ids.len > maximum)
        return error.InvalidRecomputeGradient;
    if (!std.math.isFinite(result.loss)) return error.InvalidRecomputeLoss;
}

/// Borrowed dEL, valid only while head remains alive. Seeded sessions return
/// the selected-parameter order with disconnected entries omitted. Validate
/// that subsequence, including duplicate/unselected IDs, before any replay.
/// Actual tensor dtype/shape validation occurs in regional.backward and merge.
pub fn hiddenCotangent(built: *const graph.Built, head: *const seeded.BackwardResult) !?ops.CT {
    try validateBuilt(built);
    try validateResult(head, built.parameters.len);
    var cursor: usize = 0;
    var hidden: ?ops.CT = null;
    for (head.parameter_ids, head.gradients.outputs) |local, tensor| {
        while (cursor < built.parameters.len and built.parameters[cursor] != local) cursor += 1;
        if (cursor == built.parameters.len) return error.InvalidRecomputeGradient;
        cursor += 1;
        if (local == built.final_hidden) {
            if (hidden != null or !built.regional.needsBackward()) return error.InvalidRecomputeGradient;
            hidden = tensor;
        } else _ = try originalForHead(built, local);
    }
    return hidden;
}

const Route = struct {
    original: Id,
    shape: Shape,
    head_local: ?Id = null,
    encoder_selected: bool = false,
    head_tensor: ?ops.CT = null,
    encoder_tensor: ?ops.CT = null,
    fn less(_: void, left: Route, right: Route) bool {
        return left.original < right.original;
    }
};
const Prepared = struct {
    allocator: Allocator,
    storage: []Route,
    routes: []Route,
    estimate: Admission,
    fn deinit(self: *Prepared) void {
        self.allocator.free(self.storage);
    }
    fn find(self: *Prepared, original: Id) ?*Route {
        var start: usize = 0;
        var end = self.routes.len;
        while (start < end) {
            const middle = start + (end - start) / 2;
            const value = &self.routes[middle];
            if (value.original == original) return value;
            if (value.original < original) start = middle + 1 else end = middle;
        }
        return null;
    }
};

fn prepare(a: Allocator, built: *const graph.Built, execution: seeded.Execution, primitive: primitive_ops.Limits, control: ?Control) !Prepared {
    try check(control);
    try validateBuilt(built);
    if (execution != built.regional.options.session.execution) return error.UnsupportedSeededTrainingBackend;
    const capacity = try add(built.parameters.len, built.regional.parameters.len);
    const storage = try a.alloc(Route, capacity);
    errdefer a.free(storage);
    var estimate = Admission{};
    var count: usize = 0;
    var hidden_selected = false;
    for (built.parameters) |local| {
        try check(control);
        if (local >= built.original_ids.len or built.head.graph.node(local).op != .parameter) return error.InvalidRecomputeGradient;
        const shape = built.head.graph.node(local).output_shape;
        estimate.head_gradient_bytes = try add(estimate.head_gradient_bytes, try bytes(shape, primitive));
        if (local == built.final_hidden) {
            if (hidden_selected or !built.regional.needsBackward()) return error.InvalidRecomputeGradient;
            hidden_selected = true;
            continue;
        }
        storage[count] = .{ .original = try originalForHead(built, local), .shape = shape, .head_local = local };
        count += 1;
    }
    if (hidden_selected != built.regional.needsBackward()) return error.InvalidRecomputeGradient;
    for (built.regional.parameters, built.regional.parameter_shapes) |original, shape| {
        try check(control);
        if (original >= built.regional.source.nodeCount() or built.regional.source.node(original).op != .parameter or
            !equalShape(shape, built.regional.source.node(original).output_shape)) return error.InvalidRecomputeGradient;
        estimate.encoder_gradient_bytes = try add(estimate.encoder_gradient_bytes, try bytes(shape, primitive));
        storage[count] = .{ .original = original, .shape = shape, .encoder_selected = true };
        count += 1;
    }
    std.mem.sort(Route, storage[0..count], {}, Route.less);
    var unique: usize = 0;
    for (0..count) |index| {
        try check(control);
        const incoming = storage[index];
        if (unique != 0 and storage[unique - 1].original == incoming.original) {
            const previous = &storage[unique - 1];
            if (!equalShape(previous.shape, incoming.shape) or
                (previous.head_local != null and incoming.head_local != null) or
                (previous.encoder_selected and incoming.encoder_selected)) return error.InvalidRecomputeGradient;
            previous.head_local = previous.head_local orelse incoming.head_local;
            previous.encoder_selected = previous.encoder_selected or incoming.encoder_selected;
        } else {
            storage[unique] = incoming;
            unique += 1;
        }
    }
    if (unique > max_selected) return error.RecomputeLimitExceeded;
    var largest_shared: usize = 0;
    for (storage[0..unique]) |route| {
        try check(control);
        const n = try bytes(route.shape, primitive);
        estimate.result_tensor_bytes = try add(estimate.result_tensor_bytes, n);
        const instruction = if (route.head_local != null and route.encoder_selected)
            ops.resident_program.Instruction{ .op = .add, .output = route.shape, .inputs = .{ route.shape, route.shape, .{}, .{} }, .num_inputs = 2 }
        else
            ops.resident_program.Instruction{ .op = .{ .reshape = .{ .new_shape = route.shape } }, .output = route.shape, .inputs = .{ route.shape, .{}, .{}, .{} }, .num_inputs = 1 };
        const geometry = try instruction.validate(.{ .primitive = primitive });
        estimate.total_work = std.math.add(u64, estimate.total_work, geometry.work_items) catch return error.RecomputeLimitExceeded;
        if (route.head_local != null and route.encoder_selected) {
            estimate.shared_parameters += 1;
            estimate.shared_sum_bytes = try add(estimate.shared_sum_bytes, n);
            largest_shared = @max(largest_shared, n);
        }
    }
    estimate.parameters = unique;
    estimate.borrowed_gradient_bytes = try add(estimate.head_gradient_bytes, estimate.encoder_gradient_bytes);
    if (execution == .native) {
        estimate.native_lease_copy_bytes = estimate.result_tensor_bytes - estimate.shared_sum_bytes;
        estimate.native_operand_scratch_bytes = try mul(largest_shared, 2);
    }
    estimate.backend_upper_bound_bytes = try add(estimate.shared_sum_bytes, try add(estimate.native_lease_copy_bytes, estimate.native_operand_scratch_bytes));
    estimate.host_upper_bound_bytes = try add(try mul(capacity, @sizeOf(Route)), try add(try mul(unique, @sizeOf(Id) + @sizeOf(ops.CT)), 8 * @sizeOf(i64)));
    // Include bounded route sorting/search work separately from numeric work.
    estimate.total_work = std.math.add(u64, estimate.total_work, try mul(capacity, 64)) catch return error.RecomputeLimitExceeded;
    const limits = built.regional.options.limits;
    if (try add(estimate.borrowed_gradient_bytes, estimate.backend_upper_bound_bytes) > limits.max_backend_bytes or
        estimate.host_upper_bound_bytes > limits.max_host_bytes or estimate.total_work > limits.max_total_work)
        return error.RecomputeLimitExceeded;
    try check(control);
    return .{ .allocator = a, .storage = storage, .routes = storage[0..unique], .estimate = estimate };
}

/// Call during complete-plan admission, before encoder/head execution. This
/// upper bound includes every selected route, regardless of later None values.
pub fn admission(a: Allocator, built: *const graph.Built, execution: seeded.Execution, primitive: primitive_ops.Limits, control: ?Control) !Admission {
    var prepared = try prepare(a, built, execution, primitive, control);
    defer prepared.deinit();
    return prepared.estimate;
}

const CombinedControl = struct {
    original: ?Control,
    request: ?Control,
    fn apply(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(self.original);
        try check(self.request);
    }
    fn value(self: *@This()) Control {
        var result = self.request orelse self.original orelse Control{};
        if (self.original) |original| {
            if (result.io == null) result.io = original.io;
            if (result.hard_cancellation == null) result.hard_cancellation = original.hard_cancellation;
            if (original.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        result.ptr = self;
        result.check_fn = apply;
        return result;
    }
};

/// Owns only the returned tensors/vectors. Both input results can be released
/// immediately after success, in either order. On failure they remain intact.
/// Canonical output order is ascending original graph parameter ID. A shared
/// parameter receives head + encoder, in that explicit operand order.
pub fn merge(a: Allocator, backend: *const ops.ComputeBackend, built: *const graph.Built, head: *const seeded.BackwardResult, encoder: *const seeded.BackwardResult, execution: seeded.Execution, primitive: primitive_ops.Limits, control: ?Control) !seeded.BackwardResult {
    var checks = CombinedControl{ .original = backend.execution_control, .request = control };
    const combined = checks.value();
    var cb = backend.*;
    cb.execution_control = combined;
    try cb.checkExecutionControl();
    switch (execution) {
        .native => if (cb.kind() != .native or cb.vtable.reshapeOp == null) return error.UnsupportedSeededTrainingBackend,
        .resident_metal => if (cb.kind() != .metal or cb.vtable.residentTrainingInstruction == null) return error.UnsupportedSeededTrainingBackend,
    }
    var prepared = try prepare(a, built, execution, primitive, combined);
    defer prepared.deinit();
    const hidden = try hiddenCotangent(built, head);
    try validateResult(encoder, built.regional.parameters.len);
    if (@as(u32, @bitCast(head.loss)) != @as(u32, @bitCast(encoder.loss))) return error.InvalidRecomputeLoss;
    if (hidden == null and encoder.parameter_ids.len != 0) return error.UnexpectedRecomputeCotangent;
    const control_bytes = try add(head.control_readback_bytes, encoder.control_readback_bytes);
    for (head.parameter_ids, head.gradients.outputs) |local, tensor| {
        try cb.checkExecutionControl();
        const shape = built.head.graph.node(local).output_shape;
        try seeded.validateTensor(a, &cb, tensor, shape);
        if (local == built.final_hidden) continue;
        const route = prepared.find(try originalForHead(built, local)) orelse return error.InvalidRecomputeGradient;
        if (route.head_local != local or route.head_tensor != null) return error.InvalidRecomputeGradient;
        route.head_tensor = tensor;
    }
    for (encoder.parameter_ids, encoder.gradients.outputs) |original, tensor| {
        try cb.checkExecutionControl();
        const route = prepared.find(original) orelse return error.InvalidRecomputeGradient;
        if (!route.encoder_selected or route.encoder_tensor != null) return error.InvalidRecomputeGradient;
        try seeded.validateTensor(a, &cb, tensor, route.shape);
        route.encoder_tensor = tensor;
    }
    var count: usize = 0;
    for (prepared.routes) |route| if (route.head_tensor != null or route.encoder_tensor != null) {
        count += 1;
    };
    const ids = try a.alloc(Id, count);
    errdefer a.free(ids);
    const outputs = try a.alloc(ops.CT, count);
    errdefer a.free(outputs);
    var made: usize = 0;
    errdefer for (outputs[0..made]) |tensor| cb.free(tensor);
    for (prepared.routes) |route| {
        if (route.head_tensor == null and route.encoder_tensor == null) continue;
        try cb.checkExecutionControl();
        const incoming = route.head_tensor orelse route.encoder_tensor.?;
        const output = if (route.head_tensor != null and route.encoder_tensor != null) sum: {
            const instruction = ops.resident_program.Instruction{ .op = .add, .output = route.shape, .inputs = .{ route.shape, route.shape, .{}, .{} }, .num_inputs = 2 };
            break :sum if (execution == .resident_metal)
                try cb.residentTrainingInstruction(&instruction, &.{ route.head_tensor.?, route.encoder_tensor.? }, .{ .primitive = primitive })
            else
                try cb.add(route.head_tensor.?, route.encoder_tensor.?);
        } else if (execution == .resident_metal)
            try resident.lease(&cb, incoming, route.shape, .{ .program = .{ .instruction = .{ .primitive = primitive } } })
        else
            try cb.primReshape(incoming, route.shape.dims[0..route.shape.rank_]);
        // A backend may share storage through an independently owned handle;
        // returning a borrowed handle itself violates the primitive contract.
        if (output == route.head_tensor or output == route.encoder_tensor) return error.UnexpectedRecomputeGradientAlias;
        ids[made] = route.original;
        outputs[made] = output;
        made += 1;
        try seeded.validateTensor(a, &cb, output, route.shape);
        try cb.checkExecutionControl();
    }
    try cb.checkExecutionControl();
    return .{ .loss = head.loss, .gradients = .{ .outputs = outputs, .allocator = a }, .parameter_ids = ids, .allocator = a, .control_readback_bytes = control_bytes };
}

const Tiny = struct {
    allocator: Allocator,
    source: *ml.Graph,
    built: graph.Built,
    shared: Id,
    encoder_only: Id,
    explicit_zero: Id,
    absent: Id,
    unselected: Id,

    fn init(a: Allocator) !Tiny {
        const source = try a.create(ml.Graph);
        errdefer a.destroy(source);
        source.* = ml.Graph.init(a);
        errdefer source.deinit();
        var b = ml.Builder.init(source);
        const shape = Shape.init(.f32, &.{ 1, 2 });
        const input = try b.parameter("__input", shape);
        const relative_input = try b.parameter("__relative", shape);
        const shared = try b.parameter("shared.weight", shape);
        const encoder_only = try b.parameter("encoder.weight", shape);
        const explicit_zero = try b.parameter("head.zero", shape);
        const absent = try b.parameter("head.absent", shape);
        const unselected = try b.parameter("head.frozen", shape);
        const embedded = try b.mul(input, shared);
        const relative = try b.add(relative_input, relative_input);
        const hidden = try b.mul(try b.add(embedded, relative), encoder_only);
        const output = try b.add(try b.mul(hidden, shared), try b.add(try b.add(explicit_zero, absent), unselected));
        var layers = [_]@import("gliner_boundary_encoder_graph.zig").LayerRegion{.{ .ordinal = 0, .input = embedded, .output = hidden }};
        const built = try graph.build(a, source, .{ .embedding_output = embedded, .normalized_relative = relative, .layers = &layers, .output = hidden }, &.{ absent, encoder_only, shared, explicit_zero }, &.{}, .{ .outputs = &.{output} }, .{});
        return .{ .allocator = a, .source = source, .built = built, .shared = shared, .encoder_only = encoder_only, .explicit_zero = explicit_zero, .absent = absent, .unselected = unselected };
    }
    fn deinit(self: *Tiny) void {
        self.built.deinit();
        self.source.deinit();
        self.allocator.destroy(self.source);
    }
};

fn resultOf(a: Allocator, cb: *const ops.ComputeBackend, ids: []const Id, values: []const [2]f32) !seeded.BackwardResult {
    if (ids.len != values.len) return error.InvalidRecomputeGradient;
    const owned_ids = try a.dupe(Id, ids);
    errdefer a.free(owned_ids);
    const outputs = try a.alloc(ops.CT, ids.len);
    errdefer a.free(outputs);
    var made: usize = 0;
    errdefer for (outputs[0..made]) |tensor| cb.free(tensor);
    for (outputs, values) |*output, value| {
        output.* = try cb.fromFloat32Shape(&value, &.{ 1, 2 });
        made += 1;
    }
    return .{ .loss = 7.25, .gradients = .{ .outputs = outputs, .allocator = a }, .parameter_ids = owned_ids, .allocator = a };
}

fn headOf(a: Allocator, cb: *const ops.ComputeBackend, tiny: *const Tiny) !seeded.BackwardResult {
    return resultOf(a, cb, &.{ tiny.built.headId(tiny.shared), tiny.built.headId(tiny.explicit_zero), tiny.built.final_hidden }, &.{ .{ 1, 2 }, .{ 0, 0 }, .{ 9, 9 } });
}
fn encoderOf(a: Allocator, cb: *const ops.ComputeBackend, tiny: *const Tiny) !seeded.BackwardResult {
    return resultOf(a, cb, &.{ tiny.encoder_only, tiny.shared }, &.{ .{ 3, 4 }, .{ 10, 20 } });
}

fn forbidDownload(_: *anyopaque, _: ops.CT, _: Allocator) anyerror![]f32 {
    return error.UnexpectedRecomputePayloadReadback;
}

fn exerciseMerge(a: Allocator, tiny: *const Tiny) !void {
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var head = try headOf(a, &cb, tiny);
    var head_live = true;
    defer if (head_live) head.deinit(&cb);
    var encoder = try encoderOf(a, &cb, tiny);
    var encoder_live = true;
    defer if (encoder_live) encoder.deinit(&cb);
    try std.testing.expectEqual(head.gradients.outputs[2], (try hiddenCotangent(&tiny.built, &head)).?);
    var guarded_vtable = cb.vtable.*;
    guarded_vtable.toFloat32 = forbidDownload;
    var guarded = cb;
    guarded.vtable = &guarded_vtable;
    var result = try merge(a, &guarded, &tiny.built, &head, &encoder, .native, .{}, null);
    defer result.deinit(&cb);
    try std.testing.expectEqualSlices(Id, &.{ tiny.shared, tiny.encoder_only, tiny.explicit_zero }, result.parameter_ids);
    try std.testing.expectEqual(@as(f32, 7.25), result.loss);
    try std.testing.expectEqual(@as(usize, 0), result.control_readback_bytes);
    for (result.gradients.outputs) |output| {
        for (head.gradients.outputs) |input| try std.testing.expect(output != input);
        for (encoder.gradients.outputs) |input| try std.testing.expect(output != input);
    }
    // Free both source result owners before observing merged values.
    encoder.deinit(&cb);
    encoder_live = false;
    head.deinit(&cb);
    head_live = false;
    const expected = [_][2]f32{ .{ 11, 22 }, .{ 3, 4 }, .{ 0, 0 } };
    for (result.gradients.outputs, expected) |tensor, values| {
        const actual = try cb.toFloat32(tensor, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, &values, actual);
    }
}

test "boundary recomputed execution merges canonical shared parameters and preserves zero None and independent lifetime" {
    var tiny = try Tiny.init(std.testing.allocator);
    defer tiny.deinit();
    try exerciseMerge(std.testing.allocator, &tiny);
}

test "boundary recomputed execution numerical ownership unwinds every allocation failure" {
    var tiny = try Tiny.init(std.testing.allocator);
    defer tiny.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseMerge, .{&tiny});
}

test "boundary recomputed execution admission counts pending shared sums and native operand copies" {
    const a = std.testing.allocator;
    var tiny = try Tiny.init(a);
    defer tiny.deinit();
    const estimate = try admission(a, &tiny.built, .native, .{}, null);
    try std.testing.expectEqual(@as(usize, 4), estimate.parameters);
    try std.testing.expectEqual(@as(usize, 1), estimate.shared_parameters);
    try std.testing.expectEqual(@as(usize, 32), estimate.head_gradient_bytes);
    try std.testing.expectEqual(@as(usize, 16), estimate.encoder_gradient_bytes);
    try std.testing.expectEqual(@as(usize, 48), estimate.borrowed_gradient_bytes);
    try std.testing.expectEqual(@as(usize, 32), estimate.result_tensor_bytes);
    try std.testing.expectEqual(@as(usize, 8), estimate.shared_sum_bytes);
    try std.testing.expectEqual(@as(usize, 24), estimate.native_lease_copy_bytes);
    try std.testing.expectEqual(@as(usize, 16), estimate.native_operand_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 48), estimate.backend_upper_bound_bytes);
    tiny.built.regional.options.limits.max_backend_bytes = 95;
    try std.testing.expectError(error.RecomputeLimitExceeded, admission(a, &tiny.built, .native, .{}, null));
    tiny.built.regional.options.limits.max_backend_bytes = 96;
    _ = try admission(a, &tiny.built, .native, .{}, null);
    tiny.built.regional.options.session.execution = .resident_metal;
    const device = try admission(a, &tiny.built, .resident_metal, .{}, null);
    try std.testing.expectEqual(@as(usize, 8), device.backend_upper_bound_bytes);
    try std.testing.expectEqual(@as(usize, 0), device.native_lease_copy_bytes);
    try std.testing.expectEqual(@as(usize, 0), device.native_operand_scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, admission(a, &tiny.built, .resident_metal, .{ .max_tensor_bytes = 7 }, null));
}

test "boundary recomputed execution rejects duplicate unselected malformed shape and mismatched loss without consuming inputs" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var head = try headOf(a, &cb, &tiny);
    defer head.deinit(&cb);
    var encoder = try encoderOf(a, &cb, &tiny);
    defer encoder.deinit(&cb);
    const head_id = head.parameter_ids[1];
    head.parameter_ids[1] = head.parameter_ids[0];
    try std.testing.expectError(error.InvalidRecomputeGradient, hiddenCotangent(&tiny.built, &head));
    try std.testing.expectError(error.InvalidRecomputeGradient, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    head.parameter_ids[1] = tiny.built.headId(tiny.unselected);
    try std.testing.expectError(error.InvalidRecomputeGradient, hiddenCotangent(&tiny.built, &head));
    head.parameter_ids[1] = head_id;
    const encoder_id = encoder.parameter_ids[1];
    encoder.parameter_ids[1] = encoder.parameter_ids[0];
    try std.testing.expectError(error.InvalidRecomputeGradient, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    encoder.parameter_ids[1] = tiny.unselected;
    try std.testing.expectError(error.InvalidRecomputeGradient, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    encoder.parameter_ids[1] = encoder_id;
    encoder.loss += 1;
    try std.testing.expectError(error.InvalidRecomputeLoss, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    encoder.loss = head.loss;
    head.loss = std.math.nan(f32);
    try std.testing.expectError(error.InvalidRecomputeLoss, hiddenCotangent(&tiny.built, &head));
    head.loss = encoder.loss;
    const wrong_shape = try cb.fromFloat32Shape(&.{ 1, 2 }, &.{2});
    defer cb.free(wrong_shape);
    {
        const saved_tensor = head.gradients.outputs[1];
        head.gradients.outputs[1] = wrong_shape;
        defer head.gradients.outputs[1] = saved_tensor;
        try std.testing.expectError(error.TrainingBindingShapeMismatch, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    }
    const original = tiny.built.original_ids[head_id];
    tiny.built.original_ids[head_id] = tiny.unselected;
    try std.testing.expectError(error.InvalidRecomputeGradient, admission(a, &tiny.built, .native, .{}, null));
    tiny.built.original_ids[head_id] = original;
    head.control_readback_bytes = std.math.maxInt(usize);
    encoder.control_readback_bytes = 1;
    try std.testing.expectError(error.RecomputeLimitExceeded, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
    head.control_readback_bytes = 0;
    encoder.control_readback_bytes = 0;
    var result = try merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null);
    defer result.deinit(&cb);
    try std.testing.expectEqual(@as(usize, 3), result.parameter_ids.len);
}

test "boundary recomputed execution absent dEL permits head only and empty results while explicit zero stays present" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var head = try resultOf(a, &cb, &.{tiny.built.headId(tiny.explicit_zero)}, &.{.{ 0, 0 }});
    defer head.deinit(&cb);
    var empty = try resultOf(a, &cb, &.{}, &.{});
    defer empty.deinit(&cb);
    try std.testing.expectEqual(@as(?ops.CT, null), try hiddenCotangent(&tiny.built, &head));
    var head_only = try merge(a, &cb, &tiny.built, &head, &empty, .native, .{}, null);
    defer head_only.deinit(&cb);
    try std.testing.expectEqualSlices(Id, &.{tiny.explicit_zero}, head_only.parameter_ids);
    var no_gradients = try merge(a, &cb, &tiny.built, &empty, &empty, .native, .{}, null);
    defer no_gradients.deinit(&cb);
    try std.testing.expectEqual(@as(usize, 0), no_gradients.parameter_ids.len);
    var encoder = try encoderOf(a, &cb, &tiny);
    defer encoder.deinit(&cb);
    try std.testing.expectError(error.UnexpectedRecomputeCotangent, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null));
}

test "boundary recomputed execution preserves both controls and retries after every cancellation boundary" {
    const Gate = struct {
        remaining: usize,
        fn apply(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.remaining == 0) return error.Cancelled;
            self.remaining -= 1;
        }
        fn control(self: *@This()) Control {
            return .{ .ptr = self, .check_fn = apply };
        }
    };
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var head = try headOf(a, &cb, &tiny);
    defer head.deinit(&cb);
    var encoder = try encoderOf(a, &cb, &tiny);
    defer encoder.deinit(&cb);
    var original = Gate{ .remaining = 0 };
    cb.execution_control = original.control();
    var request = Gate{ .remaining = 1000 };
    try std.testing.expectError(error.Cancelled, merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, request.control()));
    cb.execution_control = null;
    var succeeded = false;
    var cancellations: usize = 0;
    for (0..256) |limit| {
        request.remaining = limit;
        var result = merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, request.control()) catch |err| {
            try std.testing.expectEqual(error.Cancelled, err);
            cancellations += 1;
            continue;
        };
        result.deinit(&cb);
        succeeded = true;
        break;
    }
    try std.testing.expect(succeeded and cancellations > 10);
    var retry = try merge(a, &cb, &tiny.built, &head, &encoder, .native, .{}, null);
    defer retry.deinit(&cb);
    try std.testing.expectEqual(@as(usize, 3), retry.parameter_ids.len);
}
