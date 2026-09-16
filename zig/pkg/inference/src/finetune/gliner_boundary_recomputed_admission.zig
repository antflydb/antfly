// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pure admission for the separately cut head in encoder-region recomputation.
//! No runtime execution, optimizer transaction or model/source owner is created
//! here. The caller adds those existing owners before sealing the regional Plan.
const std = @import("std");
const ml = @import("ml").graph;
const seeded = @import("../graph/seeded_training.zig");
const staged = @import("../graph/multi_stage_training.zig");
const program = @import("../graph/resident_training_program.zig");
const recomputed = @import("../graph/recomputed_training.zig");
const A = std.mem.Allocator;
const Id = ml.NodeId;

pub const HeadAdmission = struct {
    enclosing: recomputed.EnclosingAdmission = .{},
    programs: usize = 0,
    /// Separate compilation envelope. When an enclosing host allocator's
    /// already-live total includes the cut head and Session, do not add this
    /// again. Generic callers must reserve that owner before constructing it.
    compiled_host_upper_bound_bytes: usize = 0,
    /// Includes named weights, the final encoder cut, deferred inputs and
    /// cotangents. Conservatively charged in enclosing.head_tape_bytes. An
    /// integration may subtract only bytes proven to share an existing owner.
    head_binding_bytes: usize = 0,
    retained_tape_bytes: usize = 0,
    cotangent_bytes: usize = 0,
    gradient_output_bytes: usize = 0,
    largest_gradient_bytes: usize = 0,
    duplicate_gradient_copy_bytes: usize = 0,
    duplicate_gradient_host_staging_bytes: usize = 0,
    /// In addition to both old accumulators and incoming gradient outputs.
    pending_merge_backend_bytes: usize = 0,
    /// Measured finite-cotangent summaries and internal instruction bounds are
    /// distinct. Their sum is a transfer admission ceiling, not a measurement.
    finite_control_readback_upper_bound_bytes: usize = 0,
    instruction_control_readback_upper_bound_bytes: usize = 0,
    control_readback_upper_bound_bytes: usize = 0,
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.RecomputeLimitExceeded;
}
fn addWork(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.RecomputeLimitExceeded;
}
fn shapeBytes(shape: ml.Shape) !usize {
    if (shape.rank_ > 8 or (shape.dtype != .f32 and shape.dtype != .i32)) return error.InvalidRecomputeShape;
    for (shape.dims[0..shape.rank_], shape.bounds[0..shape.rank_]) |dim, bound| {
        if (dim <= 0 or dim > std.math.maxInt(i32) or bound != 0) return error.InvalidRecomputeShape;
    }
    return seeded.shapeBytes(shape);
}
fn localBytes(admission: program.Admission) !usize {
    return add(try add(admission.constant_bytes, admission.working_upper_bound_bytes), admission.upload_staging_bytes);
}

/// Stages, when present, must contain this exact base Session. Every program
/// is inspected without executing it; native temporary compilation rejects a
/// geometry/aggregate compile denial before any head numeric work can begin.
pub fn headAdmission(a: A, base: *const seeded.Session, stages: ?*const staged.Session, remaining_compile: usize) !HeadAdmission {
    if (base.active_tape or remaining_compile == 0) return error.InvalidRecomputeAdmission;
    if (stages) |value| {
        if (&value.base != base or value.stages.len < 2 or value.stages.len > staged.max_stages) return error.InvalidRecomputeAdmission;
    }
    var result = HeadAdmission{};
    result.retained_tape_bytes = if (stages) |value| value.tape_bytes else base.tape_bytes;
    for (base.differentiated.graph.parameters.items) |id| {
        result.head_binding_bytes = try add(result.head_binding_bytes, try shapeBytes(base.differentiated.graph.node(id).output_shape));
    }
    for (base.seeds) |seed| {
        const id = base.differentiated.id_map[seed.cotangent];
        if (id == ml.null_node) return error.InvalidRecomputeAdmission;
        result.cotangent_bytes = try add(result.cotangent_bytes, try shapeBytes(base.differentiated.graph.node(id).output_shape));
    }
    for (base.backward.graph.outputs.items, 0..) |id, index| {
        const n = try shapeBytes(base.backward.graph.node(id).output_shape);
        result.gradient_output_bytes = try add(result.gradient_output_bytes, n);
        result.largest_gradient_bytes = @max(result.largest_gradient_bytes, n);
        if (std.mem.indexOfScalar(Id, base.backward.graph.outputs.items[0..index], id) != null) {
            // Native interpreter outputs can share a handle for repeated graph
            // IDs. Normalize ownership without charging every large gradient
            // twice; independent resident snapshots only need cheap leases.
            if (base.options.execution == .native) {
                result.duplicate_gradient_copy_bytes = try add(result.duplicate_gradient_copy_bytes, n);
                result.duplicate_gradient_host_staging_bytes = @max(result.duplicate_gradient_host_staging_bytes, n);
            }
        }
    }
    result.enclosing.head_tape_bytes = try add(result.retained_tape_bytes, result.head_binding_bytes);
    result.enclosing.head_gradient_bytes = result.gradient_output_bytes;
    result.pending_merge_backend_bytes = result.largest_gradient_bytes;
    var local: usize = 0;
    if (base.options.execution == .resident_metal) {
        const compiled = if (base.resident) |*value| value else return error.InvalidRecomputeAdmission;
        const expected_forwards = if (stages) |value| value.stages.len else 1;
        if (compiled.forward.len != expected_forwards or compiled.admission.retained_capture_bytes != result.retained_tape_bytes or
            compiled.admission.backward_output_bytes != result.gradient_output_bytes or compiled.admission.persistent_binding_bytes != result.head_binding_bytes)
            return error.InvalidRecomputeAdmission;
        if (compiled.admission.compile_upper_bound_bytes > remaining_compile) return error.RecomputeLimitExceeded;
        result.programs = compiled.admission.programs;
        result.enclosing.head_compile_bytes = compiled.admission.compile_upper_bound_bytes;
        result.enclosing.head_host_metadata_bytes = compiled.admission.host_metadata_upper_bound_bytes;
        result.enclosing.head_work = compiled.admission.total_work;
        local = compiled.admission.finite_check_scratch_bytes;
        for (compiled.forward) |*forward| local = @max(local, try localBytes(forward.admission));
        if (compiled.backward) |*reverse| local = @max(local, try localBytes(reverse.admission));
        result.finite_control_readback_upper_bound_bytes = compiled.admission.finite_check_readback_bytes;
        result.instruction_control_readback_upper_bound_bytes = compiled.admission.instruction_control_readback_upper_bound_bytes;
    } else {
        if (base.resident != null) return error.InvalidRecomputeAdmission;
        if (stages) |value| {
            for (value.stages) |*stage| try includeNative(a, &stage.program.graph, stage.captures, base, remaining_compile, &result, &local);
        } else try includeNative(a, &base.differentiated.graph, base.captures, base, remaining_compile, &result, &local);
        if (base.backward.graph.outputs.items.len != 0) try includeNative(a, &base.backward.graph, base.backward.graph.outputs.items, base, remaining_compile, &result, &local);
        // Native finite-cotangent validation creates one checked host copy at
        // a time. Count the complete cotangent set conservatively; fixed input
        // storage above already includes the original cotangents.
        local = @max(local, result.cotangent_bytes);
        result.enclosing.head_host_metadata_bytes = try add(result.enclosing.head_host_metadata_bytes, result.cotangent_bytes);
        result.enclosing.head_work = try addWork(result.enclosing.head_work, result.cotangent_bytes / 4);
    }
    result.enclosing.head_local_bytes = try add(local, try add(result.duplicate_gradient_copy_bytes, result.pending_merge_backend_bytes));
    // Head compilation is already present in the owning trainer's fixed host
    // census. Expose its conservative envelope separately; only future runtime
    // metadata/staging belongs in this additive enclosing term.
    result.compiled_host_upper_bound_bytes = result.enclosing.head_compile_bytes;
    result.enclosing.head_host_metadata_bytes = try add(result.enclosing.head_host_metadata_bytes, result.duplicate_gradient_host_staging_bytes);
    result.control_readback_upper_bound_bytes = try add(result.finite_control_readback_upper_bound_bytes, result.instruction_control_readback_upper_bound_bytes);
    // Upper bound for the optional final original-parameter merge. Source
    // models usually have disjoint encoder/head slots, but this helper does
    // not infer that property from names or assume the merge is free.
    result.enclosing.head_work = try addWork(result.enclosing.head_work, result.gradient_output_bytes / 4);
    return result;
}

fn includeNative(a: A, graph: *const ml.Graph, targets: []const Id, base: *const seeded.Session, remaining_compile: usize, result: *HeadAdmission, local: *usize) !void {
    var limits = base.options.resident.program;
    limits.max_compile_bytes = @min(limits.max_compile_bytes, remaining_compile -| result.enclosing.head_compile_bytes);
    var compiled = try program.Program.init(a, graph, targets, limits);
    defer compiled.deinit();
    result.programs = try add(result.programs, 1);
    result.enclosing.head_compile_bytes = try add(result.enclosing.head_compile_bytes, compiled.admission.compile_upper_bound_bytes);
    result.enclosing.head_host_metadata_bytes = try add(result.enclosing.head_host_metadata_bytes, compiled.admission.host_metadata_upper_bound_bytes);
    result.enclosing.head_work = try addWork(result.enclosing.head_work, compiled.admission.total_work);
    local.* = @max(local.*, try recomputed.nativeLocalBytes(&compiled));
}

fn fixture(a: A, execution: seeded.Execution) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{ 2, 3 });
    const input = try b.parameter("encoder.cut", shape);
    const weight = try b.parameter("head.weight", shape);
    const deferred = try b.parameter("__candidates", shape);
    const first = try b.mul(input, weight);
    const last = try b.mul(try b.add(first, deferred), weight);
    const seed = try b.parameter("__cotangent", shape);
    var session = try staged.Session.init(a, &graph, &.{.{ .output = last, .cotangent = seed }}, &.{ input, weight }, &.{.{ .outputs = &.{first}, .deferred_parameters = &.{deferred} }}, .{ .execution = execution });
    defer session.deinit();
    const admission = try headAdmission(a, &session.base, &session, 64 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 3), admission.programs);
    try std.testing.expectEqual(@as(usize, 96), admission.head_binding_bytes);
    try std.testing.expectEqual(@as(usize, 24), admission.cotangent_bytes);
    try std.testing.expectEqual(@as(usize, 48), admission.gradient_output_bytes);
    try std.testing.expectEqual(@as(usize, 24), admission.pending_merge_backend_bytes);
    try std.testing.expectEqual(session.tape_bytes + admission.head_binding_bytes, admission.enclosing.head_tape_bytes);
    try std.testing.expectEqual(@as(usize, 48), admission.enclosing.head_gradient_bytes);
    try std.testing.expect(admission.enclosing.head_local_bytes > 24);
    try std.testing.expectEqual(admission.finite_control_readback_upper_bound_bytes + admission.instruction_control_readback_upper_bound_bytes, admission.control_readback_upper_bound_bytes);
    if (execution == .resident_metal) try std.testing.expect(admission.finite_control_readback_upper_bound_bytes > 0) else try std.testing.expectEqual(@as(usize, 0), admission.control_readback_upper_bound_bytes);
}

test "boundary recomputed head admission accounts staged programs bindings gradients and pending sums" {
    try fixture(std.testing.allocator, .native);
    // Pure resident compilation/admission, no backend or GPU is constructed.
    try fixture(std.testing.allocator, .resident_metal);
}

fn ownership(a: A) !void {
    try fixture(a, .native);
}
test "boundary recomputed head admission compilation unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ownership, .{});
}

test "boundary recomputed head admission handles direct no-gradient sessions and compile denials" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{3});
    const input = try b.parameter("input", shape);
    const output = try b.mul(input, input);
    const seed = try b.parameter("seed", shape);
    var session = try seeded.Session.init(a, &graph, &.{.{ .output = output, .cotangent = seed }}, &.{}, .{ .allow_no_gradients = true });
    defer session.deinit();
    const admission = try headAdmission(a, &session, null, 64 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 1), admission.programs);
    try std.testing.expectEqual(@as(usize, 0), admission.gradient_output_bytes);
    try std.testing.expectEqual(@as(usize, 0), admission.pending_merge_backend_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, headAdmission(a, &session, null, 1));
    session.active_tape = true;
    try std.testing.expectError(error.InvalidRecomputeAdmission, headAdmission(a, &session, null, 1024));
    session.active_tape = false;
}
