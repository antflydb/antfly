// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resident execution plans shared by direct and staged training sessions.
//! Graph programs own independent device captures. Only finite-check scalar
//! summaries cross to the host; objectives and detached decisions remain the
//! caller's explicit responsibility.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const program = @import("resident_training_program.zig");
const interpreter = @import("interpreter.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Limits = struct {
    program: program.Limits = .{},
    max_compile_bytes: usize = 1024 * 1024 * 1024,
    max_device_bytes: usize = 8 * 1024 * 1024 * 1024,
    max_host_metadata_bytes: usize = 256 * 1024 * 1024,
};
pub const Admission = struct {
    programs: usize,
    compile_upper_bound_bytes: usize,
    persistent_binding_bytes: usize,
    retained_capture_bytes: usize,
    backward_output_bytes: usize,
    finite_check_scratch_bytes: usize,
    finite_check_readback_bytes: usize,
    instruction_control_readback_upper_bound_bytes: usize = 0,
    device_upper_bound_bytes: usize,
    host_metadata_upper_bound_bytes: usize,
    total_work: u64,
};
pub const Spec = struct { graph: *const ml.Graph, targets: []const ml.NodeId };
pub const CotangentAdmission = struct { scratch_bytes: usize, readback_bytes: usize, metadata_bytes: usize, work: u64 };

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}
fn localBytes(admission: program.Admission) !usize {
    return add(try add(admission.constant_bytes, admission.working_upper_bound_bytes), admission.upload_staging_bytes);
}

/// The strict norm ABI uses 1,024-element partials and three FP32 summary
/// values per partial/tensor, with at most 16,384 tensors per invocation.
pub fn planCotangents(shapes: []const ml.Shape, max_bytes: usize, limits: Limits) !CotangentAdmission {
    if (shapes.len == 0 or shapes.len > 16384) return error.ResourceLimitExceeded;
    var elements: usize = 0;
    var partial_bytes: usize = 0;
    for (shapes) |shape| {
        if (shape.dtype != .f32) return error.UnsupportedResidentProgramDType;
        const count = try ops.resident_program.count(shape, limits.program.instruction.primitive);
        elements = try add(elements, count);
        partial_bytes = try add(partial_bytes, try std.math.mul(usize, try std.math.divCeil(usize, count, 1024), 12));
    }
    if (elements > max_bytes / 4 or partial_bytes > (ops.resident_training.NormLimits{}).max_partial_bytes) return error.TrainingTapeLimitExceeded;
    const summaries = try std.math.mul(usize, shapes.len, 12);
    const metadata = try add(summaries, try std.math.mul(usize, shapes.len, @sizeOf(ml.Shape) + @sizeOf(ops.resident_training.NormInput)));
    // The private summary output remains live while the readback allocates a
    // same-sized shared staging buffer; partials also survive that fence.
    return .{ .scratch_bytes = try add(partial_bytes, try std.math.mul(usize, summaries, 2)), .readback_bytes = summaries, .metadata_bytes = metadata, .work = try std.math.mul(u64, try add(elements, partial_bytes / 12), 2) };
}

pub const Plans = struct {
    allocator: Allocator,
    forward: []program.Program,
    /// Absent only for an explicitly admitted forward-only training tape.
    backward: ?program.Program,
    admission: Admission,
    limits: Limits,

    pub fn init(a: Allocator, stages: []const Spec, backward_spec: Spec, persistent_binding_bytes: usize, retained_capture_bytes: usize, cotangents: CotangentAdmission, limits: Limits) !Plans {
        if (stages.len == 0 or stages.len > 8) return error.InvalidTrainingStage;
        const forward = try a.alloc(program.Program, stages.len);
        errdefer a.free(forward);
        var initialized: usize = 0;
        errdefer for (forward[0..initialized]) |*value| value.deinit();
        var compilation: usize = 0;
        var metadata: usize = cotangents.metadata_bytes;
        var local_peak: usize = cotangents.scratch_bytes;
        var work: u64 = cotangents.work;
        var capture_bytes: usize = 0;
        var instruction_control_readback_bytes: usize = 0;
        const binding_bound = try add(persistent_binding_bytes, retained_capture_bytes);
        for (stages, forward) |stage, *compiled| {
            // Enforce the aggregate compile ceiling before each independent
            // lowering allocation, including the program about to be built.
            var remaining = limits.program;
            remaining.max_compile_bytes = @min(remaining.max_compile_bytes, limits.max_compile_bytes -| compilation);
            compiled.* = try program.Program.init(a, stage.graph, stage.targets, remaining);
            initialized += 1;
            compilation = try add(compilation, compiled.admission.compile_upper_bound_bytes);
            metadata = try add(metadata, compiled.admission.host_metadata_upper_bound_bytes);
            capture_bytes = try add(capture_bytes, compiled.admission.capture_bytes);
            if (compiled.admission.binding_bytes > binding_bound or capture_bytes > retained_capture_bytes) return error.InvalidResidentTrainingAdmission;
            local_peak = @max(local_peak, try localBytes(compiled.admission));
            work = std.math.add(u64, work, compiled.admission.total_work) catch return error.ResourceLimitExceeded;
            instruction_control_readback_bytes = try add(instruction_control_readback_bytes, compiled.admission.instruction_control_readback_upper_bound_bytes);
        }
        var backward: ?program.Program = null;
        errdefer if (backward) |*compiled| compiled.deinit();
        var backward_bytes: usize = 0;
        if (backward_spec.targets.len != 0) {
            var remaining = limits.program;
            remaining.max_compile_bytes = @min(remaining.max_compile_bytes, limits.max_compile_bytes -| compilation);
            backward = try program.Program.init(a, backward_spec.graph, backward_spec.targets, remaining);
            const compiled = &backward.?;
            if (compiled.admission.binding_bytes > binding_bound) return error.InvalidResidentTrainingAdmission;
            compilation = try add(compilation, compiled.admission.compile_upper_bound_bytes);
            metadata = try add(metadata, compiled.admission.host_metadata_upper_bound_bytes);
            work = std.math.add(u64, work, compiled.admission.total_work) catch return error.ResourceLimitExceeded;
            instruction_control_readback_bytes = try add(instruction_control_readback_bytes, compiled.admission.instruction_control_readback_upper_bound_bytes);
            backward_bytes = compiled.admission.capture_bytes;
            local_peak = @max(local_peak, try add(try localBytes(compiled.admission), backward_bytes));
        }
        // Every original/deferred/cotangent input can stay leased until the
        // final backward. Forward captures coexist with backward temporaries
        // and owned gradient outputs. Stages never replay prior activations.
        const device_bytes = try add(try add(persistent_binding_bytes, retained_capture_bytes), local_peak);
        if (compilation > limits.max_compile_bytes or device_bytes > limits.max_device_bytes or metadata > limits.max_host_metadata_bytes)
            return error.ResourceLimitExceeded;
        return .{ .allocator = a, .forward = forward, .backward = backward, .limits = limits, .admission = .{
            .programs = stages.len + @intFromBool(backward != null),
            .compile_upper_bound_bytes = compilation,
            .persistent_binding_bytes = persistent_binding_bytes,
            .retained_capture_bytes = retained_capture_bytes,
            .backward_output_bytes = backward_bytes,
            .finite_check_scratch_bytes = cotangents.scratch_bytes,
            .finite_check_readback_bytes = cotangents.readback_bytes,
            .instruction_control_readback_upper_bound_bytes = instruction_control_readback_bytes,
            .device_upper_bound_bytes = device_bytes,
            .host_metadata_upper_bound_bytes = metadata,
            .total_work = work,
        } };
    }

    pub fn deinit(self: *Plans) void {
        for (self.forward) |*value| value.deinit();
        self.allocator.free(self.forward);
        if (self.backward) |*compiled| compiled.deinit();
        self.* = undefined;
    }

    fn execute(self: *const Plans, compiled: *const program.Program, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, control: ?Control) !program.Result {
        var bindings = std.ArrayListUnmanaged(program.Binding).empty;
        defer bindings.deinit(self.allocator);
        for (inputs) |input| if (compiled.usesParameter(input.node_id)) try bindings.append(self.allocator, .{ .node_id = input.node_id, .value = input.value });
        return compiled.execute(self.allocator, cb, bindings.items, control);
    }

    pub fn capture(self: *const Plans, stage: usize, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, control: ?Control) !interpreter.CapturedValuesResult {
        if (stage >= self.forward.len) return error.InvalidTrainingStage;
        const result = try self.execute(&self.forward[stage], cb, inputs, control);
        return .{ .values = result.outputs, .allocator = result.allocator };
    }

    pub fn gradients(self: *const Plans, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, control: ?Control) !interpreter.ExecutionResult {
        const compiled = if (self.backward) |*value| value else {
            if (control) |active| try active.check();
            return .{ .outputs = try self.allocator.alloc(ops.CT, 0), .allocator = self.allocator };
        };
        const result = try self.execute(compiled, cb, inputs, control);
        return .{ .outputs = result.outputs, .allocator = result.allocator };
    }
};

/// Retain a physical resident alias after validating the actual owner, dtype,
/// shape and storage. The managed parameter owner forbids mutation during the
/// tape epoch; freeing a caller's handle cannot invalidate this lease.
pub fn lease(cb: *const ops.ComputeBackend, value: ops.CT, shape: ml.Shape, limits: Limits) !ops.CT {
    const instruction = ops.resident_program.Instruction{ .op = .{ .reshape = .{ .new_shape = shape } }, .output = shape, .inputs = .{ shape, .{}, .{}, .{} }, .num_inputs = 1 };
    return cb.residentTrainingInstruction(&instruction, &.{value}, limits.program.instruction);
}

const CombinedControl = struct {
    original: ?Control,
    request: ?Control,
    fn check(raw: ?*anyopaque) !void {
        const self: *CombinedControl = @ptrCast(@alignCast(raw.?));
        if (self.original) |active| try active.check();
        if (self.request) |active| try active.check();
    }
};

pub fn finiteCotangents(a: Allocator, cb: *const ops.ComputeBackend, tensors: []const ops.CT, shapes: []const ml.Shape, max_bytes: usize, limits: Limits, control: ?Control) !usize {
    if (tensors.len != shapes.len or tensors.len == 0) return error.InvalidTrainingCotangent;
    var combined = CombinedControl{ .original = cb.execution_control, .request = control };
    var controlled = cb.*;
    controlled.execution_control = .{ .ptr = &combined, .check_fn = CombinedControl.check };
    try controlled.checkExecutionControl();
    const inputs = try a.alloc(ops.resident_training.NormInput, tensors.len);
    defer a.free(inputs);
    var elements: usize = 0;
    for (tensors, shapes, inputs) |tensor, shape, *input| {
        if (shape.dtype != .f32) return error.InvalidTrainingCotangent;
        const count = try ops.resident_program.count(shape, limits.program.instruction.primitive);
        elements = try add(elements, count);
        if (elements > max_bytes / 4) return error.TrainingTapeLimitExceeded;
        input.* = .{ .tensor = tensor, .elem_count = count };
    }
    const summary = try controlled.residentTrainingNorm(inputs, .{ .primitive = limits.program.instruction.primitive, .max_tensors = tensors.len, .max_total_elements = max_bytes / 4 });
    if (!summary.finite) return error.InvalidTrainingCotangent;
    return summary.download_bytes;
}

test "resident session finite cotangents preserve both independent cancellation controls" {
    const Fake = struct {
        calls: usize = 0,
        fn norm(raw: *anyopaque, _: []const ops.resident_training.NormInput, _: ops.resident_training.NormLimits, _: ?Control) anyerror!ops.resident_training.NormSummary {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return .{ .sum_squares = 1, .norm = 1, .finite = true, .tensor_count = 1, .partial_bytes = 12, .download_bytes = 12 };
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var fake = Fake{};
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.residentTrainingNorm = Fake.norm;
    var cb = ops.ComputeBackend{ .ptr = &fake, .vtable = &vtable, .execution_control = .{ .check_fn = Fake.cancel } };
    const tensors = [_]ops.CT{@ptrCast(&fake)};
    const shapes = [_]ml.Shape{ml.Shape.init(.f32, &.{1})};
    try std.testing.expectError(error.Cancelled, finiteCotangents(std.testing.allocator, &cb, &tensors, &shapes, 4, .{}, .{}));
    cb.execution_control = .{};
    try std.testing.expectError(error.Cancelled, finiteCotangents(std.testing.allocator, &cb, &tensors, &shapes, 4, .{}, .{ .check_fn = Fake.cancel }));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectEqual(@as(usize, 12), try finiteCotangents(std.testing.allocator, &cb, &tensors, &shapes, 4, .{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}
