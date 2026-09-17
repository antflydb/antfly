// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Static, fully resident execution for the measured boundary training subset.
//! Compilation owns the lowered program, checked descriptors and liveness.
//! All live parameters require explicit resident bindings; there is no named
//! weight lookup, interpreter fallback or activation readback in this module.
//! Initial dispatches finish individually, allowing bounded reclamation and
//! cancellation. Command batching is a separate qualification step.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const instructions = @import("../ops/resident_program_ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const nil = ml.null_node;
pub const Binding = struct { node_id: Id, value: ops.CT };

pub const Limits = struct {
    instruction: instructions.Limits = .{},
    max_source_nodes: usize = 262144,
    max_program_nodes: usize = 131072,
    max_outputs: usize = 4096,
    max_compile_bytes: usize = 256 * 1024 * 1024,
    max_constant_bytes: usize = 64 * 1024 * 1024,
    max_binding_bytes: usize = 8 * 1024 * 1024 * 1024,
    max_working_bytes: usize = 512 * 1024 * 1024,
    max_capture_bytes: usize = 1024 * 1024 * 1024,
    max_device_bytes: usize = 10 * 1024 * 1024 * 1024,
    max_host_metadata_bytes: usize = 256 * 1024 * 1024,
    /// Scalar iterations, including contraction K and reduction extents; this
    /// is an admission unit and is not a FLOP or latency estimate.
    max_total_work: u64 = 1 << 42,
};
pub const Admission = struct {
    source_nodes: usize,
    program_nodes: usize,
    compile_upper_bound_bytes: usize = 0,
    parameters: usize = 0,
    constant_bytes: usize = 0,
    binding_bytes: usize = 0,
    working_upper_bound_bytes: usize = 0,
    capture_bytes: usize = 0,
    upload_staging_bytes: usize = 0,
    host_metadata_upper_bound_bytes: usize = 0,
    device_upper_bound_bytes: usize = 0,
    total_work: u64 = 0,
    instruction_control_readback_upper_bound_bytes: usize = 0,
};
pub const Result = struct {
    allocator: Allocator,
    /// Physically independent copies aligned with the requested target order.
    outputs: []ops.CT,
    admission: Admission,
    pub fn deinit(self: *Result, cb: *const ops.ComputeBackend) void {
        for (self.outputs) |output| cb.free(output);
        self.allocator.free(self.outputs);
        self.* = undefined;
    }
};

fn checkedAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}
fn nodeBytes(shape: ml.Shape, limits: Limits) !usize {
    return std.math.mul(usize, try instructions.count(shape, limits.instruction.primitive), 4) catch error.ResourceLimitExceeded;
}

// One synchronous execution owns this stack adapter. Preserve both borrowed
// stop conditions and the worker carrier; never cache it in the compiled plan.
const CombinedControl = struct {
    original: ?Control,
    request: ?Control,

    fn checkBoth(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.original) |original| try original.check();
        if (self.request) |request| try request.check();
    }

    fn control(self: *@This()) Control {
        var result = self.request orelse self.original orelse Control{};
        if (self.original) |original| {
            if (result.io == null) result.io = original.io;
            if (result.hard_cancellation == null) result.hard_cancellation = original.hard_cancellation;
            if (result.progress == null) result.progress = original.progress;
            if (original.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        result.ptr = self;
        result.check_fn = checkBoth;
        return result;
    }
};

pub const Program = struct {
    allocator: Allocator,
    lowered: ml.lower.LowerResult,
    descriptors: []?instructions.Instruction,
    last_use: []Id,
    first_capture: []Id,
    next_capture: []Id,
    source_parameters: []bool,
    admission: Admission,
    limits: Limits,

    pub fn init(a: Allocator, source: *const ml.Graph, targets: []const Id, limits: Limits) !Program {
        const source_count = source.nodes.items.len;
        if (source_count == 0 or source_count > limits.max_source_nodes or targets.len == 0 or targets.len > limits.max_outputs or
            source_count > std.math.maxInt(Id) or limits.max_program_nodes > std.math.maxInt(Id))
            return error.ResourceLimitExceeded;
        // Include the lowering copy and bounded topology/descriptor scratch
        // before asking the lowering implementation to allocate anything.
        const per_node = @sizeOf(ml.Node) * 2 + @sizeOf(instructions.Instruction) + 128;
        const compilation_bytes = try checkedAdd(std.math.mul(usize, source_count, per_node) catch return error.ResourceLimitExceeded, try checkedAdd(std.math.mul(usize, source.string_table.items.len, 2) catch return error.ResourceLimitExceeded, std.math.mul(usize, source.constant_pool.items.len, 2) catch return error.ResourceLimitExceeded));
        if (compilation_bytes > limits.max_compile_bytes or source.constant_pool.items.len > limits.max_constant_bytes)
            return error.ResourceLimitExceeded;
        for (targets) |target| if (target >= source_count) return error.InvalidResidentProgramTarget;
        var view = source.*;
        view.outputs = .{ .items = @constCast(targets), .capacity = targets.len };
        var lowered = try ml.lower.lower(a, &view);
        errdefer lowered.deinit();
        const graph = &lowered.graph;
        const count = graph.nodes.items.len;
        if (count > limits.max_program_nodes) return error.ResourceLimitExceeded;
        const descriptors = try a.alloc(?instructions.Instruction, count);
        errdefer a.free(descriptors);
        @memset(descriptors, null);
        const last_use = try a.alloc(Id, count);
        errdefer a.free(last_use);
        for (last_use, 0..) |*last, index| last.* = @intCast(index);
        const first_capture = try a.alloc(Id, count);
        errdefer a.free(first_capture);
        @memset(first_capture, nil);
        const next_capture = try a.alloc(Id, targets.len);
        errdefer a.free(next_capture);
        const source_parameters = try a.alloc(bool, source_count);
        errdefer a.free(source_parameters);
        for (source.nodes.items, source_parameters) |node, *parameter| parameter.* = node.op == .parameter;
        var admission = Admission{ .source_nodes = source_count, .program_nodes = count, .compile_upper_bound_bytes = compilation_bytes };
        var index_metadata_bytes: usize = 0;
        var operation_metadata_bytes: usize = 0;
        for (graph.outputs.items, 0..) |id, capture| {
            next_capture[capture] = first_capture[id];
            first_capture[id] = @intCast(capture);
            admission.capture_bytes = try checkedAdd(admission.capture_bytes, try nodeBytes(graph.node(id).output_shape, limits));
        }
        for (graph.nodes.items, 0..) |node, id| {
            const bytes = try nodeBytes(node.output_shape, limits);
            for (node.getInputs()) |input| {
                if (input >= id) return error.InvalidResidentProgramDependency;
                last_use[input] = @intCast(id);
            }
            switch (node.op) {
                .parameter => {
                    if (node.num_inputs != 0) return error.InvalidResidentProgramDependency;
                    admission.parameters += 1;
                    admission.binding_bytes = try checkedAdd(admission.binding_bytes, bytes);
                    if (node.output_shape.dtype == .i32) index_metadata_bytes = try checkedAdd(index_metadata_bytes, bytes);
                },
                .constant => |attrs| {
                    if (node.num_inputs != 0 or attrs.data_len != bytes / 4 or
                        attrs.data_offset > graph.constant_pool.items.len or bytes > graph.constant_pool.items.len - attrs.data_offset or
                        attrs.data_offset % 4 != 0) return error.InvalidResidentProgramConstant;
                    admission.constant_bytes = try checkedAdd(admission.constant_bytes, bytes);
                    admission.upload_staging_bytes = @max(admission.upload_staging_bytes, bytes);
                    if (node.output_shape.dtype == .i32) index_metadata_bytes = try checkedAdd(index_metadata_bytes, bytes);
                },
                else => {
                    const instruction = try instructions.Instruction.fromNode(graph, &node);
                    const geometry = try instruction.validate(limits.instruction);
                    descriptors[id] = instruction;
                    operation_metadata_bytes = @max(operation_metadata_bytes, geometry.host_metadata_bytes);
                    admission.total_work = std.math.add(u64, admission.total_work, geometry.work_items) catch return error.ResourceLimitExceeded;
                    admission.instruction_control_readback_upper_bound_bytes = try checkedAdd(admission.instruction_control_readback_upper_bound_bytes, geometry.control_readback_upper_bound_bytes);
                },
            }
        }
        var live: usize = 0;
        for (graph.nodes.items, 0..) |node, id| {
            if (descriptors[id]) |instruction| {
                const geometry = try instruction.validate(limits.instruction);
                live = try checkedAdd(live, geometry.output_elements * 4);
                admission.working_upper_bound_bytes = @max(admission.working_upper_bound_bytes, try checkedAdd(live, geometry.scratch_bytes));
            }
            for (node.getInputs(), 0..) |input, ordinal| {
                if (last_use[input] != id or descriptors[input] == null) continue;
                if (std.mem.indexOfScalar(Id, node.getInputs()[0..ordinal], input) != null) continue;
                live -= try nodeBytes(graph.node(input).output_shape, limits);
            }
            if (last_use[id] == id and descriptors[id] != null) live -= try nodeBytes(node.output_shape, limits);
        }
        std.debug.assert(live == 0);
        admission.device_upper_bound_bytes = try checkedAdd(try checkedAdd(admission.binding_bytes, admission.constant_bytes), try checkedAdd(try checkedAdd(admission.working_upper_bound_bytes, admission.capture_bytes), admission.upload_staging_bytes));
        const execution_metadata = try checkedAdd(std.math.mul(usize, count, @sizeOf(?ops.CT)) catch return error.ResourceLimitExceeded, std.math.mul(usize, targets.len, @sizeOf(?ops.CT) + @sizeOf(ops.CT)) catch return error.ResourceLimitExceeded);
        admission.host_metadata_upper_bound_bytes = try checkedAdd(try checkedAdd(index_metadata_bytes, operation_metadata_bytes), execution_metadata);
        if (admission.binding_bytes > limits.max_binding_bytes or admission.constant_bytes > limits.max_constant_bytes or
            admission.working_upper_bound_bytes > limits.max_working_bytes or admission.capture_bytes > limits.max_capture_bytes or
            admission.device_upper_bound_bytes > limits.max_device_bytes or admission.host_metadata_upper_bound_bytes > limits.max_host_metadata_bytes or admission.total_work > limits.max_total_work)
            return error.ResourceLimitExceeded;
        return .{ .allocator = a, .lowered = lowered, .descriptors = descriptors, .last_use = last_use, .first_capture = first_capture, .next_capture = next_capture, .source_parameters = source_parameters, .admission = admission, .limits = limits };
    }

    pub fn deinit(self: *Program) void {
        self.lowered.deinit();
        self.allocator.free(self.descriptors);
        self.allocator.free(self.last_use);
        self.allocator.free(self.first_capture);
        self.allocator.free(self.next_capture);
        self.allocator.free(self.source_parameters);
        self.* = undefined;
    }

    pub fn usesParameter(self: *const Program, source_id: Id) bool {
        return source_id < self.source_parameters.len and self.source_parameters[source_id] and self.lowered.id_map[source_id] != nil;
    }

    pub fn execute(self: *const Program, a: Allocator, backend: *const ops.ComputeBackend, bindings: []const Binding, control: ?Control) !Result {
        var combined = CombinedControl{ .original = backend.execution_control, .request = control };
        const active = combined.control();
        try active.check();
        if (backend.kind() != .metal or backend.vtable.residentTrainingInstruction == null)
            return error.UnsupportedResidentProgramBackend;
        if (bindings.len != self.admission.parameters) return error.MissingResidentProgramBinding;
        var cb = backend.*;
        cb.execution_control = active;
        const graph = &self.lowered.graph;
        const values = try a.alloc(?ops.CT, graph.nodes.items.len);
        defer a.free(values);
        @memset(values, null);
        defer for (values) |value| if (value) |tensor| cb.free(tensor);
        const outputs = try a.alloc(?ops.CT, self.next_capture.len);
        defer a.free(outputs);
        @memset(outputs, null);
        errdefer for (outputs) |value| if (value) |tensor| cb.free(tensor);
        // Validate every explicit binding before constant uploads or numeric
        // execution. The strict reshape returns an owned immutable alias and
        // verifies exact owner, physical dtype, storage and logical shape.
        for (bindings) |binding| {
            if (!self.usesParameter(binding.node_id)) return error.InvalidResidentProgramBinding;
            const id = self.lowered.id_map[binding.node_id];
            if (values[id] != null) return error.DuplicateResidentProgramBinding;
            const shape = graph.node(id).output_shape;
            const instruction = instructions.Instruction{ .op = .{ .reshape = .{ .new_shape = shape } }, .output = shape, .inputs = .{ shape, .{}, .{}, .{} }, .num_inputs = 1 };
            values[id] = try cb.residentTrainingInstruction(&instruction, &.{binding.value}, self.limits.instruction);
        }
        for (graph.nodes.items, 0..) |node, id| {
            try active.check();
            if (node.op == .constant) {
                const shape = node.output_shape;
                var dimensions: [8]i32 = undefined;
                for (shape.dims[0..shape.rank_], 0..) |dim, axis| dimensions[axis] = @intCast(dim);
                const attrs = node.op.constant;
                const raw = graph.constant_pool.items[attrs.data_offset..][0 .. @as(usize, attrs.data_len) * 4];
                values[id] = switch (shape.dtype) {
                    .f32 => try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = std.mem.bytesAsSlice(f32, @as([]align(4) const u8, @alignCast(raw))), .shape = dimensions[0..shape.rank_] } }, self.limits.instruction.primitive),
                    .i32 => try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = std.mem.bytesAsSlice(i32, @as([]align(4) const u8, @alignCast(raw))), .shape = dimensions[0..shape.rank_] } }, self.limits.instruction.primitive),
                    else => return error.UnsupportedResidentProgramDType,
                };
            } else if (self.descriptors[id]) |instruction| {
                var inputs: [4]ops.CT = undefined;
                for (node.getInputs(), 0..) |input, ordinal| inputs[ordinal] = values[input] orelse return error.MissingResidentProgramValue;
                values[id] = try cb.residentTrainingInstruction(&instruction, inputs[0..node.num_inputs], self.limits.instruction);
            }
            const value = values[id] orelse return error.MissingResidentProgramBinding;
            var capture = self.first_capture[id];
            while (capture != nil) : (capture = self.next_capture[capture]) {
                var dimensions: [8]i32 = undefined;
                for (node.output_shape.dims[0..node.output_shape.rank_], 0..) |dim, axis| dimensions[axis] = @intCast(dim);
                outputs[capture] = try cb.snapshotTensorShape(value, dimensions[0..node.output_shape.rank_]);
            }
            for (node.getInputs()) |input| if (self.last_use[input] == id) {
                if (values[input]) |owned| cb.free(owned);
                values[input] = null;
            };
            if (self.last_use[id] == id) {
                cb.free(value);
                values[id] = null;
            }
        }
        try active.check();
        const completed = try a.alloc(ops.CT, outputs.len);
        for (outputs, completed) |output, *value| value.* = output orelse unreachable;
        return .{ .allocator = a, .outputs = completed, .admission = self.admission };
    }
};

fn allocationCheck(a: Allocator) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const input = try b.parameter("input", ml.Shape.init(.f32, &.{ 2, 3 }));
    const scalar = try b.scalarConst(.f32, 2);
    const squared = try b.mul(input, input);
    const output = try b.add(squared, scalar);
    var program = try Program.init(a, &graph, &.{ squared, output }, .{});
    defer program.deinit();
    try std.testing.expect(program.usesParameter(input));
    try std.testing.expectEqual(@as(usize, 48), program.admission.capture_bytes);
    try std.testing.expectEqual(@as(usize, 24), program.admission.binding_bytes);
    try std.testing.expectEqual(@as(usize, 48), program.admission.working_upper_bound_bytes);
}

test "resident program compilation plans liveness captures and cleans every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCheck, .{});
}

test "resident program compilation rejects unsupported integer conversions instructions and resource limits" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const input = try b.parameter("input", ml.Shape.init(.i32, &.{2}));
    const wide = try b.convertDtype(input, .i64);
    try std.testing.expectError(error.UnsupportedResidentProgramDType, Program.init(a, &graph, &.{wide}, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, Program.init(a, &graph, &.{input}, .{ .max_compile_bytes = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, Program.init(a, &graph, &.{input}, .{ .max_capture_bytes = 1 }));
    const source = try b.parameter("source", ml.Shape.init(.f32, &.{2}));
    const unsupported = try b.sinOp(source);
    try std.testing.expectError(error.UnsupportedResidentProgramInstruction, Program.init(a, &graph, &.{unsupported}, .{}));
}

test "resident program preserves backend and request controls without GPU dispatch" {
    const execution = @import("../execution_control.zig");
    const Probe = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        cancelled: bool = false,
        monitor: ?execution.MonitorControl = null,
        arms: usize = 0,
        disarms: usize = 0,
        progress_count: usize = 0,
        fn checkControl(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.failure) |err| return err;
        }
        fn isCancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled;
        }
        fn arm(raw: *anyopaque, monitor: execution.MonitorControl) anyerror!u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.monitor = monitor;
            self.arms += 1;
            return 11;
        }
        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 11 and self.monitor != null);
            self.monitor = null;
            self.disarms += 1;
        }
        fn progress(raw: ?*anyopaque, _: execution.Progress) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.progress_count += 1;
        }
    };
    const Fake = struct {
        original: *Probe,
        request: *Probe,
        carrier: *Probe,
        expected_io: std.Io,
        expected_deadline: u64,
        calls: usize = 0,
        fn kind(_: *anyopaque) ops.BackendKind {
            return .metal;
        }
        fn instruction(raw: *anyopaque, _: *const instructions.Instruction, _: []const ops.CT, _: instructions.Limits, maybe_control: ?Control) anyerror!ops.CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const active = maybe_control orelse return error.MissingProbeControl;
            try std.testing.expectEqual(self.expected_io, active.io.?);
            try std.testing.expectEqual(self.expected_deadline, active.deadline_ns.?);
            try std.testing.expectEqual(@as(*anyopaque, @ptrCast(self.carrier)), active.hard_cancellation.?.ptr);
            try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(self.original)), active.progress.?.ptr);
            active.progress.?.update(.{ .phase = .executing });
            var guard = try active.enterUninterruptible(.process_required);
            defer guard.deinit();
            const monitor = self.carrier.monitor.?;
            try std.testing.expectEqual(self.expected_deadline, monitor.deadline_ns.?);
            try monitor.check();
            self.original.failure = error.OriginalControlStopped;
            try std.testing.expectError(error.OriginalControlStopped, monitor.check());
            self.original.failure = null;
            self.request.failure = error.RequestControlStopped;
            try std.testing.expectError(error.RequestControlStopped, monitor.check());
            self.request.failure = null;
            self.original.cancelled = true;
            try std.testing.expectError(error.Cancelled, monitor.check());
            self.original.cancelled = false;
            self.request.cancelled = true;
            try std.testing.expectError(error.Cancelled, monitor.check());
            self.request.cancelled = false;
            // This fake never returns a tensor or starts native/device work.
            return error.ProbeReached;
        }
    };
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const input = try builder.parameter("input", ml.Shape.init(.f32, &.{1}));
    var program = try Program.init(a, &graph, &.{input}, .{});
    defer program.deinit();
    var original = Probe{};
    var request = Probe{};
    const future = std.math.maxInt(u64);
    var base = Control{
        .io = std.testing.io,
        .deadline_ns = future - 2,
        .ptr = &original,
        .check_fn = Probe.checkControl,
        .cancellation = .{ .ptr = &original, .is_cancelled_fn = Probe.isCancelled },
        .hard_cancellation = .{ .ptr = &original, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm },
        .progress = .{ .ptr = &original, .update_fn = Probe.progress },
    };
    var requested = Control{
        .deadline_ns = future - 1,
        .ptr = &request,
        .check_fn = Probe.checkControl,
        .cancellation = .{ .ptr = &request, .is_cancelled_fn = Probe.isCancelled },
    };
    var fake = Fake{ .original = &original, .request = &request, .carrier = &original, .expected_io = std.testing.io, .expected_deadline = future - 2 };
    var vtable: ops.ComputeBackend.VTable = undefined;
    vtable.backendKind = Fake.kind;
    vtable.residentTrainingInstruction = Fake.instruction;
    var cb = ops.ComputeBackend{ .ptr = &fake, .vtable = &vtable, .execution_control = base };
    const bindings = [_]Binding{.{ .node_id = input, .value = @ptrCast(&fake) }};
    original.failure = error.OriginalControlStopped;
    try std.testing.expectError(error.OriginalControlStopped, program.execute(a, &cb, &bindings, requested));
    original.failure = null;
    request.failure = error.RequestControlStopped;
    try std.testing.expectError(error.RequestControlStopped, program.execute(a, &cb, &bindings, requested));
    request.failure = null;
    try std.testing.expectEqual(@as(usize, 0), fake.calls);

    try std.testing.expectError(error.ProbeReached, program.execute(a, &cb, &bindings, requested));
    try std.testing.expectEqual(@as(usize, 1), original.arms);
    try std.testing.expectEqual(@as(usize, 1), original.disarms);
    try std.testing.expect(original.monitor == null);
    try std.testing.expect(original.calls > 0 and request.calls > 0);
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&original)), cb.execution_control.?.ptr);

    // A later invocation supplies a different worker carrier and an earlier
    // deadline. The compiled plan must not retain the previous stack adapter.
    requested.io = .{ .userdata = &request, .vtable = std.testing.io.vtable };
    requested.hard_cancellation = .{ .ptr = &request, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm };
    requested.deadline_ns = future - 3;
    fake.carrier = &request;
    fake.expected_io = requested.io.?;
    fake.expected_deadline = future - 3;
    try std.testing.expectError(error.ProbeReached, program.execute(a, &cb, &bindings, requested));
    try std.testing.expectEqual(@as(usize, 1), request.arms);
    try std.testing.expectEqual(@as(usize, 1), request.disarms);
    try std.testing.expect(request.monitor == null);
    try std.testing.expectEqual(@as(usize, 2), original.progress_count);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);

    base.deadline_ns = 0;
    cb.execution_control = base;
    try std.testing.expectError(error.Timeout, program.execute(a, &cb, &bindings, requested));
    base.deadline_ns = future - 2;
    cb.execution_control = base;
    requested.deadline_ns = 0;
    try std.testing.expectError(error.Timeout, program.execute(a, &cb, &bindings, requested));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}
