// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A model forward is a frontend, not an execution engine. Capture invokes its
//! ordinary backend operations once and owns only semantic IR and ABI bindings.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const tensor = @import("../backends/tensor.zig");
const tracing = @import("../graph/tracing_compute.zig");
const program = @import("program.zig");

pub const Input = struct {
    tensor: tensor.Tensor,
    /// Descriptor-only host metadata for a tensor already held by a backend.
    /// The source and value are borrowed only through synchronous submission.
    resident: ?struct { backend: *const ops.ComputeBackend, value: ops.CT } = null,

    /// Host control/visibility metadata that determines graph structure. Its
    /// bytes participate in specialization; ordinary activation/ID values do not.
    specialize: bool = false,
    state: bool = false,

    pub fn shape(self: Input) !ml.graph.Shape {
        if (self.resident == null) return inputShape(self.tensor);
        if (self.specialize or self.tensor.data.len != 0 or self.tensor.shape.len > ml.graph.shape.max_rank) return error.InvalidInputShape;
        const result = ml.graph.Shape.init(switch (self.tensor.dtype) {
            inline else => |dtype| @field(ml.graph.DType, @tagName(dtype)),
        }, self.tensor.shape);
        _ = result.numElements() orelse return error.InvalidInputShape;
        return result;
    }

    pub fn value(self: Input, cb: *const ops.ComputeBackend) !ops.CT {
        if (self.resident) |held| {
            if (cb.kind() == .graph) return tracing.TracingCompute.namedRuntimeInput(cb, self.tensor.name, try self.shape());
            if (try cb.copyTensorFromBackend(held.backend, held.value)) |copied| return copied;
            // Native operands already have host storage. Borrow that storage
            // for the ordinary typed upload, never a device readback fallback.
            if (held.backend.kind() == .native and self.tensor.dtype == .f32) {
                const values = (try held.backend.hostFloat32Read(held.value)) orelse return error.UnsupportedProgramTransport;
                return self.fromHostF32(cb, values);
            }
            return error.UnsupportedProgramTransport;
        }
        if (self.tensor.dtype == .f32) return self.fromHostF32(cb, self.tensor.asFloat32());
        return (try cb.fromConstantBytes(self.tensor.data, switch (self.tensor.dtype) {
            inline else => |dtype| @field(ml.graph.DType, @tagName(dtype)),
        }, self.tensor.shape)) orelse error.UnsupportedProgramTransport;
    }

    fn fromHostF32(self: Input, cb: *const ops.ComputeBackend, values: []const f32) !ops.CT {
        var dims: [8]i32 = undefined;
        if (self.tensor.shape.len > dims.len) return error.InvalidInputShape;
        for (self.tensor.shape, 0..) |dim, index| dims[index] = std.math.cast(i32, dim) orelse return error.InvalidInputShape;
        return cb.fromFloat32Shape(values, dims[0..self.tensor.shape.len]);
    }
};

pub const Output = struct {
    name: []const u8,
    value: ops.CT,
    state: bool = false,
    /// Public F32 transforms occur after model tensor publication and must
    /// not inherit the internal BF16 rounding policy.
    float32_transform: ?struct { scale: f32 = 1, softcap: f32 = 0 } = null,
};

pub const Forward = struct {
    context: *anyopaque,
    /// Checkpoint/configuration and arithmetic-contract identity. The cache
    /// belongs to its loaded model, never a process-global checkpoint name.
    fingerprint: u64,
    /// Authored native schedules retain their original projection calls and
    /// packed weight operands; only unreachable nodes are removed.
    optimization: enum { general, authored_schedule } = .general,
    /// Assigned CUDA inputs trade a device copy for stable reusable banks.
    /// Callers opt in only after qualifying their workload's replay benefit.
    device_inputs: enum { direct, assigned } = .direct,
    resident_capacity: usize = 0,
    resident_initial: bool = false,
    precision: @import("precision.zig").Policy = .f32,
    residual_storage: @import("precision.zig").ResidualStorage = .activation,
    /// Public output ABI names retained by the model, independent of cache entries.
    output_names: []const []const u8 = &.{},
    /// Static, model-owned names for authored stages with distinct phase ABIs.
    output_abi: ?*const fn (*anyopaque, []const Input) anyerror![]const []const u8 = null,
    input_info: []const tensor.TensorInfo = &.{},
    /// Rechecked on every invocation, including cache hits with new values.
    validate: ?*const fn (*anyopaque, []const Input) anyerror!void = null,
    /// Existing authored semantic graphs enter the same cache/executable path
    /// directly, without recreating their arithmetic in backend wrappers.
    author: ?*const fn (*anyopaque, std.mem.Allocator, []const Input) anyerror!program.ModelProgram = null,
    run: *const fn (*anyopaque, *const ops.ComputeBackend, std.mem.Allocator, []const Input) anyerror![]Output = unsupportedForward,
};

fn unsupportedForward(_: *anyopaque, _: *const ops.ComputeBackend, _: std.mem.Allocator, _: []const Input) ![]Output {
    return error.UnsupportedModelProgram;
}

pub fn inputShape(input: tensor.Tensor) !ml.graph.Shape {
    if (input.shape.len > ml.graph.shape.max_rank) return error.UnsupportedShape;
    const shape = ml.graph.Shape.init(switch (input.dtype) {
        inline else => |dtype| @field(ml.graph.DType, @tagName(dtype)),
    }, input.shape);
    const count = shape.numElements() orelse return error.UnsupportedShape;
    if (input.data.len != try std.math.mul(usize, @intCast(count), input.dtype.byteSize())) return error.InvalidInputShape;
    return shape;
}

/// Runtime values remain independent of this key. Host metadata is explicit,
/// so changed padding/ranges cannot accidentally reuse a captured constant.
pub fn specializationKey(forward: Forward, inputs: []const Input) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(std.mem.asBytes(&forward.fingerprint));
    hash.update(&.{ @intFromEnum(forward.optimization), @intFromEnum(forward.device_inputs) });
    hash.update(std.mem.asBytes(&forward.resident_capacity));
    hash.update(&.{@intFromBool(forward.resident_initial)});
    hash.update(&.{@backingInt(forward.precision)});
    hash.update(&.{@backingInt(forward.residual_storage)});
    for (inputs) |input| {
        const shape = try input.shape();
        const name_len = input.tensor.name.len;
        hash.update(std.mem.asBytes(&name_len));
        hash.update(input.tensor.name);
        hash.update(&.{ @backingInt(shape.dtype), shape.rank(), @intFromBool(input.specialize), @intFromBool(input.state) });
        hash.update(std.mem.sliceAsBytes(input.tensor.shape));
        if (input.specialize) hash.update(input.tensor.data);
    }
    return hash.finalResult();
}

pub fn trace(allocator: std.mem.Allocator, forward: Forward, inputs: []const Input, weights: tracing.WeightShapeResolver) !program.ModelProgram {
    for (inputs) |input| _ = try input.shape();
    if (forward.validate) |validate| try validate(forward.context, inputs);
    if (forward.author) |author| return author(forward.context, allocator, inputs);
    var tracer = tracing.TracingCompute.initWithWeightResolver(allocator, weights);
    defer tracer.deinit();
    tracer.require_resident_outputs = true;
    tracer.precision = forward.precision;
    tracer.residual_storage = forward.residual_storage;
    tracer.resident_decoder = forward.resident_capacity != 0;
    var bindings: std.ArrayList(program.Binding) = .empty;
    defer bindings.deinit(allocator);
    for (inputs) |input| {
        const shape = try input.shape();
        const node = if (input.resident != null)
            try tracer.bindRuntimeInputShape(input.tensor.name, shape)
        else
            try tracer.bindRuntimeInput(input.tensor.name, shape, input.tensor.data);
        try bindings.append(allocator, .{ .name = input.tensor.name, .node_id = node, .kind = if (input.state) .state_input else .input });
    }
    const backend = tracer.backend();
    const outputs = try forward.run(forward.context, &backend, allocator, inputs);
    defer allocator.free(outputs);
    defer for (outputs) |output| backend.free(output.value);
    if (outputs.len == 0) return error.NoModelProgramOutputs;
    for (outputs) |output| {
        var id = try tracer.markOutput(output.value);
        if (output.float32_transform) |transform| {
            var builder = ml.graph.Builder.init(tracer.getGraphMut());
            if (transform.scale != 1) id = try builder.mul(id, try builder.scalarConst(.f32, transform.scale));
            if (transform.softcap > 0) {
                const cap = try builder.scalarConst(.f32, transform.softcap);
                id = try builder.mul(try builder.tanhOp(try builder.div(id, cap)), cap);
            }
            tracer.getGraphMut().outputs.items[tracer.getGraphMut().outputs.items.len - 1] = id;
        }
        try bindings.append(allocator, .{ .name = output.name, .node_id = id, .kind = if (output.state) .state_output else .output });
    }
    const graph = tracer.getGraph();
    for (graph.parameters.items) |node| {
        var external = false;
        for (bindings.items) |binding| if (binding.node_id == node) {
            external = true;
            break;
        };
        if (!external) try bindings.append(allocator, .{ .name = graph.parameterName(graph.node(node)), .node_id = node, .kind = .weight });
    }
    return program.ModelProgram.initWithPipeline(allocator, graph, bindings.items, if (forward.optimization == .authored_schedule) ml.graph.passes.pipeline.Pipeline.cleanup else ml.graph.passes.pipeline.Pipeline.default);
}

const TestForward = struct {
    fn noWeight(_: ?*anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?ml.graph.Shape {
        return null;
    }
    fn run(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const Input) anyerror![]Output {
        const x = inputs[0].tensor;
        const value = try cb.fromFloat32Shape(x.asFloat32(), &.{ 2, 3 });
        defer cb.free(value);
        const transposed = try cb.primTranspose(value, &.{ 1, 0 }, x.shape);
        defer cb.free(transposed);
        const output = try cb.primReduceSum(transposed, &.{1}, &.{ 3, 2 });
        errdefer cb.free(output);
        const result = try allocator.alloc(Output, 1);
        result[0] = .{ .name = "sum", .value = output };
        return result;
    }
    fn readback(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const Input) anyerror![]Output {
        const value = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 2, 3 });
        defer cb.free(value);
        const values = try cb.toFloat32(value, allocator);
        defer allocator.free(values);
        return error.UnexpectedReadback;
    }
    fn failureCase(allocator: std.mem.Allocator) !void {
        var marker: u8 = 0;
        var x = try tensor.Tensor.initFloat32(allocator, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
        defer x.deinit();
        var captured = try trace(allocator, .{ .context = &marker, .fingerprint = 1, .run = run }, &.{.{ .tensor = x }}, .{ .resolve = noWeight });
        defer captured.deinit();
        try std.testing.expectEqual(@as(usize, 1), captured.graph.outputs.items.len);
    }
};

test "model program capture reuses runtime values and preserves shape operations" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    var x = try tensor.Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer x.deinit();
    var captured = try trace(a, .{ .context = &marker, .fingerprint = 1, .run = TestForward.run }, &.{.{ .tensor = x }}, .{ .resolve = TestForward.noWeight });
    defer captured.deinit();
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var executable = try @import("executable.zig").prepare(a, &captured, &cb, .compiled_preferred);
    defer executable.deinit();
    const expected = [_][3]f32{ .{ 5, 7, 9 }, .{ 12, 16, 20 } };
    for (expected, 0..) |want, iteration| {
        if (iteration != 0) for (x.asFloat32Mut(), 0..) |*value, i| {
            value.* = @floatFromInt(2 * i + 3);
        };
        const input = try context.importHostTensor(a, &x);
        defer cb.free(input);
        const runtime_inputs = [_]@import("../graph/interpreter.zig").RuntimeInput{.{ .node_id = captured.bindings[0].node_id, .value = input }};
        var result = try executable.run(a, .{ .runtime_inputs = &runtime_inputs, .output_placement = .producer });
        defer result.deinit(&executable.runtime);
        const owner = result.backendFor(&executable.runtime, 0);
        const values = try owner.toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &want, values);
        const output_shape = captured.graph.node(captured.graph.outputs.items[0]).output_shape;
        try std.testing.expectEqualSlices(i64, &.{ 3, 1 }, output_shape.dims[0..output_shape.rank()]);
    }
}

test "model program capture specializes explicit metadata without freezing token IDs" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const forward = Forward{ .context = &marker, .fingerprint = 17, .run = TestForward.run };
    var ids = try tensor.Tensor.initInt64(a, "input_ids", &.{2}, &.{ 16777217, 16777219 });
    defer ids.deinit();
    var mask = try tensor.Tensor.initInt64(a, "attention_mask", &.{2}, &.{ 1, 1 });
    defer mask.deinit();
    const inputs = [_]Input{ .{ .tensor = ids }, .{ .tensor = mask, .specialize = true } };
    const original = try specializationKey(forward, &inputs);
    std.mem.bytesAsSlice(i64, @as([]align(8) u8, @alignCast(ids.data)))[0] += 2;
    try std.testing.expectEqual(original, try specializationKey(forward, &inputs));
    std.mem.bytesAsSlice(i64, @as([]align(8) u8, @alignCast(mask.data)))[1] = 0;
    try std.testing.expect(!std.mem.eql(u8, &original, &try specializationKey(forward, &inputs)));
}

test "model program capture rejects host observations and refunds every failed allocation" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    var x = try tensor.Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
    defer x.deinit();
    try std.testing.expectError(error.HostReadbackDuringCapture, trace(a, .{ .context = &marker, .fingerprint = 1, .run = TestForward.readback }, &.{.{ .tensor = x }}, .{ .resolve = TestForward.noWeight }));
    try std.testing.checkAllAllocationFailures(a, TestForward.failureCase, .{});
}

test "model program precision participates in specialization identity" {
    var context: u8 = 0;
    var forward = Forward{ .context = &context, .fingerprint = 0, .run = TestForward.run };
    const strict = try specializationKey(forward, &.{});
    forward.residual_storage = .f32;
    try std.testing.expect(!std.mem.eql(u8, &strict, &try specializationKey(forward, &.{})));
    forward.residual_storage = .activation;
    forward.precision = .bf16;
    try std.testing.expect(!std.mem.eql(u8, &strict, &try specializationKey(forward, &.{})));
}
