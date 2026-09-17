// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict boundary and retained-training adapter. Shape semantics are owned by
//! the shared planners; numerical work uses the existing CUDA backend. Uploaded
//! integer metadata is retained for validation/grouping, never converted to f32.
const std = @import("std");
const cuda = @import("cuda_compute.zig");
const api = cuda.gliner25_api;
const ops = @import("../ops.zig");
const boundary = ops.gliner_boundary_device;
const resident = ops.resident_training;
const program = ops.resident_program;
const buffer = @import("buffer.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const CT = ops.CT;
const Compute = cuda.CudaCompute;
const Tensor = cuda.CudaTensor;
const attention_plan = @import("../deberta_training_attention_device.zig");
const attention_schedule = @import("../deberta_training_attention_schedule.zig");

const AttentionDispatcher = struct {
    self: *Compute,
    buffers: [10]buffer.DeviceBuffer,
    control: ?Control,

    pub fn dispatch(d: @This(), params: *const attention_schedule.Params) !void {
        if (d.control) |c| try c.check();
        try d.self.kernels.launchGliner25Attention(&d.self.ctx, d.buffers, params.*);
        d.self.stats.launch_other += 1;
        var status: u32 = 0;
        try api.copyToHostTracked(d.self, d.buffers[9], std.mem.asBytes(&status));
        try api.synchronizeAndDrainDeferredDeviceFrees(d.self);
        if (status & 2 != 0) return error.InvalidDebertaTrainingAttentionControl;
        if (status != 0) return error.NonFiniteDebertaTrainingAttention;
        if (d.control) |c| try c.check();
    }
};

fn trainingAttention(self: *Compute, inputs: []const CT, attrs: attention_plan.Attrs, backward: bool, limits: program.Limits, control: ?Control) !CT {
    const admitted = try attention_plan.plan(attrs, backward, .{
        .max_tensor_bytes = limits.primitive.max_tensor_bytes,
        .max_scratch_bytes = limits.max_scratch_bytes,
        .max_host_metadata_bytes = limits.primitive.max_index_metadata_bytes,
        .max_work_items = limits.max_instruction_work,
    });
    if (!self.kernels.hasGliner25Attention()) return error.DebertaTrainingAttentionProfileUnavailable;
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    const words = tensor(inputs[2]).resident_indices orelse return error.UnsupportedResidentTrainingIndexProof;
    const decoded = try ops.deberta_training_attention.validateControl(attrs, words, .{ .control = control });
    var grouped: ?@import("../resident_training_groups.zig").Grouped = null;
    defer if (grouped) |*g| g.deinit();
    if (backward) grouped = try @import("../resident_training_groups.zig").build(self.allocator, words[6 + admitted.batch_tokens ..], attrs.relative_rows, admitted.grouping_limits, control);
    var owned: [5]?CT = @splat(null);
    defer for (owned) |t| if (t) |value| api.freeTensor(self, value);
    var refs: [10]buffer.DeviceBuffer = @splat(.{});
    for (inputs, 0..) |input, i| refs[i] = tensor(input).buffer;
    if (backward) {
        owned[0] = try allocate(self, &.{@intCast(admitted.row_scratch_bytes / 4)}, .f32, limits.primitive);
        refs[5] = tensor(owned[0].?).buffer;
    }
    if (grouped) |g| {
        for ([_][]const i32{ g.rows, g.offsets, g.order }, 0..) |values, i| {
            owned[i + 1] = try upload(self, i32, values, &.{@intCast(values.len)}, limits.primitive);
            refs[i + 6] = tensor(owned[i + 1].?).buffer;
        }
    }
    owned[4] = try upload(self, i32, &.{0}, &.{1}, limits.primitive);
    refs[9] = tensor(owned[4].?).buffer;
    const layout = try attrs.layout();
    const shape = if (backward) layout.gradientShape() else layout.outputShape();
    const output = try allocate(self, shape.dims[0..shape.rank_], .f32, limits.primitive);
    errdefer api.freeTensor(self, output);
    refs[4] = tensor(output).buffer;
    const params = attention_schedule.Params{
        .batch = attrs.batch,
        .sequence = attrs.seq_len,
        .heads = attrs.num_heads,
        .dimension = attrs.head_dim,
        .relative_rows = attrs.relative_rows,
        .threads = admitted.threads,
        .dropout_threshold = @intCast(decoded.dropout.threshold),
        .dropout_scale = decoded.dropout.scale,
        .attention_scale = @sqrt(@as(f32, @floatFromInt(attrs.head_dim)) * 3),
        .backward = @intFromBool(backward),
        .dropout_stream = decoded.dropout.stream,
    };
    try attention_schedule.run(attrs, admitted, grouped, params, AttentionDispatcher{ .self = self, .buffers = refs, .control = control });
    return output;
}

pub fn debertaTrainingAttentionV1(ctx: *anyopaque, qkv: CT, relative: CT, controls: CT, attrs: attention_plan.Attrs, control: ?Control) anyerror!CT {
    const layout = try attrs.layout();
    const instruction = program.Instruction{
        .op = .{ .fused_deberta_training_attention_v1 = attrs },
        .output = layout.outputShape(),
        .inputs = .{ layout.qkvShape(), layout.relativeShape(), layout.controlShape(), .{} },
        .num_inputs = 3,
    };
    return residentTrainingInstruction(ctx, &instruction, &.{ qkv, relative, controls }, .{}, control);
}

pub fn debertaTrainingAttentionBackwardV1(ctx: *anyopaque, qkv: CT, relative: CT, controls: CT, dout: CT, attrs: attention_plan.Attrs, control: ?Control) anyerror!CT {
    const layout = try attrs.layout();
    const instruction = program.Instruction{
        .op = .{ .fused_deberta_training_attention_backward_v1 = attrs },
        .output = layout.gradientShape(),
        .inputs = .{ layout.qkvShape(), layout.relativeShape(), layout.controlShape(), layout.outputShape() },
        .num_inputs = 4,
    };
    return residentTrainingInstruction(ctx, &instruction, &.{ qkv, relative, controls, dout }, .{}, control);
}

fn backend(ctx: *anyopaque) *Compute {
    return @ptrCast(@alignCast(ctx));
}

fn tensor(ct: CT) *Tensor {
    return @ptrCast(@alignCast(ct));
}

fn check(self: *Compute, ct: CT, dtype: ?@import("../../backends/tensor.zig").DType, limits: resident.Limits) !*Tensor {
    const t = tensor(ct);
    if (t.resident_owner != self) return error.ForeignResidentTrainingTensor;
    if (t.quant_type != null or (dtype != null and t.dtype != dtype.?) or
        (t.dtype != .f32 and t.dtype != .i32 and t.dtype != .f16)) return error.UnsupportedResidentTrainingDType;
    const count = try resident.shapeElements(i64, t.shape, limits);
    if (count != t.elem_count or t.buffer.ptr == 0 or t.buffer.len < count * @as(usize, if (t.dtype == .f16) 2 else 4)) return error.InvalidResidentTrainingShape;
    return t;
}

fn allocate(self: *Compute, shape: []const i64, dtype: @import("../../backends/tensor.zig").DType, limits: resident.Limits) !CT {
    const count = try resident.shapeElements(i64, shape, limits);
    const owned_shape = try self.allocator.dupe(i64, shape);
    errdefer self.allocator.free(owned_shape);
    var device = try api.allocDeviceBuffer(self, count * @as(usize, if (dtype == .f16) 2 else 4));
    errdefer device.free(&self.ctx);
    const result = try api.createTensorWithDType(self, device, owned_shape, count, dtype);
    tensor(result).resident_owner = self;
    tensor(result).strict_resident_training = true;
    return result;
}

fn setShape(self: *Compute, ct: CT, shape: []const i64, limits: resident.Limits) !CT {
    const t = tensor(ct);
    if (try resident.shapeElements(i64, shape, limits) != t.elem_count) return error.InvalidResidentTrainingShape;
    const owned_shape = try self.allocator.dupe(i64, shape);
    if (t.owns_shape) self.allocator.free(t.shape);
    t.shape = owned_shape;
    t.owns_shape = true;
    t.resident_owner = self;
    t.strict_resident_training = true;
    return ct;
}

fn upload(self: *Compute, comptime T: type, values: []const T, shape: []const i32, limits: resident.Limits) !CT {
    if (try resident.shapeElements(i32, shape, limits) != values.len) return error.InvalidResidentTrainingShape;
    // Passing a scalar by value to the fill kernel avoids an H2D transfer and
    // the synchronization needed to keep pageable upload storage alive.
    if (T == f32 and values.len == 1) {
        var dims: [8]i64 = undefined;
        for (shape, 0..) |dim, i| dims[i] = dim;
        const result = try allocate(self, dims[0..shape.len], .f32, limits);
        errdefer api.freeTensor(self, result);
        try self.kernels.launchFillF32(&self.ctx, tensor(result).buffer, 1, values[0]);
        self.stats.launch_other += 1;
        return result;
    }
    const indices: ?[]i32 = if (T == i32) blk: {
        if (values.len * 4 > limits.max_index_metadata_bytes) return error.ResourceLimitExceeded;
        break :blk try self.allocator.dupe(i32, values);
    } else null;
    errdefer if (indices) |v| self.allocator.free(v);
    const result = if (T == i32)
        (try api.fromInt32ShapeOp(self, values, shape)) orelse return error.UnsupportedResidentTrainingDType
    else
        try api.fromFloat32ShapeOp(self, values, shape);
    tensor(result).resident_owner = self;
    tensor(result).strict_resident_training = true;
    tensor(result).resident_indices = indices;
    return result;
}

fn view(self: *Compute, ct: CT, shape: []const i32, limits: resident.Limits) !CT {
    const source = try check(self, ct, null, limits);
    if (try resident.shapeElements(i32, shape, limits) != source.elem_count) return error.InvalidResidentTrainingShape;
    if (!source.owned_by_tensor) return error.InvalidResidentTrainingShape;
    const dims = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(dims);
    for (shape, 0..) |d, i| dims[i] = d;
    const result = try self.allocator.create(Tensor);
    result.* = source.*;
    result.shape = dims;
    result.owns_shape = true;
    result.owns_buffer = false;
    result.owns_tc_quant = false;
    result.owns_bf16_mirror = false;
    result.owns_training_upload_host = false;
    result.owns_resident_indices = false;
    result.resident_parent = source;
    result.resident_references = 1;
    source.resident_references += 1;
    return result;
}

fn copy(self: *Compute, ct: CT, shape: []const i32, limits: resident.Limits) !CT {
    const source = try check(self, ct, null, limits);
    const count = try resident.shapeElements(i32, shape, limits);
    if (count != source.elem_count) return error.InvalidResidentTrainingShape;
    // A snapshot duplicates the retained index proof as well as device data.
    // Admit that new host allocation against this request's limits.
    if (source.resident_indices) |indices| {
        if (indices.len > limits.max_index_metadata_bytes / @sizeOf(i32)) return error.ResourceLimitExceeded;
    }
    var dims: [8]i64 = undefined;
    for (shape, 0..) |d, i| dims[i] = d;
    const result = try allocate(self, dims[0..shape.len], source.dtype, limits);
    errdefer api.freeTensor(self, result);
    try api.copyFromDeviceTracked(self, tensor(result).buffer, source.buffer, count * @as(usize, if (source.dtype == .f16) 2 else 4));
    if (source.resident_indices) |indices| tensor(result).resident_indices = try self.allocator.dupe(i32, indices);
    return result;
}

fn kernelInto(self: *Compute, request: boundary.Kernel, output: CT) !void {
    const layout = try request.layout();
    const out = try check(self, output, .f32, .{});
    if (out.elem_count != layout.output_elements) return error.InvalidBoundaryDeviceShape;
    var inputs: [10]buffer.DeviceBuffer = @splat(.{});
    for (request.inputs, 0..) |input, i| {
        if (layout.input_elements[i] == 0) {
            if (input != null) return error.InvalidBoundaryDeviceShape;
            continue;
        }
        const t = try check(self, input orelse return error.InvalidBoundaryDeviceShape, if (request.kind == .cast_half and i == 0) .f16 else if (layout.integer_inputs & (@as(u16, 1) << @intCast(i)) != 0) .i32 else .f32, .{});
        if (t.elem_count != layout.input_elements[i]) return error.InvalidBoundaryDeviceShape;
        inputs[i] = t.buffer;
    }
    try self.kernels.launchGliner25Boundary(&self.ctx, inputs, out.buffer, request.params(), layout.work_items);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
}

fn kernel(self: *Compute, request: boundary.Kernel) !CT {
    const layout = try request.layout();
    const output = try allocate(self, &.{@intCast(layout.output_elements)}, .f32, .{});
    errdefer api.freeTensor(self, output);
    try kernelInto(self, request, output);
    return output;
}

fn gather(self: *Compute, input: CT, indices: CT, input_shape: []const i64, axis: u8, limits: resident.Limits) !CT {
    if (axis != 0 or input_shape.len == 0) return error.UnsupportedResidentTrainingPrimitive;
    const source = try check(self, input, .f32, limits);
    const index = try check(self, indices, .i32, limits);
    if (!std.mem.eql(i64, source.shape, input_shape)) return error.InvalidResidentTrainingShape;
    const rows: usize = @intCast(input_shape[0]);
    const proof = try resident.IndexBounds.of(index.resident_indices orelse return error.UnsupportedResidentTrainingIndexProof);
    try proof.validate(rows);
    const rank = index.shape.len + input_shape.len - 1;
    if (rank > 8) return error.InvalidResidentTrainingShape;
    var shape: [8]i64 = undefined;
    @memcpy(shape[0..index.shape.len], index.shape);
    @memcpy(shape[index.shape.len..rank], input_shape[1..]);
    _ = try resident.shapeElements(i64, shape[0..rank], limits);
    var request = boundary.Kernel{ .kind = .gather_i32_exact };
    request.inputs[0] = input;
    request.inputs[1] = indices;
    request.dims[0] = @intCast(rows);
    request.dims[1] = @intCast(source.elem_count / rows);
    request.dims[2] = @intCast(index.elem_count);
    const result = try kernel(self, request);
    errdefer api.freeTensor(self, result);
    return setShape(self, result, shape[0..rank], limits);
}

fn scatter(self: *Compute, values: CT, indices: CT, input_shape: []const i64, output_shape: []const i64, axis: u8, reduction: @import("ml").graph.node.ScatterReduction, padding_index: ?u32, limits: resident.Limits, control: ?Control) !CT {
    if (axis != 0 or input_shape.len != 2 or output_shape.len != 2) return error.UnsupportedResidentTrainingPrimitive;
    const source = try check(self, values, .f32, limits);
    const index = try check(self, indices, .i32, limits);
    const input_count = try resident.shapeElements(i64, input_shape, limits);
    const output_count = try resident.shapeElements(i64, output_shape, limits);
    if (input_count != source.elem_count or input_shape[1] != output_shape[1] or input_shape[0] != index.elem_count) return error.InvalidResidentTrainingShape;
    _ = try program.scatterWork(output_count, input_count, reduction, limits);
    if (reduction != .serial_v1 and self.training_math == null) return error.CudaKernelUnavailable;
    var groups = try @import("../resident_training_groups.zig").build(self.allocator, index.resident_indices orelse return error.UnsupportedResidentTrainingIndexProof, @intCast(output_shape[0]), .{ .max_metadata_bytes = limits.max_index_metadata_bytes }, control);
    defer groups.deinit();
    const rows = try upload(self, i32, groups.rows, &.{@intCast(groups.rows.len)}, limits);
    defer api.freeTensor(self, rows);
    const offsets = try upload(self, i32, groups.offsets, &.{@intCast(groups.offsets.len)}, limits);
    defer api.freeTensor(self, offsets);
    const order = try upload(self, i32, groups.order, &.{@intCast(groups.order.len)}, limits);
    defer api.freeTensor(self, order);
    var zero = boundary.Kernel{ .kind = .fill_zero };
    zero.dims[0] = @intCast(output_count);
    const result = try kernel(self, zero);
    errdefer api.freeTensor(self, result);
    var request = boundary.Kernel{ .kind = .scatter_grouped_i32 };
    request.inputs[0..4].* = .{ values, rows, offsets, order };
    request.dims[0..4].* = .{ @intCast(output_shape[0]), @intCast(output_shape[1]), @intCast(index.elem_count), @intCast(groups.rows.len) };
    if (reduction != .serial_v1) {
        try self.training_math.?.launchScatter(&self.ctx, tensor(result).buffer, source.buffer, tensor(rows).buffer, tensor(offsets).buffer, tensor(order).buffer, groups.rows.len, @intCast(output_shape[1]), index.elem_count, @intCast(output_shape[0]), reduction, padding_index);
        self.stats.launch_other += 1;
        self.boundary_scope.stats.dispatches +|= 1;
    } else try kernelInto(self, request, result);
    return setShape(self, result, output_shape, limits);
}

pub fn residentTrainingPrimitive(ctx: *anyopaque, request: *const resident.Request, limits: resident.Limits, control: ?Control) anyerror!CT {
    if (control) |c| try c.check();
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    return switch (request.*) {
        .upload_f32 => |r| upload(self, f32, r.values, r.shape, limits),
        .upload_i32 => |r| upload(self, i32, r.values, r.shape, limits),
        .snapshot => |r| copy(self, r.input, r.shape, limits),
        .reshape => |r| view(self, r.input, r.shape, limits),
        .gather => |r| gather(self, r.input, r.indices, r.input_shape, r.axis, limits),
        .scatter_add => |r| scatter(self, r.values, r.indices, r.input_shape, r.output_shape, r.axis, .serial_v1, null, limits, control),
    };
}

pub fn snapshotTensorShape(ctx: *anyopaque, input: CT, shape: []const i32) anyerror!CT {
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    return copy(self, input, shape, .{});
}

pub fn glinerBoundaryDownload(ctx: *anyopaque, input: CT, output: []f32) anyerror!void {
    const self = backend(ctx);
    const t = try check(self, input, .f32, .{});
    if (t.elem_count != output.len) return error.InvalidBoundaryDeviceShape;
    try api.synchronizeAndDrainDeferredDeviceFrees(self);
    try api.copyToHostTracked(self, t.buffer, std.mem.sliceAsBytes(output));
    try api.synchronizeAndDrainDeferredDeviceFrees(self);
}

/// Shared losses provide validated labels and already reduced cotangents.
/// Temporary tensors use the normal training allocator and tracked transfers.
pub fn elementwiseLossGradient(ctx: *anyopaque, request: *const ops.elementwise_loss_math.Request) anyerror!void {
    try request.validate();
    if (request.logits.len == 0) return;
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
    const n = request.logits.len;
    const shape = [_]i32{@intCast(n)};
    const x = try upload(self, f32, request.logits, &shape, .{});
    defer api.freeTensor(self, x);
    const y = try upload(self, f32, request.targets, &shape, .{});
    defer api.freeTensor(self, y);
    const seed = try upload(self, f32, request.cotangents, &shape, .{});
    defer api.freeTensor(self, seed);
    const result = try allocate(self, &.{@intCast(n)}, .f32, .{});
    defer api.freeTensor(self, result);
    if (request.control) |control| try control.check();
    try math.launchElementwiseVjp(&self.ctx, tensor(result).buffer, tensor(x).buffer, tensor(y).buffer, tensor(seed).buffer, n, request.settings);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
    try glinerBoundaryDownload(self, result, request.cotangents);
    for (request.cotangents) |value| if (!std.math.isFinite(value)) return error.NonFiniteElementwiseLossMath;
    if (request.control) |control| try control.check();
}

/// The shared controller owns masks, stable integer descriptors and scalar
/// cotangents. All device temporaries fit the same admitted consistency plan.
pub fn consistencyLossGradient(ctx: *anyopaque, request: *const ops.consistency_loss_math.Request) anyerror!void {
    try request.validate();
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
    _ = try request.plan();
    const n = request.pairs.len;
    const m = request.margins[0].len;
    var owned: [23]?CT = @splat(null);
    defer for (owned) |value| if (value) |ct| api.freeTensor(self, ct);
    owned[0] = try upload(self, f32, request.pairs, &.{@intCast(n)}, .{});
    owned[1] = try upload(self, i32, request.valid, &.{@intCast(n)}, .{});
    for (0..2) |d| {
        const base = 2 + d * 6;
        owned[base] = try upload(self, i32, request.indices[d], &.{@intCast(n)}, .{});
        owned[base + 1] = try upload(self, f32, request.margins[d], &.{@intCast(m)}, .{});
        owned[base + 2] = try upload(self, i32, request.keep[d], &.{@intCast(m)}, .{});
        for ([_][]const i32{ request.groups[d].rows, request.groups[d].offsets, request.groups[d].order }, 0..) |values, i|
            owned[base + 3 + i] = try upload(self, i32, values, &.{@intCast(values.len)}, .{});
    }
    for (0..3) |i| owned[14 + i] = try allocate(self, &.{@intCast(n)}, .f32, .{});
    // 17/18 sums; 19/20 survival cotangents; 21/22 marginal cotangents.
    for (17..23) |i| owned[i] = try allocate(self, &.{@intCast(m)}, .f32, .{});
    var buffers: [23]buffer.DeviceBuffer = undefined;
    for (owned, 0..) |ct, i| buffers[i] = tensor(ct.?).buffer;
    if (request.control) |control| try control.check();
    try math.launchConsistencyPrepare(&self.ctx, .{ buffers[14], buffers[15], buffers[0], buffers[1] }, n);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
    for (0..2) |d| {
        if (request.control) |control| try control.check();
        const base = 2 + d * 6;
        var zero = boundary.Kernel{ .kind = .fill_zero };
        zero.dims[0] = @intCast(m);
        try kernelInto(self, zero, owned[17 + d].?);
        try math.launchScatter(&self.ctx, buffers[17 + d], buffers[15], buffers[base + 3], buffers[base + 4], buffers[base + 5], request.groups[d].rows.len, 1, n, m, .pytorch_gather_v1, null);
        try math.launchConsistencyBoundary(&self.ctx, .{ buffers[19 + d], buffers[21 + d], buffers[17 + d], buffers[base + 1], buffers[base + 2] }, m, request.counts[d], request.weight);
        self.stats.launch_other += 2;
        self.boundary_scope.stats.dispatches +|= 2;
    }
    try math.launchConsistencyPair(&self.ctx, .{ buffers[16], buffers[14], buffers[19], buffers[20], buffers[2], buffers[8], buffers[1] }, n, m);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
    for ([_]usize{ 16, 21, 22 }, request.gradients) |index, output| {
        try glinerBoundaryDownload(self, owned[index].?, output);
        for (output) |value| if (!std.math.isFinite(value)) return error.NonFiniteConsistencyLossMath;
    }
    if (request.control) |control| try control.check();
}

/// Reuse reference sum planning for logsumexp and the shared-proposal VJP.
pub fn listwiseLossGradient(ctx: *anyopaque, request: *const ops.listwise_loss_math.Request) anyerror!void {
    try request.validate();
    if (request.logits.len == 0) {
        @memset(request.gradient, 0);
        return;
    }
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
    const rows = try std.math.mul(usize, request.batch, request.queries);
    const n = request.logits.len;
    const geometry = try @import("listwise_plan.zig").Plan.init(request.batch, request.queries, request.candidates, request.reduce_queries, math.reductionDevice());
    const row_plan = geometry.row;
    const query_plan = geometry.query;
    const scratch_bytes = geometry.scratch_bytes;
    var owned: [11]?CT = @splat(null);
    defer for (owned) |value| if (value) |ct| api.freeTensor(self, ct);
    owned[0] = try upload(self, f32, request.logits, &.{@intCast(n)}, .{});
    owned[1] = try upload(self, i32, request.masks, &.{@intCast(n)}, .{});
    owned[2] = try upload(self, f32, request.maxima, &.{@intCast(rows * 2)}, .{});
    owned[3] = try upload(self, f32, request.seeds, &.{@intCast(rows)}, .{});
    for ([_]usize{ n, n, rows, rows, n }, 4..) |count, i| owned[i] = try allocate(self, &.{@intCast(count)}, .f32, .{});
    if (scratch_bytes != 0) owned[9] = try allocate(self, &.{@intCast(scratch_bytes / 4)}, .f32, .{});
    if (query_plan != null) owned[10] = try allocate(self, &.{@intCast(request.gradient.len)}, .f32, .{});
    var buffers: [11]@import("buffer.zig").DeviceBuffer = @splat(.{});
    for (owned, 0..) |value, i| if (value) |ct| {
        buffers[i] = tensor(ct).buffer;
    };
    if (request.control) |control| try control.check();
    try math.launchListwisePrepare(&self.ctx, .{ buffers[4], buffers[5], buffers[0], buffers[1], buffers[2], buffers[3] }, rows, request.candidates, request.queries, request.candidate_major);
    try math.launchReduction(&self.ctx, row_plan, buffers[6], buffers[4], buffers[9]);
    try math.launchReduction(&self.ctx, row_plan, buffers[7], buffers[5], buffers[9]);
    try math.launchListwiseVjp(&self.ctx, .{ buffers[8], buffers[0], buffers[1], buffers[2], buffers[3], buffers[6], buffers[7] }, rows, request.candidates, request.queries, request.candidate_major, request.reduce_queries);
    var launches: usize = 2 + 2 * (if (row_plan.config.ctas > 1) @as(usize, 2) else 1);
    if (query_plan) |plan| {
        try math.launchReduction(&self.ctx, plan, buffers[10], buffers[8], buffers[9]);
        launches += if (plan.config.ctas > 1) @as(usize, 2) else 1;
    }
    self.stats.launch_other += launches;
    self.boundary_scope.stats.dispatches +|= launches;
    try glinerBoundaryDownload(self, owned[if (query_plan != null) 10 else 8].?, request.gradient);
    for (request.gradient) |value| if (!std.math.isFinite(value)) return error.NonFiniteListwiseLossMath;
    if (request.control) |control| try control.check();
}

/// The shared controller preserves row/target identities and reduced seeds.
/// All temporaries and transfers follow the same admitted plan as the caller.
pub fn recordLossGradient(ctx: *anyopaque, request: *const ops.record_loss_math.Request) anyerror!void {
    try request.validate();
    if (request.logits.len == 0) return;
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
    const rows = request.seeds.len;
    const n = request.logits.len;
    const plan = try @import("record_loss_plan.zig").Plan.init(rows, request.width, math.reductionDevice());
    var owned: [11]?CT = @splat(null);
    defer for (owned) |value| if (value) |ct| api.freeTensor(self, ct);
    owned[0] = try upload(self, f32, request.logits, &.{@intCast(n)}, .{});
    owned[1] = try upload(self, i32, request.masks, &.{@intCast(n)}, .{});
    owned[2] = try upload(self, i32, request.target_columns, &.{@intCast(rows)}, .{});
    owned[3] = try upload(self, f32, request.seeds, &.{@intCast(rows)}, .{});
    for ([_]usize{ n, n, rows, n, n, rows }, 4..) |count, i| owned[i] = try allocate(self, &.{@intCast(count)}, .f32, .{});
    if (plan.scratch_bytes != 0) owned[10] = try allocate(self, &.{@intCast(plan.scratch_bytes / 4)}, .f32, .{});
    var buffers: [11]@import("buffer.zig").DeviceBuffer = @splat(.{});
    for (owned, 0..) |value, i| if (value) |ct| {
        buffers[i] = tensor(ct).buffer;
    };
    if (request.control) |control| try control.check();
    try math.launchLogSoftmax(&self.ctx, buffers[4], buffers[0], buffers[4], rows, request.width, false);
    try math.launchRecord(&self.ctx, .prepare, &.{ buffers[5], buffers[4], buffers[1], buffers[2] }, rows, request.width);
    try math.launchReduction(&self.ctx, plan.row, buffers[6], buffers[5], buffers[10]);
    try math.launchRecord(&self.ctx, .vjp, &.{ buffers[7], buffers[9], buffers[4], buffers[1], buffers[2], buffers[6], buffers[3] }, rows, request.width);
    try math.launchLogSoftmax(&self.ctx, buffers[8], buffers[7], buffers[4], rows, request.width, true);
    try math.launchRecord(&self.ctx, .mask, &.{ buffers[8], buffers[1], buffers[3] }, rows, request.width);
    const launches: usize = 5 + @as(usize, if (plan.row.config.ctas > 1) 2 else 1);
    self.stats.launch_other += launches;
    self.boundary_scope.stats.dispatches +|= launches;
    try glinerBoundaryDownload(self, owned[8].?, request.gradient);
    try glinerBoundaryDownload(self, owned[9].?, request.losses);
    for (request.gradient) |value| if (!std.math.isFinite(value)) return error.NonFiniteRecordLossMath;
    for (request.losses) |value| if (!std.math.isFinite(value)) return error.NonFiniteRecordLossMath;
    if (request.control) |control| try control.check();
}

pub fn glinerBoundaryScope(ctx: *anyopaque, request: *const boundary.ScopeRequest) anyerror!boundary.ScopeStats {
    const self = backend(ctx);
    switch (request.*) {
        .snapshot => {},
        // Do not claim support for retained command scopes until physical
        // buffer retention and pending-byte admission are implemented.
        .begin => return error.UnsupportedGlinerBoundaryScope,
        inline .finish, .cancel => |r| {
            try self.boundary_scope.validateGeneration(r.generation);
            return error.GlinerBoundaryScopeNotActive;
        },
    }
    return self.boundary_scope.stats;
}

fn validateMatrix(self: *Compute, ct: CT, rows: usize, columns: usize, precision: boundary.WeightPrecision) !void {
    const t = tensor(ct);
    if (t.resident_owner != self) return error.ForeignResidentTrainingTensor;
    const bytes = try precision.byteLen(rows, columns);
    if (!std.mem.eql(i64, t.shape, &.{ @intCast(rows), @intCast(columns) }) or
        t.elem_count != try std.math.mul(usize, rows, columns) or t.buffer.ptr == 0 or t.buffer.len < bytes)
        return error.InvalidGlinerBoundaryWeightShape;
    const valid = switch (precision) {
        .f32 => t.dtype == .f32 and t.quant_type == null,
        .f16 => t.dtype == .f16 and t.quant_type == null,
        .q8_0, .q4_0, .q4_k => if (t.quant_type) |q| switch (q) {
            .known => |k| k == @as(@TypeOf(k), switch (precision) {
                .q8_0 => .Q8_0,
                .q4_0 => .Q4_0,
                .q4_k => .Q4_K,
                else => unreachable,
            }),
            else => false,
        } else false,
    };
    if (!valid) return error.UnsupportedGlinerBoundaryPrecision;
}

pub fn glinerBoundaryDevice(ctx: *anyopaque, request: *const boundary.Request) anyerror!CT {
    const self = backend(ctx);
    if (self.kernels.gliner25_boundary_f32 == null) return error.CudaKernelUnavailable;
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    return switch (request.*) {
        .upload_f32 => |r| upload(self, f32, r.values, r.shape, .{}),
        .upload_i32 => |r| upload(self, i32, r.values, r.shape, .{}),
        .kernel => |r| kernel(self, r),
        .load_f32_weight => |r| blk: {
            const value = try api.acquireWeight(self, r.name);
            errdefer api.freeTensor(self, value);
            const t = try check(self, value, .f32, .{});
            if (!std.mem.eql(i64, t.shape, r.shape)) return error.InvalidGlinerBoundaryWeightShape;
            self.boundary_scope.stats.resident_weight_acquires +|= 1;
            break :blk value;
        },
        .load_matrix => |r| blk: {
            const value = try api.acquireWeight(self, r.name);
            errdefer api.freeTensor(self, value);
            try validateMatrix(self, value, r.rows, r.columns, r.precision);
            break :blk value;
        },
        .linear_reduced => |r| blk: {
            const input = try check(self, r.input, .f32, .{});
            const bias = try check(self, r.bias, .f32, .{});
            if (r.rows == 0 or r.in_dim == 0 or r.out_dim == 0 or
                input.elem_count != try std.math.mul(usize, r.rows, r.in_dim) or bias.elem_count != r.out_dim)
                return error.InvalidBoundaryDeviceShape;
            try validateMatrix(self, r.weight, r.out_dim, r.in_dim, r.precision);
            const result = try api.linear(self, r.input, r.weight, r.bias, r.rows, r.in_dim, r.out_dim);
            tensor(result).resident_owner = self;
            tensor(result).strict_resident_training = true;
            break :blk result;
        },
        .embedding_reduced => |r| blk: {
            try validateMatrix(self, r.weight, r.vocabulary, r.width, r.precision);
            if (r.ids.len == 0 or r.ids.len > std.math.maxInt(i32)) return error.InvalidBoundaryDeviceShape;
            for (r.ids) |id| if (id < 0 or id >= r.vocabulary) return error.InvalidBoundaryRouting;
            const result = try api.embeddingLookup(self, r.weight, r.ids, r.ids.len, r.width);
            tensor(result).resident_owner = self;
            tensor(result).strict_resident_training = true;
            break :blk result;
        },
        .resident_f32 => |r| blk: {
            const t = try check(self, r.input, if (r.source_precision == .f16) .f16 else .f32, .{});
            if (r.source_precision == .f16) {
                var cast = boundary.Kernel{ .kind = .cast_half };
                cast.inputs[0] = r.input;
                cast.dims[0] = @intCast(t.elem_count);
                break :blk try kernel(self, cast);
            }
            var shape: [8]i32 = undefined;
            for (t.shape, 0..) |d, i| shape[i] = @intCast(d);
            break :blk try view(self, r.input, shape[0..t.shape.len], .{});
        },
        .linear => |r| blk: {
            const input = try check(self, r.input, .f32, .{});
            const weight = try check(self, r.weight, .f32, .{});
            const bias = try check(self, r.bias, .f32, .{});
            if (r.rows == 0 or r.in_dim == 0 or r.out_dim == 0 or
                input.elem_count != try std.math.mul(usize, r.rows, r.in_dim) or
                weight.elem_count != try std.math.mul(usize, r.out_dim, r.in_dim) or bias.elem_count != r.out_dim)
                return error.InvalidBoundaryDeviceShape;
            const result = try api.linear(ctx, r.input, r.weight, r.bias, r.rows, r.in_dim, r.out_dim);
            tensor(result).resident_owner = self;
            tensor(result).strict_resident_training = true;
            self.boundary_scope.stats.dispatches +|= 1;
            break :blk result;
        },
        else => error.UnsupportedGlinerBoundaryDevice,
    };
}

/// Ordered per-tensor FP32 norms, followed by one FP32 vector norm. The shared
/// optimizer retains its separate scaled finite/state checks. Only a scalar
/// leaves the device; input gradient tensors remain borrowed and unchanged.
fn pytorchTrainingNorm(ctx: *anyopaque, inputs: []const resident.NormInput, limits: resident.NormLimits, control: ?Control) !resident.NormSummary {
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    if (control) |c| try c.check();
    if (inputs.len > limits.max_tensors or inputs.len > 16384) return error.ResourceLimitExceeded;
    var total: usize = 0;
    var largest: usize = 0;
    for (inputs) |input| {
        const t = try check(self, input.tensor, .f32, limits.primitive);
        if (t.elem_count != input.elem_count or input.elem_count == 0) return error.InvalidResidentTrainingShape;
        total = try std.math.add(usize, total, t.elem_count);
        largest = @max(largest, t.elem_count);
    }
    if (total > limits.max_total_elements) return error.ResourceLimitExceeded;
    if (inputs.len == 0) return .{ .sum_squares = 0, .norm = 0, .finite = true, .tensor_count = 0, .partial_bytes = 0, .download_bytes = 0 };
    const chunks = try std.math.divCeil(usize, largest, 65536);
    const partial_bytes = try resident.pytorchNormScratch(inputs.len, largest);
    if (partial_bytes > limits.max_partial_bytes) return error.ResourceLimitExceeded;
    const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
    const partial = try allocate(self, &.{@intCast(chunks)}, .f32, limits.primitive);
    defer api.freeTensor(self, partial);
    const values = try allocate(self, &.{@intCast(inputs.len)}, .f32, limits.primitive);
    defer api.freeTensor(self, values);
    const result = try allocate(self, &.{1}, .f32, limits.primitive);
    defer api.freeTensor(self, result);
    for (inputs, 0..) |input, i| {
        if (control) |c| try c.check();
        try math.launchNorm(&self.ctx, .chunks, tensor(partial).buffer, tensor(input.tensor).buffer, input.elem_count);
        const count = (input.elem_count + 65535) / 65536;
        // PyTorch pads each tensor's partials with positive zeros to a common
        // width. Omitting that suffix retains every nonzero addition's order.
        const output = buffer.DeviceBuffer{ .ptr = tensor(values).buffer.ptr + i * 4, .len = 4 };
        try math.launchNorm(&self.ctx, .finish, output, tensor(partial).buffer, count);
        self.stats.launch_other += 2;
        self.boundary_scope.stats.dispatches +|= 2;
    }
    try math.launchNorm(&self.ctx, .total, tensor(result).buffer, tensor(values).buffer, inputs.len);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
    var scalar: [1]f32 = undefined;
    try glinerBoundaryDownload(ctx, result, &scalar);
    if (control) |c| try c.check();
    const value: f64 = scalar[0];
    return .{ .sum_squares = value * value, .norm = value, .finite = std.math.isFinite(value), .tensor_count = inputs.len, .partial_bytes = partial_bytes, .download_bytes = 4 };
}

pub fn residentTrainingValidate(ctx: *anyopaque, inputs: []const resident.ValidationInput, limits: resident.ValidationLimits, control: ?Control) anyerror!resident.ValidationSummary {
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    if (control) |c| try c.check();
    if (inputs.len > limits.max_tensors or inputs.len > 16384) return error.ResourceLimitExceeded;
    var total: usize = 0;
    var chunks: usize = 0;
    // Complete ownership, dtype, byte extent and work admission before scratch
    // allocation or any launch. No payload or launch metadata is uploaded.
    for (inputs) |input| {
        const t = try check(self, input.tensor, .f32, limits.primitive);
        if (t.elem_count != input.elem_count) return error.InvalidResidentTrainingShape;
        total = std.math.add(usize, total, input.elem_count) catch return error.ResourceLimitExceeded;
        chunks = std.math.add(usize, chunks, try resident.validationChunks(input.elem_count)) catch return error.ResourceLimitExceeded;
    }
    const partial_bytes = try resident.validationScratch(inputs.len, chunks);
    if (total > limits.max_total_elements or partial_bytes > limits.max_partial_bytes) return error.ResourceLimitExceeded;
    if (inputs.len == 0) return .{ .finite = true, .all_zero = true, .tensor_count = 0, .partial_bytes = 0, .download_bytes = 0 };
    if (self.kernels.training_validate_f32 == null or self.kernels.training_validate_finish == null) return error.CudaKernelUnavailable;
    const scratch = try allocate(self, &.{@intCast(partial_bytes / 4)}, .i32, limits.primitive);
    defer api.freeTensor(self, scratch);
    const partials = buffer.DeviceBuffer{ .ptr = tensor(scratch).buffer.ptr, .len = chunks * 4 };
    const result = buffer.DeviceBuffer{ .ptr = partials.ptr + partials.len, .len = 4 };
    var start: usize = 0;
    var offset: usize = 0;
    while (start < inputs.len) {
        if (control) |c| try c.check();
        var batch: @import("kernels.zig").KernelModule.TrainingValidationBatch = .{};
        const end = @min(start + batch.entries.len, inputs.len);
        var blocks: usize = 0;
        for (inputs[start..end], 0..) |input, i| {
            batch.entries[i] = .{ .pointer = tensor(input.tensor).buffer.ptr, .count = @intCast(input.elem_count), .first_block = @intCast(blocks) };
            blocks += try resident.validationChunks(input.elem_count);
        }
        const output = buffer.DeviceBuffer{ .ptr = partials.ptr + offset * 4, .len = blocks * 4 };
        try self.kernels.launchTrainingValidation(&self.ctx, output, &batch, end - start, blocks);
        self.stats.launch_other += 1;
        self.boundary_scope.stats.dispatches +|= 1;
        offset += blocks;
        start = end;
    }
    std.debug.assert(offset == chunks);
    if (control) |c| try c.check();
    try self.kernels.finishTrainingValidation(&self.ctx, result, partials, chunks);
    self.stats.launch_other += 1;
    self.boundary_scope.stats.dispatches +|= 1;
    var flags: u32 = 0;
    try api.copyToHostTracked(self, result, std.mem.asBytes(&flags));
    try api.synchronizeAndDrainDeferredDeviceFrees(self);
    if (control) |c| try c.check();
    if (flags & ~@as(u32, 3) != 0) return error.InvalidDeviceOptimizerResult;
    return .{ .finite = flags & 1 == 0, .all_zero = flags == 0, .tensor_count = inputs.len, .partial_bytes = partial_bytes, .download_bytes = 4 };
}

pub fn residentTrainingNorm(ctx: *anyopaque, inputs: []const resident.NormInput, limits: resident.NormLimits, control: ?Control) anyerror!resident.NormSummary {
    if (limits.profile == .pytorch_f32) return pytorchTrainingNorm(ctx, inputs, limits, control);
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    if (control) |c| try c.check();
    if (inputs.len > limits.max_tensors or inputs.len > 16384) return error.ResourceLimitExceeded;
    var total: usize = 0;
    var partial_bytes: usize = 0;
    for (inputs) |input| {
        const t = try check(self, input.tensor, .f32, limits.primitive);
        if (t.elem_count != input.elem_count) return error.InvalidResidentTrainingShape;
        total = try std.math.add(usize, total, t.elem_count);
        partial_bytes = try std.math.add(usize, partial_bytes, try std.math.mul(usize, try std.math.divCeil(usize, t.elem_count, 1024), 12));
    }
    if (total > limits.max_total_elements or partial_bytes > limits.max_partial_bytes) return error.ResourceLimitExceeded;
    if (inputs.len == 0) return .{ .sum_squares = 0, .norm = 0, .finite = true, .tensor_count = 0, .partial_bytes = 0, .download_bytes = 0 };
    // Keep the per-tensor reduction and host summation order unchanged, but
    // collect the summaries on the stream before doing one bounded readback.
    const summaries = try allocate(self, &.{@intCast(inputs.len * 3)}, .f32, .{});
    defer api.freeTensor(self, summaries);
    const host_summaries = try self.allocator.alloc(f32, inputs.len * 3);
    defer self.allocator.free(host_summaries);
    var sum_squares: f64 = 0;
    var finite = true;
    for (inputs, 0..) |input, i| {
        if (control) |c| try c.check();
        var request = boundary.Kernel{ .kind = .norm_chunks };
        request.inputs[0] = input.tensor;
        request.dims[0..2].* = .{ @intCast(input.elem_count), 1024 };
        const partial = try kernel(self, request);
        defer api.freeTensor(ctx, partial);
        request = .{ .kind = .norm_merge };
        request.inputs[0] = partial;
        request.dims[0] = @intCast(tensor(partial).elem_count / 3);
        var merged_shape = [_]i64{3};
        var merged = Tensor{
            .buffer = .{ .ptr = tensor(summaries).buffer.ptr + i * 12, .len = 12 },
            .dtype = .f32,
            .shape = &merged_shape,
            .elem_count = 3,
            .resident_owner = self,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        };
        try kernelInto(self, request, &merged);
    }
    try glinerBoundaryDownload(ctx, summaries, host_summaries);
    if (control) |c| try c.check();
    for (0..inputs.len) |i| {
        const summary = host_summaries[i * 3 ..][0..3];
        const scale: f64 = summary[0];
        const scaled_sum: f64 = summary[1];
        finite = finite and summary[2] == 0 and std.math.isFinite(scale) and std.math.isFinite(scaled_sum);
        sum_squares += scale * scale * scaled_sum;
    }
    return .{ .sum_squares = sum_squares, .norm = @sqrt(sum_squares), .finite = finite, .tensor_count = inputs.len, .partial_bytes = partial_bytes, .download_bytes = inputs.len * 12 };
}

/// The seeded resident optimizer publishes a cleared accumulation buffer with
/// its new epoch. Legacy CUDA callers retain their existing gradient contract.
pub fn trainingAdamWManyF32(ctx: *anyopaque, inputs: []const ops.TrainingAdamWBatchInput, options: ops.TrainingAdamWBatchOptions) anyerror!void {
    const self = backend(ctx);
    var strict = false;
    for (inputs) |input| strict = strict or tensor(input.grad).strict_resident_training;
    if (strict) for (inputs) |input| {
        for ([_]CT{ input.weight, input.grad, input.m, input.v }) |value| {
            const t = try check(self, value, .f32, .{});
            if (t.elem_count != input.elem_count) return error.InvalidResidentTrainingShape;
        }
    };
    if (options.pytorch_fused) |profile| {
        const math = if (self.training_math) |*value| value else return error.CudaKernelUnavailable;
        if (options.grad_scale != 1 or @as(f32, @floatCast(profile.lr)) != options.lr or
            !std.meta.eql(profile.optimizer.cast(f32), @import("ml").graph.optimizers.AdamWConfig{ .beta1 = options.beta1, .beta2 = options.beta2, .eps = options.eps, .weight_decay = options.weight_decay })) return error.InvalidOptimizerState;
        for (inputs) |input| {
            var buffers: [4]@import("buffer.zig").DeviceBuffer = undefined;
            for ([_]CT{ input.weight, input.grad, input.m, input.v }, &buffers) |value, *device_buffer| {
                const t = try check(self, value, .f32, .{});
                if (t.elem_count != input.elem_count) return error.InvalidResidentTrainingShape;
                device_buffer.* = t.buffer;
            }
            try math.launchAdamW(&self.ctx, buffers, input.elem_count, input.adam_step, profile.lr, profile.optimizer);
            self.boundary_scope.stats.dispatches +|= 1;
        }
    } else try api.trainingAdamWManyF32(ctx, inputs, options);
    if (strict) for (inputs) |input| {
        var zero = boundary.Kernel{ .kind = .fill_zero };
        zero.dims[0] = @intCast(input.elem_count);
        try kernelInto(self, zero, input.grad);
    };
}

pub fn residentTrainingInstruction(ctx: *anyopaque, instruction: *const program.Instruction, inputs: []const CT, limits: program.Limits, control: ?Control) anyerror!CT {
    const self = backend(ctx);
    if (try self.ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
    if (control) |c| try c.check();
    const geometry = try instruction.validate(limits);
    if (inputs.len != instruction.num_inputs) return error.InvalidResidentProgramShape;
    for (inputs, 0..) |input, i| {
        const expected = instruction.inputs[i];
        const t = try check(self, input, if (expected.dtype == .i32) .i32 else .f32, limits.primitive);
        if (!std.mem.eql(i64, t.shape, expected.dims[0..expected.rank_])) return error.InvalidResidentProgramShape;
    }
    const out = instruction.output;
    const shape = out.dims[0..out.rank_];
    var dims: [8]i32 = undefined;
    for (shape, 0..) |d, i| dims[i] = @intCast(d);
    const result = switch (instruction.op) {
        .fused_linear => |attrs| blk: {
            if (self.training_blas == null or self.cublaslt == null) return error.CublasLtUnavailable;
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(ctx, output);
            const dst = tensor(output).buffer;
            if (geometry.scratch_bytes != 0) {
                const workspace = try allocate(self, &.{@intCast(geometry.scratch_bytes / 4)}, .f32, limits.primitive);
                defer api.freeTensor(ctx, workspace);
                try self.cublaslt.?.matmulF32BiasF32Out(&self.ctx, dst, tensor(inputs[0]).buffer, tensor(inputs[1]).buffer, tensor(inputs[2]).buffer, tensor(workspace).buffer, attrs.rows, attrs.in_dim, attrs.out_dim);
            } else {
                // Match the reference's degenerate addmm path: initialize C
                // from bias, then let SGEMM accumulate with beta=1.
                try self.kernels.launchPrimitiveBroadcastF32(&self.ctx, dst, tensor(inputs[2]).buffer, attrs.out_dim, geometry.output_elements, .{ attrs.out_dim, 1, 1, 1, 1, 1, 1, 1 }, .{ attrs.rows, attrs.out_dim, 1, 1, 1, 1, 1, 1 }, .{ 1, 0, 0, 0, 0, 0, 0, 0 }, 1, 2);
                self.stats.launch_other += 1;
                if (control) |c| try c.check();
                try self.training_blas.?.linear(self.ctx.stream, @ptrFromInt(dst.ptr), @ptrFromInt(tensor(inputs[0]).buffer.ptr), @ptrFromInt(tensor(inputs[1]).buffer.ptr), attrs.rows, attrs.in_dim, attrs.out_dim, true);
            }
            self.stats.launch_linear += 1;
            break :blk output;
        },
        .fused_layer_norm, .fused_layer_norm_backward => |attrs| blk: {
            const backward = instruction.op == .fused_layer_norm_backward;
            const rows: u32 = @intCast(geometry.input_elements[0] / attrs.dim);
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(ctx, output);
            var buffers: [6]buffer.DeviceBuffer = .{ tensor(output).buffer, tensor(inputs[0]).buffer, tensor(inputs[1]).buffer, tensor(inputs[2]).buffer, .{}, .{} };
            if (backward) {
                const stats = try allocate(self, &.{ 2, rows }, .f32, limits.primitive);
                defer api.freeTensor(ctx, stats);
                buffers[4] = tensor(inputs[3]).buffer;
                buffers[5] = tensor(stats).buffer;
                try self.kernels.launchGliner25LayerNorm(&self.ctx, buffers, rows, attrs.dim, attrs.eps, 1);
                if (control) |c| try c.check();
                try self.kernels.launchGliner25LayerNorm(&self.ctx, buffers, rows, attrs.dim, attrs.eps, 2);
                self.stats.launch_other += 2;
                if (self.layer_norm_backward_observer) |observer|
                    try observer.observe(observer.context, .{ inputs[0], inputs[1], inputs[2], inputs[3] }, output, rows, attrs.dim, attrs.eps);
            } else {
                try self.kernels.launchGliner25LayerNorm(&self.ctx, buffers, rows, attrs.dim, attrs.eps, 0);
                self.stats.launch_other += 1;
            }
            break :blk output;
        },
        .fused_boundary_training_attention_v1, .fused_boundary_training_attention_backward_v1 => |attrs| blk: {
            const module = if (self.boundary_attention) |*value| value else return error.CudaKernelUnavailable;
            const backward = instruction.op == .fused_boundary_training_attention_backward_v1;
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(ctx, output);
            const scratch = try allocate(self, &.{@intCast(geometry.scratch_bytes / 4)}, .f32, limits.primitive);
            defer api.freeTensor(ctx, scratch);
            try module.execute(&self.ctx, attrs, backward, tensor(output).buffer, tensor(inputs[0]).buffer, tensor(inputs[1]).buffer, if (backward) tensor(inputs[2]).buffer else .{}, if (backward) tensor(inputs[3]).buffer else .{}, tensor(scratch).buffer);
            self.stats.launch_other += if (backward) @as(usize, 3) else 2;
            break :blk output;
        },
        .fused_deberta_training_attention_v1, .fused_deberta_training_attention_backward_v1 => |attrs| try trainingAttention(self, inputs, attrs, instruction.op == .fused_deberta_training_attention_backward_v1, limits, control),
        .reshape, .stop_gradient, .convert_dtype => try view(self, inputs[0], dims[0..shape.len], limits.primitive),
        .gather => try gather(self, inputs[0], inputs[1], tensor(inputs[0]).shape, 0, limits.primitive),
        .scatter_add => |attrs| blk: {
            const v: usize = if (inputs.len == 3) 1 else 0;
            const rows = shape[0];
            const width: i64 = @intCast(geometry.output_elements / @as(usize, @intCast(rows)));
            const count: i64 = @intCast(tensor(inputs[v + 1]).elem_count);
            const scattered = try scatter(self, inputs[v], inputs[v + 1], &.{ count, width }, &.{ rows, width }, 0, attrs.reduction, attrs.padding_index, limits.primitive, control);
            if (inputs.len == 2) break :blk scattered;
            defer api.freeTensor(ctx, scattered);
            _ = try setShape(self, scattered, shape, limits.primitive);
            break :blk try api.add(ctx, inputs[0], scattered);
        },
        .neg => try api.primNegateOp(ctx, inputs[0]),
        .sqrt => try api.primSqrtOp(ctx, inputs[0]),
        .rsqrt => try api.primRsqrtOp(ctx, inputs[0]),
        .exp => try api.primExpOp(ctx, inputs[0]),
        .abs => try api.primAbsOp(ctx, inputs[0]),
        .frozen_span_features_v1 => |attrs| blk: {
            const math = if (self.training_math) |*module| module else return error.CudaKernelUnavailable;
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(self, output);
            try math.launchSpanFeatures(&self.ctx, attrs, tensor(output).buffer, tensor(inputs[0]).buffer, tensor(inputs[1]).buffer);
            self.stats.launch_other += 1;
            break :blk output;
        },
        .fused_prefix_scan_v1 => |attrs| blk: {
            const math = if (self.training_math) |*module| module else return error.CudaKernelUnavailable;
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(self, output);
            var scratch: ?CT = null;
            defer if (scratch) |value| api.freeTensor(self, value);
            if (geometry.scratch_bytes != 0) scratch = try allocate(self, &.{@intCast(geometry.scratch_bytes / 4)}, .f32, limits.primitive);
            try math.launchScan(&self.ctx, attrs, tensor(output).buffer, tensor(inputs[0]).buffer, if (scratch) |value| tensor(value).buffer else .{});
            self.stats.launch_other += if (attrs.singleVector()) @as(usize, 2) else 1;
            break :blk output;
        },
        .fused_silu, .fused_silu_backward, .fused_sigmoid, .fused_sigmoid_backward => blk: {
            const backward = instruction.op == .fused_silu_backward or instruction.op == .fused_sigmoid_backward;
            const math = if (self.training_math) |*module| module else return error.CudaKernelUnavailable;
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(self, output);
            const activation: *const @TypeOf(@TypeOf(math.*).launchSigmoid) = if (instruction.op == .fused_sigmoid or instruction.op == .fused_sigmoid_backward)
                @TypeOf(math.*).launchSigmoid
            else
                @TypeOf(math.*).launchSilu;
            try activation(math, &self.ctx, tensor(output).buffer, tensor(inputs[0]).buffer, tensor(inputs[if (backward) 1 else 0]).buffer, geometry.output_elements, backward);
            self.stats.launch_other += 1;
            break :blk output;
        },
        .fused_gelu_exact, .fused_gelu_exact_backward => blk: {
            const backward = instruction.op == .fused_gelu_exact_backward;
            if (self.training_math) |*math| {
                const output = try allocate(self, shape, .f32, limits.primitive);
                errdefer api.freeTensor(self, output);
                try math.launchGelu(&self.ctx, tensor(output).buffer, tensor(inputs[0]).buffer, tensor(inputs[if (backward) 1 else 0]).buffer, geometry.output_elements, backward);
                self.stats.launch_other += 1;
                break :blk output;
            }
            break :blk if (backward) try api.binaryElementwise(ctx, inputs[0], inputs[1], .gelu_exact_backward) else try api.geluExact(ctx, inputs[0]);
        },
        .add => try api.add(ctx, inputs[0], inputs[1]),
        .mul => try api.multiply(ctx, inputs[0], inputs[1]),
        .sub => try api.primSubtractOp(ctx, inputs[0], inputs[1]),
        .div => try api.primDivideOp(ctx, inputs[0], inputs[1]),
        .less_than => try api.primLessThanOp(ctx, inputs[0], inputs[1]),
        .where_select => try api.primWhereSelectOp(ctx, inputs[0], inputs[1], inputs[2]),
        .fused_softmax, .fused_softmax_backward => |r| blk: {
            const backward = instruction.op == .fused_softmax_backward;
            if (!backward and !r.fuse_backward) break :blk try api.primSoftmaxOp(ctx, inputs[0], r.dim);
            const output = try allocate(self, shape, .f32, limits.primitive);
            errdefer api.freeTensor(self, output);
            try self.kernels.launchGliner25Softmax(&self.ctx, .{
                tensor(output).buffer,
                tensor(inputs[0]).buffer,
                tensor(inputs[if (backward) 1 else 0]).buffer,
            }, @intCast(tensor(output).elem_count / r.dim), r.dim, backward);
            self.stats.launch_other += 1;
            break :blk output;
        },
        .transpose => |r| try api.transposeOp(ctx, inputs[0], r.perm[0..r.num_axes], tensor(inputs[0]).shape),
        .broadcast_in_dim => |r| try api.primBroadcastInDimOp(ctx, inputs[0], shape, r.broadcast_axes[0..r.num_axes], tensor(inputs[0]).shape),
        .slice => |r| try api.primSliceOp(ctx, inputs[0], r.starts[0..r.num_axes], r.limits[0..r.num_axes], r.strides[0..r.num_axes], tensor(inputs[0]).shape),
        .reduce_sum, .reduce_mean => |r| blk: {
            if (geometry.cuda_reduction) |reduction| {
                const math = if (self.training_math) |*module| module else return error.CudaKernelUnavailable;
                if (!std.meta.eql(limits.cuda_reduction.?, math.reductionDevice())) return error.InvalidCudaReductionDevice;
                const output = try allocate(self, shape, .f32, limits.primitive);
                errdefer api.freeTensor(self, output);
                var scratch: ?CT = null;
                defer if (scratch) |value| api.freeTensor(self, value);
                if (reduction.scratch_bytes != 0) scratch = try allocate(self, &.{@intCast(reduction.scratch_bytes / 4)}, .f32, limits.primitive);
                try math.launchReduction(&self.ctx, reduction, tensor(output).buffer, tensor(inputs[0]).buffer, if (scratch) |value| tensor(value).buffer else .{});
                self.stats.launch_other += if (reduction.config.ctas > 1) @as(u64, 2) else 1;
                break :blk output;
            }
            break :blk if (instruction.op == .reduce_mean) try api.primReduceMeanOp(ctx, inputs[0], r.axes[0..r.num_axes], tensor(inputs[0]).shape) else try api.primReduceSumOp(ctx, inputs[0], r.axes[0..r.num_axes], tensor(inputs[0]).shape);
        },
        .reduce_max => |r| try api.primReduceMaxOp(ctx, inputs[0], r.axes[0..r.num_axes], tensor(inputs[0]).shape),
        .concat_prim => |r| try api.primConcatPrimOp(ctx, inputs[0], inputs[1], r.axis, tensor(inputs[0]).shape, tensor(inputs[1]).shape),
        .dot_general => |r| try api.primDotGeneralOp(ctx, inputs[0], inputs[1], tensor(inputs[0]).shape, tensor(inputs[1]).shape, r.lhs_contracting[0..r.num_contracting], r.rhs_contracting[0..r.num_contracting], r.lhs_batch[0..r.num_batch], r.rhs_batch[0..r.num_batch]),
        else => return error.UnsupportedResidentProgramInstruction,
    };
    errdefer api.freeTensor(ctx, result);
    if (control) |c| try c.check();
    return setShape(self, result, shape, limits.primitive);
}

test "CUDA boundary retained views keep physical integer metadata alive after source release" {
    const a = std.testing.allocator;
    var self: Compute = undefined;
    self.allocator = a;
    const source = try a.create(Tensor);
    source.* = .{
        .buffer = .{ .ptr = 0x1000, .len = 8 },
        .dtype = .i32,
        .shape = try a.dupe(i64, &.{2}),
        .elem_count = 2,
        .owns_buffer = false,
        .resident_owner = &self,
        .resident_indices = try a.dupe(i32, &.{ 16_777_216, 16_777_217 }),
    };
    const retained = try view(&self, source, &.{ 1, 2 }, .{});
    defer api.freeTensor(&self, retained);
    try std.testing.expectEqual(@as(usize, 2), source.resident_references);
    api.freeTensor(&self, source);
    try std.testing.expectEqualSlices(i32, &.{ 16_777_216, 16_777_217 }, tensor(retained).resident_indices.?);
    // Reject before touching CUDA: the original upload's allowance does not
    // authorize another copy under a tighter snapshot metadata budget.
    try std.testing.expectError(error.ResourceLimitExceeded, copy(&self, retained, &.{2}, .{ .max_index_metadata_bytes = 7 }));
    try std.testing.expectError(error.InvalidResidentTrainingShape, view(&self, retained, &.{3}, .{}));
    var foreign: Compute = undefined;
    try std.testing.expectError(error.ForeignResidentTrainingTensor, check(&foreign, retained, .i32, .{}));
}

test "CUDA boundary physical budget charges retained cache and evicts before new shape" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    device.backend.resident_training_cache = true;
    const budget = &device.backend.ctx.device_allocations;
    const initial = budget.snapshot().live;
    try budget.setLimit(initial + 32);
    const cb = device.backend.computeBackend();
    const small = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 2, 3, 4 }, .shape = &.{4} } }, .{});
    cb.free(small);
    // Logical release does not release the physical cache reservation.
    try std.testing.expectEqual(initial + 16, budget.snapshot().live);
    const larger = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, .shape = &.{8} } }, .{});
    defer cb.free(larger);
    try std.testing.expectEqual(initial + 32, budget.snapshot().live);
    try std.testing.expect(device.backend.stats.temp_buffer_evictions > 0);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{9}, .shape = &.{1} } }, .{}));
    try std.testing.expectEqual(initial + 32, budget.snapshot().live);
    var actual: [8]f32 = undefined;
    try cb.glinerBoundaryDownload(larger, &actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &actual);
    try std.testing.expect(budget.snapshot().peak <= initial + 32);
}

test "CUDA boundary strict gather grouped scatter snapshot and scaled norm" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const values = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1, 2, 3, 4, 5, 6 }, .shape = &.{ 3, 2 } } }, .{});
    defer cb.free(values);
    // Reject malformed dimensions before cuBLAS can read beyond a buffer.
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, cb.glinerBoundaryDevice(&.{ .linear = .{
        .input = values,
        .weight = values,
        .bias = values,
        .rows = 4,
        .in_dim = 2,
        .out_dim = 3,
    } }));
    try std.testing.expectError(error.InvalidBoundaryRouting, cb.glinerBoundaryDevice(&.{ .embedding_reduced = .{
        .weight = values,
        .ids = &.{3},
        .vocabulary = 3,
        .width = 2,
        .precision = .f32,
    } }));
    const indices = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &.{ 1, -1, 1 }, .shape = &.{3} } }, .{});
    defer cb.free(indices);
    const copied_indices = try cb.residentTrainingPrimitive(&.{ .snapshot = .{ .input = indices, .shape = &.{3} } }, .{ .max_index_metadata_bytes = 12 });
    defer cb.free(copied_indices);
    try std.testing.expectEqualSlices(i32, &.{ 1, -1, 1 }, tensor(copied_indices).resident_indices.?);
    const scattered = try cb.residentTrainingPrimitive(&.{ .scatter_add = .{ .values = values, .indices = indices, .input_shape = &.{ 3, 2 }, .output_shape = &.{ 3, 2 }, .axis = 0 } }, .{});
    defer cb.free(scattered);
    var actual: [6]f32 = undefined;
    try cb.glinerBoundaryDownload(scattered, &actual);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 6, 8, 3, 4 }, &actual);
    const gathered = try cb.residentTrainingPrimitive(&.{ .gather = .{ .input = scattered, .indices = indices, .input_shape = &.{ 3, 2 }, .axis = 0 } }, .{});
    defer cb.free(gathered);
    const snapshot = try cb.snapshotTensorShape(gathered, &.{6});
    defer cb.free(snapshot);
    try cb.glinerBoundaryDownload(snapshot, &actual);
    try std.testing.expectEqualSlices(f32, &.{ 6, 8, 3, 4, 6, 8 }, &actual);
    const summary = try cb.residentTrainingNorm(&.{.{ .tensor = scattered, .elem_count = 6 }}, .{});
    try std.testing.expect(summary.finite);
    try std.testing.expectApproxEqAbs(@as(f64, 125), summary.sum_squares, 1e-5);
    const batched = try cb.residentTrainingNorm(&.{ .{ .tensor = scattered, .elem_count = 6 }, .{ .tensor = values, .elem_count = 6 } }, .{});
    try std.testing.expectApproxEqAbs(@as(f64, 216), batched.sum_squares, 1e-5);
    try std.testing.expectEqual(@as(usize, 24), batched.download_bytes);
    const nonfinite = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{std.math.inf(f32)}, .shape = &.{} } }, .{});
    defer cb.free(nonfinite);
    const rejected = try cb.residentTrainingNorm(&.{ .{ .tensor = values, .elem_count = 6 }, .{ .tensor = nonfinite, .elem_count = 1 } }, .{});
    try std.testing.expect(!rejected.finite);
    const negative_zero = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{-0.0}, .shape = &.{} } }, .{});
    defer cb.free(negative_zero);
    var scalar: [1]f32 = undefined;
    try cb.glinerBoundaryDownload(negative_zero, &scalar);
    try std.testing.expectEqual(@as(u32, 0x80000000), @as(u32, @bitCast(scalar[0])));
}

// Primitive API results acquire resident ownership in the instruction adapter.
// These direct primitive tests read back explicitly after the kernel completes.
fn downloadPrimitiveForTest(self: *Compute, value: CT, output: []f32) !void {
    try tensor(value).buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(output));
    try self.ctx.synchronize();
}

test "CUDA boundary small FP32 dots use BLAS with current input buffers" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const self = device.backend;
    try std.testing.expect(self.training_blas == null);
    try self.enableResidentTrainingBlas();
    const handle = self.training_blas.?.handle;
    try self.enableResidentTrainingBlas();
    try std.testing.expectEqual(handle, self.training_blas.?.handle);
    const input = try upload(self, f32, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 2, 3 }, .{});
    defer api.freeTensor(self, input);
    const weight = try upload(self, f32, &.{ 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1 }, &.{ 4, 3 }, .{});
    defer api.freeTensor(self, weight);
    const output = try api.primDotGeneralOp(self, input, weight, &.{ 2, 3 }, &.{ 4, 3 }, &.{1}, &.{1}, &.{}, &.{});
    defer api.freeTensor(self, output);
    try std.testing.expectEqual(@as(u64, 1), self.training_blas.?.calls);
    var actual: [8]f32 = undefined;
    try downloadPrimitiveForTest(self, output, &actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 6, 4, 5, 6, 15 }, &actual);
    const changed = try upload(self, f32, &.{ 6, 5, 4, 3, 2, 1 }, &.{ 2, 3 }, .{});
    defer api.freeTensor(self, changed);
    const changed_output = try api.primDotGeneralOp(self, changed, weight, &.{ 2, 3 }, &.{ 4, 3 }, &.{1}, &.{1}, &.{}, &.{});
    defer api.freeTensor(self, changed_output);
    try std.testing.expectEqual(@as(u64, 2), self.training_blas.?.calls);
    try downloadPrimitiveForTest(self, changed_output, &actual);
    try std.testing.expectEqualSlices(f32, &.{ 6, 5, 4, 15, 3, 2, 1, 6 }, &actual);
}

test "CUDA boundary batched FP32 dots preserve batch strides and contraction axes" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const self = device.backend;
    try self.enableResidentTrainingBlas();
    const lhs = try upload(self, f32, &.{ 1, 2, 3, 4, 5, 6, -1, 0, 2, 3, 1, -2 }, &.{ 2, 2, 3 }, .{});
    defer api.freeTensor(self, lhs);
    const rhs_t = try upload(self, f32, &.{ 1, 0, 2, 0, 3, 1, 2, 1, 0, -1, 0, 4 }, &.{ 2, 2, 3 }, .{});
    defer api.freeTensor(self, rhs_t);
    const rhs_n = try upload(self, f32, &.{ 1, 0, 0, 3, 2, 1, 2, -1, 1, 0, 0, 4 }, &.{ 2, 3, 2 }, .{});
    defer api.freeTensor(self, rhs_n);
    const output_t = try api.primDotGeneralOp(self, lhs, rhs_t, &.{ 2, 2, 3 }, &.{ 2, 2, 3 }, &.{2}, &.{2}, &.{0}, &.{0});
    defer api.freeTensor(self, output_t);
    const output_n = try api.primDotGeneralOp(self, lhs, rhs_n, &.{ 2, 2, 3 }, &.{ 2, 3, 2 }, &.{2}, &.{1}, &.{0}, &.{0});
    defer api.freeTensor(self, output_n);
    try std.testing.expectEqual(@as(u64, 2), self.training_blas.?.calls);
    var actual: [8]f32 = undefined;
    const expected = [_]f32{ 7, 9, 16, 21, -2, 9, 7, -11 };
    try downloadPrimitiveForTest(self, output_t, &actual);
    try std.testing.expectEqualSlices(f32, &expected, &actual);
    try downloadPrimitiveForTest(self, output_n, &actual);
    try std.testing.expectEqualSlices(f32, &expected, &actual);

    // Exercise admission, dispatch and strict VJPs together, not only the
    // lower-level BLAS adapter above.
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    const cb = self.computeBackend();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const q = try builder.parameter("q", ml.Shape.init(.f32, &.{ 2, 2, 3 }));
    const k = try builder.parameter("k", ml.Shape.init(.f32, &.{ 2, 2, 3 }));
    const seed = try builder.parameter("seed", ml.Shape.init(.f32, &.{ 2, 2, 2 }));
    const score = try builder.matmul3DTransB(q, k);
    try graph.markOutput(score);
    var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = score, .cotangent = seed }}, &.{ q, k }, .{});
    defer ad.deinit();
    var forward = try execution.Program.init(a, &graph, &.{score}, .{});
    defer forward.deinit();
    var backward = try execution.Program.init(a, &ad.graph, ad.param_grads, .{});
    defer backward.deinit();
    var scores = try forward.execute(a, &cb, &.{ .{ .node_id = q, .value = lhs }, .{ .node_id = k, .value = rhs_t } }, null);
    defer scores.deinit(&cb);
    try cb.glinerBoundaryDownload(scores.outputs[0], &actual);
    try std.testing.expectEqualSlices(f32, &expected, &actual);
    const cotangent = try upload(self, f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 2, 2 }, .{});
    defer cb.free(cotangent);
    var gradients = try backward.execute(a, &cb, &.{ .{ .node_id = ad.id_map[q], .value = lhs }, .{ .node_id = ad.id_map[k], .value = rhs_t }, .{ .node_id = ad.id_map[seed], .value = cotangent } }, null);
    defer gradients.deinit(&cb);
    const expected_gradients = [_][12]f32{ .{ 1, 6, 4, 3, 12, 10, 4, 5, 24, 6, 7, 32 }, .{ 13, 17, 21, 18, 24, 30, 16, 7, -4, 18, 8, -4 } };
    for (gradients.outputs, expected_gradients) |value, reference| {
        var actual_gradient: [12]f32 = undefined;
        try cb.glinerBoundaryDownload(value, &actual_gradient);
        try std.testing.expectEqualSlices(f32, &reference, &actual_gradient);
    }
}

test "CUDA boundary fused linear preserves seeded gradients and refreshes cached bias" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const cb = device.backend.computeBackend();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const x = try builder.parameter("x", ml.Shape.init(.f32, &.{ 2, 3 }));
    const w = try builder.parameter("w", ml.Shape.init(.f32, &.{ 4, 3 }));
    const bias = try builder.parameter("bias", ml.Shape.init(.f32, &.{4}));
    const seed = try builder.parameter("seed", ml.Shape.init(.f32, &.{ 2, 4 }));
    const y = try builder.linear(x, w, bias, 2, 3, 4);
    graph.nodeMut(y).vjp_alternate = ml.null_node;
    try graph.markOutput(y);
    var differentiated = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w, bias }, .{});
    defer differentiated.deinit();
    var forward = try execution.Program.init(a, &graph, &.{y}, .{});
    defer forward.deinit();
    var backward = try execution.Program.init(a, &differentiated.graph, differentiated.param_grads, .{});
    defer backward.deinit();
    const input = try upload(device.backend, f32, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 2, 3 }, .{});
    defer cb.free(input);
    const weight = try upload(device.backend, f32, &.{ 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1 }, &.{ 4, 3 }, .{});
    defer cb.free(weight);
    const bias_first = try upload(device.backend, f32, &.{ 0, 0, 0, 0 }, &.{4}, .{});
    defer cb.free(bias_first);
    const bias_changed = try upload(device.backend, f32, &.{ 10, 20, 30, 40 }, &.{4}, .{});
    defer cb.free(bias_changed);
    const cotangent = try upload(device.backend, f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 4 }, .{});
    defer cb.free(cotangent);
    const expected = [_][8]f32{ .{ 1, 2, 3, 6, 4, 5, 6, 15 }, .{ 11, 22, 33, 46, 14, 25, 36, 55 } };
    var plan_count: usize = 0;
    for ([_]CT{ bias_first, bias_changed }, 0..) |bias_value, iteration| {
        var result = try forward.execute(a, &cb, &.{ .{ .node_id = x, .value = input }, .{ .node_id = w, .value = weight }, .{ .node_id = bias, .value = bias_value } }, null);
        defer result.deinit(&cb);
        var actual: [8]f32 = undefined;
        try cb.glinerBoundaryDownload(result.outputs[0], &actual);
        try std.testing.expectEqualSlices(f32, &expected[iteration], &actual);
        const count = device.backend.cublaslt.?.tensor_core_plans.count();
        if (iteration == 0) plan_count = count else try std.testing.expectEqual(plan_count, count);
    }
    const ids = [_]ml.NodeId{ x, w, bias, seed };
    const values = [_]CT{ input, weight, bias_first, cotangent };
    var bindings: [4]execution.Binding = undefined;
    var count: usize = 0;
    for (ids, values) |id, value| {
        const mapped = differentiated.id_map[id];
        if (mapped != ml.null_node and backward.usesParameter(mapped)) {
            bindings[count] = .{ .node_id = mapped, .value = value };
            count += 1;
        }
    }
    var gradients = try backward.execute(a, &cb, bindings[0..count], null);
    defer gradients.deinit(&cb);
    const expected_gradients = [_][]const f32{ &.{ 5, 6, 7, 13, 14, 15 }, &.{ 21, 27, 33, 26, 34, 42, 31, 41, 51, 36, 48, 60 }, &.{ 6, 8, 10, 12 } };
    for (gradients.outputs, expected_gradients) |value, reference| {
        var actual: [12]f32 = undefined;
        try cb.glinerBoundaryDownload(value, actual[0..reference.len]);
        try std.testing.expectEqualSlices(f32, reference, actual[0..reference.len]);
    }
}

test "CUDA boundary fused linear degenerate shapes use bias-initialized SGEMM" {
    const Shape = @import("ml").graph.Shape;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const cb = device.backend.computeBackend();
    const Case = struct { k: i32, n: i32, x: []const f32, w: []const f32, bias: []const f32, expected: []const f32 };
    const cases = [_]Case{
        .{ .k = 3, .n = 1, .x = &.{ 1, 2, 3, 4, 5, 6 }, .w = &.{ 2, 3, 4 }, .bias = &.{7}, .expected = &.{ 27, 54 } },
        .{ .k = 1, .n = 4, .x = &.{ 2, 3 }, .w = &.{ 1, 2, 3, 4 }, .bias = &.{ 10, 20, 30, 40 }, .expected = &.{ 12, 24, 36, 48, 13, 26, 39, 52 } },
    };
    for (cases) |case| {
        const x = try upload(device.backend, f32, case.x, &.{ 2, case.k }, .{});
        defer cb.free(x);
        const w = try upload(device.backend, f32, case.w, &.{ case.n, case.k }, .{});
        defer cb.free(w);
        const bias = try upload(device.backend, f32, case.bias, &.{case.n}, .{});
        defer cb.free(bias);
        const instruction = program.Instruction{
            .op = .{ .fused_linear = .{ .rows = 2, .in_dim = @intCast(case.k), .out_dim = @intCast(case.n) } },
            .output = Shape.init(.f32, &.{ 2, case.n }),
            .inputs = .{ Shape.init(.f32, &.{ 2, case.k }), Shape.init(.f32, &.{ case.n, case.k }), Shape.init(.f32, &.{case.n}), .{} },
            .num_inputs = 3,
        };
        const output = try cb.residentTrainingInstruction(&instruction, &.{ x, w, bias }, .{ .max_scratch_bytes = 0 });
        defer cb.free(output);
        var actual: [8]f32 = undefined;
        try cb.glinerBoundaryDownload(output, actual[0..case.expected.len]);
        try std.testing.expectEqualSlices(f32, case.expected, actual[0..case.expected.len]);
    }
    try std.testing.expectEqual(@as(u64, 2), device.backend.training_blas.?.calls);
}

test "CUDA resident normalization is stable and backward reductions are deterministic" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const Shape = @import("ml").graph.Shape;
    const rows = 65;
    const width = 4;
    var x: [rows * width]f32 = undefined;
    var dy: [rows * width]f32 = undefined;
    for (&x, &dy, 0..) |*value, *gradient, i| {
        value.* = 10000 + @as(f32, @floatFromInt(i % 17)) / 8;
        gradient.* = (@as(f32, @floatFromInt(i % 7)) - 3) / 4;
    }
    const gamma = [_]f32{ 0.5, 1, 1.5, 2 };
    const beta = [_]f32{ -1, 0, 1, 2 };
    var tensors: [4]CT = undefined;
    var initialized: usize = 0;
    defer for (tensors[0..initialized]) |value| cb.free(value);
    const values = [_][]const f32{ &x, &gamma, &beta, &dy };
    const input_shape = Shape.init(.f32, &.{ rows, width });
    const parameter_shape = Shape.init(.f32, &.{width});
    const shapes = [_]Shape{ input_shape, parameter_shape, parameter_shape, input_shape };
    for (&tensors, values, 0..) |*value, data, index| {
        value.* = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = data, .shape = if (index == 0 or index == 3) &.{ rows, width } else &.{width} } }, .{});
        initialized += 1;
    }
    // Pinned PyTorch 2.9.1 CUDA FP32 fixture, regenerated by the archived
    // norm-regression-probe.py. High-offset FP32 Welford arithmetic differs
    // from exact FP64 math; compare every output bit to the actual contract.
    const forward = program.Instruction{ .op = .{ .fused_layer_norm = .{ .dim = width, .eps = 1e-5 } }, .output = input_shape, .inputs = shapes, .num_inputs = 3 };
    const y = try cb.residentTrainingInstruction(&forward, tensors[0..3], .{});
    defer cb.free(y);
    var actual_y: [rows * width]f32 = undefined;
    try cb.glinerBoundaryDownload(y, &actual_y);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(&actual_y), &digest, .{});
    try std.testing.expectEqualStrings("d14c8f7ddc5da68f0e31ea4e672348877c5dd4188379d8a5bf714de6bdeca25b", &std.fmt.bytesToHex(digest, .lower));
    const backward = program.Instruction{ .op = .{ .fused_layer_norm_backward = .{ .dim = width, .eps = 1e-5 } }, .output = Shape.init(.f32, &.{ rows + 2, width }), .inputs = shapes, .num_inputs = 4 };
    var first: [(rows + 2) * width]f32 = undefined;
    for (0..3) |repetition| {
        const gradient = try cb.residentTrainingInstruction(&backward, &tensors, .{});
        defer cb.free(gradient);
        var actual: [(rows + 2) * width]f32 = undefined;
        try cb.glinerBoundaryDownload(gradient, &actual);
        std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(&actual), &digest, .{});
        try std.testing.expectEqualStrings("45ef039c772c02e6a8b41634ac27fdfe934b6289a7fd3ecba69e341ee6087877", &std.fmt.bytesToHex(digest, .lower));
        if (repetition == 0) first = actual else try std.testing.expectEqualSlices(f32, &first, &actual);
    }
}

test "CUDA boundary reductions preserve order across contiguous and disjoint axes" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const Shape = @import("ml").graph.Shape;
    var values: [24]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) - 11;
    // Cancellation makes summation order observable, unlike all-small inputs.
    values[0..3].* = .{ 16777216, 1, -16777216 };
    const input = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &values, .shape = &.{ 2, 3, 4 } } }, .{});
    defer cb.free(input);
    for (1..8) |mask| {
        var axes: [8]u8 = @splat(0);
        var num_axes: u8 = 0;
        var shape = [_]i64{ 2, 3, 4 };
        var output_count: usize = 1;
        var reduce_count: usize = 1;
        for (&shape, 0..) |*dim, axis| {
            if (mask & (@as(usize, 1) << @intCast(axis)) != 0) {
                axes[num_axes] = @intCast(axis);
                num_axes += 1;
                reduce_count *= @intCast(dim.*);
                dim.* = 1;
            }
            output_count *= @intCast(dim.*);
        }
        inline for (.{ .reduce_sum, .reduce_mean, .reduce_max }) |tag| {
            var expected: [24]f32 = @splat(if (tag == .reduce_max) -std.math.floatMax(f32) else 0);
            for (values, 0..) |value, i| {
                const coords = [_]usize{ i / 12, (i / 4) % 3, i % 4 };
                var index: usize = 0;
                for (shape, coords) |dim, coordinate| index = index * @as(usize, @intCast(dim)) + if (dim == 1) @as(usize, 0) else coordinate;
                expected[index] = if (tag == .reduce_max) @max(expected[index], value) else expected[index] + value;
            }
            if (tag == .reduce_mean) for (expected[0..output_count]) |*value| {
                value.* /= @floatFromInt(reduce_count);
            };
            const instruction = program.Instruction{
                .op = @unionInit(@import("ml").graph.OpCode, @tagName(tag), .{ .axes = axes, .num_axes = num_axes }),
                .output = Shape.init(.f32, &shape),
                .inputs = .{ Shape.init(.f32, &.{ 2, 3, 4 }), .{}, .{}, .{} },
                .num_inputs = 1,
            };
            const result = try cb.residentTrainingInstruction(&instruction, &.{input}, .{});
            defer cb.free(result);
            var actual: [24]f32 = undefined;
            try cb.glinerBoundaryDownload(result, actual[0..output_count]);
            try std.testing.expectEqualSlices(f32, expected[0..output_count], actual[0..output_count]);
        }
    }
}

test "CUDA boundary exact GELU matches pinned Torch within erf rounding error" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const Shape = @import("ml").graph.Shape;
    // PyTorch 2.9.1+cu128, L4, F.gelu(approximate="none"), unit upstream.
    const values = [_]f32{ -6.0, -5.699999809265137, -5.5, -5.0, -4.5, -4.0, -3.0, -1.0, 0.0, 1.0, 3.0, 5.0 };
    const forward = [_]f32{ -0.0, -0.0, -1.6391277313232422e-07, -1.341104507446289e-06, -1.5288591384887695e-05, -0.0001266002655029297, -0.004049777984619141, -0.15865525603294373, 0.0, 0.8413447141647339, 2.995950222015381, 4.999999046325684 };
    const backward = [_]f32{ -3.6455301000160034e-08, -2.0029564495871455e-07, -5.625344670079357e-07, -7.165377155615715e-06, -6.852936348877847e-05, -0.0005036708316765726, -0.01194562017917633, -0.08331547677516937, 0.5, 1.0833154916763306, 1.0119456052780151, 1.000007152557373 };
    const ones = [_]f32{1} ** values.len;
    const input = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &values, .shape = &.{values.len} } }, .{});
    defer cb.free(input);
    const upstream = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &ones, .shape = &.{values.len} } }, .{});
    defer cb.free(upstream);
    const shape = Shape.init(.f32, &.{values.len});
    inline for (.{ false, true }) |reverse| {
        const instruction = program.Instruction{
            .op = if (reverse) .{ .fused_gelu_exact_backward = {} } else .{ .fused_gelu_exact = {} },
            .output = shape,
            .inputs = .{ shape, if (reverse) shape else .{}, .{}, .{} },
            .num_inputs = if (reverse) 2 else 1,
        };
        const inputs = [_]CT{ input, upstream };
        const output = try cb.residentTrainingInstruction(&instruction, inputs[0..instruction.num_inputs], .{});
        defer cb.free(output);
        var actual: [values.len]f32 = undefined;
        try cb.glinerBoundaryDownload(output, &actual);
        for (if (reverse) backward else forward, actual, values) |expected, value, x| {
            // CUDA 12.8 and 13.2 libdevice erf can differ by one FP32 ULP near
            // +/-1. Bound the resulting CDF cancellation error explicitly.
            const cdf_error: f32 = 0x1p-24;
            const tolerance = (if (reverse) cdf_error else @abs(x) * cdf_error) + @abs(expected) * 2e-7;
            if (expected == 0) try std.testing.expectEqual(expected, value) else try std.testing.expectApproxEqAbs(expected, value, tolerance);
        }
    }
}

test "CUDA boundary training GELU matches pinned forward and seeded backward bits" {
    try checkPinnedTrainingActivation(.gelu, "0746e385e88d473e6e3e931029d41ea1f285dfd4e6b338e220ec29501aef9bbf");
}

test "CUDA boundary training SiLU matches pinned forward and seeded backward bits" {
    try checkPinnedTrainingActivation(.silu, "b411643079e9e03183e93b8de0b76d048b62d32933da0421321bbdfc96450358");
}

test "CUDA boundary training sigmoid matches pinned forward and saved backward bits" {
    try checkPinnedTrainingActivation(.sigmoid, "5207c722e249d72ef49116efec157718d729086fb0c6bfef26bfbcbbe52fc2bb");
}

fn checkPinnedTrainingActivation(comptime kind: enum { gelu, silu, sigmoid }, expected_hash: []const u8) !void {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try std.testing.expect(device.backend.training_math == null);
    try device.backend.enableResidentTrainingMath();
    const loaded = device.backend.training_math.?.module;
    try device.backend.enableResidentTrainingMath();
    try std.testing.expectEqual(loaded, device.backend.training_math.?.module);
    const cb = device.backend.computeBackend();
    const count = if (kind == .gelu) 1025 else 2049; // Partial block and both activation tails.
    var values: [count]f32 = undefined;
    var seeds: [count]f32 = undefined;
    for (&values, &seeds, 0..) |*value, *seed, i| {
        value.* = if (kind == .gelu) (@as(f32, @floatFromInt(i)) - 512) / 64 else (@as(f32, @floatFromInt(i)) - 1024) / 8;
        seed.* = (@as(f32, @floatFromInt((i * 17) % 97)) - 48) / 16;
    }
    const shape = ml.Shape.init(.f32, &.{count});
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const x = try builder.parameter("x", shape);
    const seed = try builder.parameter("seed", shape);
    const y = switch (kind) {
        .gelu => try builder.geluExact(x),
        .silu => try builder.silu(x),
        .sigmoid => try builder.sigmoidRetained(x),
    };
    if (kind == .silu) graph.nodeMut(y).vjp_alternate = ml.null_node;
    var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
    defer ad.deinit();
    var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[y], ad.param_grads[0] }, .{});
    defer compiled.deinit();
    const input = try upload(device.backend, f32, &values, &.{count}, .{});
    defer cb.free(input);
    const upstream = try upload(device.backend, f32, &seeds, &.{count}, .{});
    defer cb.free(upstream);
    var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
    defer result.deinit(&cb);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (result.outputs) |output| {
        var actual: [count]f32 = undefined;
        try cb.glinerBoundaryDownload(output, &actual);
        hash.update(std.mem.sliceAsBytes(&actual));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // PyTorch 2.9.1+cu128 / L4: every output and non-unit seeded gradient bit.
    try std.testing.expectEqualStrings(expected_hash, &std.fmt.bytesToHex(digest, .lower));
    const math = &device.backend.training_math.?;
    const launch = switch (kind) {
        .gelu => @TypeOf(math.*).launchGelu,
        .silu => @TypeOf(math.*).launchSilu,
        .sigmoid => @TypeOf(math.*).launchSigmoid,
    };
    try std.testing.expectError(error.InvalidCudaState, launch(math, &device.backend.ctx, .{}, tensor(input).buffer, tensor(upstream).buffer, count, true));
    try std.testing.expectError(error.InvalidCudaState, launch(math, &device.backend.ctx, tensor(input).buffer, tensor(input).buffer, .{}, count, true));
    try launch(math, &device.backend.ctx, .{}, .{}, .{}, 0, false);
}

test "CUDA boundary fused softmax matches pinned forward and seeded backward bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    // PyTorch 2.9.1+cu128 / L4: every output and seeded gradient bit across
    // widths straddling all warp dispatch boundaries, odd rows and finite masks.
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]u32{ 1, 2, 4, 7, 11, 22, 32, 59, 63, 64, 65, 118, 128, 192, 256, 384, 512, 1000, 1024 }) |width| {
        const rows = 7;
        const shape = ml.Shape.init(.f32, &.{ rows, width });
        const x = try a.alloc(f32, rows * width);
        defer a.free(x);
        const dy = try a.alloc(f32, x.len);
        defer a.free(dy);
        for (x, dy, 0..) |*value, *gradient, i| {
            value.* = if (i < width or (i / width == 2 and i % width >= width / 2)) -std.math.floatMax(f32) else (@as(f32, @floatFromInt(i % 37)) - 18) / 8;
            gradient.* = (@as(f32, @floatFromInt(i % 13)) - 6) / 4;
        }
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        const input_id = try builder.parameter("x", shape);
        const seed_id = try builder.parameter("seed", shape);
        const y_id = try builder.softmax(input_id);
        graph.nodeMut(y_id).op.fused_softmax.fuse_backward = true;
        var differentiated = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y_id, .cotangent = seed_id }}, &.{input_id}, .{});
        defer differentiated.deinit();
        var compiled = try execution.Program.init(a, &differentiated.graph, &.{ differentiated.id_map[y_id], differentiated.param_grads[0] }, .{});
        defer compiled.deinit();
        const input = try upload(device.backend, f32, x, &.{ rows, @intCast(width) }, .{});
        defer cb.free(input);
        const seed = try upload(device.backend, f32, dy, &.{ rows, @intCast(width) }, .{});
        defer cb.free(seed);
        var result = try compiled.execute(a, &cb, &.{ .{ .node_id = differentiated.id_map[input_id], .value = input }, .{ .node_id = differentiated.id_map[seed_id], .value = seed } }, null);
        defer result.deinit(&cb);
        const actual = try a.alloc(f32, x.len);
        defer a.free(actual);
        for (result.outputs) |output| {
            try cb.glinerBoundaryDownload(output, actual);
            hash.update(std.mem.sliceAsBytes(actual));
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    try std.testing.expectEqualStrings("63fc2ebcb273121226864a0ac926a8de60b4362e241b4a52b3483453395588f4", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary retained dense backward matches pinned Python bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const cb = device.backend.computeBackend();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const x = try builder.parameter("x", ml.Shape.init(.f32, &.{ 7, 384 }));
    const w = try builder.parameter("w", ml.Shape.init(.f32, &.{ 384, 384 }));
    const bias = try builder.parameter("bias", ml.Shape.init(.f32, &.{384}));
    const seed = try builder.parameter("seed", ml.Shape.init(.f32, &.{ 7, 384 }));
    const y = try builder.linear(x, w, bias, 7, 384, 384);
    graph.nodeMut(y).vjp_alternate = ml.null_node;
    graph.nodeMut(y).op.fused_linear.retain_backward_storage = true;
    var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w }, .{});
    defer ad.deinit();
    var compiled = try execution.Program.init(a, &ad.graph, ad.param_grads, .{});
    defer compiled.deinit();
    const xv = try a.alloc(f32, 7 * 384);
    defer a.free(xv);
    const wv = try a.alloc(f32, 384 * 384);
    defer a.free(wv);
    const sv = try a.alloc(f32, 7 * 384);
    defer a.free(sv);
    for (xv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 17 % 97)) - 48)) / 29.0;
    for (wv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 13 % 101)) - 50)) / 31.0;
    for (sv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 19 % 89)) - 44)) / 37.0;
    const input = try upload(device.backend, f32, xv, &.{ 7, 384 }, .{});
    defer cb.free(input);
    const weight = try upload(device.backend, f32, wv, &.{ 384, 384 }, .{});
    defer cb.free(weight);
    const upstream = try upload(device.backend, f32, sv, &.{ 7, 384 }, .{});
    defer cb.free(upstream);
    const calls_before = device.backend.training_blas.?.calls;
    var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[w], .value = weight }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
    defer result.deinit(&cb);
    try std.testing.expectEqual(@as(u64, 2), device.backend.training_blas.?.calls - calls_before);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (result.outputs, [_][]f32{ xv, wv }) |output, values| {
        try cb.glinerBoundaryDownload(output, values);
        hash.update(std.mem.sliceAsBytes(values));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // Full input/weight gradient bits, pinned PyTorch 2.9.1+cu128 / L4.
    // The copied-storage path differs on both gradients for this fixture.
    try std.testing.expectEqualStrings("c372c6b464dd01d3a9070771e99fa803320b60ca25a9e39ee21bfa28c4b87b29", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary retained batched gradients match pinned Python across layouts" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]bool{ false, true }) |lt| {
        for ([_]bool{ false, true }) |rt| {
            const xs = [_]i64{ 2, if (lt) 32 else 7, if (lt) 7 else 32 };
            const ws = [_]i64{ 2, if (rt) 11 else 32, if (rt) 32 else 11 };
            var graph = ml.Graph.init(a);
            defer graph.deinit();
            var builder = ml.Builder.init(&graph);
            const x = try builder.parameter("x", ml.Shape.init(.f32, &xs));
            const w = try builder.parameter("w", ml.Shape.init(.f32, &ws));
            const seed = try builder.parameter("seed", ml.Shape.init(.f32, &.{ 2, 7, 11 }));
            const y = try builder.matmul3DLayout(x, w, lt, rt);
            graph.nodeMut(y).op.dot_general.retain_backward_storage = true;
            var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w }, .{});
            defer ad.deinit();
            var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[y], ad.param_grads[0], ad.param_grads[1] }, .{});
            defer compiled.deinit();
            var xv: [2 * 7 * 32]f32 = undefined;
            var wv: [2 * 32 * 11]f32 = undefined;
            var sv: [2 * 7 * 11]f32 = undefined;
            for (&xv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 17 % 97)) - 48)) / 29.0;
            for (&wv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 13 % 101)) - 50)) / 31.0;
            for (&sv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 19 % 89)) - 44)) / 37.0;
            const input = try upload(device.backend, f32, &xv, &.{ @intCast(xs[0]), @intCast(xs[1]), @intCast(xs[2]) }, .{});
            defer cb.free(input);
            const weight = try upload(device.backend, f32, &wv, &.{ @intCast(ws[0]), @intCast(ws[1]), @intCast(ws[2]) }, .{});
            defer cb.free(weight);
            const upstream = try upload(device.backend, f32, &sv, &.{ 2, 7, 11 }, .{});
            defer cb.free(upstream);
            const calls = device.backend.training_blas.?.calls;
            var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[w], .value = weight }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
            defer result.deinit(&cb);
            try std.testing.expectEqual(@as(u64, 3), device.backend.training_blas.?.calls - calls);
            for (result.outputs, [_][]f32{ &sv, &xv, &wv }) |output, values| {
                try cb.glinerBoundaryDownload(output, values);
                hash.update(std.mem.sliceAsBytes(values));
            }
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // Full forward and seeded-gradient bits for four layouts, Torch 2.9.1+cu128 / L4.
    try std.testing.expectEqualStrings("3098047a7fcfe93c95bf2849b0ecc7e65ae329f752dfbe6362ad96977b579faa", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary fused attention matches pinned Python forward and strict gradients" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentBoundaryAttention();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    const cases = [_][4]u32{ .{ 1, 1, 1, 0 }, .{ 1, 11, 1, 2 }, .{ 2, 22, 4, 0 }, .{ 2, 59, 4, 0 }, .{ 2, 64, 4, 3 }, .{ 2, 65, 4, 0 }, .{ 3, 118, 1, 2 }, .{ 2, 129, 4, 0 } };
    for (cases) |case| {
        const attrs = ml.node.BoundaryTrainingAttentionAttrs{ .batch = case[0], .seq_len = case[1], .num_heads = case[2], .window = case[3] };
        const layout = try attrs.layout();
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var b = ml.Builder.init(&graph);
        const x = try b.parameter("qkv", layout.qkvShape());
        const mask = try b.parameter("mask", attrs.maskShape());
        const seed = try b.parameter("seed", layout.attendedShape());
        const saved = try b.boundaryTrainingAttentionV1(x, mask, attrs);
        const y = try b.reshape(try b.sliceLastDim(saved, 0, layout.output_elements), layout.attendedShape());
        var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
        defer ad.deinit();
        var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[y], ad.param_grads[0] }, .{});
        defer compiled.deinit();
        const xv = try a.alloc(f32, @intCast(layout.output_elements * 3));
        defer a.free(xv);
        const sv = try a.alloc(f32, @intCast(layout.output_elements));
        defer a.free(sv);
        const mv = try a.alloc(f32, @intCast(layout.rows));
        defer a.free(mv);
        for (xv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 17 % 97)) - 48)) / 29.0;
        for (sv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 19 % 89)) - 44)) / 37.0;
        for (mv, 0..) |*value, i| value.* = if (i % attrs.seq_len < attrs.seq_len -| (i / attrs.seq_len * 3 + 1)) 1 else 0;
        const input = try upload(device.backend, f32, xv, &.{ @intCast(layout.rows), @intCast(3 * layout.hidden) }, .{});
        defer cb.free(input);
        const valid = try upload(device.backend, f32, mv, &.{ @intCast(attrs.batch), @intCast(attrs.seq_len) }, .{});
        defer cb.free(valid);
        const upstream = try upload(device.backend, f32, sv, &.{ @intCast(layout.rows), @intCast(layout.hidden) }, .{});
        defer cb.free(upstream);
        var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[mask], .value = valid }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
        defer result.deinit(&cb);
        for (result.outputs, [_][]f32{ sv, xv }) |output, values| {
            try cb.glinerBoundaryDownload(output, values);
            hash.update(std.mem.sliceAsBytes(values));
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // All forward/packed-gradient bits, including window and all-masked cases.
    try std.testing.expectEqualStrings("0da0df5697bf7070bbf20286f50a597f838546a7dad87dba8b513fa18633004e", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary prefix scans match pinned forward and strict gradient bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    const cases = [_]struct { b: u32, n: u32, d: u32, inner: bool }{
        .{ .b = 2, .n = 10, .d = 64, .inner = false },
        .{ .b = 2, .n = 59, .d = 64, .inner = false },
        .{ .b = 1, .n = 129, .d = 128, .inner = false },
        .{ .b = 2, .n = 59, .d = 1, .inner = false },
        .{ .b = 3, .n = 1, .d = 1, .inner = true },
        .{ .b = 2, .n = 59, .d = 1, .inner = true },
        .{ .b = 31, .n = 129, .d = 1, .inner = true },
        .{ .b = 32, .n = 65, .d = 1, .inner = true },
        .{ .b = 33, .n = 1025, .d = 1, .inner = true },
        .{ .b = 2, .n = 2051, .d = 1, .inner = true },
        .{ .b = 1, .n = 1, .d = 1, .inner = false },
        .{ .b = 1, .n = 16, .d = 1, .inner = true },
        .{ .b = 1, .n = 17, .d = 1, .inner = false },
        .{ .b = 1, .n = 129, .d = 1, .inner = true },
        .{ .b = 1, .n = 8193, .d = 1, .inner = true },
        .{ .b = 1, .n = 16384, .d = 1, .inner = false },
    };
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (cases) |case| {
        const attrs = ml.node.PrefixScanAttrs{ .batch = case.b, .width = case.n, .channels = case.d, .reference = if (case.inner) .inner else .outer };
        const shape = try attrs.shape();
        const count = try attrs.elements();
        const values = try a.alloc(f32, count);
        defer a.free(values);
        const seeds = try a.alloc(f32, count);
        defer a.free(seeds);
        for (values, seeds, 0..) |*value, *seed, i| {
            value.* = (@as(f32, @floatFromInt((i * 17) % 97)) - 48) / 29;
            seed.* = (@as(f32, @floatFromInt((i * 19) % 89)) - 44) / 37;
        }
        values[0] = -0.0;
        seeds[0] = -0.0;
        if (count > 1) {
            values[1] = 0;
            seeds[1] = 0;
        }
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        const x = try builder.parameter("x", shape);
        const seed = try builder.parameter("seed", shape);
        const y = try builder.prefixScanV1(x, attrs);
        var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
        defer ad.deinit();
        var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[y], ad.param_grads[0] }, .{});
        defer compiled.deinit();
        const device_shape = [_]i32{ @intCast(case.b * case.n), @intCast(case.d) };
        const input = try upload(device.backend, f32, values, &device_shape, .{});
        defer cb.free(input);
        const upstream = try upload(device.backend, f32, seeds, &device_shape, .{});
        defer cb.free(upstream);
        var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
        defer result.deinit(&cb);
        const actual = try a.alloc(f32, count);
        defer a.free(actual);
        for (result.outputs) |output| {
            try cb.glinerBoundaryDownload(output, actual);
            hash.update(std.mem.sliceAsBytes(actual));
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // PyTorch 2.9.1+cu128 / L4, deterministic cumsum, all output and seed bits.
    try std.testing.expectEqualStrings("968b6062aba7b7a99026d8473696340626b1fa73c35b0739d8c597d444ac7b99", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary reference reductions match pinned sums means and generated VJP bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    const limits = program.Limits{ .cuda_reduction = device.backend.training_math.?.reductionDevice() };
    const Case = struct { shape: []const i64, axes: []const u8 };
    const cases = [_]Case{
        .{ .shape = &.{ 384, 128 }, .axes = &.{1} },
        .{ .shape = &.{ 22, 128 }, .axes = &.{0} },
        .{ .shape = &.{ 118, 768 }, .axes = &.{0} },
        .{ .shape = &.{ 2, 192, 7, 128 }, .axes = &.{1} },
        .{ .shape = &.{ 2, 59, 128 }, .axes = &.{ 0, 1, 2 } },
        .{ .shape = &.{131072}, .axes = &.{0} },
        .{ .shape = &.{ 131072, 8 }, .axes = &.{0} },
        .{ .shape = &.{ 3, 129 }, .axes = &.{1} },
        .{ .shape = &.{ 3, 1025 }, .axes = &.{1} },
        .{ .shape = &.{1}, .axes = &.{0} },
        .{ .shape = &.{ 1, 3, 1, 7, 1 }, .axes = &.{ 1, 3 } },
        .{ .shape = &.{ 1, 3, 1, 7, 1 }, .axes = &.{1} },
        .{ .shape = &.{ 1, 3, 1, 7, 1 }, .axes = &.{ 0, 2, 4 } },
        .{ .shape = &.{ 2, 3, 5, 7 }, .axes = &.{ 0, 2 } },
        .{ .shape = &.{ 3, 129, 7 }, .axes = &.{ 0, 2 } },
    };
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (cases) |case| {
        const input_shape = ml.Shape.init(.f32, case.shape);
        var output_shape = input_shape;
        var attrs = ml.node.ReduceAttrs{ .num_axes = @intCast(case.axes.len) };
        for (case.axes, 0..) |axis, i| {
            output_shape.dims[axis] = 1;
            attrs.axes[i] = axis;
        }
        const n = try program.count(input_shape, .{});
        const out_n = try program.count(output_shape, .{});
        const values = try a.alloc(f32, n);
        defer a.free(values);
        for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 17) % 97)) - 48)) / 29.0;
        values[0] = -0.0;
        var device_shape: [8]i32 = undefined;
        for (case.shape, 0..) |dim, i| device_shape[i] = @intCast(dim);
        const input = try upload(device.backend, f32, values, device_shape[0..case.shape.len], .{});
        defer cb.free(input);
        const actual = try a.alloc(f32, out_n);
        defer a.free(actual);
        inline for (.{ false, true }) |mean| {
            const instruction = program.Instruction{ .op = if (mean) .{ .reduce_mean = attrs } else .{ .reduce_sum = attrs }, .output = output_shape, .inputs = .{ input_shape, .{}, .{}, .{} }, .num_inputs = 1 };
            const output = try cb.residentTrainingInstruction(&instruction, &.{input}, limits);
            defer cb.free(output);
            try cb.glinerBoundaryDownload(output, actual);
            hash.update(std.mem.sliceAsBytes(actual));
        }
        // The reduction in this graph is emitted by strict autodiff, not by
        // forward construction. It must receive the same CUDA profile.
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        const bias = try builder.parameter("bias", output_shape);
        const seed = try builder.parameter("seed", input_shape);
        var broadcast = ml.node.BroadcastAttrs{ .target_shape = input_shape, .num_axes = input_shape.rank_ };
        for (0..input_shape.rank_) |i| broadcast.broadcast_axes[i] = @intCast(i);
        const expanded = try graph.addNode(.{ .op = .{ .broadcast_in_dim = broadcast }, .output_shape = input_shape, .inputs = .{ bias, ml.null_node, ml.null_node, ml.null_node }, .num_inputs = 1 });
        var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = expanded, .cotangent = seed }}, &.{bias}, .{});
        defer ad.deinit();
        var compiled = try execution.Program.init(a, &ad.graph, ad.param_grads, .{ .instruction = limits });
        defer compiled.deinit();
        try std.testing.expectEqual(@as(usize, 1), compiled.admission.parameters);
        var result = try compiled.execute(a, &cb, &.{.{ .node_id = ad.id_map[seed], .value = input }}, null);
        defer result.deinit(&cb);
        try cb.glinerBoundaryDownload(result.outputs[0], actual);
        hash.update(std.mem.sliceAsBytes(actual));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // PyTorch 2.9.1+cu128 / L4, sum, mean and expanded-bias gradients.
    try std.testing.expectEqualStrings("725ddfd3844ff511228dad2017a17f6585d4c225559c97db321439ee1bd975e9", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA resident training cuBLAS runtime selection fails closed" {
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const version = device.backend.training_blas.?.version;
    try std.testing.expect(version > 0);
    const Blas = @import("libraries.zig").CublasF32;
    const configured = @import("antfly_platform").env.getenv("ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY");
    var explicit = try Blas.initWithLibrary(device.backend.ctx.stream, configured);
    defer explicit.deinit();
    try std.testing.expectEqual(version, explicit.version);
    try std.testing.expectError(error.InvalidCudaTrainingLibraryPath, Blas.initWithLibrary(device.backend.ctx.stream, "libcublas.so.12"));
    // An installed default runtime must not rescue a missing explicit path.
    if (Blas.initWithLibrary(device.backend.ctx.stream, "/__antfly_missing_training_runtime__/libcublas.so")) |loaded| {
        var unexpected = loaded;
        unexpected.deinit();
        return error.ExpectedError;
    } else |_| {}
}

test "CUDA boundary retained forward transposes match pinned head dots and gradients" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingBlas();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    const cases = [_][4]i32{ .{ 2, 7, 128, 11 }, .{ 2, 7, 128, 10 }, .{ 2, 11, 128, 59 }, .{ 1, 3, 64, 22 }, .{ 2, 192, 128, 7 } };
    for (cases) |case| {
        const batch = case[0];
        const m = case[1];
        const k = case[2];
        const n = case[3];
        for ([_]bool{ false, true }) |lt| for ([_]bool{ false, true }) |rt| {
            const xs = [_]i64{ batch, if (lt) k else m, if (lt) m else k };
            const ws = [_]i64{ batch, if (rt) n else k, if (rt) k else n };
            const ys = [_]i64{ batch, m, n };
            var graph = ml.Graph.init(a);
            defer graph.deinit();
            var builder = ml.Builder.init(&graph);
            const x = try builder.parameter("x", ml.Shape.init(.f32, &xs));
            const w = try builder.parameter("w", ml.Shape.init(.f32, &ws));
            const seed = try builder.parameter("seed", ml.Shape.init(.f32, &ys));
            const lhs = if (lt) try builder.transpose(x, &.{ 0, 2, 1 }) else x;
            const rhs = if (rt) try builder.transpose(w, &.{ 0, 2, 1 }) else w;
            const y = try builder.matmul3D(lhs, rhs);
            // Exercise the exact opt-in rewrite used by the trainer, not a
            // manually constructed dot that already has retained storage.
            try std.testing.expect(ml.passes.fuse.retainTrainingDotStorage(&graph, y));
            try std.testing.expect(!ml.passes.fuse.retainTrainingDotStorage(&graph, y));
            var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w }, .{});
            defer ad.deinit();
            var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[y], ad.param_grads[0], ad.param_grads[1] }, .{});
            defer compiled.deinit();
            // Retained operands remove forward copies. A transposed LHS
            // still needs dA's output-layout restoration to match Python's
            // product order; this is not a copied contraction operand.
            var transpose_count: usize = 0;
            for (compiled.lowered.graph.nodes.items) |node| if (node.op == .transpose) {
                transpose_count += 1;
            };
            try std.testing.expectEqual(@as(usize, @intFromBool(lt)), transpose_count);
            const forward = compiled.lowered.graph.node(compiled.lowered.id_map[ad.id_map[y]]);
            for (forward.getInputs()) |input_id| try std.testing.expect(compiled.lowered.graph.node(input_id).op != .transpose);
            const dx = compiled.lowered.graph.node(compiled.lowered.id_map[ad.param_grads[0]]);
            try std.testing.expectEqual(lt, dx.op == .transpose);
            const xv = try a.alloc(f32, @intCast(batch * m * k));
            defer a.free(xv);
            const wv = try a.alloc(f32, @intCast(batch * k * n));
            defer a.free(wv);
            const sv = try a.alloc(f32, @intCast(batch * m * n));
            defer a.free(sv);
            for (xv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 17 % 97)) - 48)) / 29.0;
            for (wv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 13 % 101)) - 50)) / 31.0;
            for (sv, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i * 19 % 89)) - 44)) / 37.0;
            const input = try upload(device.backend, f32, xv, &.{ batch, @intCast(xs[1]), @intCast(xs[2]) }, .{});
            defer cb.free(input);
            const weight = try upload(device.backend, f32, wv, &.{ batch, @intCast(ws[1]), @intCast(ws[2]) }, .{});
            defer cb.free(weight);
            const upstream = try upload(device.backend, f32, sv, &.{ batch, m, n }, .{});
            defer cb.free(upstream);
            const calls = device.backend.training_blas.?.calls;
            var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[x], .value = input }, .{ .node_id = ad.id_map[w], .value = weight }, .{ .node_id = ad.id_map[seed], .value = upstream } }, null);
            defer result.deinit(&cb);
            try std.testing.expectEqual(@as(u64, 3), device.backend.training_blas.?.calls - calls);
            for (result.outputs, [_][]f32{ sv, xv, wv }) |output, values| {
                try cb.glinerBoundaryDownload(output, values);
                hash.update(std.mem.sliceAsBytes(values));
            }
        };
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // Pinned Torch 2.9.1+cu128 / L4, 20 layout/geometry combinations.
    try std.testing.expectEqualStrings("493ecb9fe8ba5af61a55eabb753ce5858b96c34507a248a30233d30099008251", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary frozen span features match pinned feature bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    const attrs = ml.node.FrozenSpanFeaturesAttrs{ .batch = 3, .capacity = 65537 };
    const rows = try attrs.rows();
    const lengths = try a.alloc(f32, rows);
    defer a.free(lengths);
    for (lengths, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % attrs.capacity)) - 1;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const lens = try builder.parameter("lengths", ml.Shape.init(.f32, &.{ rows, 1 }));
    const ns = try builder.parameter("counts", ml.Shape.init(.f32, &.{ attrs.batch, 1 }));
    const features = try builder.frozenSpanFeaturesV1(lens, ns, attrs);
    var compiled = try execution.Program.init(a, &graph, &.{features}, .{});
    defer compiled.deinit();
    const input = try upload(device.backend, f32, lengths, &.{ @intCast(rows), 1 }, .{});
    defer cb.free(input);
    const counts = try upload(device.backend, f32, &.{ 0, 17, 59 }, &.{ 3, 1 }, .{});
    defer cb.free(counts);
    var result = try compiled.execute(a, &cb, &.{ .{ .node_id = lens, .value = input }, .{ .node_id = ns, .value = counts } }, null);
    defer result.deinit(&cb);
    const values = try a.alloc(f32, rows * 3);
    defer a.free(values);
    try cb.glinerBoundaryDownload(result.outputs[0], values);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(values), &digest, .{});
    // Pinned PyTorch 2.9.1+cu128; all three features for three sample counts,
    // including negative/zero lengths, empty samples and an incomplete block.
    try std.testing.expectEqualStrings("9b5055a00ce7fce0061fcfe1f1e51046dc0ffff674799c66128706d6ebb1a9bb", &std.fmt.bytesToHex(digest, .lower));
    const math = &device.backend.training_math.?;
    try std.testing.expectError(error.InvalidCudaState, math.launchSpanFeatures(&device.backend.ctx, attrs, .{}, tensor(input).buffer, tensor(counts).buffer));
    try std.testing.expectError(error.InvalidCudaState, math.launchSpanFeatures(&device.backend.ctx, attrs, tensor(result.outputs[0]).buffer, .{}, tensor(counts).buffer));
    try std.testing.expectError(error.InvalidCudaState, math.launchSpanFeatures(&device.backend.ctx, attrs, tensor(result.outputs[0]).buffer, tensor(input).buffer, .{}));
}

test "CUDA boundary gather backward matches pinned duplicate reduction bits" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]u32{ 1, 31, 32, 33, 63, 64, 65, 147, 156, 257, 1025 }) |duplicates| {
        for ([_]u32{ 1, 7, 128 }) |width| {
            const count = duplicates + 13;
            const indices = try a.alloc(i32, count);
            defer a.free(indices);
            for (indices, 0..) |*index, i| index.* = if (i < duplicates) 0 else @intCast(1 + 2 * (i % 2));
            const values = try a.alloc(f32, 5 * width);
            defer a.free(values);
            for (values, 0..) |*value, i| value.* = (@as(f32, @floatFromInt((i * 11) % 79)) - 39) / 13;
            const seeds = try a.alloc(f32, count * width);
            defer a.free(seeds);
            for (seeds, 0..) |*value, i| value.* = (@as(f32, @floatFromInt((i * 17) % 97)) - 48) / 29;
            seeds[0] = -0.0;
            var graph = ml.Graph.init(a);
            defer graph.deinit();
            var builder = ml.Builder.init(&graph);
            const table = try builder.parameter("table", ml.Shape.init(.f32, &.{ 5, width }));
            const index = try builder.parameter("index", ml.Shape.init(.i32, &.{count}));
            const shape = ml.Shape.init(.f32, &.{ count, width });
            const seed = try builder.parameter("seed", shape);
            const output = try builder.gather(table, index, shape);
            graph.nodeMut(output).op.gather.backward_reduction = .pytorch_gather_v1;
            var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = output, .cotangent = seed }}, &.{table}, .{});
            defer ad.deinit();
            var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[output], ad.param_grads[0] }, .{});
            defer compiled.deinit();
            const x = try upload(device.backend, f32, values, &.{ 5, @intCast(width) }, .{});
            defer cb.free(x);
            const ix = try upload(device.backend, i32, indices, &.{@intCast(count)}, .{});
            defer cb.free(ix);
            const dy = try upload(device.backend, f32, seeds, &.{ @intCast(count), @intCast(width) }, .{});
            defer cb.free(dy);
            var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[table], .value = x }, .{ .node_id = ad.id_map[index], .value = ix }, .{ .node_id = ad.id_map[seed], .value = dy } }, null);
            defer result.deinit(&cb);
            for (result.outputs, [_]usize{ count * width, 5 * width }) |value, length| {
                const actual = try a.alloc(f32, length);
                defer a.free(actual);
                try cb.glinerBoundaryDownload(value, actual);
                hash.update(std.mem.sliceAsBytes(actual));
            }
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // Python uses the exact host-generated input bits above. Constructing its
    // fixtures with GPU scalar division would instead multiply by a rounded
    // reciprocal, changing the inputs before gather is evaluated.
    try std.testing.expectEqualStrings("acfc4ae085b97d38a9f4d22667e7781352af3adb7551973df7001cfbe2dab3c2", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary fused AdamW matches pinned repeated updates and clears gradients" {
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    const Profile = @typeInfo(@FieldType(ops.TrainingAdamWBatchOptions, "pytorch_fused")).optional.child;
    const profiles = [_]Profile{
        .{ .lr = 1e-5, .optimizer = .{} },
        .{ .lr = 5e-4, .optimizer = .{ .eps = 1e-6 } },
        .{ .lr = 0.0123456789, .optimizer = .{ .beta1 = 0.8, .beta2 = 0.98, .eps = 3.14159e-7, .weight_decay = 0 } },
        .{ .lr = 0, .optimizer = .{ .beta1 = 0, .beta2 = 0, .eps = 1, .weight_decay = 0.5 } },
    };
    // Exact NumPy FP32 power inputs from the independent Python CUDA golden.
    const powers = [_]u32{ 0x3f800000, 0x3dcccccd, 0x3c23d70a, 0x3a83126f, 0x38d1b717, 0x3727c5ad, 0x358637bd, 0x33d6bf95, 0x322bcc77, 0x3089705f, 0x2edbe6ff, 0x2d2febff, 0x2b8cbccc, 0x29e12e13, 0x283424dc, 0x26901d7d };
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]usize{ 1, 33, 8193 }) |count| {
        const values = try a.alloc(f32, count);
        defer a.free(values);
        const gradient = try a.alloc(f32, count);
        defer a.free(gradient);
        const output = try a.alloc(f32, count);
        defer a.free(output);
        const shape = [_]i32{@intCast(count)};
        for (profiles) |profile| {
            for (values, 0..) |*value, i| value.* = (@as(f32, @floatFromInt((i * 13) % 197)) - 98) / 37;
            const w = try upload(device.backend, f32, values, &shape, .{});
            defer cb.free(w);
            @memset(values, 0);
            const m = try upload(device.backend, f32, values, &shape, .{});
            defer cb.free(m);
            const v = try upload(device.backend, f32, values, &shape, .{});
            defer cb.free(v);
            const o = profile.optimizer.cast(f32);
            const options = ops.TrainingAdamWBatchOptions{ .lr = @floatCast(profile.lr), .beta1 = o.beta1, .beta2 = o.beta2, .eps = o.eps, .weight_decay = o.weight_decay, .pytorch_fused = profile };
            for (1..101) |step| {
                const factor = (@as(f32, @floatFromInt(step % 7)) - 3) / 4;
                for (gradient, 0..) |*value, i| {
                    const base = (@as(f32, @floatFromInt((i * 17) % 101)) - 50) / 31;
                    value.* = (base * @as(f32, @bitCast(powers[i % powers.len]))) * factor;
                }
                const g = try upload(device.backend, f32, gradient, &shape, .{});
                defer cb.free(g);
                const input = ops.TrainingAdamWBatchInput{ .weight = w, .grad = g, .m = m, .v = v, .elem_count = count, .bias_correction1 = 1, .bias_correction2 = 1, .adam_step = @intCast(step) };
                try cb.trainingAdamWManyF32(&.{input}, options);
                for ([_]CT{ w, m, v }) |tensor_value| {
                    try cb.glinerBoundaryDownload(tensor_value, output);
                    hash.update(std.mem.sliceAsBytes(output));
                }
                try cb.glinerBoundaryDownload(g, output);
                for (output) |value| try std.testing.expectEqual(@as(f32, 0), value);
            }
        }
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    try std.testing.expectEqualStrings("587f6f17e80e1c8b4bf1bb3df10c78793521eae501dd1efdee78aaf94a4f78a0", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary embedding backward matches pinned padding and chunk reductions" {
    const ml = @import("ml").graph;
    const execution = @import("../../graph/resident_training_program.zig");
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    const cb = device.backend.computeBackend();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]u32{ 1, 31, 32, 33, 1025, 3072, 3073, 8193 }) |count| for ([_]u32{ 1, 33, 384 }) |width| for (0..4) |pattern| {
        const ids = try a.alloc(i32, count);
        defer a.free(ids);
        var output_rows: u32 = 3;
        for (ids, 0..) |*id, i| {
            id.* = @intCast(switch (pattern) {
                0 => 0,
                1 => (i * 17) % 71,
                2 => i,
                else => i % 3,
            });
            output_rows = @max(output_rows, @as(u32, @intCast(id.*)) + 1);
        }
        const values = try a.alloc(f32, output_rows * width);
        defer a.free(values);
        for (values, 0..) |*x, i| x.* = (@as(f32, @floatFromInt((i * 11) % 79)) - 39) / 13;
        const seeds = try a.alloc(f32, count * width);
        defer a.free(seeds);
        for (seeds, 0..) |*x, i| x.* = switch ((i / width) % 7) {
            0 => 16777216,
            1 => -16777216,
            2 => 1,
            else => (@as(f32, @floatFromInt((i * 17) % 101)) - 50) / 31,
        };
        for ([_]?u32{ null, 2 }) |padding| {
            var graph = ml.Graph.init(a);
            defer graph.deinit();
            var builder = ml.Builder.init(&graph);
            const table = try builder.parameter("table", ml.Shape.init(.f32, &.{ output_rows, width }));
            const index = try builder.parameter("index", ml.Shape.init(.i32, &.{count}));
            const shape = ml.Shape.init(.f32, &.{ count, width });
            const seed = try builder.parameter("seed", shape);
            const output = try builder.gather(table, index, shape);
            graph.nodeMut(output).op.gather.backward_reduction = .pytorch_embedding_v1;
            graph.nodeMut(output).op.gather.backward_padding_index = padding;
            var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = output, .cotangent = seed }}, &.{table}, .{});
            defer ad.deinit();
            try std.testing.expectEqual(padding, ad.graph.node(ad.param_grads[0]).op.scatter_add.padding_index);
            var compiled = try execution.Program.init(a, &ad.graph, &.{ ad.id_map[output], ad.param_grads[0] }, .{});
            defer compiled.deinit();
            const x = try upload(device.backend, f32, values, &.{ @intCast(output_rows), @intCast(width) }, .{});
            defer cb.free(x);
            const ix = try upload(device.backend, i32, ids, &.{@intCast(count)}, .{});
            defer cb.free(ix);
            const dy = try upload(device.backend, f32, seeds, &.{ @intCast(count), @intCast(width) }, .{});
            defer cb.free(dy);
            var result = try compiled.execute(a, &cb, &.{ .{ .node_id = ad.id_map[table], .value = x }, .{ .node_id = ad.id_map[index], .value = ix }, .{ .node_id = ad.id_map[seed], .value = dy } }, null);
            defer result.deinit(&cb);
            for (result.outputs, [_]usize{ count * width, output_rows * width }) |value, length| {
                const actual = try a.alloc(f32, length);
                defer a.free(actual);
                try cb.glinerBoundaryDownload(value, actual);
                for (actual) |*v| if (v.* == 0) {
                    v.* = 0;
                };
                hash.update(std.mem.sliceAsBytes(actual));
            }
        }
    };
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    // Independent PyTorch 2.9.1+cu128 embedding autograd, exact host input bits.
    try std.testing.expectEqualStrings("d33f5c0216a6a32af04cb556efd21c698c12169042a5e7a5d6fce3c77f0ca421", &std.fmt.bytesToHex(digest, .lower));
}

test "CUDA boundary binary loss gradients match pinned seeded VJPs" {
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    const configurations = [_]ops.elementwise_loss_math.Settings{
        .{ .kind = .bce, .negative_weight = 0.5 },
        .{ .kind = .asymmetric_focal, .negative_weight = 0.5 },
        .{ .kind = .asymmetric_focal, .gamma_positive = 1, .gamma_negative = 4, .negative_weight = 0.5 },
        .{ .kind = .asymmetric_focal, .gamma_positive = 0.5, .gamma_negative = 1.5, .clip = 0.1, .negative_weight = 0.5 },
    };
    for (configurations) |config| {
        var settings = config;
        settings.positive_backward_power = settings.gamma_positive - 1;
        settings.negative_backward_power = settings.gamma_negative - 1;
        for ([_]bool{ false, true }) |soft| for ([_]usize{ 1, 31, 257 }) |count| {
            const values = try a.alloc(f32, count);
            defer a.free(values);
            const targets = try a.alloc(f32, count);
            defer a.free(targets);
            const seeds = try a.alloc(f32, count);
            defer a.free(seeds);
            for (values, targets, seeds, 0..) |*x, *y, *seed, i| {
                x.* = (@as(f32, @floatFromInt((i * 11) % 71)) - 35) / 4;
                y.* = if (soft) @as(f32, @floatFromInt((i * 7) % 9)) / 8 else @floatFromInt(i % 2);
                seed.* = ((@as(f32, @floatFromInt((i * 17) % 31)) - 15) * 2 + 1) / 32;
            }
            const request = ops.elementwise_loss_math.Request{ .logits = values, .targets = targets, .cotangents = seeds, .settings = settings, .max_elements = count };
            try elementwiseLossGradient(device.backend, &request);
            hash.update(std.mem.sliceAsBytes(seeds));
        };
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    try std.testing.expectEqualStrings("046f83ccef3841eebf6ac2035079adc54bd3bbc37c528a90934c075f315b08f4", &std.fmt.bytesToHex(digest, .lower));
    var masked = [_]f32{ 0, 0 };
    const request = ops.elementwise_loss_math.Request{ .logits = &.{ std.math.nan(f32), std.math.inf(f32) }, .targets = &.{ -1, std.math.nan(f32) }, .cotangents = &masked, .settings = .{}, .max_elements = 2 };
    try elementwiseLossGradient(device.backend, &request);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, &masked);
    try std.testing.expectError(error.InvalidCudaState, device.backend.training_math.?.launchElementwiseVjp(&device.backend.ctx, .{}, .{}, .{}, .{}, 1, .{}));
}

test "CUDA boundary query loss matches pinned gradients and masking" {
    const a = std.testing.allocator;
    const losses = @import("../../finetune/gliner_boundary_losses.zig");
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    // Independent PyTorch 2.9.1+cu128 autograd golden, including normalization
    // and the outer loss weight. Zero signs are canonicalized in both arms.
    for ([_]usize{ 1, 7, 14, 31, 512, 8193 }) |rows| for ([_]f32{ 0, 0.2, 0.37, 1, 3.3 }) |weight| {
        for ([_]losses.QueryObjective{ .abstention, .poisson_count }) |objective| for (0..3) |mask_kind| {
            const logits = try a.alloc(f32, rows);
            defer a.free(logits);
            const mask = try a.alloc(bool, rows);
            defer a.free(mask);
            const mentions = try a.alloc(bool, rows * 5);
            defer a.free(mentions);
            for (logits, mask, 0..) |*x, *active, i| {
                x.* = (@as(f32, @floatFromInt((i * 11) % 71)) - 35) / 4;
                active.* = if (mask_kind == 0) i % 4 != 1 else mask_kind == 1;
                for (0..5) |c| mentions[i * 5 + c] = c < i % 6;
            }
            var result = try losses.queryLossWithBackend(a, logits, .{ .batch = 1, .queries = rows, .capacity = 5, .values = mentions }, mask, objective, .{ .max_queries = rows }, .tensor_f32, .{ .backend = .{ .ptr = device.backend, .apply = elementwiseLossGradient }, .cotangent = weight });
            defer result.deinit();
            try std.testing.expectEqual(@as(?f32, weight), result.gradient_cotangent);
            for (result.gradient) |*g| if (g.* == 0) {
                g.* = 0;
            };
            digest.update(std.mem.sliceAsBytes(result.gradient));
        };
    };
    var hash: [32]u8 = undefined;
    digest.final(&hash);
    try std.testing.expectEqualStrings("49c24a3fcff5dbd51b421d9cc96d8b1d5886da01ef8c709bf22709ac026270b0", &std.fmt.bytesToHex(hash, .lower));
    const options = losses.QueryOptions{ .backend = .{ .ptr = device.backend, .apply = elementwiseLossGradient }, .cotangent = 0.2 };
    var masked = try losses.queryLossWithBackend(a, &.{ std.math.nan(f32), std.math.inf(f32) }, .{ .batch = 1, .queries = 2, .capacity = 0, .values = &.{} }, &.{ false, false }, .poisson_count, .{}, .tensor_f32, options);
    defer masked.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, masked.gradient);
    try std.testing.expectError(error.NonFiniteElementwiseLossMath, losses.queryLossWithBackend(a, &.{100}, .{ .batch = 1, .queries = 1, .capacity = 0, .values = &.{} }, &.{true}, .poisson_count, .{}, .tensor_f32, options));
}

test "CUDA boundary listwise loss matches pinned gradients and broadcast reductions" {
    const a = std.testing.allocator;
    const losses = @import("../../finetune/gliner_boundary_losses.zig");
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]usize{ 1, 7, 31, 129, 1025, 65539, 262147 }) |width| {
        const batch: usize = if (width > 2000) 1 else 2;
        const queries: usize = if (width > 2000) 1 else 7;
        const n = batch * queries * width;
        const values = try a.alloc(f32, n);
        defer a.free(values);
        const gold = try a.alloc(bool, n);
        defer a.free(gold);
        const valid = try a.alloc(bool, n);
        defer a.free(valid);
        const qm = try a.alloc(bool, batch * queries);
        defer a.free(qm);
        for (qm, 0..) |*value, row| value.* = row % 3 != 1;
        for ([_]bool{ false, true }) |candidate_major| for ([_]bool{ false, true }) |shared| {
            const shape = losses.Shape{ .batch = batch, .queries = queries, .candidates = width, .layout = if (candidate_major) .candidate_query else .query_candidate };
            for (0..batch) |b| for (0..queries) |q| for (0..width) |c| {
                const j = (b * queries + q) * width + c;
                const i = shape.index(b, q, c);
                const vi = if (shared) b * width + c else j;
                valid[i] = j % 5 != 0;
                gold[i] = j % 7 == 0;
                values[i] = if (!shared and !valid[i]) std.math.nan(f32) else (@as(f32, @floatFromInt((vi * 17) % 97)) - 48) / 8;
            };
            var loss = try losses.listwiseWithBackend(a, .{ .shape = shape, .values = values }, gold, valid, qm, .{ .max_candidates = @max(width, 65536) }, .tensor_f32, .{
                .backend = .{ .ptr = device.backend, .apply = listwiseLossGradient },
                .cotangent = 0.375,
                .reduce_queries = shared,
            });
            defer loss.deinit();
            try std.testing.expectEqual(@as(?f32, 0.375), loss.gradient_cotangent);
            try std.testing.expectEqual(if (shared) batch * width else n, loss.gradient.len);
            digest.update(std.mem.sliceAsBytes(loss.gradient));
        };
    }
    var hash: [32]u8 = undefined;
    digest.final(&hash);
    try std.testing.expectEqualStrings("039c5460dffd4dd851cbfd17c88ebad8c110246cee5313dc90e978f0df8c327f", &std.fmt.bytesToHex(hash, .lower));
    const math = &device.backend.training_math.?;
    try std.testing.expectError(error.InvalidCudaState, math.launchListwisePrepare(&device.backend.ctx, @splat(.{}), 1, 1, 1, false));
    try std.testing.expectError(error.InvalidCudaState, math.launchListwiseVjp(&device.backend.ctx, @splat(.{}), 1, 1, 1, false, false));
}

test "CUDA boundary scalar record loss matches pinned gradients across supported widths" {
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]usize{ 1, 7, 31, 129, 193, 1024, 1025, 2048, 2049, 3072, 9217, 10240, 12256, 16384, 65537, 262147 }) |width| {
        const rows: usize = if (width > 8192) 3 else 7;
        const n = rows * width;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const values = try scratch.alloc(f32, n);
        const masks = try scratch.alloc(i32, n);
        const columns = try scratch.alloc(i32, rows);
        const seeds = try scratch.alloc(f32, rows);
        const gradient = try scratch.alloc(f32, n);
        const losses = try scratch.alloc(f32, rows);
        for (values, masks, 0..) |*value, *mask, i| {
            const valid = i % width == 0 or i % 5 != 0;
            value.* = if (valid) (@as(f32, @floatFromInt((i * 17) % 97)) - 48) / 8 else -10000;
            mask.* = @as(i32, if (valid) 1 else 0) | @as(i32, if (i % width != 0 and i % 7 == 0 and valid) 2 else 0);
        }
        for (columns, seeds, 0..) |*column, *seed, row| {
            var selected: ?usize = null;
            for (0..width) |c| if (masks[row * width + c] & 2 != 0) {
                if (selected == null or values[row * width + c] > values[row * width + selected.?]) selected = c;
            };
            if (selected == null) {
                masks[row * width] |= 2;
                selected = 0;
            }
            column.* = if (row % 3 == 1) -1 else @intCast(selected.?);
            seed.* = if (row % 3 == 1) 0 else @as(f32, @floatFromInt(row + 1)) / 32;
        }
        const request = ops.record_loss_math.Request{ .logits = values, .masks = masks, .target_columns = columns, .seeds = seeds, .gradient = gradient, .losses = losses, .width = width, .max_elements = n };
        try recordLossGradient(device.backend, &request);
        // Inactive signed zero is not part of the numerical contract.
        for (gradient) |*v| if (v.* == 0) {
            v.* = 0;
        };
        for (losses) |*v| if (v.* == 0) {
            v.* = 0;
        };
        digest.update(std.mem.sliceAsBytes(gradient));
        digest.update(std.mem.sliceAsBytes(losses));
    }
    var hash: [32]u8 = undefined;
    digest.final(&hash);
    try std.testing.expectEqualStrings("1207960b7957f3ca15e201295b745c53e4a2730fd3dc8305840ac49b818b4268", &std.fmt.bytesToHex(hash, .lower));
}

test "CUDA boundary mixed record fields and objects preserve weighted reduction VJPs" {
    const a = std.testing.allocator;
    const matching = @import("../../finetune/gliner_boundary_matching.zig");
    const record_loss = @import("../../finetune/gliner_boundary_record_loss.zig");
    const schema = @import("../../pipelines/extraction_schema.zig");
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var targets: [3]matching.TargetMap = undefined;
    var matches: [3]matching.Matches = undefined;
    var groups: [3]record_loss.Group = undefined;
    var assignments: [3][27]f32 = undefined;
    for ([_]schema.RecordMode{ .natural, .latent, .anchorless }, 0..) |mode, index| {
        const instances: []const bool = if (mode == .anchorless) &.{ true, true, true } else &.{ true, true, false };
        targets[index] = .{ .arena = undefined, .fingerprint = @splat(0), .mode = mode, .fields = &.{ .{ .query = 0, .cardinality = .required_one }, .{ .query = 1, .cardinality = .zero_or_more }, .{ .query = 2, .cardinality = .optional_one } }, .candidate_spans = &.{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 } }, .candidate_valid = &.{ true, true }, .field_membership = &.{ true, true, true, false, true, true }, .instance_mask = instances, .anchor_field = if (mode == .natural) 0 else null, .anchor_candidates = if (mode == .natural) &.{ 0, 1, null } else &.{}, .record_ids = &.{"r"}, .record_indices = &.{0}, .gold_indicator = &.{ true, false, true, false, false, false }, .work = 0 };
        matches[index] = .{ .arena = undefined, .target_fingerprint = @splat(0), .pairs = &.{.{ .instance = 0, .record = 0, .annotation = 0 }}, .object_targets = if (mode == .natural) &.{ 0, 0, 0 } else &.{ 1, 0, 0 }, .object_mask = if (mode == .natural) &.{ false, false, false } else instances, .total_cost = 0, .work = 0 };
        for (&assignments[index], 0..) |*value, i| value.* = (@as(f32, @floatFromInt((i * 17 + index * 13) % 97)) - 48) / 8;
        groups[index] = .{ .target = &targets[index], .matches = &matches[index], .logits = .{ .objects = &.{ 0.2, -0.4, 0.8 }, .assignments = &assignments[index] } };
    }
    var result = try record_loss.computeWithBackends(a, &groups, .{ .object_weight = 1.3, .field_weight = 0.7 }, .{ .scalar = .{ .ptr = device.backend, .apply = recordLossGradient }, .binary = .{ .ptr = device.backend, .apply = elementwiseLossGradient }, .outer_weight = 0.37 });
    defer result.deinit();
    for (result.storage) |*value| if (value.* == 0) {
        value.* = 0;
    };
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.sliceAsBytes(result.storage), &hash, .{});
    try std.testing.expectEqualStrings("e046ba5c92837ee7f5d7c4fe307644ab350783fd280ae8d343e147c6dae07f5c", &std.fmt.bytesToHex(hash, .lower));
    try std.testing.expectApproxEqAbs(@as(f32, 0.6786817312240601), result.object_loss, 2e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.214106321334839), result.field_loss, 2e-6);
}

test "CUDA boundary consistency loss matches pinned weighted VJPs" {
    const a = std.testing.allocator;
    const losses = @import("../../finetune/gliner_boundary_losses.zig");
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    try device.backend.enableResidentTrainingMath();
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]usize{ 1, 31, 32, 33, 65, 384, 1025 }) |width| for (0..3) |pattern| {
        const batch: usize = 2;
        const queries: usize = 3;
        const bounds: usize = 9;
        const n = batch * queries * width;
        const m = batch * queries * bounds;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const x = try scratch.alloc(f32, n);
        const starts = try scratch.alloc(f32, m);
        const ends = try scratch.alloc(f32, m);
        const spans = try scratch.alloc(losses.Span, n);
        const valid = try scratch.alloc(bool, n);
        const keep = try scratch.alloc(bool, m);
        for (x, spans, valid, 0..) |*value, *span, *active, i| {
            value.* = (@as(f32, @floatFromInt((i * 17) % 401)) - 200) / 13;
            active.* = i % 5 != 3 and pattern != 2;
            const end = 1 + ((i % width) * 11) % (bounds - 1);
            span.* = .{ .start = if (!active.*) -9 else if (pattern == 1) 0 else @intCast(((i % width) * 7) % end), .end = if (!active.*) bounds + 9 else if (pattern == 1) 1 else @intCast(end) };
        }
        for (starts, ends, keep, 0..) |*s, *e, *active, i| {
            s.* = (@as(f32, @floatFromInt((i * 19) % 181)) - 90) / 17;
            e.* = (@as(f32, @floatFromInt((i * 23) % 181)) - 90) / 17;
            active.* = i % 7 != 0;
        }
        for ([_]f32{ 0.0001, 0.137, 1 }) |weight| {
            var result = try losses.marginalConsistencyWithBackend(a, .{ .shape = .{ .batch = batch, .queries = queries, .candidates = width }, .values = x }, spans, valid, .{ .shape = .{ .batch = batch, .queries = queries, .candidates = bounds }, .values = starts }, ends, keep, .{}, .{ .backend = .{ .ptr = device.backend, .apply = consistencyLossGradient }, .cotangent = weight });
            defer result.deinit();
            try std.testing.expectEqual(@as(?f32, weight), result.gradient_cotangent);
            for ([_][]f32{ result.pair_gradient, result.start_gradient, result.end_gradient }) |values| {
                for (values) |*v| if (v.* == 0) {
                    v.* = 0;
                };
                digest.update(std.mem.sliceAsBytes(values));
            }
        }
    };
    var hash: [32]u8 = undefined;
    digest.final(&hash);
    try std.testing.expectEqualStrings("8fc41553899b09e0a5fa2fb797468ec002c5e4609d5a2eb4672c445621f35fdc", &std.fmt.bytesToHex(hash, .lower));
    const math = &device.backend.training_math.?;
    try std.testing.expectError(error.InvalidCudaState, math.launchConsistencyPrepare(&device.backend.ctx, @splat(.{}), 1));
    try std.testing.expectError(error.InvalidCudaState, math.launchConsistencyBoundary(&device.backend.ctx, @splat(.{}), 1, 1, 1));
    try std.testing.expectError(error.InvalidCudaState, math.launchConsistencyPair(&device.backend.ctx, @splat(.{}), 1, 1));
}

test "CUDA resident clipping norm matches pinned Python CUDA reduction" {
    const a = std.testing.allocator;
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    try device.backend.enableResidentTrainingMath();
    const small = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 0.125, -0.75, 3.5 }, .shape = &.{3} } }, .{});
    defer cb.free(small);
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]usize{ 1, 3, 31, 32, 33, 127, 128, 129, 511, 512, 513, 2047, 2048, 2049, 65535, 65536, 65537, 131073, 1048576 }) |n| for (0..3) |mode| {
        const host = try a.alloc(f32, n);
        defer a.free(host);
        for (host, 0..) |*v, i| {
            v.* = (@as(f32, @floatFromInt((i * 17) % 401)) - 200) / 32;
            if (mode == 1 and i % 101 != 0) v.* = 0;
            if (mode == 2) v.* *= 1e10;
        }
        const t = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = host, .shape = &.{@intCast(n)} } }, .{});
        defer cb.free(t);
        const result = try cb.residentTrainingNorm(&.{ .{ .tensor = t, .elem_count = n }, .{ .tensor = small, .elem_count = 3 } }, .{ .profile = .pytorch_f32 });
        try std.testing.expect(result.finite);
        try std.testing.expectEqual(@as(usize, 4), result.download_bytes);
        try std.testing.expectEqual(@as(usize, 2), result.tensor_count);
        const value: f32 = @floatCast(result.norm);
        digest.update(std.mem.asBytes(&value));
    };
    var inputs: [334]resident.NormInput = undefined;
    var owned: usize = 0;
    defer for (inputs[0..owned]) |input| cb.free(input.tensor);
    for (&inputs, 0..) |*input, i| {
        const values = [_]f32{ @as(f32, @floatFromInt(i % 17)) - 8, 0.25, 7 };
        input.* = .{ .tensor = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &values, .shape = &.{3} } }, .{}), .elem_count = 3 };
        owned += 1;
    }
    for ([_]usize{ 1, 2, 3, 31, 32, 33, 127, 128, 129, 136, 255, 256, 257, 295, 334 }) |count| {
        const result = try cb.residentTrainingNorm(inputs[0..count], .{ .profile = .pytorch_f32 });
        try std.testing.expect(result.finite);
        const value: f32 = @floatCast(result.norm);
        digest.update(std.mem.asBytes(&value));
    }
    var hash: [32]u8 = undefined;
    digest.final(&hash);
    var expected: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&expected, "7b97d0c17c2c14c769e4c39d2fdc1ad51ef8e1c7bf48acc00029775476531e53");
    try std.testing.expectEqualSlices(u8, &expected, &hash);
    const empty = try cb.residentTrainingNorm(&.{}, .{ .profile = .pytorch_f32 });
    try std.testing.expectEqual(@as(f64, 0), empty.norm);
    try std.testing.expectEqual(@as(usize, 0), empty.download_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingNorm(&inputs, .{ .profile = .pytorch_f32, .max_tensors = 333 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingNorm(inputs[0..1], .{ .profile = .pytorch_f32, .max_partial_bytes = 11 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingNorm(inputs[0..1], .{ .profile = .pytorch_f32, .max_total_elements = 2 }));
    try std.testing.expectError(error.InvalidResidentTrainingShape, cb.residentTrainingNorm(&.{.{ .tensor = small, .elem_count = 4 }}, .{ .profile = .pytorch_f32 }));
    for ([_]f32{ std.math.inf(f32), std.math.nan(f32), std.math.floatMax(f32) }) |value| {
        const t = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{value}, .shape = &.{1} } }, .{});
        defer cb.free(t);
        const result = try cb.residentTrainingNorm(&.{.{ .tensor = t, .elem_count = 1 }}, .{ .profile = .pytorch_f32 });
        try std.testing.expect(!result.finite);
    }
}

test "CUDA resident validation preserves raw bits across tensor and chunk boundaries" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const empty = try cb.residentTrainingValidate(&.{}, .{});
    try std.testing.expect(empty.finite and empty.all_zero);
    try std.testing.expectEqual(@as(usize, 0), empty.partial_bytes + empty.download_bytes);
    const zero = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 0, -0.0 }, .shape = &.{2} } }, .{});
    defer cb.free(zero);
    // Includes signaling/quiet NaNs, both infinities, subnormals, normal extrema
    // and both signed zeros. Expectations classify IEEE bits independently.
    const patterns = [_]u32{ 0, 0x80000000, 1, 0x80000001, 0x007fffff, 0x00800000, 0x7f7fffff, 0x7f800000, 0xff800000, 0x7fc00001, 0x7f800001, 0xff800001 };
    for ([_]usize{ 1, 31, 32, 33, 255, 256, 257, 65535, 65536, 65537, 131073 }) |n| {
        const values = try std.testing.allocator.alloc(f32, n);
        defer std.testing.allocator.free(values);
        const actual = try std.testing.allocator.alloc(f32, n);
        defer std.testing.allocator.free(actual);
        for (patterns) |bits| {
            @memset(values, 0);
            values[n - 1] = @bitCast(bits);
            const input = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = &.{@intCast(n)} } }, .{});
            defer cb.free(input);
            var batch: [257]resident.ValidationInput = @splat(.{ .tensor = zero, .elem_count = 2 });
            batch[128] = .{ .tensor = input, .elem_count = n };
            const before = device.backend.stats;
            const summary = try cb.residentTrainingValidate(&batch, .{});
            try std.testing.expectEqual(bits & 0x7f800000 != 0x7f800000, summary.finite);
            try std.testing.expectEqual(bits & 0x7fffffff == 0, summary.all_zero);
            try std.testing.expectEqual(batch.len, summary.tensor_count);
            try std.testing.expectEqual(@as(usize, 4), summary.download_bytes);
            try std.testing.expectEqual(try resident.validationScratch(batch.len, 256 + try resident.validationChunks(n)), summary.partial_bytes);
            try std.testing.expectEqual(@as(u64, 4), device.backend.stats.d2h_bytes - before.d2h_bytes);
            try std.testing.expectEqual(before.h2d_bytes, device.backend.stats.h2d_bytes);
            try cb.glinerBoundaryDownload(input, actual);
            try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(values), std.mem.sliceAsBytes(actual));
        }
    }
    const batch: [257]resident.ValidationInput = @splat(.{ .tensor = zero, .elem_count = 2 });
    for ([_]usize{ 1, 127, 128, 129, 255, 256, 257 }) |n| {
        const summary = try cb.residentTrainingValidate(batch[0..n], .{});
        try std.testing.expect(summary.finite and summary.all_zero);
    }
    // Optional-backend fallback retains scaled norm semantics and accounting.
    var fallback = cb;
    var table = cb.vtable.*;
    table.residentTrainingValidate = null;
    fallback.vtable = &table;
    const summary = try fallback.residentTrainingValidate(batch[0..2], .{});
    try std.testing.expect(summary.finite and summary.all_zero);
    try std.testing.expectEqual(@as(usize, 24), summary.download_bytes);
}

test "CUDA resident validation rejects malformed input capture and cancellation before dispatch" {
    var device = try @import("../../graph/resident_training_fixture.zig").CudaDevice.init(std.testing.allocator);
    defer device.deinit();
    var cb = device.backend.computeBackend();
    const input = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{1}, .shape = &.{1} } }, .{});
    defer cb.free(input);
    const integers = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = &.{1}, .shape = &.{1} } }, .{});
    defer cb.free(integers);
    const values = [_]resident.ValidationInput{.{ .tensor = input, .elem_count = 1 }};
    const before = device.backend.stats;
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingValidate(&values, .{ .max_tensors = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingValidate(&values, .{ .max_total_elements = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingValidate(&values, .{ .max_partial_bytes = 7 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingValidate(&values, .{ .primitive = .{ .max_tensor_bytes = 4 } }));
    try std.testing.expectError(error.InvalidResidentTrainingShape, cb.residentTrainingValidate(&.{.{ .tensor = input, .elem_count = 2 }}, .{}));
    try std.testing.expectError(error.UnsupportedResidentTrainingDType, cb.residentTrainingValidate(&.{.{ .tensor = integers, .elem_count = 1 }}, .{}));
    const t = tensor(input);
    t.resident_owner = null;
    try std.testing.expectError(error.ForeignResidentTrainingTensor, cb.residentTrainingValidate(&values, .{}));
    t.resident_owner = device.backend;
    const bytes = t.buffer.len;
    t.buffer.len = 3;
    try std.testing.expectError(error.InvalidResidentTrainingShape, cb.residentTrainingValidate(&values, .{}));
    t.buffer.len = bytes;
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    cb.execution_control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, cb.residentTrainingValidate(&values, .{}));
    cb.execution_control = null;
    try device.backend.ctx.beginStreamCapture(@import("driver.zig").CU_STREAM_CAPTURE_MODE_THREAD_LOCAL);
    try std.testing.expectError(error.ResidentTrainingExternalFrame, cb.residentTrainingValidate(&values, .{}));
    const graph = try device.backend.ctx.endStreamCapture();
    device.backend.ctx.destroyGraph(graph);
    try std.testing.expectEqual(before.launch_other, device.backend.stats.launch_other);
    try std.testing.expectEqual(before.d2h_bytes, device.backend.stats.d2h_bytes);
    try std.testing.expectEqual(before.h2d_bytes, device.backend.stats.h2d_bytes);
    // Cancellation after the first descriptor batch must release private
    // scratch safely. A subsequent call may reuse it and must overwrite all
    // partials, rather than consuming uninitialized/stale flags.
    const CancelAfter = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 4) return error.Cancelled;
        }
    };
    var cancellation = CancelAfter{};
    cb.execution_control = .{ .ptr = &cancellation, .check_fn = CancelAfter.check };
    const batch: [257]resident.ValidationInput = @splat(values[0]);
    try std.testing.expectError(error.Cancelled, cb.residentTrainingValidate(&batch, .{}));
    cb.execution_control = null;
    try std.testing.expect(device.backend.stats.launch_other > before.launch_other);
    const summary = try cb.residentTrainingValidate(&batch, .{});
    try std.testing.expect(summary.finite and !summary.all_zero);
}
