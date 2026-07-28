// Copyright 2026 Antfly, Inc.
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
const ops = @import("../ops/ops.zig");
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const BackendType = @import("backends.zig").BackendType;
const memory = @import("../runtime/tier/memory.zig");

pub const ResidentInput = struct {
    value: ops.CT,
    backend: *const ops.ComputeBackend,
    estimated_bytes: usize = 0,
    shape: []const i64 = &.{},
};

pub const ResidentOutputs = struct {
    outputs: []ops.CT,
    backend: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    resource_lease: ?memory.AdmissionLease = null,

    pub fn deinit(self: *ResidentOutputs) void {
        for (self.outputs, 0..) |output, idx| {
            var seen = false;
            for (self.outputs[0..idx]) |prev| {
                if (prev == output) {
                    seen = true;
                    break;
                }
            }
            if (!seen) self.backend.free(output);
        }
        self.allocator.free(self.outputs);
        if (self.resource_lease) |*lease| lease.release();
        self.outputs = &.{};
        self.resource_lease = null;
    }
};

/// Allocation-free request admission attached by ModelManager to serving
/// sessions. The static workspace estimate covers model intermediates that
/// cannot be derived from public output shapes; tensor-derived bytes scale it
/// for dynamic batches and sequence lengths.
pub const RunAdmission = struct {
    pub const ModelProfile = struct {
        hidden_size: usize = 0,
        intermediate_size: usize = 0,
        attention_heads: usize = 0,
        quadratic_attention: bool = false,
    };

    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    static_workspace_bytes: usize,
    backend_workspace_reserved: bool = false,
    model_profile: ModelProfile = .{},

    fn acquire(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        const request = try RunRequest.fromTensors(inputs);
        return self.controller.tryAcquire(
            self.backend_class,
            self.limits,
            try self.estimateRequest(request, output_info),
            true,
        );
    }

    fn acquireRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        return self.controller.tryAcquire(
            self.backend_class,
            self.limits,
            try self.estimateRequest(request, output_info),
            true,
        );
    }

    fn acquireResidentInputs(
        self: RunAdmission,
        inputs: []const ResidentInput,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        return self.controller.tryAcquire(
            self.backend_class,
            self.limits,
            try self.estimateResidentAmounts(inputs, output_info),
            true,
        );
    }

    fn estimateResidentAmounts(
        self: RunAdmission,
        inputs: []const ResidentInput,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        var input_bytes: usize = 0;
        for (inputs) |input| input_bytes = try addBytes(input_bytes, input.estimated_bytes);
        return self.estimateRequest(.{
            .batch = residentShapeDimension(inputs, 0),
            .sequence = residentShapeDimension(inputs, 1),
            .input_bytes = input_bytes,
        }, output_info);
    }

    fn estimateAmounts(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        return self.estimateRequest(try RunRequest.fromTensors(inputs), output_info);
    }

    fn estimateRequest(
        self: RunAdmission,
        request: RunRequest,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        const input_bytes = request.input_bytes;
        const reference_shape = [_]i64{
            std.math.cast(i64, request.batch) orelse return error.ResourceLimitExceeded,
            std.math.cast(i64, request.sequence) orelse return error.ResourceLimitExceeded,
        };
        const output_bytes = try estimatedOutputBytes(&reference_shape, output_info);
        const dynamic_base = try addBytes(input_bytes, output_bytes);
        const dynamic_workspace = try mulBytes(dynamic_base, 6);
        const workspace = @max(
            self.static_workspace_bytes,
            @max(dynamic_workspace, try self.profiledWorkspace(request)),
        );
        const host_output_peak = try mulBytes(output_bytes, 2);
        const host_io_peak = try addBytes(
            request.host_preprocess_bytes,
            try addBytes(input_bytes, host_output_peak),
        );

        return switch (self.backend_class) {
            .cpu => .{
                .host_scratch_bytes = try addBytes(host_io_peak, workspace),
            },
            .gpu => .{
                // Request inputs and materialized outputs occupy shared host
                // RAM. Device staging, activations, and outputs share the
                // backend workspace.
                .host_scratch_bytes = host_io_peak,
                .backend_scratch_bytes = if (self.backend_workspace_reserved)
                    0
                else
                    workspace,
            },
        };
    }

    fn profiledWorkspace(self: RunAdmission, request: RunRequest) !usize {
        const profile = self.model_profile;
        if (profile.hidden_size == 0 or request.batch == 0 or request.sequence == 0)
            return 0;

        const tokens = try mulBytes(request.batch, request.sequence);
        // Attention and FFN phases are sequential, so reserve the larger peak
        // rather than summing mutually exclusive intermediates.
        const attention_floats = try mulBytes(profile.hidden_size, 6);
        const ffn_floats = try addBytes(
            try mulBytes(profile.hidden_size, 2),
            try mulBytes(profile.intermediate_size, 3),
        );
        const floats_per_token = @max(attention_floats, ffn_floats);
        var peak = try mulBytes(
            try mulBytes(tokens, floats_per_token),
            @sizeOf(f32),
        );

        if (profile.quadratic_attention and profile.attention_heads > 0) {
            const score_elements = try mulBytes(
                try mulBytes(
                    try mulBytes(request.batch, profile.attention_heads),
                    request.sequence,
                ),
                request.sequence,
            );
            peak = @max(peak, try mulBytes(score_elements, @sizeOf(f32)));
        }
        return peak;
    }
};

pub const RunRequest = struct {
    batch: usize = 1,
    sequence: usize = 1,
    input_bytes: usize = 0,
    host_preprocess_bytes: usize = 0,

    pub fn fromTensors(tensors: []const Tensor) !RunRequest {
        var input_bytes: usize = 0;
        for (tensors) |tensor| input_bytes = try addBytes(input_bytes, tensor.data.len);
        const shape = if (tensors.len > 0) tensors[0].shape else &.{};
        return .{
            .batch = positiveDimension(shape, 0),
            .sequence = positiveDimension(shape, 1),
            .input_bytes = input_bytes,
        };
    }
};

fn addBytes(lhs: usize, rhs: usize) !usize {
    return std.math.add(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

fn mulBytes(lhs: usize, rhs: usize) !usize {
    return std.math.mul(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

fn estimatedOutputBytes(
    reference_shape: []const i64,
    output_info: []const TensorInfo,
) !usize {
    var total: usize = 0;
    for (output_info) |info| {
        var elements: usize = 1;
        for (info.shape, 0..) |declared_dim, axis| {
            const resolved_dim: usize = if (declared_dim > 0)
                std.math.cast(usize, declared_dim) orelse
                    return error.ResourceLimitExceeded
            else if (axis < reference_shape.len and reference_shape[axis] > 0)
                std.math.cast(usize, reference_shape[axis]) orelse
                    return error.ResourceLimitExceeded
            else
                1;
            elements = try mulBytes(elements, resolved_dim);
        }
        const bytes = try mulBytes(elements, info.dtype.byteSize());
        total = try addBytes(total, bytes);
    }
    return total;
}

fn positiveDimension(shape: []const i64, axis: usize) usize {
    if (axis >= shape.len or shape[axis] <= 0) return 1;
    return std.math.cast(usize, shape[axis]) orelse 1;
}

fn residentShapeDimension(inputs: []const ResidentInput, axis: usize) usize {
    if (inputs.len == 0) return 1;
    return positiveDimension(inputs[0].shape, axis);
}

const OutputAdmission = struct {
    allocator: std.mem.Allocator,
    lease: memory.AdmissionLease,
    remaining: std.atomic.Value(usize),

    fn release(raw: *anyopaque) void {
        const self: *OutputAdmission = @ptrCast(@alignCast(raw));
        const previous = self.remaining.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        self.lease.release();
        self.allocator.destroy(self);
    }
};

/// Session represents a loaded model that can run forward passes.
/// This is the core abstraction all backends implement.
pub const Session = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    run_admission: ?RunAdmission = null,

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor,
        inputInfo: *const fn (ptr: *anyopaque) []const TensorInfo,
        outputInfo: *const fn (ptr: *anyopaque) []const TensorInfo,
        backend: *const fn (ptr: *anyopaque) BackendType,
        close: *const fn (ptr: *anyopaque) void,
        runResident: ?*const fn (ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror!?ResidentOutputs = null,
        runResidentInputs: ?*const fn (ptr: *anyopaque, inputs: []const ResidentInput, allocator: std.mem.Allocator) anyerror!?ResidentOutputs = null,
    };

    /// Run a forward pass with the given input tensors.
    pub fn run(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        var resource_lease = if (self.run_admission) |admission|
            try admission.acquire(inputs, self.outputInfo())
        else
            null;
        errdefer if (resource_lease) |*lease| lease.release();
        const outputs = try self.vtable.run(self.ptr, inputs, allocator);
        if (resource_lease == null) return outputs;
        if (outputs.len == 0) {
            resource_lease.?.release();
            return outputs;
        }
        var retained_output_bytes: usize = 0;
        for (outputs) |output| {
            retained_output_bytes = addBytes(
                retained_output_bytes,
                output.data.len,
            ) catch std.math.maxInt(usize);
            retained_output_bytes = addBytes(
                retained_output_bytes,
                mulBytes(output.shape.len, @sizeOf(i64)) catch
                    std.math.maxInt(usize),
            ) catch std.math.maxInt(usize);
        }
        resource_lease.?.retain(.{
            .host_scratch_bytes = retained_output_bytes,
        }) catch {};

        const output_admission = allocator.create(OutputAdmission) catch |err| {
            deinitTensorSlice(outputs, allocator);
            return err;
        };
        output_admission.* = .{
            .allocator = allocator,
            .lease = resource_lease.?,
            .remaining = std.atomic.Value(usize).init(outputs.len),
        };
        resource_lease = null;
        for (outputs) |*output| {
            std.debug.assert(output.lifetime == null);
            output.lifetime = .{
                .context = output_admission,
                .release = OutputAdmission.release,
            };
        }
        return outputs;
    }

    /// Reserve the full request peak before preprocessing allocates input
    /// buffers. The permit owns that lease and executes without double-counting.
    pub fn admit(self: Session, request: RunRequest) !RunPermit {
        return .{
            .session = self,
            .lease = if (self.run_admission) |admission|
                try admission.acquireRequest(request, self.outputInfo())
            else
                null,
        };
    }

    pub fn inputInfo(self: Session) []const TensorInfo {
        return self.vtable.inputInfo(self.ptr);
    }

    pub fn outputInfo(self: Session) []const TensorInfo {
        return self.vtable.outputInfo(self.ptr);
    }

    pub fn backend(self: Session) BackendType {
        return self.vtable.backend(self.ptr);
    }

    pub fn close(self: Session) void {
        self.vtable.close(self.ptr);
    }

    pub fn runResident(self: Session, inputs: []const Tensor, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.vtable.runResident) |run_resident| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquire(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var outputs = (try run_resident(self.ptr, inputs, allocator)) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            return outputs;
        }
        return null;
    }

    pub fn runResidentInputs(self: Session, inputs: []const ResidentInput, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.vtable.runResidentInputs) |run_resident_inputs| {
            var resource_lease = if (self.run_admission) |admission|
                try admission.acquireResidentInputs(inputs, self.outputInfo())
            else
                null;
            errdefer if (resource_lease) |*lease| lease.release();
            var outputs = (try run_resident_inputs(self.ptr, inputs, allocator)) orelse {
                if (resource_lease) |*lease| lease.release();
                return null;
            };
            std.debug.assert(outputs.resource_lease == null);
            outputs.resource_lease = resource_lease;
            return outputs;
        }
        return null;
    }
};

pub const RunPermit = struct {
    session: Session,
    lease: ?memory.AdmissionLease,

    pub fn run(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        return self.session.vtable.run(self.session.ptr, inputs, allocator);
    }

    pub fn runResident(
        self: *RunPermit,
        inputs: []const Tensor,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        const run_resident = self.session.vtable.runResident orelse return null;
        return run_resident(self.session.ptr, inputs, allocator);
    }

    pub fn runResidentInputs(
        self: *RunPermit,
        inputs: []const ResidentInput,
        allocator: std.mem.Allocator,
    ) !?ResidentOutputs {
        const run_resident_inputs = self.session.vtable.runResidentInputs orelse
            return null;
        return run_resident_inputs(self.session.ptr, inputs, allocator);
    }

    pub fn deinit(self: *RunPermit) void {
        if (self.lease) |*lease| lease.release();
        self.lease = null;
    }
};

fn deinitTensorSlice(tensors: []Tensor, allocator: std.mem.Allocator) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

test "session vtable layout" {
    // Ensure the vtable has all required function pointers.
    const info = @typeInfo(Session.VTable);
    try std.testing.expectEqual(@as(usize, 7), info.@"struct".fields.len);
}

const AdmissionProbeSession = struct {
    controller: *memory.AdmissionController,
    observed_active_lease: bool = false,

    fn run(
        ptr: *anyopaque,
        _: []const Tensor,
        allocator: std.mem.Allocator,
    ) ![]Tensor {
        const self: *AdmissionProbeSession = @ptrCast(@alignCast(ptr));
        self.observed_active_lease =
            self.controller.snapshot().host_scratch_bytes > 0;
        const outputs = try allocator.alloc(Tensor, 1);
        errdefer allocator.free(outputs);
        outputs[0] = try Tensor.initFloat32(
            allocator,
            "output",
            &.{1},
            &.{1.0},
        );
        return outputs;
    }

    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{};
    }

    fn backend(_: *anyopaque) BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}

    const vtable = Session.VTable{
        .run = run,
        .inputInfo = inputInfo,
        .outputInfo = outputInfo,
        .backend = backend,
        .close = close,
    };
};

test "run admission scales dynamic outputs and honors reserved backend workspace" {
    var controller = memory.AdmissionController{};
    var input_bytes = [_]u8{0} ** 64;
    const input = Tensor{
        .data = &input_bytes,
        .dtype = .i64,
        .shape = &.{ 2, 4 },
        .name = "input_ids",
        .allocator = std.testing.allocator,
        .owns_data = false,
        .owns_shape = false,
    };
    const outputs = [_]TensorInfo{.{
        .name = "last_hidden_state",
        .dtype = .f32,
        .shape = &.{ -1, -1, 8 },
    }};

    const cpu = RunAdmission{
        .context = &controller,
        .acquire_fn = acquireFromController,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
    };
    const cpu_amounts = try cpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 4672), cpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpu_amounts.backend_scratch_bytes);

    const gpu = RunAdmission{
        .context = &controller,
        .acquire_fn = acquireFromController,
        .backend_class = .gpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .backend_workspace_reserved = true,
    };
    const gpu_amounts = try gpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 576), gpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), gpu_amounts.backend_scratch_bytes);
    const resident_shape = [_]i64{ 8, 4 };
    const resident_amounts = try gpu.estimateResidentAmounts(&.{.{
        .value = undefined,
        .backend = undefined,
        .estimated_bytes = 8 * 4 * 8,
        .shape = &resident_shape,
    }}, &outputs);
    try std.testing.expectEqual(@as(usize, 2304), resident_amounts.host_scratch_bytes);

    var probe = AdmissionProbeSession{ .controller = &controller };
    const admitted_session = Session{
        .ptr = &probe,
        .vtable = &AdmissionProbeSession.vtable,
        .run_admission = cpu,
    };
    const result = try admitted_session.run(&.{input}, std.testing.allocator);
    try std.testing.expect(probe.observed_active_lease);
    try std.testing.expect(
        controller.snapshot().host_scratch_bytes >= @sizeOf(f32),
    );
    for (result) |*output| output.deinit();
    std.testing.allocator.free(result);
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );

    var permit = try admitted_session.admit(.{
        .batch = 2,
        .sequence = 4,
        .input_bytes = 64,
        .host_preprocess_bytes = 128,
    });
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
    const permitted_result = try permit.run(&.{input}, std.testing.allocator);
    for (permitted_result) |*output| output.deinit();
    std.testing.allocator.free(permitted_result);
    try std.testing.expect(controller.snapshot().host_scratch_bytes > 0);
    permit.deinit();
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );

    const profiled = RunAdmission{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .model_profile = .{
            .hidden_size = 768,
            .intermediate_size = 3072,
            .attention_heads = 12,
            .quadratic_attention = true,
        },
    };
    const profiled_amounts = try profiled.estimateRequest(.{
        .batch = 16,
        .sequence = 512,
        .input_bytes = 16 * 512 * 16,
        .host_preprocess_bytes = 16 * 512 * 32,
    }, &outputs);
    try std.testing.expect(profiled_amounts.host_scratch_bytes > 256 * 1024 * 1024);
}
