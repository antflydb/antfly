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
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    static_workspace_bytes: usize,
    backend_workspace_reserved: bool = false,

    fn acquire(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionLease {
        return self.controller.tryAcquire(
            self.backend_class,
            self.limits,
            try self.estimateAmounts(inputs, output_info),
            true,
        );
    }

    fn estimateAmounts(
        self: RunAdmission,
        inputs: []const Tensor,
        output_info: []const TensorInfo,
    ) !memory.AdmissionAmounts {
        const input_bytes = try tensorBytes(inputs);
        const output_bytes = try estimatedOutputBytes(inputs, output_info);
        const dynamic_base = try addBytes(input_bytes, output_bytes);
        const dynamic_workspace = try mulBytes(dynamic_base, 6);
        const workspace = @max(self.static_workspace_bytes, dynamic_workspace);
        const host_output_peak = try mulBytes(output_bytes, 2);
        const host_io_peak = try addBytes(input_bytes, host_output_peak);

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
};

fn tensorBytes(tensors: []const Tensor) !usize {
    var total: usize = 0;
    for (tensors) |tensor|
        total = try addBytes(total, tensor.data.len);
    return total;
}

fn addBytes(lhs: usize, rhs: usize) !usize {
    return std.math.add(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

fn mulBytes(lhs: usize, rhs: usize) !usize {
    return std.math.mul(usize, lhs, rhs) catch error.ResourceLimitExceeded;
}

fn estimatedOutputBytes(
    inputs: []const Tensor,
    output_info: []const TensorInfo,
) !usize {
    const reference_shape: []const i64 = if (inputs.len > 0)
        inputs[0].shape
    else
        &.{};
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
        defer if (resource_lease) |*lease| lease.release();
        return self.vtable.run(self.ptr, inputs, allocator);
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
                try admission.acquire(&.{}, self.outputInfo())
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
        return allocator.alloc(Tensor, 0);
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
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
    };
    const cpu_amounts = try cpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 4672), cpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpu_amounts.backend_scratch_bytes);

    const gpu = RunAdmission{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{},
        .static_workspace_bytes = 4096,
        .backend_workspace_reserved = true,
    };
    const gpu_amounts = try gpu.estimateAmounts(&.{input}, &outputs);
    try std.testing.expectEqual(@as(usize, 576), gpu_amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), gpu_amounts.backend_scratch_bytes);

    var probe = AdmissionProbeSession{ .controller = &controller };
    const admitted_session = Session{
        .ptr = &probe,
        .vtable = &AdmissionProbeSession.vtable,
        .run_admission = cpu,
    };
    const result = try admitted_session.run(&.{input}, std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expect(probe.observed_active_lease);
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );
}
