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

pub const ResidentInput = struct {
    value: ops.CT,
    backend: *const ops.ComputeBackend,
};

pub const ResidentOutputs = struct {
    outputs: []ops.CT,
    backend: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ResidentOutputs) void {
        for (self.outputs) |output| self.backend.free(output);
        self.allocator.free(self.outputs);
        self.outputs = &.{};
    }
};

/// Session represents a loaded model that can run forward passes.
/// This is the core abstraction all backends implement.
pub const Session = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

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
            return try run_resident(self.ptr, inputs, allocator);
        }
        return null;
    }

    pub fn runResidentInputs(self: Session, inputs: []const ResidentInput, allocator: std.mem.Allocator) !?ResidentOutputs {
        if (self.vtable.runResidentInputs) |run_resident_inputs| {
            return try run_resident_inputs(self.ptr, inputs, allocator);
        }
        return null;
    }
};

test "session vtable layout" {
    // Ensure the vtable has all required function pointers.
    const info = @typeInfo(Session.VTable);
    try std.testing.expectEqual(@as(usize, 7), info.@"struct".fields.len);
}
