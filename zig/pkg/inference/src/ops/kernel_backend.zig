// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Target-owned descriptor execution and storage hooks. A backend installs
//! its own table as a unit; deriving other compute operations from a native
//! table never inherits native tensor casts into a device implementation.
const CT = @import("../graph/backend_contracts.zig").CT;
const kernel_descriptor = @import("kernel_descriptor.zig");
const physical_storage = @import("physical_storage.zig");
pub const VTable = struct {
    /// Borrow the current frame's stable storage table. Entries may be filled
    /// during warmup; capture retains their leases before recording addresses.
    physicalExecutionScope: ?*const fn (ctx: *anyopaque, storages: ?[]const ?*physical_storage.Lease) anyerror!void = null,
    allocatePhysicalStorage: ?*const fn (ctx: *anyopaque, byte_size: usize) anyerror!?CT = null,
    physicalTensorView: ?*const fn (ctx: *anyopaque, request: physical_storage.View) anyerror!?CT = null,
    assignedKernelAuxiliaries: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) anyerror!kernel_descriptor.AuxiliaryRequirements = null,
    assignedKernelWorkspace: ?*const fn (ctx: *anyopaque, descriptor: *const kernel_descriptor.Descriptor) anyerror!kernel_descriptor.WorkspaceRequirement = null,
    runAssignedKernel: ?*const fn (ctx: *anyopaque, request: kernel_descriptor.Invocation) anyerror!bool = null,
    createPhysicalCompletion: ?*const fn (ctx: *anyopaque) anyerror!physical_storage.Completion = null,
    readFloat32Into: ?*const fn (ctx: *anyopaque, tensor: CT, output: []f32) anyerror!bool = null,
    writePhysicalFloat32: ?*const fn (ctx: *anyopaque, output: CT, input: []const f32) anyerror!void = null,
    retainImmutableTensor: ?*const fn (ctx: *anyopaque, tensor: CT) anyerror!?CT = null,
};
