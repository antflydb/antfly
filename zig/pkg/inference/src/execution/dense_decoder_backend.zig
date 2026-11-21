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

//! Accepted target services for the original eager F32 decoder programs.
//! Preparation proves current model roles and destinations; the common
//! schedule owns admission, liveness and storage, never numerical dispatch.
const ops = @import("../ops/ops.zig");
const Services = @import("../ops/kernel_backend.zig").DenseDecoderServices;
const Plane = @import("storage_ownership.zig").Plane;
pub const ProjectionMode = @import("../ops/kernel_backend.zig").ProjectionMode;

fn services(cb: *const ops.ComputeBackend) ?*const Services {
    const kernels = cb.vtable.kernels orelse return null;
    // Reject copied/foreign tables BEFORE any opaque context or CT access.
    if (!kernels.accepts(cb.vtable)) return null;
    const selected = kernels.denseDecoder orelse return null;
    if (!selected.available(cb.ptr)) return null;
    return selected;
}

pub fn available(cb: *const ops.ComputeBackend) bool {
    return services(cb) != null;
}

pub fn heldWeight(cb: *const ops.ComputeBackend, model: *const anyopaque, value: ops.CT, name: ?[]const u8) !Plane {
    const selected = services(cb) orelse return error.UnsupportedDenseSchedule;
    return selected.heldWeightF32(cb.ptr, model, value, name);
}

pub fn heldShape(cb: *const ops.ComputeBackend, model: *const anyopaque, value: ops.CT, name: []const u8) !@import("ml").graph.Shape {
    return (try heldWeight(cb, model, value, name)).descriptor.shape;
}

pub fn projectionInto(cb: *const ops.ComputeBackend, descriptor: *const ops.kernel_descriptor.Descriptor, inputs: []const ops.CT, output: ops.CT, mode: ProjectionMode) !bool {
    const selected = services(cb) orelse return error.UnsupportedDenseSchedule;
    return selected.projectionInto(cb.ptr, descriptor, inputs, output, mode);
}

test "dense services reject foreign tables before model tensor or context access" {
    const std = @import("std");
    const native = @import("../ops/native_compute.zig");
    var copied = native.vtable_impl;
    std.mem.doNotOptimizeAway(&copied);
    const cb = ops.ComputeBackend{ .ptr = @ptrFromInt(16), .vtable = &copied };
    try std.testing.expect(!available(&cb));
    try std.testing.expectError(error.UnsupportedDenseSchedule, heldWeight(&cb, @ptrFromInt(16), @ptrFromInt(16), "not-a-weight"));
    try std.testing.expectError(error.UnsupportedDenseSchedule, projectionInto(&cb, undefined, &.{}, @ptrFromInt(16), .query));
}
