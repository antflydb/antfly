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

//! Immutable model preparation before publication. Serving holds construction
//! admission and model serialization; this owner holds process protection and
//! tears down temporary backends before publishing resident preparation.
const std = @import("std");
const build_options = @import("build_options");
const Session = @import("../backends/session.zig").Session;
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const boundary = @import("../ops/gliner_boundary_resident.zig");
const MetalCompute = if (build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute else void;

/// Synchronous borrowed construction service. The actual model owner outlives
/// this call; preparation never retains the callback or a temporary backend.
pub const BackendSource = struct {
    ptr: *anyopaque,
    session: Session,
    create: *const fn (*anyopaque, Session, std.mem.Allocator, bool) anyerror!ops.ComputeBackend,
    runtime_identity: ?*const fn (*anyopaque) anyerror!*anyopaque = null,

    fn acquire(self: BackendSource, allocator: std.mem.Allocator, begin_request: bool) !ops.ComputeBackend {
        return self.create(self.ptr, self.session, allocator, begin_request);
    }
};

pub fn prepareBoundary(source: BackendSource, published: Session, allocator: std.mem.Allocator, owner: *boundary.Owner, store: ?@import("tensor_store.zig").TensorStore, config: *const @import("gliner_boundary.zig").Config, control: ?Control) !void {
    if (comptime !build_options.enable_metal) return;
    const effective = control orelse Control{};
    try effective.check();
    if (owner.isReady()) return;
    var protection = try effective.enterUninterruptible(published.interruption());
    defer protection.deinit();
    errdefer owner.abortPreparation();
    {
        // Constant ownership metadata belongs to the model allocator.
        var backend = try source.acquire(allocator, true);
        defer backend.deinit();
        backend.execution_control = control;
        const runtime_identity = try (source.runtime_identity orelse return error.UnsupportedGlinerBoundaryDevice)(source.ptr);
        try owner.prepare(store orelse return error.UnsupportedGlinerBoundaryBundle, runtime_identity, control);
        try backend.glinerBoundaryResidentPreparation(true);
        var preparation_access = true;
        defer if (preparation_access) backend.glinerBoundaryResidentPreparation(false) catch {};
        try @import("../architectures/gliner/boundary_engine_device.zig").prepareResidentConstants(&backend, allocator, config, control);
        try backend.glinerBoundaryResidentPreparation(false);
        preparation_access = false;
    }
    try effective.check();
    try owner.finishPreparation();
}

pub fn prepareLaya(source: BackendSource, published: Session, allocator: std.mem.Allocator, config: @import("../architectures/modern_bert.zig").Config, control: ?Control) !void {
    if (comptime !build_options.enable_metal) return;
    const active = control orelse Control{};
    try active.check();
    // Preserve offline cooperative construction and the existing explicit
    // controlled-call process boundary for this preparation family.
    var protection = if (control) |c| try c.enterUninterruptible(published.interruption()) else null;
    defer if (protection) |*p| p.deinit();
    var backend = try source.acquire(allocator, false);
    defer backend.deinit();
    backend.execution_control = control;
    const compute: *MetalCompute = @ptrCast(@alignCast(backend.ptr));
    _ = try @import("../ops/laya_metal.zig").Owner.prepare(compute, config, control);
}
