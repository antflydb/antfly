// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Metadata-only target publication. The caller retains the loaded-model owner
//! from snapshot through independent endpoint retention and actual adoption.
//! This value grants no request operation, stream, tensor or numerical access.
//! Late owners retain independent endpoints; they never dereference this borrow.
pub const Target = union(enum) {
    cuda: Cuda,
    metal: Metal,

    pub const Cuda = struct {
        implementation_context: *anyopaque,
        implementation_table: *const anyopaque,
        primary_context: *anyopaque,
        device: i32,
    };
    pub const Metal = struct {
        /// Exact initialized provider runtime, never merely an MTLDevice.
        runtime: *anyopaque,
        /// Short cold publication/cancel exclusion in the actual provider.
        /// Caller model pin keeps this pointer alive; never retain it in a
        /// late owner or hold it while joining the model execution lane.
        exclusion: *@import("../execution/operation_gate.zig").Gate,
    };
};

/// Actual model-owned primary/provider lifetime through the cold transaction.
/// This is consumed by a bank wrapper; it cannot become a late SDK record.
/// Successful adoption releases it only at the explicit post-lane boundary.
pub const Retained = struct {
    target: Target,
    model: ?*@import("session.zig").Session.ModelLifetime,

    pub fn acquire(session: @import("session.zig").Session, control: ?@import("../execution_control.zig").InferenceExecutionControl) !?Retained {
        const model = session.model_lifetime orelse return null;
        _ = model.retain();
        var held = true;
        defer if (held) model.release();
        const target = (try session.coldComputeTarget(control)) orelse return null;
        held = false;
        return .{ .target = target, .model = model };
    }
    pub fn deinitWithBoundary(self: *Retained, epoch: *const @import("../execution/retirement_sink.zig").Epoch) void {
        const model = self.model orelse return;
        self.model = null;
        model.releaseWithBoundary(epoch);
    }
    pub fn deinit(self: *Retained) void {
        const model = self.model orelse return;
        self.model = null;
        model.release();
    }
};
