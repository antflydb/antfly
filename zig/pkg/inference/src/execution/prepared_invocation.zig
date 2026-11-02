// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One synchronous execution's selected program and claimed physical storage.
//! Preparation owns exact model handles; admission sees verified warm storage.
const std = @import("std");
const session_mod = @import("../backends/session.zig");
const Session = session_mod.Session;
const Tensor = @import("../backends/tensor.zig").Tensor;
const memory = @import("../runtime/tier/memory.zig");
const storage = @import("storage_admission.zig");
const physical = @import("physical_buffers.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Inputs = union(enum) { host: []const Tensor, resident: []const session_mod.ResidentInput };
pub const Placement = enum { host, resident };
pub const Result = union(Placement) { host: []Tensor, resident: session_mod.ResidentOutputs };

pub const Requirement = struct {
    /// IO/output/retained-input reservations are protected from physical credit.
    protected: memory.AdmissionAmounts = .{},
    /// Compatibility operators still need their independently estimated scratch.
    compatibility: memory.AdmissionAmounts = .{},
    /// Exact prepared program's mutable physical spans; constants are excluded.
    physical: memory.AdmissionAmounts = .{},
    /// Already-admitted spans in this handle's exclusively claimed idle frame.
    warm_credit: memory.AdmissionAmounts = .{},
};

pub const Services = struct {
    /// Check public dispatch identity before casting any implementation pointer.
    accepts: *const fn (Session) bool,
    prepare: *const fn (Session, session_mod.ShapeInputs, std.mem.Allocator, ?Control) anyerror!PreparedInvocation,
};

pub fn prepare(session: Session, shapes: session_mod.ShapeInputs, allocator: std.mem.Allocator, control: ?Control) !?PreparedInvocation {
    const services = session.invocation_services orelse return null;
    if (!services.accepts(session)) return null;
    if (control) |active| try active.check();
    return try services.prepare(session, shapes, allocator, control);
}

pub const PreparedInvocation = struct {
    /// Preserve public execution/admission policy while selecting physical code.
    published: Session,
    target: *anyopaque,
    /// Root owner is retained by preparation, even without manager resources.
    root_lifetime: *Session.ModelLifetime,
    /// Null when root and selected program are the same numerical owner.
    target_lifetime: ?*Session.ModelLifetime = null,
    frame_claim: ?physical.FrameClaim = null,
    requirement: Requirement = .{},
    /// Backend-owned inline scalar metadata (for example preparation events).
    /// Contains no borrowed pointer and needs no per-invocation allocation.
    backend_cookie: usize = 0,
    dispatch: *const fn (*PreparedInvocation, Inputs, Placement, std.mem.Allocator, ?Control, ?storage.Capability) anyerror!Result,
    reclaim_idle: ?*const fn (*PreparedInvocation, ?Control) anyerror!bool = null,
    executed: bool = false,
    closed: bool = false,

    pub fn run(self: *PreparedInvocation, inputs: Inputs, placement: Placement, allocator: std.mem.Allocator, control: ?Control, capability: ?storage.Capability) !Result {
        if (self.closed or self.executed) return error.PreparedInvocationAlreadyConsumed;
        if (control) |active| try active.check();
        self.executed = true;
        return @import("invocation_observation.zig").call(self.published.invocation_diagnostics, self.dispatch, .{ self, inputs, placement, allocator, control, capability });
    }

    pub fn deinit(self: *PreparedInvocation) void {
        if (self.closed) return;
        self.closed = true;
        // Releasing an evicted program may enter a driver even after admission
        // rejection. Keep its model's cleanup protection and IO owner alive.
        var scope = self.published.beginClose();
        defer scope.deinit();
        if (self.frame_claim) |*claim| claim.release();
        self.frame_claim = null;
        if (self.target_lifetime) |owner| owner.release();
        self.target_lifetime = null;
        self.root_lifetime.release();
    }
};
