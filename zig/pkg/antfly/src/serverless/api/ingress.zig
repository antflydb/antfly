// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const admission = @import("../../common/workload_admission.zig");
const Owner = @import("../../common/workload_allocator.zig").Owner;

/// One accepted frontend request, retained by context and exported output.
/// The outstanding lease belongs to a stable account, never a server pointer.
pub const Scope = struct {
    owner: *Owner,
    outstanding: admission.MemoryAccount.OutstandingLease,
    refs: std.atomic.Value(usize) = .init(1),
    execution_started: bool = false,
    write_started: bool = false,
    write_completed: bool = false,
    previous_output: ?struct { ptr: *anyopaque, release: *const fn (*anyopaque) void } = null,

    pub fn create(backing: std.mem.Allocator, gate: *admission.Controller) !*Scope {
        const owner = try Owner.create(backing, gate);
        errdefer owner.release();
        var outstanding = try owner.account.acquireOutstanding();
        errdefer outstanding.release();
        const self = try owner.allocator().create(Scope);
        self.* = .{ .owner = owner, .outstanding = outstanding };
        return self;
    }

    pub fn retain(self: *Scope) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
    }

    pub fn release(self: *Scope) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const owner = self.owner;
        var outstanding = self.outstanding;
        std.debug.assert(self.previous_output == null);
        owner.allocator().destroy(self);
        outstanding.release();
        owner.release();
    }

    pub fn retainOpaque(raw: *anyopaque) void {
        const self: *Scope = @ptrCast(@alignCast(raw));
        self.retain();
    }

    pub fn releaseOpaque(raw: *anyopaque) void {
        const self: *Scope = @ptrCast(@alignCast(raw));
        self.release();
    }

    /// Exactly one transport response consumes this callback. Context cleanup
    /// retains its own reference and can occur before or after output drain.
    pub fn releaseOutput(raw: *anyopaque) void {
        const self: *Scope = @ptrCast(@alignCast(raw));
        const previous = self.previous_output;
        self.previous_output = null;
        if (previous) |value| value.release(value.ptr);
        self.release();
    }
};

test "workload admission serverless ingress scope retains count through output and owner teardown" {
    const alloc = std.testing.allocator;
    var gate = admission.Controller.initConfigured(1, .{ .max_retained_bytes = 8192 });
    var gate_live = true;
    defer if (gate_live) gate.deinitMemory();
    const scope = try Scope.create(alloc, &gate);
    scope.retain(); // context and output
    const bytes = try scope.owner.allocator().dupe(u8, "output");
    try std.testing.expectEqual(@as(usize, 1), gate.stats().in_flight);
    try std.testing.expectError(error.AdmissionFull, Scope.create(alloc, &gate));
    scope.release(); // context retires first
    try std.testing.expectEqual(@as(usize, 1), gate.stats().in_flight);
    gate.deinitMemory();
    gate_live = false;
    gate = undefined;
    scope.owner.allocator().free(bytes);
    Scope.releaseOutput(scope);
}
