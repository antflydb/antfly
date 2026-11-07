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
    owned_payload: ?@import("owned_coverage.zig").Complete = null,
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
    /// Optional synchronous construction into the caller's owned payload. On
    /// error the provider retires partial ownership; Session is never borrowed
    /// by the returned invocation beyond this call. Value providers remain valid.
    prepare_into: ?*const fn (*const Session, session_mod.ShapeInputs, std.mem.Allocator, ?Control, *PreparedInvocation) anyerror!void = null,
    prepare_into_with_cleanup: ?*const fn (*const Session, session_mod.ShapeInputs, std.mem.Allocator, ?Control, *PreparedInvocation, @import("cleanup_context.zig").Context) anyerror!void = null,
    /// Actual provider-owned cold work must run at an explicit Source phase,
    /// before Model/FIFO/numerical entry and with its real cleanup owner.
    needs_before_execution: ?*const fn (Session) anyerror!bool = null,
};

pub fn needsBeforeExecution(session: Session) !bool {
    const services = session.invocation_services orelse return false;
    if (!services.accepts(session)) return false;
    const query = services.needs_before_execution orelse return false;
    return query(session);
}

pub fn prepare(session: Session, shapes: session_mod.ShapeInputs, allocator: std.mem.Allocator, control: ?Control) !?PreparedInvocation {
    var result: ?PreparedInvocation = null;
    try prepareInto(&session, shapes, allocator, control, &result);
    return result;
}

/// Construct into fresh optional storage. The explicitly typed non-optional
/// value establishes a defined Some discriminator before taking its payload
/// address. The payload is unread until provider success; failed construction
/// clears the tag after the provider has retired its partial ownership.
pub fn prepareInto(session: *const Session, shapes: session_mod.ShapeInputs, allocator: std.mem.Allocator, control: ?Control, result: *?PreparedInvocation) !void {
    return prepareIntoWithCleanup(session, shapes, allocator, control, result, .{});
}

pub fn prepareIntoWithCleanup(session: *const Session, shapes: session_mod.ShapeInputs, allocator: std.mem.Allocator, control: ?Control, result: *?PreparedInvocation, cleanup: @import("cleanup_context.zig").Context) !void {
    result.* = null;
    const services = session.invocation_services orelse return;
    if (!services.accepts(session.*)) return;
    if (control) |active| try active.check();
    if (services.prepare_into_with_cleanup) |create| {
        result.* = @as(PreparedInvocation, undefined);
        errdefer result.* = null;
        try create(session, shapes, allocator, control, &result.*.?, cleanup);
    } else if (services.prepare_into) |create| {
        result.* = @as(PreparedInvocation, undefined);
        errdefer result.* = null;
        try create(session, shapes, allocator, control, &result.*.?);
    } else result.* = try services.prepare(session.*, shapes, allocator, control);
}

pub const PreparedInvocation = struct {
    /// Preserve public execution/admission policy while selecting physical code.
    published: Session,
    target: *anyopaque,
    /// Root owner is retained by preparation, even without manager resources.
    root_lifetime: *Session.ModelLifetime,
    /// Null when root and selected program are the same numerical owner.
    target_lifetime: ?*Session.ModelLifetime = null,
    physical_pool: ?*physical.Pool = null,
    frame_claim: ?physical.FrameClaim = null,
    region_claims: ?@import("region_claims.zig").Set = null,
    region_execution_profile: ?@import("region_execution_profile.zig").Claim = null,
    routes: ?*@import("prepared_routes.zig").PreparedRoutes = null,
    immutable_claim: ?@import("immutable_literals.zig").SharedLiteralStore.ImmutableClaim = null,
    requirement: Requirement = .{},
    /// Backend-owned inline scalar metadata (for example preparation events).
    /// Contains no borrowed pointer and needs no per-invocation allocation.
    backend_cookie: usize = 0,
    dispatch: *const fn (*PreparedInvocation, Inputs, Placement, std.mem.Allocator, ?Control, ?storage.Capability) anyerror!Result,
    reclaim_idle: ?*const fn (*PreparedInvocation, ?Control) anyerror!bool = null,
    reclaim_idle_with_cleanup: ?*const fn (*PreparedInvocation, ?Control, @import("cleanup_context.zig").Context) anyerror!bool = null,
    executed: bool = false,
    closed: bool = false,

    pub fn hasCompleteOwnedPayload(self: *const PreparedInvocation) bool {
        const declared = self.requirement.owned_payload orelse return false;
        const routes = self.routes orelse return false;
        const actual = routes.pool.completeOwnedPayload() orelse return false;
        return declared.epoch == routes.pool.selection.?.epoch and std.meta.eql(declared, actual);
    }

    pub fn run(self: *PreparedInvocation, inputs: Inputs, placement: Placement, allocator: std.mem.Allocator, control: ?Control, capability: ?storage.Capability) !Result {
        if (self.closed or self.executed) return error.PreparedInvocationAlreadyConsumed;
        if (control) |active| try active.check();
        self.executed = true;
        return @import("invocation_observation.zig").callInputs(self.published.invocation_diagnostics, inputs, self.dispatch, .{ self, inputs, placement, allocator, control, capability });
    }

    /// Rejection/fallback must release borrowed region pins BEFORE the warm
    /// FrameClaim. No provider callback or driver retirement runs under Pool
    /// mutex; immutable/root lifetime owners remain alive throughout cleanup.
    pub fn releasePhysicalPreparation(self: *PreparedInvocation) void {
        self.releasePhysicalPreparationWithCleanup(.{});
    }
    pub fn releasePhysicalPreparationWithCleanup(self: *PreparedInvocation, cleanup: @import("cleanup_context.zig").Context) void {
        if (self.region_claims) |*claims| claims.releaseUnsubmitted();
        self.region_claims = null;
        if (self.frame_claim) |*claim| claim.releaseWithCleanup(cleanup);
        self.frame_claim = null;
        if (self.immutable_claim) |*claim| claim.releaseWithCleanup(cleanup);
        self.immutable_claim = null;
        if (self.routes) |routes| routes.releaseWithCleanup(cleanup);
        self.routes = null;
        self.physical_pool = null;
        self.requirement.physical = .{};
        self.requirement.warm_credit = .{};
        self.requirement.owned_payload = null;
    }

    pub fn deinit(self: *PreparedInvocation) void {
        self.deinitWithCleanup(.{ .outside_execution = true });
    }
    pub fn deinitWithCleanup(self: *PreparedInvocation, cleanup: @import("cleanup_context.zig").Context) void {
        if (self.closed) return;
        self.closed = true;
        // Releasing an evicted program may enter a driver even after admission
        // rejection. Keep its model's cleanup protection and IO owner alive.
        var scope = self.published.beginClose();
        defer scope.deinit();
        self.releasePhysicalPreparationWithCleanup(cleanup);
        if (self.region_execution_profile) |*profile| profile.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.region_execution_profile = null;
        scope.deinit(); // SDK close protection ends before model/quota producers.
        if (self.target_lifetime) |owner| owner.releaseWithCleanup(cleanup);
        self.target_lifetime = null;
        self.root_lifetime.releaseWithCleanup(cleanup);
    }
};

test "prepared rejection releases borrowed region pins before returning the warm frame" {
    const region = @import("region_route.zig");
    const memory_mod = @import("../runtime/tier/memory.zig");
    const Fixture = struct {
        frame: *physical.Frame,
        releases: usize = 0,
        fn retain(_: *anyopaque) void {}
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Callback may acquire provider/metadata locks, so it must run
            // outside Pool mutex, while the warm claim still excludes reuse.
            std.debug.assert(self.frame.pool.mutex.tryLock());
            self.frame.pool.mutex.unlock();
            std.debug.assert(self.frame.claimed and self.frame.in_use);
            self.releases += 1;
        }
        fn validate(_: *anyopaque, _: region.Validate) !void {}
        fn transfer(_: *anyopaque, _: region.Submission) !void {
            return error.UnexpectedSubmission;
        }
        fn drain(_: *anyopaque) !void {
            return error.UnexpectedDrain;
        }
        const table = region.PayloadClaim.VTable{ .validate = validate, .transfer_to_submission = transfer, .drain = drain };
    };
    var controller = memory_mod.AdmissionController{};
    defer controller.deinit();
    var pool = physical.Pool{ .allocator = std.testing.allocator, .plan = undefined, .kernels = &.{}, .transfers = &.{}, .backends = &.{}, .workspace_allocations = &.{} };
    var claims: [1]?region.PayloadClaim = @splat(null);
    var frame = physical.Frame{ .pool = &pool, .storages = &.{}, .completions = &.{}, .region_claims = &claims, .claimed = true, .in_use = true, .claim_nonce = 17 };
    var fixture = Fixture{ .frame = &frame };
    var borrowed = try @import("region_claims.zig").Set.borrowed(@intFromPtr(&pool), 23, &claims);
    borrowed.claims[0] = .{ .key = 1, .resource = .{ .context = &fixture, .retain_fn = Fixture.retain, .release_fn = Fixture.release }, .vtable = &Fixture.table };
    // This focused cancellation method touches no session/model callbacks.
    var prepared = PreparedInvocation{ .published = undefined, .target = undefined, .root_lifetime = undefined, .dispatch = undefined, .frame_claim = .{ .frame = &frame, .identity = .{ .controller = &controller, .backend_class = .cpu }, .credit = .{}, .nonce = 17 }, .region_claims = borrowed };
    prepared.releasePhysicalPreparation();
    prepared.releasePhysicalPreparation();
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
    try std.testing.expect(!frame.claimed and !frame.in_use);
    try std.testing.expect(claims[0] == null and prepared.region_claims == null and prepared.frame_claim == null);
}
