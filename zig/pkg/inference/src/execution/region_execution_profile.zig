// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Operation-owned execution proof, held from preparation through dispatch.
//! A frontend name or last request's mutable provider state is not this proof.
const std = @import("std");
const kernel = @import("kernel_route.zig");
const region = @import("region_route.zig");

pub const Request = struct {
    operation: enum { graph_execution },
    implementation: kernel.Implementation,
    program: *const @import("ml").graph.Graph,
};

pub const Claim = struct {
    context: *anyopaque,
    implementation: kernel.Implementation,
    program: *const @import("ml").graph.Graph,
    profile: region.ExecutionProfile,
    policy_key: u64,
    held: bool = true,
    phase: enum { prepared, active, submitted } = .prepared,
    /// Composed backend operation; begin activates it before numerical work.
    /// The common Runtime/Imported entry borrows, never reacquires it.
    operation_lease: ?@import("backend_operation.zig").Borrowed = null,
    vtable: *const VTable,
    pub const VTable = struct {
        /// Metadata-only. Owner must stabilize this operation's profile through
        /// execution, including excluding incompatible foreign/capture scopes.
        validate: *const fn (*anyopaque, Request) anyerror!void,
        /// Establish provider-owned scope BEFORE first numerical work. Every
        /// touched real command buffer must inherit an independent owner pin.
        begin: *const fn (*anyopaque) anyerror!void,
        /// End the explicit scope and prove actual submission/completion owns
        /// independent pins. This never attaches only to a post-flush fresh CB.
        finish: *const fn (*anyopaque) anyerror!void,
        /// Explicit terminal proof after partial/uncertain numerical failure.
        drain: *const fn (*anyopaque) anyerror!void,
        /// Metadata/reference retirement. Before begin this performs no SDK
        /// work; after finish CB handlers retain independent owner references.
        release: *const fn (*anyopaque) void,
    };
    pub fn validate(self: *const Claim, request: Request) !void {
        if (!self.held) return error.RegionExecutionProfileConsumed;
        if (self.profile == .unknown or self.policy_key == 0 or self.program != request.program or self.implementation.context != request.implementation.context or self.implementation.table != request.implementation.table) return error.RegionExecutionProfileChanged;
        try self.vtable.validate(self.context, request);
    }
    /// Synchronous nested Runtime use may borrow a scope established by its
    /// Imported invocation owner; it does not own/end that outer scope.
    pub fn ensureScope(self: *Claim, request: Request) !bool {
        try self.validate(request);
        if (self.phase == .active) return false;
        if (self.phase != .prepared) return error.RegionExecutionProfileConsumed;
        self.phase = .active; // a failed begin may already have touched a CB
        self.vtable.begin(self.context) catch |err| {
            // begin may have registered/submitted an earlier chunk before
            // reporting failure; its owner supplies actual terminal proof.
            self.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        };
        return true;
    }
    pub fn finishSubmission(self: *Claim) !void {
        if (!self.held or self.phase != .active) return error.RegionExecutionProfileConsumed;
        self.vtable.finish(self.context) catch |err| {
            // Every caller must see a terminal profile before unwinding its
            // numerical outputs. A failed drain cannot safely return to an
            // output errdefer while GPU-visible addresses may remain live.
            self.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        };
        self.phase = .submitted;
        self.release();
    }
    pub fn deinitChecked(self: *Claim) !void {
        if (!self.held) return;
        if (self.phase == .active) {
            try self.vtable.drain(self.context);
            self.phase = .submitted;
        }
        self.release();
    }
    pub fn release(self: *Claim) void {
        if (!self.held) return;
        std.debug.assert(self.phase != .active);
        self.held = false;
        self.vtable.release(self.context);
    }
};

pub fn acquire(backend: @import("../ops/ops.zig").ComputeBackend, program: *const @import("ml").graph.Graph) !?Claim {
    const functions = backend.vtable.kernels orelse return null;
    if (!functions.accepts(backend.vtable)) return null;
    const callback = functions.claimRegionExecutionProfile orelse return null;
    const request = Request{ .operation = .graph_execution, .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = program };
    var claim = (try callback(backend.ptr, request)) orelse return null;
    errdefer claim.release();
    try claim.validate(request);
    const query = functions.regionPolicyKey orelse return error.RegionExecutionProfileChanged;
    if ((try query(backend.ptr)) != claim.policy_key) return error.RegionExecutionProfileChanged;
    return claim;
}

test "execution profile proof rejects foreign program backend and consumed ownership" {
    const ml = @import("ml");
    const Fixture = struct {
        released: usize = 0,
        valid: bool = true,
        fn validate(raw: *anyopaque, _: Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.valid) return error.RegionExecutionProfileChanged;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.released += 1;
        }
        fn scope(_: *anyopaque) !void {}
        const table = Claim.VTable{ .validate = validate, .release = release, .begin = scope, .finish = scope, .drain = scope };
    };
    var a = ml.graph.Graph.init(std.testing.allocator);
    defer a.deinit();
    var b = ml.graph.Graph.init(std.testing.allocator);
    defer b.deinit();
    var fixture = Fixture{};
    var identity: u8 = 0;
    var claim = Claim{ .context = &fixture, .implementation = .{ .context = &fixture, .table = &identity }, .program = &a, .profile = .ordinary_frame, .policy_key = 1, .vtable = &Fixture.table };
    const request = Request{ .operation = .graph_execution, .implementation = claim.implementation, .program = &a };
    try claim.validate(request);
    var wrong = request;
    wrong.program = &b;
    try std.testing.expectError(error.RegionExecutionProfileChanged, claim.validate(wrong));
    wrong = request;
    wrong.implementation.context = &identity;
    try std.testing.expectError(error.RegionExecutionProfileChanged, claim.validate(wrong));
    fixture.valid = false;
    try std.testing.expectError(error.RegionExecutionProfileChanged, claim.validate(request));
    claim.release();
    claim.release();
    try std.testing.expectEqual(@as(usize, 1), fixture.released);
    try std.testing.expectError(error.RegionExecutionProfileConsumed, claim.validate(request));
}

test "execution profile scope hands stabilization to actual completion and drains partial begin" {
    const ml = @import("ml");
    const Fixture = struct {
        refs: usize = 1,
        tickets: usize = 0,
        scopes: usize = 0,
        drains: usize = 0,
        fail_begin: bool = false,
        fn validate(_: *anyopaque, _: Request) !void {}
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.scopes += 1;
            self.tickets += 1;
            self.refs += 1;
            if (self.fail_begin) return error.TestProfileBeginFailed;
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.scopes != 1 or self.tickets == 0) return error.MissingProfileSubmissionProof;
            self.scopes = 0;
        }
        fn complete(self: *@This()) void {
            self.refs -= self.tickets;
            self.tickets = 0;
        }
        fn drain(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.drains += 1;
            self.scopes = 0;
            self.complete();
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        const table = Claim.VTable{ .validate = validate, .begin = begin, .finish = finish, .drain = drain, .release = release };
        fn claim(self: *@This(), graph: *const ml.graph.Graph) Claim {
            return .{ .context = self, .implementation = .{ .context = self, .table = &table }, .program = graph, .profile = .ordinary_frame, .policy_key = 1, .vtable = &table };
        }
    };
    var graph = ml.graph.Graph.init(std.testing.allocator);
    defer graph.deinit();
    var fixture = Fixture{};
    var claim = fixture.claim(&graph);
    const request = Request{ .operation = .graph_execution, .implementation = claim.implementation, .program = &graph };
    try std.testing.expect(try claim.ensureScope(request));
    try std.testing.expect(!try claim.ensureScope(request)); // explicit nested Runtime borrow
    try claim.finishSubmission();
    try claim.deinitChecked();
    try std.testing.expectEqual(@as(usize, 1), fixture.refs);
    try std.testing.expectEqual(@as(usize, 0), fixture.drains);
    fixture.complete();
    try std.testing.expectEqual(@as(usize, 0), fixture.refs);
    fixture = .{};
    claim = fixture.claim(&graph);
    try claim.deinitChecked(); // rejection never begins/submits/drains
    try std.testing.expectEqual(@as(usize, 0), fixture.refs);
    try std.testing.expectEqual(@as(usize, 0), fixture.drains);
    fixture = .{ .fail_begin = true };
    claim = fixture.claim(&graph);
    try std.testing.expectError(error.TestProfileBeginFailed, claim.ensureScope(request));
    try std.testing.expectEqual(@as(usize, 1), fixture.drains);
    try std.testing.expectEqual(@as(usize, 0), fixture.refs);
}

test "Runtime profile finish failure drains the actual numerical output before destruction" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const native = @import("../ops/native_compute.zig");
    const ops = @import("../ops/ops.zig");
    const ml = @import("ml");
    const runtime_mod = @import("../graph/runtime.zig");
    const Probe = struct {
        compute: native.NativeCompute,
        base_table: *const ops.ComputeBackend.VTable = undefined,
        finish_started: bool = false,
        drained: bool = false,
        freed_before_drain: bool = false,
        post_finish_frees: usize = 0,
        drains: usize = 0,
        releases: usize = 0,
        fn free(raw: *anyopaque, value: ops.CT) void {
            const compute: *native.NativeCompute = @ptrCast(@alignCast(raw));
            const self: *@This() = @fieldParentPtr("compute", compute);
            if (self.finish_started) {
                self.post_finish_frees += 1;
                if (!self.drained) self.freed_before_drain = true;
            }
            self.base_table.freeTensor(raw, value);
        }
        fn validate(_: *anyopaque, _: Request) !void {}
        fn begin(_: *anyopaque) !void {}
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.finish_started = true;
            // The real Native graph has produced its owned output. Model an
            // uncertain completion report without submitting undrained GPU
            // work in this bounded fault-injection fixture.
            return error.InjectedProfileFinishFailure;
        }
        fn drain(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.drains += 1;
            self.drained = true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
        const table = Claim.VTable{ .validate = validate, .begin = begin, .finish = finish, .drain = drain, .release = release };
    };
    const a = std.testing.allocator;
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.resident_weights.deinit(a);
    defer weights.lazy_weights.deinit(a);
    var probe = Probe{ .compute = native.NativeCompute.init(a, &weights, null) };
    defer probe.compute.deinit();
    var cb = probe.compute.computeBackend();
    probe.base_table = cb.vtable;
    var copied = cb.vtable.*;
    copied.freeTensor = Probe.free;
    cb.vtable = &copied;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const y = try builder.neg(x);
    try graph.markOutput(y);
    const value = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer cb.free(value);
    var runtime = try runtime_mod.Runtime.init(a, &graph, &cb, .interpreter);
    defer runtime.deinit();
    var claim = Claim{ .context = &probe, .implementation = .{ .context = cb.ptr, .table = cb.vtable }, .program = &graph, .profile = .ordinary_frame, .policy_key = 1, .vtable = &Probe.table };
    defer claim.deinitChecked() catch @panic("profile ordering fixture cleanup failed");
    const inputs = [_]@import("../graph/interpreter.zig").RuntimeInput{.{ .node_id = x, .value = value }};
    var result: runtime_mod.Result = undefined;
    try std.testing.expectError(error.InjectedProfileFinishFailure, runtime.executeInto(a, &graph, &.{ .runtime_inputs = &inputs, .region_execution_profile = &claim }, &result));
    try std.testing.expect(probe.finish_started and probe.drained);
    try std.testing.expect(!probe.freed_before_drain);
    try std.testing.expect(probe.post_finish_frees > 0);
    try std.testing.expectEqual(@as(usize, 1), probe.drains);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expect(!claim.held);
}
