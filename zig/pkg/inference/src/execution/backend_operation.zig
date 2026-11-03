// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit numerical-operation exclusion. A lifetime/provider cache lease
//! cannot stand in for this scope when several sessions share one backend.
//! The backend owns exclusion, command-buffer completion and model lifetime;
//! this adapter preserves the original numerical pointer and callback table.
const kernel = @import("kernel_route.zig");

pub const Mode = enum { metadata, legacy_unknown_frame, managed };

pub fn allowsMode(held: Mode, requested: Mode) bool {
    // Metadata is a distinct read capability (Metal also owns its index
    // lock). Numerical or unknown-frame exclusion never upgrades into it.
    if (requested == .metadata) return held == .metadata;
    return held == .managed or held == requested;
}

pub const Request = struct {
    implementation: kernel.Implementation,
    program: ?*const @import("ml").graph.Graph,
    mode: Mode,
};

/// Cold, explicitly bound outer cleanup boundary. No current-task lookup.
pub const CleanupEpochServices = struct {
    bind: *const fn (*anyopaque, u64, *const @import("retirement_sink.zig").Epoch) anyerror!void,
    retain: *const fn (*anyopaque, u64) anyerror!?@import("retirement_sink.zig").Epoch,
};

pub const Lease = struct {
    context: *anyopaque,
    nonce: u64,
    mode: Mode,
    implementation: kernel.Implementation,
    phase: enum { prepared, active, submitted, released } = .prepared,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Validate live nonce/provider ownership. A borrowed nested scope may
        /// use another graph; frozen region profiles prove their own graph.
        validate: *const fn (*anyopaque, u64, Request) anyerror!void,
        validate_active: *const fn (*anyopaque, u64, Request) anyerror!void,
        /// Validate the live nonce/provider/request before the first import or
        /// SDK call, then obtain actual provider
        /// exclusion and establishes declared policy; an early busy result
        /// cannot have encoded numerical work. Errors after commitment must
        /// remain drainable through the same nonce.
        begin: *const fn (*anyopaque, u64, Request) anyerror!void,
        /// Confirm the real submission boundary. Encoded-only host state may
        /// release exclusion immediately. GPU-visible mutable resources/modes
        /// retain independent actual-CB ownership until handlers finish.
        /// Never attach only to a fresh post-flush command buffer. Success
        /// consumes the host metadata reference; failure leaves it drainable.
        finish: *const fn (*anyopaque, u64) anyerror!void,
        /// Prove terminal cleanup after uncertain work. Failure propagates to
        /// the existing supervisor; it never frees possibly-live addresses.
        drain: *const fn (*anyopaque, u64) anyerror!void,
        /// Retire metadata references only. Any actual-CB ticket is separately
        /// retained and the provider's checked teardown waits for its handlers.
        release: *const fn (*anyopaque, u64) void,
        cleanup_epoch: ?*const CleanupEpochServices = null,
    };

    pub fn borrow(self: *const Lease) ?Borrowed {
        if (self.phase != .active) return null;
        return .{ .context = self.context, .nonce = self.nonce, .mode = self.mode, .vtable = self.vtable };
    }
    pub fn bindCleanupEpoch(self: *const Lease, epoch: *const @import("retirement_sink.zig").Epoch) !bool {
        if (self.phase != .active or self.nonce == 0) return error.BackendOperationInactive;
        if (self.mode == .metadata) return false;
        const bridge = self.vtable.cleanup_epoch orelse return false;
        try bridge.bind(self.context, self.nonce, epoch);
        return true;
    }
    pub fn validate(self: *const Lease, request: Request) !void {
        if (self.phase == .released or self.nonce == 0) return error.BackendOperationConsumed;
        if (!allowsMode(self.mode, request.mode)) return error.BackendOperationChanged;
        // Backend validation proves the canonical table/context and actual
        // provider. A cached runtime may have another original ComputeBackend
        // on the SAME provider; no pointer-only casts or copied tables pass.
        try self.vtable.validate(self.context, self.nonce, request);
    }
    pub fn begin(self: *Lease, request: Request) !void {
        if (self.phase != .prepared or self.nonce == 0) return error.BackendOperationConsumed;
        if (!allowsMode(self.mode, request.mode)) return error.BackendOperationChanged;
        // Even an error may have installed a callback or changed driver state.
        self.phase = .active;
        self.vtable.begin(self.context, self.nonce, request) catch |err| {
            self.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        };
    }
    pub fn finish(self: *Lease) !void {
        if (self.phase != .active) return error.BackendOperationConsumed;
        self.vtable.finish(self.context, self.nonce) catch |err| {
            // Returning this error authorizes caller output errdefers. Retire
            // uncertain work before any such output can lose its backing.
            self.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        };
        self.phase = .released;
    }
    pub fn deinitChecked(self: *Lease) !void {
        if (self.phase == .released) return;
        if (self.phase == .active) {
            try self.vtable.drain(self.context, self.nonce);
            self.phase = .submitted;
        }
        self.release();
    }
    fn release(self: *Lease) void {
        std.debug.assert(self.phase != .active);
        if (self.phase == .released) return;
        self.phase = .released;
        self.vtable.release(self.context, self.nonce);
    }
};

/// Borrowed by value: no caller stack/request pointer can escape in a backend
/// or ModelOutput. The stable provider record validates nonce and active scope.
/// This is not an independent model pin; the numerical/backend owner remains
/// required, exactly as for the original ComputeBackend callback table.
pub const Borrowed = struct {
    context: *anyopaque,
    nonce: u64,
    mode: Mode,
    vtable: *const Lease.VTable,
    pub fn retainCleanupEpoch(self: Borrowed) !?@import("retirement_sink.zig").Epoch {
        if (self.nonce == 0) return error.BackendOperationConsumed;
        if (self.mode == .metadata) return null;
        const bridge = self.vtable.cleanup_epoch orelse return null;
        return bridge.retain(self.context, self.nonce);
    }
    pub fn validate(self: Borrowed, request: Request) !void {
        if (self.nonce == 0 or !allowsMode(self.mode, request.mode)) return error.BackendOperationChanged;
        try self.vtable.validate_active(self.context, self.nonce, request);
    }
};

pub const Services = struct {
    /// Copied numerical tables must not inherit an unchecked context cast.
    accepts: *const fn (*const @import("../ops/ops.zig").ComputeBackend.VTable) bool,
    /// Metadata-only preparation. No SDK work/quota mutation; the actual
    /// operation is established by begin after invocation admission.
    prepare: ?*const fn (*anyopaque, Request) anyerror!?Lease = null,
    /// Caller-owned cold/hot preparation. False leaves output uninitialized;
    /// no optional Lease aggregate needs construction on an ownerless path.
    prepare_into: ?*const fn (*anyopaque, Request, *Lease) anyerror!bool = null,
    /// Only explicit per-request engine owners expose their own scope. A
    /// shared backend MUST NOT lend whichever provider operation is active.
    currentToken: ?*const fn (*anyopaque) ?Borrowed = null,
    /// Cold explicit binding of the concrete target's OWN engine Lease.
    /// Borrowed/raw targets return false; ambient provider lookup is forbidden.
    bindCleanupEpoch: ?*const fn (*anyopaque, *const @import("retirement_sink.zig").Epoch) anyerror!bool = null,
    /// Move only on success, into the target's actual independently owned
    /// provider lifetime. A borrowed engine cannot consume a shorter boundary.
    retainCleanupGuard: ?*const fn (*anyopaque, *@import("retirement_finish_holder.zig").Holder) anyerror!bool = null,
    retainCleanupFinish: ?*const fn (*anyopaque) ?*@import("retirement_finish_holder.zig").Holder = null,
};

pub const Scope = struct {
    owned: ?Lease = null,
    borrowed: ?Borrowed = null,
    /// A nested borrow may confirm only its already-bound actual owner.
    /// It cannot replace the outer operation with a newly invented boundary.
    pub fn bindCleanupEpoch(self: *const Scope, epoch: *const @import("retirement_sink.zig").Epoch) !bool {
        if (self.owned) |*owned| return owned.bindCleanupEpoch(epoch);
        const borrowed = self.borrowed orelse return false;
        var retained = (try borrowed.retainCleanupEpoch()) orelse return false;
        defer retained.release();
        if (retained.record != epoch.record or retained.nonce != epoch.nonce) return error.BackendOperationCleanupChanged;
        return true;
    }
    pub fn retainCleanupEpoch(self: *const Scope) !?@import("retirement_sink.zig").Epoch {
        const borrowed = self.token() orelse return null;
        return borrowed.retainCleanupEpoch();
    }
    pub fn token(self: *const Scope) ?Borrowed {
        if (self.borrowed) |value| return value;
        return if (self.owned) |*value| value.borrow() else null;
    }
    /// Move a newly owned operation into an actual backend lifetime. Borrowed
    /// outer operations never transfer ownership; their complete source holder
    /// must remain pinned by the consumer.
    pub fn takeOwned(self: *Scope) ?Lease {
        const value = self.owned;
        self.owned = null;
        return value;
    }
    pub fn finish(self: *Scope) !void {
        if (self.owned) |*value| try value.finish();
    }
    pub fn deinitChecked(self: *Scope) !void {
        if (self.owned) |*value| try value.deinitChecked();
    }
};

/// Imported entry owns the scope before importing inputs. Its synchronous
/// Runtime call borrows the exact token; a direct Runtime entry owns its own.
/// Region-profile begin likewise owns a lease and lends that same token.
pub fn enter(backend: @import("../ops/ops.zig").ComputeBackend, program: ?*const @import("ml").graph.Graph, borrowed: ?Borrowed, mode: Mode) !Scope {
    var result: Scope = undefined;
    try enterInto(&backend, program, borrowed, mode, &result);
    return result;
}

/// The same protocol writes caller-owned scope metadata at hot boundaries.
/// Numerical implementation/context ownership and metadata mode are unchanged.
pub fn enterInto(backend: *const @import("../ops/ops.zig").ComputeBackend, program: ?*const @import("ml").graph.Graph, borrowed: ?Borrowed, mode: Mode, output: *Scope) !void {
    output.* = .{};
    const request = Request{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = program, .mode = mode };
    if (borrowed) |value| {
        const services = backend.vtable.operation_services orelse return error.BackendOperationChanged;
        if (!services.accepts(backend.vtable)) return error.BackendOperationChanged;
        try value.validate(request);
        output.borrowed = value;
        return;
    }
    const services = backend.vtable.operation_services orelse return;
    if (!services.accepts(backend.vtable)) return;
    var prepared: Lease = undefined;
    if (services.prepare_into) |prepare| {
        if (!try prepare(backend.ptr, request, &prepared)) return;
    } else {
        const prepare = services.prepare orelse return error.InvalidBackendOperationServices;
        prepared = (try prepare(backend.ptr, request)) orelse return;
    }
    output.owned = prepared;
    errdefer output.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try output.owned.?.begin(request);
}

const std = @import("std");

/// Whole-model/executor numerical callbacks share the same provider protocol
/// without replacing their ptr/table or numerical dispatch implementation.
pub inline fn call(backend: ?@import("../ops/ops.zig").ComputeBackend, borrowed: ?Borrowed, callback: anytype, args: anytype) anyerror!@typeInfo(@TypeOf(@call(.auto, callback, args))).error_union.payload {
    return callWithMode(backend, borrowed, .managed, callback, args);
}

/// Existing operation callbacks may be encoded into an explicitly external
/// caller's frame. They retain full opaque accounting and their original
/// caller-owned submission boundary; this does not authorize frozen regions.
pub inline fn callOpaque(backend: ?@import("../ops/ops.zig").ComputeBackend, borrowed: ?Borrowed, callback: anytype, args: anytype) anyerror!@typeInfo(@TypeOf(@call(.auto, callback, args))).error_union.payload {
    return callWithMode(backend, borrowed, .legacy_unknown_frame, callback, args);
}

inline fn callWithMode(backend: ?@import("../ops/ops.zig").ComputeBackend, borrowed: ?Borrowed, mode: Mode, callback: anytype, args: anytype) anyerror!@typeInfo(@TypeOf(@call(.auto, callback, args))).error_union.payload {
    const selected = backend orelse return @call(.auto, callback, args);
    var scope: Scope = undefined;
    try enterInto(&selected, null, borrowed orelse selected.operationToken(), mode, &scope);
    var succeeded = false;
    defer {
        if (succeeded) scope.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker() else scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    const result = try @call(.auto, callback, args);
    succeeded = true;
    return result;
}

test "operation metadata authority is explicit across every held mode" {
    inline for (.{ Mode.metadata, Mode.legacy_unknown_frame, Mode.managed }) |held| {
        try std.testing.expectEqual(held == .metadata, allowsMode(held, .metadata));
    }
    try std.testing.expect(!allowsMode(.metadata, .managed));
    try std.testing.expect(!allowsMode(.metadata, .legacy_unknown_frame));
    try std.testing.expect(!allowsMode(.legacy_unknown_frame, .managed));
    try std.testing.expect(allowsMode(.managed, .legacy_unknown_frame));
}

test "operation finish failure drains actual Native output before scope unwind" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const native = @import("../ops/native_compute.zig");
    const ops = @import("../ops/ops.zig");
    const Probe = struct {
        compute: native.NativeCompute,
        table: *const ops.ComputeBackend.VTable = undefined,
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
            self.table.freeTensor(raw, value);
        }
        fn validate(_: *anyopaque, _: u64, _: Request) !void {}
        fn begin(raw: *anyopaque, nonce: u64, request: Request) !void {
            try validate(raw, nonce, request);
        }
        fn finish(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.finish_started = true;
            return error.InjectedOperationFinishFailure;
        }
        fn drain(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.drains += 1;
            self.drained = true;
        }
        fn release(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
        const hooks = Lease.VTable{ .validate = validate, .validate_active = validate, .begin = begin, .finish = finish, .drain = drain, .release = release };
        fn produce(self: *@This(), backend: *const ops.ComputeBackend, input: ops.CT) !ops.CT {
            const request = Request{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = null, .mode = .managed };
            var scope = Scope{ .owned = .{ .context = self, .nonce = 1, .mode = .managed, .implementation = request.implementation, .vtable = &hooks } };
            try scope.owned.?.begin(request);
            errdefer scope.deinitChecked() catch @panic("operation fixture drain failed");
            const output = try backend.primNegate(input);
            errdefer backend.free(output);
            try scope.finish();
            return output;
        }
    };
    const a = std.testing.allocator;
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.resident_weights.deinit(a);
    defer weights.lazy_weights.deinit(a);
    var probe = Probe{ .compute = native.NativeCompute.init(a, &weights, null) };
    defer probe.compute.deinit();
    var backend = probe.compute.computeBackend();
    probe.table = backend.vtable;
    var copied = backend.vtable.*;
    copied.freeTensor = Probe.free;
    backend.vtable = &copied;
    const input = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer backend.free(input);
    try std.testing.expectError(error.InjectedOperationFinishFailure, probe.produce(&backend, input));
    try std.testing.expect(probe.finish_started and probe.drained);
    try std.testing.expect(!probe.freed_before_drain);
    try std.testing.expect(probe.post_finish_frees > 0);
    try std.testing.expectEqual(@as(usize, 1), probe.drains);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
}

test "caller-owned operation preparation declines without constructing a lease or using compatibility callback" {
    const ops = @import("../ops/ops.zig");
    const Fixture = struct {
        calls: usize = 0,
        fn accepts(table: *const ops.ComputeBackend.VTable) bool {
            return table.operation_services == &services;
        }
        fn legacy(_: *anyopaque, _: Request) !?Lease {
            return error.UnexpectedCompatibilityPrepare;
        }
        fn prepareInto(raw: *anyopaque, _: Request, _: *Lease) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return false;
        }
        const services = Services{ .accepts = accepts, .prepare = legacy, .prepare_into = prepareInto };
    };
    var fixture = Fixture{};
    // Only the declared services field is addressable by this metadata-only
    // denial. No backend method or undefined numerical field may be read.
    var table: ops.ComputeBackend.VTable = undefined;
    table.operation_services = &Fixture.services;
    const backend = ops.ComputeBackend{ .ptr = &fixture, .vtable = &table };
    var output: Scope = undefined;
    try enterInto(&backend, null, null, .managed, &output);
    try std.testing.expect(output.token() == null);
    try output.finish();
    try output.deinitChecked();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
}

test "advertised operation services without preparation fail closed" {
    const ops = @import("../ops/ops.zig");
    const Fixture = struct {
        fn accepts(_: *const ops.ComputeBackend.VTable) bool {
            return true;
        }
        const services = Services{ .accepts = accepts };
    };
    var marker: u8 = 0;
    var table: ops.ComputeBackend.VTable = undefined;
    table.operation_services = &Fixture.services;
    const backend = ops.ComputeBackend{ .ptr = &marker, .vtable = &table };
    var output: Scope = undefined;
    try std.testing.expectError(error.InvalidBackendOperationServices, enterInto(&backend, null, null, .managed, &output));
    try std.testing.expect(output.token() == null);
}
