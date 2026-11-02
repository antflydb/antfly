// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One optional numerical-dispatch observation across public session routes.
//! Preparation/admission latency and independently submitting worker threads
//! are outside this scope. Disabled dispatch starts no clocks or owner registry.
const std = @import("std");
const diagnostics_mod = @import("diagnostics.zig");
const ops = @import("../ops/ops.zig");
const registry_mod = @import("allocation_registry.zig");
const observer = @import("allocation_observer.zig");
const observation = @import("runtime_observation.zig");
const build_options = @import("build_options");
const metal = if (build_options.enable_metal) @import("../backends/metal_telemetry.zig") else struct {};

/// Borrowed model-owned capabilities, valid until the retaining Session closes.
/// A stable backend must outlive every synchronous invocation. Temporary native
/// computes register their allocator State through the canonical factory.
pub const Binding = struct {
    diagnostics: *diagnostics_mod.Diagnostics,
    capture_programs: bool = false,
    model_allocator: ?std.mem.Allocator = null,
    stable_backend: ?ops.ComputeBackend = null,
};

pub inline fn call(binding: ?Binding, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const capability = binding orelse return @call(.auto, callback, args);
    if (!capability.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observed(capability, callback, args);
}

noinline fn observed(binding: Binding, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    var scope: Invocation = undefined;
    scope.begin(binding);
    defer scope.finish();
    const result = @call(.auto, callback, args) catch |err| {
        scope.error_name = @errorName(err);
        return err;
    };
    if (comptime @typeInfo(@TypeOf(result)) == .optional) {
        if (result == null) scope.unsupported = true;
    }
    return result;
}

const Invocation = struct {
    binding: Binding,
    capture: diagnostics_mod.Capture,
    owners: registry_mod.Registry,
    scope: observation.Scope,
    before: ?ops.RuntimeTelemetry = null,
    process_before: ?@import("runtime_telemetry.zig").Process = null,
    error_name: ?[]const u8 = null,
    unsupported: bool = false,

    fn begin(self: *Invocation, binding: Binding) void {
        self.* = .{ .binding = binding, .capture = undefined, .owners = undefined, .scope = observation.Scope.init() };
        self.capture.beginPrograms(binding.diagnostics, binding.capture_programs);
        self.owners.begin(registry_mod.metadataAllocator());
        if (binding.model_allocator) |allocator| {
            if (observer.State.forAllocator(allocator)) |owner| registry_mod.bind(.model, owner) else registry_mod.missing(.model);
        }
        if (binding.stable_backend) |backend| {
            backend.enableRuntimeObservation();
            self.before = backend.debugTimingSnapshot().runtime;
        }
        if (comptime build_options.enable_metal) self.process_before = metal.runtimeProcessSnapshot();
    }

    fn finish(self: *Invocation) void {
        // Restore capture before publishing so this invocation cannot capture
        // its own final snapshot. Parent observations still see child work.
        const captured = self.capture.finish();
        var snapshot = captured orelse diagnostics_mod.Snapshot{};
        if (captured == null) if (self.binding.stable_backend) |backend| {
            const after = backend.debugTimingSnapshot();
            snapshot.runtime = if (self.before) |before| after.runtime.since(before) else .{};
            snapshot.provider = if (after.native_quant_null) null else after.provider;
            snapshot.context_backend = backend.kind();
            snapshot.context_runtime_scope = .model_stable_backend_context;
        };
        snapshot.execution_scope = if (snapshot.execution != null) .last_published_program else .unavailable;
        snapshot.runtime.submission = self.scope.snapshot();
        snapshot.runtime.allocation_owners = self.owners.finish();
        if (comptime build_options.enable_metal) {
            if (self.process_before) |before| snapshot.runtime.process = metal.runtimeProcessSnapshot().since(before);
        }
        snapshot.outcome = if (self.error_name != null) .failed else if (self.unsupported) .unsupported else .success;
        snapshot.error_name = if (self.error_name) |name| diagnostics_mod.Text.init(name) else null;
        snapshot.scope = .session_numerical_dispatch;
        self.scope.deinit();
        self.binding.diagnostics.publish(snapshot);
    }
};

test "uniform invocation observations retain unavailable legacy program totals and unwind errors" {
    const Probe = struct {
        fn success() anyerror!usize {
            try std.testing.expect(registry_mod.enabled());
            observation.nativeAssignedDispatch();
            return 7;
        }
        fn fail() anyerror!usize {
            observation.nativeAssignedDispatch();
            return error.ObservedInvocationFailed;
        }
        fn unsupported() anyerror!?usize {
            return null;
        }
    };
    var diagnostics = diagnostics_mod.Diagnostics{};
    const binding = Binding{ .diagnostics = &diagnostics };
    diagnostics.configure(true);
    try std.testing.expectEqual(@as(usize, 7), try call(binding, Probe.success, .{}));
    var measured = diagnostics.snapshot().?;
    try std.testing.expect(measured.execution == null and measured.provider == null);
    try std.testing.expectEqual(@as(u64, 1), measured.runtime.submission.?.native_assigned_dispatches);
    try std.testing.expect(measured.scope == .session_numerical_dispatch and measured.outcome == .success);
    try std.testing.expect(!registry_mod.enabled());
    try std.testing.expectError(error.ObservedInvocationFailed, call(binding, Probe.fail, .{}));
    measured = diagnostics.snapshot().?;
    try std.testing.expectEqualStrings("ObservedInvocationFailed", measured.error_name.?.view());
    try std.testing.expect(measured.outcome == .failed);
    try std.testing.expect(!registry_mod.enabled());
    try std.testing.expect((try call(binding, Probe.unsupported, .{})) == null);
    try std.testing.expect(diagnostics.snapshot().?.outcome == .unsupported);
}

test "uniform invocation diagnostics preserve exact matching program publications in nested scopes" {
    const Probe = struct {
        fn inner(owner: *diagnostics_mod.Diagnostics) anyerror!void {
            observation.nativeAssignedDispatch();
            owner.publish(.{ .execution = .{ .assigned_kernel_dispatches = 3 } });
        }
        fn outer(owner: *diagnostics_mod.Diagnostics, unrelated: *diagnostics_mod.Diagnostics) anyerror!void {
            try call(Binding{ .diagnostics = owner }, inner, .{owner});
            // The wrong owner's publication must never replace this call's
            // immutable captured program snapshot.
            unrelated.publish(.{ .execution = .{ .assigned_kernel_dispatches = 99 } });
            observation.nativeAssignedDispatch();
        }
    };
    var owner = diagnostics_mod.Diagnostics{};
    owner.configure(true);
    var other = diagnostics_mod.Diagnostics{};
    other.configure(true);
    try call(Binding{ .diagnostics = &owner }, Probe.outer, .{ &owner, &other });
    const measured = owner.snapshot().?;
    try std.testing.expectEqual(@as(u64, 3), measured.execution.?.assigned_kernel_dispatches);
    try std.testing.expectEqual(@as(u64, 2), measured.runtime.submission.?.native_assigned_dispatches);
    try std.testing.expect(!registry_mod.enabled());
}

test "disabled invocation diagnostics keep the raw callback and allocator path" {
    const Probe = struct {
        fn run() anyerror!void {
            try std.testing.expect(!registry_mod.enabled());
        }
    };
    var diagnostics = diagnostics_mod.Diagnostics{};
    try call(Binding{ .diagnostics = &diagnostics }, Probe.run, .{});
    try std.testing.expect(diagnostics.snapshot() == null);
}

test "architecture observation collects real child program events without enabling child owners globally" {
    const Probe = struct {
        fn run(program: *diagnostics_mod.Diagnostics, unrelated: *diagnostics_mod.Diagnostics) anyerror!void {
            try std.testing.expect(program.isEnabled());
            program.publishProgram(.{ .execution = .{ .assigned_kernel_dispatches = 7 } });
            program.recordSpecializations(.{ .hits = 3 });
            unrelated.publish(.{ .execution = .{ .assigned_kernel_dispatches = 99 } });
        }
    };
    var owner = diagnostics_mod.Diagnostics{};
    owner.configure(true);
    var program = diagnostics_mod.Diagnostics{};
    var unrelated = diagnostics_mod.Diagnostics{};
    try call(Binding{ .diagnostics = &owner, .capture_programs = true }, Probe.run, .{ &program, &unrelated });
    const measured = owner.snapshot().?;
    try std.testing.expectEqual(@as(u64, 7), measured.execution.?.assigned_kernel_dispatches);
    try std.testing.expectEqual(@as(u64, 3), measured.specializations.?.hits);
    try std.testing.expect(measured.execution_scope == .last_published_program);
    try std.testing.expect(!program.isEnabled() and !unrelated.isEnabled());
    try std.testing.expect(program.snapshot() == null and unrelated.snapshot() == null);
}
