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
const metal = @import("../backends/metal_telemetry.zig");

/// Borrowed model-owned capabilities, valid until the retaining Session closes.
/// A stable backend must outlive every synchronous invocation. Temporary native
/// computes register their allocator State through the canonical factory.
pub const Binding = struct {
    diagnostics: *diagnostics_mod.Diagnostics,
    operation: diagnostics_mod.Operation = .forward,
    publication_scope: diagnostics_mod.PublicationScope = .session_numerical_dispatch,
    /// Synchronous observation borrow only; publication copies all metadata.
    batch: ?*const diagnostics_mod.Batch = null,
    pending_interval: ?*const diagnostics_mod.PendingInterval = null,
    capture_programs: bool = false,
    native_definition_templates: ?diagnostics_mod.NativeDefinitionTemplates = null,
    native_program_construction: ?diagnostics_mod.NativeProgramConstruction = null,
    model_allocator: ?std.mem.Allocator = null,
    stable_backend: ?ops.ComputeBackend = null,
    /// Shared providers require their own short metadata exclusion. Raw/private
    /// callers explicitly promise quiescence or their existing numerical lane.
    context_sampling: enum { caller_exclusive, provider_metadata } = .caller_exclusive,
    context_runtime_scope: diagnostics_mod.ContextRuntimeScope = .model_stable_backend_context,
    /// Preserve the numerical ptr/table; sample only while that owner is live.
    /// Caller holds the published owner execution gate, or has quiesced a raw
    /// ungated backend. Installation completes before enabled is published.
    pub fn configure(self: Binding, enabled: bool) bool {
        if (!enabled) {
            self.diagnostics.configure(false);
            return true;
        }
        if (self.context_sampling == .provider_metadata) {
            const backend = self.stable_backend orelse return false;
            const operation = @import("backend_operation.zig");
            var scope = operation.enter(backend, null, null, .metadata) catch return false;
            defer scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            if (scope.token() == null) return false;
            backend.enableRuntimeObservation();
            scope.finish() catch return false;
            self.diagnostics.configure(true);
        } else if (self.stable_backend) |backend| {
            self.diagnostics.configureBackend(true, &backend);
        } else self.diagnostics.configure(true);
        return true;
    }
    pub fn forBackend(self: Binding, backend: ops.ComputeBackend) Binding {
        var copy = self;
        copy.stable_backend = backend;
        copy.context_runtime_scope = .borrowed_execution_backend_context;
        return copy;
    }
};

pub inline fn call(binding: ?Binding, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const capability = binding orelse return @call(.auto, callback, args);
    if (!capability.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observed(capability, callback, args);
}

pub inline fn callOperation(binding: ?Binding, operation: diagnostics_mod.Operation, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    var selected = binding orelse return @call(.auto, callback, args);
    selected.operation = operation;
    return call(selected, callback, args);
}

/// Physical owners can distinguish a clean retry/decline from a successful
/// numerical result without changing the original callback's return/error ABI.
pub const ResultClassification = struct { unsupported: bool = false, error_name: ?[]const u8 = null };
pub inline fn callClassified(binding: ?Binding, comptime classify: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const capability = binding orelse return @call(.auto, callback, args);
    if (!capability.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observedClassified(capability, classify, callback, args);
}
noinline fn observedClassified(binding: Binding, comptime classify: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    var scope: Invocation = undefined;
    scope.begin(binding);
    defer scope.finish();
    const result = @call(.auto, callback, args) catch |err| {
        scope.error_name = @errorName(err);
        return err;
    };
    const classified = classify(result);
    scope.unsupported = classified.unsupported;
    scope.error_name = classified.error_name;
    return result;
}

/// Acquisition/eligibility failed before the operation owned its backend lane.
/// Publish the real error without sampling an independently active provider.
pub fn recordFailureWithoutBackendSample(binding: ?Binding, operation: diagnostics_mod.Operation, failure: anyerror) void {
    const selected = binding orelse return;
    if (!selected.diagnostics.isEnabled()) return;
    selected.diagnostics.publish(.{ .scope = .session_numerical_dispatch, .operation = operation, .outcome = .failed, .error_name = diagnostics_mod.Text.init(@errorName(failure)) });
}

/// Borrow the retaining model request's publication owner and the operation's
/// ACTUAL backend. The caller holds its existing numerical lane or quiescence;
/// this adds no gate, allocator wrapper, tensor traversal or GPU completion wait.
pub inline fn callContextOperation(context: anytype, operation: diagnostics_mod.Operation, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const selected = contextBinding(context, operation) orelse return @call(.auto, callback, args);
    return call(selected, callback, args);
}
pub inline fn callContextOperationClassified(context: anytype, operation: diagnostics_mod.Operation, comptime classify: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const selected = contextBinding(context, operation) orelse return @call(.auto, callback, args);
    return callClassified(selected, classify, callback, args);
}
inline fn contextBinding(context: anytype, operation: diagnostics_mod.Operation) ?Binding {
    const source = context.diagnosticsBinding() orelse return null;
    if (!source.diagnostics.isEnabled()) return null;
    var selected = source.forBackend(context.cb.*);
    selected.capture_programs = true;
    selected.operation = operation;
    return selected;
}
pub fn recordContextAcquisitionFailure(context: anytype, operation: diagnostics_mod.Operation, failure: anyerror) void {
    recordFailureWithoutBackendSample(context.diagnosticsBinding(), operation, failure);
}

test "classified physical retries and declines preserve results and replace success only while enabled" {
    const Probe = struct {
        const Outcome = enum { complete, retry, declined };
        var classify_calls: usize = 0;
        var numerical_calls: usize = 0;
        fn run(value: Outcome) !Outcome {
            numerical_calls += 1;
            return value;
        }
        fn classify(value: Outcome) ResultClassification {
            classify_calls += 1;
            return switch (value) {
                .complete => .{},
                .retry => .{ .error_name = "MemoryBudgetExceeded" },
                .declined => .{ .unsupported = true },
            };
        }
    };
    Probe.classify_calls = 0;
    Probe.numerical_calls = 0;
    var diagnostics = diagnostics_mod.Diagnostics{};
    const binding = Binding{ .diagnostics = &diagnostics, .operation = .prefill };
    try std.testing.expectEqual(Probe.Outcome.retry, try callClassified(binding, Probe.classify, Probe.run, .{Probe.Outcome.retry}));
    try std.testing.expectEqual(@as(usize, 0), Probe.classify_calls);
    try std.testing.expect(diagnostics.snapshot() == null and !registry_mod.enabled());
    diagnostics.configure(true);
    try std.testing.expectEqual(Probe.Outcome.complete, try callClassified(binding, Probe.classify, Probe.run, .{Probe.Outcome.complete}));
    try std.testing.expect(diagnostics.snapshot().?.outcome == .success);
    try std.testing.expectEqual(Probe.Outcome.retry, try callClassified(binding, Probe.classify, Probe.run, .{Probe.Outcome.retry}));
    const retry = diagnostics.snapshot().?;
    try std.testing.expect(retry.operation == .prefill and retry.outcome == .failed);
    try std.testing.expectEqualStrings("MemoryBudgetExceeded", retry.error_name.?.view());
    try std.testing.expectEqual(Probe.Outcome.declined, try callClassified(binding, Probe.classify, Probe.run, .{Probe.Outcome.declined}));
    try std.testing.expect(diagnostics.snapshot().?.outcome == .unsupported);
    try std.testing.expectEqual(@as(usize, 4), Probe.numerical_calls);
    try std.testing.expectEqual(@as(usize, 3), Probe.classify_calls);
    try std.testing.expect(!registry_mod.enabled());
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

/// Register each foreign resident owner once at the public invocation boundary.
pub inline fn callResident(binding: ?Binding, inputs: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const capability = binding orelse return @call(.auto, callback, args);
    if (!capability.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observedInputs(false, capability, inputs, callback, args);
}
pub inline fn callStage(binding: ?Binding, inputs: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const capability = binding orelse return @call(.auto, callback, args);
    if (!capability.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observedInputs(true, capability, inputs, callback, args);
}
pub inline fn callInputs(binding: ?Binding, inputs: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    return switch (inputs) {
        .host => call(binding, callback, args),
        .resident => |resident| callResident(binding, resident, callback, args),
    };
}
noinline fn observedInputs(comptime stages: bool, binding: Binding, inputs: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    var scope: Invocation = undefined;
    scope.begin(binding);
    defer scope.finish();
    for (inputs) |input| {
        if (comptime stages) {
            if (input == .resident) input.resident.backend.registerTensorOwnershipObservation(input.resident.value);
        } else input.backend.registerTensorOwnershipObservation(input.value);
    }
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
    device_owners: @import("device_owner_registry.zig").Registry,
    device_roots: @import("device_allocation_registry.zig").Registry,
    scope: observation.Scope,
    transfers: @import("transfer_audit.zig").Audit = .{},
    kernels: observation.KernelScope = .{},
    metal_buffers: metal.BufferInterval = .{},
    video_resources: @import("antfly_video").preparation.OwnedResourceInterval = .{},
    before: ?ops.RuntimeTelemetry = null,
    process_before: ?@import("runtime_telemetry.zig").Process = null,
    error_name: ?[]const u8 = null,
    unsupported: bool = false,

    fn begin(self: *Invocation, binding: Binding) void {
        self.* = .{ .binding = binding, .capture = undefined, .owners = undefined, .device_owners = undefined, .device_roots = undefined, .scope = observation.Scope.init() };
        self.metal_buffers.begin(build_options.enable_metal);
        self.video_resources.begin(build_options.enable_metal);
        self.capture.beginPrograms(binding.diagnostics, binding.capture_programs);
        self.owners.begin(registry_mod.metadataAllocator());
        self.device_owners.begin(registry_mod.metadataAllocator());
        self.device_roots.begin(registry_mod.metadataAllocator());
        if (binding.model_allocator) |allocator| {
            if (observer.State.forAllocator(allocator)) |owner| registry_mod.bind(.model, owner) else registry_mod.missing(.model);
        }
        if (sampleBackend(binding)) |sample| self.before = sample.value.runtime;
        if (comptime build_options.enable_metal) self.process_before = metal.runtimeProcessSnapshot();
        // self is already in its final stack placement. Never install this
        // address in a returned-by-value constructor; nested strict audits
        // remain our parent and enforce policy across all child transfers.
        self.kernels.begin();
        @import("transfer_audit.zig").begin(&self.transfers) catch unreachable;
    }

    fn finish(self: *Invocation) void {
        @import("transfer_audit.zig").end();
        // Restore capture before publishing so this invocation cannot capture
        // its own final snapshot. Parent observations still see child work.
        const captured = self.capture.finish();
        var snapshot = captured orelse diagnostics_mod.Snapshot{};
        if (captured == null) if (self.before) |before| if (sampleBackend(self.binding)) |after| {
            snapshot.runtime = after.value.runtime.since(before);
            snapshot.provider = if (after.value.native_quant_null) null else after.value.provider;
            snapshot.context_backend = after.backend;
            snapshot.context_runtime_scope = if (self.binding.context_sampling == .provider_metadata) .shared_backend_context_interval else self.binding.context_runtime_scope;
        };
        snapshot.execution_scope = if (snapshot.execution != null) .last_published_program else .unavailable;
        snapshot.runtime.transfers = self.transfers.measurements();
        snapshot.runtime.resolved_project_metal_dispatches = if (comptime build_options.enable_metal) self.kernels.metal else null;
        snapshot.runtime.terminal_native_bodies = self.kernels.native_bodies;
        snapshot.runtime.selected_native_dispatches = self.kernels.native_dispatches;
        snapshot.runtime.joined_native_workers = self.kernels.native_workers;
        snapshot.runtime.native_quant_worker_callbacks = self.kernels.quant_worker_callbacks;
        snapshot.runtime.native_attention_programs = self.kernels.attention_programs;
        snapshot.runtime.native_activation_programs = self.kernels.activation_programs;
        snapshot.runtime.authored_native_programs = self.kernels.native_programs;
        snapshot.runtime.authored_native_program_bank = self.kernels.native_program_bank;
        if (self.binding.native_definition_templates) |value| snapshot.native_definition_templates = value;
        if (self.binding.native_program_construction) |value| snapshot.native_program_construction = value;
        snapshot.runtime.owned_native_fallbacks = self.kernels.native_fallbacks;
        snapshot.runtime.resolved_project_cuda_launches = self.kernels.finish();
        snapshot.runtime.submission = self.scope.snapshot();
        snapshot.runtime.allocation_owners = self.owners.finish();
        snapshot.runtime.device_owners = self.device_owners.end();
        snapshot.runtime.device_allocator_roots = self.device_roots.end();
        snapshot.runtime.metal_buffer_interval = self.metal_buffers.finish(build_options.enable_metal);
        if (self.video_resources.finish(build_options.enable_metal)) |value| snapshot.runtime.video_owned_resources = @import("runtime_telemetry.zig").VideoOwnedResources.from(value);
        if (comptime build_options.enable_metal) {
            if (self.process_before) |before| snapshot.runtime.process = metal.runtimeProcessSnapshot().since(before);
        }
        snapshot.outcome = if (self.error_name != null) .failed else if (self.unsupported) .unsupported else .success;
        snapshot.error_name = if (self.error_name) |name| diagnostics_mod.Text.init(name) else null;
        snapshot.operation = self.binding.operation;
        snapshot.scope = self.binding.publication_scope;
        snapshot.batch = if (self.binding.batch) |cohort| cohort.* else null;
        snapshot.pending_interval = if (self.binding.pending_interval) |interval| interval.* else null;
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

test "typed incremental observations use model-owned operation names without wrapping backend identity" {
    const Probe = struct {
        fn step() anyerror!void {
            observation.nativeAssignedDispatch();
        }
    };
    var diagnostics = diagnostics_mod.Diagnostics{};
    diagnostics.configure(true);
    try callOperation(Binding{ .diagnostics = &diagnostics }, .decode_step, Probe.step, .{});
    const measured = diagnostics.snapshot().?;
    try std.testing.expect(measured.operation == .decode_step);
    try std.testing.expectEqual(@as(u64, 1), measured.runtime.submission.?.native_assigned_dispatches);
    try std.testing.expect(measured.execution == null);
}

test "resident owner observations preserve the disabled raw path and unwind source owners on failure" {
    const devices = @import("device_owner_registry.zig");
    const Probe = struct {
        references: usize = 1,
        callbacks: usize = 0,
        registrations: usize = 0,
        fail: bool = false,
        expected_enabled: bool = false,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn snapshot(raw: *anyopaque) devices.Report {
            return .{ .identity = @intFromPtr(raw), .phase = .detached_cleanup, .physical_live_bytes = 32 };
        }
        const owner_vtable = devices.Owner.VTable{ .retain = retain, .release = release, .snapshot = snapshot };
        fn register(raw: *anyopaque, _: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.registrations += 1;
            devices.bind(.{ .context = self, .vtable = &owner_vtable });
        }
        fn dispatch(self: *@This()) !usize {
            self.callbacks += 1;
            try std.testing.expectEqual(self.expected_enabled, devices.enabled());
            try std.testing.expectEqual(@as(usize, if (self.expected_enabled) 2 else 1), self.references);
            if (self.fail) return error.ObservedForeignOwnerFailure;
            return 7;
        }
    };
    const Input = struct { backend: *const ops.ComputeBackend, value: ops.CT };
    var probe = Probe{};
    var table = @import("../ops/native_compute.zig").vtable_impl;
    const kernels = ops.KernelBackend{ .implementation_table = &table, .registerTensorOwnershipObservation = &Probe.register };
    table.kernels = &kernels;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    var diagnostics = diagnostics_mod.Diagnostics{};
    const binding = Binding{ .diagnostics = &diagnostics };
    const unread: [1]Input = undefined;
    // Disabled dispatch must not visit even the input metadata/owner callbacks.
    try std.testing.expectEqual(@as(usize, 7), try callResident(binding, &unread, Probe.dispatch, .{&probe}));
    try std.testing.expectEqual(@as(usize, 0), probe.registrations);
    try std.testing.expect(diagnostics.snapshot() == null);
    const input = [_]Input{.{ .backend = &backend, .value = &probe }};
    diagnostics.configure(true);
    probe.expected_enabled = true;
    try std.testing.expectEqual(@as(usize, 7), try callResident(binding, &input, Probe.dispatch, .{&probe}));
    var measured = diagnostics.snapshot().?;
    try std.testing.expectEqual(@as(usize, 1), measured.runtime.device_owners.?.owners);
    try std.testing.expectEqual(@as(usize, 32), measured.runtime.device_owners.?.reports[0].physical_live_bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.references);
    probe.fail = true;
    try std.testing.expectError(error.ObservedForeignOwnerFailure, callResident(binding, &input, Probe.dispatch, .{&probe}));
    measured = diagnostics.snapshot().?;
    try std.testing.expectEqual(.failed, measured.outcome);
    try std.testing.expectEqual(@as(usize, 1), measured.runtime.device_owners.?.owners);
    try std.testing.expectEqual(@as(usize, 1), probe.references);
    try std.testing.expect(!devices.enabled());
}

/// Only actual provider exclusion can authorize snapshots of mutable provider
/// metadata. Shared contexts fail closed if no target owner exists or is busy.
/// A Session gate is not proof of shared backend ownership.
const ContextSample = struct { backend: ops.BackendKind, value: ops.BackendDebugTimingSnapshot };
fn sampleBackend(binding: Binding) ?ContextSample {
    const backend = binding.stable_backend orelse return null;
    if (binding.context_sampling == .caller_exclusive) {
        backend.registerRuntimeObservation();
        return .{ .backend = backend.kind(), .value = backend.debugTimingSnapshot() };
    }
    const operation = @import("backend_operation.zig");
    var scope = operation.enter(backend, null, null, .metadata) catch return null;
    defer scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    if (scope.token() == null) return null;
    backend.registerRuntimeObservation();
    const result = ContextSample{ .backend = backend.kind(), .value = backend.debugTimingSnapshot() };
    scope.finish() catch return null;
    return result;
}

test "uniform report chained transfer scopes preserve outer strict policy and unwind final placement" {
    const audit = @import("transfer_audit.zig");
    const Probe = struct {
        fn run() !void {
            audit.upload(7);
            var child = audit.Audit{};
            try audit.begin(&child);
            defer audit.end();
            audit.upload(11);
            audit.deviceCopy(13);
            try audit.checkDeviceHostAccess();
        }
    };
    var outer = audit.Audit{ .forbid_device_host_access = true };
    try audit.begin(&outer);
    defer audit.end();
    var diagnostics = diagnostics_mod.Diagnostics{};
    diagnostics.configure(true);
    for (0..2) |_| {
        try std.testing.expectError(error.DeviceReadbackForbidden, call(Binding{ .diagnostics = &diagnostics }, Probe.run, .{}));
        const measured = diagnostics.snapshot().?;
        const report = measured.report.?;
        try std.testing.expectEqualStrings("failed", report.outcome);
        try std.testing.expectEqual(@as(u64, 2), report.transfers.?.upload_calls);
        try std.testing.expectEqual(@as(u64, 18), report.transfers.?.upload_bytes);
        try std.testing.expectEqual(@as(?u64, 1), report.transfers.?.device_copy_calls);
        try std.testing.expectEqual(@as(?u64, 13), report.transfers.?.device_copy_bytes);
        try std.testing.expectEqual(@as(u64, 0), report.transfers.?.download_calls);
        try std.testing.expectError(error.DeviceReadbackForbidden, audit.checkDeviceHostAccess());
    }
    audit.upload(17); // proves the invocation restored the actual outer parent
    try std.testing.expectEqual(@as(u64, 53), outer.upload_bytes);
    try std.testing.expectEqual(@as(u64, 26), outer.device_copy_bytes);
}

fn controlledUniformReport(comptime kind: @import("../backends/backends.zig").BackendType) !void {
    if (kind == .native and !build_options.enable_native) return error.SkipZigTest;
    if (kind == .metal and !build_options.enable_metal) return error.SkipZigTest;
    if (kind == .cuda and !build_options.enable_cuda) return error.SkipZigTest;
    const shared = @import("../backends/compute_context.zig").SharedBackendContext.initWithOptions(std.testing.allocator, kind, std.testing.io, .{ .observe_native_allocations = kind == .native }) catch |err| switch (@as(anyerror, err)) {
        error.CudaUnavailable, error.NoCudaDevices, error.MetalNotEnabled, error.MetalUnavailable, error.MetalRuntimeUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer shared.release();
    const backend = shared.stableBackend();
    var diagnostics = diagnostics_mod.Diagnostics{};
    const binding = Binding{ .diagnostics = &diagnostics, .stable_backend = backend.* };
    _ = binding.configure(true);
    var operation = try @import("backend_operation.zig").enter(backend.*, null, null, .managed);
    defer operation.deinitChecked() catch unreachable;
    const Probe = struct {
        fn run(cb: *const ops.ComputeBackend) !void {
            // Explicit resident upload avoids environment-controlled Metal
            // host-only fromFloat32 staging and makes actual device work required.
            const input = if (cb.kind() == .metal)
                try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{ 1.25, -2.5, 3.75, 0.125 }, .shape = &.{4} } }, .{})
            else
                try cb.fromFloat32Shape(&.{ 1.25, -2.5, 3.75, 0.125 }, &.{4});
            defer cb.free(input);
            const added = try cb.add(input, input);
            defer cb.free(added);
            const result = try cb.toFloat32(added, std.testing.allocator);
            defer std.testing.allocator.free(result);
            try std.testing.expectEqualSlices(f32, &.{ 2.5, -5, 7.5, 0.25 }, result);
        }
    };
    try call(binding, Probe.run, .{backend});
    const measured = diagnostics.snapshot().?;
    const report = measured.report.?;
    try std.testing.expectEqualStrings(@tagName(kind), report.backend.?);
    try std.testing.expect(report.transfers_scope == .invocation_submitting_thread);
    try std.testing.expect(report.whole_backend_residency_and_invocation_peak == .unavailable);
    var found = false;
    const entries = report.implementation_entries.?;
    for (entries.records[0..entries.count]) |entry| if (entry.family == .add) {
        found = true;
        try std.testing.expectEqual(@as(u64, 1), entry.successful_returns);
        try std.testing.expectEqual(@intFromPtr(backend.vtable.add), entry.last_implementation_identity);
    };
    try std.testing.expect(found);
    if (kind == .native) {
        try std.testing.expectEqual(@as(u64, 0), report.transfers.?.upload_calls);
        try std.testing.expectEqual(@as(u64, 0), report.transfers.?.download_calls);
        try std.testing.expect(report.cuda == null);
    } else {
        try std.testing.expect(report.transfers.?.upload_calls >= 1);
        try std.testing.expect(report.transfers.?.upload_bytes >= 16);
        // Shared Metal host aliases are access, never a fabricated DtoH copy.
        try std.testing.expect(report.transfers.?.download_bytes >= 16 or report.transfers.?.host_access_bytes >= 16);
        if (kind == .cuda) {
            try std.testing.expect(report.transfers.?.download_calls >= 1);
            try std.testing.expect(report.transfers.?.download_bytes >= 16);
            const kernels = report.resolved_project_cuda_launches.?;
            try std.testing.expect(kernels.total_calls >= 1 and kernels.count >= 1);
            try std.testing.expect(kernels.records[0].function_identity != 0);
            try std.testing.expect(kernels.records[0].grid[0] != 0 and kernels.records[0].block[0] != 0);
        } else {
            try std.testing.expect(report.metal_encoded_dispatches.? >= 1);
            const actual = report.resolved_project_metal_dispatches.?;
            try std.testing.expect(actual.total_messages >= 1 and actual.count >= 1);
            try std.testing.expect(actual.unknown_pipeline_messages == 0 and !actual.truncated);
            try std.testing.expect(actual.records[0].pipeline_identity != 0 and actual.records[0].encoder_identity != 0);
            try std.testing.expect(actual.records[0].threads[0] > 0 and actual.records[0].grid[0] > 0);
        }
    }
    diagnostics.configure(false);
    try call(binding, Probe.run, .{backend});
    try std.testing.expect(diagnostics.snapshot() == null and !observation.enabled());
    try operation.finish();
}

test "uniform report actual controlled Native numerical dispatch" {
    try controlledUniformReport(.native);
}
test "uniform report actual controlled Metal numerical dispatch" {
    try controlledUniformReport(.metal);
}
test "uniform report actual controlled CUDA numerical dispatch and resolved launch geometry" {
    try controlledUniformReport(.cuda);
}

test "uniform resolved launcher scopes chain on final placement and declare truncation" {
    var parent = observation.KernelScope{};
    parent.begin();
    observation.cudaKernel(1, 2, 3, false, .{ 1, 1, 1 }, .{ 32, 1, 1 }, 0, false, false);
    var child = observation.KernelScope{};
    child.begin();
    observation.cudaKernel(1, 2, 3, false, .{ 2, 1, 1 }, .{ 32, 1, 1 }, 4, true, true);
    const nested = child.finish();
    try std.testing.expectEqual(@as(u64, 1), nested.total_calls);
    try std.testing.expectEqual(@as(u64, 1), nested.records[0].capture_recording_calls);
    try std.testing.expectEqual(@as(u64, 1), nested.records[0].foreign_stream_calls);
    for (0..8) |index| observation.cudaKernel(1, 10 + index, 3, false, .{ 1, 1, 1 }, .{ 32, 1, 1 }, 0, false, false);
    const measured = parent.finish();
    try std.testing.expectEqual(@as(u64, 10), measured.total_calls);
    try std.testing.expect(measured.truncated and measured.unrecorded_calls == 1);
    try std.testing.expectEqual(@as(u64, 1), measured.records[0].launch_geometry_changes);
    try std.testing.expectEqual(@as(u64, 1), measured.records[0].accepted_direct_calls);
    observation.cudaKernel(1, 2, 3, false, .{ 1, 1, 1 }, .{ 32, 1, 1 }, 0, false, false); // detached is inert
}

test "uniform report actual terminal Native activation body nested scopes and off path" {
    const Probe = struct {
        fn body() !void {
            var data: [35]f32 = @splat(0);
            @import("../backends/activations.zig").gelu(&data);
            try std.testing.expectEqualSlices(f32, &@as([35]f32, @splat(0)), &data);
        }
        fn nested(inner: Binding) !void {
            try body();
            try call(inner, body, .{});
            try body();
        }
    };
    var outer = diagnostics_mod.Diagnostics{};
    outer.configure(true);
    var inner = diagnostics_mod.Diagnostics{};
    inner.configure(true);
    try call(Binding{ .diagnostics = &outer }, Probe.nested, .{Binding{ .diagnostics = &inner }});
    const child = inner.snapshot().?.report.?.terminal_native_bodies.?;
    const parent = outer.snapshot().?.report.?.terminal_native_bodies.?;
    try std.testing.expectEqual(@as(u64, 1), child.total_calls);
    try std.testing.expectEqual(@as(u64, 3), parent.total_calls);
    try std.testing.expectEqualStrings("gelu", parent.records[0].activation);
    const width = parent.records[0].vector_width;
    try std.testing.expect(width > 0);
    try std.testing.expectEqual(@as(u64, 3 * (35 / width)), parent.records[0].vector_iterations);
    try std.testing.expectEqual(@as(u64, 3 * (35 % width)), parent.records[0].scalar_elements);
    outer.configure(false);
    try call(Binding{ .diagnostics = &outer }, Probe.body, .{});
    try std.testing.expect(outer.snapshot() == null and !observation.enabled());
}

test "uniform report resolved Metal dispatch sink nested parent restoration unknown and truncation" {
    var parent = observation.KernelScope{};
    parent.begin();
    observation.antfly_runtime_observe_metal_dispatch(1, 2, 0, 32, 1, 1, 32, 1, 1, false);
    var child = observation.KernelScope{};
    child.begin();
    observation.antfly_runtime_observe_metal_dispatch(1, 2, 0, 64, 1, 1, 32, 1, 1, false);
    try std.testing.expectEqual(@as(u64, 1), child.metal.total_messages);
    _ = child.finish();
    observation.antfly_runtime_observe_metal_dispatch(1, 0, 0, 32, 1, 1, 32, 1, 1, true);
    for (0..8) |index| observation.antfly_runtime_observe_metal_dispatch(1, index + 10, 1, 1, 1, 1, 32, 1, 1, false);
    try std.testing.expectEqual(@as(u64, 11), parent.metal.total_messages);
    try std.testing.expectEqual(@as(u64, 1), parent.metal.unknown_pipeline_messages);
    try std.testing.expectEqual(@as(u64, 1), parent.metal.unrecorded_messages);
    try std.testing.expect(parent.metal.truncated and parent.metal.association_table_overflowed_during_enclosing_scope);
    try std.testing.expectEqual(@as(u64, 1), parent.metal.records[0].geometry_changes);
    _ = parent.finish();
    observation.antfly_runtime_observe_metal_dispatch(1, 2, 0, 32, 1, 1, 32, 1, 1, false); // detached is inert
}

test "uniform report actual nested Metal pipelines restore parent and disabled calls stay unreported" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const shared = @import("../backends/compute_context.zig").SharedBackendContext.initWithOptions(std.testing.allocator, .metal, std.testing.io, .{}) catch |err| switch (@as(anyerror, err)) {
        error.MetalNotEnabled, error.MetalUnavailable, error.MetalRuntimeUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer shared.release();
    const backend = shared.stableBackend();
    var outer = diagnostics_mod.Diagnostics{};
    var inner = diagnostics_mod.Diagnostics{};
    const outer_binding = Binding{ .diagnostics = &outer, .stable_backend = backend.* };
    const inner_binding = Binding{ .diagnostics = &inner, .stable_backend = backend.* };
    _ = outer_binding.configure(true);
    _ = inner_binding.configure(true);
    var operation = try @import("backend_operation.zig").enter(backend.*, null, null, .managed);
    defer operation.deinitChecked() catch unreachable;
    const Probe = struct {
        fn body(cb: *const ops.ComputeBackend) !void {
            const input = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{1.5}, .shape = &.{1} } }, .{});
            defer cb.free(input);
            const output = try cb.add(input, input);
            defer cb.free(output);
            const values = try cb.toFloat32(output, std.testing.allocator);
            defer std.testing.allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{3}, values);
        }
        fn nested(cb: *const ops.ComputeBackend, inner_binding_value: Binding) !void {
            try body(cb);
            try call(inner_binding_value, body, .{cb});
            try body(cb);
        }
    };
    try call(outer_binding, Probe.nested, .{ backend, inner_binding });
    const child = inner.snapshot().?.report.?.resolved_project_metal_dispatches.?;
    const parent = outer.snapshot().?.report.?.resolved_project_metal_dispatches.?;
    try std.testing.expect(child.total_messages > 0 and child.count > 0);
    try std.testing.expect(parent.total_messages >= child.total_messages + 2);
    try std.testing.expect(parent.unknown_pipeline_messages == 0 and child.unknown_pipeline_messages == 0);
    for (parent.records[0..parent.count]) |record| try std.testing.expect(record.pipeline_identity != 0 and record.encoder_identity != 0);
    outer.configure(false);
    inner.configure(false);
    try call(outer_binding, Probe.body, .{backend});
    try std.testing.expect(outer.snapshot() == null and !observation.enabled());
    _ = outer_binding.configure(true);
    try call(outer_binding, Probe.body, .{backend});
    const fresh = outer.snapshot().?.report.?.resolved_project_metal_dispatches.?;
    try std.testing.expect(fresh.total_messages > 0 and fresh.unknown_pipeline_messages == 0);
    try operation.finish();
}

test "uniform report actual same Metal encoder inherits then changes pipeline across nested scope" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    // These extern declarations and all shader/SDK work belong only to this
    // test. Other imported-test roots may link the ordinary C runtime object.
    const sdk = struct {
        extern fn antfly_metal_observation_identity_fixture(context: *anyopaque, nested: *const fn (*anyopaque, bool) callconv(.c) void) c_int;
        extern fn antfly_metal_observation_binding_map_fixture() c_int;
    };
    const expected_main_test_root = comptime expected: {
        const root = @import("root");
        if (!@hasDecl(root, "execution") or !@hasDecl(root, "backends")) break :expected false;
        break :expected root.execution == @import("root.zig") and root.backends == @import("../backends/backends.zig");
    };
    const Fixture = struct {
        scope: ?observation.Scope = null,
        inner: observation.KernelScope = .{},
        child: observation.MetalDispatches = .{},
        fn nested(raw: *anyopaque, enter: bool) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (enter) {
                std.debug.assert(self.scope == null);
                self.scope = observation.Scope.init();
                self.inner.begin();
            } else {
                self.child = self.inner.metal;
                _ = self.inner.finish();
                self.scope.?.deinit();
                self.scope = null;
            }
        }
    };
    var parent = observation.KernelScope{};
    var thread = observation.Scope.init();
    parent.begin();
    var fixture = Fixture{};
    const result = sdk.antfly_metal_observation_identity_fixture(&fixture, Fixture.nested);
    const measured = parent.metal;
    _ = parent.finish();
    thread.deinit();
    if (result == 2) {
        // Main inference unit root defines the fixture macro. Compiled-out
        // there is a failing configuration, never an accidentally green skip.
        try std.testing.expect(!expected_main_test_root);
        return error.SkipZigTest;
    }
    if (result == 1) return error.SkipZigTest; // no actual Metal device
    try std.testing.expectEqual(@as(c_int, 0), result);
    try std.testing.expect(fixture.scope == null);
    try std.testing.expectEqual(@as(u64, 2), fixture.child.total_messages);
    try std.testing.expectEqual(@as(u64, 3), measured.total_messages);
    try std.testing.expectEqual(@as(u8, 2), measured.count);
    try std.testing.expectEqual(@as(u8, 2), fixture.child.count);
    try std.testing.expectEqual(@as(u64, 0), measured.unknown_pipeline_messages);
    try std.testing.expectEqual(@as(u64, 0), fixture.child.unknown_pipeline_messages);
    try std.testing.expectEqual(measured.records[0].encoder_identity, measured.records[1].encoder_identity);
    try std.testing.expect(measured.records[0].pipeline_identity != measured.records[1].pipeline_identity);
    try std.testing.expectEqual(@as(u64, 1), measured.records[0].encoded_messages);
    try std.testing.expectEqual(@as(u64, 2), measured.records[1].encoded_messages);
    try std.testing.expectEqual(@as(u64, 1), fixture.child.records[0].encoded_messages);
    try std.testing.expectEqual(@as(u64, 1), fixture.child.records[1].encoded_messages);
    // This invokes the real map helpers without any SDK call or sink event.
    try std.testing.expectEqual(@as(c_int, 0), sdk.antfly_metal_observation_binding_map_fixture());
}
