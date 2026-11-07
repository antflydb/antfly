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

//! Optional snapshots. Disabled execution starts no observation scopes or
//! provider sampling. An explicitly enabled allocation observer stays installed
//! until owner retirement so escaped allocations keep a valid release path.
//! Snapshots never expose mutable backend owners.
const std = @import("std");
const ops = @import("../ops/ops.zig");
pub const ExecutionStats = @import("stats.zig").ExecutionStats;
pub const Specializations = @import("specialization_cache.zig").Stats;
const observation = @import("runtime_observation.zig");
pub const Start = struct {
    runtime: ops.RuntimeTelemetry,
    scope: observation.Scope,
    device_owners: ?*const @import("device_owner_registry.zig").Registry = null,
    device_roots: ?*const @import("device_allocation_registry.zig").Registry = null,
    kernels: ?*const observation.KernelScope = null,
    metal_buffers: ?*@import("../backends/metal_telemetry.zig").BufferInterval = null,
    video_resources: ?*@import("antfly_video").preparation.OwnedResourceInterval = null,
};
pub const Text = struct {
    bytes: [128]u8 = @splat(0),
    len: u8 = 0,
    truncated: bool = false,
    pub fn init(value: []const u8) Text {
        var result = Text{};
        const count = @min(value.len, result.bytes.len);
        @memcpy(result.bytes[0..count], value[0..count]);
        result.len = @intCast(count);
        result.truncated = count != value.len;
        return result;
    }
    /// Borrows this value; copying Text never copies a self-referential slice.
    pub fn view(self: *const Text) []const u8 {
        return self.bytes[0..self.len];
    }
};

pub const Operation = enum { forward, encoder, decoder_init, prefill, decode_step };

pub const ContextRuntimeScope = enum { unavailable, program_backend_context, model_stable_backend_context, borrowed_execution_backend_context, shared_backend_context_interval };

/// All fields are owned by value. IDs are process-local numerical operation
/// identities, not completion claims or pointers to live request resources.
pub const Batch = struct {
    pub const Kind = enum { scheduled_step, positioned_prefill };
    pub const Participant = struct {
        request_id: ?u64 = null,
        phase: enum { unknown, prefill, decode } = .unknown,
        query_rows: usize = 0,
        total_rows: usize = 0,
    };
    identity: u64,
    kind: Kind,
    participant_count: usize,
    participants: [32]Participant = @splat(.{}),
    stored_count: u8 = 0,
    truncated: bool = false,
};
/// A synchronous phase in a longer pending resource lifetime. A successful
/// submission is not GPU completion. Await labels describe only the actual
/// readback/driver-await return; completed kernel totals remain unavailable.
pub const PendingInterval = struct {
    pub const Phase = enum { acquire, submit, encode_next, await_readback, cancel, drain };
    pub const Completion = enum { unavailable, readback_returned, driver_await_returned };
    identity: u64,
    request_id: ?u64 = null,
    sequence: u64,
    phase: Phase,
    completion: Completion = .unavailable,
    /// Earlier physical failure remains visible through later cleanup phases.
    prior_failure: ?Text = null,
    counters_scope: enum { synchronous_phase_only } = .synchronous_phase_only,
    whole_interval_counters: enum { unavailable } = .unavailable,
};
pub const PublicationScope = enum { program_execution, session_numerical_dispatch, shared_batch_numerical_dispatch, session_preparation };

/// Copied from the actual retaining owner. The default counts successful
/// Runtime constructor publications. Whisper's distinct scope samples its
/// Model cache lifetime at construction, including concurrent constructors;
/// neither scope represents this invocation's template hits.
pub const NativeDefinitionTemplates = struct {
    scope: enum { retained_definition_lifetime_successful_constructor_publications, model_whisper_cache_lifetime_at_constructor_observation } = .retained_definition_lifetime_successful_constructor_publications,
    phase: enum { unspecified, decode_prefix, initial_causal } = .unspecified,
    availability: enum { sampled, busy } = .busy,
    stats: ?Specializations = null,
};
pub const NativeProgramConstruction = struct {
    scope: enum { retained_runtime_constructor_result, whisper_cold_preparation_result } = .retained_runtime_constructor_result,
    producer: enum { decoder_runtime, whisper_single_token_schedule, whisper_dense_decoder_schedule } = .decoder_runtime,
    outcome: enum { not_attempted, declined, prepared, failed },
    error_name: ?Text = null,
    initial_query_rows: ?usize = null,
    /// Independently sampled initial-phase cache; not an atomic two-cache view.
    initial_definition_templates: ?NativeDefinitionTemplates = null,
    /// Actual independently held admission, copied while the cold owner lives.
    /// These are distinct lifetime reservations, not new grants or memory peaks.
    metadata: ?struct {
        scope: enum { retained_cold_owner_admission_snapshot } = .retained_cold_owner_admission_snapshot,
        private_host_scratch_bytes: usize,
        /// Sum of distinct retained phase Definition reservations, not a new grant.
        shared_definition_host_scratch_bytes: ?usize = null,
        decode_definition_host_scratch_bytes: ?usize = null,
        initial_definition_host_scratch_bytes: ?usize = null,
    } = null,
};
pub const Snapshot = struct {
    native_program_construction: ?NativeProgramConstruction = null,
    native_definition_templates: ?NativeDefinitionTemplates = null,
    report: ?@import("invocation_report.zig").Report = null,
    specializations: ?Specializations = null,
    /// Legacy architecture execution has no program plan/counters.
    execution: ?ExecutionStats = null,
    execution_scope: enum { unavailable, program_execution, last_published_program } = .unavailable,
    provider: ?ops.NativeQuantTimingStats = null,
    runtime: ops.RuntimeTelemetry = .{},
    /// Context counters/gauges remain the sampled backend's observations,
    /// even when the outer submitting-thread scope spans several providers.
    context_backend: ?ops.BackendKind = null,
    context_runtime_scope: ContextRuntimeScope = .unavailable,
    operation: Operation = .forward,
    scope: PublicationScope = .program_execution,
    batch: ?Batch = null,
    pending_interval: ?PendingInterval = null,
    outcome: enum { success, failed, unsupported } = .success,
    error_name: ?Text = null,
    /// Owned error text from the last program snapshot. The corresponding
    /// borrowed ExecutionStats.physical_replay_error is cleared on publication.
    program_error: ?Text = null,
    pub fn owned(self: Snapshot) Snapshot {
        var result = self;
        if (result.execution) |*program| if (program.physical_replay_error) |message| {
            result.program_error = Text.init(message);
            program.physical_replay_error = null;
        };
        result.report = @import("invocation_report.zig").Report.init(result);
        return result;
    }
};

threadlocal var active_capture: ?*Capture = null;
threadlocal var program_observation_depth: usize = 0;
/// Captures exact publications from one owner on this submitting thread.
/// This avoids reading a shared last-snapshot that another request can replace.
pub const Capture = struct {
    diagnostics: *const Diagnostics,
    previous: ?*Capture = null,
    latest: ?Snapshot = null,
    include_programs: bool = false,
    last_program_owner: ?usize = null,
    pub fn begin(self: *Capture, diagnostics: *const Diagnostics) void {
        self.beginPrograms(diagnostics, false);
    }
    pub fn beginPrograms(self: *Capture, diagnostics: *const Diagnostics, include_programs: bool) void {
        self.* = .{ .diagnostics = diagnostics, .previous = active_capture, .include_programs = include_programs };
        active_capture = self;
        if (include_programs) program_observation_depth += 1;
    }
    fn programObservationEnabled() bool {
        return program_observation_depth != 0;
    }
    fn programPublished(diagnostics: *const Diagnostics, value: Snapshot) void {
        var current = active_capture;
        while (current) |capture| : (current = capture.previous) if (capture.include_programs or capture.diagnostics == diagnostics) {
            capture.latest = value;
            capture.last_program_owner = @intFromPtr(diagnostics);
        };
    }
    fn specializationsPublished(diagnostics: *const Diagnostics, stats: Specializations) void {
        var current = active_capture;
        while (current) |capture| : (current = capture.previous) if (capture.last_program_owner == @intFromPtr(diagnostics)) {
            if (capture.latest) |*value| {
                value.specializations = stats;
                value.* = value.owned();
            }
        };
    }
    pub fn finish(self: *Capture) ?Snapshot {
        std.debug.assert(active_capture == self);
        active_capture = self.previous;
        if (self.include_programs) program_observation_depth -= 1;
        return self.latest;
    }
    fn published(diagnostics: *const Diagnostics, value: Snapshot) void {
        var current = active_capture;
        while (current) |capture| : (current = capture.previous) if (capture.diagnostics == diagnostics or capture.last_program_owner == @intFromPtr(diagnostics)) {
            capture.latest = value;
        };
    }
};
pub const ProgramMetadata = union(enum) {
    fixed: Specializations,
    since: struct { counters: *const Specializations, before: Specializations },
    fn snapshot(self: ProgramMetadata) Specializations {
        return switch (self) {
            .fixed => |value| value,
            .since => |value| value.counters.since(value.before),
        };
    }
};

fn observedRuntime(after: ops.RuntimeTelemetry, before: ?Start) ops.RuntimeTelemetry {
    var runtime = if (before) |start| after.since(start.runtime) else ops.RuntimeTelemetry{};
    if (before) |start| {
        runtime.submission = start.scope.snapshot();
        if (start.device_owners) |owners| runtime.device_owners = owners.snapshot();
        if (start.device_roots) |roots| runtime.device_allocator_roots = roots.snapshot();
        if (start.kernels) |kernels| {
            runtime.resolved_project_cuda_launches = kernels.values;
            runtime.resolved_project_metal_dispatches = if (comptime @import("build_options").enable_metal) kernels.metal else null;
            runtime.terminal_native_bodies = kernels.native_bodies;
            runtime.selected_native_dispatches = kernels.native_dispatches;
            runtime.joined_native_workers = kernels.native_workers;
            runtime.native_quant_worker_callbacks = kernels.quant_worker_callbacks;
            runtime.native_attention_programs = kernels.attention_programs;
            runtime.native_activation_programs = kernels.activation_programs;
            runtime.authored_native_programs = kernels.native_programs;
            runtime.authored_native_program_bank = kernels.native_program_bank;
            runtime.owned_native_fallbacks = kernels.native_fallbacks;
        }
        if (start.metal_buffers) |buffers| runtime.metal_buffer_interval = buffers.snapshot(@import("build_options").enable_metal);
        if (start.video_resources) |resources| if (resources.snapshot(@import("build_options").enable_metal)) |value| {
            runtime.video_owned_resources = @import("runtime_telemetry.zig").VideoOwnedResources.from(value);
        };
    }
    return runtime;
}

pub const Diagnostics = struct {
    enabled: std.atomic.Value(bool) = .init(false),
    export_owner: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    last: ?Snapshot = null,

    /// Cold policy installation before publication. This method performs no IO.
    pub fn configureExport(self: *Diagnostics) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.export_owner == 0) self.export_owner = @import("diagnostic_export.zig").ownerIdentity();
    }
    pub fn configure(self: *Diagnostics, enabled: bool) void {
        const previous = self.enabled.swap(enabled, .acq_rel);
        if (previous and !enabled) {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            self.last = null;
        }
    }
    /// Owner configuration occurs at a quiescent cold boundary. Configuring
    /// raw statistics alone intentionally never mutates a numerical provider.
    pub fn configureBackend(self: *Diagnostics, enabled: bool, backend: *const ops.ComputeBackend) void {
        if (enabled) backend.enableRuntimeObservation();
        self.configure(enabled);
    }
    pub fn isEnabled(self: *const Diagnostics) bool {
        return self.enabled.load(.acquire) or Capture.programObservationEnabled();
    }
    pub fn snapshot(self: *Diagnostics) ?Snapshot {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.last;
    }
    pub fn publish(self: *Diagnostics, value: Snapshot) void {
        const owned = value.owned();
        Capture.published(self, owned);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.enabled.load(.acquire)) {
            self.last = owned;
            if (self.export_owner != 0) @import("diagnostic_export.zig").enqueue(self.export_owner, owned);
        }
    }
    /// Only real executable events opt into an enclosing model's program
    /// observation. Arbitrary unrelated-owner publish calls stay excluded.
    pub fn publishProgram(self: *Diagnostics, value: Snapshot) void {
        const owned = value.owned();
        Capture.programPublished(self, owned);
        self.publish(owned);
    }
    /// Disabled callers keep their existing callback/stack path. The large
    /// captured snapshot is allocated only by the separate enabled body.
    pub inline fn callProgram(self: *Diagnostics, source: *const Diagnostics, metadata: ?ProgramMetadata, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        const details = metadata orelse return @call(.auto, callback, args);
        if (!self.isEnabled()) return @call(.auto, callback, args);
        return self.callObservedProgram(source, details, callback, args);
    }
    noinline fn callObservedProgram(self: *Diagnostics, source: *const Diagnostics, metadata: ProgramMetadata, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        var capture: Capture = undefined;
        capture.beginPrograms(source, true);
        defer if (capture.finish()) |snapshot_value| {
            var measured = snapshot_value;
            measured.specializations = metadata.snapshot();
            self.publishProgram(measured);
        };
        return @call(.auto, callback, args);
    }
    pub fn recordSpecializations(self: *Diagnostics, stats: Specializations) void {
        if (!self.isEnabled()) return;
        Capture.specializationsPublished(self, stats);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.enabled.load(.acquire)) if (self.last) |*last| {
            last.specializations = stats;
            last.* = last.owned();
        };
    }
    pub fn begin(self: *const Diagnostics, backend: *const ops.ComputeBackend) ?Start {
        if (!self.isEnabled()) return null;
        return self.beginEnabled(backend);
    }
    /// The caller checked enabled at a quiescent owner boundary. Returned state
    /// contains no address-registered owner; it may move before being borrowed.
    pub fn beginEnabled(_: *const Diagnostics, backend: *const ops.ComputeBackend) Start {
        backend.registerRuntimeObservation();
        return .{ .scope = observation.Scope.init(), .runtime = backend.debugTimingSnapshot().runtime };
    }
    /// Every caller defers this immediately after begin, including error exits.
    pub fn end(before: ?Start) void {
        if (before) |value| {
            var scope = value.scope;
            scope.deinit();
        }
    }
    pub fn recordFailure(self: *Diagnostics, backend: *const ops.ComputeBackend, before: ?Start, failure: anyerror) void {
        if (!self.isEnabled()) return;
        const after = backend.debugTimingSnapshot();
        self.publishProgram(.{ .outcome = .failed, .error_name = Text.init(@errorName(failure)), .context_backend = backend.kind(), .context_runtime_scope = .program_backend_context, .provider = if (after.native_quant_null) null else after.provider, .runtime = observedRuntime(after.runtime, before) });
    }
    pub fn record(self: *Diagnostics, stats: ExecutionStats, backend: *const ops.ComputeBackend, before: ?Start) void {
        if (!self.isEnabled()) return;
        const after = backend.debugTimingSnapshot();
        const runtime = observedRuntime(after.runtime, before);
        self.publishProgram(.{ .execution = stats, .execution_scope = .program_execution, .context_backend = backend.kind(), .context_runtime_scope = .program_backend_context, .provider = if (after.native_quant_null) null else after.provider, .runtime = runtime });
    }
};

test "native execution observations include nested work and unwind every failed invocation" {
    const a = std.testing.allocator;
    const ml = @import("ml");
    const contexts = @import("../backends/compute_context.zig");
    const programs = @import("program.zig");
    const executables = @import("executable.zig");
    var context = try contexts.BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.relu(input));
    var program = try programs.ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try executables.prepare(a, &program, &backend, .compiled_preferred);
    defer executable.deinit();
    executable.configureExecutionDiagnostics(true);
    const value = try backend.fromFloat32Shape(&.{ -1, 2, -3, 4 }, &.{4});
    defer backend.free(value);
    const inputs = [_]@import("../graph/interpreter.zig").RuntimeInput{.{ .node_id = program.id_map[input], .value = value }};
    var outer = observation.Scope.init();
    defer outer.deinit();
    for (0..2) |_| {
        var result = try executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        const measured = executable.diagnostics.snapshot().?;
        try std.testing.expect(measured.runtime.submission != null);
        const observed = measured.runtime.native_allocations.?.compute.?;
        try std.testing.expectEqual(@import("allocation_observer.zig").Coverage.since_enable, observed.coverage);
        try std.testing.expect(observed.counter_scope == .owner_interval);
        try std.testing.expectEqual(result.stats.assigned_kernel_dispatches, measured.runtime.submission.?.native_assigned_dispatches);
        try std.testing.expect(measured.runtime.submission.?.native_assigned_dispatches > 0);
        const values = try backend.toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 0, 2, 0, 4 }, values);
    }
    try std.testing.expectEqual(@as(u64, 2), outer.snapshot().native_assigned_dispatches);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, prepared: *executables.Executable, bindings: []const @import("../graph/interpreter.zig").RuntimeInput) !void {
            var result = try prepared.run(allocator, .{ .runtime_inputs = bindings, .collect_partition_stats = true });
            defer result.deinit(&prepared.runtime);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{ &executable, @as([]const @import("../graph/interpreter.zig").RuntimeInput, &inputs) });
    // Child failures must not turn off the still-live outer scope.
    const before = outer.snapshot().native_assigned_dispatches;
    executable.configureExecutionDiagnostics(false);
    var result = try executable.run(a, .{ .runtime_inputs = &inputs });
    defer result.deinit(&executable.runtime);
    try std.testing.expectEqual(before + 1, outer.snapshot().native_assigned_dispatches);
}

test "diagnostic snapshots own published error text through copies scope retirement and owner clearing" {
    var owner = Diagnostics{};
    owner.configure(true);
    var bytes: [256]u8 = @splat('x');
    @memcpy(bytes[0..12], "ProgramError");
    var capture: Capture = undefined;
    capture.begin(&owner);
    owner.publish(.{ .execution = .{ .physical_replay_error = &bytes } });
    const captured = capture.finish().?;
    const copied = owner.snapshot().?;
    @memset(&bytes, 'z');
    owner.configure(false);
    try std.testing.expect(copied.execution.?.physical_replay_error == null);
    try std.testing.expect(copied.program_error.?.truncated);
    try std.testing.expectEqual(@as(u8, 128), copied.program_error.?.len);
    try std.testing.expectEqualStrings("ProgramError", copied.program_error.?.view()[0..12]);
    try std.testing.expectEqualStrings(copied.program_error.?.view(), captured.program_error.?.view());
    const copied_report = copied.report.?;
    const captured_report = captured.report.?;
    try std.testing.expect(copied_report.program_failure.?.truncated);
    try std.testing.expectEqual(@as(u8, 64), copied_report.program_failure.?.len);
    try std.testing.expectEqualStrings("ProgramError", copied_report.program_failure.?.bytes[0..12]);
    try std.testing.expectEqualDeep(copied_report.program_failure, captured_report.program_failure);
    try std.testing.expectEqualStrings("success", copied_report.outcome);
    try std.testing.expectEqual(@as(u8, 0), copied_report.failure.len);
    owner.configure(true);
    owner.publish(.{});
    try std.testing.expect(owner.snapshot().?.report.?.program_failure == null);
    try std.testing.expectEqualStrings("ProgramError", copied_report.program_failure.?.bytes[0..12]);
}

test "program callback capture preserves comptime arguments error unwinding and call-local cache metadata" {
    const Probe = struct {
        fn run(child: *Diagnostics, counters: *Specializations, comptime fail: bool) anyerror!u8 {
            counters.hits += 2;
            if (child.isEnabled()) child.publishProgram(.{ .execution = .{ .assigned_kernel_dispatches = 11 } });
            if (fail) return error.ObservedChildFailed;
            return 9;
        }
    };
    var root_owner = Diagnostics{};
    var child = Diagnostics{};
    var counters = Specializations{};
    root_owner.configure(true);
    try std.testing.expectEqual(@as(u8, 9), try root_owner.callProgram(&child, .{ .since = .{ .counters = &counters, .before = counters } }, Probe.run, .{ &child, &counters, false }));
    try std.testing.expectEqual(@as(u64, 2), root_owner.snapshot().?.specializations.?.hits);
    try std.testing.expectEqual(@as(u64, 11), root_owner.snapshot().?.execution.?.assigned_kernel_dispatches);
    try std.testing.expect(child.snapshot() == null and !child.isEnabled());
    try std.testing.expectError(error.ObservedChildFailed, root_owner.callProgram(&child, .{ .since = .{ .counters = &counters, .before = counters } }, Probe.run, .{ &child, &counters, true }));
    try std.testing.expectEqual(@as(u64, 2), root_owner.snapshot().?.specializations.?.hits);
    try std.testing.expect(!child.isEnabled());
    root_owner.configure(false);
    try std.testing.expectEqual(@as(u8, 9), try root_owner.callProgram(&child, .{ .fixed = .{} }, Probe.run, .{ &child, &counters, false }));
    try std.testing.expect(root_owner.snapshot() == null and child.snapshot() == null);
}
