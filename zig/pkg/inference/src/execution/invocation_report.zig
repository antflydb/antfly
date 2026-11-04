// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Owned uniform per-publication view. Units and ownership scopes stay separate;
//! no lifetime peak is represented as a request peak or vendor kernel census.
const runtime = @import("runtime_telemetry.zig");
const storage = @import("storage_accounting.zig");
const Specializations = @import("specialization_cache.zig").Stats;
pub const Report = struct {
    schema_version: u8 = 1,
    operation: []const u8,
    publication_scope: []const u8,
    synchronous_interval: enum { invocation_body, pending_phase_only } = .invocation_body,
    backend: ?[]const u8 = null,
    backend_counter_scope: []const u8,
    outcome: []const u8,
    failure: @import("runtime_observation.zig").FailureText = .{},
    /// Observed failure from the corresponding program; ordinary execution can
    /// still succeed after capture fails. Inline text never borrows its owner.
    program_failure: ?@import("runtime_observation.zig").FailureText = null,
    transfers: ?@import("transfer_audit.zig").Measurements = null,
    transfers_scope: enum { unavailable, invocation_submitting_thread, last_program_only } = .unavailable,
    implementation_entries: ?@import("runtime_observation.zig").Entries = null,
    native_assigned_dispatches: ?u64 = null,
    metal_encoded_dispatches: ?u64 = null,
    metal_vendor_encodes: ?u64 = null,
    metal_command_submissions: ?u64 = null,
    metal_command_waits: ?u64 = null,
    /// Actual accepted driver API/topology units, not completed device kernels.
    cuda: ?runtime.Cuda = null,
    resolved_project_cuda_launches: ?@import("runtime_observation.zig").Kernels = null,
    resolved_project_metal_dispatches: ?@import("runtime_observation.zig").MetalDispatches = null,
    terminal_native_bodies: ?@import("runtime_observation.zig").NativeBodies = null,
    selected_native_dispatches: ?@import("runtime_observation.zig").NativeDispatches = null,
    joined_native_workers: ?@import("runtime_observation.zig").NativeWorkers = null,
    native_quant_worker_callbacks: ?@import("runtime_observation.zig").NativeQuantWorkerCallbacks = null,
    native_attention_programs: ?@import("runtime_observation.zig").NativeAttentionPrograms = null,
    native_activation_programs: ?@import("runtime_observation.zig").NativeActivationPrograms = null,
    owned_native_fallbacks: ?@import("runtime_observation.zig").NativeFallbacks = null,
    device_allocator_roots: ?@import("device_allocation_registry.zig").Summary = null,
    metal_buffer_interval: ?@import("../backends/metal_telemetry.zig").BufferIntervalReport = null,
    video_owned_resources: ?runtime.VideoOwnedResources = null,
    backend_counters: runtime.Counters = .{},
    completed_device_kernels: enum { unavailable_opaque_vendor } = .unavailable_opaque_vendor,
    backend_buffer_memory: runtime.Memory = .{},
    observed_host_requested_live_bytes: ?u64 = null,
    observed_host_owner_interval_peak_bytes: ?u64 = null,
    host_peak_scope: enum { registered_single_root_owner_interval_including_concurrent_activity } = .registered_single_root_owner_interval_including_concurrent_activity,
    observed_allocator_roots: ?usize = null,
    observed_allocator_root_identity: ?usize = null,
    host_allocation_counter_scope: enum { registered_single_root_invocation_interval } = .registered_single_root_invocation_interval,
    host_allocations: ?u64 = null,
    host_frees: ?u64 = null,
    host_resizes: ?u64 = null,
    host_remaps: ?u64 = null,
    maximum_individual_host_owner_lifetime_peak_bytes: ?u64 = null,
    allocator_coverage_incomplete: bool = true,
    retained_device_owner_count: ?usize = null,
    detached_bytes_added_to_backend_gauge: bool = false,
    worker_process: ?runtime.Process = null,
    /// Actual planned-owner class gauges; lifetime peaks remain lifetime peaks.
    program_storage_scope: []const u8 = "unavailable",
    program_constants: ?storage.Usage = null,
    program_activations: ?storage.Usage = null,
    program_workspace: ?storage.Usage = null,
    program_host_workspace: ?storage.Usage = null,
    program_storage_reuse_hits: ?u64 = null,
    specialization_scope: enum { unavailable, corresponding_program_publication } = .unavailable,
    specializations: ?Specializations = null,
    actual_interpreter_fallback_dispatches: ?u64 = null,
    program_route_decision: ?[]const u8 = null,
    owned_program_fallbacks: ?ProgramFallbacks = null,
    fallback_detail_scope: enum { invocation_errors_and_corresponding_program_only } = .invocation_errors_and_corresponding_program_only,
    unobserved_fallback_reasons: enum { unavailable } = .unavailable,
    whole_backend_residency_and_invocation_peak: enum { unavailable } = .unavailable,

    pub fn init(value: anytype) Report {
        var report = Report{ .operation = @tagName(value.operation), .publication_scope = @tagName(value.scope), .backend_counter_scope = @tagName(value.context_runtime_scope), .outcome = @tagName(value.outcome), .backend = if (value.context_backend) |backend| @tagName(backend) else null, .backend_counters = value.runtime.counters, .cuda = value.runtime.cuda, .resolved_project_cuda_launches = value.runtime.resolved_project_cuda_launches, .backend_buffer_memory = value.runtime.memory, .worker_process = value.runtime.process };
        report.resolved_project_metal_dispatches = value.runtime.resolved_project_metal_dispatches;
        report.terminal_native_bodies = value.runtime.terminal_native_bodies;
        report.selected_native_dispatches = value.runtime.selected_native_dispatches;
        report.joined_native_workers = value.runtime.joined_native_workers;
        report.native_quant_worker_callbacks = value.runtime.native_quant_worker_callbacks;
        report.native_attention_programs = value.runtime.native_attention_programs;
        report.native_activation_programs = value.runtime.native_activation_programs;
        report.owned_native_fallbacks = value.runtime.owned_native_fallbacks;
        report.device_allocator_roots = value.runtime.device_allocator_roots;
        report.metal_buffer_interval = value.runtime.metal_buffer_interval;
        report.video_owned_resources = value.runtime.video_owned_resources;
        if (value.pending_interval != null) report.synchronous_interval = .pending_phase_only;
        if (value.error_name) |failure| report.failure = @import("runtime_observation.zig").FailureText.init(failure.view());
        if (value.program_error) |failure| {
            var text = @import("runtime_observation.zig").FailureText.init(failure.view());
            text.truncated = text.truncated or failure.truncated;
            report.program_failure = text;
        }
        if (value.runtime.transfers) |transfers| {
            report.transfers = transfers;
            report.transfers_scope = .invocation_submitting_thread;
        }
        if (value.runtime.submission) |submission| {
            report.implementation_entries = submission.backend_entries;
            report.native_assigned_dispatches = submission.native_assigned_dispatches;
            report.metal_encoded_dispatches = submission.metal_encoded_dispatches;
            report.metal_vendor_encodes = submission.metal_vendor_encodes;
            report.metal_command_submissions = submission.metal_command_submissions;
            report.metal_command_waits = submission.metal_command_waits;
        }
        if (value.runtime.allocation_owners) |owners| {
            report.observed_host_requested_live_bytes = owners.requested_live_bytes;
            report.observed_host_owner_interval_peak_bytes = owners.requested_owner_interval_peak_bytes;
            report.observed_allocator_roots = owners.observed_roots;
            report.observed_allocator_root_identity = owners.aggregate_owner_identity;
            report.host_allocations = owners.allocations;
            report.host_frees = owners.frees;
            report.host_resizes = owners.resizes;
            report.host_remaps = owners.remaps;
            report.maximum_individual_host_owner_lifetime_peak_bytes = owners.maximum_individual_lifetime_peak_bytes;
            report.allocator_coverage_incomplete = owners.owners == 0 or owners.observed_roots != 1 or owners.missing_model_registrations != 0 or owners.missing_compute_registrations != 0 or owners.metadata_failures != 0 or owners.reports_truncated or owners.since_enable_owners != 0;
        }
        if (value.runtime.device_owners) |owners| report.retained_device_owner_count = owners.owners;
        if (value.execution) |program| {
            report.owned_program_fallbacks = ProgramFallbacks.init(program);
            report.program_storage_scope = @tagName(value.execution_scope);
            report.program_storage_reuse_hits = program.physical_storage_reuses;
            report.actual_interpreter_fallback_dispatches = program.interpreter_fallbacks;
            report.program_route_decision = @tagName(program.physical_replay);
            if (program.planned_storage) |owned| {
                report.program_constants = owned.constant;
                report.program_activations = owned.activation;
                report.program_workspace = owned.workspace;
                report.program_host_workspace = owned.host_workspace;
            }
            if (report.transfers == null) {
                report.transfers = .{ .upload_calls = program.actual_upload_calls, .upload_bytes = program.actual_upload_bytes, .download_calls = program.actual_download_calls, .download_bytes = program.actual_download_bytes, .host_accesses = program.device_host_accesses, .host_access_bytes = program.device_host_access_bytes, .index_metadata_host_accesses = program.index_metadata_host_accesses, .index_metadata_host_access_bytes = program.index_metadata_host_access_bytes };
                report.transfers_scope = .last_program_only;
            }
        }
        if (value.specializations) |specializations| {
            report.specializations = specializations;
            report.specialization_scope = .corresponding_program_publication;
        }
        return report;
    }
};

/// Actual owned reason counters already produced by execution. Region/private
/// declines without a reason producer remain unavailable, never inferred.
pub const ProgramFallbacks = struct {
    scope: enum { corresponding_program_publication_only } = .corresponding_program_publication_only,
    physical_route: @import("stats.zig").PhysicalReplayDecision,
    generated_artifact_missing: u64,
    generated_runtime_not_wired: u64,
    unsupported_format: u64,
    unsupported_shape: u64,
    unsupported_epilogue: u64,
    unsupported_backend: u64,
    tensor_core_repack_required: u64,
    unsupported: u64,
    busy_physical_frame: u64,
    pub fn init(program: @import("stats.zig").ExecutionStats) ProgramFallbacks {
        return .{
            .physical_route = program.physical_replay,
            .generated_artifact_missing = program.quant_kernel_fallback_generated_artifact_missing,
            .generated_runtime_not_wired = program.quant_kernel_fallback_generated_runtime_not_wired,
            .unsupported_format = program.quant_kernel_fallback_unsupported_format,
            .unsupported_shape = program.quant_kernel_fallback_unsupported_shape,
            .unsupported_epilogue = program.quant_kernel_fallback_unsupported_epilogue,
            .unsupported_backend = program.quant_kernel_fallback_unsupported_backend,
            .tensor_core_repack_required = program.quant_kernel_fallback_tensor_core_repack_required,
            .unsupported = program.quant_kernel_fallback_unsupported,
            .busy_physical_frame = program.physical_frame_busy_fallbacks,
        };
    }
};

test "uniform report owns actual program fallback reason counters instead of inferring aggregate declines" {
    const std = @import("std");
    var source = @import("diagnostics.zig").Diagnostics{};
    source.configure(true);
    source.publish(.{ .execution = .{ .physical_replay = .unsupported_owner, .quant_kernel_fallback_unsupported_shape = 3, .quant_kernel_fallback_tensor_core_repack_required = 2, .physical_frame_busy_fallbacks = 1 } });
    const retained = source.snapshot().?.report.?.owned_program_fallbacks.?;
    source.configure(false);
    try std.testing.expectEqual(.unsupported_owner, retained.physical_route);
    try std.testing.expectEqual(@as(u64, 3), retained.unsupported_shape);
    try std.testing.expectEqual(@as(u64, 2), retained.tensor_core_repack_required);
    try std.testing.expectEqual(@as(u64, 1), retained.busy_physical_frame);
    try std.testing.expectEqual(@as(u64, 0), retained.unsupported_backend);
}

test "uniform video owned root values survive producer mutation without independent peak sums" {
    const std = @import("std");
    const Value = struct { root_identity: usize = 17, live_bytes: [3]u64 = .{ 30, 20, 10 }, interval_peak_bytes: [3]u64 = .{ 50, 45, 30 }, lifetime_peak_bytes: [3]u64 = .{ 70, 60, 40 }, buffer_allocations: u64 = 3, buffer_frees: u64 = 1, texture_allocations: u64 = 2, texture_frees: u64 = 1, lifetime_metadata_failures: u64 = 1, interval_metadata_failures: u64 = 0 };
    var producer: Value = .{};
    var source = @import("diagnostics.zig").Diagnostics{};
    source.configure(true);
    source.publish(.{ .runtime = .{ .video_owned_resources = runtime.VideoOwnedResources.from(producer) } });
    const retained = source.snapshot().?.report.?.video_owned_resources.?;
    producer.live_bytes = @splat(0);
    source.configure(false);
    try std.testing.expectEqual(@as(u64, 30), retained.live_bytes[0]);
    try std.testing.expectEqual(@as(u64, 50), retained.interval_peak_bytes[0]);
    try std.testing.expectEqual(.partial_metadata_failure, retained.coverage);
    try std.testing.expect(retained.interval_peak_bytes[0] != retained.interval_peak_bytes[1] + retained.interval_peak_bytes[2]);
}
