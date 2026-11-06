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
    /// Exact model-owned cohort metadata. A shared publication cannot be
    /// attributed to an arbitrary single participant or request.
    batch: ?@import("diagnostics.zig").Batch = null,
    /// Copied phase identity and outcome survive pending/model retirement.
    /// Counters still cover this synchronous phase, not the whole lifetime.
    pending_interval: ?@import("diagnostics.zig").PendingInterval = null,
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
    native_definition_templates: ?@import("diagnostics.zig").NativeDefinitionTemplates = null,
    native_program_construction: ?@import("diagnostics.zig").NativeProgramConstruction = null,
    authored_native_programs: ?@import("runtime_observation.zig").NativePrograms = null,
    authored_native_program_bank: ?@import("runtime_observation.zig").NativeProgramBank = null,
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
    /// Authored existing payloads at registration; not final-live, allocation
    /// events, exclusive invocation peaks or an aggregate with opaque heaps.
    preexisting_owned_payloads: ?@import("allocation_registry.zig").OwnedPayloadBaselines = null,
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
    program_storage_allocations: ?u64 = null,
    program_storage_requested_allocation_bytes: ?u64 = null,
    program_storage_counter_scope: enum { unavailable, corresponding_program_physical_frame_publication, latest_entered_native_call_acquire_prepare_run } = .unavailable,
    specialization_scope: enum { unavailable, corresponding_program_publication } = .unavailable,
    specializations: ?Specializations = null,
    actual_interpreter_fallback_dispatches: ?u64 = null,
    program_route_decision: ?[]const u8 = null,
    /// Prepared admission coverage from an actual Frame/entered Native Call.
    /// A compatibility reason is not a numerical fallback or a quota grant.
    payload_admission_coverage: ?PayloadAdmissionCoverage = null,
    metal_region_policy: ?MetalRegionPolicy = null,
    owned_program_fallbacks: ?ProgramFallbacks = null,
    program_output_copies: ?ProgramOutputCopies = null,
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
        report.authored_native_programs = value.runtime.authored_native_programs;
        report.authored_native_program_bank = value.runtime.authored_native_program_bank;
        report.native_definition_templates = value.native_definition_templates;
        report.native_program_construction = value.native_program_construction;
        report.owned_native_fallbacks = value.runtime.owned_native_fallbacks;
        report.device_allocator_roots = value.runtime.device_allocator_roots;
        report.metal_buffer_interval = value.runtime.metal_buffer_interval;
        report.video_owned_resources = value.runtime.video_owned_resources;
        report.batch = value.batch;
        report.pending_interval = value.pending_interval;
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
            if (owners.preexisting_owned_payloads.present()) report.preexisting_owned_payloads = owners.preexisting_owned_payloads;
            report.host_allocations = owners.allocations;
            report.host_frees = owners.frees;
            report.host_resizes = owners.resizes;
            report.host_remaps = owners.remaps;
            report.maximum_individual_host_owner_lifetime_peak_bytes = owners.maximum_individual_lifetime_peak_bytes;
            report.allocator_coverage_incomplete = owners.preexisting_owned_payloads.present() or owners.owners == 0 or owners.observed_roots != 1 or owners.missing_model_registrations != 0 or owners.missing_compute_registrations != 0 or owners.metadata_failures != 0 or owners.reports_truncated or owners.since_enable_owners != 0;
        }
        if (value.runtime.device_owners) |owners| report.retained_device_owner_count = owners.owners;
        if (value.execution) |program| {
            if (program.prepared_payload_coverage) |coverage| report.payload_admission_coverage = .{ .scope = .corresponding_program_last_finished_frame_prepared_metadata, .value = coverage };
            if (program.metal_region_policy_observed) report.metal_region_policy = MetalRegionPolicy.init(program);
            report.owned_program_fallbacks = ProgramFallbacks.init(program);
            report.program_output_copies = .{ .total_owned = program.graph_output_owned_copies, .planned_literal = program.planned_literal_output_copies, .opaque_metal_graph_output = program.opaque_graph_output_copies };
            report.program_storage_scope = @tagName(value.execution_scope);
            report.program_storage_reuse_hits = program.physical_storage_reuses;
            report.program_storage_allocations = program.physical_storage_allocations;
            report.program_storage_requested_allocation_bytes = program.physical_storage_bytes;
            report.program_storage_counter_scope = .corresponding_program_physical_frame_publication;
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
        // An execution publication retains its original precedence. The
        // Native sample remains separately visible if both producers exist.
        if (value.execution == null) if (report.authored_native_program_bank) |bank| {
            if (bank.prepared_payload_coverage) |coverage| report.payload_admission_coverage = .{ .scope = .latest_entered_native_call_prepared_metadata, .value = coverage };
            report.program_storage_reuse_hits = bank.frame_reuse_accesses;
            report.program_storage_allocations = bank.frame_allocations;
            report.program_storage_requested_allocation_bytes = bank.frame_requested_allocation_bytes;
            report.program_storage_counter_scope = .latest_entered_native_call_acquire_prepare_run;
            if (bank.owned_ledger) |owned| {
                report.program_storage_scope = @tagName(bank.ledger_scope);
                report.program_constants = owned.constant;
                report.program_activations = owned.activation;
                report.program_workspace = owned.workspace;
                report.program_host_workspace = owned.host_workspace;
            }
        };
        if (value.specializations) |specializations| {
            report.specializations = specializations;
            report.specialization_scope = .corresponding_program_publication;
        }
        return report;
    }
};

/// Counts from the corresponding published program's two known output-copy
/// producers. Unobserved backend/vendor/internal copies are outside this scope.
pub const ProgramOutputCopies = struct {
    scope: enum { corresponding_program_known_output_copy_producers_only } = .corresponding_program_known_output_copy_producers_only,
    total_owned: u64,
    planned_literal: u64,
    opaque_metal_graph_output: u64,
};

/// Inline value only; copied coverage never borrows a Pool or graph and never
/// substitutes for the actual current admission/source capability check.
pub const PayloadAdmissionCoverage = struct {
    scope: enum { corresponding_program_last_finished_frame_prepared_metadata, latest_entered_native_call_prepared_metadata },
    value: @import("owned_coverage.zig").Coverage,
};

/// Existing instrumented planner/dispatch counters from the corresponding
/// program. Null until the real Metal planner runs, even on a Metal backend.
/// Broad optional-region declines retain their original aggregate meaning;
/// no missing internal reason is inferred from an attempted callback.
pub const MetalRegionPolicy = struct {
    scope: enum { corresponding_program_instrumented_metal_region_planner } = .corresponding_program_instrumented_metal_region_planner,
    active_regions: u64,
    covered_nodes: u64,
    elided_nodes: u64,
    pre_skipped_nodes: u64,
    external_consumer_pre_skip_declines: u64,
    optional_region_preparation_declines: u64,
    graph_pattern_fallbacks: u64,
    frame_candidates: u64,
    frame_eligible: u64,
    frame_metadata_ready: u64,
    no_regions: u64,
    missing_qkv: u64,
    missing_attention: u64,
    missing_ffn: u64,
    missing_ple: u64,
    single_row: u64,
    non_layer_order: u64,
    shape_mismatch: u64,
    missing_model_metadata: u64,
    pub fn init(program: @import("stats.zig").ExecutionStats) MetalRegionPolicy {
        return .{
            .active_regions = program.runtime_region_plan_active_regions,
            .covered_nodes = program.runtime_region_plan_covered_nodes,
            .elided_nodes = program.runtime_region_plan_elided_nodes,
            .pre_skipped_nodes = program.runtime_region_pre_skipped_nodes,
            .external_consumer_pre_skip_declines = program.runtime_region_pre_skip_declined_external_consumers,
            .optional_region_preparation_declines = program.runtime_region_fallbacks,
            .graph_pattern_fallbacks = program.graph_region_fallbacks,
            .frame_candidates = program.runtime_frame_candidates,
            .frame_eligible = program.runtime_frame_eligible,
            .frame_metadata_ready = program.runtime_frame_metadata_ready,
            .no_regions = program.runtime_frame_ineligible_no_regions,
            .missing_qkv = program.runtime_frame_ineligible_missing_qkv,
            .missing_attention = program.runtime_frame_ineligible_missing_attention,
            .missing_ffn = program.runtime_frame_ineligible_missing_ffn,
            .missing_ple = program.runtime_frame_ineligible_missing_ple,
            .single_row = program.runtime_frame_ineligible_single_row,
            .non_layer_order = program.runtime_frame_ineligible_non_layer_order,
            .shape_mismatch = program.runtime_frame_ineligible_shape_mismatch,
            .missing_model_metadata = program.runtime_frame_ineligible_missing_model_metadata,
        };
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

test "uniform report separates planned literal publication from opaque Metal output copies" {
    const std = @import("std");
    var source = @import("diagnostics.zig").Diagnostics{};
    source.configure(true);
    source.publish(.{});
    try std.testing.expect(source.snapshot().?.report.?.program_output_copies == null);
    source.publish(.{ .execution = .{ .graph_output_owned_copies = 3, .planned_literal_output_copies = 2, .opaque_graph_output_copies = 1 } });
    const retained = source.snapshot().?.report.?.program_output_copies.?;
    source.configure(false);
    try std.testing.expect(source.snapshot() == null);
    try std.testing.expectEqual(@as(u64, 3), retained.total_owned);
    try std.testing.expectEqual(@as(u64, 2), retained.planned_literal);
    try std.testing.expectEqual(@as(u64, 1), retained.opaque_metal_graph_output);
    try std.testing.expectEqual(retained.total_owned, retained.planned_literal + retained.opaque_metal_graph_output);
}

test "uniform Native Call bank report preserves unavailable classes and execution publication precedence" {
    const std = @import("std");
    var source = @import("diagnostics.zig").Diagnostics{};
    source.configure(true);
    const bank = @import("runtime_observation.zig").NativeProgramBank{
        .frame_allocations = 2,
        .frame_requested_allocation_bytes = 64,
        .frame_reuse_accesses = 3,
        .owned_ledger = null,
    };
    source.publish(.{ .runtime = .{ .authored_native_program_bank = bank } });
    const unavailable = source.snapshot().?.report.?;
    try std.testing.expect(unavailable.program_constants == null and unavailable.program_activations == null);
    try std.testing.expect(unavailable.program_workspace == null and unavailable.program_host_workspace == null);
    try std.testing.expectEqualStrings("unavailable", unavailable.program_storage_scope);
    try std.testing.expectEqual(@as(u64, 64), unavailable.program_storage_requested_allocation_bytes.?);
    source.publish(.{ .runtime = .{ .authored_native_program_bank = bank }, .execution = .{ .physical_storage_allocations = 7, .physical_storage_bytes = 128, .physical_storage_reuses = 9 } });
    const simultaneous = source.snapshot().?.report.?;
    try std.testing.expectEqual(@as(u64, 7), simultaneous.program_storage_allocations.?);
    try std.testing.expectEqual(@as(u64, 128), simultaneous.program_storage_requested_allocation_bytes.?);
    try std.testing.expectEqual(@as(u64, 9), simultaneous.program_storage_reuse_hits.?);
    try std.testing.expectEqualDeep(bank, simultaneous.authored_native_program_bank.?);
    const interval = (runtime.Snapshot{ .authored_native_program_bank = bank }).since(.{});
    try std.testing.expectEqualDeep(bank, interval.authored_native_program_bank.?);
    source.configure(false);
    try std.testing.expect(source.snapshot() == null);
    try std.testing.expectEqualDeep(bank, unavailable.authored_native_program_bank.?);
}

test "coverage publication preserves absence and corresponding program precedence without inventing quota" {
    const std = @import("std");
    const D = @import("diagnostics.zig");
    var diagnostics = D.Diagnostics{};
    diagnostics.configure(true);
    diagnostics.publish(.{ .context_backend = .metal, .execution = .{} });
    const unavailable = diagnostics.snapshot().?.report.?;
    try std.testing.expect(unavailable.payload_admission_coverage == null);
    try std.testing.expect(unavailable.metal_region_policy == null);
    const bank = @import("runtime_observation.zig").NativeProgramBank{ .prepared_payload_coverage = .{ .complete = .{ .epoch = 19 } }, .frame_allocations = 0, .frame_requested_allocation_bytes = 0, .frame_reuse_accesses = 0, .owned_ledger = null };
    diagnostics.publish(.{ .execution = .{ .prepared_payload_coverage = .{ .compatibility = .alias } }, .runtime = .{ .authored_native_program_bank = bank } });
    const program = diagnostics.snapshot().?.report.?;
    try std.testing.expectEqual(.corresponding_program_last_finished_frame_prepared_metadata, program.payload_admission_coverage.?.scope);
    try std.testing.expectEqual(@import("owned_coverage.zig").Reason.alias, program.payload_admission_coverage.?.value.compatibility);
    diagnostics.publish(.{ .execution = .{}, .runtime = .{ .authored_native_program_bank = bank } });
    try std.testing.expect(diagnostics.snapshot().?.report.?.payload_admission_coverage == null);
    diagnostics.publish(.{ .runtime = .{ .authored_native_program_bank = bank } });
    const native = diagnostics.snapshot().?.report.?;
    diagnostics.configure(false);
    try std.testing.expectEqual(.latest_entered_native_call_prepared_metadata, native.payload_admission_coverage.?.scope);
    try std.testing.expectEqual(@as(u64, 19), native.payload_admission_coverage.?.value.complete.epoch);
    try std.testing.expectEqual(@import("owned_coverage.zig").Reason.alias, program.payload_admission_coverage.?.value.compatibility);
}
