// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly = @import("antfly-zig");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const graph_query_mod = antfly.graph_query;

const HarnessProfile = enum {
    smoke,
    promotion,
};

const RuntimeRole = enum {
    combined,
    coordinator,
    worker,
    worker_pool,
};

const ChildRuntimeTelemetry = struct {
    role: RuntimeRole = .combined,
    runtime_id_hash: u64 = 0,
    owner_id_hash: u64 = 0,
    lease_key_hash: u64 = 0,
    worker_id_hash: u64 = 0,
    worker_count: usize = 0,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
    lost_leases: u64 = 0,
    ticks_started: u64 = 0,
    ticks_completed: u64 = 0,
    idle_ticks: u64 = 0,
    error_ticks: u64 = 0,
    has_last_error: bool = false,
};

const ChildRunSummary = struct {
    telemetry: ?ChildRuntimeTelemetry = null,
};

const SupervisorSummary = struct {
    rounds_executed: usize = 0,
    exit_reason: []const u8 = "",
    succeeded: bool = false,
    coordinator: ChildRunSummary = .{},
    worker_pool: ChildRunSummary = .{},
};

const RuntimeStats = struct {
    role: RuntimeRole = .combined,
    runtime_id_hash: u64 = 0,
    owner_id_hash: u64 = 0,
    lease_key_hash: u64 = 0,
    worker_id_hash: u64 = 0,
    worker_count: usize = 0,
    lease_owned: bool = false,
    has_lease: bool = false,
    acquisition_count: u64 = 0,
    takeover_count: u64 = 0,
    lease_acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    ticks_started: u64 = 0,
    ticks_completed: u64 = 0,
    durable_progress_ticks: u64 = 0,
    idle_ticks: u64 = 0,
    error_ticks: u64 = 0,
};

const SchedulerResult = struct {
    pages_claimed: usize = 0,
    pages_completed: usize = 0,
    phases_advanced: usize = 0,
    published: usize = 0,
    failed_builds: usize = 0,
};

const RoleRunSummary = struct {
    durable_progressed: bool = false,
    result: SchedulerResult = .{},
    stats: RuntimeStats = .{},
};

const PageLeaseSnapshot = struct {
    job_id: u64 = 0,
    page_id: u64 = 0,
    iteration: u32 = 0,
    attempt: u64 = 0,
    lease_expires_at_ms: u64 = 0,
    total_units: u64 = 0,
};

const ProcessHarnessReleaseSummary = struct {
    launch_families: usize = 0,
    service_owner_restart_families: usize = 0,
    service_publish_cleanup_families: usize = 0,
    service_publish_failure_families: usize = 0,
    service_multipage_worker_pool_families: usize = 0,
    service_multipage_coordinator_takeover_families: usize = 0,
    service_multipage_worker_pool_takeover_families: usize = 0,
    service_multipage_worker_phase_proofs: usize = 0,
    service_multipage_coordinator_phase_proofs: usize = 0,
    service_multipage_takeover_phase_proofs: usize = 0,
    service_cleanup_takeover_families: usize = 0,
    service_active_public_read_families: usize = 0,
    direct_publish_cleanup_families: usize = 0,
    direct_publish_failure_families: usize = 0,
    direct_active_public_read_families: usize = 0,
    direct_page_reclaim_phase_proofs: usize = 0,
    direct_reclaimed_attempt_completion_phase_proofs: usize = 0,
    direct_stale_attempt_rejection_phase_proofs: usize = 0,
    fixed_iteration_families: usize = 0,
    exhausted_attempt_families: usize = 0,
    same_worker_fencing_proofs: usize = 0,
};

const required_process_harness_release_summary = ProcessHarnessReleaseSummary{
    .launch_families = 4,
    .service_owner_restart_families = 4,
    .service_publish_cleanup_families = 4,
    .service_publish_failure_families = 3,
    .service_multipage_worker_pool_families = 4,
    .service_multipage_coordinator_takeover_families = 4,
    .service_multipage_worker_pool_takeover_families = 4,
    .service_multipage_worker_phase_proofs = 27,
    .service_multipage_coordinator_phase_proofs = 31,
    .service_multipage_takeover_phase_proofs = 8,
    .service_cleanup_takeover_families = 4,
    .service_active_public_read_families = 4,
    .direct_publish_cleanup_families = 4,
    .direct_publish_failure_families = 3,
    .direct_active_public_read_families = 4,
    .direct_page_reclaim_phase_proofs = 20,
    .direct_reclaimed_attempt_completion_phase_proofs = 20,
    .direct_stale_attempt_rejection_phase_proofs = 20,
    .fixed_iteration_families = 3,
    .exhausted_attempt_families = 3,
    .same_worker_fencing_proofs = 2,
};

fn recordDirectPageReclaimProof(summary: *ProcessHarnessReleaseSummary) void {
    summary.direct_page_reclaim_phase_proofs += 1;
    summary.direct_reclaimed_attempt_completion_phase_proofs += 1;
    summary.direct_stale_attempt_rejection_phase_proofs += 1;
}

fn recordServiceMultipagePhaseProofs(
    summary: *ProcessHarnessReleaseSummary,
    worker_phase_proofs: usize,
    coordinator_phase_proofs: usize,
    takeover_phase_proofs: usize,
) void {
    summary.service_multipage_worker_phase_proofs += worker_phase_proofs;
    summary.service_multipage_coordinator_phase_proofs += coordinator_phase_proofs;
    summary.service_multipage_takeover_phase_proofs += takeover_phase_proofs;
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const argv = try init.minimal.args.toSlice(arena);
    if (argv.len >= 2 and std.mem.eql(u8, argv[1], "claim-degree-page-hold")) {
        try runClaimDegreePageHoldMode(alloc, init.io, argv);
        return;
    }
    if (argv.len >= 2 and std.mem.eql(u8, argv[1], "claim-metric-page-hold")) {
        try runClaimMetricPageHoldMode(alloc, init.io, argv);
        return;
    }
    if (!(argv.len == 2 or (argv.len == 4 and std.mem.eql(u8, argv[2], "--profile")))) {
        std.debug.print("usage: graph_metric_process_harness <antfly-executable> [--profile smoke|promotion]\n", .{});
        std.debug.print("       graph_metric_process_harness claim-degree-page-hold <db-path> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
        std.debug.print("       graph_metric_process_harness claim-metric-page-hold <db-path> <metric-name> <phase> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
        std.process.exit(2);
    }
    const harness_exe = argv[0];
    const antfly_exe = argv[1];
    const profile = if (argv.len == 4) try parseHarnessProfile(argv[3]) else HarnessProfile.promotion;
    var release_summary: ProcessHarnessReleaseSummary = .{};

    try verifyRoleProcessArgvPreflightSelfTest();

    const supervisor_db_path = ".zig-cache/tmp/graph-metric-process-degree-db";
    std.Io.Dir.cwd().deleteTree(init.io, supervisor_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, supervisor_db_path) catch {};

    const launch_db_path = ".zig-cache/tmp/graph-metric-process-launch-degree-db";
    std.Io.Dir.cwd().deleteTree(init.io, launch_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, launch_db_path) catch {};

    const pagerank_launch_db_path = ".zig-cache/tmp/graph-metric-process-launch-pagerank-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_launch_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_launch_db_path) catch {};

    const eigenvector_launch_db_path = ".zig-cache/tmp/graph-metric-process-launch-eigenvector-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_launch_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_launch_db_path) catch {};

    const hits_launch_db_path = ".zig-cache/tmp/graph-metric-process-launch-hits-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_launch_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_launch_db_path) catch {};

    const lease_db_path = ".zig-cache/tmp/graph-metric-process-lease-db";
    std.Io.Dir.cwd().deleteTree(init.io, lease_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, lease_db_path) catch {};

    const service_owner_db_path = ".zig-cache/tmp/graph-metric-process-service-owner-db";
    std.Io.Dir.cwd().deleteTree(init.io, service_owner_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, service_owner_db_path) catch {};

    const degree_service_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-degree-service-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, degree_service_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, degree_service_publish_cleanup_db_path) catch {};

    const degree_service_multipage_db_path = ".zig-cache/tmp/graph-metric-process-degree-service-multipage-db";
    std.Io.Dir.cwd().deleteTree(init.io, degree_service_multipage_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, degree_service_multipage_db_path) catch {};

    const pagerank_service_owner_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-service-owner-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_owner_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_owner_db_path) catch {};

    const pagerank_service_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-service-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_publish_cleanup_db_path) catch {};

    const pagerank_service_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-service-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_publish_failure_db_path) catch {};

    const pagerank_service_multipage_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-service-multipage-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_multipage_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_multipage_db_path) catch {};

    const eigenvector_service_owner_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-service-owner-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_owner_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_owner_db_path) catch {};

    const eigenvector_service_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-service-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_publish_cleanup_db_path) catch {};

    const eigenvector_service_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-service-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_publish_failure_db_path) catch {};

    const eigenvector_service_multipage_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-service-multipage-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_multipage_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_multipage_db_path) catch {};

    const hits_service_owner_db_path = ".zig-cache/tmp/graph-metric-process-hits-service-owner-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_service_owner_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_service_owner_db_path) catch {};

    const hits_service_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-hits-service-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_service_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_service_publish_cleanup_db_path) catch {};

    const hits_service_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-hits-service-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_service_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_service_publish_failure_db_path) catch {};

    const hits_service_multipage_db_path = ".zig-cache/tmp/graph-metric-process-hits-service-multipage-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_service_multipage_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_service_multipage_db_path) catch {};

    const degree_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-degree-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, degree_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, degree_active_public_read_db_path) catch {};

    const pagerank_service_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-service-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_service_active_public_read_db_path) catch {};

    const degree_service_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-degree-service-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, degree_service_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, degree_service_active_public_read_db_path) catch {};

    const worker_page_db_path = ".zig-cache/tmp/graph-metric-process-worker-page-db";
    std.Io.Dir.cwd().deleteTree(init.io, worker_page_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, worker_page_db_path) catch {};

    const worker_runtime_db_path = ".zig-cache/tmp/graph-metric-process-worker-runtime-db";
    std.Io.Dir.cwd().deleteTree(init.io, worker_runtime_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, worker_runtime_db_path) catch {};

    const publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, publish_cleanup_db_path) catch {};

    const pagerank_scan_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-scan-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_scan_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_scan_db_path) catch {};

    const pagerank_initialize_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-initialize-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_initialize_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_initialize_db_path) catch {};

    const pagerank_contribution_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-contribution-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_contribution_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_contribution_db_path) catch {};

    const pagerank_reduce_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-reduce-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_reduce_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_reduce_db_path) catch {};

    const pagerank_convergence_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-convergence-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_convergence_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_convergence_db_path) catch {};

    const pagerank_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_publish_cleanup_db_path) catch {};

    const pagerank_cleanup_reclaim_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-cleanup-reclaim-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_cleanup_reclaim_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_cleanup_reclaim_db_path) catch {};

    const pagerank_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_publish_failure_db_path) catch {};

    const pagerank_fixed_iteration_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-fixed-iteration-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_fixed_iteration_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_fixed_iteration_db_path) catch {};

    const pagerank_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_active_public_read_db_path) catch {};

    const pagerank_same_worker_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-same-worker-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_same_worker_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_same_worker_db_path) catch {};

    const pagerank_later_contribution_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-later-contribution-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_contribution_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_contribution_db_path) catch {};

    const pagerank_later_reduce_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-later-reduce-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_reduce_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_reduce_db_path) catch {};

    const pagerank_later_convergence_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-later-convergence-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_convergence_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_later_convergence_db_path) catch {};

    const pagerank_exhausted_attempt_db_path = ".zig-cache/tmp/graph-metric-process-pagerank-exhausted-attempt-db";
    std.Io.Dir.cwd().deleteTree(init.io, pagerank_exhausted_attempt_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, pagerank_exhausted_attempt_db_path) catch {};

    const eigenvector_supervisor_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-supervisor-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_supervisor_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_supervisor_db_path) catch {};

    const eigenvector_fixed_iteration_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-fixed-iteration-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_fixed_iteration_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_fixed_iteration_db_path) catch {};

    const hits_fixed_iteration_db_path = ".zig-cache/tmp/graph-metric-process-hits-fixed-iteration-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_fixed_iteration_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_fixed_iteration_db_path) catch {};

    const eigenvector_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_publish_cleanup_db_path) catch {};

    const eigenvector_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_publish_failure_db_path) catch {};

    const eigenvector_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_active_public_read_db_path) catch {};

    const eigenvector_service_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-service-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_service_active_public_read_db_path) catch {};

    const eigenvector_scan_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-scan-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_scan_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_scan_db_path) catch {};

    const eigenvector_initialize_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-initialize-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_initialize_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_initialize_db_path) catch {};

    const eigenvector_contribution_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-contribution-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_contribution_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_contribution_db_path) catch {};

    const eigenvector_reduce_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-reduce-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_reduce_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_reduce_db_path) catch {};

    const eigenvector_convergence_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-convergence-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_convergence_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_convergence_db_path) catch {};

    const eigenvector_exhausted_attempt_db_path = ".zig-cache/tmp/graph-metric-process-eigenvector-exhausted-attempt-db";
    std.Io.Dir.cwd().deleteTree(init.io, eigenvector_exhausted_attempt_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, eigenvector_exhausted_attempt_db_path) catch {};

    const hits_supervisor_db_path = ".zig-cache/tmp/graph-metric-process-hits-supervisor-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_supervisor_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_supervisor_db_path) catch {};

    const hits_publish_cleanup_db_path = ".zig-cache/tmp/graph-metric-process-hits-publish-cleanup-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_publish_cleanup_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_publish_cleanup_db_path) catch {};

    const hits_publish_failure_db_path = ".zig-cache/tmp/graph-metric-process-hits-publish-failure-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_publish_failure_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_publish_failure_db_path) catch {};

    const hits_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-hits-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_active_public_read_db_path) catch {};

    const hits_service_active_public_read_db_path = ".zig-cache/tmp/graph-metric-process-hits-service-active-public-read-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_service_active_public_read_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_service_active_public_read_db_path) catch {};

    const hits_exhausted_attempt_db_path = ".zig-cache/tmp/graph-metric-process-hits-exhausted-attempt-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_exhausted_attempt_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_exhausted_attempt_db_path) catch {};

    const hits_authority_contribution_db_path = ".zig-cache/tmp/graph-metric-process-hits-authority-contribution-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_authority_contribution_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_authority_contribution_db_path) catch {};

    const hits_authority_reduce_db_path = ".zig-cache/tmp/graph-metric-process-hits-authority-reduce-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_authority_reduce_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_authority_reduce_db_path) catch {};

    const hits_convergence_db_path = ".zig-cache/tmp/graph-metric-process-hits-convergence-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_convergence_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_convergence_db_path) catch {};

    const hits_hub_contribution_db_path = ".zig-cache/tmp/graph-metric-process-hits-hub-contribution-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_hub_contribution_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_hub_contribution_db_path) catch {};

    const hits_hub_reduce_db_path = ".zig-cache/tmp/graph-metric-process-hits-hub-reduce-db";
    std.Io.Dir.cwd().deleteTree(init.io, hits_hub_reduce_db_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(init.io, hits_hub_reduce_db_path) catch {};

    const target_generation = try seedDegreeDb(alloc, supervisor_db_path);
    try runSupervisorProcess(alloc, init.io, antfly_exe, supervisor_db_path);
    try verifyDegreeFresh(alloc, supervisor_db_path, target_generation);

    const launch_target_generation = try seedDegreeDb(alloc, launch_db_path);
    try runLaunchProcess(alloc, init.io, antfly_exe, launch_db_path, "degree");
    try verifyDegreeFresh(alloc, launch_db_path, launch_target_generation);
    release_summary.launch_families += 1;

    const pagerank_launch_target_generation = try seedPageRankDb(alloc, pagerank_launch_db_path);
    try runLaunchProcess(alloc, init.io, antfly_exe, pagerank_launch_db_path, "pagerank");
    try verifyMetricFresh(alloc, pagerank_launch_db_path, "pagerank", pagerank_launch_target_generation);
    release_summary.launch_families += 1;

    const eigenvector_launch_target_generation = try seedEigenvectorDb(alloc, eigenvector_launch_db_path);
    try runLaunchProcess(alloc, init.io, antfly_exe, eigenvector_launch_db_path, "eigenvector");
    try verifyMetricFresh(alloc, eigenvector_launch_db_path, "eigenvector", eigenvector_launch_target_generation);
    release_summary.launch_families += 1;

    const hits_launch_target_generation = try seedHitsDbWithSources(alloc, hits_launch_db_path, 8);
    try runLaunchProcess(alloc, init.io, antfly_exe, hits_launch_db_path, "hits");
    try verifyHitsFresh(alloc, hits_launch_db_path, hits_launch_target_generation);
    release_summary.launch_families += 1;

    _ = try seedDegreeDb(alloc, lease_db_path);
    try verifyCoordinatorLeaseExpiryTakeover(alloc, init.io, antfly_exe, lease_db_path);

    const service_owner_target_generation = try seedDegreeDb(alloc, service_owner_db_path);
    try verifyServiceTargetedMetricOwnerRestartProcess(alloc, init.io, antfly_exe, service_owner_db_path, "degree", service_owner_target_generation);
    try verifyDegreeFresh(alloc, service_owner_db_path, service_owner_target_generation);
    release_summary.service_owner_restart_families += 1;

    const degree_service_publish_cleanup_target_generation = try seedDegreeDb(alloc, degree_service_publish_cleanup_db_path);
    try verifyDegreeServiceTargetedPublishAndCleanupRestartProcess(
        alloc,
        init.io,
        antfly_exe,
        degree_service_publish_cleanup_db_path,
        degree_service_publish_cleanup_target_generation,
    );
    release_summary.service_publish_cleanup_families += 1;
    release_summary.service_cleanup_takeover_families += 1;

    const degree_service_multipage_target_generation = try seedDegreeDbWithSources(alloc, degree_service_multipage_db_path, 130);
    try verifyDegreeServiceTargetedMultiPageWorkerPoolProcess(
        alloc,
        init.io,
        antfly_exe,
        degree_service_multipage_db_path,
        degree_service_multipage_target_generation,
    );
    release_summary.service_multipage_worker_pool_families += 1;
    release_summary.service_multipage_coordinator_takeover_families += 1;
    release_summary.service_multipage_worker_pool_takeover_families += 1;
    recordServiceMultipagePhaseProofs(&release_summary, 4, 5, 2);

    const pagerank_service_owner_target_generation = try seedPageRankDb(alloc, pagerank_service_owner_db_path);
    try verifyServiceTargetedMetricOwnerRestartProcess(alloc, init.io, antfly_exe, pagerank_service_owner_db_path, "pagerank", pagerank_service_owner_target_generation);
    try verifyMetricFresh(alloc, pagerank_service_owner_db_path, "pagerank", pagerank_service_owner_target_generation);
    release_summary.service_owner_restart_families += 1;

    const pagerank_service_publish_cleanup_target_generation = try seedPageRankDb(alloc, pagerank_service_publish_cleanup_db_path);
    try verifyPageRankServiceTargetedPublishAndCleanupRestartProcess(
        alloc,
        init.io,
        antfly_exe,
        pagerank_service_publish_cleanup_db_path,
        pagerank_service_publish_cleanup_target_generation,
    );
    release_summary.service_publish_cleanup_families += 1;
    release_summary.service_cleanup_takeover_families += 1;

    const pagerank_service_publish_failure_initial_generation = try seedPageRankDb(alloc, pagerank_service_publish_failure_db_path);
    try verifyPageRankServiceTargetedPublishVerifierFailureProcess(
        alloc,
        init.io,
        antfly_exe,
        pagerank_service_publish_failure_db_path,
        pagerank_service_publish_failure_initial_generation,
    );
    release_summary.service_publish_failure_families += 1;

    const pagerank_service_multipage_target_generation = try seedPageRankDbWithSources(alloc, pagerank_service_multipage_db_path, 130);
    try verifyPageRankServiceTargetedMultiPageWorkerPoolProcess(
        alloc,
        init.io,
        antfly_exe,
        pagerank_service_multipage_db_path,
        pagerank_service_multipage_target_generation,
    );
    release_summary.service_multipage_worker_pool_families += 1;
    release_summary.service_multipage_coordinator_takeover_families += 1;
    release_summary.service_multipage_worker_pool_takeover_families += 1;
    recordServiceMultipagePhaseProofs(&release_summary, 7, 8, 2);

    const eigenvector_service_owner_target_generation = try seedEigenvectorDb(alloc, eigenvector_service_owner_db_path);
    try verifyServiceTargetedMetricOwnerRestartProcess(alloc, init.io, antfly_exe, eigenvector_service_owner_db_path, "eigenvector", eigenvector_service_owner_target_generation);
    try verifyMetricFresh(alloc, eigenvector_service_owner_db_path, "eigenvector", eigenvector_service_owner_target_generation);
    release_summary.service_owner_restart_families += 1;

    const eigenvector_service_publish_cleanup_target_generation = try seedEigenvectorDbWithSources(alloc, eigenvector_service_publish_cleanup_db_path, 130);
    try verifyEigenvectorServiceTargetedPublishAndCleanupRestartProcess(
        alloc,
        init.io,
        antfly_exe,
        eigenvector_service_publish_cleanup_db_path,
        eigenvector_service_publish_cleanup_target_generation,
    );
    release_summary.service_publish_cleanup_families += 1;
    release_summary.service_cleanup_takeover_families += 1;

    const eigenvector_service_publish_failure_initial_generation = try seedEigenvectorDb(alloc, eigenvector_service_publish_failure_db_path);
    try verifyEigenvectorServiceTargetedPublishVerifierFailureProcess(
        alloc,
        init.io,
        antfly_exe,
        eigenvector_service_publish_failure_db_path,
        eigenvector_service_publish_failure_initial_generation,
    );
    release_summary.service_publish_failure_families += 1;

    const eigenvector_service_multipage_target_generation = try seedEigenvectorDbWithSources(alloc, eigenvector_service_multipage_db_path, 130);
    try verifyEigenvectorServiceTargetedMultiPageWorkerPoolProcess(
        alloc,
        init.io,
        antfly_exe,
        eigenvector_service_multipage_db_path,
        eigenvector_service_multipage_target_generation,
    );
    release_summary.service_multipage_worker_pool_families += 1;
    release_summary.service_multipage_coordinator_takeover_families += 1;
    release_summary.service_multipage_worker_pool_takeover_families += 1;
    recordServiceMultipagePhaseProofs(&release_summary, 7, 8, 2);

    const hits_service_owner_target_generation = try seedHitsBackgroundDb(alloc, hits_service_owner_db_path);
    try verifyServiceTargetedMetricOwnerRestartProcess(alloc, init.io, antfly_exe, hits_service_owner_db_path, "hits_authority", hits_service_owner_target_generation);
    try verifyHitsFresh(alloc, hits_service_owner_db_path, hits_service_owner_target_generation);
    release_summary.service_owner_restart_families += 1;

    const hits_service_publish_cleanup_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_service_publish_cleanup_db_path);
    try verifyHitsServiceTargetedPublishAndCleanupRestartProcess(
        alloc,
        init.io,
        antfly_exe,
        hits_service_publish_cleanup_db_path,
        hits_service_publish_cleanup_target_generation,
    );
    release_summary.service_publish_cleanup_families += 1;
    release_summary.service_cleanup_takeover_families += 1;

    const hits_service_publish_failure_initial_generation = try seedHitsDbWithActiveBuild(alloc, hits_service_publish_failure_db_path);
    try verifyHitsServiceTargetedPublishVerifierFailureProcess(
        alloc,
        init.io,
        antfly_exe,
        hits_service_publish_failure_db_path,
        hits_service_publish_failure_initial_generation,
    );
    release_summary.service_publish_failure_families += 1;

    const hits_service_multipage_target_generation = try seedHitsDbWithSources(alloc, hits_service_multipage_db_path, 130);
    try verifyHitsServiceTargetedMultiPageWorkerPoolProcess(
        alloc,
        init.io,
        antfly_exe,
        hits_service_multipage_db_path,
        hits_service_multipage_target_generation,
    );
    release_summary.service_multipage_worker_pool_families += 1;
    release_summary.service_multipage_coordinator_takeover_families += 1;
    release_summary.service_multipage_worker_pool_takeover_families += 1;
    recordServiceMultipagePhaseProofs(&release_summary, 9, 10, 2);

    const degree_active_public_read_initial_generation = try seedDegreeSearchDb(alloc, degree_active_public_read_db_path);
    try verifyDegreeActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        degree_active_public_read_db_path,
        degree_active_public_read_initial_generation,
    );
    release_summary.direct_active_public_read_families += 1;

    const degree_service_active_public_read_initial_generation = try seedDegreeSearchDb(alloc, degree_service_active_public_read_db_path);
    try verifyDegreeServiceActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        degree_service_active_public_read_db_path,
        degree_service_active_public_read_initial_generation,
    );
    release_summary.service_active_public_read_families += 1;

    const pagerank_service_active_public_read_initial_generation = try seedPageRankSearchDb(alloc, pagerank_service_active_public_read_db_path);
    try verifyPageRankServiceActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        pagerank_service_active_public_read_db_path,
        pagerank_service_active_public_read_initial_generation,
    );
    release_summary.service_active_public_read_families += 1;

    const worker_page_target_generation = try seedDegreeDb(alloc, worker_page_db_path);
    try verifyWorkerPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, worker_page_db_path, worker_page_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const worker_runtime_target_generation = try seedDegreeDbWithSources(alloc, worker_runtime_db_path, 130);
    try verifyWorkerRuntimeSameWorkerLeaseFencing(alloc, init.io, antfly_exe, worker_runtime_db_path, worker_runtime_target_generation);
    release_summary.same_worker_fencing_proofs += 1;

    const publish_cleanup_target_generation = try seedDegreeDb(alloc, publish_cleanup_db_path);
    try verifyPublishAndCleanupRestart(alloc, init.io, antfly_exe, publish_cleanup_db_path, publish_cleanup_target_generation);
    release_summary.direct_publish_cleanup_families += 1;

    const pagerank_scan_target_generation = try seedPageRankDb(alloc, pagerank_scan_db_path);
    try verifyPageRankScanPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_scan_db_path, pagerank_scan_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_initialize_target_generation = try seedPageRankDb(alloc, pagerank_initialize_db_path);
    try verifyPageRankInitializePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_initialize_db_path, pagerank_initialize_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_contribution_target_generation = try seedPageRankDb(alloc, pagerank_contribution_db_path);
    try verifyPageRankContributionPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_contribution_db_path, pagerank_contribution_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_reduce_target_generation = try seedPageRankDb(alloc, pagerank_reduce_db_path);
    try verifyPageRankReducePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_reduce_db_path, pagerank_reduce_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_convergence_target_generation = try seedPageRankDb(alloc, pagerank_convergence_db_path);
    try verifyPageRankConvergencePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_convergence_db_path, pagerank_convergence_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_publish_cleanup_target_generation = try seedPageRankDb(alloc, pagerank_publish_cleanup_db_path);
    try verifyPageRankPublishAndCleanupRestart(alloc, init.io, antfly_exe, pagerank_publish_cleanup_db_path, pagerank_publish_cleanup_target_generation);
    release_summary.direct_publish_cleanup_families += 1;

    const pagerank_cleanup_reclaim_target_generation = try seedPageRankDb(alloc, pagerank_cleanup_reclaim_db_path);
    try verifyPageRankCleanupPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_cleanup_reclaim_db_path, pagerank_cleanup_reclaim_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_publish_failure_initial_generation = try seedPageRankDb(alloc, pagerank_publish_failure_db_path);
    try verifyPageRankPublishVerifierFailureProcess(alloc, init.io, antfly_exe, pagerank_publish_failure_db_path, pagerank_publish_failure_initial_generation);
    release_summary.direct_publish_failure_families += 1;

    const pagerank_fixed_iteration_target_generation = try seedPageRankDbWithMaxIterations(alloc, pagerank_fixed_iteration_db_path, 2);
    try runSupervisorProcess(alloc, init.io, antfly_exe, pagerank_fixed_iteration_db_path);
    try verifyPageRankFixedIterationMetadata(
        alloc,
        pagerank_fixed_iteration_db_path,
        pagerank_fixed_iteration_target_generation,
        2,
    );
    release_summary.fixed_iteration_families += 1;

    const pagerank_active_public_read_initial_generation = try seedPageRankSearchDb(alloc, pagerank_active_public_read_db_path);
    try verifyPageRankActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        pagerank_active_public_read_db_path,
        pagerank_active_public_read_initial_generation,
    );
    release_summary.direct_active_public_read_families += 1;

    const pagerank_same_worker_target_generation = try seedPageRankDb(alloc, pagerank_same_worker_db_path);
    try verifyPageRankSameWorkerReplacementAttemptFence(alloc, init.io, harness_exe, antfly_exe, pagerank_same_worker_db_path, pagerank_same_worker_target_generation);
    release_summary.same_worker_fencing_proofs += 1;

    const pagerank_later_contribution_target_generation = try seedPageRankDbWithMaxIterations(alloc, pagerank_later_contribution_db_path, 2);
    try verifyPageRankLaterContributionPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_later_contribution_db_path, pagerank_later_contribution_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_later_reduce_target_generation = try seedPageRankDbWithMaxIterations(alloc, pagerank_later_reduce_db_path, 2);
    try verifyPageRankLaterReducePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_later_reduce_db_path, pagerank_later_reduce_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_later_convergence_target_generation = try seedPageRankDbWithMaxIterations(alloc, pagerank_later_convergence_db_path, 2);
    try verifyPageRankLaterConvergencePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, pagerank_later_convergence_db_path, pagerank_later_convergence_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const pagerank_exhausted_attempt_initial_generation = try seedPageRankDbWithMaxIterations(alloc, pagerank_exhausted_attempt_db_path, 2);
    try verifyPageRankExhaustedAttemptProcess(alloc, init.io, harness_exe, antfly_exe, pagerank_exhausted_attempt_db_path, pagerank_exhausted_attempt_initial_generation);
    release_summary.exhausted_attempt_families += 1;

    const eigenvector_supervisor_target_generation = try seedEigenvectorDb(alloc, eigenvector_supervisor_db_path);
    try runSupervisorProcess(alloc, init.io, antfly_exe, eigenvector_supervisor_db_path);
    try verifyMetricFresh(alloc, eigenvector_supervisor_db_path, "eigenvector", eigenvector_supervisor_target_generation);

    const eigenvector_fixed_iteration_target_generation = try seedEigenvectorDbWithMaxIterations(alloc, eigenvector_fixed_iteration_db_path, 1);
    try runSupervisorProcess(alloc, init.io, antfly_exe, eigenvector_fixed_iteration_db_path);
    try verifyFixedIterationMetadata(
        alloc,
        eigenvector_fixed_iteration_db_path,
        "eigenvector",
        eigenvector_fixed_iteration_target_generation,
        1,
    );
    release_summary.fixed_iteration_families += 1;

    const eigenvector_publish_cleanup_target_generation = try seedEigenvectorDb(alloc, eigenvector_publish_cleanup_db_path);
    try verifyEigenvectorPublishAndCleanupRestart(alloc, init.io, antfly_exe, eigenvector_publish_cleanup_db_path, eigenvector_publish_cleanup_target_generation);
    release_summary.direct_publish_cleanup_families += 1;

    const eigenvector_publish_failure_initial_generation = try seedEigenvectorDb(alloc, eigenvector_publish_failure_db_path);
    try verifyEigenvectorPublishVerifierFailureProcess(alloc, init.io, antfly_exe, eigenvector_publish_failure_db_path, eigenvector_publish_failure_initial_generation);
    release_summary.direct_publish_failure_families += 1;

    const eigenvector_active_public_read_initial_generation = try seedEigenvectorSearchDb(alloc, eigenvector_active_public_read_db_path);
    try verifyEigenvectorActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        eigenvector_active_public_read_db_path,
        eigenvector_active_public_read_initial_generation,
    );
    release_summary.direct_active_public_read_families += 1;

    const eigenvector_service_active_public_read_initial_generation = try seedEigenvectorSearchDb(alloc, eigenvector_service_active_public_read_db_path);
    try verifyEigenvectorServiceActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        eigenvector_service_active_public_read_db_path,
        eigenvector_service_active_public_read_initial_generation,
    );
    release_summary.service_active_public_read_families += 1;

    const eigenvector_scan_target_generation = try seedEigenvectorDb(alloc, eigenvector_scan_db_path);
    try verifyEigenvectorScanPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, eigenvector_scan_db_path, eigenvector_scan_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const eigenvector_initialize_target_generation = try seedEigenvectorDb(alloc, eigenvector_initialize_db_path);
    try verifyEigenvectorInitializePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, eigenvector_initialize_db_path, eigenvector_initialize_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const eigenvector_contribution_target_generation = try seedEigenvectorDb(alloc, eigenvector_contribution_db_path);
    try verifyEigenvectorContributionPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, eigenvector_contribution_db_path, eigenvector_contribution_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const eigenvector_reduce_target_generation = try seedEigenvectorDb(alloc, eigenvector_reduce_db_path);
    try verifyEigenvectorReducePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, eigenvector_reduce_db_path, eigenvector_reduce_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const eigenvector_convergence_target_generation = try seedEigenvectorDb(alloc, eigenvector_convergence_db_path);
    try verifyEigenvectorConvergencePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, eigenvector_convergence_db_path, eigenvector_convergence_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const eigenvector_exhausted_attempt_initial_generation = try seedEigenvectorDbWithMaxIterations(alloc, eigenvector_exhausted_attempt_db_path, 2);
    try verifyEigenvectorExhaustedAttemptProcess(alloc, init.io, harness_exe, antfly_exe, eigenvector_exhausted_attempt_db_path, eigenvector_exhausted_attempt_initial_generation);
    release_summary.exhausted_attempt_families += 1;

    const hits_supervisor_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_supervisor_db_path);
    try runSupervisorProcess(alloc, init.io, antfly_exe, hits_supervisor_db_path);
    try verifyHitsFresh(alloc, hits_supervisor_db_path, hits_supervisor_target_generation);

    const hits_fixed_iteration_target_generation = try seedHitsDbWithActiveBuildMaxIterations(alloc, hits_fixed_iteration_db_path, 1, 0.000001);
    try runSupervisorProcess(alloc, init.io, antfly_exe, hits_fixed_iteration_db_path);
    try verifyHitsFixedIterationMetadata(
        alloc,
        hits_fixed_iteration_db_path,
        hits_fixed_iteration_target_generation,
        1,
    );
    release_summary.fixed_iteration_families += 1;

    const hits_publish_cleanup_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_publish_cleanup_db_path);
    try verifyHitsPublishAndCleanupRestart(alloc, init.io, antfly_exe, hits_publish_cleanup_db_path, hits_publish_cleanup_target_generation);
    release_summary.direct_publish_cleanup_families += 1;

    const hits_publish_failure_initial_generation = try seedHitsDbWithActiveBuild(alloc, hits_publish_failure_db_path);
    try verifyHitsPublishVerifierFailureProcess(alloc, init.io, antfly_exe, hits_publish_failure_db_path, hits_publish_failure_initial_generation);
    release_summary.direct_publish_failure_families += 1;

    const hits_active_public_read_initial_generation = try seedHitsBackgroundDb(alloc, hits_active_public_read_db_path);
    try verifyHitsActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        hits_active_public_read_db_path,
        hits_active_public_read_initial_generation,
    );
    release_summary.direct_active_public_read_families += 1;

    const hits_service_active_public_read_initial_generation = try seedHitsBackgroundDb(alloc, hits_service_active_public_read_db_path);
    try verifyHitsServiceActiveProcessPublicReadFreshness(
        alloc,
        init.io,
        antfly_exe,
        hits_service_active_public_read_db_path,
        hits_service_active_public_read_initial_generation,
    );
    release_summary.service_active_public_read_families += 1;

    const hits_exhausted_attempt_initial_generation = try seedHitsDbWithActiveBuildMaxIterations(alloc, hits_exhausted_attempt_db_path, 2, 0.000001);
    try verifyHitsExhaustedAttemptProcess(alloc, init.io, harness_exe, antfly_exe, hits_exhausted_attempt_db_path, hits_exhausted_attempt_initial_generation);
    release_summary.exhausted_attempt_families += 1;

    const hits_authority_contribution_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_authority_contribution_db_path);
    try verifyHitsAuthorityContributionPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, hits_authority_contribution_db_path, hits_authority_contribution_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const hits_authority_reduce_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_authority_reduce_db_path);
    try verifyHitsAuthorityReducePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, hits_authority_reduce_db_path, hits_authority_reduce_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const hits_convergence_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_convergence_db_path);
    try verifyHitsConvergencePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, hits_convergence_db_path, hits_convergence_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const hits_hub_contribution_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_hub_contribution_db_path);
    try verifyHitsHubContributionPageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, hits_hub_contribution_db_path, hits_hub_contribution_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    const hits_hub_reduce_target_generation = try seedHitsDbWithActiveBuild(alloc, hits_hub_reduce_db_path);
    try verifyHitsHubReducePageLeaseReclaim(alloc, init.io, harness_exe, antfly_exe, hits_hub_reduce_db_path, hits_hub_reduce_target_generation);
    recordDirectPageReclaimProof(&release_summary);

    try verifyProcessHarnessReleaseSummary(release_summary);
    try emitProcessHarnessReleaseSummary(init.io, profile, release_summary);
}

fn parseHarnessProfile(value: []const u8) !HarnessProfile {
    if (std.mem.eql(u8, value, "smoke")) return .smoke;
    if (std.mem.eql(u8, value, "promotion")) return .promotion;
    return error.InvalidGraphMetricProcessHarnessProfile;
}

fn verifyProcessHarnessReleaseSummary(summary: ProcessHarnessReleaseSummary) !void {
    const required = required_process_harness_release_summary;
    if (!hasRemoteOwnerReleaseGate(summary, required)) {
        return error.GraphMetricProcessReleaseCoverageMissing;
    }
}

fn hasRemoteOwnerReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasServiceRemoteOwnerReleaseGate(summary, required) and
        hasDirectRemoteOwnerReleaseGate(summary, required) and
        hasFailureReclaimReleaseGate(summary, required);
}

fn hasServiceRemoteOwnerReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasServiceLifecycleReleaseGate(summary, required) and
        hasServiceMultipageReleaseGate(summary, required) and
        hasServiceActiveReadReleaseGate(summary, required);
}

fn hasServiceLifecycleReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.launch_families == required.launch_families and
        summary.service_owner_restart_families == required.service_owner_restart_families and
        summary.service_publish_cleanup_families == required.service_publish_cleanup_families and
        summary.service_publish_failure_families == required.service_publish_failure_families and
        summary.service_cleanup_takeover_families == required.service_cleanup_takeover_families;
}

fn hasServiceMultipageReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.service_multipage_worker_pool_families == required.service_multipage_worker_pool_families and
        summary.service_multipage_coordinator_takeover_families == required.service_multipage_coordinator_takeover_families and
        summary.service_multipage_worker_pool_takeover_families == required.service_multipage_worker_pool_takeover_families and
        summary.service_multipage_worker_phase_proofs == required.service_multipage_worker_phase_proofs and
        summary.service_multipage_coordinator_phase_proofs == required.service_multipage_coordinator_phase_proofs and
        summary.service_multipage_takeover_phase_proofs == required.service_multipage_takeover_phase_proofs;
}

fn hasServiceActiveReadReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.service_active_public_read_families == required.service_active_public_read_families;
}

fn hasDirectPublishReadReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.direct_publish_cleanup_families == required.direct_publish_cleanup_families and
        summary.direct_publish_failure_families == required.direct_publish_failure_families and
        summary.direct_active_public_read_families == required.direct_active_public_read_families and
        summary.fixed_iteration_families == required.fixed_iteration_families;
}

fn hasDirectReclaimReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.direct_page_reclaim_phase_proofs == required.direct_page_reclaim_phase_proofs and
        summary.direct_reclaimed_attempt_completion_phase_proofs == required.direct_reclaimed_attempt_completion_phase_proofs and
        summary.direct_stale_attempt_rejection_phase_proofs == required.direct_stale_attempt_rejection_phase_proofs;
}

fn hasDirectExhaustionFencingReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return summary.exhausted_attempt_families == required.exhausted_attempt_families and
        summary.same_worker_fencing_proofs == required.same_worker_fencing_proofs;
}

fn hasDirectRemoteOwnerReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasDirectPublishReadReleaseGate(summary, required);
}

fn hasFailureReclaimReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasDirectReclaimReleaseGate(summary, required) and
        hasDirectExhaustionFencingReleaseGate(summary, required);
}

fn hasPublicReadReleaseGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasServiceActiveReadReleaseGate(summary, required) and
        hasDirectPublishReadReleaseGate(summary, required);
}

fn hasRolloutQualificationGate(summary: ProcessHarnessReleaseSummary, required: ProcessHarnessReleaseSummary) bool {
    return hasRemoteOwnerReleaseGate(summary, required) and
        hasPublicReadReleaseGate(summary, required);
}

fn emitProcessHarnessReleaseSummary(io: std.Io, profile: HarnessProfile, summary: ProcessHarnessReleaseSummary) !void {
    const required = required_process_harness_release_summary;
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    const out = &stdout_writer.interface;
    defer out.flush() catch {};

    try out.print(
        "{{\"event\":\"graph_metric_process_harness_summary\",\"profile\":\"{s}\",\"rollout_qualification_gate\":{},\"promotion_profile_floor_configured\":{},\"all_family_execution_configured\":{},\"public_read_release_gate\":{},\"remote_owner_release_gate\":{},\"service_remote_owner_release_gate\":{},\"service_lifecycle_release_gate\":{},\"service_multipage_release_gate\":{},\"service_active_read_release_gate\":{},\"direct_remote_owner_release_gate\":{},\"direct_publish_read_release_gate\":{},\"failure_reclaim_release_gate\":{},\"direct_reclaim_release_gate\":{},\"direct_exhaustion_fencing_release_gate\":{}",
        .{
            @tagName(profile),
            hasRolloutQualificationGate(summary, required),
            profile == .promotion,
            required.launch_families == 4 and summary.launch_families == required.launch_families,
            hasPublicReadReleaseGate(summary, required),
            hasRemoteOwnerReleaseGate(summary, required),
            hasServiceRemoteOwnerReleaseGate(summary, required),
            hasServiceLifecycleReleaseGate(summary, required),
            hasServiceMultipageReleaseGate(summary, required),
            hasServiceActiveReadReleaseGate(summary, required),
            hasDirectRemoteOwnerReleaseGate(summary, required),
            hasDirectPublishReadReleaseGate(summary, required),
            hasFailureReclaimReleaseGate(summary, required),
            hasDirectReclaimReleaseGate(summary, required),
            hasDirectExhaustionFencingReleaseGate(summary, required),
        },
    );
    try out.print(
        ",\"required_launch_families\":{d},\"launch_families\":{d},\"required_service_owner_restart_families\":{d},\"service_owner_restart_families\":{d},\"required_service_publish_cleanup_families\":{d},\"service_publish_cleanup_families\":{d},\"required_service_publish_failure_families\":{d},\"service_publish_failure_families\":{d},\"required_service_multipage_worker_pool_families\":{d},\"service_multipage_worker_pool_families\":{d},\"required_service_multipage_coordinator_takeover_families\":{d},\"service_multipage_coordinator_takeover_families\":{d},\"required_service_multipage_worker_pool_takeover_families\":{d},\"service_multipage_worker_pool_takeover_families\":{d},\"required_service_multipage_worker_phase_proofs\":{d},\"service_multipage_worker_phase_proofs\":{d},\"required_service_multipage_coordinator_phase_proofs\":{d},\"service_multipage_coordinator_phase_proofs\":{d},\"required_service_multipage_takeover_phase_proofs\":{d},\"service_multipage_takeover_phase_proofs\":{d},\"required_service_cleanup_takeover_families\":{d},\"service_cleanup_takeover_families\":{d},\"required_service_active_public_read_families\":{d},\"service_active_public_read_families\":{d}",
        .{
            required.launch_families,
            summary.launch_families,
            required.service_owner_restart_families,
            summary.service_owner_restart_families,
            required.service_publish_cleanup_families,
            summary.service_publish_cleanup_families,
            required.service_publish_failure_families,
            summary.service_publish_failure_families,
            required.service_multipage_worker_pool_families,
            summary.service_multipage_worker_pool_families,
            required.service_multipage_coordinator_takeover_families,
            summary.service_multipage_coordinator_takeover_families,
            required.service_multipage_worker_pool_takeover_families,
            summary.service_multipage_worker_pool_takeover_families,
            required.service_multipage_worker_phase_proofs,
            summary.service_multipage_worker_phase_proofs,
            required.service_multipage_coordinator_phase_proofs,
            summary.service_multipage_coordinator_phase_proofs,
            required.service_multipage_takeover_phase_proofs,
            summary.service_multipage_takeover_phase_proofs,
            required.service_cleanup_takeover_families,
            summary.service_cleanup_takeover_families,
            required.service_active_public_read_families,
            summary.service_active_public_read_families,
        },
    );
    try out.print(
        ",\"required_direct_publish_cleanup_families\":{d},\"direct_publish_cleanup_families\":{d},\"required_direct_publish_failure_families\":{d},\"direct_publish_failure_families\":{d},\"required_direct_active_public_read_families\":{d},\"direct_active_public_read_families\":{d},\"required_direct_page_reclaim_phase_proofs\":{d},\"direct_page_reclaim_phase_proofs\":{d},\"required_direct_reclaimed_attempt_completion_phase_proofs\":{d},\"direct_reclaimed_attempt_completion_phase_proofs\":{d},\"required_direct_stale_attempt_rejection_phase_proofs\":{d},\"direct_stale_attempt_rejection_phase_proofs\":{d},\"required_fixed_iteration_families\":{d},\"fixed_iteration_families\":{d},\"required_exhausted_attempt_families\":{d},\"exhausted_attempt_families\":{d},\"required_same_worker_fencing_proofs\":{d},\"same_worker_fencing_proofs\":{d}}}\n",
        .{
            required.direct_publish_cleanup_families,
            summary.direct_publish_cleanup_families,
            required.direct_publish_failure_families,
            summary.direct_publish_failure_families,
            required.direct_active_public_read_families,
            summary.direct_active_public_read_families,
            required.direct_page_reclaim_phase_proofs,
            summary.direct_page_reclaim_phase_proofs,
            required.direct_reclaimed_attempt_completion_phase_proofs,
            summary.direct_reclaimed_attempt_completion_phase_proofs,
            required.direct_stale_attempt_rejection_phase_proofs,
            summary.direct_stale_attempt_rejection_phase_proofs,
            required.fixed_iteration_families,
            summary.fixed_iteration_families,
            required.exhausted_attempt_families,
            summary.exhausted_attempt_families,
            required.same_worker_fencing_proofs,
            summary.same_worker_fencing_proofs,
        },
    );
}

fn seedDegreeDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    return seedDegreeDbWithSources(alloc, db_path, 8);
}

fn seedDegreeDbWithSources(alloc: std.mem.Allocator, db_path: []const u8, source_count: usize) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });
    for (0..source_count) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedDegreeSearchDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\",\"body\":\"hub graph\"}" }},
        .sync_level = .write,
    });
    for (0..8) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"source {d}\",\"body\":\"oldsource graph {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{ i, i },
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }
    try db.batch(.{
        .writes = &.{.{ .key = "doc:side", .value = "{\"title\":\"side\",\"body\":\"oldsource side graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:0\",\"weight\":1.0}]}}}" }},
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedPageRankDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    return seedPageRankDbWithMaxIterations(alloc, db_path, 1);
}

fn seedPageRankDbWithSources(alloc: std.mem.Allocator, db_path: []const u8, source_count: usize) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });
    for (0..source_count) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:pr:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"pagerank source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedPageRankDbWithMaxIterations(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    max_iterations: u32,
) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config_json = try std.fmt.allocPrint(
        alloc,
        "{{\"metrics\":{{\"pagerank\":{{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":0.000001,\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
        .{max_iterations},
    );
    defer alloc.free(config_json);
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedPageRankSearchDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"gamma graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"body\":\"delta graph\"}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedEigenvectorDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    return seedEigenvectorDbWithMaxIterations(alloc, db_path, 2);
}

fn seedEigenvectorDbWithSources(alloc: std.mem.Allocator, db_path: []const u8, source_count: usize) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}" }},
        .sync_level = .write,
    });
    for (0..source_count) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:ev:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"eigenvector source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:hub\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedEigenvectorDbWithMaxIterations(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    max_iterations: u32,
) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config_json = try std.fmt.allocPrint(
        alloc,
        "{{\"metrics\":{{\"eigenvector\":{{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":0.000001,\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
        .{max_iterations},
    );
    defer alloc.free(config_json);
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:a\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedEigenvectorSearchDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"eigenvector\":{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0},{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"body\":\"gamma graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:a\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"body\":\"delta graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .full_index,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedHitsDbWithActiveBuild(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    return seedHitsDbWithActiveBuildMaxIterations(alloc, db_path, 1, 0.000001);
}

fn seedHitsDbWithSources(alloc: std.mem.Allocator, db_path: []const u8, source_count: usize) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:authority", .value = "{\"title\":\"authority\"}" }},
        .sync_level = .write,
    });
    for (0..source_count) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:hits:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(
            alloc,
            "{{\"title\":\"hits source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[{{\"target\":\"doc:authority\",\"weight\":1.0}}]}}}}}}",
            .{i},
        );
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .write,
        });
    }
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn seedHitsDbWithActiveBuildMaxIterations(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    max_iterations: u32,
    tolerance: f64,
) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    const config_json = try std.fmt.allocPrint(
        alloc,
        "{{\"metrics\":{{\"hits_authority\":{{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"manual\",\"max_iterations\":{d},\"tolerance\":{d},\"edge_filter\":{{\"types\":[\"cites\"]}}}},\"hits_hub\":{{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"manual\",\"max_iterations\":{d},\"tolerance\":{d},\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
        .{ max_iterations, tolerance, max_iterations, tolerance },
    );
    defer alloc.free(config_json);
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = config_json,
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"body\":\"hub a graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"body\":\"hub b graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{
                .key = "doc:authority",
                .value = if (max_iterations > 1)
                    "{\"title\":\"authority\",\"body\":\"authority graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}"
                else
                    "{\"title\":\"authority\",\"body\":\"authority graph\"}",
            },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const target_generation = graph_entry.index.edge_generation;
    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "hits_authority", target_generation);
    defer started.deinit(alloc);
    if (started.state != antfly.graph.GraphIndex.GraphMetricState.building or started.building_generation != target_generation) {
        return error.GraphMetricBuildNotStarted;
    }
    return target_generation;
}

fn seedHitsBackgroundDb(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"hits_authority\":{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}},\"hits_hub\":{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":1,\"tolerance\":0.000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-a", .value = "{\"title\":\"hub a\",\"body\":\"hub a graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:hub-b", .value = "{\"title\":\"hub b\",\"body\":\"hub b graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:authority", .value = "{\"title\":\"authority\",\"body\":\"authority graph\"}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn addPageRankDirtyEdge(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn addDegreeDirtyEdge(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"title\":\"new source\",\"body\":\"newsource graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn addEigenvectorDirtyEdge(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:e", .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn addHitsDirtyEdge(alloc: std.mem.Allocator, db_path: []const u8) !u64 {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:hub-c", .value = "{\"title\":\"hub c\",\"body\":\"hub c graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    return graph_entry.index.edge_generation;
}

fn runClaimDegreePageHoldMode(
    alloc: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
) !void {
    if (argv.len != 7) {
        std.debug.print("usage: graph_metric_process_harness claim-degree-page-hold <db-path> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
        std.process.exit(2);
    }
    const db_path = argv[2];
    const worker_id = argv[3];
    const now_ms = try std.fmt.parseInt(u64, argv[4], 10);
    const ready_file = argv[5];
    const hold_ms = try std.fmt.parseInt(u64, argv[6], 10);

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus("degree");
    defer status.deinit(alloc);
    if (status.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree) {
        return error.GraphMetricUnexpectedPhase;
    }
    const page = try graph_entry.index.claimNextGraphMetricBuildPageAt(
        "degree",
        status.build_job_id,
        .scan_edges_and_out_degree,
        0,
        worker_id,
        now_ms,
    ) orelse return error.GraphMetricExpectedPageClaim;
    _ = try graph_entry.index.updateGraphMetricBuildPageProgressForAttempt(
        "degree",
        status.build_job_id,
        .scan_edges_and_out_degree,
        0,
        page.page_id,
        worker_id,
        page.attempt,
        "process-dead-cursor",
        if (page.total_units > 0) 1 else 0,
        page.total_units,
    );

    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = ready_file,
        .data = "ready\n",
    });
    platform.time.sleepNs(hold_ms * std.time.ns_per_ms);
}

fn runClaimMetricPageHoldMode(
    alloc: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
) !void {
    if (argv.len != 9) {
        std.debug.print("usage: graph_metric_process_harness claim-metric-page-hold <db-path> <metric-name> <phase> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
        std.process.exit(2);
    }
    const db_path = argv[2];
    const metric_name = argv[3];
    const phase = try parseBuildPhase(argv[4]);
    const worker_id = argv[5];
    const now_ms = try std.fmt.parseInt(u64, argv[6], 10);
    const ready_file = argv[7];
    const hold_ms = try std.fmt.parseInt(u64, argv[8], 10);

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.phase != phase) return error.GraphMetricUnexpectedPhase;
    const iteration: u32 = if (phase == .cleanup_old_generations) 0 else status.build_iteration;
    const page = try graph_entry.index.claimNextGraphMetricBuildPageAt(
        metric_name,
        status.build_job_id,
        phase,
        iteration,
        worker_id,
        now_ms,
    ) orelse return error.GraphMetricExpectedPageClaim;
    _ = try graph_entry.index.updateGraphMetricBuildPageProgressForAttempt(
        metric_name,
        status.build_job_id,
        phase,
        iteration,
        page.page_id,
        worker_id,
        page.attempt,
        "process-dead-cursor",
        if (page.total_units > 0) 1 else 0,
        page.total_units,
    );

    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = ready_file,
        .data = "ready\n",
    });
    platform.time.sleepNs(hold_ms * std.time.ns_per_ms);
}

fn parseBuildPhase(raw: []const u8) !antfly.graph.GraphIndex.GraphMetricBuildPhase {
    if (std.mem.eql(u8, raw, "scan_edges_and_out_degree")) return .scan_edges_and_out_degree;
    if (std.mem.eql(u8, raw, "initialize_ranks")) return .initialize_ranks;
    if (std.mem.eql(u8, raw, "iterate_contributions")) return .iterate_contributions;
    if (std.mem.eql(u8, raw, "reduce_ranks")) return .reduce_ranks;
    if (std.mem.eql(u8, raw, "hits_hub_contributions")) return .hits_hub_contributions;
    if (std.mem.eql(u8, raw, "hits_hub_reduce_ranks")) return .hits_hub_reduce_ranks;
    if (std.mem.eql(u8, raw, "check_convergence")) return .check_convergence;
    if (std.mem.eql(u8, raw, "cleanup_old_generations")) return .cleanup_old_generations;
    return error.InvalidArguments;
}

fn prepareDegreeScanBuild(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "degree", target_generation);
    defer started.deinit(alloc);
    const prepare_worker = try db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "degree", "process-prepare-worker", 1000);
    if (!prepare_worker.claimed_page or !prepare_worker.completed_page) return error.GraphMetricExpectedPageClaim;
    const prepare_coordinator = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "degree", 1001);
    if (!prepare_coordinator.advanced_phase) return error.GraphMetricUnexpectedPhase;
}

fn prepareMetricScanBuild(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", metric_name, target_generation);
    defer started.deinit(alloc);
    const prepare_worker = try db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", metric_name, "process-prepare-worker", 1000);
    if (!prepare_worker.claimed_page or !prepare_worker.completed_page) return error.GraphMetricExpectedPageClaim;
    const prepare_coordinator = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", metric_name, 1001);
    if (!prepare_coordinator.advanced_phase) return error.GraphMetricUnexpectedPhase;
}

fn prepareMetricBuildToPhase(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
    target_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
) !void {
    return prepareMetricBuildToPhaseAndIteration(alloc, db_path, metric_name, target_generation, target_phase, 0);
}

fn prepareMetricBuildToPhaseAndIteration(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
    target_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    target_iteration: u32,
) !void {
    try prepareMetricScanBuild(alloc, db_path, metric_name, target_generation);
    if (target_phase == .scan_edges_and_out_degree and target_iteration == 0) return;

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var now_ms: u64 = 1100;
    for (0..128) |_| {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus(metric_name);
        defer status.deinit(alloc);
        if (status.phase == target_phase and (status.build_iteration == target_iteration or target_phase == .publish_generation or target_phase == .cleanup_old_generations)) return;
        if (status.phase == .publish_generation or status.phase == .cleanup_old_generations or status.phase == .complete) {
            std.debug.print("metric {s} advanced past target phase {}, got {}\n", .{ metric_name, target_phase, status.phase });
            return error.GraphMetricUnexpectedPhase;
        }

        const worker_step = try db.runGraphMetricPlannedWorkerPageStepAt(
            "graph_idx",
            metric_name,
            "process-phase-prep-worker",
            now_ms,
        );
        now_ms += 1;
        if (worker_step.failed_build) return error.GraphMetricBuildFailed;

        const coordinator_step = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", metric_name, now_ms);
        now_ms += 1;
        if (coordinator_step.failed_build) return error.GraphMetricBuildFailed;
    }
    return error.GraphMetricTargetPhaseNotReached;
}

fn prepareDegreePublishReadyBuild(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var started = try db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "degree", target_generation);
    defer started.deinit(alloc);

    var now_ms: u64 = 20_000;
    for (0..128) |_| {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        switch (status.phase) {
            .publish_generation => return,
            .cleanup_old_generations, .complete => return error.GraphMetricUnexpectedPhase,
            else => {},
        }

        const worker_step = try db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "degree", "process-publish-prep-worker", now_ms);
        now_ms += 1;
        if (worker_step.phase == .publish_generation) return;
        if (worker_step.completed_page or !worker_step.claimed_page) {
            const coordinator_step = try db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "degree", now_ms);
            now_ms += 1;
            if (coordinator_step.phase == .publish_generation) return;
        }
    }
    return error.GraphMetricPublishReadyNotReached;
}

fn assertDegreePhase(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_generation: u64,
) !void {
    return assertMetricPhase(alloc, db_path, "degree", expected_phase, expected_generation);
}

fn assertMetricPhase(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.phase != expected_phase) {
        std.debug.print("expected {s} phase {}, got {}\n", .{ metric_name, expected_phase, status.phase });
        return error.GraphMetricUnexpectedPhase;
    }
    if (expected_generation != 0 and status.published_generation != expected_generation) {
        std.debug.print(
            "expected published generation {d}, got {d}\n",
            .{ expected_generation, status.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
}

fn assertHitsAfterPairedPublish(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var authority = try graph_entry.index.graphMetricStatus("hits_authority");
    defer authority.deinit(alloc);
    var hub = try graph_entry.index.graphMetricStatus("hits_hub");
    defer hub.deinit(alloc);
    if (authority.state != antfly.graph.GraphIndex.GraphMetricState.building or
        authority.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations)
    {
        std.debug.print("expected HITS authority to be cleaning after paired publish, got {}/{}\n", .{ authority.state, authority.phase });
        return error.GraphMetricProcessProofFailed;
    }
    if (hub.state != antfly.graph.GraphIndex.GraphMetricState.fresh or
        hub.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.complete)
    {
        std.debug.print("expected HITS hub to be fresh/complete after paired publish, got {}/{}\n", .{ hub.state, hub.phase });
        return error.GraphMetricProcessProofFailed;
    }
    if (authority.published_generation != target_generation or hub.published_generation != target_generation) {
        std.debug.print(
            "expected paired HITS published generation {d}, got authority {d} hub {d}\n",
            .{ target_generation, authority.published_generation, hub.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (authority.recent_events.len != 1 or hub.recent_events.len != 1 or
        authority.recent_events[0].kind != antfly.graph.GraphIndex.GraphMetricEventKind.publish or
        hub.recent_events[0].kind != antfly.graph.GraphIndex.GraphMetricEventKind.publish)
    {
        std.debug.print("expected paired HITS publish events after process publish\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
}

fn assertMetricSinglePublishEvent(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.published_generation != target_generation) {
        std.debug.print("expected {s} published generation {d}, got {d}\n", .{
            metric_name,
            target_generation,
            status.published_generation,
        });
        return error.GraphMetricProcessProofFailed;
    }
    if (status.recent_events.len != 1 or status.recent_events[0].kind != antfly.graph.GraphIndex.GraphMetricEventKind.publish) {
        std.debug.print("expected one {s} publish event after duplicate coordinator process\n", .{metric_name});
        return error.GraphMetricProcessProofFailed;
    }
}

fn assertHitsSinglePublishEventPair(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try assertMetricSinglePublishEvent(alloc, db_path, "hits_authority", target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "hits_hub", target_generation);
}

fn assertMetricRecentEventKindCount(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    kind: antfly.graph.GraphIndex.GraphMetricEventKind,
    expected_count: usize,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);

    var actual_count: usize = 0;
    for (status.recent_events) |event| {
        if (event.kind == kind) actual_count += 1;
    }
    if (actual_count != expected_count) {
        std.debug.print("expected {s} recent event kind {} count {d}, got {d}\n", .{
            metric_name,
            kind,
            expected_count,
            actual_count,
        });
        return error.GraphMetricProcessProofFailed;
    }
}

fn assertHitsRecentEventKindCountPair(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    kind: antfly.graph.GraphIndex.GraphMetricEventKind,
    expected_count: usize,
) !void {
    try assertMetricRecentEventKindCount(alloc, db_path, "hits_authority", kind, expected_count);
    try assertMetricRecentEventKindCount(alloc, db_path, "hits_hub", kind, expected_count);
}

fn assertDuplicateCoordinatorDidNotMutate(summary: RoleRunSummary, label: []const u8) !void {
    if (summary.result.published != 0 or summary.result.failed_builds != 0 or summary.result.phases_advanced != 0) {
        std.debug.print("expected duplicate coordinator process not to publish, fail, or advance {s}\n", .{label});
        return error.GraphMetricProcessProofFailed;
    }
}

fn verifyPublishAndCleanupRestart(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareDegreePublishReadyBuild(alloc, db_path, target_generation);
    try assertDegreePhase(alloc, db_path, .publish_generation, 0);

    const publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "publish-proof-coordinator",
        "5000",
        "30000",
    );
    if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
        std.debug.print("expected coordinator process to publish degree build after restart boundary\n", .{});
        return error.GraphMetricPublishRestartProofFailed;
    }
    try assertDegreePhase(alloc, db_path, .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "degree", target_generation);

    const duplicate_publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "publish-proof-duplicate-coordinator",
        "5000",
        "30000",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "degree");
    try assertDegreePhase(alloc, db_path, .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "degree", target_generation);

    const first_cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "cleanup-proof-worker-owner-a",
        "cleanup-proof-worker-a",
        "5000",
        "30001",
    );
    if (first_cleanup.result.pages_claimed != 1 or first_cleanup.result.pages_completed != 1) {
        std.debug.print("expected first cleanup worker process to complete one cleanup page\n", .{});
        return error.GraphMetricCleanupRestartProofFailed;
    }
    try assertDegreePhase(alloc, db_path, .cleanup_old_generations, target_generation);

    const final_cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "cleanup-proof-worker-owner-b",
        "cleanup-proof-worker-b",
        "5000",
        "30002",
    );
    if (final_cleanup.result.pages_claimed == 0 or final_cleanup.result.pages_completed == 0) {
        std.debug.print("expected second cleanup worker process to resume cleanup after restart boundary\n", .{});
        return error.GraphMetricCleanupRestartProofFailed;
    }
    try verifyDegreeFresh(alloc, db_path, target_generation);
}

fn verifyPageRankPublishAndCleanupRestart(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", target_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "pagerank", .publish_generation, 0);

    const publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-publish-proof-coordinator",
        "5000",
        "40000",
    );
    if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
        std.debug.print("expected coordinator process to publish PageRank build after restart boundary\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try assertMetricPhase(alloc, db_path, "pagerank", .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "pagerank", target_generation);

    const duplicate_publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-publish-proof-duplicate-coordinator",
        "5000",
        "40000",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "pagerank");
    try assertMetricPhase(alloc, db_path, "pagerank", .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "pagerank", target_generation);

    const first_cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-proof-worker-owner-a",
        "pagerank-cleanup-proof-worker-a",
        "5000",
        "40001",
    );
    if (first_cleanup.result.pages_claimed != 1 or first_cleanup.result.pages_completed != 1) {
        std.debug.print("expected first PageRank cleanup worker process to complete one cleanup page\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try assertMetricPhase(alloc, db_path, "pagerank", .cleanup_old_generations, target_generation);

    const second_cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-proof-worker-owner-b",
        "pagerank-cleanup-proof-worker-b",
        "5000",
        "40002",
    );
    if (second_cleanup.result.pages_claimed != 1 or second_cleanup.result.pages_completed != 1) {
        std.debug.print("expected second PageRank cleanup worker process to complete one cleanup page\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try assertMetricPhase(alloc, db_path, "pagerank", .cleanup_old_generations, target_generation);

    const final_cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-proof-worker-owner-c",
        "pagerank-cleanup-proof-worker-c",
        "5000",
        "40003",
    );
    if (final_cleanup.result.pages_claimed != 1 or final_cleanup.result.pages_completed != 1) {
        std.debug.print("expected final PageRank cleanup worker process to complete cleanup\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try verifyPageRankFixedIterationMetadata(alloc, db_path, target_generation, 1);
}

fn verifyEigenvectorPublishAndCleanupRestart(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "eigenvector", target_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "eigenvector", .publish_generation, 0);

    const publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-publish-proof-coordinator",
        "5000",
        "70000",
    );
    if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
        std.debug.print("expected coordinator process to publish eigenvector build after restart boundary\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try assertMetricPhase(alloc, db_path, "eigenvector", .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "eigenvector", target_generation);

    const duplicate_publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-publish-proof-duplicate-coordinator",
        "5000",
        "70000",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "eigenvector");
    try assertMetricPhase(alloc, db_path, "eigenvector", .cleanup_old_generations, target_generation);
    try assertMetricSinglePublishEvent(alloc, db_path, "eigenvector", target_generation);

    const cleanup = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-cleanup-proof-worker-owner",
        "eigenvector-cleanup-proof-worker",
        "5000",
        "70001",
    );
    if (cleanup.result.pages_claimed != 1 or cleanup.result.pages_completed != 1) {
        std.debug.print("expected eigenvector cleanup worker process to complete cleanup after restart boundary\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try verifyMetricFresh(alloc, db_path, "eigenvector", target_generation);
}

fn verifyHitsPublishAndCleanupRestart(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "hits_authority", target_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "hits_authority", .publish_generation, 0);

    const publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-publish-proof-coordinator",
        "5000",
        "72000",
    );
    if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
        std.debug.print("expected coordinator process to publish HITS pair after restart boundary\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try assertHitsAfterPairedPublish(alloc, db_path, target_generation);
    try assertHitsSinglePublishEventPair(alloc, db_path, target_generation);

    const duplicate_publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-publish-proof-duplicate-coordinator",
        "5000",
        "72000",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "hits");
    try assertHitsAfterPairedPublish(alloc, db_path, target_generation);
    try assertHitsSinglePublishEventPair(alloc, db_path, target_generation);

    var cleanup_progressed = false;
    for (0..12) |i| {
        const owner_id = try std.fmt.allocPrint(alloc, "hits-cleanup-proof-worker-owner-{d}", .{i});
        defer alloc.free(owner_id);
        const worker_id = try std.fmt.allocPrint(alloc, "hits-cleanup-proof-worker-{d}", .{i});
        defer alloc.free(worker_id);
        const now_ms = try std.fmt.allocPrint(alloc, "{d}", .{72001 + i});
        defer alloc.free(now_ms);
        const cleanup = try runWorkerRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            db_path,
            owner_id,
            worker_id,
            "5000",
            now_ms,
        );
        cleanup_progressed = cleanup_progressed or cleanup.durable_progressed or cleanup.result.pages_claimed != 0 or cleanup.result.pages_completed != 0 or cleanup.result.published != 0;
        if (cleanup.result.published != 0) {
            break;
        }
    }
    verifyHitsFresh(alloc, db_path, target_generation) catch |err| {
        if (!cleanup_progressed) {
            std.debug.print("expected HITS cleanup worker role processes to advance cleanup\n", .{});
        } else {
            std.debug.print("expected HITS cleanup to finish through worker role processes\n", .{});
        }
        return err;
    };
}

fn verifyPageRankCleanupPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", target_generation, .publish_generation);
    const publish = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-page-proof-publish-coordinator",
        "5000",
        "45000",
    );
    if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
        std.debug.print("expected coordinator process to publish PageRank before cleanup page reclaim proof\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try assertMetricPhase(alloc, db_path, "pagerank", .cleanup_old_generations, target_generation);

    const ready_file = ".zig-cache/tmp/graph-metric-process-pagerank-cleanup-ready";
    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillMetricPageOwnerAfterReady(
        io,
        harness_exe,
        db_path,
        "pagerank",
        "cleanup_old_generations",
        "process-pagerank-cleanup-dead-worker",
        "45001",
        ready_file,
    );

    const dead_page = try readLeasedMetricPage(
        alloc,
        db_path,
        "pagerank",
        .cleanup_old_generations,
        "process-pagerank-cleanup-dead-worker",
        "process-dead-cursor",
    );
    const before_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms - 1});
    defer alloc.free(before_expiry_text);
    _ = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-page-proof-early-owner",
        "process-pagerank-cleanup-reclaim-worker",
        "5000",
        before_expiry_text,
    );
    _ = try readLeasedMetricPage(
        alloc,
        db_path,
        "pagerank",
        .cleanup_old_generations,
        "process-pagerank-cleanup-dead-worker",
        "process-dead-cursor",
    );

    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const reclaim_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-cleanup-page-proof-reclaim-owner",
        "process-pagerank-cleanup-reclaim-worker",
        "5000",
        after_expiry_text,
    );
    if (reclaim_worker.result.pages_claimed == 0 or reclaim_worker.result.pages_completed == 0) {
        std.debug.print("expected PageRank replacement worker to reclaim and complete expired cleanup page lease\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try expectStaleMetricPageAttemptRejected(alloc, db_path, "pagerank", .cleanup_old_generations, dead_page, "process-pagerank-cleanup-dead-worker");

    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", target_generation);
}

fn verifyPageRankPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", initial_generation);

    const rebuild_generation = try addPageRankDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "pagerank", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "pagerank");

    const failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-publish-failure-proof-coordinator",
        "5000",
        "50000",
    );
    if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
        std.debug.print("expected coordinator process to fail PageRank publish verification without publishing\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try verifyMetricFailedPreservesPublished(alloc, db_path, "pagerank", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "pagerank", .failed, 1);

    const duplicate_failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-publish-failure-proof-duplicate-coordinator",
        "5000",
        "50001",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "pagerank publish failure");
    try verifyMetricFailedPreservesPublished(alloc, db_path, "pagerank", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "pagerank", .failed, 1);
}

fn verifyPageRankServiceTargetedPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", initial_generation);

    const rebuild_generation = try addPageRankDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "pagerank", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "pagerank");

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-failure-coordinator",
            "service-pagerank-publish-failure-coordinator-a",
            "5000",
            "86100",
        );
        if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
            std.debug.print("expected service coordinator process to fail PageRank publish verification without publishing\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }

        const duplicate_failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-failure-coordinator",
            "service-pagerank-publish-failure-coordinator-b",
            "5000",
            "86101",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "service pagerank publish failure");
    }
    try verifyMetricFailedPreservesPublished(alloc, db_path, "pagerank", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "pagerank", .failed, 1);
}

fn verifyEigenvectorPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "eigenvector", initial_generation);

    const rebuild_generation = try addEigenvectorDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "eigenvector", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "eigenvector", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "eigenvector");

    const failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-publish-failure-proof-coordinator",
        "5000",
        "71000",
    );
    if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
        std.debug.print("expected coordinator process to fail eigenvector publish verification without publishing\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try verifyMetricFailedPreservesPublished(alloc, db_path, "eigenvector", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "eigenvector", .failed, 1);

    const duplicate_failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-publish-failure-proof-duplicate-coordinator",
        "5000",
        "71001",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "eigenvector publish failure");
    try verifyMetricFailedPreservesPublished(alloc, db_path, "eigenvector", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "eigenvector", .failed, 1);
}

fn verifyEigenvectorServiceTargetedPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "eigenvector", initial_generation);

    const rebuild_generation = try addEigenvectorDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "eigenvector", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "eigenvector", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "eigenvector");

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-failure-coordinator",
            "service-eigenvector-publish-failure-coordinator-a",
            "5000",
            "86200",
        );
        if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
            std.debug.print("expected service coordinator process to fail eigenvector publish verification without publishing\n", .{});
            return error.GraphMetricProcessProofFailed;
        }

        const duplicate_failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-failure-coordinator",
            "service-eigenvector-publish-failure-coordinator-b",
            "5000",
            "86201",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "service eigenvector publish failure");
    }
    try verifyMetricFailedPreservesPublished(alloc, db_path, "eigenvector", initial_generation, "InvalidGraphMetricBuildManifest");
    try assertMetricRecentEventKindCount(alloc, db_path, "eigenvector", .failed, 1);
}

fn verifyPageRankExhaustedAttemptProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", initial_generation);

    const rebuild_generation = try addPageRankDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try verifyMetricExhaustedAttemptProcess(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "pagerank",
        initial_generation,
        rebuild_generation,
        .iterate_contributions,
        1,
        "iterate_contributions",
        "PageRank",
        "pagerank",
        70_000,
    );
}

fn verifyEigenvectorExhaustedAttemptProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "eigenvector", initial_generation);

    const rebuild_generation = try addEigenvectorDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try verifyMetricExhaustedAttemptProcess(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "eigenvector",
        initial_generation,
        rebuild_generation,
        .iterate_contributions,
        1,
        "iterate_contributions",
        "eigenvector",
        "eigenvector",
        72_000,
    );
}

fn verifyMetricExhaustedAttemptProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    metric_name: []const u8,
    initial_generation: u64,
    rebuild_generation: u64,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    iteration: u32,
    phase_arg: []const u8,
    label: []const u8,
    id_prefix: []const u8,
    first_now_ms: u64,
) !void {
    try prepareMetricBuildToPhaseAndIteration(
        alloc,
        db_path,
        metric_name,
        rebuild_generation,
        phase,
        iteration,
    );
    try assertMetricPhase(alloc, db_path, metric_name, phase, initial_generation);

    var now_ms = first_now_ms;
    var exhausted_page_id: u64 = 0;
    var last_lease_expires_at_ms: u64 = 0;
    for (0..3) |attempt_index| {
        const worker_id = try std.fmt.allocPrint(alloc, "process-{s}-exhausted-worker-{d}", .{ id_prefix, attempt_index });
        defer alloc.free(worker_id);
        const ready_file = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/graph-metric-process-{s}-exhausted-attempt-{d}-ready", .{ id_prefix, attempt_index });
        defer alloc.free(ready_file);
        std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};

        const now_text = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_text);
        try runAndKillMetricPageOwnerAfterReady(
            io,
            harness_exe,
            db_path,
            metric_name,
            phase_arg,
            worker_id,
            now_text,
            ready_file,
        );
        const leased = try readSingleLeasedMetricPage(
            alloc,
            db_path,
            metric_name,
            phase,
            worker_id,
            "process-dead-cursor",
        );
        if (leased.iteration != iteration) {
            std.debug.print("expected exhausted {s} page at iteration {d}, got {d}\n", .{ label, iteration, leased.iteration });
            return error.GraphMetricProcessProofFailed;
        }
        if (attempt_index == 0) {
            exhausted_page_id = leased.page_id;
        } else if (leased.page_id != exhausted_page_id) {
            std.debug.print("expected {s} exhausted attempts to reclaim page {d}, got {d}\n", .{ label, exhausted_page_id, leased.page_id });
            return error.GraphMetricProcessProofFailed;
        }
        const expected_attempt: u64 = @intCast(attempt_index + 1);
        if (leased.attempt != expected_attempt) {
            std.debug.print("expected {s} page attempt {d}, got {d}\n", .{ label, expected_attempt, leased.attempt });
            return error.GraphMetricProcessProofFailed;
        }
        last_lease_expires_at_ms = leased.lease_expires_at_ms;
        now_ms = leased.lease_expires_at_ms + 1;
    }

    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{last_lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const coordinator_owner_id = try std.fmt.allocPrint(alloc, "{s}-exhausted-attempt-proof-coordinator", .{id_prefix});
    defer alloc.free(coordinator_owner_id);
    const failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        coordinator_owner_id,
        "5000",
        after_expiry_text,
    );
    if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
        std.debug.print("expected coordinator process to fail exhausted {s} page attempts without publishing\n", .{label});
        return error.GraphMetricProcessProofFailed;
    }
    try verifyMetricFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        metric_name,
        initial_generation,
        "GraphMetricBuildPageAttemptsExhausted",
        phase,
        iteration,
    );
    try assertMetricRecentEventKindCount(alloc, db_path, metric_name, .failed, 1);

    const duplicate_coordinator_owner_id = try std.fmt.allocPrint(alloc, "{s}-exhausted-attempt-proof-duplicate-coordinator", .{id_prefix});
    defer alloc.free(duplicate_coordinator_owner_id);
    const duplicate_failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        duplicate_coordinator_owner_id,
        "5000",
        after_expiry_text,
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, label);
    try verifyMetricFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        metric_name,
        initial_generation,
        "GraphMetricBuildPageAttemptsExhausted",
        phase,
        iteration,
    );
    try assertMetricRecentEventKindCount(alloc, db_path, metric_name, .failed, 1);
}

fn verifyEigenvectorActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "eigenvector", initial_generation);

    const rebuild_generation = try addEigenvectorDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const started = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-active-read-proof-coordinator",
        "5000",
        "72000",
    );
    if (!started.durable_progressed or !started.stats.has_lease) {
        std.debug.print("expected coordinator process to start active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "eigenvector-active-read-proof-worker-owner",
        "eigenvector-active-read-proof-worker",
        "5000",
        "72001",
    );
    if (!worker.durable_progressed or worker.result.pages_claimed == 0 or worker.result.pages_completed == 0) {
        std.debug.print("expected worker process to advance active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 1) {
        std.debug.print("expected one eigenvector graph metric result during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const result = published_result.graph_metric_results[0];
    if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected active eigenvector query status building, got {}\n", .{result.status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (result.status.published_generation != initial_generation or result.status.building_generation != rebuild_generation) {
        std.debug.print(
            "expected eigenvector published generation {d} and building generation {d}, got {d}/{d}\n",
            .{
                initial_generation,
                rebuild_generation,
                result.status.published_generation,
                result.status.building_generation,
            },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (result.scores.len == 0) {
        std.debug.print("expected active eigenvector published read to serve prior scores\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (result.scores) |score| {
        if (std.mem.eql(u8, score.node, "doc:e")) {
            std.debug.print("active eigenvector published read exposed rebuilding node {s}\n", .{score.node});
            return error.GraphMetricProcessProofFailed;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "eigenvector",
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len == 0) {
        std.debug.print("expected eigenvector graph traversal result during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    for (traversal.nodes) |node| {
        if (node.metrics.len != 1 or node.metrics[0].score == null) {
            std.debug.print("expected traversal published metric projection to serve prior eigenvector score\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    if (traversal.metric_status.len != 1 or traversal.metric_status[0].state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected traversal metric status building during active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    if (traversal.metric_status[0].published_generation != initial_generation or traversal.metric_status[0].building_generation != rebuild_generation) {
        std.debug.print("expected traversal status to report eigenvector published/building generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "eigenvector",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "eigenvector",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "eigenvector",
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "eigenvector",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected search rerank status building, got {}\n", .{rerank_status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation or rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected eigenvector rerank status to report published/building generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:e")) {
            std.debug.print("active eigenvector search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected eigenvector reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected eigenvector reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "eigenvector",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

fn verifyHitsPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyHitsFresh(alloc, db_path, initial_generation);

    const rebuild_generation = try addHitsDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "hits_authority", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "hits_authority", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "hits_authority");

    const failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-publish-failure-proof-coordinator",
        "5000",
        "73000",
    );
    if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
        std.debug.print("expected coordinator process to fail HITS publish verification without publishing\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try verifyHitsFailedPreservesPublished(alloc, db_path, initial_generation, "InvalidGraphMetricBuildManifest");
    try assertHitsRecentEventKindCountPair(alloc, db_path, .failed, 1);

    const duplicate_failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-publish-failure-proof-duplicate-coordinator",
        "5000",
        "73001",
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "hits publish failure");
    try verifyHitsFailedPreservesPublished(alloc, db_path, initial_generation, "InvalidGraphMetricBuildManifest");
    try assertHitsRecentEventKindCountPair(alloc, db_path, .failed, 1);
}

fn verifyHitsServiceTargetedPublishVerifierFailureProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyHitsFresh(alloc, db_path, initial_generation);

    const rebuild_generation = try addHitsDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhase(alloc, db_path, "hits_authority", rebuild_generation, .publish_generation);
    try assertMetricPhase(alloc, db_path, "hits_authority", .publish_generation, initial_generation);
    try invalidateMetricBuildManifestConfigFingerprintForTest(alloc, db_path, "hits_authority");

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-failure-coordinator",
            "service-hits-publish-failure-coordinator-a",
            "5000",
            "86300",
        );
        if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
            std.debug.print("expected service coordinator process to fail HITS publish verification without publishing\n", .{});
            return error.GraphMetricProcessProofFailed;
        }

        const duplicate_failed = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-failure-coordinator",
            "service-hits-publish-failure-coordinator-b",
            "5000",
            "86301",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "service hits publish failure");
    }
    try verifyHitsFailedPreservesPublished(alloc, db_path, initial_generation, "InvalidGraphMetricBuildManifest");
    try assertHitsRecentEventKindCountPair(alloc, db_path, .failed, 1);
}

fn verifyHitsExhaustedAttemptProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyHitsFresh(alloc, db_path, initial_generation);

    const rebuild_generation = try addHitsDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;
    try prepareMetricBuildToPhaseAndIteration(
        alloc,
        db_path,
        "hits_authority",
        rebuild_generation,
        .hits_hub_reduce_ranks,
        1,
    );
    try assertMetricPhase(alloc, db_path, "hits_authority", .hits_hub_reduce_ranks, initial_generation);

    var now_ms: u64 = 75_000;
    var exhausted_page_id: u64 = 0;
    var last_lease_expires_at_ms: u64 = 0;
    const worker_ids = [_][]const u8{
        "process-hits-exhausted-worker-a",
        "process-hits-exhausted-worker-b",
        "process-hits-exhausted-worker-c",
    };
    const ready_files = [_][]const u8{
        ".zig-cache/tmp/graph-metric-process-hits-exhausted-attempt-a-ready",
        ".zig-cache/tmp/graph-metric-process-hits-exhausted-attempt-b-ready",
        ".zig-cache/tmp/graph-metric-process-hits-exhausted-attempt-c-ready",
    };
    for (worker_ids, ready_files, 0..) |worker_id, ready_file, attempt_index| {
        std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};

        const now_text = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_text);
        try runAndKillMetricPageOwnerAfterReady(
            io,
            harness_exe,
            db_path,
            "hits_authority",
            "hits_hub_reduce_ranks",
            worker_id,
            now_text,
            ready_file,
        );
        const leased = try readLeasedMetricPage(
            alloc,
            db_path,
            "hits_authority",
            .hits_hub_reduce_ranks,
            worker_id,
            "process-dead-cursor",
        );
        if (leased.iteration != 1) {
            std.debug.print("expected exhausted HITS page at iteration 1, got {d}\n", .{leased.iteration});
            return error.GraphMetricProcessProofFailed;
        }
        if (attempt_index == 0) {
            exhausted_page_id = leased.page_id;
        } else if (leased.page_id != exhausted_page_id) {
            std.debug.print("expected HITS exhausted attempts to reclaim page {d}, got {d}\n", .{ exhausted_page_id, leased.page_id });
            return error.GraphMetricProcessProofFailed;
        }
        const expected_attempt = @as(u32, @intCast(attempt_index + 1));
        if (leased.attempt != expected_attempt) {
            std.debug.print("expected HITS page attempt {d}, got {d}\n", .{ expected_attempt, leased.attempt });
            return error.GraphMetricProcessProofFailed;
        }
        last_lease_expires_at_ms = leased.lease_expires_at_ms;
        now_ms = leased.lease_expires_at_ms + 1;
    }

    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{last_lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-exhausted-attempt-proof-coordinator",
        "5000",
        after_expiry_text,
    );
    if (failed.result.failed_builds != 1 or failed.result.published != 0 or failed.result.phases_advanced != 0) {
        std.debug.print("expected coordinator process to fail exhausted HITS page attempts without publishing\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    try verifyHitsFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        initial_generation,
        "GraphMetricBuildPageAttemptsExhausted",
        .hits_hub_reduce_ranks,
        1,
    );
    try assertHitsRecentEventKindCountPair(alloc, db_path, .failed, 1);

    const duplicate_failed = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-exhausted-attempt-proof-duplicate-coordinator",
        "5000",
        after_expiry_text,
    );
    try assertDuplicateCoordinatorDidNotMutate(duplicate_failed, "hits exhausted attempt failure");
    try verifyHitsFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        initial_generation,
        "GraphMetricBuildPageAttemptsExhausted",
        .hits_hub_reduce_ranks,
        1,
    );
    try assertHitsRecentEventKindCountPair(alloc, db_path, .failed, 1);
}

fn verifyHitsActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyHitsFresh(alloc, db_path, initial_generation);

    const rebuild_generation = try addHitsDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const started = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-active-read-proof-coordinator",
        "5000",
        "74000",
    );
    if (!started.durable_progressed or !started.stats.has_lease) {
        std.debug.print("expected coordinator process to start active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "hits-active-read-proof-worker-owner",
        "hits-active-read-proof-worker",
        "5000",
        "74001",
    );
    if (!worker.durable_progressed or worker.result.pages_claimed == 0 or worker.result.pages_completed == 0) {
        std.debug.print("expected worker process to advance active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 2) {
        std.debug.print("expected two HITS graph metric results during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (published_result.graph_metric_results) |result| {
        if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building and
            result.status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
        {
            std.debug.print("expected active HITS query status building or stale, got {}\n", .{result.status.state});
            return error.GraphMetricProcessProofFailed;
        }
        if (result.status.published_generation != initial_generation) {
            std.debug.print(
                "expected HITS published generation {d}, got {d}\n",
                .{ initial_generation, result.status.published_generation },
            );
            return error.GraphMetricGenerationMismatch;
        }
        if (result.status.state == antfly.graph.GraphIndex.GraphMetricState.building and result.status.building_generation != rebuild_generation) {
            std.debug.print(
                "expected HITS building generation {d}, got {d}\n",
                .{
                    rebuild_generation,
                    result.status.building_generation,
                },
            );
            return error.GraphMetricGenerationMismatch;
        }
        if (result.scores.len == 0) {
            std.debug.print("expected active HITS published read to serve prior scores\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        for (result.scores) |score| {
            if (std.mem.eql(u8, score.node, "doc:hub-c")) {
                std.debug.print("active HITS published read exposed rebuilding node {s}\n", .{score.node});
                return error.GraphMetricProcessProofFailed;
            }
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected one HITS graph traversal result during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!std.mem.eql(u8, traversal.nodes[0].key, "doc:authority")) {
        std.debug.print("expected HITS traversal to return doc:authority, got {s}\n", .{traversal.nodes[0].key});
        return error.GraphMetricProcessProofFailed;
    }
    if (traversal.nodes[0].metrics.len != 2) {
        std.debug.print("expected HITS traversal to project authority and hub scores\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.nodes[0].metrics) |metric| {
        if (metric.score == null) {
            std.debug.print("expected HITS traversal metric {s} to serve a prior published score\n", .{metric.name});
            return error.GraphMetricProcessProofFailed;
        }
    }
    if (traversal.metric_status.len != 2) {
        std.debug.print("expected two HITS traversal metric statuses during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.metric_status) |status| {
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building and
            status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
        {
            std.debug.print("expected HITS traversal status building or stale, got {}\n", .{status.state});
            return error.GraphMetricProcessProofFailed;
        }
        if (status.published_generation != initial_generation) {
            std.debug.print("expected HITS traversal published generation {d}, got {d}\n", .{ initial_generation, status.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.building and status.building_generation != rebuild_generation) {
            std.debug.print("expected HITS traversal building generation {d}, got {d}\n", .{ rebuild_generation, status.building_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 3,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building and
        rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
    {
        std.debug.print("expected HITS search rerank status building or stale, got {}\n", .{rerank_status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation) {
        std.debug.print("expected HITS rerank published generation {d}, got {d}\n", .{ initial_generation, rerank_status.published_generation });
        return error.GraphMetricGenerationMismatch;
    }
    if (rerank_status.state == antfly.graph.GraphIndex.GraphMetricState.building and rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected HITS rerank building generation {d}, got {d}\n", .{ rebuild_generation, rerank_status.building_generation });
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:hub-c")) {
            std.debug.print("active HITS search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected HITS reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected HITS reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 3,
        .include_stored = false,
    }));
}

fn verifyPageRankActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", initial_generation);

    const rebuild_generation = try addPageRankDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const started = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-active-read-proof-coordinator",
        "5000",
        "55000",
    );
    if (!started.durable_progressed or !started.stats.has_lease) {
        std.debug.print("expected coordinator process to start active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 1) {
        std.debug.print("expected one PageRank graph metric result during active rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const result = published_result.graph_metric_results[0];
    if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected active PageRank query status building, got {}\n", .{result.status.state});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (result.status.published_generation != initial_generation or result.status.building_generation != rebuild_generation) {
        std.debug.print(
            "expected published generation {d} and building generation {d}, got {d}/{d}\n",
            .{
                initial_generation,
                rebuild_generation,
                result.status.published_generation,
                result.status.building_generation,
            },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (result.scores.len == 0) {
        std.debug.print("expected active PageRank published read to serve prior scores\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    for (result.scores) |score| {
        if (std.mem.eql(u8, score.node, "doc:e")) {
            std.debug.print("active PageRank published read exposed rebuilding node {s}\n", .{score.node});
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected one PageRank graph traversal result during active rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!std.mem.eql(u8, traversal.nodes[0].key, "doc:b")) {
        std.debug.print("expected traversal to return doc:b, got {s}\n", .{traversal.nodes[0].key});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.nodes[0].metrics.len != 1 or traversal.nodes[0].metrics[0].score == null) {
        std.debug.print("expected traversal published metric projection to serve prior PageRank score\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.metric_status.len != 1 or traversal.metric_status[0].state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected traversal metric status building during active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.metric_status[0].published_generation != initial_generation or traversal.metric_status[0].building_generation != rebuild_generation) {
        std.debug.print("expected traversal status to report published/building generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "pagerank",
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected search rerank status building, got {}\n", .{rerank_status.state});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation or rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected rerank status to report published/building generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:e")) {
            std.debug.print("active PageRank search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricPageRankProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricPageRankProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

fn verifyPageRankSameWorkerReplacementAttemptFence(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", target_generation, .scan_edges_and_out_degree);

    const worker_id = "process-pagerank-same-worker";
    const ready_file = ".zig-cache/tmp/graph-metric-process-pagerank-same-worker-ready";
    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillMetricPageOwnerAfterReady(
        io,
        harness_exe,
        db_path,
        "pagerank",
        "scan_edges_and_out_degree",
        worker_id,
        "60000",
        ready_file,
    );

    const dead_page = try readSingleLeasedMetricPage(
        alloc,
        db_path,
        "pagerank",
        .scan_edges_and_out_degree,
        worker_id,
        "process-dead-cursor",
    );
    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const reclaim_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-same-worker-proof-replacement-owner",
        worker_id,
        "5000",
        after_expiry_text,
    );
    if (reclaim_worker.result.pages_claimed == 0 or reclaim_worker.result.pages_completed == 0) {
        std.debug.print("expected same worker id replacement process to reclaim and complete expired PageRank page lease\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    try expectStaleMetricPageAttemptRejected(alloc, db_path, "pagerank", .scan_edges_and_out_degree, dead_page, worker_id);

    _ = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "pagerank-same-worker-proof-coordinator",
        "5000",
        after_expiry_text,
    );
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, "pagerank", target_generation);
}

fn verifyPageRankScanPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyPageRankPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .scan_edges_and_out_degree,
        "scan_edges_and_out_degree",
        "scan",
        ".zig-cache/tmp/graph-metric-process-pagerank-scan-ready",
        "process-pagerank-scan-dead-worker",
        "process-pagerank-scan-reclaim-worker",
        "pagerank-scan-proof-early-owner",
        "pagerank-scan-proof-reclaim-owner",
        "pagerank-scan-proof-coordinator",
    );
}

fn verifyPageRankInitializePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyPageRankPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .initialize_ranks,
        "initialize_ranks",
        "initialize",
        ".zig-cache/tmp/graph-metric-process-pagerank-initialize-ready",
        "process-pagerank-initialize-dead-worker",
        "process-pagerank-initialize-reclaim-worker",
        "pagerank-initialize-proof-early-owner",
        "pagerank-initialize-proof-reclaim-owner",
        "pagerank-initialize-proof-coordinator",
    );
}

fn verifyPageRankContributionPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyPageRankPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .iterate_contributions,
        "iterate_contributions",
        "contribution",
        ".zig-cache/tmp/graph-metric-process-pagerank-contribution-ready",
        "process-pagerank-contribution-dead-worker",
        "process-pagerank-contribution-reclaim-worker",
        "pagerank-contribution-proof-early-owner",
        "pagerank-contribution-proof-reclaim-owner",
        "pagerank-contribution-proof-coordinator",
    );
}

fn verifyPageRankReducePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyPageRankPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .reduce_ranks,
        "reduce_ranks",
        "reduce",
        ".zig-cache/tmp/graph-metric-process-pagerank-reduce-ready",
        "process-pagerank-reduce-dead-worker",
        "process-pagerank-reduce-reclaim-worker",
        "pagerank-reduce-proof-early-owner",
        "pagerank-reduce-proof-reclaim-owner",
        "pagerank-reduce-proof-coordinator",
    );
}

fn verifyPageRankConvergencePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyPageRankPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .check_convergence,
        "check_convergence",
        "convergence",
        ".zig-cache/tmp/graph-metric-process-pagerank-convergence-ready",
        "process-pagerank-convergence-dead-worker",
        "process-pagerank-convergence-reclaim-worker",
        "pagerank-convergence-proof-early-owner",
        "pagerank-convergence-proof-reclaim-owner",
        "pagerank-convergence-proof-coordinator",
    );
}

fn verifyPageRankPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    phase_arg: []const u8,
    phase_label: []const u8,
    ready_file: []const u8,
    dead_worker_id: []const u8,
    reclaim_worker_id: []const u8,
    early_owner_id: []const u8,
    reclaim_owner_id: []const u8,
    coordinator_owner_id: []const u8,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "pagerank",
        target_generation,
        phase,
        0,
        phase_arg,
        phase_label,
        ready_file,
        dead_worker_id,
        reclaim_worker_id,
        early_owner_id,
        reclaim_owner_id,
        coordinator_owner_id,
    );
}

fn verifyEigenvectorScanPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyEigenvectorPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .scan_edges_and_out_degree,
        "scan_edges_and_out_degree",
        "scan",
        ".zig-cache/tmp/graph-metric-process-eigenvector-scan-ready",
        "process-eigenvector-scan-dead-worker",
        "process-eigenvector-scan-reclaim-worker",
        "eigenvector-scan-proof-early-owner",
        "eigenvector-scan-proof-reclaim-owner",
        "eigenvector-scan-proof-coordinator",
    );
}

fn verifyEigenvectorInitializePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyEigenvectorPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .initialize_ranks,
        "initialize_ranks",
        "initialize",
        ".zig-cache/tmp/graph-metric-process-eigenvector-initialize-ready",
        "process-eigenvector-initialize-dead-worker",
        "process-eigenvector-initialize-reclaim-worker",
        "eigenvector-initialize-proof-early-owner",
        "eigenvector-initialize-proof-reclaim-owner",
        "eigenvector-initialize-proof-coordinator",
    );
}

fn verifyEigenvectorContributionPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyEigenvectorPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .iterate_contributions,
        "iterate_contributions",
        "contribution",
        ".zig-cache/tmp/graph-metric-process-eigenvector-contribution-ready",
        "process-eigenvector-contribution-dead-worker",
        "process-eigenvector-contribution-reclaim-worker",
        "eigenvector-contribution-proof-early-owner",
        "eigenvector-contribution-proof-reclaim-owner",
        "eigenvector-contribution-proof-coordinator",
    );
}

fn verifyEigenvectorReducePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyEigenvectorPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .reduce_ranks,
        "reduce_ranks",
        "reduce",
        ".zig-cache/tmp/graph-metric-process-eigenvector-reduce-ready",
        "process-eigenvector-reduce-dead-worker",
        "process-eigenvector-reduce-reclaim-worker",
        "eigenvector-reduce-proof-early-owner",
        "eigenvector-reduce-proof-reclaim-owner",
        "eigenvector-reduce-proof-coordinator",
    );
}

fn verifyEigenvectorConvergencePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyEigenvectorPageLeaseReclaim(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        target_generation,
        .check_convergence,
        "check_convergence",
        "convergence",
        ".zig-cache/tmp/graph-metric-process-eigenvector-convergence-ready",
        "process-eigenvector-convergence-dead-worker",
        "process-eigenvector-convergence-reclaim-worker",
        "eigenvector-convergence-proof-early-owner",
        "eigenvector-convergence-proof-reclaim-owner",
        "eigenvector-convergence-proof-coordinator",
    );
}

fn verifyEigenvectorPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    phase_arg: []const u8,
    phase_label: []const u8,
    ready_file: []const u8,
    dead_worker_id: []const u8,
    reclaim_worker_id: []const u8,
    early_owner_id: []const u8,
    reclaim_owner_id: []const u8,
    coordinator_owner_id: []const u8,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "eigenvector",
        target_generation,
        phase,
        0,
        phase_arg,
        phase_label,
        ready_file,
        dead_worker_id,
        reclaim_worker_id,
        early_owner_id,
        reclaim_owner_id,
        coordinator_owner_id,
    );
}

fn verifyHitsAuthorityContributionPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "hits_authority",
        target_generation,
        .iterate_contributions,
        0,
        "iterate_contributions",
        "HITS authority contribution",
        ".zig-cache/tmp/graph-metric-process-hits-authority-contribution-ready",
        "process-hits-authority-contribution-dead-worker",
        "process-hits-authority-contribution-reclaim-worker",
        "hits-authority-contribution-proof-early-owner",
        "hits-authority-contribution-proof-reclaim-owner",
        "hits-authority-contribution-proof-coordinator",
    );
    try verifyHitsFresh(alloc, db_path, target_generation);
}

fn verifyHitsAuthorityReducePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "hits_authority",
        target_generation,
        .reduce_ranks,
        0,
        "reduce_ranks",
        "HITS authority reduce",
        ".zig-cache/tmp/graph-metric-process-hits-authority-reduce-ready",
        "process-hits-authority-reduce-dead-worker",
        "process-hits-authority-reduce-reclaim-worker",
        "hits-authority-reduce-proof-early-owner",
        "hits-authority-reduce-proof-reclaim-owner",
        "hits-authority-reduce-proof-coordinator",
    );
    try verifyHitsFresh(alloc, db_path, target_generation);
}

fn verifyHitsConvergencePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "hits_authority",
        target_generation,
        .check_convergence,
        0,
        "check_convergence",
        "HITS convergence",
        ".zig-cache/tmp/graph-metric-process-hits-convergence-ready",
        "process-hits-convergence-dead-worker",
        "process-hits-convergence-reclaim-worker",
        "hits-convergence-proof-early-owner",
        "hits-convergence-proof-reclaim-owner",
        "hits-convergence-proof-coordinator",
    );
    try verifyHitsFresh(alloc, db_path, target_generation);
}

fn verifyHitsHubContributionPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "hits_authority",
        target_generation,
        .hits_hub_contributions,
        0,
        "hits_hub_contributions",
        "HITS hub contribution",
        ".zig-cache/tmp/graph-metric-process-hits-hub-contribution-ready",
        "process-hits-hub-contribution-dead-worker",
        "process-hits-hub-contribution-reclaim-worker",
        "hits-hub-contribution-proof-early-owner",
        "hits-hub-contribution-proof-reclaim-owner",
        "hits-hub-contribution-proof-coordinator",
    );
    try verifyHitsFresh(alloc, db_path, target_generation);
}

fn verifyHitsHubReducePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "hits_authority",
        target_generation,
        .hits_hub_reduce_ranks,
        0,
        "hits_hub_reduce_ranks",
        "HITS hub reduce",
        ".zig-cache/tmp/graph-metric-process-hits-hub-reduce-ready",
        "process-hits-hub-reduce-dead-worker",
        "process-hits-hub-reduce-reclaim-worker",
        "hits-hub-reduce-proof-early-owner",
        "hits-hub-reduce-proof-reclaim-owner",
        "hits-hub-reduce-proof-coordinator",
    );
    try verifyHitsFresh(alloc, db_path, target_generation);
}

fn verifyPageRankLaterContributionPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "pagerank",
        target_generation,
        .iterate_contributions,
        1,
        "iterate_contributions",
        "later contribution",
        ".zig-cache/tmp/graph-metric-process-pagerank-later-contribution-ready",
        "process-pagerank-later-contribution-dead-worker",
        "process-pagerank-later-contribution-reclaim-worker",
        "pagerank-later-contribution-proof-early-owner",
        "pagerank-later-contribution-proof-reclaim-owner",
        "pagerank-later-contribution-proof-coordinator",
    );
}

fn verifyPageRankLaterReducePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "pagerank",
        target_generation,
        .reduce_ranks,
        1,
        "reduce_ranks",
        "later reduce",
        ".zig-cache/tmp/graph-metric-process-pagerank-later-reduce-ready",
        "process-pagerank-later-reduce-dead-worker",
        "process-pagerank-later-reduce-reclaim-worker",
        "pagerank-later-reduce-proof-early-owner",
        "pagerank-later-reduce-proof-reclaim-owner",
        "pagerank-later-reduce-proof-coordinator",
    );
}

fn verifyPageRankLaterConvergencePageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try verifyMetricPageLeaseReclaimAtIteration(
        alloc,
        io,
        harness_exe,
        antfly_exe,
        db_path,
        "pagerank",
        target_generation,
        .check_convergence,
        1,
        "check_convergence",
        "later convergence",
        ".zig-cache/tmp/graph-metric-process-pagerank-later-convergence-ready",
        "process-pagerank-later-convergence-dead-worker",
        "process-pagerank-later-convergence-reclaim-worker",
        "pagerank-later-convergence-proof-early-owner",
        "pagerank-later-convergence-proof-reclaim-owner",
        "pagerank-later-convergence-proof-coordinator",
    );
}

fn verifyMetricPageLeaseReclaimAtIteration(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    iteration: u32,
    phase_arg: []const u8,
    phase_label: []const u8,
    ready_file: []const u8,
    dead_worker_id: []const u8,
    reclaim_worker_id: []const u8,
    early_owner_id: []const u8,
    reclaim_owner_id: []const u8,
    coordinator_owner_id: []const u8,
) !void {
    try prepareMetricBuildToPhaseAndIteration(
        alloc,
        db_path,
        metric_name,
        target_generation,
        phase,
        iteration,
    );

    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillMetricPageOwnerAfterReady(
        io,
        harness_exe,
        db_path,
        metric_name,
        phase_arg,
        dead_worker_id,
        "10000",
        ready_file,
    );

    const dead_page = try readSingleLeasedMetricPage(
        alloc,
        db_path,
        metric_name,
        phase,
        dead_worker_id,
        "process-dead-cursor",
    );
    const before_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms - 1});
    defer alloc.free(before_expiry_text);
    const early_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        early_owner_id,
        reclaim_worker_id,
        "5000",
        before_expiry_text,
    );
    if (early_worker.result.pages_claimed != 0 or early_worker.result.pages_completed != 0) {
        std.debug.print("expected {s} replacement worker to be fenced before {s} page lease expiry\n", .{ metric_name, phase_label });
        return error.GraphMetricProcessProofFailed;
    }
    _ = try readSingleLeasedMetricPage(
        alloc,
        db_path,
        metric_name,
        phase,
        dead_worker_id,
        "process-dead-cursor",
    );

    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const reclaim_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        reclaim_owner_id,
        reclaim_worker_id,
        "5000",
        after_expiry_text,
    );
    if (reclaim_worker.result.pages_claimed == 0 or reclaim_worker.result.pages_completed == 0) {
        std.debug.print("expected {s} replacement worker to reclaim and complete expired {s} page lease\n", .{ metric_name, phase_label });
        return error.GraphMetricProcessProofFailed;
    }
    try expectReclaimedMetricPageCompleted(
        alloc,
        db_path,
        metric_name,
        phase,
        dead_page,
        reclaim_worker_id,
    );
    try expectStaleMetricPageAttemptRejected(alloc, db_path, metric_name, phase, dead_page, dead_worker_id);

    _ = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        coordinator_owner_id,
        "5000",
        after_expiry_text,
    );
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyMetricFresh(alloc, db_path, metric_name, target_generation);
}

fn verifyWorkerPageLeaseReclaim(
    alloc: std.mem.Allocator,
    io: std.Io,
    harness_exe: []const u8,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareDegreeScanBuild(alloc, db_path, target_generation);

    const ready_file = ".zig-cache/tmp/graph-metric-process-worker-page-ready";
    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillDegreePageOwnerAfterReady(
        io,
        harness_exe,
        db_path,
        "process-dead-worker",
        "10000",
        ready_file,
    );

    const dead_page = try readSingleLeasedDegreePage(alloc, db_path, "process-dead-worker", "process-dead-cursor");
    const before_expiry_ms = dead_page.lease_expires_at_ms - 1;
    const before_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{before_expiry_ms});
    defer alloc.free(before_expiry_text);
    const early_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "worker-page-proof-early-owner",
        "process-reclaim-worker",
        "5000",
        before_expiry_text,
    );
    if (early_worker.result.pages_claimed != 0 or early_worker.result.pages_completed != 0) {
        std.debug.print("expected replacement worker to be fenced before page lease expiry\n", .{});
        return error.GraphMetricWorkerPageProofFailed;
    }
    _ = try readSingleLeasedDegreePage(alloc, db_path, "process-dead-worker", "process-dead-cursor");

    const after_expiry_text = try std.fmt.allocPrint(alloc, "{d}", .{dead_page.lease_expires_at_ms + 1});
    defer alloc.free(after_expiry_text);
    const reclaim_worker = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "worker-page-proof-reclaim-owner",
        "process-reclaim-worker",
        "5000",
        after_expiry_text,
    );
    if (reclaim_worker.result.pages_claimed == 0 or reclaim_worker.result.pages_completed == 0) {
        std.debug.print("expected replacement worker to reclaim and complete expired page lease\n", .{});
        return error.GraphMetricWorkerPageProofFailed;
    }
    try expectReclaimedMetricPageCompleted(
        alloc,
        db_path,
        "degree",
        .scan_edges_and_out_degree,
        dead_page,
        "process-reclaim-worker",
    );
    try expectStaleDegreePageAttemptRejected(alloc, db_path, dead_page);

    _ = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "worker-page-proof-coordinator",
        "5000",
        after_expiry_text,
    );
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyDegreeFresh(alloc, db_path, target_generation);
}

fn verifyWorkerRuntimeSameWorkerLeaseFencing(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareDegreeScanBuild(alloc, db_path, target_generation);

    const ready_file = ".zig-cache/tmp/graph-metric-process-worker-runtime-ready";
    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillWorkerRoleAfterReady(
        io,
        antfly_exe,
        db_path,
        "worker-runtime-proof-owner-a",
        "worker-runtime-proof-worker",
        "5000",
        "10000",
        ready_file,
    );

    const duplicate = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "worker-runtime-proof-owner-b",
        "worker-runtime-proof-worker",
        "5000",
        "14999",
    );
    if (duplicate.durable_progressed or duplicate.stats.has_lease or duplicate.stats.lease_acquire_failures == 0) {
        std.debug.print("expected duplicate same-worker runtime owner to be fenced before lease expiry\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    const replacement = try runWorkerRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "worker-runtime-proof-owner-b",
        "worker-runtime-proof-worker",
        "5000",
        "15001",
    );
    if (!replacement.stats.has_lease or replacement.stats.acquisition_count == 0 or replacement.stats.takeover_count == 0) {
        std.debug.print("expected same-worker replacement runtime owner to acquire expired worker lease\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }
    if (!replacement.durable_progressed or replacement.result.pages_claimed == 0 or replacement.result.pages_completed == 0) {
        std.debug.print("expected same-worker replacement runtime owner to advance durable page work after takeover\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyDegreeFresh(alloc, db_path, target_generation);
}

fn runSupervisorProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "supervise",
        "--db-path",
        db_path,
        "--executable",
        antfly_exe,
        "--coordinator-owner-id",
        "process-degree-coordinator",
        "--worker-pool-owner-id",
        "process-degree-worker-pool",
        "--worker-ids",
        "process-worker-a,process-worker-b",
        "--ticks",
        "8",
        "--max-idle-ticks",
        "2",
        "--supervisor-rounds",
        "80",
        "--supervisor-idle-rounds",
        "1",
        "--tick-ms",
        "0",
        "--max-restarts",
        "0",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
    };
    const result = try std.process.run(alloc, io, .{
        .argv = argv[0..],
        .reserve_amount = 512,
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) {
            std.debug.print(
                "graph metric supervisor exited with code {d}\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ code, result.stdout, result.stderr },
            );
            return error.SupervisorProcessFailed;
        },
        else => {
            std.debug.print(
                "graph metric supervisor terminated unexpectedly\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ result.stdout, result.stderr },
            );
            return error.SupervisorProcessFailed;
        },
    }

    try verifyProcessSummaryJsonNoRawOperationalFields(alloc, result.stdout);
    var parsed = try std.json.parseFromSlice(SupervisorSummary, alloc, result.stdout, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    if (!parsed.value.succeeded or !std.mem.eql(u8, parsed.value.exit_reason, "idle")) {
        std.debug.print("unexpected supervisor summary:\n{s}\n", .{result.stdout});
        return error.SupervisorProcessFailed;
    }
    if (parsed.value.rounds_executed == 0) return error.SupervisorProcessFailed;
    try verifyProcessSupervisorTelemetry(
        parsed.value,
        "process-degree-coordinator",
        "process-degree-worker-pool",
    );
}

fn runLaunchProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    label: []const u8,
) !void {
    const summary_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/graph-metric-process-launch-{s}-summaries", .{label});
    defer alloc.free(summary_dir);
    const coordinator_owner_id = try std.fmt.allocPrint(alloc, "process-launch-{s}-coordinator", .{label});
    defer alloc.free(coordinator_owner_id);
    const worker_pool_owner_id = try std.fmt.allocPrint(alloc, "process-launch-{s}-worker-pool", .{label});
    defer alloc.free(worker_pool_owner_id);
    const worker_ids = try std.fmt.allocPrint(alloc, "process-launch-{s}-worker-a,process-launch-{s}-worker-b", .{ label, label });
    defer alloc.free(worker_ids);

    std.Io.Dir.cwd().deleteTree(io, summary_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, summary_dir) catch {};
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "launch",
        "--db-path",
        db_path,
        "--executable",
        antfly_exe,
        "--coordinator-owner-id",
        coordinator_owner_id,
        "--worker-pool-owner-id",
        worker_pool_owner_id,
        "--worker-ids",
        worker_ids,
        "--ticks",
        "16",
        "--max-idle-ticks",
        "4",
        "--supervisor-rounds",
        "20",
        "--supervisor-idle-rounds",
        "1",
        "--tick-ms",
        "0",
        "--max-restarts",
        "0",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
        "--summary-dir",
        summary_dir,
    };
    const result = try std.process.run(alloc, io, .{
        .argv = argv[0..],
        .reserve_amount = 512,
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) {
            std.debug.print(
                "graph metric launcher exited with code {d}\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ code, result.stdout, result.stderr },
            );
            return error.SupervisorProcessFailed;
        },
        else => {
            std.debug.print(
                "graph metric launcher terminated unexpectedly\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ result.stdout, result.stderr },
            );
            return error.SupervisorProcessFailed;
        },
    }

    try verifyProcessSummaryJsonNoRawOperationalFields(alloc, result.stdout);
    var parsed = try std.json.parseFromSlice(SupervisorSummary, alloc, result.stdout, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer parsed.deinit();
    if (!parsed.value.succeeded or !std.mem.eql(u8, parsed.value.exit_reason, "idle")) {
        std.debug.print("unexpected launcher summary:\n{s}\nstderr:\n{s}\n", .{ result.stdout, result.stderr });
        return error.SupervisorProcessFailed;
    }
    if (parsed.value.rounds_executed == 0) return error.SupervisorProcessFailed;
    try verifyProcessSupervisorTelemetry(parsed.value, coordinator_owner_id, worker_pool_owner_id);
}

fn verifyProcessSummaryJsonNoRawOperationalFields(alloc: std.mem.Allocator, stdout: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stdout, .{});
    defer parsed.deinit();
    try verifyJsonNoRawOperationalFields(parsed.value, error.SupervisorProcessFailed);
}

fn verifyProcessSupervisorTelemetry(
    summary: SupervisorSummary,
    coordinator_owner_id: []const u8,
    worker_pool_owner_id: []const u8,
) !void {
    const coordinator = summary.coordinator.telemetry orelse return error.SupervisorProcessFailed;
    try verifyChildTelemetry(
        coordinator,
        .coordinator,
        coordinator_owner_id,
        0,
        false,
    );

    const worker_pool = summary.worker_pool.telemetry orelse return error.SupervisorProcessFailed;
    try verifyChildTelemetry(
        worker_pool,
        .worker_pool,
        worker_pool_owner_id,
        2,
        true,
    );
}

fn verifyChildTelemetry(
    telemetry: ChildRuntimeTelemetry,
    role: RuntimeRole,
    owner_id: []const u8,
    worker_count: usize,
    expect_worker_hash: bool,
) !void {
    if (telemetry.role != role) return error.SupervisorProcessFailed;
    const expected_owner_hash = std.hash.Wyhash.hash(0, owner_id);
    if (telemetry.runtime_id_hash != expected_owner_hash) return error.SupervisorProcessFailed;
    if (telemetry.owner_id_hash != expected_owner_hash) return error.SupervisorProcessFailed;
    if (telemetry.lease_key_hash == 0) return error.SupervisorProcessFailed;
    if (expect_worker_hash) {
        if (telemetry.worker_id_hash == 0) return error.SupervisorProcessFailed;
    } else if (telemetry.worker_id_hash != 0) {
        return error.SupervisorProcessFailed;
    }
    if (telemetry.worker_count != worker_count) return error.SupervisorProcessFailed;
    if (!telemetry.lease_owned) return error.SupervisorProcessFailed;
    if (!telemetry.has_lease) return error.SupervisorProcessFailed;
    if (telemetry.acquisition_count == 0) return error.SupervisorProcessFailed;
    if (telemetry.ticks_started == 0) return error.SupervisorProcessFailed;
    if (telemetry.ticks_completed == 0) return error.SupervisorProcessFailed;
    if (telemetry.error_ticks != 0) return error.SupervisorProcessFailed;
    if (telemetry.has_last_error) return error.SupervisorProcessFailed;
}

fn verifyCoordinatorLeaseExpiryTakeover(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
) !void {
    const ready_file = ".zig-cache/tmp/graph-metric-process-lease-coordinator-ready";
    std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};
    try runAndKillCoordinatorAfterReady(
        io,
        antfly_exe,
        db_path,
        "lease-proof-coordinator-a",
        "5000",
        ready_file,
    );
    defer std.Io.Dir.cwd().deleteFile(io, ready_file) catch {};

    const worker_pool = try runWorkerPoolRoleProcess(
        alloc,
        io,
        antfly_exe,
        db_path,
        "lease-proof-worker-pool",
        "5000",
    );
    if (!worker_pool.durable_progressed or !worker_pool.stats.has_lease) {
        std.debug.print("expected worker pool to acquire independent lease and complete work\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    const coordinator_b_blocked = try runCoordinatorRoleProcess(
        alloc,
        io,
        antfly_exe,
        db_path,
        "lease-proof-coordinator-b",
        "5000",
    );
    if (coordinator_b_blocked.durable_progressed or coordinator_b_blocked.stats.has_lease or coordinator_b_blocked.stats.lease_acquire_failures == 0) {
        std.debug.print("expected duplicate coordinator to be fenced before lease expiry\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    platform.time.sleepNs(5100 * std.time.ns_per_ms);

    const coordinator_b_takeover = try runCoordinatorRoleProcess(
        alloc,
        io,
        antfly_exe,
        db_path,
        "lease-proof-coordinator-b",
        "5000",
    );
    if (!coordinator_b_takeover.stats.has_lease or coordinator_b_takeover.stats.acquisition_count == 0 or coordinator_b_takeover.stats.takeover_count == 0) {
        std.debug.print("expected replacement coordinator to acquire expired lease\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }
    if (!coordinator_b_takeover.durable_progressed) {
        std.debug.print("expected replacement coordinator to advance durable work after takeover\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }
}

fn verifyServiceTargetedMetricOwnerRestartProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
    defer api_runtime.deinit();
    const base_uri = try api_runtime.baseUri(alloc);
    defer alloc.free(base_uri);

    const coordinator_ready_file = ".zig-cache/tmp/graph-metric-process-service-coordinator-ready";
    std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
    try runAndKillServiceCoordinatorAfterReady(
        io,
        antfly_exe,
        base_uri,
        "service-process-coordinator",
        "service-process-coordinator-a",
        "200",
        "1000",
        coordinator_ready_file,
    );

    const coordinator_b_fenced = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-process-coordinator",
        "service-process-coordinator-b",
        "200",
        "1100",
    );
    if (coordinator_b_fenced.durable_progressed or coordinator_b_fenced.stats.has_lease or coordinator_b_fenced.stats.lease_acquire_failures == 0) {
        std.debug.print("expected duplicate service coordinator process to be fenced before lease expiry\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    const coordinator_b_takeover = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-process-coordinator",
        "service-process-coordinator-b",
        "200",
        "1301",
    );
    if (coordinator_b_takeover.stats.takeover_count == 0) {
        std.debug.print("expected replacement service coordinator process to acquire expired lease\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    const worker_ready_file = ".zig-cache/tmp/graph-metric-process-service-worker-pool-ready";
    std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
    try runAndKillServiceWorkerPoolAfterReady(
        io,
        antfly_exe,
        base_uri,
        "service-process-worker-pool",
        "service-process-worker-pool-a",
        "service-process-worker-a,service-process-worker-b",
        "200",
        "2000",
        worker_ready_file,
    );

    const worker_pool_b_fenced = try runServiceWorkerPoolRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-process-worker-pool",
        "service-process-worker-pool-b",
        "service-process-worker-a,service-process-worker-b",
        "200",
        "2100",
    );
    if (worker_pool_b_fenced.durable_progressed or worker_pool_b_fenced.stats.has_lease or worker_pool_b_fenced.stats.lease_acquire_failures == 0) {
        std.debug.print("expected duplicate service worker-pool process to be fenced before lease expiry\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    const worker_pool_b_takeover = try runServiceWorkerPoolRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-process-worker-pool",
        "service-process-worker-pool-b",
        "service-process-worker-a,service-process-worker-b",
        "200",
        "2301",
    );
    if (worker_pool_b_takeover.stats.takeover_count == 0) {
        std.debug.print("expected replacement service worker-pool process to acquire expired lease\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    var now_ms: u64 = 2400;
    var idle_rounds: usize = 0;
    for (0..80) |_| {
        const now_coordinator = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_coordinator);
        _ = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-process-coordinator",
            "service-process-coordinator-b",
            "200",
            now_coordinator,
        );
        now_ms += 1;

        const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_worker);
        const worker_summary = try runServiceWorkerPoolRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-process-worker-pool",
            "service-process-worker-pool-b",
            "service-process-worker-a,service-process-worker-b",
            "200",
            now_worker,
        );
        now_ms += 1;

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus(metric_name);
        defer status.deinit(alloc);
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
            if (status.published_generation != target_generation) return error.GraphMetricGenerationMismatch;
            return;
        }
        if (worker_summary.durable_progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= 8) break;
        }
    }

    return error.GraphMetricBuildNotComplete;
}

fn verifyDegreeServiceTargetedPublishAndCleanupRestartProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareDegreePublishReadyBuild(alloc, db_path, target_generation);
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-publish-cleanup-coordinator",
            "service-degree-publish-cleanup-coordinator-a",
            "5000",
            "75000",
        );
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish degree before cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "degree", .cleanup_old_generations, target_generation);

        const duplicate_publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-publish-cleanup-coordinator",
            "service-degree-publish-cleanup-coordinator-b",
            "5000",
            "75001",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "service degree cleanup");
        try assertOpenDbMetricPhase(alloc, &db, "degree", .cleanup_old_generations, target_generation);

        const cleanup_ready_file = ".zig-cache/tmp/graph-metric-process-service-degree-publish-cleanup-worker-pool-ready";
        std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        try runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
            io,
            antfly_exe,
            base_uri,
            "service-degree-publish-cleanup-worker-pool",
            "service-degree-publish-cleanup-worker-pool-killed",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "75002",
            "1",
            cleanup_ready_file,
        );
        try assertOpenDbMetricPhase(alloc, &db, "degree", .cleanup_old_generations, target_generation);

        const fenced_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-publish-cleanup-worker-pool",
            "service-degree-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "75100",
            "1",
        );
        if (fenced_cleanup.durable_progressed or fenced_cleanup.stats.has_lease or fenced_cleanup.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate service degree cleanup worker-pool to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "degree", .cleanup_old_generations, target_generation);

        const takeover_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-publish-cleanup-worker-pool",
            "service-degree-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "75203",
            "1",
        );
        if (takeover_cleanup.stats.takeover_count == 0 or takeover_cleanup.result.pages_claimed != 1 or takeover_cleanup.result.pages_completed != 1) {
            std.debug.print("expected replacement service worker-pool process to finish degree cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyDegreeFresh(alloc, db_path, target_generation);
}

fn verifyDegreeServiceTargetedMultiPageWorkerPoolProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const coordinator_runtime = "service-degree-multipage-coordinator";
        const worker_pool_runtime = "service-degree-multipage-worker-pool";
        const coordinator_ready_file = ".zig-cache/tmp/graph-metric-process-service-degree-multipage-coordinator-ready";
        std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        try runAndKillServiceCoordinatorAfterReady(
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-a",
            "200",
            "81000",
            coordinator_ready_file,
        );
        try assertOpenDbMetricActivePhase(alloc, &db, "degree", .prepare_generation, target_generation);

        const coordinator_fenced = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-b",
            "200",
            "81100",
        );
        if (coordinator_fenced.durable_progressed or coordinator_fenced.stats.has_lease or coordinator_fenced.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate multi-page service degree coordinator to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }

        const coordinator_takeover = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-b",
            "200",
            "81201",
        );
        if (coordinator_takeover.stats.takeover_count == 0) {
            std.debug.print("expected replacement multi-page service degree coordinator to acquire expired lease\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricActivePhase(alloc, &db, "degree", .prepare_generation, target_generation);

        const prepare = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            worker_pool_runtime,
            "service-degree-multipage-worker-pool-a",
            "service-process-worker-a,service-process-worker-b",
            "5000",
            "81202",
            "4",
        );
        if (prepare.result.pages_completed != 1 or prepare.stats.worker_count != 2) {
            std.debug.print("expected service worker-pool to complete the single degree prepare page with two configured workers\n", .{});
            return error.GraphMetricProcessProofFailed;
        }

        const to_scan = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-b",
            "5000",
            "81203",
        );
        if (to_scan.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to advance degree build to scan phase\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricActivePhase(alloc, &db, "degree", .scan_edges_and_out_degree, target_generation);

        const worker_ready_file = ".zig-cache/tmp/graph-metric-process-service-degree-multipage-worker-pool-ready";
        std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
        try runAndKillServiceWorkerPoolAfterReady(
            io,
            antfly_exe,
            base_uri,
            worker_pool_runtime,
            "service-degree-multipage-worker-pool-b",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "81204",
            worker_ready_file,
        );

        const worker_pool_fenced = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            worker_pool_runtime,
            "service-degree-multipage-worker-pool-c",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "81300",
            "4",
        );
        if (worker_pool_fenced.durable_progressed or worker_pool_fenced.stats.has_lease or worker_pool_fenced.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate multi-page service degree worker-pool to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }

        const scan = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            worker_pool_runtime,
            "service-degree-multipage-worker-pool-c",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "81405",
            "4",
        );
        if (scan.stats.takeover_count == 0 or scan.result.pages_completed == 0 or scan.stats.worker_count != 2) {
            std.debug.print("expected replacement service worker-pool to take over and complete remaining degree scan pages, got {d}\n", .{scan.result.pages_completed});
            return error.GraphMetricProcessProofFailed;
        }

        const to_reduce = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-c",
            "5000",
            "81406",
        );
        if (to_reduce.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to advance degree build to reduce phase\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricActivePhase(alloc, &db, "degree", .reduce_ranks, target_generation);

        const reduce = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            worker_pool_runtime,
            "service-degree-multipage-worker-pool-c",
            "service-process-worker-a,service-process-worker-b",
            "5000",
            "81407",
            "4",
        );
        if (reduce.result.pages_completed < 2 or reduce.stats.worker_count != 2) {
            std.debug.print("expected service worker-pool to complete multiple degree reduce pages, got {d}\n", .{reduce.result.pages_completed});
            return error.GraphMetricProcessProofFailed;
        }

        const to_publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-d",
            "5000",
            "81408",
        );
        if (to_publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to advance degree build to publish phase\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricActivePhase(alloc, &db, "degree", .publish_generation, target_generation);

        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-degree-multipage-coordinator-e",
            "5000",
            "81409",
        );
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish multi-page degree build\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "degree", .cleanup_old_generations, target_generation);

        var cleanup_now_ms: u64 = 81410;
        var cleanup_progressed = false;
        var cleanup_fresh = false;
        for (0..8) |i| {
            var status = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("degree");
            defer status.deinit(alloc);
            if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
                cleanup_fresh = true;
                break;
            }

            const owner_id = try std.fmt.allocPrint(alloc, "service-degree-multipage-worker-pool-cleanup-{d}", .{i});
            defer alloc.free(owner_id);
            const cleanup_now = try std.fmt.allocPrint(alloc, "{d}", .{cleanup_now_ms});
            defer alloc.free(cleanup_now);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                worker_pool_runtime,
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                cleanup_now,
                "4",
            );
            if (cleanup.stats.worker_count != 2) {
                std.debug.print("expected service cleanup worker-pool to keep two configured workers\n", .{});
                return error.GraphMetricProcessProofFailed;
            }
            cleanup_progressed = cleanup_progressed or cleanup.durable_progressed;
            cleanup_now_ms += 1;
        }
        if (!cleanup_fresh and !cleanup_progressed) {
            std.debug.print("expected service worker-pool to make cleanup progress for multi-page degree build\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyDegreeFresh(alloc, db_path, target_generation);
}

fn verifyPageRankServiceTargetedPublishAndCleanupRestartProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "pagerank", target_generation, .publish_generation);
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-cleanup-coordinator",
            "service-pagerank-publish-cleanup-coordinator-a",
            "5000",
            "76000",
        );
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish PageRank before cleanup\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "pagerank", .cleanup_old_generations, target_generation);

        const duplicate_publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-cleanup-coordinator",
            "service-pagerank-publish-cleanup-coordinator-b",
            "5000",
            "76001",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "service pagerank cleanup");
        try assertOpenDbMetricPhase(alloc, &db, "pagerank", .cleanup_old_generations, target_generation);

        const cleanup_ready_file = ".zig-cache/tmp/graph-metric-process-service-pagerank-publish-cleanup-worker-pool-ready";
        std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        try runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-cleanup-worker-pool",
            "service-pagerank-publish-cleanup-worker-pool-killed",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "76002",
            "1",
            cleanup_ready_file,
        );
        try assertOpenDbMetricPhase(alloc, &db, "pagerank", .cleanup_old_generations, target_generation);

        const fenced_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-cleanup-worker-pool",
            "service-pagerank-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "76100",
            "4",
        );
        if (fenced_cleanup.durable_progressed or fenced_cleanup.stats.has_lease or fenced_cleanup.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate service PageRank cleanup worker-pool to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "pagerank", .cleanup_old_generations, target_generation);

        const takeover_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-publish-cleanup-worker-pool",
            "service-pagerank-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "76203",
            "4",
        );
        if (takeover_cleanup.stats.takeover_count == 0 or takeover_cleanup.result.pages_claimed == 0 or takeover_cleanup.result.pages_completed == 0) {
            std.debug.print("expected replacement service PageRank cleanup worker-pool to take over and advance cleanup\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }

        var cleanup_progressed = takeover_cleanup.durable_progressed;
        for (0..6) |i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-pagerank-publish-cleanup-worker-pool-final-{d}", .{i});
            defer alloc.free(owner_id);
            const now_ms = try std.fmt.allocPrint(alloc, "{d}", .{76204 + i});
            defer alloc.free(now_ms);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                "service-pagerank-publish-cleanup-worker-pool",
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                now_ms,
                "4",
            );
            cleanup_progressed = cleanup_progressed or cleanup.durable_progressed or cleanup.result.pages_claimed != 0 or cleanup.result.pages_completed != 0;
        }
        if (!cleanup_progressed) {
            std.debug.print("expected service PageRank cleanup worker-pool processes to make cleanup progress\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }
    try verifyPageRankFixedIterationMetadata(alloc, db_path, target_generation, 1);
}

fn verifyPageRankServiceTargetedMultiPageWorkerPoolProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const coordinator_runtime = "service-pagerank-multipage-coordinator";
        const worker_pool_runtime = "service-pagerank-multipage-worker-pool";
        var now_ms: u64 = 82000;
        const coordinator_ready_file = ".zig-cache/tmp/graph-metric-process-service-pagerank-multipage-coordinator-ready";
        std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        try runAndKillServiceCoordinatorAfterReady(
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-pagerank-multipage-coordinator-start",
            "200",
            "82000",
            coordinator_ready_file,
        );
        try assertOpenDbMetricActivePhase(alloc, &db, "pagerank", .prepare_generation, target_generation);

        const coordinator_fenced = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-pagerank-multipage-coordinator-replacement",
            "200",
            "82100",
        );
        if (coordinator_fenced.durable_progressed or coordinator_fenced.stats.has_lease or coordinator_fenced.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate multi-page service PageRank coordinator to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }

        const coordinator_takeover = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-pagerank-multipage-coordinator-replacement",
            "200",
            "82201",
        );
        if (coordinator_takeover.stats.takeover_count == 0) {
            std.debug.print("expected replacement multi-page service PageRank coordinator to acquire expired lease\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
        now_ms = 82202;
        try assertOpenDbMetricActivePhase(alloc, &db, "pagerank", .prepare_generation, target_generation);

        const phases = [_]antfly.graph.GraphIndex.GraphMetricBuildPhase{
            .prepare_generation,
            .scan_edges_and_out_degree,
            .initialize_ranks,
            .iterate_contributions,
            .reduce_ranks,
            .check_convergence,
        };
        for (phases, 0..) |phase, i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-pagerank-multipage-worker-pool-{d}", .{i});
            defer alloc.free(owner_id);
            const worker_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(worker_now);
            const worker = if (phase == .scan_edges_and_out_degree) blk: {
                const worker_ready_file = ".zig-cache/tmp/graph-metric-process-service-pagerank-multipage-worker-pool-ready";
                std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                defer std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                try runAndKillServiceWorkerPoolAfterReady(
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-pagerank-multipage-worker-pool-killed",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    worker_now,
                    worker_ready_file,
                );
                now_ms += 1;

                const fenced_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
                defer alloc.free(fenced_now);
                const worker_pool_fenced = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-pagerank-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    fenced_now,
                    "4",
                );
                now_ms += 1;
                if (worker_pool_fenced.durable_progressed or worker_pool_fenced.stats.has_lease or worker_pool_fenced.stats.lease_acquire_failures == 0) {
                    std.debug.print("expected duplicate multi-page service PageRank worker-pool to be fenced before lease expiry\n", .{});
                    return error.GraphMetricLeaseProofFailed;
                }

                const takeover_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms + 205});
                defer alloc.free(takeover_now);
                const replacement = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-pagerank-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    takeover_now,
                    "4",
                );
                now_ms += 206;
                if (replacement.stats.takeover_count == 0) {
                    std.debug.print("expected replacement multi-page service PageRank worker-pool to acquire expired lease\n", .{});
                    return error.GraphMetricPageRankProcessProofFailed;
                }
                break :blk replacement;
            } else blk: {
                const summary = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    owner_id,
                    "service-process-worker-a,service-process-worker-b",
                    "5000",
                    worker_now,
                    "4",
                );
                now_ms += 1;
                break :blk summary;
            };
            const expected_min_pages: usize = if (phase == .prepare_generation or phase == .scan_edges_and_out_degree) 1 else 2;
            if (worker.result.pages_completed < expected_min_pages or worker.stats.worker_count != 2) {
                std.debug.print("expected service worker-pool to complete at least {d} PageRank pages for phase {}, got {d}\n", .{
                    expected_min_pages,
                    phase,
                    worker.result.pages_completed,
                });
                return error.GraphMetricPageRankProcessProofFailed;
            }

            const coordinator_owner = try std.fmt.allocPrint(alloc, "service-pagerank-multipage-coordinator-{d}", .{i});
            defer alloc.free(coordinator_owner);
            const coordinator_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(coordinator_now);
            const coordinator = try runServiceCoordinatorRoleProcessAt(
                alloc,
                io,
                antfly_exe,
                base_uri,
                coordinator_runtime,
                coordinator_owner,
                "5000",
                coordinator_now,
            );
            now_ms += 1;
            if (coordinator.result.phases_advanced == 0) {
                std.debug.print("expected service coordinator process to advance PageRank phase after {}\n", .{phase});
                return error.GraphMetricPageRankProcessProofFailed;
            }
            const next_phase = switch (phase) {
                .prepare_generation => antfly.graph.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree,
                .scan_edges_and_out_degree => .initialize_ranks,
                .initialize_ranks => .iterate_contributions,
                .iterate_contributions => .reduce_ranks,
                .reduce_ranks => .check_convergence,
                .check_convergence => .publish_generation,
                else => unreachable,
            };
            try assertOpenDbMetricActivePhase(alloc, &db, "pagerank", next_phase, target_generation);
        }

        const publish_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(publish_now);
        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-pagerank-multipage-coordinator-publish",
            "5000",
            publish_now,
        );
        now_ms += 1;
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish multi-page PageRank build\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "pagerank", .cleanup_old_generations, target_generation);

        var cleanup_fresh = false;
        for (0..8) |i| {
            var status = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
                cleanup_fresh = true;
                break;
            }

            const owner_id = try std.fmt.allocPrint(alloc, "service-pagerank-multipage-worker-pool-cleanup-{d}", .{i});
            defer alloc.free(owner_id);
            const cleanup_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(cleanup_now);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                worker_pool_runtime,
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                cleanup_now,
                "4",
            );
            now_ms += 1;
            if (cleanup.stats.worker_count != 2) {
                std.debug.print("expected service PageRank cleanup worker-pool to keep two configured workers\n", .{});
                return error.GraphMetricPageRankProcessProofFailed;
            }
        }
        if (!cleanup_fresh) {
            var status = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("pagerank");
            defer status.deinit(alloc);
            cleanup_fresh = status.state == antfly.graph.GraphIndex.GraphMetricState.fresh;
        }
        if (!cleanup_fresh) {
            std.debug.print("expected service worker-pool to finish multi-page PageRank cleanup\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }
    try verifyPageRankFixedIterationMetadata(alloc, db_path, target_generation, 1);
}

fn verifyEigenvectorServiceTargetedPublishAndCleanupRestartProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "eigenvector", target_generation, .publish_generation);
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-cleanup-coordinator",
            "service-eigenvector-publish-cleanup-coordinator-a",
            "5000",
            "77000",
        );
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish eigenvector before cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "eigenvector", .cleanup_old_generations, target_generation);

        const duplicate_publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-cleanup-coordinator",
            "service-eigenvector-publish-cleanup-coordinator-b",
            "5000",
            "77001",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "service eigenvector cleanup");
        try assertOpenDbMetricPhase(alloc, &db, "eigenvector", .cleanup_old_generations, target_generation);

        const cleanup_ready_file = ".zig-cache/tmp/graph-metric-process-service-eigenvector-publish-cleanup-worker-pool-ready";
        std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        try runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-cleanup-worker-pool",
            "service-eigenvector-publish-cleanup-worker-pool-killed",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "77002",
            "1",
            cleanup_ready_file,
        );
        try assertOpenDbMetricPhase(alloc, &db, "eigenvector", .cleanup_old_generations, target_generation);

        const fenced_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-cleanup-worker-pool",
            "service-eigenvector-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "77100",
            "4",
        );
        if (fenced_cleanup.durable_progressed or fenced_cleanup.stats.has_lease or fenced_cleanup.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate service eigenvector cleanup worker-pool to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "eigenvector", .cleanup_old_generations, target_generation);

        const takeover_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-publish-cleanup-worker-pool",
            "service-eigenvector-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "77203",
            "4",
        );
        if (takeover_cleanup.stats.takeover_count == 0 or takeover_cleanup.result.pages_claimed == 0 or takeover_cleanup.result.pages_completed == 0) {
            std.debug.print("expected replacement service eigenvector cleanup worker-pool to take over and advance cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }

        var cleanup_progressed = takeover_cleanup.durable_progressed;
        for (0..6) |i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-eigenvector-publish-cleanup-worker-pool-final-{d}", .{i});
            defer alloc.free(owner_id);
            const now_ms = try std.fmt.allocPrint(alloc, "{d}", .{77204 + i});
            defer alloc.free(now_ms);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                "service-eigenvector-publish-cleanup-worker-pool",
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                now_ms,
                "4",
            );
            cleanup_progressed = cleanup_progressed or cleanup.durable_progressed or cleanup.result.pages_claimed != 0 or cleanup.result.pages_completed != 0;
        }
        if (!cleanup_progressed) {
            std.debug.print("expected service worker-pool processes to make eigenvector cleanup progress\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyMetricFresh(alloc, db_path, "eigenvector", target_generation);
}

fn verifyEigenvectorServiceTargetedMultiPageWorkerPoolProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const coordinator_runtime = "service-eigenvector-multipage-coordinator";
        const worker_pool_runtime = "service-eigenvector-multipage-worker-pool";
        var now_ms: u64 = 83000;
        const coordinator_ready_file = ".zig-cache/tmp/graph-metric-process-service-eigenvector-multipage-coordinator-ready";
        std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        try runAndKillServiceCoordinatorAfterReady(
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-eigenvector-multipage-coordinator-start",
            "200",
            "83000",
            coordinator_ready_file,
        );
        try assertOpenDbMetricActivePhase(alloc, &db, "eigenvector", .prepare_generation, target_generation);

        const coordinator_fenced = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-eigenvector-multipage-coordinator-replacement",
            "200",
            "83100",
        );
        if (coordinator_fenced.durable_progressed or coordinator_fenced.stats.has_lease or coordinator_fenced.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate multi-page service eigenvector coordinator to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }

        const coordinator_takeover = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-eigenvector-multipage-coordinator-replacement",
            "200",
            "83201",
        );
        if (coordinator_takeover.stats.takeover_count == 0) {
            std.debug.print("expected replacement multi-page service eigenvector coordinator to acquire expired lease\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        now_ms = 83202;
        try assertOpenDbMetricActivePhase(alloc, &db, "eigenvector", .prepare_generation, target_generation);

        const phases = [_]antfly.graph.GraphIndex.GraphMetricBuildPhase{
            .prepare_generation,
            .scan_edges_and_out_degree,
            .initialize_ranks,
            .iterate_contributions,
            .reduce_ranks,
            .check_convergence,
        };
        for (phases, 0..) |phase, i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-eigenvector-multipage-worker-pool-{d}", .{i});
            defer alloc.free(owner_id);
            const worker_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(worker_now);
            const worker = if (phase == .scan_edges_and_out_degree) blk: {
                const worker_ready_file = ".zig-cache/tmp/graph-metric-process-service-eigenvector-multipage-worker-pool-ready";
                std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                defer std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                try runAndKillServiceWorkerPoolAfterReady(
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-eigenvector-multipage-worker-pool-killed",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    worker_now,
                    worker_ready_file,
                );
                now_ms += 1;

                const fenced_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
                defer alloc.free(fenced_now);
                const worker_pool_fenced = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-eigenvector-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    fenced_now,
                    "4",
                );
                now_ms += 1;
                if (worker_pool_fenced.durable_progressed or worker_pool_fenced.stats.has_lease or worker_pool_fenced.stats.lease_acquire_failures == 0) {
                    std.debug.print("expected duplicate multi-page service eigenvector worker-pool to be fenced before lease expiry\n", .{});
                    return error.GraphMetricLeaseProofFailed;
                }

                const takeover_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms + 205});
                defer alloc.free(takeover_now);
                const replacement = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-eigenvector-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    takeover_now,
                    "4",
                );
                now_ms += 206;
                if (replacement.stats.takeover_count == 0) {
                    std.debug.print("expected replacement multi-page service eigenvector worker-pool to acquire expired lease\n", .{});
                    return error.GraphMetricProcessProofFailed;
                }
                break :blk replacement;
            } else blk: {
                const summary = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    owner_id,
                    "service-process-worker-a,service-process-worker-b",
                    "5000",
                    worker_now,
                    "4",
                );
                now_ms += 1;
                break :blk summary;
            };
            const expected_min_pages: usize = if (phase == .prepare_generation or phase == .scan_edges_and_out_degree) 1 else 2;
            if (worker.result.pages_completed < expected_min_pages or worker.stats.worker_count != 2) {
                std.debug.print("expected service worker-pool to complete at least {d} eigenvector pages for phase {}, got {d}\n", .{
                    expected_min_pages,
                    phase,
                    worker.result.pages_completed,
                });
                return error.GraphMetricProcessProofFailed;
            }

            const coordinator_owner = try std.fmt.allocPrint(alloc, "service-eigenvector-multipage-coordinator-{d}", .{i});
            defer alloc.free(coordinator_owner);
            const coordinator_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(coordinator_now);
            const coordinator = try runServiceCoordinatorRoleProcessAt(
                alloc,
                io,
                antfly_exe,
                base_uri,
                coordinator_runtime,
                coordinator_owner,
                "5000",
                coordinator_now,
            );
            now_ms += 1;
            if (coordinator.result.phases_advanced == 0) {
                std.debug.print("expected service coordinator process to advance eigenvector phase after {}\n", .{phase});
                return error.GraphMetricProcessProofFailed;
            }
            const next_phase = switch (phase) {
                .prepare_generation => antfly.graph.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree,
                .scan_edges_and_out_degree => .initialize_ranks,
                .initialize_ranks => .iterate_contributions,
                .iterate_contributions => .reduce_ranks,
                .reduce_ranks => .check_convergence,
                .check_convergence => .publish_generation,
                else => unreachable,
            };
            try assertOpenDbMetricActivePhase(alloc, &db, "eigenvector", next_phase, target_generation);
        }

        const publish_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(publish_now);
        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-eigenvector-multipage-coordinator-publish",
            "5000",
            publish_now,
        );
        now_ms += 1;
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish multi-page eigenvector build\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbMetricPhase(alloc, &db, "eigenvector", .cleanup_old_generations, target_generation);

        var cleanup_fresh = false;
        for (0..8) |i| {
            var status = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
                cleanup_fresh = true;
                break;
            }

            const owner_id = try std.fmt.allocPrint(alloc, "service-eigenvector-multipage-worker-pool-cleanup-{d}", .{i});
            defer alloc.free(owner_id);
            const cleanup_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(cleanup_now);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                worker_pool_runtime,
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                cleanup_now,
                "4",
            );
            now_ms += 1;
            if (cleanup.stats.worker_count != 2) {
                std.debug.print("expected service eigenvector cleanup worker-pool to keep two configured workers\n", .{});
                return error.GraphMetricProcessProofFailed;
            }
        }
        if (!cleanup_fresh) {
            var status = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("eigenvector");
            defer status.deinit(alloc);
            cleanup_fresh = status.state == antfly.graph.GraphIndex.GraphMetricState.fresh;
        }
        if (!cleanup_fresh) {
            std.debug.print("expected service worker-pool to finish multi-page eigenvector cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyFixedIterationMetadata(alloc, db_path, "eigenvector", target_generation, 1);
}

fn verifyHitsServiceTargetedPublishAndCleanupRestartProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    try prepareMetricBuildToPhase(alloc, db_path, "hits_authority", target_generation, .publish_generation);
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-cleanup-coordinator",
            "service-hits-publish-cleanup-coordinator-a",
            "5000",
            "78000",
        );
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish HITS pair before cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbHitsAfterPairedPublish(alloc, &db, target_generation);

        const duplicate_publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-cleanup-coordinator",
            "service-hits-publish-cleanup-coordinator-b",
            "5000",
            "78001",
        );
        try assertDuplicateCoordinatorDidNotMutate(duplicate_publish, "service hits cleanup");
        try assertOpenDbHitsAfterPairedPublish(alloc, &db, target_generation);

        const cleanup_ready_file = ".zig-cache/tmp/graph-metric-process-service-hits-publish-cleanup-worker-pool-ready";
        std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, cleanup_ready_file) catch {};
        try runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-cleanup-worker-pool",
            "service-hits-publish-cleanup-worker-pool-killed",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "78002",
            "1",
            cleanup_ready_file,
        );
        try assertOpenDbHitsAfterPairedPublish(alloc, &db, target_generation);

        const fenced_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-cleanup-worker-pool",
            "service-hits-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "78100",
            "4",
        );
        if (fenced_cleanup.durable_progressed or fenced_cleanup.stats.has_lease or fenced_cleanup.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate service HITS cleanup worker-pool to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }
        try assertOpenDbHitsAfterPairedPublish(alloc, &db, target_generation);

        const takeover_cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-publish-cleanup-worker-pool",
            "service-hits-publish-cleanup-worker-pool-replacement",
            "service-process-worker-a,service-process-worker-b",
            "200",
            "78203",
            "4",
        );
        if (takeover_cleanup.stats.takeover_count == 0 or takeover_cleanup.result.pages_claimed == 0 or takeover_cleanup.result.pages_completed == 0) {
            std.debug.print("expected replacement service HITS cleanup worker-pool to take over and advance cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }

        var cleanup_progressed = takeover_cleanup.durable_progressed;
        for (0..12) |i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-hits-publish-cleanup-worker-pool-{d}", .{i});
            defer alloc.free(owner_id);
            const now_ms = try std.fmt.allocPrint(alloc, "{d}", .{78204 + i});
            defer alloc.free(now_ms);
            const cleanup = try runServiceWorkerPoolRoleProcessAt(
                alloc,
                io,
                antfly_exe,
                base_uri,
                "service-hits-publish-cleanup-worker-pool",
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                now_ms,
            );
            cleanup_progressed = cleanup_progressed or cleanup.durable_progressed or cleanup.result.pages_claimed != 0 or cleanup.result.pages_completed != 0 or cleanup.result.published != 0;
            if (cleanup.result.published != 0) break;
        }
        if (!cleanup_progressed) {
            std.debug.print("expected service worker-pool processes to advance HITS cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyHitsFresh(alloc, db_path, target_generation);
    try verifyHitsFixedIterationMetadata(alloc, db_path, target_generation, 1);
}

fn verifyHitsServiceTargetedMultiPageWorkerPoolProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    target_generation: u64,
) !void {
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
        defer api_runtime.deinit();
        const base_uri = try api_runtime.baseUri(alloc);
        defer alloc.free(base_uri);

        const coordinator_runtime = "service-hits-multipage-coordinator";
        const worker_pool_runtime = "service-hits-multipage-worker-pool";
        var now_ms: u64 = 84000;
        const coordinator_ready_file = ".zig-cache/tmp/graph-metric-process-service-hits-multipage-coordinator-ready";
        std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_ready_file) catch {};
        try runAndKillServiceCoordinatorAfterReady(
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-hits-multipage-coordinator-start",
            "200",
            "84000",
            coordinator_ready_file,
        );
        try assertOpenDbMetricActivePhase(alloc, &db, "hits_authority", .prepare_generation, target_generation);

        const coordinator_fenced = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-hits-multipage-coordinator-replacement",
            "200",
            "84100",
        );
        if (coordinator_fenced.durable_progressed or coordinator_fenced.stats.has_lease or coordinator_fenced.stats.lease_acquire_failures == 0) {
            std.debug.print("expected duplicate multi-page service HITS coordinator to be fenced before lease expiry\n", .{});
            return error.GraphMetricLeaseProofFailed;
        }

        const coordinator_takeover = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-hits-multipage-coordinator-replacement",
            "200",
            "84201",
        );
        if (coordinator_takeover.stats.takeover_count == 0) {
            std.debug.print("expected replacement multi-page service HITS coordinator to acquire expired lease\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        now_ms = 84202;
        try assertOpenDbMetricActivePhase(alloc, &db, "hits_authority", .prepare_generation, target_generation);

        const phases = [_]antfly.graph.GraphIndex.GraphMetricBuildPhase{
            .prepare_generation,
            .scan_edges_and_out_degree,
            .initialize_ranks,
            .iterate_contributions,
            .reduce_ranks,
            .hits_hub_contributions,
            .hits_hub_reduce_ranks,
            .check_convergence,
        };
        for (phases, 0..) |phase, i| {
            const owner_id = try std.fmt.allocPrint(alloc, "service-hits-multipage-worker-pool-{d}", .{i});
            defer alloc.free(owner_id);
            const worker_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(worker_now);
            const worker = if (phase == .scan_edges_and_out_degree) blk: {
                const worker_ready_file = ".zig-cache/tmp/graph-metric-process-service-hits-multipage-worker-pool-ready";
                std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                defer std.Io.Dir.cwd().deleteFile(io, worker_ready_file) catch {};
                try runAndKillServiceWorkerPoolAfterReady(
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-hits-multipage-worker-pool-killed",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    worker_now,
                    worker_ready_file,
                );
                now_ms += 1;

                const fenced_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
                defer alloc.free(fenced_now);
                const worker_pool_fenced = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-hits-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    fenced_now,
                    "4",
                );
                now_ms += 1;
                if (worker_pool_fenced.durable_progressed or worker_pool_fenced.stats.has_lease or worker_pool_fenced.stats.lease_acquire_failures == 0) {
                    std.debug.print("expected duplicate multi-page service HITS worker-pool to be fenced before lease expiry\n", .{});
                    return error.GraphMetricLeaseProofFailed;
                }

                const takeover_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms + 205});
                defer alloc.free(takeover_now);
                const replacement = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    "service-hits-multipage-worker-pool-replacement",
                    "service-process-worker-a,service-process-worker-b",
                    "200",
                    takeover_now,
                    "4",
                );
                now_ms += 206;
                if (replacement.stats.takeover_count == 0) {
                    std.debug.print("expected replacement multi-page service HITS worker-pool to acquire expired lease\n", .{});
                    return error.GraphMetricProcessProofFailed;
                }
                break :blk replacement;
            } else blk: {
                const summary = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                    alloc,
                    io,
                    antfly_exe,
                    base_uri,
                    worker_pool_runtime,
                    owner_id,
                    "service-process-worker-a,service-process-worker-b",
                    "5000",
                    worker_now,
                    "4",
                );
                now_ms += 1;
                break :blk summary;
            };
            const expected_min_pages: usize = if (phase == .prepare_generation or phase == .scan_edges_and_out_degree) 1 else 2;
            if (worker.result.pages_completed < expected_min_pages or worker.stats.worker_count != 2) {
                std.debug.print("expected service worker-pool to complete at least {d} HITS pages for phase {}, got {d}\n", .{
                    expected_min_pages,
                    phase,
                    worker.result.pages_completed,
                });
                return error.GraphMetricProcessProofFailed;
            }

            const coordinator_owner = try std.fmt.allocPrint(alloc, "service-hits-multipage-coordinator-{d}", .{i});
            defer alloc.free(coordinator_owner);
            const coordinator_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(coordinator_now);
            const coordinator = try runServiceCoordinatorRoleProcessAt(
                alloc,
                io,
                antfly_exe,
                base_uri,
                coordinator_runtime,
                coordinator_owner,
                "5000",
                coordinator_now,
            );
            now_ms += 1;
            if (coordinator.result.phases_advanced == 0) {
                std.debug.print("expected service coordinator process to advance HITS phase after {}\n", .{phase});
                return error.GraphMetricProcessProofFailed;
            }
            const next_phase = switch (phase) {
                .prepare_generation => antfly.graph.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree,
                .scan_edges_and_out_degree => .initialize_ranks,
                .initialize_ranks => .iterate_contributions,
                .iterate_contributions => .reduce_ranks,
                .reduce_ranks => .hits_hub_contributions,
                .hits_hub_contributions => .hits_hub_reduce_ranks,
                .hits_hub_reduce_ranks => .check_convergence,
                .check_convergence => .publish_generation,
                else => unreachable,
            };
            try assertOpenDbMetricActivePhase(alloc, &db, "hits_authority", next_phase, target_generation);
        }

        const publish_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(publish_now);
        const publish = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            coordinator_runtime,
            "service-hits-multipage-coordinator-publish",
            "5000",
            publish_now,
        );
        now_ms += 1;
        if (publish.result.published != 1 or publish.result.phases_advanced == 0) {
            std.debug.print("expected service coordinator process to publish multi-page HITS pair\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        try assertOpenDbHitsAfterPairedPublish(alloc, &db, target_generation);

        var cleanup_fresh = false;
        for (0..12) |i| {
            var authority = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("hits_authority");
            defer authority.deinit(alloc);
            var hub = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("hits_hub");
            defer hub.deinit(alloc);
            if (authority.state == antfly.graph.GraphIndex.GraphMetricState.fresh and hub.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
                cleanup_fresh = true;
                break;
            }

            const owner_id = try std.fmt.allocPrint(alloc, "service-hits-multipage-worker-pool-cleanup-{d}", .{i});
            defer alloc.free(owner_id);
            const cleanup_now = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
            defer alloc.free(cleanup_now);
            const cleanup = try runServiceWorkerPoolRoleProcessAtWithMaxPages(
                alloc,
                io,
                antfly_exe,
                base_uri,
                worker_pool_runtime,
                owner_id,
                "service-process-worker-a,service-process-worker-b",
                "5000",
                cleanup_now,
                "4",
            );
            now_ms += 1;
            if (cleanup.stats.worker_count != 2) {
                std.debug.print("expected service HITS cleanup worker-pool to keep two configured workers\n", .{});
                return error.GraphMetricProcessProofFailed;
            }
        }
        if (!cleanup_fresh) {
            var authority = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("hits_authority");
            defer authority.deinit(alloc);
            var hub = try (db.core.graphIndex("graph_idx") orelse return error.IndexNotFound).index.graphMetricStatus("hits_hub");
            defer hub.deinit(alloc);
            cleanup_fresh = authority.state == antfly.graph.GraphIndex.GraphMetricState.fresh and hub.state == antfly.graph.GraphIndex.GraphMetricState.fresh;
        }
        if (!cleanup_fresh) {
            std.debug.print("expected service worker-pool to finish multi-page HITS cleanup\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try verifyHitsFresh(alloc, db_path, target_generation);
    try verifyHitsFixedIterationMetadata(alloc, db_path, target_generation, 1);
}

fn assertOpenDbMetricPhase(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    metric_name: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_published_generation: u64,
) !void {
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.phase != expected_phase or status.published_generation != expected_published_generation) {
        std.debug.print("expected {s} phase {} published generation {d}, got phase {} published generation {d}\n", .{
            metric_name,
            expected_phase,
            expected_published_generation,
            status.phase,
            status.published_generation,
        });
        return error.GraphMetricProcessProofFailed;
    }
}

fn assertOpenDbMetricActivePhase(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    metric_name: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_building_generation: u64,
) !void {
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.phase != expected_phase or status.building_generation != expected_building_generation) {
        std.debug.print("expected active {s} phase {} building generation {d}, got phase {} building generation {d}\n", .{
            metric_name,
            expected_phase,
            expected_building_generation,
            status.phase,
            status.building_generation,
        });
        return error.GraphMetricProcessProofFailed;
    }
}

fn assertOpenDbHitsAfterPairedPublish(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    target_generation: u64,
) !void {
    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var authority = try graph_entry.index.graphMetricStatus("hits_authority");
    defer authority.deinit(alloc);
    var hub = try graph_entry.index.graphMetricStatus("hits_hub");
    defer hub.deinit(alloc);
    if (authority.state != antfly.graph.GraphIndex.GraphMetricState.building or
        authority.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.cleanup_old_generations)
    {
        std.debug.print("expected service HITS authority to be cleaning after paired publish, got {}/{}\n", .{ authority.state, authority.phase });
        return error.GraphMetricProcessProofFailed;
    }
    if (hub.state != antfly.graph.GraphIndex.GraphMetricState.fresh or
        hub.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.complete)
    {
        std.debug.print("expected service HITS hub to be fresh/complete after paired publish, got {}/{}\n", .{ hub.state, hub.phase });
        return error.GraphMetricProcessProofFailed;
    }
    if (authority.published_generation != target_generation or hub.published_generation != target_generation) {
        std.debug.print(
            "expected service paired HITS published generation {d}, got authority {d} hub {d}\n",
            .{ target_generation, authority.published_generation, hub.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
}

fn verifyDegreeActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    try runSupervisorProcess(alloc, io, antfly_exe, db_path);
    try verifyDegreeFresh(alloc, db_path, initial_generation);

    const rebuild_generation = try addDegreeDirtyEdge(alloc, db_path);
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const started = try runCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        db_path,
        "degree-active-read-proof-coordinator",
        "5000",
        "26500",
    );
    if (!started.durable_progressed or !started.stats.has_lease) {
        std.debug.print("expected coordinator process to start active degree rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building or
            status.published_generation != initial_generation or
            status.building_generation != rebuild_generation)
        {
            std.debug.print(
                "expected coordinator process to leave degree rebuilding at generations {d}/{d}, got state {} generations {d}/{d}\n",
                .{
                    initial_generation,
                    rebuild_generation,
                    status.state,
                    status.published_generation,
                    status.building_generation,
                },
            );
            return error.GraphMetricDegreeProcessProofFailed;
        }
    }

    try verifyDegreeActivePublicReadSurface(alloc, &db, initial_generation, rebuild_generation);
}

fn verifyDegreeServiceActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
    defer api_runtime.deinit();
    const base_uri = try api_runtime.baseUri(alloc);
    defer alloc.free(base_uri);

    var now_ms: u64 = 2700;
    var idle_rounds: usize = 0;
    var initial_fresh = false;
    for (0..80) |_| {
        const now_coordinator = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_coordinator);
        const coordinator_summary = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-active-public-read-coordinator",
            "service-degree-active-public-read-coordinator",
            "200",
            now_coordinator,
        );
        now_ms += 1;

        const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_worker);
        const worker_summary = try runServiceWorkerPoolRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-degree-active-public-read-worker-pool",
            "service-degree-active-public-read-worker-pool",
            "service-process-worker-a,service-process-worker-b",
            "200",
            now_worker,
        );
        now_ms += 1;

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
            if (status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
            initial_fresh = true;
            break;
        }
        if (coordinator_summary.durable_progressed or worker_summary.durable_progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= 8) break;
        }
    }
    if (!initial_fresh) return error.GraphMetricBuildNotComplete;

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:new",
            .value = "{\"title\":\"new source\",\"body\":\"newsource graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const rebuild_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const now_rebuild = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(now_rebuild);
    _ = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-degree-active-public-read-coordinator",
        "service-degree-active-public-read-coordinator",
        "200",
        now_rebuild,
    );
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building or
            status.published_generation != initial_generation or
            status.building_generation != rebuild_generation)
        {
            std.debug.print(
                "expected service coordinator process to leave degree rebuilding at generations {d}/{d}, got state {} generations {d}/{d}\n",
                .{
                    initial_generation,
                    rebuild_generation,
                    status.state,
                    status.published_generation,
                    status.building_generation,
                },
            );
            return error.GraphMetricDegreeProcessProofFailed;
        }
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        if (pending.active_builds == 0) {
            std.debug.print("expected service coordinator process to leave active degree rebuild work\n", .{});
            return error.GraphMetricDegreeProcessProofFailed;
        }
    }
    try verifyDegreeActivePublicReadSurface(alloc, &db, initial_generation, rebuild_generation);
}

fn isProcessHarnessDoc0Node(node: []const u8) bool {
    return std.mem.eql(u8, node, "doc:0") or std.mem.eql(u8, node, "0");
}

fn verifyDegreeActivePublicReadSurface(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    initial_generation: u64,
    rebuild_generation: u64,
) !void {
    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 3,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 1) {
        std.debug.print("expected one degree graph metric result during service active rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    const result = published_result.graph_metric_results[0];
    if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service active degree query status building, got {}\n", .{result.status.state});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    if (result.status.published_generation != initial_generation or result.status.building_generation != rebuild_generation) {
        std.debug.print("expected service degree published/building generations {d}/{d}, got {d}/{d}\n", .{
            initial_generation,
            rebuild_generation,
            result.status.published_generation,
            result.status.building_generation,
        });
        return error.GraphMetricGenerationMismatch;
    }
    if (result.scores.len == 0) {
        std.debug.print("expected service active degree published top-k scores\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    var found_prior_source_score = false;
    for (result.scores) |score| {
        if (std.mem.eql(u8, score.node, "doc:new") or std.mem.eql(u8, score.node, "new")) {
            std.debug.print("service active degree published top-k exposed rebuilding-only source {s}\n", .{score.node});
            return error.GraphMetricDegreeProcessProofFailed;
        }
        if (score.score == 1.0) {
            try std.testing.expectApproxEqAbs(@as(f64, 1.0), score.score, 0.0000001);
            found_prior_source_score = true;
        }
    }
    if (!found_prior_source_score) {
        std.debug.print("expected service active degree published top-k to include a prior source score\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "degree",
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:side"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected one degree graph traversal result during service active rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!isProcessHarnessDoc0Node(traversal.nodes[0].key)) {
        std.debug.print("expected degree traversal to return doc:0, got {s}\n", .{traversal.nodes[0].key});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    if (traversal.nodes[0].metrics.len != 1 or traversal.nodes[0].metrics[0].score == null) {
        std.debug.print("expected service traversal published projection to serve prior degree score\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), traversal.nodes[0].metrics[0].score.?, 0.0000001);
    if (traversal.metric_status.len != 1 or traversal.metric_status[0].state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service traversal metric status building during active degree rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    if (traversal.metric_status[0].published_generation != initial_generation or traversal.metric_status[0].building_generation != rebuild_generation) {
        std.debug.print("expected service degree traversal status to report generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "degree",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 3,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during service active degree rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during service active degree rebuild\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service degree rerank status building, got {}\n", .{rerank_status.state});
        return error.GraphMetricDegreeProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation or rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected service degree rerank status generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }
    var found_prior_metric_score = false;
    for (rerank_result.hits) |hit| {
        const details = hit.score_details orelse {
            std.debug.print("expected service degree reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricDegreeProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected service degree reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
        if (std.mem.eql(u8, hit.id, "doc:new") or std.mem.eql(u8, hit.id, "new")) {
            if (details.metric_score != null or !details.missing_score_used) {
                std.debug.print("service active degree search rerank gave rebuilding-only document {s} a published metric score\n", .{hit.id});
                return error.GraphMetricDegreeProcessProofFailed;
            }
            continue;
        }
        if (details.metric_score) |metric_score| {
            try std.testing.expectApproxEqAbs(@as(f64, 1.0), metric_score, 0.0000001);
            found_prior_metric_score = true;
        }
    }
    if (!found_prior_metric_score) {
        std.debug.print("expected service degree rerank to include a prior published metric score\n", .{});
        return error.GraphMetricDegreeProcessProofFailed;
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 3,
        .include_stored = false,
    }));
}

fn verifyPageRankServiceActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
    defer api_runtime.deinit();
    const base_uri = try api_runtime.baseUri(alloc);
    defer alloc.free(base_uri);

    var now_ms: u64 = 3000;
    var idle_rounds: usize = 0;
    var initial_fresh = false;
    for (0..80) |_| {
        const now_coordinator = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_coordinator);
        const coordinator_summary = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-active-public-read-coordinator",
            "service-pagerank-active-public-read-coordinator",
            "200",
            now_coordinator,
        );
        now_ms += 1;

        const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_worker);
        const worker_summary = try runServiceWorkerPoolRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-pagerank-active-public-read-worker-pool",
            "service-pagerank-active-public-read-worker-pool",
            "service-process-worker-a,service-process-worker-b",
            "200",
            now_worker,
        );
        now_ms += 1;

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
            if (status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
            initial_fresh = true;
            break;
        }
        if (coordinator_summary.durable_progressed or worker_summary.durable_progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= 8) break;
        }
    }
    if (!initial_fresh) return error.GraphMetricBuildNotComplete;

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:e",
            .value = "{\"title\":\"epsilon\",\"body\":\"epsilon graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const rebuild_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const now_rebuild = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(now_rebuild);
    _ = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-pagerank-active-public-read-coordinator",
        "service-pagerank-active-public-read-coordinator",
        "200",
        now_rebuild,
    );
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building or
            status.published_generation != initial_generation or
            status.building_generation != rebuild_generation)
        {
            std.debug.print(
                "expected service coordinator process to leave PageRank rebuilding at generations {d}/{d}, got state {} generations {d}/{d}\n",
                .{
                    initial_generation,
                    rebuild_generation,
                    status.state,
                    status.published_generation,
                    status.building_generation,
                },
            );
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        if (pending.active_builds == 0) {
            std.debug.print("expected service coordinator process to leave active PageRank rebuild work\n", .{});
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }
    try verifyPageRankActivePublicReadSurface(alloc, &db, initial_generation, rebuild_generation);
}

fn verifyPageRankActivePublicReadSurface(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    initial_generation: u64,
    rebuild_generation: u64,
) !void {
    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 1) {
        std.debug.print("expected one PageRank graph metric result during service active rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const result = published_result.graph_metric_results[0];
    if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service active PageRank query status building, got {}\n", .{result.status.state});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (result.status.published_generation != initial_generation or result.status.building_generation != rebuild_generation) {
        std.debug.print("expected service published/building generations {d}/{d}, got {d}/{d}\n", .{
            initial_generation,
            rebuild_generation,
            result.status.published_generation,
            result.status.building_generation,
        });
        return error.GraphMetricGenerationMismatch;
    }
    if (result.scores.len == 0) {
        std.debug.print("expected service active PageRank published read to serve prior scores\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    for (result.scores) |score| {
        if (std.mem.eql(u8, score.node, "doc:e")) {
            std.debug.print("service active PageRank published read exposed rebuilding node {s}\n", .{score.node});
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected one PageRank graph traversal result during service active rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!std.mem.eql(u8, traversal.nodes[0].key, "doc:b")) {
        std.debug.print("expected traversal to return doc:b, got {s}\n", .{traversal.nodes[0].key});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.nodes[0].metrics.len != 1 or traversal.nodes[0].metrics[0].score == null) {
        std.debug.print("expected service traversal published projection to serve prior PageRank score\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.metric_status.len != 1 or traversal.metric_status[0].state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service traversal metric status building during active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (traversal.metric_status[0].published_generation != initial_generation or traversal.metric_status[0].building_generation != rebuild_generation) {
        std.debug.print("expected service traversal status to report generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during service active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during service active PageRank rebuild\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service rerank status building, got {}\n", .{rerank_status.state});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation or rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected service rerank status generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:e")) {
            std.debug.print("service active PageRank search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricPageRankProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricPageRankProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected service reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

fn verifyEigenvectorServiceActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
    defer api_runtime.deinit();
    const base_uri = try api_runtime.baseUri(alloc);
    defer alloc.free(base_uri);

    var now_ms: u64 = 3300;
    var idle_rounds: usize = 0;
    var initial_fresh = false;
    for (0..80) |_| {
        const now_coordinator = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_coordinator);
        const coordinator_summary = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-active-public-read-coordinator",
            "service-eigenvector-active-public-read-coordinator",
            "200",
            now_coordinator,
        );
        now_ms += 1;

        const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_worker);
        const worker_summary = try runServiceWorkerPoolRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-eigenvector-active-public-read-worker-pool",
            "service-eigenvector-active-public-read-worker-pool",
            "service-process-worker-a,service-process-worker-b",
            "200",
            now_worker,
        );
        now_ms += 1;

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.fresh) {
            if (status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
            initial_fresh = true;
            break;
        }
        if (coordinator_summary.durable_progressed or worker_summary.durable_progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= 8) break;
        }
    }
    if (!initial_fresh) return error.GraphMetricBuildNotComplete;

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:e",
            .value = "{\"title\":\"epsilon\",\"body\":\"epsilon graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:c\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const rebuild_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const now_rebuild = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(now_rebuild);
    _ = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-eigenvector-active-public-read-coordinator",
        "service-eigenvector-active-public-read-coordinator",
        "200",
        now_rebuild,
    );
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("eigenvector");
        defer status.deinit(alloc);
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building or
            status.published_generation != initial_generation or
            status.building_generation != rebuild_generation)
        {
            std.debug.print(
                "expected service coordinator process to leave eigenvector rebuilding at generations {d}/{d}, got state {} generations {d}/{d}\n",
                .{
                    initial_generation,
                    rebuild_generation,
                    status.state,
                    status.published_generation,
                    status.building_generation,
                },
            );
            return error.GraphMetricProcessProofFailed;
        }
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        if (pending.active_builds == 0) {
            std.debug.print("expected service coordinator process to leave active eigenvector rebuild work\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }

    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 10,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 1) {
        std.debug.print("expected one eigenvector graph metric result during service active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const result = published_result.graph_metric_results[0];
    if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service active eigenvector query status building, got {}\n", .{result.status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (result.status.published_generation != initial_generation or result.status.building_generation != rebuild_generation) {
        std.debug.print("expected service eigenvector published/building generations {d}/{d}, got {d}/{d}\n", .{
            initial_generation,
            rebuild_generation,
            result.status.published_generation,
            result.status.building_generation,
        });
        return error.GraphMetricGenerationMismatch;
    }
    if (result.scores.len == 0) {
        std.debug.print("expected service active eigenvector published read to serve prior scores\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (result.scores) |score| {
        if (std.mem.eql(u8, score.node, "doc:e")) {
            std.debug.print("service active eigenvector published read exposed rebuilding node {s}\n", .{score.node});
            return error.GraphMetricProcessProofFailed;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "central",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "eigenvector",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "eigenvector",
        .freshness = .published,
    }};
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len == 0) {
        std.debug.print("expected eigenvector graph traversal result during service active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    for (traversal.nodes) |node| {
        if (node.metrics.len != 1 or node.metrics[0].score == null) {
            std.debug.print("expected service traversal published projection to serve prior eigenvector score\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    if (traversal.metric_status.len != 1 or traversal.metric_status[0].state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service traversal metric status building during active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    if (traversal.metric_status[0].published_generation != initial_generation or traversal.metric_status[0].building_generation != rebuild_generation) {
        std.debug.print("expected service traversal status to report eigenvector published/building generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "eigenvector",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "eigenvector",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during service active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during service active eigenvector rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building) {
        std.debug.print("expected service eigenvector rerank status building, got {}\n", .{rerank_status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation or rerank_status.building_generation != rebuild_generation) {
        std.debug.print("expected service eigenvector rerank status generations {d}/{d}\n", .{ initial_generation, rebuild_generation });
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:e")) {
            std.debug.print("service active eigenvector search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected service eigenvector reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        };
        if (details.published_generation != initial_generation) {
            std.debug.print("expected service eigenvector reranked hit {s} to use published generation {d}, got {d}\n", .{ hit.id, initial_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "eigenvector",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

fn verifyHitsServiceActiveProcessPublicReadFreshness(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    initial_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const api_runtime = try ProcessHarnessApiRuntime.start(alloc, io, &db);
    defer api_runtime.deinit();
    const base_uri = try api_runtime.baseUri(alloc);
    defer alloc.free(base_uri);

    var now_ms: u64 = 3600;
    var idle_rounds: usize = 0;
    var initial_fresh = false;
    for (0..80) |_| {
        const now_coordinator = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_coordinator);
        const coordinator_summary = try runServiceCoordinatorRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-active-public-read-coordinator",
            "service-hits-active-public-read-coordinator",
            "200",
            now_coordinator,
        );
        now_ms += 1;

        const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
        defer alloc.free(now_worker);
        const worker_summary = try runServiceWorkerPoolRoleProcessAt(
            alloc,
            io,
            antfly_exe,
            base_uri,
            "service-hits-active-public-read-worker-pool",
            "service-hits-active-public-read-worker-pool",
            "service-process-worker-a,service-process-worker-b",
            "200",
            now_worker,
        );
        now_ms += 1;

        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        if (authority.state == antfly.graph.GraphIndex.GraphMetricState.fresh and
            hub.state == antfly.graph.GraphIndex.GraphMetricState.fresh)
        {
            if (authority.published_generation != initial_generation or
                hub.published_generation != initial_generation)
            {
                return error.GraphMetricGenerationMismatch;
            }
            initial_fresh = true;
            break;
        }
        if (coordinator_summary.durable_progressed or worker_summary.durable_progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= 8) break;
        }
    }
    if (!initial_fresh) return error.GraphMetricBuildNotComplete;

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:hub-c",
            .value = "{\"title\":\"hub c\",\"body\":\"hub c graph\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    const rebuild_generation = blk: {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    if (rebuild_generation <= initial_generation) return error.GraphMetricGenerationMismatch;

    const now_rebuild = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(now_rebuild);
    _ = try runServiceCoordinatorRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-hits-active-public-read-coordinator",
        "service-hits-active-public-read-coordinator",
        "200",
        now_rebuild,
    );
    now_ms += 1;

    const now_worker = try std.fmt.allocPrint(alloc, "{d}", .{now_ms});
    defer alloc.free(now_worker);
    _ = try runServiceWorkerPoolRoleProcessAt(
        alloc,
        io,
        antfly_exe,
        base_uri,
        "service-hits-active-public-read-worker-pool",
        "service-hits-active-public-read-worker-pool",
        "service-process-worker-a,service-process-worker-b",
        "200",
        now_worker,
    );

    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var authority = try graph_entry.index.graphMetricStatus("hits_authority");
        defer authority.deinit(alloc);
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(alloc);
        if (authority.state != antfly.graph.GraphIndex.GraphMetricState.building or
            authority.published_generation != initial_generation or
            authority.building_generation != rebuild_generation)
        {
            std.debug.print(
                "expected service coordinator process to leave HITS authority rebuilding at generations {d}/{d}, got state {} generations {d}/{d}\n",
                .{
                    initial_generation,
                    rebuild_generation,
                    authority.state,
                    authority.published_generation,
                    authority.building_generation,
                },
            );
            return error.GraphMetricProcessProofFailed;
        }
        if (hub.state != antfly.graph.GraphIndex.GraphMetricState.building and
            hub.state != antfly.graph.GraphIndex.GraphMetricState.stale)
        {
            std.debug.print("expected service HITS hub status building or stale, got {}\n", .{hub.state});
            return error.GraphMetricProcessProofFailed;
        }
        if (hub.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
        if (hub.state == antfly.graph.GraphIndex.GraphMetricState.building and hub.building_generation != rebuild_generation) {
            return error.GraphMetricGenerationMismatch;
        }
    }
    {
        const pending = db.pendingWorkStats().graph_metric;
        if (pending.active_builds == 0) {
            std.debug.print("expected service coordinator process to leave active HITS rebuild work\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }

    try verifyHitsActivePublicReadSurface(alloc, &db, initial_generation, rebuild_generation);
}

fn verifyHitsActivePublicReadSurface(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    initial_generation: u64,
    rebuild_generation: u64,
) !void {
    var published_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer published_result.deinit();
    if (published_result.graph_metric_results.len != 2) {
        std.debug.print("expected two service HITS graph metric results during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (published_result.graph_metric_results) |result| {
        if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.building and
            result.status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
        {
            std.debug.print("expected service active HITS query status building or stale, got {}\n", .{result.status.state});
            return error.GraphMetricProcessProofFailed;
        }
        if (result.status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
        if (result.status.state == antfly.graph.GraphIndex.GraphMetricState.building and
            result.status.building_generation != rebuild_generation)
        {
            return error.GraphMetricGenerationMismatch;
        }
        if (result.scores.len == 0) {
            std.debug.print("expected service active HITS published read to serve prior scores\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
        for (result.scores) |score| {
            if (std.mem.eql(u8, score.node, "doc:hub-c")) {
                std.debug.print("service active HITS published read exposed rebuilding node {s}\n", .{score.node});
                return error.GraphMetricProcessProofFailed;
            }
        }
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 1,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected one service HITS graph traversal result during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!std.mem.eql(u8, traversal.nodes[0].key, "doc:authority")) {
        std.debug.print("expected service HITS traversal to return doc:authority, got {s}\n", .{traversal.nodes[0].key});
        return error.GraphMetricProcessProofFailed;
    }
    if (traversal.nodes[0].metrics.len != 2) {
        std.debug.print("expected service HITS traversal to project authority and hub scores\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.nodes[0].metrics) |metric| {
        if (metric.score == null) {
            std.debug.print("expected service HITS traversal metric {s} to serve a prior published score\n", .{metric.name});
            return error.GraphMetricProcessProofFailed;
        }
    }
    if (traversal.metric_status.len != 2) {
        std.debug.print("expected two service HITS traversal metric statuses during active rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.metric_status) |status| {
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.building and
            status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
        {
            std.debug.print("expected service HITS traversal status building or stale, got {}\n", .{status.state});
            return error.GraphMetricProcessProofFailed;
        }
        if (status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
        if (status.state == antfly.graph.GraphIndex.GraphMetricState.building and status.building_generation != rebuild_generation) {
            return error.GraphMetricGenerationMismatch;
        }
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    const fresh_metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_order_query = published_graph_query;
    fresh_order_query.order_by = &fresh_metric_orders;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_order_query }},
        .limit = 0,
    }));

    const fresh_metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = "hits_authority",
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    var fresh_filter_query = published_graph_query;
    fresh_filter_query.where_metric = &fresh_metric_filters;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_filter_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 3,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected search rerank hits during service active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected search rerank status during service active HITS rebuild\n", .{});
        return error.GraphMetricProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.building and
        rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.stale)
    {
        std.debug.print("expected service HITS search rerank status building or stale, got {}\n", .{rerank_status.state});
        return error.GraphMetricProcessProofFailed;
    }
    if (rerank_status.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
    if (rerank_status.state == antfly.graph.GraphIndex.GraphMetricState.building and rerank_status.building_generation != rebuild_generation) {
        return error.GraphMetricGenerationMismatch;
    }
    for (rerank_result.hits) |hit| {
        if (std.mem.eql(u8, hit.id, "doc:hub-c")) {
            std.debug.print("service active HITS search rerank exposed rebuilding-only document {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        }
        const details = hit.score_details orelse {
            std.debug.print("expected service HITS reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        };
        if (details.published_generation != initial_generation) return error.GraphMetricGenerationMismatch;
    }

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 3,
        .include_stored = false,
    }));
}

const ProcessHarnessStatusSource = struct {
    fn iface(self: *ProcessHarnessStatusSource) antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
            },
        };
    }

    fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
    }
};

/// Owns the same opaque API-kernel and `httpx` composition used by production
/// runtimes. Keeping the owner heap-stable is required because the API server
/// retains pointers to the status and table-write sources below.
const ProcessHarnessApiRuntime = struct {
    alloc: std.mem.Allocator,
    write_source: antfly.public_api.BoundTableWriteSource,
    status_source: ProcessHarnessStatusSource,
    api_server: antfly.public_api.kernel_bridge.ApiHttpServer,
    handler: antfly.public_api.kernel_bridge.HttpxHandler,
    http_server: httpx.Server,
    listener_task: httpx.ListenerTask,

    fn start(
        alloc: std.mem.Allocator,
        io: std.Io,
        db: *antfly.db.DB,
    ) !*ProcessHarnessApiRuntime {
        const runtime = try alloc.create(ProcessHarnessApiRuntime);
        errdefer alloc.destroy(runtime);

        runtime.alloc = alloc;
        runtime.write_source = antfly.public_api.BoundTableWriteSource.init("docs", db);
        runtime.status_source = .{};
        runtime.api_server = try antfly.public_api.kernel_bridge.ApiHttpServer.initWithConfig(
            alloc,
            .{},
            runtime.status_source.iface(),
            null,
            runtime.write_source.source(),
        );
        errdefer runtime.api_server.deinit();

        runtime.handler = try antfly.public_api.kernel_bridge.createHandler(&runtime.api_server);
        errdefer antfly.public_api.kernel_bridge.deinitHandler(&runtime.handler);
        try runtime.handler.initRuntime(alloc);

        runtime.http_server = httpx.Server.initWithConfig(alloc, io, .{
            .host = "127.0.0.1",
            .port = 0,
            .max_connections = 32,
            .max_request_tasks = 32,
        });
        errdefer runtime.http_server.deinit();
        try runtime.handler.registerRoutes(&runtime.http_server);

        runtime.listener_task = httpx.ListenerTask.init(&runtime.http_server);
        try runtime.listener_task.start();
        return runtime;
    }

    fn deinit(self: *ProcessHarnessApiRuntime) void {
        const alloc = self.alloc;
        self.listener_task.shutdown(30_000);
        self.listener_task.join() catch |err| {
            std.log.err("graph metric process HTTP listener failed during shutdown err={s}", .{@errorName(err)});
        };
        self.http_server.deinit();
        antfly.public_api.kernel_bridge.deinitHandler(&self.handler);
        self.api_server.deinit();
        alloc.destroy(self);
    }

    fn baseUri(self: *ProcessHarnessApiRuntime, alloc: std.mem.Allocator) ![]u8 {
        const address = self.http_server.boundAddress() orelse return error.NotListening;
        return std.fmt.allocPrint(alloc, "http://{f}", .{address});
    }
};

fn runAndKillCoordinatorAfterReady(
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--db-path",
        db_path,
        "--role",
        "coordinator",
        "--runtime-id",
        owner_id,
        "--owner-id",
        owner_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--test-ready-file",
        ready_file,
        "--test-hold-after-run-ms",
        "10000",
    };
    try verifyRoleProcessArgvScoped(argv[0..]);
    var child = try std.process.spawn(io, .{
        .argv = argv[0..],
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    errdefer child.kill(io);

    var ready = false;
    for (0..100) |_| {
        std.Io.Dir.cwd().access(io, ready_file, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        ready = true;
        break;
    }
    if (!ready) {
        child.kill(io);
        std.debug.print("timed out waiting for killable coordinator ready marker\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    child.kill(io);
}

fn runAndKillServiceCoordinatorAfterReady(
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--base-uri",
        base_uri,
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "coordinator",
        "--runtime-id",
        runtime_id,
        "--owner-id",
        owner_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
        "--test-now-ms",
        test_now_ms,
        "--test-ready-file",
        ready_file,
        "--test-hold-after-run-ms",
        "10000",
    };
    try runAndKillRoleProcessAfterReady(io, argv[0..], ready_file, error.GraphMetricLeaseProofFailed);
}

fn runAndKillServiceWorkerPoolAfterReady(
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    worker_ids: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
    ready_file: []const u8,
) !void {
    try runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
        io,
        antfly_exe,
        base_uri,
        runtime_id,
        owner_id,
        worker_ids,
        lease_ttl_ms,
        test_now_ms,
        "2",
        ready_file,
    );
}

fn runAndKillServiceWorkerPoolAfterReadyWithMaxPages(
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    worker_ids: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
    max_pages: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--base-uri",
        base_uri,
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "worker_pool",
        "--runtime-id",
        runtime_id,
        "--owner-id",
        owner_id,
        "--worker-ids",
        worker_ids,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--coordinator-start-background-builds",
        "false",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        max_pages,
        "--test-now-ms",
        test_now_ms,
        "--test-ready-file",
        ready_file,
        "--test-hold-after-run-ms",
        "10000",
    };
    try runAndKillRoleProcessAfterReady(io, argv[0..], ready_file, error.GraphMetricLeaseProofFailed);
}

fn runAndKillRoleProcessAfterReady(
    io: std.Io,
    argv: []const []const u8,
    ready_file: []const u8,
    err: anyerror,
) !void {
    try verifyRoleProcessArgvScoped(argv);
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    errdefer child.kill(io);

    var ready = false;
    for (0..100) |_| {
        std.Io.Dir.cwd().access(io, ready_file, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        ready = true;
        break;
    }
    if (!ready) {
        child.kill(io);
        std.debug.print("timed out waiting for killable service role ready marker\n", .{});
        return err;
    }

    child.kill(io);
}

fn runAndKillDegreePageOwnerAfterReady(
    io: std.Io,
    harness_exe: []const u8,
    db_path: []const u8,
    worker_id: []const u8,
    now_ms: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        harness_exe,
        "claim-degree-page-hold",
        db_path,
        worker_id,
        now_ms,
        ready_file,
        "10000",
    };
    var child = try std.process.spawn(io, .{
        .argv = argv[0..],
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    errdefer child.kill(io);

    var ready = false;
    for (0..100) |_| {
        std.Io.Dir.cwd().access(io, ready_file, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        ready = true;
        break;
    }
    if (!ready) {
        child.kill(io);
        std.debug.print("timed out waiting for killable page owner ready marker\n", .{});
        return error.GraphMetricWorkerPageProofFailed;
    }

    child.kill(io);
}

fn runAndKillWorkerRoleAfterReady(
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    worker_id: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--db-path",
        db_path,
        "--role",
        "worker",
        "--runtime-id",
        owner_id,
        "--owner-id",
        owner_id,
        "--worker-id",
        worker_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--test-now-ms",
        test_now_ms,
        "--test-ready-file",
        ready_file,
        "--test-hold-after-run-ms",
        "10000",
    };
    try verifyRoleProcessArgvScoped(argv[0..]);
    var child = try std.process.spawn(io, .{
        .argv = argv[0..],
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    errdefer child.kill(io);

    var ready = false;
    for (0..100) |_| {
        std.Io.Dir.cwd().access(io, ready_file, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        ready = true;
        break;
    }
    if (!ready) {
        child.kill(io);
        std.debug.print("timed out waiting for killable worker runtime ready marker\n", .{});
        return error.GraphMetricLeaseProofFailed;
    }

    child.kill(io);
}

fn runAndKillMetricPageOwnerAfterReady(
    io: std.Io,
    harness_exe: []const u8,
    db_path: []const u8,
    metric_name: []const u8,
    phase: []const u8,
    worker_id: []const u8,
    now_ms: []const u8,
    ready_file: []const u8,
) !void {
    const argv = [_][]const u8{
        harness_exe,
        "claim-metric-page-hold",
        db_path,
        metric_name,
        phase,
        worker_id,
        now_ms,
        ready_file,
        "10000",
    };
    var child = try std.process.spawn(io, .{
        .argv = argv[0..],
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .inherit,
    });
    errdefer child.kill(io);

    var ready = false;
    for (0..100) |_| {
        std.Io.Dir.cwd().access(io, ready_file, .{}) catch {
            platform.time.sleepNs(50 * std.time.ns_per_ms);
            continue;
        };
        ready = true;
        break;
    }
    if (!ready) {
        child.kill(io);
        std.debug.print("timed out waiting for killable metric page owner ready marker\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }

    child.kill(io);
}

fn readSingleLeasedDegreePage(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    expected_worker_id: []const u8,
    expected_cursor: []const u8,
) !PageLeaseSnapshot {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus("degree");
    defer status.deinit(alloc);
    if (status.build_pages.len != 1) {
        std.debug.print("expected one active degree page, got {d}\n", .{status.build_pages.len});
        return error.GraphMetricWorkerPageProofFailed;
    }
    const page = status.build_pages[0];
    if (page.state != antfly.graph.GraphIndex.GraphMetricBuildPageState.leased or
        page.phase != antfly.graph.GraphIndex.GraphMetricBuildPhase.scan_edges_and_out_degree or
        !std.mem.eql(u8, page.worker_id, expected_worker_id) or
        !std.mem.eql(u8, page.cursor, expected_cursor))
    {
        std.debug.print("unexpected active page state in worker page proof\n", .{});
        return error.GraphMetricWorkerPageProofFailed;
    }
    return .{
        .job_id = status.build_job_id,
        .page_id = page.page_id,
        .iteration = page.iteration,
        .attempt = page.attempt,
        .lease_expires_at_ms = page.lease_expires_at_ms,
        .total_units = page.total_units,
    };
}

fn readSingleLeasedMetricPage(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_worker_id: []const u8,
    expected_cursor: []const u8,
) !PageLeaseSnapshot {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.build_pages.len != 1) {
        std.debug.print("expected one active metric page, got {d}\n", .{status.build_pages.len});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    const page = status.build_pages[0];
    if (page.state != antfly.graph.GraphIndex.GraphMetricBuildPageState.leased or
        page.phase != expected_phase or
        !std.mem.eql(u8, page.worker_id, expected_worker_id) or
        !std.mem.eql(u8, page.cursor, expected_cursor))
    {
        std.debug.print("unexpected active metric page state in process proof\n", .{});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    return .{
        .job_id = status.build_job_id,
        .page_id = page.page_id,
        .iteration = page.iteration,
        .attempt = page.attempt,
        .lease_expires_at_ms = page.lease_expires_at_ms,
        .total_units = page.total_units,
    };
}

fn readLeasedMetricPage(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_worker_id: []const u8,
    expected_cursor: []const u8,
) !PageLeaseSnapshot {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    for (status.build_pages) |page| {
        if (page.state == antfly.graph.GraphIndex.GraphMetricBuildPageState.leased and
            page.phase == expected_phase and
            std.mem.eql(u8, page.worker_id, expected_worker_id) and
            std.mem.eql(u8, page.cursor, expected_cursor))
        {
            return .{
                .job_id = status.build_job_id,
                .page_id = page.page_id,
                .iteration = page.iteration,
                .attempt = page.attempt,
                .lease_expires_at_ms = page.lease_expires_at_ms,
                .total_units = page.total_units,
            };
        }
    }
    std.debug.print("expected leased metric page for {s} phase {} owned by {s}\n", .{ metric_name, expected_phase, expected_worker_id });
    return error.GraphMetricPageRankProcessProofFailed;
}

fn invalidateMetricBuildManifestConfigFingerprintForTest(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.phase != .publish_generation or status.build_job_id == 0) {
        std.debug.print("expected {s} publish build before manifest invalidation\n", .{metric_name});
        return error.GraphMetricUnexpectedPhase;
    }
    try graph_entry.index.invalidateGraphMetricBuildManifestConfigFingerprintForTest(metric_name, status.build_job_id);
}

fn expectStaleDegreePageAttemptRejected(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    stale_page: PageLeaseSnapshot,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    _ = graph_entry.index.completeGraphMetricBuildPageForAttempt(
        "degree",
        stale_page.job_id,
        .scan_edges_and_out_degree,
        0,
        stale_page.page_id,
        "process-dead-worker",
        stale_page.attempt,
        stale_page.total_units,
        0,
    ) catch |err| switch (err) {
        error.GraphMetricBuildPageNotLeased => return,
        error.GraphMetricBuildPageNotFound => return,
        else => return err,
    };
    std.debug.print("expected stale degree page attempt completion to be rejected\n", .{});
    return error.GraphMetricWorkerPageProofFailed;
}

fn expectStaleMetricPageAttemptRejected(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    stale_page: PageLeaseSnapshot,
    stale_worker_id: []const u8,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    _ = graph_entry.index.completeGraphMetricBuildPageForAttempt(
        metric_name,
        stale_page.job_id,
        phase,
        stale_page.iteration,
        stale_page.page_id,
        stale_worker_id,
        stale_page.attempt,
        stale_page.total_units,
        0,
    ) catch |err| switch (err) {
        error.GraphMetricBuildPageNotLeased => return,
        error.GraphMetricBuildPageNotFound => return,
        else => return err,
    };
    std.debug.print("expected stale metric page attempt completion to be rejected\n", .{});
    return error.GraphMetricPageRankProcessProofFailed;
}

fn expectReclaimedMetricPageCompleted(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    stale_page: PageLeaseSnapshot,
    reclaim_worker_id: []const u8,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const page = try graph_entry.index.graphMetricBuildPageSnapshotForTest(
        metric_name,
        stale_page.job_id,
        phase,
        stale_page.iteration,
        stale_page.page_id,
    ) orelse {
        std.debug.print("expected reclaimed {s} page record to remain durable\n", .{metric_name});
        return error.GraphMetricProcessProofFailed;
    };
    if (page.state != antfly.graph.GraphIndex.GraphMetricBuildPageState.complete or
        page.worker_id_hash != identityHash(reclaim_worker_id) or
        page.attempt <= stale_page.attempt or
        page.completed_units != page.total_units)
    {
        std.debug.print("expected reclaimed {s} page to complete under replacement worker and newer attempt\n", .{metric_name});
        return error.GraphMetricProcessProofFailed;
    }
}

fn runCoordinatorRoleProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
) !RoleRunSummary {
    return try runCoordinatorRoleProcessAt(alloc, io, antfly_exe, db_path, owner_id, lease_ttl_ms, null);
}

fn runCoordinatorRoleProcessAt(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: ?[]const u8,
) !RoleRunSummary {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--db-path",
        db_path,
        "--role",
        "coordinator",
        "--runtime-id",
        owner_id,
        "--owner-id",
        owner_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    };
    if (test_now_ms) |now_ms| {
        const argv_with_now = [_][]const u8{
            antfly_exe,
            "__graph-metric-maintenance",
            "--db-path",
            db_path,
            "--role",
            "coordinator",
            "--runtime-id",
            owner_id,
            "--owner-id",
            owner_id,
            "--lease-ttl-ms",
            lease_ttl_ms,
            "--ticks",
            "1",
            "--max-rounds",
            "1",
            "--max-metrics",
            "4",
            "--max-pages",
            "1",
            "--test-now-ms",
            now_ms,
        };
        const summary = try runRoleProcess(alloc, io, argv_with_now[0..]);
        try verifyRoleProcessTelemetry(summary, .coordinator, owner_id, 0, 0);
        return summary;
    }
    const summary = try runRoleProcess(alloc, io, argv[0..]);
    try verifyRoleProcessTelemetry(summary, .coordinator, owner_id, 0, 0);
    return summary;
}

fn runServiceCoordinatorRoleProcessAt(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
) !RoleRunSummary {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--base-uri",
        base_uri,
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "coordinator",
        "--runtime-id",
        runtime_id,
        "--owner-id",
        owner_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
        "--test-now-ms",
        test_now_ms,
    };
    const summary = try runRoleProcess(alloc, io, argv[0..]);
    try verifyServiceRoleProcessTelemetry(summary, .coordinator, runtime_id, owner_id, 0, 0);
    return summary;
}

fn runWorkerRoleProcessAt(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    worker_id: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
) !RoleRunSummary {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--db-path",
        db_path,
        "--role",
        "worker",
        "--runtime-id",
        owner_id,
        "--owner-id",
        owner_id,
        "--worker-id",
        worker_id,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--test-now-ms",
        test_now_ms,
    };
    const summary = try runRoleProcess(alloc, io, argv[0..]);
    try verifyRoleProcessTelemetry(summary, .worker, owner_id, identityHash(worker_id), 1);
    return summary;
}

fn runWorkerPoolRoleProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
    owner_id: []const u8,
    lease_ttl_ms: []const u8,
) !RoleRunSummary {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--db-path",
        db_path,
        "--role",
        "worker_pool",
        "--runtime-id",
        owner_id,
        "--owner-id",
        owner_id,
        "--worker-ids",
        "lease-proof-worker-a,lease-proof-worker-b",
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--ticks",
        "4",
        "--max-idle-ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
    };
    const summary = try runRoleProcess(alloc, io, argv[0..]);
    const worker_ids = [_][]const u8{ "lease-proof-worker-a", "lease-proof-worker-b" };
    try verifyRoleProcessTelemetry(summary, .worker_pool, owner_id, workerSetHash(worker_ids[0..]), worker_ids.len);
    return summary;
}

fn runServiceWorkerPoolRoleProcessAt(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    worker_ids_csv: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
) !RoleRunSummary {
    return runServiceWorkerPoolRoleProcessAtWithMaxPages(
        alloc,
        io,
        antfly_exe,
        base_uri,
        runtime_id,
        owner_id,
        worker_ids_csv,
        lease_ttl_ms,
        test_now_ms,
        "2",
    );
}

fn runServiceWorkerPoolRoleProcessAtWithMaxPages(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    base_uri: []const u8,
    runtime_id: []const u8,
    owner_id: []const u8,
    worker_ids_csv: []const u8,
    lease_ttl_ms: []const u8,
    test_now_ms: []const u8,
    max_pages: []const u8,
) !RoleRunSummary {
    const argv = [_][]const u8{
        antfly_exe,
        "__graph-metric-maintenance",
        "--base-uri",
        base_uri,
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "worker_pool",
        "--runtime-id",
        runtime_id,
        "--owner-id",
        owner_id,
        "--worker-ids",
        worker_ids_csv,
        "--lease-ttl-ms",
        lease_ttl_ms,
        "--coordinator-start-background-builds",
        "false",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        max_pages,
        "--test-now-ms",
        test_now_ms,
    };
    const summary = try runRoleProcess(alloc, io, argv[0..]);
    const worker_ids = [_][]const u8{ "service-process-worker-a", "service-process-worker-b" };
    try verifyServiceRoleProcessTelemetry(summary, .worker_pool, runtime_id, owner_id, workerSetHash(worker_ids[0..]), worker_ids.len);
    return summary;
}

fn verifyRoleProcessTelemetry(
    summary: RoleRunSummary,
    role: RuntimeRole,
    owner_id: []const u8,
    worker_id_hash: u64,
    worker_count: usize,
) !void {
    const stats = summary.stats;
    if (stats.role != role) return error.GraphMetricRoleProcessFailed;
    const expected_owner_hash = identityHash(owner_id);
    if (stats.runtime_id_hash != expected_owner_hash) return error.GraphMetricRoleProcessFailed;
    if (stats.owner_id_hash != expected_owner_hash) return error.GraphMetricRoleProcessFailed;
    if (stats.lease_key_hash == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.worker_id_hash != worker_id_hash) return error.GraphMetricRoleProcessFailed;
    if (stats.worker_count != worker_count) return error.GraphMetricRoleProcessFailed;
    if (!stats.lease_owned) return error.GraphMetricRoleProcessFailed;
    try verifyRoleProcessLeaseAccounting(stats);
    if (stats.ticks_started == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.ticks_completed == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.error_ticks != 0) return error.GraphMetricRoleProcessFailed;
    try verifyRoleProcessTickAccounting(stats);
}

fn verifyServiceRoleProcessTelemetry(
    summary: RoleRunSummary,
    role: RuntimeRole,
    runtime_id: []const u8,
    owner_id: []const u8,
    worker_id_hash: u64,
    worker_count: usize,
) !void {
    const stats = summary.stats;
    if (stats.role != role) return error.GraphMetricRoleProcessFailed;
    if (stats.runtime_id_hash != identityHash(runtime_id)) return error.GraphMetricRoleProcessFailed;
    if (stats.owner_id_hash != identityHash(owner_id)) return error.GraphMetricRoleProcessFailed;
    if (stats.lease_key_hash == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.worker_id_hash != worker_id_hash) return error.GraphMetricRoleProcessFailed;
    if (stats.worker_count != worker_count) return error.GraphMetricRoleProcessFailed;
    if (!stats.lease_owned) return error.GraphMetricRoleProcessFailed;
    try verifyRoleProcessLeaseAccounting(stats);
    if (stats.ticks_started == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.ticks_completed == 0) return error.GraphMetricRoleProcessFailed;
    if (stats.error_ticks != 0) return error.GraphMetricRoleProcessFailed;
    try verifyRoleProcessTickAccounting(stats);
}

fn verifyRoleProcessLeaseAccounting(stats: RuntimeStats) !void {
    if (stats.acquisition_count == 0 and stats.lease_acquire_failures == 0) {
        return error.GraphMetricRoleProcessFailed;
    }
    if (stats.has_lease and stats.acquisition_count == 0) {
        return error.GraphMetricRoleProcessFailed;
    }
}

fn verifyRoleProcessTickAccounting(stats: RuntimeStats) !void {
    if (stats.ticks_completed > stats.ticks_started) return error.GraphMetricRoleProcessFailed;
    const accounted_ticks = stats.durable_progress_ticks + stats.idle_ticks + stats.error_ticks;
    if (accounted_ticks > stats.ticks_completed) return error.GraphMetricRoleProcessFailed;
    const fenced_ticks = stats.lease_acquire_failures + stats.lost_leases;
    if (accounted_ticks + fenced_ticks < stats.ticks_completed) return error.GraphMetricRoleProcessFailed;
}

fn identityHash(value: []const u8) u64 {
    if (value.len == 0) return 0;
    return std.hash.Wyhash.hash(0, value);
}

fn workerSetHash(worker_ids: []const []const u8) u64 {
    var xor_hash: u64 = 0;
    var sum_hash: u64 = 0;
    for (worker_ids) |worker_id| {
        const item_hash = identityHash(worker_id);
        xor_hash ^= item_hash;
        sum_hash +%= item_hash;
    }
    const fingerprint_words = [_]u64{
        @intCast(worker_ids.len),
        xor_hash,
        sum_hash,
    };
    return std.hash.Wyhash.hash(0, std.mem.asBytes(&fingerprint_words));
}

fn runRoleProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
) !RoleRunSummary {
    try verifyRoleProcessArgvScoped(argv);
    const result = try std.process.run(alloc, io, .{
        .argv = argv,
        .reserve_amount = 512,
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) {
            std.debug.print(
                "graph metric role process exited with code {d}\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ code, result.stdout, result.stderr },
            );
            return error.GraphMetricRoleProcessFailed;
        },
        else => {
            std.debug.print(
                "graph metric role process terminated unexpectedly\nstdout:\n{s}\nstderr:\n{s}\n",
                .{ result.stdout, result.stderr },
            );
            return error.GraphMetricRoleProcessFailed;
        },
    }
    try verifyRoleProcessJsonStats(alloc, result.stdout);
    var parsed = try std.json.parseFromSlice(RoleRunSummary, alloc, result.stdout, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return parsed.value;
}

fn verifyRoleProcessJsonStats(alloc: std.mem.Allocator, stdout: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stdout, .{});
    defer parsed.deinit();
    try verifyJsonNoRawOperationalFields(parsed.value, error.GraphMetricRoleProcessFailed);
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.GraphMetricRoleProcessFailed,
    };
    const stats = switch (object.get("stats") orelse return error.GraphMetricRoleProcessFailed) {
        .object => |stats| stats,
        else => return error.GraphMetricRoleProcessFailed,
    };
    _ = stats.get("durable_progress_ticks") orelse return error.GraphMetricRoleProcessFailed;
    _ = stats.get("idle_ticks") orelse return error.GraphMetricRoleProcessFailed;
    _ = stats.get("error_ticks") orelse return error.GraphMetricRoleProcessFailed;
    switch (stats.get("last_error_name") orelse return error.GraphMetricRoleProcessFailed) {
        .null => {},
        else => return error.GraphMetricRoleProcessFailed,
    }
}

fn verifyJsonNoRawOperationalFields(value: std.json.Value, comptime failure_error: anyerror) !void {
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (roleProcessJsonFieldForbidden(entry.key_ptr.*)) {
                    std.debug.print("graph metric process summary leaked raw graph metric field {s}\n", .{entry.key_ptr.*});
                    return failure_error;
                }
                try verifyJsonNoRawOperationalFields(entry.value_ptr.*, failure_error);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try verifyJsonNoRawOperationalFields(item, failure_error);
            }
        },
        else => {},
    }
}

fn roleProcessJsonFieldForbidden(field: []const u8) bool {
    const forbidden = [_][]const u8{
        "metric_name",
        "metric_names",
        "index_name",
        "target_generation",
        "building_generation",
        "job_id",
        "page_id",
        "attempt",
        "attempt_namespace",
        "manifest_path",
        "score_prefix",
        "output_prefix",
        "metric_config",
        "metric_configs",
        "config_fingerprint",
        "db_path",
        "base_uri",
        "process_id",
        "pid",
        "summary_file",
        "writer_guard",
    };
    for (forbidden) |item| {
        if (std.mem.eql(u8, field, item)) return true;
    }
    return false;
}

fn verifyRoleProcessArgvScoped(argv: []const []const u8) !void {
    if (argv.len < 4) return error.GraphMetricRoleProcessFailed;
    if (!std.mem.eql(u8, argv[1], "__graph-metric-maintenance")) return error.GraphMetricRoleProcessFailed;
    try verifyRoleProcessArgvAllowlist(argv);
    const has_db_path = processArgvContains(argv, "--db-path");
    const has_base_uri = processArgvContains(argv, "--base-uri") or processArgvContains(argv, "--service-base-uri");
    const has_group_id = processArgvContains(argv, "--group-id");
    const has_table_name = processArgvContains(argv, "--table-name");
    if (has_db_path and (has_base_uri or has_group_id or has_table_name)) return error.GraphMetricRoleProcessFailed;
    if (!has_db_path and !(has_base_uri and has_group_id and has_table_name)) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--role")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--runtime-id")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--owner-id")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--lease-ttl-ms")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--ticks")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--max-rounds")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--max-metrics")) return error.GraphMetricRoleProcessFailed;
    if (!processArgvContains(argv, "--max-pages")) return error.GraphMetricRoleProcessFailed;

    const forbidden = [_][]const u8{
        "--index",
        "--index-name",
        "--metric",
        "--metric-name",
        "--metric-config",
        "--target-generation",
        "--job-id",
        "--page-id",
        "--phase",
        "--summary-file",
        "--local-db-writer-lock",
    };
    if (processArgvContainsAny(argv, forbidden[0..])) return error.GraphMetricRoleProcessFailed;

    const role = processArgvValue(argv, "--role") orelse return error.GraphMetricRoleProcessFailed;
    if (std.mem.eql(u8, role, "coordinator")) {
        if (processArgvContains(argv, "--worker-id")) return error.GraphMetricRoleProcessFailed;
        if (processArgvContains(argv, "--worker-ids")) return error.GraphMetricRoleProcessFailed;
    } else if (std.mem.eql(u8, role, "worker")) {
        if (!processArgvContains(argv, "--worker-id")) return error.GraphMetricRoleProcessFailed;
        if (processArgvContains(argv, "--worker-ids")) return error.GraphMetricRoleProcessFailed;
    } else if (std.mem.eql(u8, role, "worker_pool")) {
        if (processArgvContains(argv, "--worker-id")) return error.GraphMetricRoleProcessFailed;
        if (!processArgvContains(argv, "--worker-ids")) return error.GraphMetricRoleProcessFailed;
    } else {
        return error.GraphMetricRoleProcessFailed;
    }
}

fn verifyRoleProcessArgvAllowlist(argv: []const []const u8) !void {
    var i: usize = 2;
    while (i < argv.len) : (i += 2) {
        const flag = argv[i];
        if (!std.mem.startsWith(u8, flag, "--")) return error.GraphMetricRoleProcessFailed;
        if (!roleProcessArgvFlagAllowed(flag)) return error.GraphMetricRoleProcessFailed;
        if (i + 1 >= argv.len) return error.GraphMetricRoleProcessFailed;
        if (std.mem.startsWith(u8, argv[i + 1], "--")) return error.GraphMetricRoleProcessFailed;
    }
}

fn roleProcessArgvFlagAllowed(flag: []const u8) bool {
    const allowed = [_][]const u8{
        "--db-path",
        "--base-uri",
        "--service-base-uri",
        "--group-id",
        "--table-name",
        "--role",
        "--runtime-id",
        "--owner-id",
        "--worker-id",
        "--worker-ids",
        "--lease-ttl-ms",
        "--coordinator-start-background-builds",
        "--ticks",
        "--max-idle-ticks",
        "--max-rounds",
        "--max-metrics",
        "--max-pages",
        "--test-now-ms",
        "--test-ready-file",
        "--test-hold-after-run-ms",
    };
    for (allowed) |allowed_flag| {
        if (std.mem.eql(u8, flag, allowed_flag)) return true;
    }
    return false;
}

fn verifyRoleProcessArgvPreflightSelfTest() !void {
    try verifyRoleProcessArgvScoped(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "coordinator",
        "--runtime-id",
        "coordinator-owner",
        "--owner-id",
        "coordinator-owner",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try verifyRoleProcessArgvScoped(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--test-now-ms",
        "1000",
    });
    try verifyRoleProcessArgvScoped(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker_pool",
        "--runtime-id",
        "pool-owner",
        "--owner-id",
        "pool-owner",
        "--worker-ids",
        "worker-a,worker-b",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-idle-ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
    });
    try verifyRoleProcessArgvScoped(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--base-uri",
        "http://127.0.0.1:8080",
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "coordinator",
        "--runtime-id",
        "service-coordinator-owner",
        "--owner-id",
        "service-coordinator-owner",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try verifyRoleProcessArgvScoped(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--base-uri",
        "http://127.0.0.1:8080",
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "worker_pool",
        "--runtime-id",
        "service-pool-owner",
        "--owner-id",
        "service-pool-owner",
        "--worker-ids",
        "worker-a,worker-b",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-idle-ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
    });

    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "coordinator",
        "--runtime-id",
        "coordinator-owner",
        "--owner-id",
        "coordinator-owner",
        "--worker-ids",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker_pool",
        "--runtime-id",
        "pool-owner",
        "--owner-id",
        "pool-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "2",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--base-uri",
        "http://127.0.0.1:8080",
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--role",
        "coordinator",
        "--runtime-id",
        "coordinator-owner",
        "--owner-id",
        "coordinator-owner",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--base-uri",
        "http://127.0.0.1:8080",
        "--group-id",
        "7",
        "--role",
        "coordinator",
        "--runtime-id",
        "coordinator-owner",
        "--owner-id",
        "coordinator-owner",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--metric-name",
        "pagerank",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--local-db-writer-lock",
        "true",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
        "1",
        "--unexpected-owner-input",
        "value",
    });
    try expectRoleProcessArgvRejected(&.{
        "antfly",
        "__graph-metric-maintenance",
        "--db-path",
        "/tmp/db",
        "--role",
        "worker",
        "--runtime-id",
        "worker-owner",
        "--owner-id",
        "worker-owner",
        "--worker-id",
        "worker-a",
        "--lease-ttl-ms",
        "5000",
        "--ticks",
        "1",
        "--max-rounds",
        "1",
        "--max-metrics",
        "4",
        "--max-pages",
    });
}

fn expectRoleProcessArgvRejected(argv: []const []const u8) !void {
    verifyRoleProcessArgvScoped(argv) catch return;
    return error.GraphMetricProcessProofFailed;
}

fn processArgvContains(argv: []const []const u8, needle: []const u8) bool {
    for (argv) |arg| {
        if (std.mem.eql(u8, arg, needle)) return true;
    }
    return false;
}

fn processArgvContainsAny(argv: []const []const u8, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (processArgvContains(argv, needle)) return true;
    }
    return false;
}

fn processArgvValue(argv: []const []const u8, flag: []const u8) ?[]const u8 {
    for (argv, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, flag)) {
            if (i + 1 >= argv.len) return null;
            return argv[i + 1];
        }
    }
    return null;
}

fn verifyDegreeFresh(alloc: std.mem.Allocator, db_path: []const u8, target_generation: u64) !void {
    return verifyMetricFresh(alloc, db_path, "degree", target_generation);
}

fn verifyMetricFresh(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.state != antfly.graph.GraphIndex.GraphMetricState.fresh) {
        std.debug.print("expected fresh graph metric, got {}\n", .{status.state});
        return error.GraphMetricNotFresh;
    }
    if (status.published_generation != target_generation) {
        std.debug.print(
            "expected published generation {d}, got {d}\n",
            .{ target_generation, status.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
}

fn verifyHitsFresh(alloc: std.mem.Allocator, db_path: []const u8, target_generation: u64) !void {
    try verifyMetricFresh(alloc, db_path, "hits_authority", target_generation);
    try verifyMetricFresh(alloc, db_path, "hits_hub", target_generation);

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    const authority_top = try graph_entry.index.graphMetricTopK("hits_authority", 3);
    defer {
        for (authority_top) |*score| score.deinit(alloc);
        alloc.free(authority_top);
    }
    const hub_top = try graph_entry.index.graphMetricTopK("hits_hub", 3);
    defer {
        for (hub_top) |*score| score.deinit(alloc);
        alloc.free(hub_top);
    }
    if (authority_top.len == 0 or hub_top.len == 0) {
        std.debug.print("expected paired HITS top-k scores after process supervisor publish\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
}

fn verifyPageRankFixedIterationMetadata(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
    expected_iterations_completed: u32,
) !void {
    return verifyFixedIterationMetadata(
        alloc,
        db_path,
        "pagerank",
        target_generation,
        expected_iterations_completed,
    );
}

fn verifyFixedIterationMetadata(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    target_generation: u64,
    expected_iterations_completed: u32,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.state != antfly.graph.GraphIndex.GraphMetricState.fresh) {
        std.debug.print("expected fresh {s} metric, got {}\n", .{ metric_name, status.state });
        return error.GraphMetricNotFresh;
    }
    if (status.published_generation != target_generation) {
        std.debug.print(
            "expected {s} published generation {d}, got {d}\n",
            .{ metric_name, target_generation, status.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (status.converged) {
        std.debug.print("expected bounded {s} process publish to report converged=false\n", .{metric_name});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (status.iterations_completed != expected_iterations_completed) {
        std.debug.print(
            "expected {s} iterations_completed {d}, got {d}\n",
            .{ metric_name, expected_iterations_completed, status.iterations_completed },
        );
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (!std.math.isFinite(status.delta) or status.delta <= 0.0) {
        std.debug.print("expected positive finite {s} fixed-iteration delta, got {d}\n", .{ metric_name, status.delta });
        return error.GraphMetricPageRankProcessProofFailed;
    }
}

fn verifyHitsFixedIterationMetadata(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    target_generation: u64,
    expected_iterations_completed: u32,
) !void {
    try verifyFixedIterationMetadata(
        alloc,
        db_path,
        "hits_authority",
        target_generation,
        expected_iterations_completed,
    );
    try verifyFixedIterationMetadata(
        alloc,
        db_path,
        "hits_hub",
        target_generation,
        expected_iterations_completed,
    );
}

fn verifyMetricFailedPreservesPublished(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    published_generation: u64,
    expected_last_error: []const u8,
) !void {
    return verifyMetricFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        metric_name,
        published_generation,
        expected_last_error,
        .publish_generation,
        null,
    );
}

fn verifyMetricFailedPreservesPublishedAtPhase(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    metric_name: []const u8,
    published_generation: u64,
    expected_last_error: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_iteration: ?u32,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(alloc);
    if (status.state != antfly.graph.GraphIndex.GraphMetricState.failed) {
        std.debug.print("expected failed graph metric, got {}\n", .{status.state});
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (status.published_generation != published_generation) {
        std.debug.print(
            "expected failed metric to preserve published generation {d}, got {d}\n",
            .{ published_generation, status.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (!std.mem.eql(u8, status.last_error, expected_last_error)) {
        std.debug.print("expected last error {s}, got {s}\n", .{ expected_last_error, status.last_error });
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (status.recent_failures.len == 0 or status.recent_failures[0].phase != expected_phase) {
        std.debug.print("expected retained {s} failure diagnostics for phase {}\n", .{ metric_name, expected_phase });
        return error.GraphMetricPageRankProcessProofFailed;
    }
    if (expected_iteration) |iteration| {
        if (status.recent_failures[0].iteration != iteration) {
            std.debug.print("expected retained {s} failure diagnostics for phase {} iteration {d}\n", .{ metric_name, expected_phase, iteration });
            return error.GraphMetricPageRankProcessProofFailed;
        }
    }
}

fn verifyHitsFailedPreservesPublished(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    published_generation: u64,
    expected_last_error: []const u8,
) !void {
    return verifyHitsFailedPreservesPublishedAtPhase(
        alloc,
        db_path,
        published_generation,
        expected_last_error,
        .publish_generation,
        0,
    );
}

fn verifyHitsFailedPreservesPublishedAtPhase(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    published_generation: u64,
    expected_last_error: []const u8,
    expected_phase: antfly.graph.GraphIndex.GraphMetricBuildPhase,
    expected_iteration: u32,
) !void {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var authority = try graph_entry.index.graphMetricStatus("hits_authority");
    defer authority.deinit(alloc);
    var hub = try graph_entry.index.graphMetricStatus("hits_hub");
    defer hub.deinit(alloc);
    if (authority.state != antfly.graph.GraphIndex.GraphMetricState.failed or
        hub.state != antfly.graph.GraphIndex.GraphMetricState.failed)
    {
        std.debug.print("expected failed HITS pair, got authority {} hub {}\n", .{ authority.state, hub.state });
        return error.GraphMetricProcessProofFailed;
    }
    if (authority.published_generation != published_generation or hub.published_generation != published_generation) {
        std.debug.print(
            "expected failed HITS pair to preserve published generation {d}, got authority {d} hub {d}\n",
            .{ published_generation, authority.published_generation, hub.published_generation },
        );
        return error.GraphMetricGenerationMismatch;
    }
    if (!std.mem.eql(u8, authority.last_error, expected_last_error) or !std.mem.eql(u8, hub.last_error, expected_last_error)) {
        std.debug.print(
            "expected HITS pair last error {s}, got authority {s} hub {s}\n",
            .{ expected_last_error, authority.last_error, hub.last_error },
        );
        return error.GraphMetricProcessProofFailed;
    }
    if (authority.recent_failures.len == 0 or hub.recent_failures.len == 0 or
        authority.recent_failures[0].phase != expected_phase or
        hub.recent_failures[0].phase != expected_phase or
        authority.recent_failures[0].iteration != expected_iteration or
        hub.recent_failures[0].iteration != expected_iteration)
    {
        std.debug.print("expected retained paired HITS failure diagnostics for phase {} iteration {d}\n", .{ expected_phase, expected_iteration });
        return error.GraphMetricProcessProofFailed;
    }

    const authority_top = try graph_entry.index.graphMetricTopK("hits_authority", 3);
    defer {
        for (authority_top) |*score| score.deinit(alloc);
        alloc.free(authority_top);
    }
    const hub_top = try graph_entry.index.graphMetricTopK("hits_hub", 3);
    defer {
        for (hub_top) |*score| score.deinit(alloc);
        alloc.free(hub_top);
    }
    if (authority_top.len == 0 or hub_top.len == 0) {
        std.debug.print("expected failed HITS pair to keep prior published top-k output visible\n", .{});
        return error.GraphMetricProcessProofFailed;
    }

    var direct_result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = "authority",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_authority",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
            .{
                .name = "hub",
                .query = .{
                    .index_name = "graph_idx",
                    .metric_name = "hits_hub",
                    .top_k = 3,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer direct_result.deinit();
    if (direct_result.graph_metric_results.len != 2) {
        std.debug.print("expected failed HITS direct query to return two graph metric results\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (direct_result.graph_metric_results) |result| {
        if (result.status.state != antfly.graph.GraphIndex.GraphMetricState.failed or
            result.status.published_generation != published_generation)
        {
            std.debug.print(
                "expected failed HITS direct result to preserve generation {d}, got state {} generation {d}\n",
                .{ published_generation, result.status.state, result.status.published_generation },
            );
            return error.GraphMetricProcessProofFailed;
        }
        if (result.scores.len == 0) {
            std.debug.print("expected failed HITS direct result to keep prior scores visible\n", .{});
            return error.GraphMetricProcessProofFailed;
        }
    }
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "authority",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "hits_authority",
                .top_k = 1,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    }));

    const published_metric_reads = [_]graph_query_mod.GraphMetricRead{
        .{ .name = "hits_authority", .freshness = .published },
        .{ .name = "hits_hub", .freshness = .published },
    };
    const published_graph_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:hub-a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1, .max_results = 10 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal_result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = published_graph_query }},
        .limit = 0,
    });
    defer traversal_result.deinit();
    if (traversal_result.graph_results.len != 1 or traversal_result.graph_results[0].nodes.len != 1) {
        std.debug.print("expected failed HITS traversal query to return one graph result\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const traversal = traversal_result.graph_results[0];
    if (!std.mem.eql(u8, traversal.nodes[0].key, "doc:authority") or traversal.nodes[0].metrics.len != 2) {
        std.debug.print("expected failed HITS traversal to project prior authority and hub scores\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.nodes[0].metrics) |metric| {
        if (metric.score == null) {
            std.debug.print("expected failed HITS traversal metric {s} to keep prior score visible\n", .{metric.name});
            return error.GraphMetricProcessProofFailed;
        }
    }
    if (traversal.metric_status.len != 2) {
        std.debug.print("expected failed HITS traversal to return two metric statuses\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    for (traversal.metric_status) |status| {
        if (status.state != antfly.graph.GraphIndex.GraphMetricState.failed or
            status.published_generation != published_generation)
        {
            std.debug.print(
                "expected failed HITS traversal status to preserve generation {d}, got state {} generation {d}\n",
                .{ published_generation, status.state, status.published_generation },
            );
            return error.GraphMetricProcessProofFailed;
        }
    }

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "hits_authority",
        .freshness = .fresh,
    }};
    var fresh_projection_query = published_graph_query;
    fresh_projection_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_projection_query }},
        .limit = 0,
    }));

    var rerank_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .published,
            .base_weight = 0.0,
            .weight = 1.0,
            .missing_score = -1.0,
        },
        .limit = 3,
        .include_stored = false,
    });
    defer rerank_result.deinit();
    if (rerank_result.hits.len == 0) {
        std.debug.print("expected failed HITS rerank to return hits from prior generation\n", .{});
        return error.GraphMetricProcessProofFailed;
    }
    const rerank_status = rerank_result.graph_metric_rerank_status orelse {
        std.debug.print("expected failed HITS rerank status\n", .{});
        return error.GraphMetricProcessProofFailed;
    };
    if (rerank_status.state != antfly.graph.GraphIndex.GraphMetricState.failed or
        rerank_status.published_generation != published_generation)
    {
        std.debug.print(
            "expected failed HITS rerank status to preserve generation {d}, got state {} generation {d}\n",
            .{ published_generation, rerank_status.state, rerank_status.published_generation },
        );
        return error.GraphMetricProcessProofFailed;
    }
    for (rerank_result.hits) |hit| {
        const details = hit.score_details orelse {
            std.debug.print("expected failed HITS reranked hit score details for {s}\n", .{hit.id});
            return error.GraphMetricProcessProofFailed;
        };
        if (details.published_generation != published_generation) {
            std.debug.print("expected failed HITS reranked hit {s} to use generation {d}, got {d}\n", .{ hit.id, published_generation, details.published_generation });
            return error.GraphMetricGenerationMismatch;
        }
    }
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "hits_authority",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 3,
        .include_stored = false,
    }));
}
