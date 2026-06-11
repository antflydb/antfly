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

const db_mod = antfly.db;
const graph_mod = antfly.graph;
const graph_query_mod = antfly.graph_query;
const platform_time = antfly.platform_time;
const query_api = antfly.public_api.query;
const types = db_mod.types;

const graph_index_name = "graph_idx";
const full_text_index_name = "release_ft";
const edge_type_name = "cites";
const active_probe_worker_id = "release-qualification-active-probe";
const stale_probe_worker_id = "release-qualification-stale-probe";
const early_probe_worker_id = "release-qualification-early-probe";

const Family = enum {
    degree,
    pagerank,
    eigenvector,
    hits,

    fn label(self: Family) []const u8 {
        return switch (self) {
            .degree => "degree",
            .pagerank => "pagerank",
            .eigenvector => "eigenvector",
            .hits => "hits",
        };
    }

    fn primaryMetric(self: Family) []const u8 {
        return switch (self) {
            .degree => "degree",
            .pagerank => "pagerank",
            .eigenvector => "eigenvector",
            .hits => "hits_authority",
        };
    }
};

const MaintenanceMode = enum {
    combined,
    split,

    fn label(self: MaintenanceMode) []const u8 {
        return switch (self) {
            .combined => "combined",
            .split => "split",
        };
    }

    fn parse(value: []const u8) !MaintenanceMode {
        if (std.mem.eql(u8, value, "combined")) return .combined;
        if (std.mem.eql(u8, value, "split")) return .split;
        std.debug.print("invalid maintenance mode: {s}\n", .{value});
        return error.InvalidArgument;
    }
};

const Config = struct {
    profile: []const u8 = "custom",
    maintenance_mode: MaintenanceMode = .combined,
    docs: usize = 130,
    fanout: usize = 1,
    top_k: usize = 32,
    synthetic_fan_in_shards: usize = 2,
    active_mutation_writes: usize = 1,
    workers: usize = 2,
    max_ticks: usize = 1000,
    max_rounds_per_tick: usize = 1,
    max_metrics_per_round: usize = 8,
    max_pages_per_round: usize = 1,
    max_iterations: usize = 3,
    failure_repeats: usize = 3,
    max_failure_diagnostics: usize = 8,
    max_status_pages: usize = 8,
    max_local_latency_ns: u64 = 0,
    max_planned_latency_ns: u64 = 0,
    max_cleanup_latency_ns: u64 = 0,
    max_published_read_latency_ns: u64 = 0,
    max_fresh_fail_latency_ns: u64 = 0,
    max_fan_in_latency_ns: u64 = 0,
    max_storage_metric_records: usize = 0,
    max_storage_control_records: usize = 0,
    max_page_claims: usize = 0,
    max_cleanup_ticks: usize = 0,
    max_rounds_executed: usize = 0,
    max_failure_retry_count: u64 = 0,
    max_worker_steps: usize = 0,
    max_coordinator_steps: usize = 0,
    tolerance: f64 = 0.000000001,
    family_filter: ?[]const u8 = null,
    reopen_between_ticks: bool = false,
    keep_tmp: bool = false,
};

const FamilyResult = struct {
    target_generation: u64,
    maintenance_mode: MaintenanceMode,
    graph_nodes: usize,
    edge_count: usize,
    graph_expected_nodes: usize,
    graph_expected_edges: usize,
    graph_source_nodes: usize,
    graph_sink_nodes: usize,
    graph_authority_nodes: usize,
    graph_sink_edges: usize,
    graph_cycle_edges: usize,
    graph_bipartite_edges: usize,
    graph_authority_self_edges: usize,
    graph_max_out_degree: usize,
    ticks: usize,
    budget_exhausted: bool,
    scheduler: db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
    combined_sweeps: usize,
    coordinator_sweeps: usize,
    worker_pool_sweeps: usize,
    worker_identities_configured: usize,
    split_worker_identities_with_progress: usize,
    pre_drain_metrics_scanned: usize,
    pre_drain_queued_builds: usize,
    pre_publish_not_ready_rejections: usize,
    hits_pre_publish_paired_not_ready_rejections: usize,
    pre_publish_rerank_not_ready_rejections: usize,
    hits_pre_publish_paired_rerank_not_ready_rejections: usize,
    pre_publish_traversal_projection_not_ready: usize,
    hits_pre_publish_paired_traversal_projection_not_ready: usize,
    pre_publish_traversal_not_ready_rejections: usize,
    hits_pre_publish_paired_traversal_not_ready_rejections: usize,
    pre_publish_status_not_ready: usize,
    hits_pre_publish_paired_status_not_ready: usize,
    fresh_pending_work: usize,
    fresh_active_builds: usize,
    fresh_active_pages: usize,
    fresh_failed_pages: usize,
    fresh_truncated_pages: bool,
    local_latency_ns: u64,
    planned_latency_ns: u64,
    cleanup_ticks: usize,
    cleanup_latency_ns: u64,
    reopen_count: usize,
    fresh_status_pages: usize,
    fresh_status_pages_truncated: bool,
    expected_score_records: usize,
    fresh_storage_score_records: usize,
    fresh_storage_metric_records: usize,
    fresh_storage_control_records: usize,
    fresh_storage_job_namespace_records: usize,
    active_published_read_ns: u64,
    active_fresh_fail_ns: u64,
    active_rerank_published_ns: u64,
    active_rerank_fresh_fail_ns: u64,
    active_traversal_published_ns: u64,
    active_traversal_fresh_fail_ns: u64,
    active_traversal_published_checks: usize,
    active_traversal_fresh_rejections: usize,
    active_profile_entries: usize,
    active_mutation_writes: usize,
    active_target_generation: u64,
    active_generation_delta: u64,
    hits_active_paired_published_results: usize,
    hits_active_paired_fresh_rejections: usize,
    hits_active_paired_rerank_published_results: usize,
    hits_active_paired_rerank_fresh_rejections: usize,
    hits_active_paired_traversal_metric_results: usize,
    active_page_probe_claimed: bool,
    active_page_probe_reclaimed: bool,
    active_status_pages: usize,
    active_status_pages_truncated: bool,
    active_status_progress: f64,
    active_pending_work: usize,
    active_work_active_builds: usize,
    active_work_active_pages: usize,
    active_work_failed_pages: usize,
    active_work_truncated_pages: bool,
    failed_published_read_ns: u64,
    failed_fresh_fail_ns: u64,
    failed_rerank_published_ns: u64,
    failed_rerank_fresh_fail_ns: u64,
    failed_traversal_published_ns: u64,
    failed_traversal_fresh_fail_ns: u64,
    failed_traversal_published_checks: usize,
    failed_traversal_fresh_rejections: usize,
    failed_profile_entries: usize,
    hits_failed_paired_published_results: usize,
    hits_failed_paired_fresh_rejections: usize,
    hits_failed_paired_rerank_published_results: usize,
    hits_failed_paired_rerank_fresh_rejections: usize,
    hits_failed_paired_traversal_metric_results: usize,
    failure_repeats: usize,
    max_failure_diagnostics: usize,
    failure_retry_count: u64,
    failure_recent_events: usize,
    failure_recent_failures: usize,
    paired_failure_recent_events: usize,
    paired_failure_recent_failures: usize,
    failed_status_pages: usize,
    failed_status_pages_truncated: bool,
    failed_storage_score_records: usize,
    failed_storage_metric_records: usize,
    failed_storage_control_records: usize,
    failed_storage_job_namespace_records: usize,
    failed_storage_failure_records: usize,
    failed_storage_event_records: usize,
    failed_pending_work: usize,
    failed_active_builds: usize,
    failed_active_pages: usize,
    failed_failed_pages: usize,
    failed_truncated_pages: bool,
    fan_in_shards: usize,
    fan_in_merged_scores: usize,
    fan_in_active_published_scores: usize,
    fan_in_fresh_rejections: usize,
    fan_in_generation_rejections: usize,
    fan_in_metadata_rejections: usize,
    fan_in_edge_filter_rejections: usize,
    fan_in_missing_rejections: usize,
    fan_in_merge_ns: u64,
    fan_in_active_published_ns: u64,
    fan_in_fresh_fail_ns: u64,
    fan_in_generation_fail_ns: u64,
    fan_in_metadata_fail_ns: u64,
    fan_in_edge_filter_fail_ns: u64,
    fan_in_missing_fail_ns: u64,
    fan_in_paired_metric_results: usize,
    fan_in_paired_active_published_metric_results: usize,
    fan_in_paired_fresh_rejections: usize,
    fan_in_paired_generation_rejections: usize,
    fan_in_paired_metadata_rejections: usize,
    fan_in_paired_edge_filter_rejections: usize,
    fan_in_paired_missing_rejections: usize,
    fan_in_paired_merge_ns: u64,
    fan_in_paired_active_published_ns: u64,
    fan_in_paired_fresh_fail_ns: u64,
    fan_in_paired_generation_fail_ns: u64,
    fan_in_paired_metadata_fail_ns: u64,
    fan_in_paired_edge_filter_fail_ns: u64,
    fan_in_paired_missing_fail_ns: u64,
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.smp_allocator;
    const cfg = try parseArgs(init.minimal.args);

    var stdout_buffer: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    defer out.flush() catch {};

    try out.print(
        "{{\"event\":\"graph_metric_release_qualification_config\",\"profile\":\"{s}\",\"maintenance_mode\":\"{s}\",\"docs\":{d},\"fanout\":{d},\"top_k\":{d},\"synthetic_fan_in_shards\":{d},\"active_mutation_writes\":{d},\"workers\":{d},\"max_ticks\":{d},\"max_rounds_per_tick\":{d},\"max_metrics_per_round\":{d},\"max_pages_per_round\":{d},\"max_iterations\":{d},\"failure_repeats\":{d},\"max_failure_diagnostics\":{d},\"max_status_pages\":{d}",
        .{ cfg.profile, cfg.maintenance_mode.label(), cfg.docs, cfg.fanout, cfg.top_k, cfg.synthetic_fan_in_shards, cfg.active_mutation_writes, cfg.workers, cfg.max_ticks, cfg.max_rounds_per_tick, cfg.max_metrics_per_round, cfg.max_pages_per_round, cfg.max_iterations, cfg.failure_repeats, cfg.max_failure_diagnostics, cfg.max_status_pages },
    );
    try out.print(
        ",\"max_local_latency_ns\":{d},\"max_planned_latency_ns\":{d},\"max_cleanup_latency_ns\":{d},\"max_published_read_latency_ns\":{d},\"max_fresh_fail_latency_ns\":{d},\"max_fan_in_latency_ns\":{d},\"max_storage_metric_records\":{d},\"max_storage_control_records\":{d},\"max_page_claims\":{d},\"max_cleanup_ticks\":{d},\"max_rounds_executed\":{d},\"max_failure_retry_count\":{d},\"max_worker_steps\":{d},\"max_coordinator_steps\":{d},\"reopen_between_ticks\":{},\"tolerance\":{d:.12}}}\n",
        .{ cfg.max_local_latency_ns, cfg.max_planned_latency_ns, cfg.max_cleanup_latency_ns, cfg.max_published_read_latency_ns, cfg.max_fresh_fail_latency_ns, cfg.max_fan_in_latency_ns, cfg.max_storage_metric_records, cfg.max_storage_control_records, cfg.max_page_claims, cfg.max_cleanup_ticks, cfg.max_rounds_executed, cfg.max_failure_retry_count, cfg.max_worker_steps, cfg.max_coordinator_steps, cfg.reopen_between_ticks, cfg.tolerance },
    );

    const families = [_]Family{ .degree, .pagerank, .eigenvector, .hits };
    for (families) |family| {
        if (!shouldRunFamily(cfg, family)) continue;
        const result = try runFamily(init.io, alloc, cfg, family);
        try emitFamilyResult(out, family, result);
    }
}

fn runFamily(io: anytype, alloc: std.mem.Allocator, cfg: Config, family: Family) !FamilyResult {
    var local_path_buf: [256]u8 = undefined;
    const local_path = std.fmt.bufPrint(
        &local_path_buf,
        "/tmp/antfly-graph-metric-rq-{s}-local-{d}",
        .{ family.label(), platform_time.monotonicNs() },
    ) catch unreachable;
    var planned_path_buf: [256]u8 = undefined;
    const planned_path = std.fmt.bufPrint(
        &planned_path_buf,
        "/tmp/antfly-graph-metric-rq-{s}-planned-{d}",
        .{ family.label(), platform_time.monotonicNs() },
    ) catch unreachable;
    std.Io.Dir.cwd().deleteTree(io, local_path) catch {};
    std.Io.Dir.cwd().deleteTree(io, planned_path) catch {};
    defer {
        if (!cfg.keep_tmp) {
            std.Io.Dir.cwd().deleteTree(io, local_path) catch {};
            std.Io.Dir.cwd().deleteTree(io, planned_path) catch {};
        }
    }

    var local_db = try db_mod.DB.open(alloc, local_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer local_db.close();
    var planned_db = try db_mod.DB.open(alloc, planned_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer planned_db.close();

    const config_json = try graphIndexConfig(alloc, cfg, family);
    defer alloc.free(config_json);
    try local_db.addIndex(.{
        .name = full_text_index_name,
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try planned_db.addIndex(.{
        .name = full_text_index_name,
        .kind = .full_text,
        .config_json = "{\"store\":true}",
    });
    try local_db.addIndex(.{
        .name = graph_index_name,
        .kind = .graph,
        .config_json = config_json,
    });
    try planned_db.addIndex(.{
        .name = graph_index_name,
        .kind = .graph,
        .config_json = config_json,
    });

    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    try seedWrites(alloc, &writes, family, cfg);
    try local_db.batch(.{ .writes = writes.items, .sync_level = .write });
    try planned_db.batch(.{ .writes = writes.items, .sync_level = .write });
    try local_db.runDerivedUntil(local_db.core.nextDerivedSequence());
    try planned_db.runDerivedUntil(planned_db.core.nextDerivedSequence());

    const target_generation = blk: {
        const graph_entry = planned_db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    const graph_shape = try verifyGraphShape(&planned_db, family, cfg, target_generation);
    const pre_drain_work = try verifyQueuedPlannedWork(&planned_db, family);
    const pre_publish_reads = try verifyPrePublishNotReady(alloc, &planned_db, family, target_generation);

    const local_started = platform_time.monotonicNs();
    try refreshLocalOracle(alloc, &local_db, family, target_generation);
    const local_latency_ns = platform_time.monotonicNs() - local_started;

    const planned_started = platform_time.monotonicNs();
    const drain = try drainPlanned(alloc, &planned_db, planned_path, cfg, family, target_generation);
    const planned_latency_ns = platform_time.monotonicNs() - planned_started;

    try compareFamilyTopK(alloc, &local_db, &planned_db, family, cfg.top_k);

    const expected_score_records = expectedScoreRecords(cfg, family);
    const fresh_status = try verifyPublishedStatusBounded(&planned_db, family, target_generation);
    const fresh_storage = try verifyFreshStorageFootprintBounded(&planned_db, family, expected_score_records);
    const fresh_work = try verifyNoPendingPlannedWork(&planned_db);
    const fan_in = try verifySyntheticFanIn(alloc, &planned_db, family, cfg, target_generation);
    const active_reads = try measureActiveReadContract(alloc, &planned_db, cfg, family, target_generation, expected_score_records);

    const result: FamilyResult = .{
        .target_generation = target_generation,
        .maintenance_mode = cfg.maintenance_mode,
        .graph_nodes = graph_shape.actual_nodes,
        .edge_count = graph_shape.actual_edges,
        .graph_expected_nodes = graph_shape.expected_nodes,
        .graph_expected_edges = graph_shape.expected_edges,
        .graph_source_nodes = graph_shape.source_nodes,
        .graph_sink_nodes = graph_shape.sink_nodes,
        .graph_authority_nodes = graph_shape.authority_nodes,
        .graph_sink_edges = graph_shape.sink_edges,
        .graph_cycle_edges = graph_shape.cycle_edges,
        .graph_bipartite_edges = graph_shape.bipartite_edges,
        .graph_authority_self_edges = graph_shape.authority_self_edges,
        .graph_max_out_degree = graph_shape.max_out_degree,
        .ticks = drain.ticks,
        .budget_exhausted = drain.total.budget_exhausted,
        .scheduler = drain.total,
        .combined_sweeps = drain.combined_sweeps,
        .coordinator_sweeps = drain.coordinator_sweeps,
        .worker_pool_sweeps = drain.worker_pool_sweeps,
        .worker_identities_configured = cfg.workers,
        .split_worker_identities_with_progress = drain.split_worker_identities_with_progress,
        .pre_drain_metrics_scanned = pre_drain_work.metrics_scanned,
        .pre_drain_queued_builds = pre_drain_work.queued_builds,
        .pre_publish_not_ready_rejections = pre_publish_reads.primary_not_ready_rejections,
        .hits_pre_publish_paired_not_ready_rejections = pre_publish_reads.hits_paired_not_ready_rejections,
        .pre_publish_rerank_not_ready_rejections = pre_publish_reads.primary_rerank_not_ready_rejections,
        .hits_pre_publish_paired_rerank_not_ready_rejections = pre_publish_reads.hits_paired_rerank_not_ready_rejections,
        .pre_publish_traversal_projection_not_ready = pre_publish_reads.primary_traversal_projection_not_ready,
        .hits_pre_publish_paired_traversal_projection_not_ready = pre_publish_reads.hits_paired_traversal_projection_not_ready,
        .pre_publish_traversal_not_ready_rejections = pre_publish_reads.primary_traversal_not_ready_rejections,
        .hits_pre_publish_paired_traversal_not_ready_rejections = pre_publish_reads.hits_paired_traversal_not_ready_rejections,
        .pre_publish_status_not_ready = pre_publish_reads.primary_status_not_ready,
        .hits_pre_publish_paired_status_not_ready = pre_publish_reads.hits_paired_status_not_ready,
        .fresh_pending_work = fresh_work.pendingWork(),
        .fresh_active_builds = fresh_work.active_builds,
        .fresh_active_pages = fresh_work.active_pages,
        .fresh_failed_pages = fresh_work.failed_pages,
        .fresh_truncated_pages = fresh_work.truncated_pages,
        .local_latency_ns = local_latency_ns,
        .planned_latency_ns = planned_latency_ns,
        .cleanup_ticks = drain.cleanup_ticks,
        .cleanup_latency_ns = drain.cleanup_latency_ns,
        .reopen_count = drain.reopen_count,
        .fresh_status_pages = fresh_status.build_pages,
        .fresh_status_pages_truncated = fresh_status.build_pages_truncated,
        .expected_score_records = expected_score_records,
        .fresh_storage_score_records = fresh_storage.score_records,
        .fresh_storage_metric_records = fresh_storage.metric_records,
        .fresh_storage_control_records = fresh_storage.control_records,
        .fresh_storage_job_namespace_records = fresh_storage.job_namespace_records,
        .active_published_read_ns = active_reads.published_ns,
        .active_fresh_fail_ns = active_reads.fresh_fail_ns,
        .active_rerank_published_ns = active_reads.rerank_published_ns,
        .active_rerank_fresh_fail_ns = active_reads.rerank_fresh_fail_ns,
        .active_traversal_published_ns = active_reads.traversal_published_ns,
        .active_traversal_fresh_fail_ns = active_reads.traversal_fresh_fail_ns,
        .active_traversal_published_checks = active_reads.traversal_published_checks,
        .active_traversal_fresh_rejections = active_reads.traversal_fresh_rejections,
        .active_profile_entries = active_reads.profile_entries,
        .active_mutation_writes = cfg.active_mutation_writes,
        .active_target_generation = active_reads.target_generation,
        .active_generation_delta = active_reads.generation_delta,
        .hits_active_paired_published_results = active_reads.hits_active_paired_published_results,
        .hits_active_paired_fresh_rejections = active_reads.hits_active_paired_fresh_rejections,
        .hits_active_paired_rerank_published_results = active_reads.hits_active_paired_rerank_published_results,
        .hits_active_paired_rerank_fresh_rejections = active_reads.hits_active_paired_rerank_fresh_rejections,
        .hits_active_paired_traversal_metric_results = active_reads.hits_active_paired_traversal_metric_results,
        .active_page_probe_claimed = active_reads.active_page_probe_claimed,
        .active_page_probe_reclaimed = active_reads.active_page_probe_reclaimed,
        .active_status_pages = active_reads.active_status_pages,
        .active_status_pages_truncated = active_reads.active_status_pages_truncated,
        .active_status_progress = active_reads.active_status_progress,
        .active_pending_work = active_reads.active_pending_work,
        .active_work_active_builds = active_reads.active_work_active_builds,
        .active_work_active_pages = active_reads.active_work_active_pages,
        .active_work_failed_pages = active_reads.active_work_failed_pages,
        .active_work_truncated_pages = active_reads.active_work_truncated_pages,
        .failed_published_read_ns = active_reads.failed_published_ns,
        .failed_fresh_fail_ns = active_reads.failed_fresh_fail_ns,
        .failed_rerank_published_ns = active_reads.failed_rerank_published_ns,
        .failed_rerank_fresh_fail_ns = active_reads.failed_rerank_fresh_fail_ns,
        .failed_traversal_published_ns = active_reads.failed_traversal_published_ns,
        .failed_traversal_fresh_fail_ns = active_reads.failed_traversal_fresh_fail_ns,
        .failed_traversal_published_checks = active_reads.failed_traversal_published_checks,
        .failed_traversal_fresh_rejections = active_reads.failed_traversal_fresh_rejections,
        .failed_profile_entries = active_reads.failed_profile_entries,
        .hits_failed_paired_published_results = active_reads.hits_failed_paired_published_results,
        .hits_failed_paired_fresh_rejections = active_reads.hits_failed_paired_fresh_rejections,
        .hits_failed_paired_rerank_published_results = active_reads.hits_failed_paired_rerank_published_results,
        .hits_failed_paired_rerank_fresh_rejections = active_reads.hits_failed_paired_rerank_fresh_rejections,
        .hits_failed_paired_traversal_metric_results = active_reads.hits_failed_paired_traversal_metric_results,
        .failure_repeats = active_reads.failure_repeats,
        .max_failure_diagnostics = active_reads.max_failure_diagnostics,
        .failure_retry_count = active_reads.failure_retry_count,
        .failure_recent_events = active_reads.failure_recent_events,
        .failure_recent_failures = active_reads.failure_recent_failures,
        .paired_failure_recent_events = active_reads.paired_failure_recent_events,
        .paired_failure_recent_failures = active_reads.paired_failure_recent_failures,
        .failed_status_pages = active_reads.failed_status_pages,
        .failed_status_pages_truncated = active_reads.failed_status_pages_truncated,
        .failed_storage_score_records = active_reads.failed_storage_score_records,
        .failed_storage_metric_records = active_reads.failed_storage_metric_records,
        .failed_storage_control_records = active_reads.failed_storage_control_records,
        .failed_storage_job_namespace_records = active_reads.failed_storage_job_namespace_records,
        .failed_storage_failure_records = active_reads.failed_storage_failure_records,
        .failed_storage_event_records = active_reads.failed_storage_event_records,
        .failed_pending_work = active_reads.failed_pending_work,
        .failed_active_builds = active_reads.failed_active_builds,
        .failed_active_pages = active_reads.failed_active_pages,
        .failed_failed_pages = active_reads.failed_failed_pages,
        .failed_truncated_pages = active_reads.failed_truncated_pages,
        .fan_in_shards = fan_in.shards,
        .fan_in_merged_scores = fan_in.merged_scores,
        .fan_in_active_published_scores = fan_in.active_published_scores,
        .fan_in_fresh_rejections = fan_in.fresh_rejections,
        .fan_in_generation_rejections = fan_in.generation_rejections,
        .fan_in_metadata_rejections = fan_in.metadata_rejections,
        .fan_in_edge_filter_rejections = fan_in.edge_filter_rejections,
        .fan_in_missing_rejections = fan_in.missing_rejections,
        .fan_in_merge_ns = fan_in.merge_ns,
        .fan_in_active_published_ns = fan_in.active_published_ns,
        .fan_in_fresh_fail_ns = fan_in.fresh_fail_ns,
        .fan_in_generation_fail_ns = fan_in.generation_fail_ns,
        .fan_in_metadata_fail_ns = fan_in.metadata_fail_ns,
        .fan_in_edge_filter_fail_ns = fan_in.edge_filter_fail_ns,
        .fan_in_missing_fail_ns = fan_in.missing_fail_ns,
        .fan_in_paired_metric_results = fan_in.paired_metric_results,
        .fan_in_paired_active_published_metric_results = fan_in.paired_active_published_metric_results,
        .fan_in_paired_fresh_rejections = fan_in.paired_fresh_rejections,
        .fan_in_paired_generation_rejections = fan_in.paired_generation_rejections,
        .fan_in_paired_metadata_rejections = fan_in.paired_metadata_rejections,
        .fan_in_paired_edge_filter_rejections = fan_in.paired_edge_filter_rejections,
        .fan_in_paired_missing_rejections = fan_in.paired_missing_rejections,
        .fan_in_paired_merge_ns = fan_in.paired_merge_ns,
        .fan_in_paired_active_published_ns = fan_in.paired_active_published_ns,
        .fan_in_paired_fresh_fail_ns = fan_in.paired_fresh_fail_ns,
        .fan_in_paired_generation_fail_ns = fan_in.paired_generation_fail_ns,
        .fan_in_paired_metadata_fail_ns = fan_in.paired_metadata_fail_ns,
        .fan_in_paired_edge_filter_fail_ns = fan_in.paired_edge_filter_fail_ns,
        .fan_in_paired_missing_fail_ns = fan_in.paired_missing_fail_ns,
    };
    try verifyMaintenanceModeEvidence(result);
    try verifyReopenEvidence(cfg, result);
    try verifyCleanupEvidence(result);
    try verifySchedulerEvidence(family, result);
    try verifyPrePublishReadEvidence(family, result);
    try verifyFanInEvidence(cfg, family, result);
    try verifyStatusAndWorkEvidence(family, result);
    try verifyFailureDiagnosticsEvidence(cfg, family, result);
    try verifyReleaseBudgets(cfg, result);
    return result;
}

fn verifyMaintenanceModeEvidence(result: FamilyResult) !void {
    switch (result.maintenance_mode) {
        .combined => {
            if (result.combined_sweeps == 0 or
                result.combined_sweeps != result.ticks or
                result.coordinator_sweeps != 0 or
                result.worker_pool_sweeps != 0 or
                result.split_worker_identities_with_progress != 0)
            {
                return error.GraphMetricReleaseQualificationMaintenanceModeMismatch;
            }
        },
        .split => {
            if (result.combined_sweeps != 0 or
                result.worker_pool_sweeps == 0 or
                result.coordinator_sweeps != result.worker_pool_sweeps * 2 or
                result.worker_pool_sweeps != result.scheduler.rounds_executed or
                result.split_worker_identities_with_progress == 0 or
                result.split_worker_identities_with_progress > result.worker_identities_configured)
            {
                return error.GraphMetricReleaseQualificationMaintenanceModeMismatch;
            }
            if (result.worker_identities_configured > 1 and
                result.scheduler.worker_steps > 1 and
                result.split_worker_identities_with_progress < 2)
            {
                return error.GraphMetricReleaseQualificationMaintenanceModeMismatch;
            }
        },
    }
}

fn verifyReopenEvidence(cfg: Config, result: FamilyResult) !void {
    if (cfg.reopen_between_ticks) {
        if (result.ticks == 0 or result.reopen_count + 1 != result.ticks) {
            return error.GraphMetricReleaseQualificationReopenEvidenceMismatch;
        }
    } else if (result.reopen_count != 0) {
        return error.GraphMetricReleaseQualificationReopenEvidenceMismatch;
    }
}

fn verifyCleanupEvidence(result: FamilyResult) !void {
    if ((result.cleanup_ticks == 0 and result.cleanup_latency_ns != 0) or
        (result.cleanup_ticks != 0 and result.cleanup_latency_ns == 0))
    {
        return error.GraphMetricReleaseQualificationCleanupEvidenceMismatch;
    }
}

fn verifySchedulerEvidence(family: Family, result: FamilyResult) !void {
    const expected_metrics_scanned: usize = switch (family) {
        .hits => 2,
        else => 1,
    };
    if (result.pre_drain_metrics_scanned != expected_metrics_scanned or result.pre_drain_queued_builds != 1) {
        return error.GraphMetricReleaseQualificationQueuedWorkMismatch;
    }
    if (result.scheduler.builds_started != 1 or result.scheduler.published != 1 or result.scheduler.failed_builds != 0) {
        return error.GraphMetricReleaseQualificationSchedulerTerminalMismatch;
    }
    if (result.scheduler.pages_claimed == 0 or
        result.scheduler.pages_completed == 0 or
        result.scheduler.pages_completed > result.scheduler.pages_claimed)
    {
        return error.GraphMetricReleaseQualificationSchedulerPageCountMismatch;
    }
    if (result.scheduler.pages_claimed > result.scheduler.worker_steps or
        result.scheduler.pages_completed > result.scheduler.worker_steps or
        result.scheduler.pages_completed < result.scheduler.phases_advanced)
    {
        return error.GraphMetricReleaseQualificationSchedulerPageCountMismatch;
    }
    if (result.scheduler.phases_advanced == 0 or
        result.scheduler.rounds_executed == 0 or
        result.scheduler.worker_steps == 0 or
        result.scheduler.coordinator_steps == 0)
    {
        return error.GraphMetricReleaseQualificationSchedulerProgressMissing;
    }
    const coordinator_decisions =
        result.scheduler.builds_started +
        result.scheduler.phases_advanced +
        result.scheduler.published +
        result.scheduler.failed_builds;
    if (coordinator_decisions > result.scheduler.coordinator_steps) {
        return error.GraphMetricReleaseQualificationSchedulerProgressMissing;
    }
}

fn verifyFanInEvidence(cfg: Config, family: Family, result: FamilyResult) !void {
    const expected_scores = @min(result.graph_nodes, cfg.top_k);
    if (result.fan_in_shards != cfg.synthetic_fan_in_shards or
        result.fan_in_merged_scores != expected_scores or
        result.fan_in_active_published_scores != expected_scores)
    {
        return error.GraphMetricReleaseQualificationFanInEvidenceMismatch;
    }
    if (result.fan_in_fresh_rejections != 1 or
        result.fan_in_generation_rejections != 1 or
        result.fan_in_metadata_rejections != 1 or
        result.fan_in_edge_filter_rejections != 1 or
        result.fan_in_missing_rejections != 1)
    {
        return error.GraphMetricReleaseQualificationFanInEvidenceMismatch;
    }
    if (family == .hits) {
        if (result.fan_in_paired_metric_results != 2 or
            result.fan_in_paired_active_published_metric_results != 2 or
            result.fan_in_paired_fresh_rejections != 1 or
            result.fan_in_paired_generation_rejections != 1 or
            result.fan_in_paired_metadata_rejections != 1 or
            result.fan_in_paired_edge_filter_rejections != 1 or
            result.fan_in_paired_missing_rejections != 1)
        {
            return error.GraphMetricReleaseQualificationFanInEvidenceMismatch;
        }
    } else if (result.fan_in_paired_metric_results != 0 or
        result.fan_in_paired_active_published_metric_results != 0 or
        result.fan_in_paired_fresh_rejections != 0 or
        result.fan_in_paired_generation_rejections != 0 or
        result.fan_in_paired_metadata_rejections != 0 or
        result.fan_in_paired_edge_filter_rejections != 0 or
        result.fan_in_paired_missing_rejections != 0)
    {
        return error.GraphMetricReleaseQualificationFanInEvidenceMismatch;
    }
}

fn verifyPrePublishReadEvidence(family: Family, result: FamilyResult) !void {
    if (result.pre_publish_not_ready_rejections != 2 or
        result.pre_publish_rerank_not_ready_rejections != 2 or
        result.pre_publish_traversal_projection_not_ready != 1 or
        result.pre_publish_traversal_not_ready_rejections != 3 or
        result.pre_publish_status_not_ready != 1)
    {
        return error.GraphMetricReleaseQualificationPrePublishNotReadyMismatch;
    }
    if (family == .hits) {
        if (result.hits_pre_publish_paired_not_ready_rejections != 2 or
            result.hits_pre_publish_paired_rerank_not_ready_rejections != 2 or
            result.hits_pre_publish_paired_traversal_projection_not_ready != 1 or
            result.hits_pre_publish_paired_traversal_not_ready_rejections != 3 or
            result.hits_pre_publish_paired_status_not_ready != 1)
        {
            return error.GraphMetricReleaseQualificationPrePublishNotReadyMismatch;
        }
    } else if (result.hits_pre_publish_paired_not_ready_rejections != 0 or
        result.hits_pre_publish_paired_rerank_not_ready_rejections != 0 or
        result.hits_pre_publish_paired_traversal_projection_not_ready != 0 or
        result.hits_pre_publish_paired_traversal_not_ready_rejections != 0 or
        result.hits_pre_publish_paired_status_not_ready != 0)
    {
        return error.GraphMetricReleaseQualificationPrePublishNotReadyMismatch;
    }
}

fn verifyStatusAndWorkEvidence(family: Family, result: FamilyResult) !void {
    if (result.fresh_pending_work != 0 or
        result.fresh_active_builds != 0 or
        result.fresh_active_pages != 0 or
        result.fresh_failed_pages != 0 or
        result.fresh_truncated_pages or
        result.fresh_status_pages != 0 or
        result.fresh_status_pages_truncated)
    {
        return error.GraphMetricReleaseQualificationTerminalWorkVisible;
    }

    if (!result.active_page_probe_claimed or
        !result.active_page_probe_reclaimed or
        result.active_status_pages == 0 or
        result.active_status_pages_truncated or
        !std.math.isFinite(result.active_status_progress) or
        result.active_status_progress < 0.0 or
        result.active_status_progress > 1.0)
    {
        return error.GraphMetricReleaseQualificationActiveStatusEvidenceMismatch;
    }
    if (result.active_work_active_builds != 1 or
        result.active_work_active_pages == 0 or
        result.active_work_failed_pages != 0 or
        result.active_work_truncated_pages)
    {
        return error.GraphMetricReleaseQualificationActiveWorkEvidenceMismatch;
    }
    if (result.active_pending_work != result.active_work_active_builds + result.active_work_active_pages + result.active_work_failed_pages) {
        return error.GraphMetricReleaseQualificationActiveWorkEvidenceMismatch;
    }

    if (result.failed_pending_work != 0 or
        result.failed_active_builds != 0 or
        result.failed_active_pages != 0 or
        result.failed_failed_pages != 0 or
        result.failed_truncated_pages or
        result.failed_status_pages != 0 or
        result.failed_status_pages_truncated)
    {
        return error.GraphMetricReleaseQualificationTerminalWorkVisible;
    }

    if (result.active_traversal_published_checks != 3 or
        result.active_traversal_fresh_rejections != 3 or
        result.failed_traversal_published_checks != 3 or
        result.failed_traversal_fresh_rejections != 3)
    {
        return error.GraphMetricReleaseQualificationTraversalReadEvidenceMismatch;
    }
    const expected_profile_entries: usize = if (family == .hits) 4 else 3;
    if (result.active_profile_entries != expected_profile_entries or
        result.failed_profile_entries != expected_profile_entries)
    {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }

    if (family == .hits) {
        if (result.hits_active_paired_published_results != 2 or
            result.hits_active_paired_fresh_rejections != 1 or
            result.hits_active_paired_rerank_published_results != 2 or
            result.hits_active_paired_rerank_fresh_rejections != 2 or
            result.hits_active_paired_traversal_metric_results != 2 or
            result.hits_failed_paired_published_results != 2 or
            result.hits_failed_paired_fresh_rejections != 1 or
            result.hits_failed_paired_rerank_published_results != 2 or
            result.hits_failed_paired_rerank_fresh_rejections != 2 or
            result.hits_failed_paired_traversal_metric_results != 2)
        {
            return error.GraphMetricReleaseQualificationPairedReadEvidenceMismatch;
        }
    } else if (result.hits_active_paired_published_results != 0 or
        result.hits_active_paired_fresh_rejections != 0 or
        result.hits_active_paired_rerank_published_results != 0 or
        result.hits_active_paired_rerank_fresh_rejections != 0 or
        result.hits_active_paired_traversal_metric_results != 0 or
        result.hits_failed_paired_published_results != 0 or
        result.hits_failed_paired_fresh_rejections != 0 or
        result.hits_failed_paired_rerank_published_results != 0 or
        result.hits_failed_paired_rerank_fresh_rejections != 0 or
        result.hits_failed_paired_traversal_metric_results != 0)
    {
        return error.GraphMetricReleaseQualificationPairedReadEvidenceMismatch;
    }
}

fn verifyFailureDiagnosticsEvidence(cfg: Config, family: Family, result: FamilyResult) !void {
    const expected_recent_events = @min(cfg.failure_repeats + 1, cfg.max_failure_diagnostics);
    const expected_recent_failures = @min(cfg.failure_repeats, cfg.max_failure_diagnostics);
    if (result.failure_repeats != cfg.failure_repeats or
        result.max_failure_diagnostics != cfg.max_failure_diagnostics or
        result.failure_retry_count != cfg.failure_repeats or
        result.failure_recent_events != expected_recent_events or
        result.failure_recent_failures != expected_recent_failures)
    {
        return error.GraphMetricReleaseQualificationFailureDiagnosticsMismatch;
    }
    if (family == .hits) {
        if (result.paired_failure_recent_events != expected_recent_events or
            result.paired_failure_recent_failures != expected_recent_failures)
        {
            return error.GraphMetricReleaseQualificationPairedFailureDiagnosticsMismatch;
        }
    } else if (result.paired_failure_recent_events != 0 or result.paired_failure_recent_failures != 0) {
        return error.GraphMetricReleaseQualificationPairedFailureDiagnosticsMismatch;
    }
}

fn verifyReleaseBudgets(cfg: Config, result: FamilyResult) !void {
    if (cfg.max_local_latency_ns != 0 and result.local_latency_ns > cfg.max_local_latency_ns) {
        return error.GraphMetricReleaseQualificationLocalLatencyBudgetExceeded;
    }
    if (cfg.max_planned_latency_ns != 0 and result.planned_latency_ns > cfg.max_planned_latency_ns) {
        return error.GraphMetricReleaseQualificationPlannedLatencyBudgetExceeded;
    }
    if (cfg.max_cleanup_latency_ns != 0 and result.cleanup_latency_ns > cfg.max_cleanup_latency_ns) {
        return error.GraphMetricReleaseQualificationCleanupLatencyBudgetExceeded;
    }
    if (cfg.max_published_read_latency_ns != 0 and
        (result.active_published_read_ns > cfg.max_published_read_latency_ns or
            result.failed_published_read_ns > cfg.max_published_read_latency_ns or
            result.active_rerank_published_ns > cfg.max_published_read_latency_ns or
            result.failed_rerank_published_ns > cfg.max_published_read_latency_ns or
            result.active_traversal_published_ns > cfg.max_published_read_latency_ns or
            result.failed_traversal_published_ns > cfg.max_published_read_latency_ns))
    {
        return error.GraphMetricReleaseQualificationPublishedReadLatencyBudgetExceeded;
    }
    if (cfg.max_fresh_fail_latency_ns != 0 and
        (result.active_fresh_fail_ns > cfg.max_fresh_fail_latency_ns or
            result.failed_fresh_fail_ns > cfg.max_fresh_fail_latency_ns or
            result.active_rerank_fresh_fail_ns > cfg.max_fresh_fail_latency_ns or
            result.failed_rerank_fresh_fail_ns > cfg.max_fresh_fail_latency_ns or
            result.active_traversal_fresh_fail_ns > cfg.max_fresh_fail_latency_ns or
            result.failed_traversal_fresh_fail_ns > cfg.max_fresh_fail_latency_ns))
    {
        return error.GraphMetricReleaseQualificationFreshFailLatencyBudgetExceeded;
    }
    if (cfg.max_fan_in_latency_ns != 0 and
        (result.fan_in_merge_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_active_published_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_fresh_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_generation_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_metadata_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_edge_filter_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_missing_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_merge_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_active_published_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_fresh_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_generation_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_metadata_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_edge_filter_fail_ns > cfg.max_fan_in_latency_ns or
            result.fan_in_paired_missing_fail_ns > cfg.max_fan_in_latency_ns))
    {
        return error.GraphMetricReleaseQualificationFanInLatencyBudgetExceeded;
    }
    if (cfg.max_storage_metric_records != 0 and
        (result.fresh_storage_metric_records > cfg.max_storage_metric_records or
            result.failed_storage_metric_records > cfg.max_storage_metric_records))
    {
        return error.GraphMetricReleaseQualificationStorageMetricBudgetExceeded;
    }
    if (cfg.max_storage_control_records != 0 and
        (result.fresh_storage_control_records > cfg.max_storage_control_records or
            result.failed_storage_control_records > cfg.max_storage_control_records))
    {
        return error.GraphMetricReleaseQualificationStorageControlBudgetExceeded;
    }
    if (cfg.max_page_claims != 0 and result.scheduler.pages_claimed > cfg.max_page_claims) {
        return error.GraphMetricReleaseQualificationPageClaimsBudgetExceeded;
    }
    if (cfg.max_cleanup_ticks != 0 and result.cleanup_ticks > cfg.max_cleanup_ticks) {
        return error.GraphMetricReleaseQualificationCleanupTicksBudgetExceeded;
    }
    if (cfg.max_rounds_executed != 0 and result.scheduler.rounds_executed > cfg.max_rounds_executed) {
        return error.GraphMetricReleaseQualificationRoundsExecutedBudgetExceeded;
    }
    if (cfg.max_failure_retry_count != 0 and result.failure_retry_count > cfg.max_failure_retry_count) {
        return error.GraphMetricReleaseQualificationFailureRetryBudgetExceeded;
    }
    if (cfg.max_worker_steps != 0 and result.scheduler.worker_steps > cfg.max_worker_steps) {
        return error.GraphMetricReleaseQualificationWorkerStepsBudgetExceeded;
    }
    if (cfg.max_coordinator_steps != 0 and result.scheduler.coordinator_steps > cfg.max_coordinator_steps) {
        return error.GraphMetricReleaseQualificationCoordinatorStepsBudgetExceeded;
    }
}

const StatusBoundResult = struct {
    build_pages: usize,
    build_pages_truncated: bool,
};

const ActiveStatusBoundResult = struct {
    build_pages: usize,
    build_pages_truncated: bool,
    progress: f64,
};

const PlannedWorkSnapshot = struct {
    metrics_scanned: usize,
    queued_builds: usize,
    active_builds: usize,
    active_pages: usize,
    failed_pages: usize,
    paused_metrics: usize,
    truncated_pages: bool,

    fn pendingWork(self: @This()) usize {
        return self.queued_builds + self.active_builds + self.active_pages + self.failed_pages;
    }
};

const PrePublishReadResult = struct {
    primary_not_ready_rejections: usize,
    primary_rerank_not_ready_rejections: usize,
    primary_traversal_projection_not_ready: usize,
    primary_traversal_not_ready_rejections: usize,
    primary_status_not_ready: usize,
    hits_paired_not_ready_rejections: usize = 0,
    hits_paired_rerank_not_ready_rejections: usize = 0,
    hits_paired_traversal_projection_not_ready: usize = 0,
    hits_paired_traversal_not_ready_rejections: usize = 0,
    hits_paired_status_not_ready: usize = 0,
};

const PrePublishTraversalProjectionResult = struct {
    primary_metric_statuses: usize,
    hits_paired_metric_statuses: usize = 0,
};

const StorageFootprintSummary = struct {
    score_records: usize,
    metric_records: usize,
    control_records: usize,
    job_namespace_records: usize,
    failure_records: usize,
    event_records: usize,
};

const GraphShapeSummary = struct {
    expected_nodes: usize,
    expected_edges: usize,
    actual_nodes: usize,
    actual_edges: usize,
    source_nodes: usize,
    sink_nodes: usize,
    authority_nodes: usize,
    sink_edges: usize,
    cycle_edges: usize,
    bipartite_edges: usize,
    authority_self_edges: usize,
    max_out_degree: usize,
};

const FanInResult = struct {
    shards: usize,
    merged_scores: usize,
    active_published_scores: usize,
    fresh_rejections: usize,
    generation_rejections: usize,
    metadata_rejections: usize,
    edge_filter_rejections: usize,
    missing_rejections: usize,
    merge_ns: u64,
    active_published_ns: u64,
    fresh_fail_ns: u64,
    generation_fail_ns: u64,
    metadata_fail_ns: u64,
    edge_filter_fail_ns: u64,
    missing_fail_ns: u64,
    paired_metric_results: usize = 0,
    paired_active_published_metric_results: usize = 0,
    paired_fresh_rejections: usize = 0,
    paired_generation_rejections: usize = 0,
    paired_metadata_rejections: usize = 0,
    paired_edge_filter_rejections: usize = 0,
    paired_missing_rejections: usize = 0,
    paired_merge_ns: u64 = 0,
    paired_active_published_ns: u64 = 0,
    paired_fresh_fail_ns: u64 = 0,
    paired_generation_fail_ns: u64 = 0,
    paired_metadata_fail_ns: u64 = 0,
    paired_edge_filter_fail_ns: u64 = 0,
    paired_missing_fail_ns: u64 = 0,
};

fn verifyPublishedStatusBounded(db: *db_mod.DB, family: Family, target_generation: u64) !StatusBoundResult {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var primary = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer primary.deinit(db.core.index_manager.alloc);
    if (primary.state != .fresh or primary.phase != .complete or primary.published_generation != target_generation) {
        return error.GraphMetricReleaseQualificationExpectedFreshStatus;
    }
    try verifyNoActiveBuildStatus(primary);

    var paired_pages: usize = 0;
    var paired_truncated = false;
    if (family == .hits) {
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(db.core.index_manager.alloc);
        if (hub.state != .fresh or hub.phase != .complete or hub.published_generation != target_generation) {
            return error.GraphMetricReleaseQualificationExpectedFreshStatus;
        }
        try verifyNoActiveBuildStatus(hub);
        paired_pages = hub.build_pages.len;
        paired_truncated = hub.build_pages_truncated;
    }

    return .{
        .build_pages = primary.build_pages.len + paired_pages,
        .build_pages_truncated = primary.build_pages_truncated or paired_truncated,
    };
}

fn verifyNoActiveBuildStatus(status: graph_mod.GraphIndex.GraphMetricStatus) !void {
    if (status.build_pages.len != 0 or status.build_pages_truncated) {
        return error.GraphMetricReleaseQualificationUnboundedStatusPayload;
    }
    if (status.build_job_id != 0 or status.building_generation != 0 or status.build_started_at_ms != 0) {
        return error.GraphMetricReleaseQualificationLeakedBuildStatus;
    }
    if (status.build_worker_id.len != 0 or status.build_cursor.len != 0) {
        return error.GraphMetricReleaseQualificationLeakedBuildStatus;
    }
}

fn verifyPrePublishNotReady(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    target_generation: u64,
) !PrePublishReadResult {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;

    var primary_not_ready_rejections: usize = 0;
    primary_not_ready_rejections += try expectGraphMetricNotReady(alloc, db, "pre_publish_primary_published", family.primaryMetric(), .published);
    primary_not_ready_rejections += try expectGraphMetricNotReady(alloc, db, "pre_publish_primary_fresh", family.primaryMetric(), .fresh);
    var primary_rerank_not_ready_rejections: usize = 0;
    primary_rerank_not_ready_rejections += try expectGraphMetricRerankNotReady(alloc, db, family.primaryMetric(), .published);
    primary_rerank_not_ready_rejections += try expectGraphMetricRerankNotReady(alloc, db, family.primaryMetric(), .fresh);
    const traversal_projection = try verifyPrePublishTraversalProjection(alloc, db, family, target_generation);
    const primary_traversal_not_ready_rejections = try countPrePublishTraversalNotReadyFailures(alloc, db, family, family.primaryMetric());
    var primary_status = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer primary_status.deinit(db.core.index_manager.alloc);
    const primary_status_not_ready = try expectPrePublishStatusNotReady(primary_status, target_generation);

    var hits_paired_not_ready_rejections: usize = 0;
    var hits_paired_rerank_not_ready_rejections: usize = 0;
    var hits_paired_traversal_not_ready_rejections: usize = 0;
    var hits_paired_status_not_ready: usize = 0;
    if (family == .hits) {
        hits_paired_not_ready_rejections += try expectGraphMetricNotReady(alloc, db, "pre_publish_hub_published", "hits_hub", .published);
        hits_paired_not_ready_rejections += try expectGraphMetricNotReady(alloc, db, "pre_publish_hub_fresh", "hits_hub", .fresh);
        hits_paired_rerank_not_ready_rejections += try expectGraphMetricRerankNotReady(alloc, db, "hits_hub", .published);
        hits_paired_rerank_not_ready_rejections += try expectGraphMetricRerankNotReady(alloc, db, "hits_hub", .fresh);
        hits_paired_traversal_not_ready_rejections = try countPrePublishTraversalNotReadyFailures(alloc, db, family, "hits_hub");
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(db.core.index_manager.alloc);
        hits_paired_status_not_ready = try expectPrePublishStatusNotReady(hub_status, target_generation);
    }

    return .{
        .primary_not_ready_rejections = primary_not_ready_rejections,
        .primary_rerank_not_ready_rejections = primary_rerank_not_ready_rejections,
        .primary_traversal_projection_not_ready = traversal_projection.primary_metric_statuses,
        .primary_traversal_not_ready_rejections = primary_traversal_not_ready_rejections,
        .primary_status_not_ready = primary_status_not_ready,
        .hits_paired_not_ready_rejections = hits_paired_not_ready_rejections,
        .hits_paired_rerank_not_ready_rejections = hits_paired_rerank_not_ready_rejections,
        .hits_paired_traversal_projection_not_ready = traversal_projection.hits_paired_metric_statuses,
        .hits_paired_traversal_not_ready_rejections = hits_paired_traversal_not_ready_rejections,
        .hits_paired_status_not_ready = hits_paired_status_not_ready,
    };
}

fn expectPrePublishStatusNotReady(status: graph_mod.GraphIndex.GraphMetricStatus, target_generation: u64) !usize {
    if (status.state != .not_ready or
        status.phase != .idle or
        !status.build_queued or
        status.published_generation != 0 or
        status.edge_generation != target_generation or
        status.target_edge_generation != target_generation or
        status.queued_generation != target_generation or
        status.building_generation != 0 or
        status.build_job_id != 0 or
        status.build_started_at_ms != 0 or
        status.build_pages.len != 0 or
        status.build_pages_truncated or
        status.build_worker_id.len != 0 or
        status.build_cursor.len != 0 or
        !std.math.isFinite(status.progress) or
        status.progress != 0.0)
    {
        return error.GraphMetricReleaseQualificationPrePublishStatusMismatch;
    }
    return 1;
}

fn expectGraphMetricNotReady(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    query_name: []const u8,
    metric_name: []const u8,
    freshness: types.GraphMetricFreshness,
) !usize {
    const result = db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = query_name,
            .query = .{
                .index_name = graph_index_name,
                .metric_name = metric_name,
                .top_k = 1,
                .freshness = freshness,
            },
        }},
        .limit = 0,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationPrePublishReadDidNotFail;
    } else |err| switch (err) {
        error.MetricNotReady => return 1,
        else => return err,
    }
}

fn expectGraphMetricRerankNotReady(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    metric_name: []const u8,
    freshness: types.GraphMetricFreshness,
) !usize {
    const result = db.search(alloc, .{
        .index_name = full_text_index_name,
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = graph_index_name,
            .metric_name = metric_name,
            .freshness = freshness,
            .weight = 1.0,
        },
        .limit = 2,
        .include_stored = false,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationPrePublishRerankDidNotFail;
    } else |err| switch (err) {
        error.MetricNotReady => return 1,
        else => return err,
    }
}

fn verifyPrePublishTraversalProjection(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    target_generation: u64,
) !PrePublishTraversalProjectionResult {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    if (family == .hits) {
        const metric_reads = [_]graph_query_mod.GraphMetricRead{
            .{ .name = family.primaryMetric(), .freshness = .published },
            .{ .name = "hits_hub", .freshness = .published },
        };
        const query = graph_query_mod.GraphQuery{
            .query_type = .neighbors,
            .index_name = graph_index_name,
            .start_nodes = .{ .keys = &start_nodes },
            .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
            .metrics = &metric_reads,
            .include_metric_status = true,
        };
        const expected_names = [_][]const u8{ family.primaryMetric(), "hits_hub" };
        const projected = try verifyPrePublishTraversalProjectionQuery(alloc, db, family, query, &expected_names, target_generation);
        if (projected != 2) return error.GraphMetricReleaseQualificationPrePublishTraversalMismatch;
        return .{
            .primary_metric_statuses = 1,
            .hits_paired_metric_statuses = 1,
        };
    }

    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .include_metric_status = true,
    };
    const expected_names = [_][]const u8{family.primaryMetric()};
    const projected = try verifyPrePublishTraversalProjectionQuery(alloc, db, family, query, &expected_names, target_generation);
    if (projected != 1) return error.GraphMetricReleaseQualificationPrePublishTraversalMismatch;
    return .{ .primary_metric_statuses = 1 };
}

fn verifyPrePublishTraversalProjectionQuery(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    query: graph_query_mod.GraphQuery,
    expected_metric_names: []const []const u8,
    target_generation: u64,
) !usize {
    var result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "pre_publish_traversal_projection", .query = query }},
        .limit = 0,
    });
    defer result.deinit();
    if (result.graph_results.len != 1) return error.GraphMetricReleaseQualificationMissingTraversalRead;
    const traversal = result.graph_results[0];
    if (traversal.nodes.len == 0) return error.GraphMetricReleaseQualificationMissingTraversalRead;
    if (!std.mem.eql(u8, traversal.nodes[0].key, traversalExpectedNode(family))) {
        return error.GraphMetricReleaseQualificationTraversalNodeMismatch;
    }
    if (traversal.nodes[0].metrics.len != expected_metric_names.len or traversal.metric_status.len != expected_metric_names.len) {
        return error.GraphMetricReleaseQualificationPrePublishTraversalMismatch;
    }
    for (expected_metric_names, 0..) |metric_name, i| {
        const metric = traversal.nodes[0].metrics[i];
        if (!std.mem.eql(u8, metric.name, metric_name) or metric.score != null) {
            return error.GraphMetricReleaseQualificationPrePublishTraversalMismatch;
        }
        const status = traversal.metric_status[i];
        if (!std.mem.eql(u8, status.name, metric_name) or
            status.state != .not_ready or
            status.phase != .idle or
            !status.build_queued or
            status.published_generation != 0 or
            status.edge_generation != target_generation or
            status.target_edge_generation != target_generation or
            status.queued_generation != target_generation or
            status.building_generation != 0 or
            status.build_job_id != 0 or
            status.build_started_at_ms != 0 or
            status.build_worker_id.len != 0 or
            !std.math.isFinite(status.progress) or
            status.progress != 0.0)
        {
            return error.GraphMetricReleaseQualificationPrePublishTraversalMismatch;
        }
    }
    return expected_metric_names.len;
}

fn countPrePublishTraversalNotReadyFailures(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    metric_name: []const u8,
) !usize {
    var rejections: usize = 0;
    rejections += try countPrePublishTraversalFreshProjectionFails(alloc, db, family, metric_name);
    rejections += try countPrePublishTraversalPublishedOrderFails(alloc, db, family, metric_name);
    rejections += try countPrePublishTraversalPublishedFilterFails(alloc, db, family, metric_name);
    return rejections;
}

fn countPrePublishTraversalFreshProjectionFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    metric_name: []const u8,
) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = metric_name,
        .freshness = .fresh,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .include_metric_status = true,
    };
    return try countPrePublishTraversalMetricNotReady(alloc, db, query);
}

fn countPrePublishTraversalPublishedOrderFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    metric_name: []const u8,
) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = metric_name,
        .freshness = .published,
    }};
    const metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = metric_name,
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .order_by = &metric_orders,
        .include_metric_status = true,
    };
    return try countPrePublishTraversalMetricNotReady(alloc, db, query);
}

fn countPrePublishTraversalPublishedFilterFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    metric_name: []const u8,
) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = metric_name,
        .freshness = .published,
    }};
    const metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = metric_name,
        .op = .gte,
        .value = 0.0,
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .where_metric = &metric_filters,
        .include_metric_status = true,
    };
    return try countPrePublishTraversalMetricNotReady(alloc, db, query);
}

fn countPrePublishTraversalMetricNotReady(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    query: graph_query_mod.GraphQuery,
) !usize {
    const result = db.search(alloc, .{
        .graph_queries = &.{.{ .name = "pre_publish_traversal_not_ready", .query = query }},
        .limit = 0,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationPrePublishTraversalDidNotFail;
    } else |err| switch (err) {
        error.MetricNotReady => return 1,
        else => return err,
    }
}

fn verifyGraphShape(db: *db_mod.DB, family: Family, cfg: Config, target_generation: u64) !GraphShapeSummary {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    const stats = try graph_entry.index.stats(db.core.index_manager.alloc);
    if (graph_entry.index.edge_generation != target_generation) {
        return error.GraphMetricReleaseQualificationGraphShapeMismatch;
    }

    const actual_nodes = std.math.cast(usize, stats.node_count) orelse {
        return error.GraphMetricReleaseQualificationGraphShapeMismatch;
    };
    const actual_edges = std.math.cast(usize, stats.edge_count) orelse {
        return error.GraphMetricReleaseQualificationGraphShapeMismatch;
    };

    const expected_nodes = initialNodeCount(cfg, family);
    const expected_edges = initialEdgeCount(cfg, family);
    if (actual_nodes != expected_nodes or actual_edges != expected_edges) {
        return error.GraphMetricReleaseQualificationGraphShapeMismatch;
    }

    return expectedGraphShape(family, cfg);
}

fn expectedGraphShape(family: Family, cfg: Config) GraphShapeSummary {
    const expected_nodes = initialNodeCount(cfg, family);
    const expected_edges = initialEdgeCount(cfg, family);
    return switch (family) {
        .hits => .{
            .expected_nodes = expected_nodes,
            .expected_edges = expected_edges,
            .actual_nodes = expected_nodes,
            .actual_edges = expected_edges,
            .source_nodes = cfg.docs,
            .sink_nodes = 0,
            .authority_nodes = cfg.fanout,
            .sink_edges = 0,
            .cycle_edges = 0,
            .bipartite_edges = cfg.docs * cfg.fanout,
            .authority_self_edges = cfg.fanout,
            .max_out_degree = cfg.fanout,
        },
        else => .{
            .expected_nodes = expected_nodes,
            .expected_edges = expected_edges,
            .actual_nodes = expected_nodes,
            .actual_edges = expected_edges,
            .source_nodes = cfg.docs,
            .sink_nodes = 1,
            .authority_nodes = 0,
            .sink_edges = cfg.docs,
            .cycle_edges = cfg.docs * (cfg.fanout - 1),
            .bipartite_edges = 0,
            .authority_self_edges = 0,
            .max_out_degree = cfg.fanout,
        },
    };
}

fn verifyFreshStorageFootprintBounded(
    db: *db_mod.DB,
    family: Family,
    expected_score_records: usize,
) !StorageFootprintSummary {
    const footprint = try familyStorageFootprint(db, family);
    if (footprint.score_records != expected_score_records) return error.GraphMetricReleaseQualificationMissingPublishedScores;
    if (footprint.job_namespace_records != 0) return error.GraphMetricReleaseQualificationLeakedBuildStorage;
    if (footprint.control_records != 1) return error.GraphMetricReleaseQualificationLeakedBuildStorage;
    return footprint;
}

fn verifySyntheticFanIn(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    cfg: Config,
    target_generation: u64,
) !FanInResult {
    const metric_name = family.primaryMetric();
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(metric_name);
    defer status.deinit(db.core.index_manager.alloc);
    if (status.published_generation != target_generation) return error.GraphMetricReleaseQualificationFanInGenerationMismatch;

    const raw_scores = try graph_entry.index.graphMetricTopK(metric_name, cfg.top_k);
    defer {
        for (raw_scores) |*score| score.deinit(db.core.index_manager.alloc);
        if (raw_scores.len > 0) db.core.index_manager.alloc.free(raw_scores);
    }
    if (raw_scores.len == 0) return error.GraphMetricReleaseQualificationFanInMissingScores;
    try verifySyntheticFanInShardLayout(raw_scores.len, cfg.synthetic_fan_in_shards);

    const shard_results = try alloc.alloc(types.SearchResult, cfg.synthetic_fan_in_shards);
    var initialized_shards: usize = 0;
    defer {
        for (shard_results[0..initialized_shards]) |*shard| shard.deinit();
        alloc.free(shard_results);
    }
    for (shard_results, 0..) |*shard, shard_index| {
        const range = shardScoreRange(raw_scores.len, shard_index, shard_results.len);
        shard.* = try syntheticFanInShardResult(
            alloc,
            metric_name,
            raw_scores[range.start..range.end],
            status,
            target_generation,
        );
        initialized_shards += 1;
    }

    const graph_metric_queries = [_]types.NamedGraphMetricQuery{.{
        .name = "release_fan_in",
        .query = .{
            .index_name = graph_index_name,
            .metric_name = metric_name,
            .top_k = @intCast(cfg.top_k),
            .freshness = .published,
        },
    }};
    const merge_started = platform_time.monotonicNs();
    var merged = try query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    );
    const merge_ns = platform_time.monotonicNs() - merge_started;
    defer merged.deinit();

    if (merged.graph_metric_results.len != 1) return error.GraphMetricReleaseQualificationFanInMissingResult;
    const merged_metric = merged.graph_metric_results[0];
    if (merged_metric.status.published_generation != target_generation) {
        return error.GraphMetricReleaseQualificationFanInGenerationMismatch;
    }
    const expected_scores = @min(raw_scores.len, cfg.top_k);
    if (merged_metric.scores.len != expected_scores) {
        return error.GraphMetricReleaseQualificationFanInScoreCountMismatch;
    }
    for (merged_metric.scores, raw_scores[0..expected_scores]) |actual, expected| {
        if (!std.mem.eql(u8, actual.node, expected.node)) return error.GraphMetricReleaseQualificationFanInScoreMismatch;
        if (@abs(actual.score - expected.score) > cfg.tolerance) return error.GraphMetricReleaseQualificationFanInScoreMismatch;
    }

    const active_shard = &shard_results[shard_results.len - 1];
    active_shard.graph_metric_results[0].status.state = .building;
    active_shard.graph_metric_results[0].status.phase = .prepare_generation;
    active_shard.graph_metric_results[0].status.edge_generation = target_generation + 1;
    active_shard.graph_metric_results[0].status.target_edge_generation = target_generation + 1;
    active_shard.graph_metric_results[0].status.building_generation = target_generation + 1;
    active_shard.graph_metric_results[0].status.build_job_id = 1;
    active_shard.graph_metric_results[0].status.progress = 0.01;
    const active_published_started = platform_time.monotonicNs();
    var active_published = try query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    );
    const active_published_ns = platform_time.monotonicNs() - active_published_started;
    defer active_published.deinit();
    if (active_published.graph_metric_results.len != 1) return error.GraphMetricReleaseQualificationFanInMissingResult;
    const active_published_metric = active_published.graph_metric_results[0];
    if (active_published_metric.status.published_generation != target_generation or active_published_metric.status.state != .building) {
        return error.GraphMetricReleaseQualificationFanInGenerationMismatch;
    }
    if (active_published_metric.scores.len != expected_scores) {
        return error.GraphMetricReleaseQualificationFanInScoreCountMismatch;
    }
    for (active_published_metric.scores, raw_scores[0..expected_scores]) |actual, expected| {
        if (!std.mem.eql(u8, actual.node, expected.node)) return error.GraphMetricReleaseQualificationFanInScoreMismatch;
        if (@abs(actual.score - expected.score) > cfg.tolerance) return error.GraphMetricReleaseQualificationFanInScoreMismatch;
    }

    const fresh_graph_metric_queries = [_]types.NamedGraphMetricQuery{.{
        .name = "release_fan_in",
        .query = .{
            .index_name = graph_index_name,
            .metric_name = metric_name,
            .top_k = @intCast(cfg.top_k),
            .freshness = .fresh,
        },
    }};
    var fresh_rejections: usize = 0;
    const fresh_fail_started = platform_time.monotonicNs();
    var fresh_fail_ns: u64 = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &fresh_graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        fresh_fail_ns = platform_time.monotonicNs() - fresh_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInFreshReadDidNotFail;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            fresh_fail_ns = platform_time.monotonicNs() - fresh_fail_started;
            fresh_rejections += 1;
        },
        else => return err,
    }

    active_shard.graph_metric_results[0].status.published_generation = target_generation + 1;
    active_shard.graph_metric_results[0].status.edge_generation = target_generation + 1;
    active_shard.graph_metric_results[0].status.target_edge_generation = target_generation + 1;
    var generation_rejections: usize = 0;
    const generation_fail_started = platform_time.monotonicNs();
    var generation_fail_ns: u64 = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        generation_fail_ns = platform_time.monotonicNs() - generation_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleGeneration;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            generation_fail_ns = platform_time.monotonicNs() - generation_fail_started;
            generation_rejections += 1;
        },
        else => return err,
    }

    active_shard.graph_metric_results[0].status.published_generation = target_generation;
    const first_metadata_status = &shard_results[0].graph_metric_results[0].status;
    const active_metadata_status = &active_shard.graph_metric_results[0].status;
    const original_first_metadata_version = first_metadata_status.metadata_version;
    const original_active_metadata_version = active_metadata_status.metadata_version;
    first_metadata_status.metadata_version = 1;
    active_metadata_status.metadata_version = 2;
    var metadata_rejections: usize = 0;
    const metadata_fail_started = platform_time.monotonicNs();
    var metadata_fail_ns: u64 = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        metadata_fail_ns = platform_time.monotonicNs() - metadata_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleMetadata;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            metadata_fail_ns = platform_time.monotonicNs() - metadata_fail_started;
            metadata_rejections += 1;
        },
        else => return err,
    }
    first_metadata_status.metadata_version = original_first_metadata_version;
    active_metadata_status.metadata_version = original_active_metadata_version;

    var edge_filter_rejections: usize = 0;
    var edge_filter_fail_ns: u64 = 0;
    {
        const edge_filter_status = &active_shard.graph_metric_results[0].status;
        const original_edge_filter = edge_filter_status.edge_filter;
        edge_filter_status.edge_filter = try syntheticMismatchedEdgeFilter(alloc);
        defer {
            edge_filter_status.edge_filter.deinit(alloc);
            edge_filter_status.edge_filter = original_edge_filter;
        }

        const edge_filter_fail_started = platform_time.monotonicNs();
        if (query_api.mergeSearchResults(
            alloc,
            .{ .graph_metric_queries = &graph_metric_queries },
            shard_results,
            0,
            @intCast(cfg.top_k),
        )) |bad_merge| {
            edge_filter_fail_ns = platform_time.monotonicNs() - edge_filter_fail_started;
            var mutable_bad_merge = bad_merge;
            mutable_bad_merge.deinit();
            return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleEdgeFilter;
        } else |err| switch (err) {
            error.UnsupportedQueryRequest => {
                edge_filter_fail_ns = platform_time.monotonicNs() - edge_filter_fail_started;
                edge_filter_rejections += 1;
            },
            else => return err,
        }
    }

    const missing_shard_results = try alloc.alloc(types.SearchResult, shard_results.len);
    defer alloc.free(missing_shard_results);
    @memcpy(missing_shard_results, shard_results);
    missing_shard_results[missing_shard_results.len - 1] = emptySyntheticFanInShard(alloc);
    var missing_rejections: usize = 0;
    const missing_fail_started = platform_time.monotonicNs();
    var missing_fail_ns: u64 = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        missing_shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        missing_fail_ns = platform_time.monotonicNs() - missing_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInMissingShardAccepted;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            missing_fail_ns = platform_time.monotonicNs() - missing_fail_started;
            missing_rejections += 1;
        },
        else => return err,
    }

    const paired = if (family == .hits)
        try verifySyntheticHitsPairFanIn(alloc, db, cfg, target_generation)
    else
        FanInResult{
            .shards = 0,
            .merged_scores = 0,
            .active_published_scores = 0,
            .fresh_rejections = 0,
            .generation_rejections = 0,
            .metadata_rejections = 0,
            .edge_filter_rejections = 0,
            .missing_rejections = 0,
            .merge_ns = 0,
            .active_published_ns = 0,
            .fresh_fail_ns = 0,
            .generation_fail_ns = 0,
            .metadata_fail_ns = 0,
            .edge_filter_fail_ns = 0,
            .missing_fail_ns = 0,
        };

    return .{
        .shards = shard_results.len,
        .merged_scores = merged_metric.scores.len,
        .active_published_scores = active_published_metric.scores.len,
        .fresh_rejections = fresh_rejections,
        .generation_rejections = generation_rejections,
        .metadata_rejections = metadata_rejections,
        .edge_filter_rejections = edge_filter_rejections,
        .missing_rejections = missing_rejections,
        .merge_ns = merge_ns,
        .active_published_ns = active_published_ns,
        .fresh_fail_ns = fresh_fail_ns,
        .generation_fail_ns = generation_fail_ns,
        .metadata_fail_ns = metadata_fail_ns,
        .edge_filter_fail_ns = edge_filter_fail_ns,
        .missing_fail_ns = missing_fail_ns,
        .paired_metric_results = paired.merged_scores,
        .paired_active_published_metric_results = paired.active_published_scores,
        .paired_fresh_rejections = paired.fresh_rejections,
        .paired_generation_rejections = paired.generation_rejections,
        .paired_metadata_rejections = paired.metadata_rejections,
        .paired_edge_filter_rejections = paired.edge_filter_rejections,
        .paired_missing_rejections = paired.missing_rejections,
        .paired_merge_ns = paired.merge_ns,
        .paired_active_published_ns = paired.active_published_ns,
        .paired_fresh_fail_ns = paired.fresh_fail_ns,
        .paired_generation_fail_ns = paired.generation_fail_ns,
        .paired_metadata_fail_ns = paired.metadata_fail_ns,
        .paired_edge_filter_fail_ns = paired.edge_filter_fail_ns,
        .paired_missing_fail_ns = paired.missing_fail_ns,
    };
}

fn verifySyntheticHitsPairFanIn(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    target_generation: u64,
) !FanInResult {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var authority_status = try graph_entry.index.graphMetricStatus("hits_authority");
    defer authority_status.deinit(db.core.index_manager.alloc);
    var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
    defer hub_status.deinit(db.core.index_manager.alloc);
    if (authority_status.published_generation != target_generation or hub_status.published_generation != target_generation) {
        return error.GraphMetricReleaseQualificationFanInGenerationMismatch;
    }

    const authority_scores = try graph_entry.index.graphMetricTopK("hits_authority", cfg.top_k);
    defer {
        for (authority_scores) |*score| score.deinit(db.core.index_manager.alloc);
        if (authority_scores.len > 0) db.core.index_manager.alloc.free(authority_scores);
    }
    const hub_scores = try graph_entry.index.graphMetricTopK("hits_hub", cfg.top_k);
    defer {
        for (hub_scores) |*score| score.deinit(db.core.index_manager.alloc);
        if (hub_scores.len > 0) db.core.index_manager.alloc.free(hub_scores);
    }
    if (authority_scores.len == 0 or hub_scores.len == 0) return error.GraphMetricReleaseQualificationFanInMissingScores;
    try verifySyntheticFanInShardLayout(authority_scores.len, cfg.synthetic_fan_in_shards);
    try verifySyntheticFanInShardLayout(hub_scores.len, cfg.synthetic_fan_in_shards);

    const shard_results = try alloc.alloc(types.SearchResult, cfg.synthetic_fan_in_shards);
    var initialized_shards: usize = 0;
    defer {
        for (shard_results[0..initialized_shards]) |*shard| shard.deinit();
        alloc.free(shard_results);
    }
    for (shard_results, 0..) |*shard, shard_index| {
        const authority_range = shardScoreRange(authority_scores.len, shard_index, shard_results.len);
        const hub_range = shardScoreRange(hub_scores.len, shard_index, shard_results.len);
        shard.* = try syntheticHitsPairFanInShardResult(
            alloc,
            authority_scores[authority_range.start..authority_range.end],
            authority_status,
            hub_scores[hub_range.start..hub_range.end],
            hub_status,
            target_generation,
        );
        initialized_shards += 1;
    }

    const graph_metric_queries = [_]types.NamedGraphMetricQuery{
        .{
            .name = "release_hits_authority",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = "hits_authority",
                .top_k = @intCast(cfg.top_k),
                .freshness = .published,
            },
        },
        .{
            .name = "release_hits_hub",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = "hits_hub",
                .top_k = @intCast(cfg.top_k),
                .freshness = .published,
            },
        },
    };
    const merge_started = platform_time.monotonicNs();
    var merged = try query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    );
    const merge_ns = platform_time.monotonicNs() - merge_started;
    defer merged.deinit();
    try verifyHitsPairFanInMerged(merged, authority_scores, hub_scores, cfg, target_generation, .fresh);

    const active_shard = &shard_results[shard_results.len - 1];
    for (active_shard.graph_metric_results) |*metric_result| {
        metric_result.status.state = .building;
        metric_result.status.phase = .prepare_generation;
        metric_result.status.edge_generation = target_generation + 1;
        metric_result.status.target_edge_generation = target_generation + 1;
        metric_result.status.building_generation = target_generation + 1;
        metric_result.status.build_job_id = 1;
        metric_result.status.progress = 0.01;
    }
    const active_published_started = platform_time.monotonicNs();
    var active_published = try query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    );
    const active_published_ns = platform_time.monotonicNs() - active_published_started;
    defer active_published.deinit();
    try verifyHitsPairFanInMerged(active_published, authority_scores, hub_scores, cfg, target_generation, .building);

    const fresh_graph_metric_queries = [_]types.NamedGraphMetricQuery{
        .{
            .name = "release_hits_authority",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = "hits_authority",
                .top_k = @intCast(cfg.top_k),
                .freshness = .fresh,
            },
        },
        .{
            .name = "release_hits_hub",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = "hits_hub",
                .top_k = @intCast(cfg.top_k),
                .freshness = .fresh,
            },
        },
    };
    const fresh_fail_started = platform_time.monotonicNs();
    var fresh_fail_ns: u64 = 0;
    var fresh_rejections: usize = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &fresh_graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        fresh_fail_ns = platform_time.monotonicNs() - fresh_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInFreshReadDidNotFail;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            fresh_fail_ns = platform_time.monotonicNs() - fresh_fail_started;
            fresh_rejections += 1;
        },
        else => return err,
    }

    active_shard.graph_metric_results[1].status.published_generation = target_generation + 1;
    active_shard.graph_metric_results[1].status.edge_generation = target_generation + 1;
    active_shard.graph_metric_results[1].status.target_edge_generation = target_generation + 1;
    const generation_fail_started = platform_time.monotonicNs();
    var generation_fail_ns: u64 = 0;
    var generation_rejections: usize = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        generation_fail_ns = platform_time.monotonicNs() - generation_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleGeneration;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            generation_fail_ns = platform_time.monotonicNs() - generation_fail_started;
            generation_rejections += 1;
        },
        else => return err,
    }

    active_shard.graph_metric_results[1].status.published_generation = target_generation;
    const authority_metadata_status = &active_shard.graph_metric_results[0].status;
    const hub_metadata_status = &active_shard.graph_metric_results[1].status;
    const original_authority_metadata_version = authority_metadata_status.metadata_version;
    const original_hub_metadata_version = hub_metadata_status.metadata_version;
    authority_metadata_status.metadata_version = 1;
    hub_metadata_status.metadata_version = 2;
    const metadata_fail_started = platform_time.monotonicNs();
    var metadata_fail_ns: u64 = 0;
    var metadata_rejections: usize = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        metadata_fail_ns = platform_time.monotonicNs() - metadata_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleMetadata;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            metadata_fail_ns = platform_time.monotonicNs() - metadata_fail_started;
            metadata_rejections += 1;
        },
        else => return err,
    }
    authority_metadata_status.metadata_version = original_authority_metadata_version;
    hub_metadata_status.metadata_version = original_hub_metadata_version;

    var edge_filter_rejections: usize = 0;
    var edge_filter_fail_ns: u64 = 0;
    {
        const hub_edge_filter_status = &active_shard.graph_metric_results[1].status;
        const original_edge_filter = hub_edge_filter_status.edge_filter;
        hub_edge_filter_status.edge_filter = try syntheticMismatchedEdgeFilter(alloc);
        defer {
            hub_edge_filter_status.edge_filter.deinit(alloc);
            hub_edge_filter_status.edge_filter = original_edge_filter;
        }

        const edge_filter_fail_started = platform_time.monotonicNs();
        if (query_api.mergeSearchResults(
            alloc,
            .{ .graph_metric_queries = &graph_metric_queries },
            shard_results,
            0,
            @intCast(cfg.top_k),
        )) |bad_merge| {
            edge_filter_fail_ns = platform_time.monotonicNs() - edge_filter_fail_started;
            var mutable_bad_merge = bad_merge;
            mutable_bad_merge.deinit();
            return error.GraphMetricReleaseQualificationFanInAcceptedIncompatibleEdgeFilter;
        } else |err| switch (err) {
            error.UnsupportedQueryRequest => {
                edge_filter_fail_ns = platform_time.monotonicNs() - edge_filter_fail_started;
                edge_filter_rejections += 1;
            },
            else => return err,
        }
    }

    const missing_shard_results = try alloc.alloc(types.SearchResult, shard_results.len);
    defer alloc.free(missing_shard_results);
    @memcpy(missing_shard_results, shard_results);
    missing_shard_results[missing_shard_results.len - 1] = emptySyntheticFanInShard(alloc);
    const missing_fail_started = platform_time.monotonicNs();
    var missing_fail_ns: u64 = 0;
    var missing_rejections: usize = 0;
    if (query_api.mergeSearchResults(
        alloc,
        .{ .graph_metric_queries = &graph_metric_queries },
        missing_shard_results,
        0,
        @intCast(cfg.top_k),
    )) |bad_merge| {
        missing_fail_ns = platform_time.monotonicNs() - missing_fail_started;
        var mutable_bad_merge = bad_merge;
        mutable_bad_merge.deinit();
        return error.GraphMetricReleaseQualificationFanInMissingShardAccepted;
    } else |err| switch (err) {
        error.UnsupportedQueryRequest => {
            missing_fail_ns = platform_time.monotonicNs() - missing_fail_started;
            missing_rejections += 1;
        },
        else => return err,
    }

    return .{
        .shards = shard_results.len,
        .merged_scores = merged.graph_metric_results.len,
        .active_published_scores = active_published.graph_metric_results.len,
        .fresh_rejections = fresh_rejections,
        .generation_rejections = generation_rejections,
        .metadata_rejections = metadata_rejections,
        .edge_filter_rejections = edge_filter_rejections,
        .missing_rejections = missing_rejections,
        .merge_ns = merge_ns,
        .active_published_ns = active_published_ns,
        .fresh_fail_ns = fresh_fail_ns,
        .generation_fail_ns = generation_fail_ns,
        .metadata_fail_ns = metadata_fail_ns,
        .edge_filter_fail_ns = edge_filter_fail_ns,
        .missing_fail_ns = missing_fail_ns,
    };
}

fn verifyHitsPairFanInMerged(
    result: types.SearchResult,
    authority_scores: []const graph_mod.GraphIndex.GraphMetricScore,
    hub_scores: []const graph_mod.GraphIndex.GraphMetricScore,
    cfg: Config,
    target_generation: u64,
    expected_state: graph_mod.GraphIndex.GraphMetricState,
) !void {
    if (result.graph_metric_results.len != 2) return error.GraphMetricReleaseQualificationFanInMissingResult;
    const authority = graphMetricResultByName(result.graph_metric_results, "release_hits_authority") orelse {
        return error.GraphMetricReleaseQualificationFanInMissingResult;
    };
    const hub = graphMetricResultByName(result.graph_metric_results, "release_hits_hub") orelse {
        return error.GraphMetricReleaseQualificationFanInMissingResult;
    };
    try verifyMergedFanInMetric(
        authority,
        "release_hits_authority",
        "hits_authority",
        authority_scores,
        cfg,
        target_generation,
        expected_state,
    );
    try verifyMergedFanInMetric(
        hub,
        "release_hits_hub",
        "hits_hub",
        hub_scores,
        cfg,
        target_generation,
        expected_state,
    );
}

fn graphMetricResultByName(
    results: []const types.GraphMetricResult,
    name: []const u8,
) ?types.GraphMetricResult {
    for (results) |result| {
        if (std.mem.eql(u8, result.name, name)) return result;
    }
    return null;
}

fn verifyMergedFanInMetric(
    actual: types.GraphMetricResult,
    query_name: []const u8,
    metric_name: []const u8,
    expected_scores: []const graph_mod.GraphIndex.GraphMetricScore,
    cfg: Config,
    target_generation: u64,
    expected_state: graph_mod.GraphIndex.GraphMetricState,
) !void {
    if (!std.mem.eql(u8, actual.name, query_name)) return error.GraphMetricReleaseQualificationFanInMissingResult;
    if (!std.mem.eql(u8, actual.index_name, graph_index_name)) return error.GraphMetricReleaseQualificationFanInMissingResult;
    if (!std.mem.eql(u8, actual.metric_name, metric_name)) return error.GraphMetricReleaseQualificationFanInMissingResult;
    if (!std.mem.eql(u8, actual.status.name, metric_name)) return error.GraphMetricReleaseQualificationFanInMissingResult;
    if (actual.status.published_generation != target_generation or actual.status.state != expected_state) {
        return error.GraphMetricReleaseQualificationFanInGenerationMismatch;
    }
    const expected_count = @min(expected_scores.len, cfg.top_k);
    if (actual.scores.len != expected_count) return error.GraphMetricReleaseQualificationFanInScoreCountMismatch;
    for (actual.scores, expected_scores[0..expected_count]) |actual_score, expected_score| {
        if (!std.mem.eql(u8, actual_score.node, expected_score.node)) return error.GraphMetricReleaseQualificationFanInScoreMismatch;
        if (@abs(actual_score.score - expected_score.score) > cfg.tolerance) {
            return error.GraphMetricReleaseQualificationFanInScoreMismatch;
        }
    }
}

const ShardScoreRange = struct {
    start: usize,
    end: usize,
};

fn shardScoreRange(total: usize, shard_index: usize, shard_count: usize) ShardScoreRange {
    return .{
        .start = total * shard_index / shard_count,
        .end = total * (shard_index + 1) / shard_count,
    };
}

fn verifySyntheticFanInShardLayout(total_scores: usize, shard_count: usize) !void {
    if (total_scores == 0 or shard_count < 2) return error.GraphMetricReleaseQualificationFanInShardLayoutMismatch;

    var covered: usize = 0;
    var min_count: usize = std.math.maxInt(usize);
    var max_count: usize = 0;
    for (0..shard_count) |shard_index| {
        const range = shardScoreRange(total_scores, shard_index, shard_count);
        if (range.start != covered or range.end < range.start or range.end > total_scores) {
            return error.GraphMetricReleaseQualificationFanInShardLayoutMismatch;
        }
        const count = range.end - range.start;
        min_count = @min(min_count, count);
        max_count = @max(max_count, count);
        covered = range.end;
    }
    if (covered != total_scores or max_count > min_count + 1) {
        return error.GraphMetricReleaseQualificationFanInShardLayoutMismatch;
    }
}

fn syntheticFanInShardResult(
    alloc: std.mem.Allocator,
    metric_name: []const u8,
    scores: []const graph_mod.GraphIndex.GraphMetricScore,
    status: graph_mod.GraphIndex.GraphMetricStatus,
    published_generation: u64,
) !types.SearchResult {
    const metric_results = try alloc.alloc(types.GraphMetricResult, 1);
    var initialized: usize = 0;
    errdefer {
        for (metric_results[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(metric_results);
    }
    metric_results[0] = try syntheticFanInMetricResult(
        alloc,
        "release_fan_in",
        metric_name,
        scores,
        status,
        published_generation,
    );
    initialized += 1;

    return .{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = metric_results,
    };
}

fn emptySyntheticFanInShard(alloc: std.mem.Allocator) types.SearchResult {
    return .{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = &.{},
    };
}

fn syntheticMismatchedEdgeFilter(alloc: std.mem.Allocator) !graph_mod.GraphMetricEdgeFilter {
    const edge_types = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(edge_types);
    edge_types[0] = try alloc.dupe(u8, "__release_fan_in_probe__");
    return .{ .mode = .types, .types = edge_types };
}

fn syntheticHitsPairFanInShardResult(
    alloc: std.mem.Allocator,
    authority_scores: []const graph_mod.GraphIndex.GraphMetricScore,
    authority_status: graph_mod.GraphIndex.GraphMetricStatus,
    hub_scores: []const graph_mod.GraphIndex.GraphMetricScore,
    hub_status: graph_mod.GraphIndex.GraphMetricStatus,
    published_generation: u64,
) !types.SearchResult {
    const metric_results = try alloc.alloc(types.GraphMetricResult, 2);
    var initialized: usize = 0;
    errdefer {
        for (metric_results[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(metric_results);
    }
    metric_results[0] = try syntheticFanInMetricResult(
        alloc,
        "release_hits_authority",
        "hits_authority",
        authority_scores,
        authority_status,
        published_generation,
    );
    initialized += 1;
    metric_results[1] = try syntheticFanInMetricResult(
        alloc,
        "release_hits_hub",
        "hits_hub",
        hub_scores,
        hub_status,
        published_generation,
    );
    initialized += 1;

    return .{
        .alloc = alloc,
        .hits = &.{},
        .total_hits = 0,
        .graph_metric_results = metric_results,
    };
}

fn syntheticFanInMetricResult(
    alloc: std.mem.Allocator,
    query_name: []const u8,
    metric_name: []const u8,
    scores: []const graph_mod.GraphIndex.GraphMetricScore,
    status: graph_mod.GraphIndex.GraphMetricStatus,
    published_generation: u64,
) !types.GraphMetricResult {
    const name = try alloc.dupe(u8, query_name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, graph_index_name);
    errdefer alloc.free(index_name);
    const metric_name_owned = try alloc.dupe(u8, metric_name);
    errdefer alloc.free(metric_name_owned);

    const out_scores = try alloc.alloc(types.GraphMetricScore, scores.len);
    var initialized_scores: usize = 0;
    errdefer {
        for (out_scores[0..initialized_scores]) |*score| score.deinit(alloc);
        if (out_scores.len > 0) alloc.free(out_scores);
    }
    for (scores, 0..) |score, i| {
        out_scores[i] = .{
            .node = try alloc.dupe(u8, score.node),
            .score = score.score,
        };
        initialized_scores += 1;
    }

    const result_status = try syntheticFanInStatus(alloc, status, published_generation);
    errdefer {
        var mutable_status = result_status;
        mutable_status.deinit(alloc);
    }

    return .{
        .name = name,
        .index_name = index_name,
        .metric_name = metric_name_owned,
        .scores = out_scores,
        .status = result_status,
    };
}

fn syntheticFanInStatus(
    alloc: std.mem.Allocator,
    status: graph_mod.GraphIndex.GraphMetricStatus,
    published_generation: u64,
) !types.GraphMetricStatus {
    return .{
        .name = try alloc.dupe(u8, status.name),
        .state = status.state,
        .phase = status.phase,
        .edge_filter = try status.edge_filter.cloneAlloc(alloc),
        .metadata_version = status.metadata_version,
        .maintenance_paused = status.maintenance_paused,
        .build_queued = status.build_queued,
        .published_generation = published_generation,
        .edge_generation = published_generation,
        .target_edge_generation = published_generation,
        .queued_generation = 0,
        .building_generation = 0,
        .build_job_id = 0,
        .build_started_at_ms = 0,
        .build_iteration = status.build_iteration,
        .build_lease_expires_at_ms = 0,
        .build_worker_id = "",
        .build_cursor = "",
        .build_completed_units = 0,
        .build_total_units = 0,
        .retry_count = status.retry_count,
        .last_error = "",
        .progress = status.progress,
        .converged = status.converged,
        .iterations_completed = status.iterations_completed,
        .delta = status.delta,
        .computed_at_ms = status.computed_at_ms,
        .last_event = status.last_event,
        .recent_events = &.{},
    };
}

fn familyStorageFootprint(db: *db_mod.DB, family: Family) !StorageFootprintSummary {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var primary = try graph_entry.index.graphMetricStorageFootprint(family.primaryMetric());
    if (family == .hits) {
        const hub = try graph_entry.index.graphMetricStorageFootprint("hits_hub");
        primary.score_records += hub.score_records;
        primary.metric_records += hub.metric_records;
        primary.control_records += hub.control_records;
        primary.job_namespace_records += hub.job_namespace_records;
        primary.failure_records += hub.failure_records;
        primary.event_records += hub.event_records;
    }
    return .{
        .score_records = primary.score_records,
        .metric_records = primary.metric_records,
        .control_records = primary.control_records,
        .job_namespace_records = primary.job_namespace_records,
        .failure_records = primary.failure_records,
        .event_records = primary.event_records,
    };
}

fn verifyActiveStatusBounded(
    db: *db_mod.DB,
    family: Family,
    previous_generation: u64,
    target_generation: u64,
    max_status_pages: usize,
    require_probe_page: bool,
) !ActiveStatusBoundResult {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer status.deinit(db.core.index_manager.alloc);
    if (status.state != .building) return error.GraphMetricReleaseQualificationExpectedActiveBuild;
    if (status.published_generation != previous_generation or status.building_generation != target_generation) {
        return error.GraphMetricReleaseQualificationPublishedGenerationChanged;
    }
    if (status.build_job_id == 0 or status.build_started_at_ms == 0) {
        return error.GraphMetricReleaseQualificationMissingBuildStatus;
    }
    if (status.build_pages.len > max_status_pages) {
        return error.GraphMetricReleaseQualificationUnboundedStatusPayload;
    }
    if (require_probe_page) {
        try verifyActiveProbePageStatus(status);
    }
    if (!std.math.isFinite(status.progress) or status.progress < 0.0 or status.progress > 1.0) {
        return error.GraphMetricReleaseQualificationInvalidStatusProgress;
    }
    return .{
        .build_pages = status.build_pages.len,
        .build_pages_truncated = status.build_pages_truncated,
        .progress = status.progress,
    };
}

const ActivePageProbeResult = struct {
    claimed: bool,
    reclaimed: bool,
};

fn claimActiveStatusProbePage(db: *db_mod.DB, family: Family) !ActivePageProbeResult {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer status.deinit(db.core.index_manager.alloc);
    if (status.state != .building or status.build_job_id == 0) {
        return error.GraphMetricReleaseQualificationExpectedActiveBuild;
    }
    const stale_claim = try graph_entry.index.claimNextGraphMetricBuildPageAt(
        family.primaryMetric(),
        status.build_job_id,
        status.phase,
        status.build_iteration,
        stale_probe_worker_id,
        status.build_started_at_ms + 1,
    ) orelse return error.GraphMetricReleaseQualificationMissingBuildStatus;
    if (!std.mem.eql(u8, stale_claim.worker_id, stale_probe_worker_id) or
        stale_claim.attempt == 0 or
        stale_claim.lease_expires_at_ms == 0)
    {
        return error.GraphMetricReleaseQualificationMissingBuildStatus;
    }
    if (try graph_entry.index.claimNextGraphMetricBuildPageAt(
        family.primaryMetric(),
        status.build_job_id,
        status.phase,
        status.build_iteration,
        early_probe_worker_id,
        stale_claim.lease_expires_at_ms - 1,
    )) |_| {
        return error.GraphMetricReleaseQualificationUnexpectedPageReclaim;
    }
    const reclaimed = try graph_entry.index.claimNextGraphMetricBuildPageAt(
        family.primaryMetric(),
        status.build_job_id,
        status.phase,
        status.build_iteration,
        active_probe_worker_id,
        stale_claim.lease_expires_at_ms + 1,
    ) orelse return error.GraphMetricReleaseQualificationMissingBuildStatus;
    if (!std.mem.eql(u8, reclaimed.worker_id, active_probe_worker_id) or
        reclaimed.page_id != stale_claim.page_id or
        reclaimed.attempt <= stale_claim.attempt or
        reclaimed.lease_expires_at_ms <= stale_claim.lease_expires_at_ms)
    {
        return error.GraphMetricReleaseQualificationMissingBuildStatus;
    }
    return .{
        .claimed = true,
        .reclaimed = true,
    };
}

fn verifyActiveProbePageStatus(status: graph_mod.GraphIndex.GraphMetricStatus) !void {
    var found = false;
    for (status.build_pages) |page| {
        if (!std.mem.eql(u8, page.worker_id, active_probe_worker_id)) continue;
        if (page.state != .leased or
            page.attempt == 0 or
            page.lease_expires_at_ms == 0 or
            page.total_units == 0 or
            page.last_error.len != 0)
        {
            return error.GraphMetricReleaseQualificationMissingBuildStatus;
        }
        found = true;
    }
    if (!found) return error.GraphMetricReleaseQualificationMissingBuildStatus;
}

fn plannedWorkSnapshot(db: *db_mod.DB) PlannedWorkSnapshot {
    const stats = db.pendingWorkStats().graph_metric;
    return .{
        .metrics_scanned = stats.metrics_scanned,
        .queued_builds = stats.queued_builds,
        .active_builds = stats.active_builds,
        .active_pages = stats.active_pages,
        .failed_pages = stats.failed_pages,
        .paused_metrics = stats.paused_metrics,
        .truncated_pages = stats.truncated_pages,
    };
}

fn verifyQueuedPlannedWork(db: *db_mod.DB, family: Family) !PlannedWorkSnapshot {
    const snapshot = plannedWorkSnapshot(db);
    const expected_metrics_scanned: usize = if (family == .hits) 2 else 1;
    if (snapshot.metrics_scanned != expected_metrics_scanned or
        snapshot.queued_builds != 1 or
        snapshot.active_builds != 0 or
        snapshot.active_pages != 0 or
        snapshot.failed_pages != 0 or
        snapshot.paused_metrics != 0 or
        snapshot.truncated_pages)
    {
        return error.GraphMetricReleaseQualificationUnexpectedPlannedWorkStats;
    }
    return snapshot;
}

fn verifyNoPendingPlannedWork(db: *db_mod.DB) !PlannedWorkSnapshot {
    const snapshot = plannedWorkSnapshot(db);
    if (snapshot.queued_builds != 0 or
        snapshot.active_builds != 0 or
        snapshot.active_pages != 0 or
        snapshot.failed_pages != 0 or
        snapshot.paused_metrics != 0 or
        snapshot.truncated_pages)
    {
        return error.GraphMetricReleaseQualificationUnexpectedPlannedWorkStats;
    }
    return snapshot;
}

fn verifyActivePlannedWork(db: *db_mod.DB, family: Family) !PlannedWorkSnapshot {
    const snapshot = plannedWorkSnapshot(db);
    const expected_metrics_scanned: usize = if (family == .hits) 2 else 1;
    if (snapshot.metrics_scanned != expected_metrics_scanned or
        snapshot.queued_builds != 0 or
        snapshot.active_builds != 1 or
        snapshot.active_pages == 0 or
        snapshot.failed_pages != 0 or
        snapshot.paused_metrics != 0 or
        snapshot.truncated_pages)
    {
        return error.GraphMetricReleaseQualificationUnexpectedPlannedWorkStats;
    }
    return snapshot;
}

const DrainResult = struct {
    ticks: usize,
    total: db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
    combined_sweeps: usize = 0,
    coordinator_sweeps: usize = 0,
    worker_pool_sweeps: usize = 0,
    split_worker_identities_with_progress: usize = 0,
    cleanup_ticks: usize = 0,
    cleanup_latency_ns: u64 = 0,
    reopen_count: usize = 0,
};

fn drainPlanned(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    db_path: []const u8,
    cfg: Config,
    family: Family,
    target_generation: u64,
) !DrainResult {
    const worker_ids = try makeWorkerIds(alloc, cfg.workers);
    defer freeWorkerIds(alloc, worker_ids);
    const worker_step_counts = try alloc.alloc(usize, worker_ids.len);
    defer alloc.free(worker_step_counts);
    @memset(worker_step_counts, 0);

    var total = db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var cleanup_started = false;
    var cleanup_ticks: usize = 0;
    var cleanup_latency_ns: u64 = 0;
    var reopen_count: usize = 0;
    var combined_sweeps: usize = 0;
    var coordinator_sweeps: usize = 0;
    var worker_pool_sweeps: usize = 0;
    var tick_index: usize = 0;
    while (tick_index < cfg.max_ticks) : (tick_index += 1) {
        const cleanup_active_before_tick = try familyCleanupActive(db, family);
        const tick_started = platform_time.monotonicNs();
        const tick = switch (cfg.maintenance_mode) {
            .combined => blk: {
                combined_sweeps += 1;
                break :blk try db.runGraphMetricPlannedMaintenanceForIdle(.{
                    .worker_ids = worker_ids,
                    .worker_id = if (worker_ids.len == 0) "release-qualification-worker" else worker_ids[0],
                    .max_rounds = cfg.max_rounds_per_tick,
                    .max_metrics_per_round = cfg.max_metrics_per_round,
                    .max_pages_per_round = cfg.max_pages_per_round,
                });
            },
            .split => blk: {
                const split = try runSplitMaintenanceTick(db, worker_ids, cfg, worker_step_counts);
                coordinator_sweeps += split.coordinator_sweeps;
                worker_pool_sweeps += split.worker_pool_sweeps;
                break :blk split.result;
            },
        };
        const tick_elapsed_ns = platform_time.monotonicNs() - tick_started;
        if (cleanup_active_before_tick) {
            cleanup_started = true;
            cleanup_ticks += 1;
        }
        if (cleanup_started) cleanup_latency_ns += tick_elapsed_ns;
        total.add(tick);
        if (try familyFresh(db, family, target_generation)) {
            return .{
                .ticks = tick_index + 1,
                .total = total,
                .combined_sweeps = combined_sweeps,
                .coordinator_sweeps = coordinator_sweeps,
                .worker_pool_sweeps = worker_pool_sweeps,
                .split_worker_identities_with_progress = countWorkerIdentitiesWithProgress(worker_step_counts),
                .cleanup_ticks = cleanup_ticks,
                .cleanup_latency_ns = cleanup_latency_ns,
                .reopen_count = reopen_count,
            };
        }
        if (tick.published != 0 or try familyCleanupActive(db, family)) cleanup_started = true;
        if (!tick.progressed()) return error.GraphMetricBuildNoEligiblePage;
        if (cfg.reopen_between_ticks) {
            try reopenPlannedDb(alloc, db, db_path);
            reopen_count += 1;
        }
    }
    return error.GraphMetricReleaseQualificationTimedOut;
}

const SplitMaintenanceTick = struct {
    result: db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult,
    coordinator_sweeps: usize,
    worker_pool_sweeps: usize,
};

fn runSplitMaintenanceTick(
    db: *db_mod.DB,
    worker_ids: []const []const u8,
    cfg: Config,
    worker_step_counts: []usize,
) !SplitMaintenanceTick {
    var total = db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var coordinator_sweeps: usize = 0;
    var worker_pool_sweeps: usize = 0;

    var rounds: usize = 0;
    while (rounds < cfg.max_rounds_per_tick) : (rounds += 1) {
        var round = db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};

        const coordinator_before = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = cfg.max_metrics_per_round,
            .start_background_builds = true,
        });
        coordinator_sweeps += 1;
        round.add(coordinator_before);

        const worker_pool = try runSplitWorkerPoolSweep(db, worker_ids, cfg, worker_step_counts);
        worker_pool_sweeps += 1;
        round.add(worker_pool);

        const coordinator_after = try db.runGraphMetricPlannedCoordinatorSweep(.{
            .max_metrics = cfg.max_metrics_per_round,
            .start_background_builds = true,
        });
        coordinator_sweeps += 1;
        round.add(coordinator_after);

        const round_budget_exhausted = round.budget_exhausted;
        round.budget_exhausted = false;
        round.rounds_executed = 1;
        total.add(round);
        if (!round.durableProgressed()) {
            total.budget_exhausted = round_budget_exhausted;
            break;
        }
    }

    if (rounds == cfg.max_rounds_per_tick and cfg.max_rounds_per_tick != 0) {
        total.budget_exhausted = true;
    }

    return .{
        .result = total,
        .coordinator_sweeps = coordinator_sweeps,
        .worker_pool_sweeps = worker_pool_sweeps,
    };
}

fn runSplitWorkerPoolSweep(
    db: *db_mod.DB,
    worker_ids: []const []const u8,
    cfg: Config,
    worker_step_counts: []usize,
) !db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult {
    if (worker_ids.len == 0) {
        return try db.runGraphMetricPlannedWorkerSweep(.{
            .worker_id = "release-qualification-worker",
            .max_pages = cfg.max_pages_per_round,
        });
    }

    var total = db_mod.IndexManager.GraphMetricPlannedSchedulerSweepResult{};
    var pages_remaining = cfg.max_pages_per_round;
    while (pages_remaining > 0) {
        var worker_progressed = false;
        for (worker_ids, 0..) |worker_id, worker_index| {
            if (pages_remaining == 0) break;
            const worker = try db.runGraphMetricPlannedWorkerSweep(.{
                .worker_id = worker_id,
                .max_pages = 1,
            });
            total.add(worker);
            worker_step_counts[worker_index] += worker.worker_steps;
            pages_remaining -= @min(pages_remaining, worker.worker_steps);
            worker_progressed = worker_progressed or worker.durableProgressed();
        }
        if (!worker_progressed) break;
    }
    return total;
}

fn countWorkerIdentitiesWithProgress(worker_step_counts: []const usize) usize {
    var active: usize = 0;
    for (worker_step_counts) |steps| {
        if (steps != 0) active += 1;
    }
    return active;
}

fn reopenPlannedDb(alloc: std.mem.Allocator, db: *db_mod.DB, db_path: []const u8) !void {
    db.close();
    db.* = try db_mod.DB.open(alloc, db_path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
}

fn familyCleanupActive(db: *db_mod.DB, family: Family) !bool {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var primary = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer primary.deinit(db.core.index_manager.alloc);
    if (primary.phase == .cleanup_old_generations) return true;
    if (family == .hits) {
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(db.core.index_manager.alloc);
        return hub.phase == .cleanup_old_generations;
    }
    return false;
}

const ActiveReadResult = struct {
    published_ns: u64,
    fresh_fail_ns: u64,
    rerank_published_ns: u64,
    rerank_fresh_fail_ns: u64,
    traversal_published_ns: u64,
    traversal_fresh_fail_ns: u64,
    traversal_published_checks: usize,
    traversal_fresh_rejections: usize,
    profile_entries: usize,
    target_generation: u64,
    generation_delta: u64,
    hits_active_paired_published_results: usize = 0,
    hits_active_paired_fresh_rejections: usize = 0,
    hits_active_paired_rerank_published_results: usize = 0,
    hits_active_paired_rerank_fresh_rejections: usize = 0,
    hits_active_paired_traversal_metric_results: usize = 0,
    active_page_probe_claimed: bool,
    active_page_probe_reclaimed: bool,
    active_status_pages: usize,
    active_status_pages_truncated: bool,
    active_status_progress: f64,
    active_pending_work: usize,
    active_work_active_builds: usize,
    active_work_active_pages: usize,
    active_work_failed_pages: usize,
    active_work_truncated_pages: bool,
    failed_published_ns: u64,
    failed_fresh_fail_ns: u64,
    failed_rerank_published_ns: u64,
    failed_rerank_fresh_fail_ns: u64,
    failed_traversal_published_ns: u64,
    failed_traversal_fresh_fail_ns: u64,
    failed_traversal_published_checks: usize,
    failed_traversal_fresh_rejections: usize,
    failed_profile_entries: usize,
    hits_failed_paired_published_results: usize = 0,
    hits_failed_paired_fresh_rejections: usize = 0,
    hits_failed_paired_rerank_published_results: usize = 0,
    hits_failed_paired_rerank_fresh_rejections: usize = 0,
    hits_failed_paired_traversal_metric_results: usize = 0,
    failure_repeats: usize,
    max_failure_diagnostics: usize,
    failure_retry_count: u64,
    failure_recent_events: usize,
    failure_recent_failures: usize,
    paired_failure_recent_events: usize = 0,
    paired_failure_recent_failures: usize = 0,
    failed_status_pages: usize,
    failed_status_pages_truncated: bool,
    failed_storage_score_records: usize,
    failed_storage_metric_records: usize,
    failed_storage_control_records: usize,
    failed_storage_job_namespace_records: usize,
    failed_storage_failure_records: usize,
    failed_storage_event_records: usize,
    failed_pending_work: usize,
    failed_active_builds: usize,
    failed_active_pages: usize,
    failed_failed_pages: usize,
    failed_truncated_pages: bool,
};

fn measureActiveReadContract(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    family: Family,
    previous_generation: u64,
    expected_score_records: usize,
) !ActiveReadResult {
    var mutation_index: usize = 0;
    while (mutation_index < cfg.active_mutation_writes) : (mutation_index += 1) {
        try applyActiveMutation(alloc, db, family, cfg.docs, mutation_index);
    }

    const next_generation = blk: {
        const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
        break :blk graph_entry.index.edge_generation;
    };
    if (next_generation <= previous_generation) return error.GraphMetricReleaseQualificationNoNewGeneration;
    const generation_delta = next_generation - previous_generation;
    if (generation_delta != cfg.active_mutation_writes) {
        return error.GraphMetricReleaseQualificationUnexpectedActiveGenerationDelta;
    }

    var active = try db.ensureGraphMetricPlannedBuild(alloc, graph_index_name, family.primaryMetric(), next_generation);
    defer active.deinit(alloc);
    if (active.state != .building) return error.GraphMetricReleaseQualificationExpectedActiveBuild;
    const active_page_probe = try claimActiveStatusProbePage(db, family);
    const active_work = try verifyActivePlannedWork(db, family);
    const active_status = try verifyActiveStatusBounded(db, family, previous_generation, next_generation, cfg.max_status_pages, active_page_probe.claimed);

    const published_started = platform_time.monotonicNs();
    var published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "active_published",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    const published_ns = platform_time.monotonicNs() - published_started;
    defer published.deinit();
    if (published.graph_metric_results.len != 1) return error.GraphMetricReleaseQualificationMissingPublishedRead;
    if (published.graph_metric_results[0].status.published_generation != previous_generation) {
        return error.GraphMetricReleaseQualificationPublishedGenerationChanged;
    }

    const fresh_started = platform_time.monotonicNs();
    const fresh_result = db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "active_fresh",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    const fresh_fail_ns = platform_time.monotonicNs() - fresh_started;
    if (fresh_result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => {},
        else => return err,
    }

    const active_rerank_published = try verifyGraphMetricRerankPublishedRead(alloc, db, family.primaryMetric(), previous_generation, .building);
    const active_rerank_fresh_fail_ns = try verifyGraphMetricRerankFreshFails(alloc, db, family.primaryMetric());
    const active_traversal = try verifyGraphTraversalReadContract(
        alloc,
        db,
        family,
        previous_generation,
        if (family == .hits) null else .building,
    );
    const active_profile_entries = try verifyGraphMetricProfileEvidence(
        alloc,
        db,
        family,
        previous_generation,
        if (family == .hits) null else .building,
    );

    var hits_active_paired_published_results: usize = 0;
    var hits_active_paired_fresh_rejections: usize = 0;
    var hits_active_paired_rerank_published_results: usize = 0;
    var hits_active_paired_rerank_fresh_rejections: usize = 0;
    if (family == .hits) {
        hits_active_paired_published_results = try verifyHitsPairPublishedRead(
            alloc,
            db,
            "active_hits_authority",
            "active_hits_hub",
            previous_generation,
            null,
        );
        hits_active_paired_fresh_rejections = try verifyHitsPairFreshFails(
            alloc,
            db,
            "active_fresh_hits_authority",
            "active_fresh_hits_hub",
        );
        hits_active_paired_rerank_published_results += 1;
        _ = try verifyGraphMetricRerankPublishedRead(alloc, db, "hits_hub", previous_generation, null);
        hits_active_paired_rerank_published_results += 1;
        hits_active_paired_rerank_fresh_rejections += 1;
        hits_active_paired_rerank_fresh_rejections += try countGraphMetricRerankFreshFails(alloc, db, "hits_hub");
    }

    var failed = try db.failGraphMetricPlannedBuild(alloc, graph_index_name, family.primaryMetric(), error.InvalidGraphMetricScore);
    defer failed.deinit(alloc);
    if (failed.state != .failed or failed.published_generation != previous_generation) {
        return error.GraphMetricReleaseQualificationFailedBuildDidNotPreservePriorGeneration;
    }

    const failed_published_started = platform_time.monotonicNs();
    var failed_published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "failed_published",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    const failed_published_ns = platform_time.monotonicNs() - failed_published_started;
    defer failed_published.deinit();
    if (failed_published.graph_metric_results.len != 1) return error.GraphMetricReleaseQualificationMissingFailedPublishedRead;
    if (failed_published.graph_metric_results[0].status.published_generation != previous_generation) {
        return error.GraphMetricReleaseQualificationFailedReadGenerationChanged;
    }

    const failed_fresh_started = platform_time.monotonicNs();
    const failed_fresh_result = db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "failed_fresh",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    const failed_fresh_fail_ns = platform_time.monotonicNs() - failed_fresh_started;
    if (failed_fresh_result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationFailedFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => {},
        else => return err,
    }

    const failed_rerank_published = try verifyGraphMetricRerankPublishedRead(alloc, db, family.primaryMetric(), previous_generation, .failed);
    const failed_rerank_fresh_fail_ns = try verifyGraphMetricRerankFreshFails(alloc, db, family.primaryMetric());
    const failed_traversal = try verifyGraphTraversalReadContract(
        alloc,
        db,
        family,
        previous_generation,
        .failed,
    );
    const failed_profile_entries = try verifyGraphMetricProfileEvidence(
        alloc,
        db,
        family,
        previous_generation,
        .failed,
    );

    var hits_failed_paired_published_results: usize = 0;
    var hits_failed_paired_fresh_rejections: usize = 0;
    var hits_failed_paired_rerank_published_results: usize = 0;
    var hits_failed_paired_rerank_fresh_rejections: usize = 0;
    if (family == .hits) {
        hits_failed_paired_published_results = try verifyHitsPairPublishedRead(
            alloc,
            db,
            "failed_hits_authority",
            "failed_hits_hub",
            previous_generation,
            .failed,
        );
        hits_failed_paired_fresh_rejections = try verifyHitsPairFreshFails(
            alloc,
            db,
            "failed_fresh_hits_authority",
            "failed_fresh_hits_hub",
        );
        hits_failed_paired_rerank_published_results += 1;
        _ = try verifyGraphMetricRerankPublishedRead(alloc, db, "hits_hub", previous_generation, .failed);
        hits_failed_paired_rerank_published_results += 1;
        hits_failed_paired_rerank_fresh_rejections += 1;
        hits_failed_paired_rerank_fresh_rejections += try countGraphMetricRerankFreshFails(alloc, db, "hits_hub");
    }

    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var failed_graph_status = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer failed_graph_status.deinit(db.core.index_manager.alloc);
    if (failed_graph_status.state != .failed) return error.GraphMetricReleaseQualificationExpectedFailedStatus;
    if (failed_graph_status.recent_events.len == 0 or failed_graph_status.recent_failures.len == 0) {
        return error.GraphMetricReleaseQualificationMissingFailureDiagnostics;
    }

    var paired_failure_recent_events: usize = 0;
    var paired_failure_recent_failures: usize = 0;
    if (family == .hits) {
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(db.core.index_manager.alloc);
        if (hub_status.state != .failed) return error.GraphMetricReleaseQualificationExpectedFailedStatus;
        if (hub_status.published_generation != previous_generation) {
            return error.GraphMetricReleaseQualificationFailedBuildDidNotPreservePriorGeneration;
        }
        if (hub_status.recent_events.len != failed_graph_status.recent_events.len or
            hub_status.recent_failures.len != failed_graph_status.recent_failures.len)
        {
            return error.GraphMetricReleaseQualificationPairedFailureDiagnosticsMismatch;
        }
        paired_failure_recent_events = hub_status.recent_events.len;
        paired_failure_recent_failures = hub_status.recent_failures.len;
    }

    const repeated = try runRepeatedFailedRebuilds(alloc, db, cfg, family, previous_generation, expected_score_records);
    paired_failure_recent_events = repeated.paired_failure_recent_events;
    paired_failure_recent_failures = repeated.paired_failure_recent_failures;
    const failed_work = try verifyNoPendingPlannedWork(db);

    return .{
        .published_ns = published_ns,
        .fresh_fail_ns = fresh_fail_ns,
        .rerank_published_ns = active_rerank_published.latency_ns,
        .rerank_fresh_fail_ns = active_rerank_fresh_fail_ns,
        .traversal_published_ns = active_traversal.published_ns,
        .traversal_fresh_fail_ns = active_traversal.fresh_fail_ns,
        .traversal_published_checks = active_traversal.published_checks,
        .traversal_fresh_rejections = active_traversal.fresh_rejections,
        .profile_entries = active_profile_entries,
        .target_generation = next_generation,
        .generation_delta = generation_delta,
        .hits_active_paired_published_results = hits_active_paired_published_results,
        .hits_active_paired_fresh_rejections = hits_active_paired_fresh_rejections,
        .hits_active_paired_rerank_published_results = hits_active_paired_rerank_published_results,
        .hits_active_paired_rerank_fresh_rejections = hits_active_paired_rerank_fresh_rejections,
        .hits_active_paired_traversal_metric_results = active_traversal.hits_paired_metric_results,
        .active_page_probe_claimed = active_page_probe.claimed,
        .active_page_probe_reclaimed = active_page_probe.reclaimed,
        .active_status_pages = active_status.build_pages,
        .active_status_pages_truncated = active_status.build_pages_truncated,
        .active_status_progress = active_status.progress,
        .active_pending_work = active_work.pendingWork(),
        .active_work_active_builds = active_work.active_builds,
        .active_work_active_pages = active_work.active_pages,
        .active_work_failed_pages = active_work.failed_pages,
        .active_work_truncated_pages = active_work.truncated_pages,
        .failed_published_ns = failed_published_ns,
        .failed_fresh_fail_ns = failed_fresh_fail_ns,
        .failed_rerank_published_ns = failed_rerank_published.latency_ns,
        .failed_rerank_fresh_fail_ns = failed_rerank_fresh_fail_ns,
        .failed_traversal_published_ns = failed_traversal.published_ns,
        .failed_traversal_fresh_fail_ns = failed_traversal.fresh_fail_ns,
        .failed_traversal_published_checks = failed_traversal.published_checks,
        .failed_traversal_fresh_rejections = failed_traversal.fresh_rejections,
        .failed_profile_entries = failed_profile_entries,
        .hits_failed_paired_published_results = hits_failed_paired_published_results,
        .hits_failed_paired_fresh_rejections = hits_failed_paired_fresh_rejections,
        .hits_failed_paired_rerank_published_results = hits_failed_paired_rerank_published_results,
        .hits_failed_paired_rerank_fresh_rejections = hits_failed_paired_rerank_fresh_rejections,
        .hits_failed_paired_traversal_metric_results = failed_traversal.hits_paired_metric_results,
        .failure_repeats = cfg.failure_repeats,
        .max_failure_diagnostics = cfg.max_failure_diagnostics,
        .failure_retry_count = repeated.retry_count,
        .failure_recent_events = repeated.recent_events,
        .failure_recent_failures = repeated.recent_failures,
        .paired_failure_recent_events = paired_failure_recent_events,
        .paired_failure_recent_failures = paired_failure_recent_failures,
        .failed_status_pages = repeated.build_pages,
        .failed_status_pages_truncated = repeated.build_pages_truncated,
        .failed_storage_score_records = repeated.storage.score_records,
        .failed_storage_metric_records = repeated.storage.metric_records,
        .failed_storage_control_records = repeated.storage.control_records,
        .failed_storage_job_namespace_records = repeated.storage.job_namespace_records,
        .failed_storage_failure_records = repeated.storage.failure_records,
        .failed_storage_event_records = repeated.storage.event_records,
        .failed_pending_work = failed_work.pendingWork(),
        .failed_active_builds = failed_work.active_builds,
        .failed_active_pages = failed_work.active_pages,
        .failed_failed_pages = failed_work.failed_pages,
        .failed_truncated_pages = failed_work.truncated_pages,
    };
}

fn applyActiveMutation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    docs: usize,
    sequence: usize,
) !void {
    var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
        }
        writes.deinit(alloc);
    }
    try seedActiveMutation(alloc, &writes, family, docs, sequence);
    try db.batch(.{ .writes = writes.items, .sync_level = .write });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
}

const TraversalReadContractResult = struct {
    published_ns: u64,
    fresh_fail_ns: u64,
    published_checks: usize,
    fresh_rejections: usize,
    hits_paired_metric_results: usize = 0,
};

fn verifyGraphMetricProfileEvidence(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !usize {
    const graph_metric_queries = [_]types.NamedGraphMetricQuery{.{
        .name = "release_profile_metric",
        .query = .{
            .index_name = graph_index_name,
            .metric_name = family.primaryMetric(),
            .top_k = 4,
            .freshness = .published,
        },
    }};
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = if (family == .hits)
        [_]graph_query_mod.GraphMetricRead{
            .{ .name = family.primaryMetric(), .freshness = .published },
            .{ .name = "hits_hub", .freshness = .published },
        }
    else
        [_]graph_query_mod.GraphMetricRead{
            .{ .name = family.primaryMetric(), .freshness = .published },
            .{ .name = family.primaryMetric(), .freshness = .published },
        };
    const graph_queries = [_]types.NamedGraphQuery{.{
        .name = "release_profile_traversal",
        .query = .{
            .query_type = .neighbors,
            .index_name = graph_index_name,
            .start_nodes = .{ .keys = &start_nodes },
            .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
            .metrics = if (family == .hits) metric_reads[0..2] else metric_reads[0..1],
            .include_metric_status = true,
        },
    }};
    const request = types.SearchRequest{
        .index_name = full_text_index_name,
        .full_text = .{ .match_all = {} },
        .graph_metric_queries = &graph_metric_queries,
        .graph_queries = &graph_queries,
        .graph_metric_rerank = .{
            .index_name = graph_index_name,
            .metric_name = family.primaryMetric(),
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
        .profile = true,
    };

    var result = try db.search(alloc, request);
    defer result.deinit();
    var encoded = try query_api.encodeQueryResponses(alloc, "release", request, .{ .took_ms = 1 }, result);
    defer encoded.deinit(alloc);

    if (std.mem.indexOf(u8, encoded.json, "\"profile\"") == null) {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    if (std.mem.indexOf(u8, encoded.json, "\"graph_metrics\"") == null) {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    if (std.mem.indexOf(u8, encoded.json, "\"source\":\"graph_metric\"") == null or
        std.mem.indexOf(u8, encoded.json, "\"source\":\"graph_query\"") == null or
        std.mem.indexOf(u8, encoded.json, "\"source\":\"graph_metric_rerank\"") == null)
    {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    if (std.mem.indexOf(u8, encoded.json, "\"freshness\":\"published\"") == null) {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    const generation_fragment = try std.fmt.allocPrint(alloc, "\"published_generation\":{d}", .{expected_generation});
    defer alloc.free(generation_fragment);
    if (std.mem.indexOf(u8, encoded.json, generation_fragment) == null) {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    if (expected_state) |state| {
        const state_fragment = try std.fmt.allocPrint(alloc, "\"state\":\"{s}\"", .{@tagName(state)});
        defer alloc.free(state_fragment);
        if (std.mem.indexOf(u8, encoded.json, state_fragment) == null) {
            return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
        }
    }
    if (family == .hits and std.mem.indexOf(u8, encoded.json, "\"metric_name\":\"hits_hub\"") == null) {
        return error.GraphMetricReleaseQualificationProfileEvidenceMismatch;
    }
    return countOccurrences(encoded.json, "\"source\":");
}

fn verifyGraphTraversalReadContract(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !TraversalReadContractResult {
    const published_started = platform_time.monotonicNs();
    const projection_metric_results = try verifyGraphTraversalPublishedProjection(
        alloc,
        db,
        family,
        expected_generation,
        expected_state,
    );
    try verifyGraphTraversalPublishedOrder(alloc, db, family, expected_generation, expected_state);
    try verifyGraphTraversalPublishedFilter(alloc, db, family, expected_generation, expected_state);
    const published_ns = platform_time.monotonicNs() - published_started;

    const fresh_started = platform_time.monotonicNs();
    var fresh_rejections: usize = 0;
    fresh_rejections += try countGraphTraversalFreshProjectionFails(alloc, db, family);
    fresh_rejections += try countGraphTraversalFreshOrderFails(alloc, db, family);
    fresh_rejections += try countGraphTraversalFreshFilterFails(alloc, db, family);
    const fresh_fail_ns = platform_time.monotonicNs() - fresh_started;

    return .{
        .published_ns = published_ns,
        .fresh_fail_ns = fresh_fail_ns,
        .published_checks = 3,
        .fresh_rejections = fresh_rejections,
        .hits_paired_metric_results = if (family == .hits) projection_metric_results else 0,
    };
}

fn verifyGraphTraversalPublishedProjection(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    if (family == .hits) {
        const metric_reads = [_]graph_query_mod.GraphMetricRead{
            .{ .name = family.primaryMetric(), .freshness = .published },
            .{ .name = "hits_hub", .freshness = .published },
        };
        const query = graph_query_mod.GraphQuery{
            .query_type = .neighbors,
            .index_name = graph_index_name,
            .start_nodes = .{ .keys = &start_nodes },
            .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
            .metrics = &metric_reads,
            .include_metric_status = true,
        };
        const expected_names = [_][]const u8{ family.primaryMetric(), "hits_hub" };
        return try verifyGraphTraversalPublishedQuery(alloc, db, family, query, expected_generation, expected_state, &expected_names);
    }

    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .include_metric_status = true,
    };
    const expected_names = [_][]const u8{family.primaryMetric()};
    return try verifyGraphTraversalPublishedQuery(alloc, db, family, query, expected_generation, expected_state, &expected_names);
}

fn verifyGraphTraversalPublishedOrder(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !void {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .order_by = &metric_orders,
        .include_metric_status = true,
    };
    const expected_names = [_][]const u8{family.primaryMetric()};
    _ = try verifyGraphTraversalPublishedQuery(alloc, db, family, query, expected_generation, expected_state, &expected_names);
}

fn verifyGraphTraversalPublishedFilter(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !void {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = family.primaryMetric(),
        .op = .gte,
        .value = 0.0,
        .freshness = .published,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .where_metric = &metric_filters,
        .include_metric_status = true,
    };
    const expected_names = [_][]const u8{family.primaryMetric()};
    _ = try verifyGraphTraversalPublishedQuery(alloc, db, family, query, expected_generation, expected_state, &expected_names);
}

fn verifyGraphTraversalPublishedQuery(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    family: Family,
    query: graph_query_mod.GraphQuery,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
    expected_metric_names: []const []const u8,
) !usize {
    var result = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "release_traversal", .query = query }},
        .limit = 0,
    });
    defer result.deinit();
    if (result.graph_results.len != 1) return error.GraphMetricReleaseQualificationMissingTraversalRead;
    const traversal = result.graph_results[0];
    if (traversal.nodes.len == 0) return error.GraphMetricReleaseQualificationMissingTraversalRead;
    if (!std.mem.eql(u8, traversal.nodes[0].key, traversalExpectedNode(family))) {
        return error.GraphMetricReleaseQualificationTraversalNodeMismatch;
    }
    if (traversal.nodes[0].metrics.len != expected_metric_names.len) {
        return error.GraphMetricReleaseQualificationTraversalMetricMismatch;
    }
    for (traversal.nodes[0].metrics, expected_metric_names) |metric, expected_name| {
        if (!std.mem.eql(u8, metric.name, expected_name) or
            metric.score == null or
            !std.math.isFinite(metric.score.?))
        {
            return error.GraphMetricReleaseQualificationTraversalMetricMismatch;
        }
    }
    if (traversal.metric_status.len != expected_metric_names.len) {
        return error.GraphMetricReleaseQualificationTraversalStatusMismatch;
    }
    for (traversal.metric_status, expected_metric_names) |status, expected_name| {
        if (!std.mem.eql(u8, status.name, expected_name) or
            status.published_generation != expected_generation)
        {
            return error.GraphMetricReleaseQualificationTraversalGenerationChanged;
        }
        if (expected_state) |state| {
            if (status.state != state) return error.GraphMetricReleaseQualificationTraversalStatusMismatch;
        }
    }
    return traversal.nodes[0].metrics.len;
}

fn countGraphTraversalFreshProjectionFails(alloc: std.mem.Allocator, db: *db_mod.DB, family: Family) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    if (family == .hits) {
        const metric_reads = [_]graph_query_mod.GraphMetricRead{
            .{ .name = family.primaryMetric(), .freshness = .fresh },
            .{ .name = "hits_hub", .freshness = .fresh },
        };
        const query = graph_query_mod.GraphQuery{
            .query_type = .neighbors,
            .index_name = graph_index_name,
            .start_nodes = .{ .keys = &start_nodes },
            .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
            .metrics = &metric_reads,
            .include_metric_status = true,
        };
        return try countGraphTraversalFreshFails(alloc, db, query);
    }

    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .fresh,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .include_metric_status = true,
    };
    return try countGraphTraversalFreshFails(alloc, db, query);
}

fn countGraphTraversalFreshOrderFails(alloc: std.mem.Allocator, db: *db_mod.DB, family: Family) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const metric_orders = [_]graph_query_mod.GraphMetricOrder{.{
        .name = family.primaryMetric(),
        .freshness = .fresh,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .order_by = &metric_orders,
        .include_metric_status = true,
    };
    return try countGraphTraversalFreshFails(alloc, db, query);
}

fn countGraphTraversalFreshFilterFails(alloc: std.mem.Allocator, db: *db_mod.DB, family: Family) !usize {
    const start_nodes = [_][]const u8{traversalStartNode(family)};
    const metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = family.primaryMetric(),
        .freshness = .published,
    }};
    const metric_filters = [_]graph_query_mod.GraphMetricFilter{.{
        .name = family.primaryMetric(),
        .op = .gte,
        .value = 0.0,
        .freshness = .fresh,
    }};
    const query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = graph_index_name,
        .start_nodes = .{ .keys = &start_nodes },
        .params = .{ .edge_types = &.{edge_type_name}, .direction = .out, .max_depth = 1, .max_results = 8 },
        .metrics = &metric_reads,
        .where_metric = &metric_filters,
        .include_metric_status = true,
    };
    return try countGraphTraversalFreshFails(alloc, db, query);
}

fn countGraphTraversalFreshFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    query: graph_query_mod.GraphQuery,
) !usize {
    const result = db.search(alloc, .{
        .graph_queries = &.{.{ .name = "release_traversal_fresh", .query = query }},
        .limit = 0,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationTraversalFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => return 1,
        else => return err,
    }
}

fn traversalStartNode(family: Family) []const u8 {
    return switch (family) {
        .hits => "doc:hub-000000",
        else => "doc:src-000000",
    };
}

fn traversalExpectedNode(family: Family) []const u8 {
    return switch (family) {
        .hits => "doc:authority",
        else => "doc:hub",
    };
}

fn countOccurrences(haystack: []const u8, needle: []const u8) usize {
    if (needle.len == 0) return 0;
    var count: usize = 0;
    var start: usize = 0;
    while (start < haystack.len) {
        const index = std.mem.indexOf(u8, haystack[start..], needle) orelse break;
        count += 1;
        start += index + needle.len;
    }
    return count;
}

const RerankPublishedReadResult = struct {
    latency_ns: u64,
};

fn verifyGraphMetricRerankPublishedRead(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    metric_name: []const u8,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !RerankPublishedReadResult {
    const started = platform_time.monotonicNs();
    var result = try db.search(alloc, .{
        .index_name = full_text_index_name,
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = graph_index_name,
            .metric_name = metric_name,
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    const latency_ns = platform_time.monotonicNs() - started;
    defer result.deinit();
    if (result.hits.len == 0) return error.GraphMetricReleaseQualificationMissingRerankRead;
    const status = result.graph_metric_rerank_status orelse return error.GraphMetricReleaseQualificationMissingRerankRead;
    if (status.published_generation != expected_generation) {
        return error.GraphMetricReleaseQualificationRerankGenerationChanged;
    }
    if (expected_state) |state| {
        if (status.state != state) return error.GraphMetricReleaseQualificationRerankStatusMismatch;
    }
    if (result.hits[0].score_details) |details| {
        if (details.published_generation != expected_generation) {
            return error.GraphMetricReleaseQualificationRerankGenerationChanged;
        }
    }
    return .{ .latency_ns = latency_ns };
}

fn verifyGraphMetricRerankFreshFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    metric_name: []const u8,
) !u64 {
    const started = platform_time.monotonicNs();
    _ = try countGraphMetricRerankFreshFails(alloc, db, metric_name);
    return platform_time.monotonicNs() - started;
}

fn countGraphMetricRerankFreshFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    metric_name: []const u8,
) !usize {
    const result = db.search(alloc, .{
        .index_name = full_text_index_name,
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = graph_index_name,
            .metric_name = metric_name,
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationRerankFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => return 1,
        else => return err,
    }
}

fn verifyHitsPairPublishedRead(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    authority_query_name: []const u8,
    hub_query_name: []const u8,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !usize {
    var result = try db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = authority_query_name,
                .query = .{
                    .index_name = graph_index_name,
                    .metric_name = "hits_authority",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
            .{
                .name = hub_query_name,
                .query = .{
                    .index_name = graph_index_name,
                    .metric_name = "hits_hub",
                    .top_k = 4,
                    .freshness = .published,
                },
            },
        },
        .limit = 0,
    });
    defer result.deinit();
    if (result.graph_metric_results.len != 2) return error.GraphMetricReleaseQualificationMissingPublishedRead;
    const authority = graphMetricResultByName(result.graph_metric_results, authority_query_name) orelse {
        return error.GraphMetricReleaseQualificationMissingPublishedRead;
    };
    const hub = graphMetricResultByName(result.graph_metric_results, hub_query_name) orelse {
        return error.GraphMetricReleaseQualificationMissingPublishedRead;
    };
    try verifyHitsPairDirectMetric(authority, authority_query_name, "hits_authority", expected_generation, expected_state);
    try verifyHitsPairDirectMetric(hub, hub_query_name, "hits_hub", expected_generation, expected_state);
    return result.graph_metric_results.len;
}

fn verifyHitsPairDirectMetric(
    result: types.GraphMetricResult,
    query_name: []const u8,
    metric_name: []const u8,
    expected_generation: u64,
    expected_state: ?graph_mod.GraphIndex.GraphMetricState,
) !void {
    if (!std.mem.eql(u8, result.name, query_name)) return error.GraphMetricReleaseQualificationMissingPublishedRead;
    if (!std.mem.eql(u8, result.metric_name, metric_name)) return error.GraphMetricReleaseQualificationMissingPublishedRead;
    if (!std.mem.eql(u8, result.status.name, metric_name)) return error.GraphMetricReleaseQualificationMissingPublishedRead;
    if (result.status.published_generation != expected_generation) {
        return error.GraphMetricReleaseQualificationPublishedGenerationChanged;
    }
    if (expected_state) |state| {
        if (result.status.state != state) return error.GraphMetricReleaseQualificationExpectedFailedStatus;
    }
    if (result.scores.len == 0) return error.GraphMetricReleaseQualificationMissingPublishedRead;
}

fn verifyHitsPairFreshFails(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    authority_query_name: []const u8,
    hub_query_name: []const u8,
) !usize {
    const result = db.search(alloc, .{
        .graph_metric_queries = &.{
            .{
                .name = authority_query_name,
                .query = .{
                    .index_name = graph_index_name,
                    .metric_name = "hits_authority",
                    .top_k = 4,
                    .freshness = .fresh,
                },
            },
            .{
                .name = hub_query_name,
                .query = .{
                    .index_name = graph_index_name,
                    .metric_name = "hits_hub",
                    .top_k = 4,
                    .freshness = .fresh,
                },
            },
        },
        .limit = 0,
    });
    if (result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => return 1,
        else => return err,
    }
}

const RepeatedFailureResult = struct {
    retry_count: u64,
    recent_events: usize,
    recent_failures: usize,
    paired_failure_recent_events: usize = 0,
    paired_failure_recent_failures: usize = 0,
    build_pages: usize,
    build_pages_truncated: bool,
    storage: StorageFootprintSummary,
};

fn runRepeatedFailedRebuilds(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    family: Family,
    published_generation: u64,
    expected_score_records: usize,
) !RepeatedFailureResult {
    var repeat_index: usize = 1;
    while (repeat_index < cfg.failure_repeats) : (repeat_index += 1) {
        var writes = std.ArrayListUnmanaged(types.BatchWrite).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            writes.deinit(alloc);
        }
        try seedActiveMutation(alloc, &writes, family, cfg.docs, repeat_index);
        try db.batch(.{ .writes = writes.items, .sync_level = .write });
        try db.runDerivedUntil(db.core.nextDerivedSequence());

        const target_generation = blk: {
            const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
            break :blk graph_entry.index.edge_generation;
        };
        if (target_generation <= published_generation) {
            return error.GraphMetricReleaseQualificationNoNewGeneration;
        }

        var building = try db.ensureGraphMetricPlannedBuild(alloc, graph_index_name, family.primaryMetric(), target_generation);
        defer building.deinit(alloc);
        if (building.state != .building) return error.GraphMetricReleaseQualificationExpectedActiveBuild;

        var failed = try db.failGraphMetricPlannedBuild(alloc, graph_index_name, family.primaryMetric(), error.InvalidGraphMetricScore);
        defer failed.deinit(alloc);
        if (failed.state != .failed or failed.published_generation != published_generation) {
            return error.GraphMetricReleaseQualificationFailedBuildDidNotPreservePriorGeneration;
        }
    }

    var published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "repeated_failed_published",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published.deinit();
    if (published.graph_metric_results.len != 1) return error.GraphMetricReleaseQualificationMissingFailedPublishedRead;
    if (published.graph_metric_results[0].status.published_generation != published_generation) {
        return error.GraphMetricReleaseQualificationFailedReadGenerationChanged;
    }

    const fresh_result = db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "repeated_failed_fresh",
            .query = .{
                .index_name = graph_index_name,
                .metric_name = family.primaryMetric(),
                .top_k = @intCast(@min(cfg.top_k, 8)),
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    if (fresh_result) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.GraphMetricReleaseQualificationFailedFreshReadDidNotFail;
    } else |err| switch (err) {
        error.MetricStale => {},
        else => return err,
    }

    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer status.deinit(db.core.index_manager.alloc);
    if (status.state != .failed) return error.GraphMetricReleaseQualificationExpectedFailedStatus;
    if (status.published_generation != published_generation) {
        return error.GraphMetricReleaseQualificationFailedBuildDidNotPreservePriorGeneration;
    }
    if (status.retry_count != cfg.failure_repeats) return error.GraphMetricReleaseQualificationFailureRetryCountMismatch;
    if (status.recent_events.len == 0 or status.recent_failures.len == 0) {
        return error.GraphMetricReleaseQualificationMissingFailureDiagnostics;
    }
    const expected_recent_events_max = @min(cfg.failure_repeats + 1, cfg.max_failure_diagnostics);
    const expected_recent_failures_max = @min(cfg.failure_repeats, cfg.max_failure_diagnostics);
    if (status.recent_events.len != expected_recent_events_max or status.recent_failures.len != expected_recent_failures_max) {
        return error.GraphMetricReleaseQualificationFailureDiagnosticsMismatch;
    }
    try verifyNoActiveBuildStatus(status);

    var paired_failure_recent_events: usize = 0;
    var paired_failure_recent_failures: usize = 0;
    var paired_build_pages: usize = 0;
    var paired_build_pages_truncated = false;
    if (family == .hits) {
        var hub_status = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub_status.deinit(db.core.index_manager.alloc);
        if (hub_status.state != .failed) return error.GraphMetricReleaseQualificationExpectedFailedStatus;
        if (hub_status.published_generation != published_generation or hub_status.retry_count != status.retry_count) {
            return error.GraphMetricReleaseQualificationFailedBuildDidNotPreservePriorGeneration;
        }
        if (hub_status.recent_events.len != status.recent_events.len or
            hub_status.recent_failures.len != status.recent_failures.len)
        {
            return error.GraphMetricReleaseQualificationPairedFailureDiagnosticsMismatch;
        }
        try verifyNoActiveBuildStatus(hub_status);
        paired_failure_recent_events = hub_status.recent_events.len;
        paired_failure_recent_failures = hub_status.recent_failures.len;
        paired_build_pages = hub_status.build_pages.len;
        paired_build_pages_truncated = hub_status.build_pages_truncated;
    }

    const storage = try familyStorageFootprint(db, family);
    const expected_diagnostic_records = if (family == .hits)
        @min(cfg.failure_repeats, cfg.max_failure_diagnostics) * 2
    else
        @min(cfg.failure_repeats, cfg.max_failure_diagnostics);
    const expected_event_records = if (family == .hits)
        @min(cfg.failure_repeats + 1, cfg.max_failure_diagnostics) * 2
    else
        @min(cfg.failure_repeats + 1, cfg.max_failure_diagnostics);
    if (storage.score_records != expected_score_records) return error.GraphMetricReleaseQualificationLeakedBuildStorage;
    if (storage.job_namespace_records != 0) return error.GraphMetricReleaseQualificationLeakedBuildStorage;
    if (storage.failure_records != expected_diagnostic_records) {
        return error.GraphMetricReleaseQualificationFailureDiagnosticsMismatch;
    }
    if (storage.event_records != expected_event_records) {
        return error.GraphMetricReleaseQualificationFailureDiagnosticsMismatch;
    }
    if (storage.control_records > expected_diagnostic_records + expected_event_records) {
        return error.GraphMetricReleaseQualificationUnboundedFailureDiagnostics;
    }

    return .{
        .retry_count = status.retry_count,
        .recent_events = status.recent_events.len,
        .recent_failures = status.recent_failures.len,
        .paired_failure_recent_events = paired_failure_recent_events,
        .paired_failure_recent_failures = paired_failure_recent_failures,
        .build_pages = status.build_pages.len + paired_build_pages,
        .build_pages_truncated = status.build_pages_truncated or paired_build_pages_truncated,
        .storage = storage,
    };
}

fn refreshLocalOracle(alloc: std.mem.Allocator, db: *db_mod.DB, family: Family, target_generation: u64) !void {
    switch (family) {
        .hits => {
            var authority = try db.refreshGraphMetric(alloc, graph_index_name, "hits_authority");
            defer authority.deinit(alloc);
            if (authority.state != .fresh or authority.published_generation != target_generation) {
                return error.GraphMetricReleaseQualificationLocalOracleFailed;
            }
            const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
            var hub = try graph_entry.index.graphMetricStatus("hits_hub");
            defer hub.deinit(db.core.index_manager.alloc);
            if (hub.state != .fresh or hub.published_generation != target_generation) {
                return error.GraphMetricReleaseQualificationLocalOracleFailed;
            }
        },
        else => {
            var status = try db.refreshGraphMetric(alloc, graph_index_name, family.primaryMetric());
            defer status.deinit(alloc);
            if (status.state != .fresh or status.published_generation != target_generation) {
                return error.GraphMetricReleaseQualificationLocalOracleFailed;
            }
        },
    }
}

fn familyFresh(db: *db_mod.DB, family: Family, target_generation: u64) !bool {
    const graph_entry = db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    var primary = try graph_entry.index.graphMetricStatus(family.primaryMetric());
    defer primary.deinit(db.core.index_manager.alloc);
    if (primary.state != .fresh or primary.phase != .complete or primary.published_generation != target_generation) {
        return false;
    }
    if (family == .hits) {
        var hub = try graph_entry.index.graphMetricStatus("hits_hub");
        defer hub.deinit(db.core.index_manager.alloc);
        return hub.state == .fresh and hub.phase == .complete and hub.published_generation == target_generation and hub.published_generation == primary.published_generation;
    }
    return true;
}

fn compareFamilyTopK(alloc: std.mem.Allocator, local_db: *db_mod.DB, planned_db: *db_mod.DB, family: Family, top_k: usize) !void {
    try compareMetricTopK(alloc, local_db, planned_db, family.primaryMetric(), top_k);
    if (family == .hits) {
        try compareMetricTopK(alloc, local_db, planned_db, "hits_hub", top_k);
    }
}

fn compareMetricTopK(alloc: std.mem.Allocator, local_db: *db_mod.DB, planned_db: *db_mod.DB, metric_name: []const u8, top_k: usize) !void {
    const local_graph = local_db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    const planned_graph = planned_db.core.graphIndex(graph_index_name) orelse return error.IndexNotFound;
    const local_top = try local_graph.index.graphMetricTopK(metric_name, top_k);
    defer freeTopK(alloc, local_top);
    const planned_top = try planned_graph.index.graphMetricTopK(metric_name, top_k);
    defer freeTopK(alloc, planned_top);

    if (local_top.len != planned_top.len) return error.GraphMetricReleaseQualificationTopKLengthMismatch;
    for (local_top, planned_top) |local, planned| {
        if (!std.mem.eql(u8, local.node, planned.node)) return error.GraphMetricReleaseQualificationTopKNodeMismatch;
        if (!std.math.isFinite(local.score) or !std.math.isFinite(planned.score)) {
            return error.GraphMetricReleaseQualificationNonFiniteScore;
        }
        if (@abs(local.score - planned.score) > 0.0000001) {
            return error.GraphMetricReleaseQualificationScoreMismatch;
        }
    }
}

fn freeTopK(alloc: std.mem.Allocator, scores: []graph_mod.GraphIndex.GraphMetricScore) void {
    for (scores) |*score| score.deinit(alloc);
    alloc.free(scores);
}

fn graphIndexConfig(alloc: std.mem.Allocator, cfg: Config, family: Family) ![]u8 {
    return switch (family) {
        .degree => try alloc.dupe(
            u8,
            "{\"metrics\":{\"degree\":{\"enabled\":true,\"kind\":\"degree\",\"refresh\":\"background\",\"edge_filter\":{\"types\":[\"cites\"]}}}}",
        ),
        .pagerank => try std.fmt.allocPrint(
            alloc,
            "{{\"metrics\":{{\"pagerank\":{{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":{d:.12},\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
            .{ cfg.max_iterations, cfg.tolerance },
        ),
        .eigenvector => try std.fmt.allocPrint(
            alloc,
            "{{\"metrics\":{{\"eigenvector\":{{\"enabled\":true,\"kind\":\"eigenvector\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":{d:.12},\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
            .{ cfg.max_iterations, cfg.tolerance },
        ),
        .hits => try std.fmt.allocPrint(
            alloc,
            "{{\"metrics\":{{\"hits_authority\":{{\"enabled\":true,\"kind\":\"hits_authority\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":{d:.12},\"edge_filter\":{{\"types\":[\"cites\"]}}}},\"hits_hub\":{{\"enabled\":true,\"kind\":\"hits_hub\",\"refresh\":\"background\",\"max_iterations\":{d},\"tolerance\":{d:.12},\"edge_filter\":{{\"types\":[\"cites\"]}}}}}}}}",
            .{ cfg.max_iterations, cfg.tolerance, cfg.max_iterations, cfg.tolerance },
        ),
    };
}

fn seedWrites(alloc: std.mem.Allocator, writes: *std.ArrayListUnmanaged(types.BatchWrite), family: Family, cfg: Config) !void {
    switch (family) {
        .hits => {
            for (0..cfg.docs) |i| {
                const key = try std.fmt.allocPrint(alloc, "doc:hub-{d:0>6}", .{i});
                errdefer alloc.free(key);
                const value = try buildHitsHubValue(alloc, i, cfg.fanout);
                errdefer alloc.free(value);
                try writes.append(alloc, .{ .key = key, .value = value });
            }
            for (0..cfg.fanout) |i| {
                const authority_key = try authorityKeyAlloc(alloc, i);
                errdefer alloc.free(authority_key);
                const authority_value = try buildAuthorityValue(alloc, i, authority_key);
                errdefer alloc.free(authority_value);
                try writes.append(alloc, .{ .key = authority_key, .value = authority_value });
            }
        },
        else => {
            for (0..cfg.docs) |i| {
                const key = try std.fmt.allocPrint(alloc, "doc:src-{d:0>6}", .{i});
                errdefer alloc.free(key);
                const value = try buildSourceValue(alloc, i, cfg.docs, cfg.fanout);
                errdefer alloc.free(value);
                try writes.append(alloc, .{ .key = key, .value = value });
            }
            const hub_key = try alloc.dupe(u8, "doc:hub");
            errdefer alloc.free(hub_key);
            const hub_value = try alloc.dupe(u8, "{\"title\":\"hub\"}");
            errdefer alloc.free(hub_value);
            try writes.append(alloc, .{ .key = hub_key, .value = hub_value });
        },
    }
}

fn buildSourceValue(alloc: std.mem.Allocator, i: usize, docs: usize, fanout: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"source {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[",
        .{i},
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    try appendCitesEdge(&out, alloc, "doc:hub", false);

    var edge_index: usize = 1;
    while (edge_index < fanout) : (edge_index += 1) {
        const target = try std.fmt.allocPrint(alloc, "doc:src-{d:0>6}", .{(i + edge_index) % docs});
        defer alloc.free(target);
        try appendCitesEdge(&out, alloc, target, true);
    }

    try out.appendSlice(alloc, "]}}}");
    return try out.toOwnedSlice(alloc);
}

fn buildHitsHubValue(alloc: std.mem.Allocator, i: usize, fanout: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"hub {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[",
        .{i},
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);

    for (0..fanout) |authority_index| {
        const target = try authorityKeyAlloc(alloc, authority_index);
        defer alloc.free(target);
        try appendCitesEdge(&out, alloc, target, authority_index != 0);
    }

    try out.appendSlice(alloc, "]}}}");
    return try out.toOwnedSlice(alloc);
}

fn buildAuthorityValue(alloc: std.mem.Allocator, i: usize, key: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"title\":\"authority {d}\",\"_edges\":{{\"graph_idx\":{{\"cites\":[",
        .{i},
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    try appendCitesEdge(&out, alloc, key, false);
    try out.appendSlice(alloc, "]}}}");
    return try out.toOwnedSlice(alloc);
}

fn appendCitesEdge(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, target: []const u8, comma: bool) !void {
    if (comma) try out.appendSlice(alloc, ",");
    const edge = try std.fmt.allocPrint(
        alloc,
        "{{\"target\":\"{s}\",\"weight\":1.0}}",
        .{target},
    );
    defer alloc.free(edge);
    try out.appendSlice(alloc, edge);
}

fn authorityKeyAlloc(alloc: std.mem.Allocator, i: usize) ![]u8 {
    if (i == 0) return try alloc.dupe(u8, "doc:authority");
    return try std.fmt.allocPrint(alloc, "doc:authority-{d:0>6}", .{i});
}

fn seedActiveMutation(
    alloc: std.mem.Allocator,
    writes: *std.ArrayListUnmanaged(types.BatchWrite),
    family: Family,
    docs: usize,
    sequence: usize,
) !void {
    switch (family) {
        .hits => {
            const key = try std.fmt.allocPrint(alloc, "doc:active-hub-{d}-{d}", .{ docs, sequence });
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, "{\"title\":\"active hub\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:authority\",\"weight\":1.0}]}}}");
            errdefer alloc.free(value);
            try writes.append(alloc, .{ .key = key, .value = value });
        },
        else => {
            const key = try std.fmt.allocPrint(alloc, "doc:active-src-{d}-{d}", .{ docs, sequence });
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, "{\"title\":\"active source\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:hub\",\"weight\":1.0}]}}}");
            errdefer alloc.free(value);
            try writes.append(alloc, .{ .key = key, .value = value });
        },
    }
}

fn initialNodeCount(cfg: Config, family: Family) usize {
    return switch (family) {
        .hits => cfg.docs + cfg.fanout,
        else => cfg.docs + 1,
    };
}

fn expectedScoreRecords(cfg: Config, family: Family) usize {
    const scores_per_node: usize = switch (family) {
        .hits => 2,
        else => 1,
    };
    return initialNodeCount(cfg, family) * scores_per_node;
}

fn initialEdgeCount(cfg: Config, family: Family) usize {
    return switch (family) {
        .hits => cfg.docs * cfg.fanout + cfg.fanout,
        else => cfg.docs * cfg.fanout,
    };
}

fn makeWorkerIds(alloc: std.mem.Allocator, workers: usize) ![]const []const u8 {
    if (workers <= 1) {
        const one = try alloc.alloc([]const u8, 1);
        one[0] = try alloc.dupe(u8, "release-qualification-worker-0");
        return one;
    }
    const out = try alloc.alloc([]const u8, workers);
    errdefer alloc.free(out);
    for (out, 0..) |*slot, i| {
        slot.* = try std.fmt.allocPrint(alloc, "release-qualification-worker-{d}", .{i});
    }
    return out;
}

fn freeWorkerIds(alloc: std.mem.Allocator, worker_ids: []const []const u8) void {
    for (worker_ids) |worker_id| alloc.free(worker_id);
    alloc.free(worker_ids);
}

fn emitFamilyResult(out: anytype, family: Family, result: FamilyResult) !void {
    try out.print(
        "{{\"event\":\"graph_metric_release_qualification_result\",\"metric_family\":\"{s}\",\"maintenance_mode\":\"{s}\",\"graph_nodes\":{d},\"edge_count\":{d},\"target_generation\":{d},\"ticks\":{d},\"budget_exhausted\":{},\"builds_started\":{d},\"worker_steps\":{d},\"coordinator_steps\":{d},\"pages_claimed\":{d},\"pages_completed\":{d},\"phases_advanced\":{d},\"published\":{d},\"failed_builds\":{d},\"rounds_executed\":{d},\"combined_sweeps\":{d},\"coordinator_sweeps\":{d},\"worker_pool_sweeps\":{d}",
        .{
            family.label(),
            result.maintenance_mode.label(),
            result.graph_nodes,
            result.edge_count,
            result.target_generation,
            result.ticks,
            result.budget_exhausted,
            result.scheduler.builds_started,
            result.scheduler.worker_steps,
            result.scheduler.coordinator_steps,
            result.scheduler.pages_claimed,
            result.scheduler.pages_completed,
            result.scheduler.phases_advanced,
            result.scheduler.published,
            result.scheduler.failed_builds,
            result.scheduler.rounds_executed,
            result.combined_sweeps,
            result.coordinator_sweeps,
            result.worker_pool_sweeps,
        },
    );
    try out.print(
        ",\"worker_identities_configured\":{d},\"split_worker_identities_with_progress\":{d},\"pre_drain_metrics_scanned\":{d},\"pre_drain_queued_builds\":{d},\"pre_publish_not_ready_rejections\":{d},\"hits_pre_publish_paired_not_ready_rejections\":{d},\"pre_publish_rerank_not_ready_rejections\":{d},\"hits_pre_publish_paired_rerank_not_ready_rejections\":{d},\"pre_publish_status_not_ready\":{d},\"hits_pre_publish_paired_status_not_ready\":{d},\"local_latency_ns\":{d},\"planned_latency_ns\":{d},\"cleanup_ticks\":{d},\"cleanup_latency_ns\":{d},\"reopen_count\":{d}",
        .{
            result.worker_identities_configured,
            result.split_worker_identities_with_progress,
            result.pre_drain_metrics_scanned,
            result.pre_drain_queued_builds,
            result.pre_publish_not_ready_rejections,
            result.hits_pre_publish_paired_not_ready_rejections,
            result.pre_publish_rerank_not_ready_rejections,
            result.hits_pre_publish_paired_rerank_not_ready_rejections,
            result.pre_publish_status_not_ready,
            result.hits_pre_publish_paired_status_not_ready,
            result.local_latency_ns,
            result.planned_latency_ns,
            result.cleanup_ticks,
            result.cleanup_latency_ns,
            result.reopen_count,
        },
    );
    try out.print(
        ",\"graph_expected_nodes\":{d},\"graph_expected_edges\":{d},\"graph_source_nodes\":{d},\"graph_sink_nodes\":{d},\"graph_authority_nodes\":{d},\"graph_sink_edges\":{d},\"graph_cycle_edges\":{d},\"graph_bipartite_edges\":{d},\"graph_authority_self_edges\":{d},\"graph_max_out_degree\":{d}",
        .{
            result.graph_expected_nodes,
            result.graph_expected_edges,
            result.graph_source_nodes,
            result.graph_sink_nodes,
            result.graph_authority_nodes,
            result.graph_sink_edges,
            result.graph_cycle_edges,
            result.graph_bipartite_edges,
            result.graph_authority_self_edges,
            result.graph_max_out_degree,
        },
    );
    try out.print(
        ",\"pre_publish_traversal_projection_not_ready\":{d},\"hits_pre_publish_paired_traversal_projection_not_ready\":{d},\"pre_publish_traversal_not_ready_rejections\":{d},\"hits_pre_publish_paired_traversal_not_ready_rejections\":{d}",
        .{
            result.pre_publish_traversal_projection_not_ready,
            result.hits_pre_publish_paired_traversal_projection_not_ready,
            result.pre_publish_traversal_not_ready_rejections,
            result.hits_pre_publish_paired_traversal_not_ready_rejections,
        },
    );
    try out.print(
        ",\"fresh_status_pages\":{d},\"fresh_status_pages_truncated\":{},\"fresh_pending_work\":{d},\"fresh_active_builds\":{d},\"fresh_active_pages\":{d},\"fresh_failed_pages\":{d},\"fresh_truncated_pages\":{},\"expected_score_records\":{d},\"fresh_storage_score_records\":{d},\"fresh_storage_metric_records\":{d},\"fresh_storage_control_records\":{d},\"fresh_storage_job_namespace_records\":{d}",
        .{
            result.fresh_status_pages,
            result.fresh_status_pages_truncated,
            result.fresh_pending_work,
            result.fresh_active_builds,
            result.fresh_active_pages,
            result.fresh_failed_pages,
            result.fresh_truncated_pages,
            result.expected_score_records,
            result.fresh_storage_score_records,
            result.fresh_storage_metric_records,
            result.fresh_storage_control_records,
            result.fresh_storage_job_namespace_records,
        },
    );
    try out.print(
        ",\"fan_in_shards\":{d},\"fan_in_merged_scores\":{d},\"fan_in_active_published_scores\":{d},\"fan_in_fresh_rejections\":{d},\"fan_in_generation_rejections\":{d},\"fan_in_metadata_rejections\":{d},\"fan_in_edge_filter_rejections\":{d},\"fan_in_missing_rejections\":{d},\"fan_in_merge_ns\":{d},\"fan_in_active_published_ns\":{d},\"fan_in_fresh_fail_ns\":{d},\"fan_in_generation_fail_ns\":{d},\"fan_in_metadata_fail_ns\":{d},\"fan_in_edge_filter_fail_ns\":{d},\"fan_in_missing_fail_ns\":{d}",
        .{
            result.fan_in_shards,
            result.fan_in_merged_scores,
            result.fan_in_active_published_scores,
            result.fan_in_fresh_rejections,
            result.fan_in_generation_rejections,
            result.fan_in_metadata_rejections,
            result.fan_in_edge_filter_rejections,
            result.fan_in_missing_rejections,
            result.fan_in_merge_ns,
            result.fan_in_active_published_ns,
            result.fan_in_fresh_fail_ns,
            result.fan_in_generation_fail_ns,
            result.fan_in_metadata_fail_ns,
            result.fan_in_edge_filter_fail_ns,
            result.fan_in_missing_fail_ns,
        },
    );
    try out.print(
        ",\"fan_in_paired_metric_results\":{d},\"fan_in_paired_active_published_metric_results\":{d},\"fan_in_paired_fresh_rejections\":{d},\"fan_in_paired_generation_rejections\":{d},\"fan_in_paired_metadata_rejections\":{d},\"fan_in_paired_edge_filter_rejections\":{d},\"fan_in_paired_missing_rejections\":{d},\"fan_in_paired_merge_ns\":{d},\"fan_in_paired_active_published_ns\":{d},\"fan_in_paired_fresh_fail_ns\":{d},\"fan_in_paired_generation_fail_ns\":{d},\"fan_in_paired_metadata_fail_ns\":{d},\"fan_in_paired_edge_filter_fail_ns\":{d},\"fan_in_paired_missing_fail_ns\":{d}",
        .{
            result.fan_in_paired_metric_results,
            result.fan_in_paired_active_published_metric_results,
            result.fan_in_paired_fresh_rejections,
            result.fan_in_paired_generation_rejections,
            result.fan_in_paired_metadata_rejections,
            result.fan_in_paired_edge_filter_rejections,
            result.fan_in_paired_missing_rejections,
            result.fan_in_paired_merge_ns,
            result.fan_in_paired_active_published_ns,
            result.fan_in_paired_fresh_fail_ns,
            result.fan_in_paired_generation_fail_ns,
            result.fan_in_paired_metadata_fail_ns,
            result.fan_in_paired_edge_filter_fail_ns,
            result.fan_in_paired_missing_fail_ns,
        },
    );
    try out.print(
        ",\"active_mutation_writes\":{d},\"active_target_generation\":{d},\"active_generation_delta\":{d},\"hits_active_paired_published_results\":{d},\"hits_active_paired_fresh_rejections\":{d},\"hits_active_paired_rerank_published_results\":{d},\"hits_active_paired_rerank_fresh_rejections\":{d},\"hits_active_paired_traversal_metric_results\":{d},\"active_profile_entries\":{d},\"active_page_probe_claimed\":{},\"active_page_probe_reclaimed\":{},\"active_status_pages\":{d},\"active_status_pages_truncated\":{},\"active_status_progress\":{d:.12},\"active_pending_work\":{d},\"active_work_active_builds\":{d},\"active_work_active_pages\":{d},\"active_work_failed_pages\":{d},\"active_work_truncated_pages\":{},\"active_published_read_ns\":{d},\"active_fresh_fail_ns\":{d},\"active_rerank_published_ns\":{d},\"active_rerank_fresh_fail_ns\":{d},\"active_traversal_published_ns\":{d},\"active_traversal_fresh_fail_ns\":{d},\"active_traversal_published_checks\":{d},\"active_traversal_fresh_rejections\":{d}",
        .{
            result.active_mutation_writes,
            result.active_target_generation,
            result.active_generation_delta,
            result.hits_active_paired_published_results,
            result.hits_active_paired_fresh_rejections,
            result.hits_active_paired_rerank_published_results,
            result.hits_active_paired_rerank_fresh_rejections,
            result.hits_active_paired_traversal_metric_results,
            result.active_profile_entries,
            result.active_page_probe_claimed,
            result.active_page_probe_reclaimed,
            result.active_status_pages,
            result.active_status_pages_truncated,
            result.active_status_progress,
            result.active_pending_work,
            result.active_work_active_builds,
            result.active_work_active_pages,
            result.active_work_failed_pages,
            result.active_work_truncated_pages,
            result.active_published_read_ns,
            result.active_fresh_fail_ns,
            result.active_rerank_published_ns,
            result.active_rerank_fresh_fail_ns,
            result.active_traversal_published_ns,
            result.active_traversal_fresh_fail_ns,
            result.active_traversal_published_checks,
            result.active_traversal_fresh_rejections,
        },
    );
    try out.print(
        ",\"failed_published_read_ns\":{d},\"failed_fresh_fail_ns\":{d},\"failed_rerank_published_ns\":{d},\"failed_rerank_fresh_fail_ns\":{d},\"failed_traversal_published_ns\":{d},\"failed_traversal_fresh_fail_ns\":{d},\"failed_traversal_published_checks\":{d},\"failed_traversal_fresh_rejections\":{d},\"failed_profile_entries\":{d},\"hits_failed_paired_published_results\":{d},\"hits_failed_paired_fresh_rejections\":{d},\"hits_failed_paired_rerank_published_results\":{d},\"hits_failed_paired_rerank_fresh_rejections\":{d},\"hits_failed_paired_traversal_metric_results\":{d}",
        .{
            result.failed_published_read_ns,
            result.failed_fresh_fail_ns,
            result.failed_rerank_published_ns,
            result.failed_rerank_fresh_fail_ns,
            result.failed_traversal_published_ns,
            result.failed_traversal_fresh_fail_ns,
            result.failed_traversal_published_checks,
            result.failed_traversal_fresh_rejections,
            result.failed_profile_entries,
            result.hits_failed_paired_published_results,
            result.hits_failed_paired_fresh_rejections,
            result.hits_failed_paired_rerank_published_results,
            result.hits_failed_paired_rerank_fresh_rejections,
            result.hits_failed_paired_traversal_metric_results,
        },
    );
    try out.print(
        ",\"failure_repeats\":{d},\"max_failure_diagnostics\":{d},\"failure_retry_count\":{d},\"failure_recent_events\":{d},\"failure_recent_failures\":{d},\"paired_failure_recent_events\":{d},\"paired_failure_recent_failures\":{d},\"failed_status_pages\":{d},\"failed_status_pages_truncated\":{},\"failed_storage_score_records\":{d},\"failed_storage_metric_records\":{d},\"failed_storage_control_records\":{d},\"failed_storage_job_namespace_records\":{d},\"failed_storage_failure_records\":{d},\"failed_storage_event_records\":{d},\"failed_pending_work\":{d},\"failed_active_builds\":{d},\"failed_active_pages\":{d},\"failed_failed_pages\":{d},\"failed_truncated_pages\":{},\"parity\":\"top_k\"}}\n",
        .{
            result.failure_repeats,
            result.max_failure_diagnostics,
            result.failure_retry_count,
            result.failure_recent_events,
            result.failure_recent_failures,
            result.paired_failure_recent_events,
            result.paired_failure_recent_failures,
            result.failed_status_pages,
            result.failed_status_pages_truncated,
            result.failed_storage_score_records,
            result.failed_storage_metric_records,
            result.failed_storage_control_records,
            result.failed_storage_job_namespace_records,
            result.failed_storage_failure_records,
            result.failed_storage_event_records,
            result.failed_pending_work,
            result.failed_active_builds,
            result.failed_active_pages,
            result.failed_failed_pages,
            result.failed_truncated_pages,
        },
    );
}

fn shouldRunFamily(cfg: Config, family: Family) bool {
    const filter = cfg.family_filter orelse return true;
    return std.mem.eql(u8, filter, "all") or std.mem.eql(u8, filter, family.label());
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--profile")) {
            const profile = args.next() orelse return error.InvalidArgument;
            try applyProfile(&cfg, profile);
        } else if (std.mem.eql(u8, arg, "--maintenance-mode")) {
            cfg.maintenance_mode = try MaintenanceMode.parse(args.next() orelse return error.InvalidArgument);
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--fanout")) {
            cfg.fanout = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--top-k")) {
            cfg.top_k = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--synthetic-fan-in-shards")) {
            cfg.synthetic_fan_in_shards = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--active-mutation-writes")) {
            cfg.active_mutation_writes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            cfg.workers = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-ticks")) {
            cfg.max_ticks = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-rounds-per-tick")) {
            cfg.max_rounds_per_tick = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-metrics-per-round")) {
            cfg.max_metrics_per_round = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-pages-per-round")) {
            cfg.max_pages_per_round = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-iterations")) {
            cfg.max_iterations = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--failure-repeats")) {
            cfg.failure_repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-failure-diagnostics")) {
            cfg.max_failure_diagnostics = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-status-pages")) {
            cfg.max_status_pages = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-local-latency-ns")) {
            cfg.max_local_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-planned-latency-ns")) {
            cfg.max_planned_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-cleanup-latency-ns")) {
            cfg.max_cleanup_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-published-read-latency-ns")) {
            cfg.max_published_read_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-fresh-fail-latency-ns")) {
            cfg.max_fresh_fail_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-fan-in-latency-ns")) {
            cfg.max_fan_in_latency_ns = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-storage-metric-records")) {
            cfg.max_storage_metric_records = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-storage-control-records")) {
            cfg.max_storage_control_records = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-page-claims")) {
            cfg.max_page_claims = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-cleanup-ticks")) {
            cfg.max_cleanup_ticks = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-rounds-executed")) {
            cfg.max_rounds_executed = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-failure-retry-count")) {
            cfg.max_failure_retry_count = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-worker-steps")) {
            cfg.max_worker_steps = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-coordinator-steps")) {
            cfg.max_coordinator_steps = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--tolerance")) {
            cfg.tolerance = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--family")) {
            cfg.family_filter = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--reopen-between-ticks")) {
            cfg.reopen_between_ticks = true;
        } else if (std.mem.eql(u8, arg, "--no-reopen-between-ticks")) {
            cfg.reopen_between_ticks = false;
        } else if (std.mem.eql(u8, arg, "--keep-tmp")) {
            cfg.keep_tmp = true;
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.fanout == 0 or cfg.fanout > cfg.docs or cfg.top_k == 0 or cfg.synthetic_fan_in_shards < 2 or cfg.active_mutation_writes == 0 or cfg.workers == 0 or cfg.max_ticks == 0 or cfg.max_rounds_per_tick == 0 or cfg.max_metrics_per_round == 0 or cfg.max_pages_per_round == 0 or cfg.max_iterations == 0 or cfg.failure_repeats == 0 or cfg.max_failure_diagnostics == 0 or cfg.max_status_pages == 0) {
        return error.InvalidArgument;
    }
    if (cfg.family_filter) |family| {
        if (!std.mem.eql(u8, family, "all") and
            !std.mem.eql(u8, family, "degree") and
            !std.mem.eql(u8, family, "pagerank") and
            !std.mem.eql(u8, family, "eigenvector") and
            !std.mem.eql(u8, family, "hits"))
        {
            return error.InvalidArgument;
        }
    }
    return cfg;
}

fn applyProfile(cfg: *Config, profile: []const u8) !void {
    if (std.mem.eql(u8, profile, "smoke")) {
        cfg.profile = "smoke";
        cfg.docs = 4;
        cfg.fanout = 2;
        cfg.top_k = 4;
        cfg.synthetic_fan_in_shards = 2;
        cfg.active_mutation_writes = 1;
        cfg.workers = 2;
        cfg.max_ticks = 500;
        cfg.max_rounds_per_tick = 1;
        cfg.max_metrics_per_round = 8;
        cfg.max_pages_per_round = 2;
        cfg.max_iterations = 2;
        cfg.failure_repeats = 3;
        cfg.max_failure_diagnostics = 8;
        cfg.max_status_pages = 8;
        cfg.maintenance_mode = .combined;
        cfg.reopen_between_ticks = true;
    } else if (std.mem.eql(u8, profile, "promotion")) {
        cfg.profile = "promotion";
        cfg.docs = 1000;
        cfg.fanout = 4;
        cfg.top_k = 32;
        cfg.synthetic_fan_in_shards = 8;
        cfg.active_mutation_writes = 3;
        cfg.workers = 4;
        cfg.max_ticks = 10000;
        cfg.max_rounds_per_tick = 1;
        cfg.max_metrics_per_round = 8;
        cfg.max_pages_per_round = 8;
        cfg.max_iterations = 8;
        cfg.failure_repeats = 5;
        cfg.max_failure_diagnostics = 16;
        cfg.max_status_pages = 16;
        cfg.maintenance_mode = .split;
        cfg.reopen_between_ticks = true;
    } else {
        std.debug.print("invalid profile: {s}\n", .{profile});
        return error.InvalidArgument;
    }
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn parseNextF64(args: *std.process.Args.Iterator, flag: []const u8) !f64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseFloat(f64, raw);
}
