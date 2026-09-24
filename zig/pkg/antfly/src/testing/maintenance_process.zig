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
const antfly = @import("../root.zig");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const graph_query_mod = antfly.graph_query;

const harness_internal_service_secret = "graph-metric-process-harness-internal-service-secret";
const harness_internal_service_issuer = "graph-metric-process-harness";
// Scoped to main's lifetime; every child gets an owned environment snapshot.
var child_environ: ?*const std.process.Environ.Map = null;

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

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var environ = try init.environ_map.clone(alloc);
    defer environ.deinit();
    try environ.put("ANTFLY_INTERNAL_SERVICE_SECRET", harness_internal_service_secret);
    try environ.put("ANTFLY_INTERNAL_SERVICE_ISSUER", harness_internal_service_issuer);
    child_environ = &environ;
    defer child_environ = null;

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const argv = try init.minimal.args.toSlice(arena);
    if (argv.len >= 2 and std.mem.eql(u8, argv[1], "__maintenance-worker")) {
        var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
        defer args.deinit();
        _ = args.next();
        _ = args.next();
        return @import("maintenance_worker.zig").runFromIterator(init, argv[0], &args);
    }
    if (argv.len >= 2 and std.mem.eql(u8, argv[1], "claim-degree-page-hold")) {
        try runClaimDegreePageHoldMode(alloc, init.io, argv);
        return;
    }
    if (argv.len >= 2 and std.mem.eql(u8, argv[1], "claim-metric-page-hold")) {
        try runClaimMetricPageHoldMode(alloc, init.io, argv);
        return;
    }
    if (argv.len != 1) return error.InvalidArguments;
    const executable = argv[0];
    try verifyRoleProcessArgvPreflightSelfTest();
    const paths = [_][]const u8{
        ".zig-cache/tmp/maintenance-process-launch",
        ".zig-cache/tmp/maintenance-process-owner",
        ".zig-cache/tmp/maintenance-process-page",
        ".zig-cache/tmp/maintenance-process-service",
        ".zig-cache/tmp/maintenance-process-cleanup",
        ".zig-cache/tmp/maintenance-process-public-read",
    };
    for (paths) |path| std.Io.Dir.cwd().deleteTree(init.io, path) catch {};
    defer for (paths) |path| std.Io.Dir.cwd().deleteTree(init.io, path) catch {};

    // Native boundaries have one representative fixture each. Family/phase
    // correctness belongs to graph/storage tests and replayable VOPR histories.
    const launch_generation = try seedDegreeDb(alloc, paths[0]);
    try runLaunchProcess(alloc, init.io, executable, paths[0], "degree");
    try verifyDegreeFresh(alloc, paths[0], launch_generation);
    _ = try seedDegreeDb(alloc, paths[1]);
    try verifyCoordinatorLeaseExpiryTakeover(alloc, init.io, executable, paths[1]);
    const page_generation = try seedDegreeDbWithSources(alloc, paths[2], 130);
    try verifyWorkerPageLeaseReclaim(alloc, init.io, executable, executable, paths[2], page_generation);
    const service_generation = try seedDegreeDb(alloc, paths[3]);
    try verifyServiceTargetedMetricOwnerRestartProcess(alloc, init.io, executable, paths[3], "degree", service_generation);
    try verifyDegreeFresh(alloc, paths[3], service_generation);
    const cleanup_generation = try seedDegreeDb(alloc, paths[4]);
    try verifyDegreeServiceTargetedPublishAndCleanupRestartProcess(alloc, init.io, executable, paths[4], cleanup_generation);
    const read_generation = try seedDegreeSearchDb(alloc, paths[5]);
    try verifyDegreeServiceActiveProcessPublicReadFreshness(alloc, init.io, executable, paths[5], read_generation);
    std.debug.print("maintenance process integration: six native boundary checks passed\n", .{});
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

fn runClaimDegreePageHoldMode(
    alloc: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
) !void {
    if (argv.len != 7) {
        std.debug.print("usage: maintenance-process-tests claim-degree-page-hold <db-path> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
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
        std.debug.print("usage: maintenance-process-tests claim-metric-page-hold <db-path> <metric-name> <phase> <worker-id> <now-ms> <ready-file> <hold-ms>\n", .{});
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

fn assertDuplicateCoordinatorDidNotMutate(summary: RoleRunSummary, label: []const u8) !void {
    if (summary.result.published != 0 or summary.result.failed_builds != 0 or summary.result.phases_advanced != 0) {
        std.debug.print("expected duplicate coordinator process not to publish, fail, or advance {s}\n", .{label});
        return error.GraphMetricProcessProofFailed;
    }
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

fn runSupervisorProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    antfly_exe: []const u8,
    db_path: []const u8,
) !void {
    const argv = [_][]const u8{
        antfly_exe,
        "__maintenance-worker",
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
        .environ_map = child_environ,
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
        "__maintenance-worker",
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
        .environ_map = child_environ,
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
    logical_owner_id: []const u8,
    worker_count: usize,
    expect_worker_hash: bool,
) !void {
    if (telemetry.role != role) return error.SupervisorProcessFailed;
    const logical_owner_hash = std.hash.Wyhash.hash(0, logical_owner_id);
    if (telemetry.runtime_id_hash != logical_owner_hash) return error.SupervisorProcessFailed;
    // The command appends a process-incarnation fence to the logical owner ID.
    // The runtime identity remains stable for observability, while the lease
    // owner must be non-zero and distinct across process restarts.
    if (telemetry.owner_id_hash == 0 or telemetry.owner_id_hash == logical_owner_hash) return error.SupervisorProcessFailed;
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
            .{
                .internal_service_secret = harness_internal_service_secret,
                .internal_service_issuer = harness_internal_service_issuer,
            },
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
        "__maintenance-worker",
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
        .environ_map = child_environ,
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        .environ_map = child_environ,
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
        .environ_map = child_environ,
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
        "__maintenance-worker",
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
            "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
    const logical_owner_hash = identityHash(owner_id);
    if (stats.runtime_id_hash != logical_owner_hash) return error.GraphMetricRoleProcessFailed;
    if (stats.owner_id_hash == 0 or stats.owner_id_hash == logical_owner_hash) return error.GraphMetricRoleProcessFailed;
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
    const logical_owner_hash = identityHash(owner_id);
    if (stats.owner_id_hash == 0 or stats.owner_id_hash == logical_owner_hash) return error.GraphMetricRoleProcessFailed;
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

/// Summary leaves/root and ordinal shards add bounded checkpoints within a
/// phase. Drive those through real service processes, stopping at exactly the
/// next coordinator barrier so callers can still assert the next phase.
fn runRoleProcess(
    alloc: std.mem.Allocator,
    io: std.Io,
    argv: []const []const u8,
) !RoleRunSummary {
    try verifyRoleProcessArgvScoped(argv);
    const result = try std.process.run(alloc, io, .{
        .environ_map = child_environ,
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
    if (!std.mem.eql(u8, argv[1], "__maintenance-worker")) return error.GraphMetricRoleProcessFailed;
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
        "__maintenance-worker",
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
