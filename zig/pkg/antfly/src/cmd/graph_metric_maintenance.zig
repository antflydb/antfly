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
const antfly = @import("../cli_root.zig");
const platform = @import("antfly_platform");
const platform_clock = antfly.platform_clock;
const graph_metric_runtime_mod = antfly.db.graph_metric_runtime;
const graph_query_mod = antfly.graph_query;
const writer_lock_mod = antfly.lmdb_engine.writer_lock;

const RuntimeRole = graph_metric_runtime_mod.Role;
const SweepResult = antfly.db.IndexManager.GraphMetricPlannedSchedulerSweepResult;
const HttpQueryResponse = antfly.public_api.http_client.QueryResponse;
const local_db_writer_lock_retries: usize = 10_000;
const local_db_writer_lock_sleep_ms: u64 = 5;
const process_subcommand = "__graph-metric-maintenance";

const ExitReason = enum {
    max_ticks,
    idle,
};

const SupervisorExitReason = enum {
    idle,
    max_rounds,
    restart_limit,
};

const CliConfig = struct {
    db_path: ?[]const u8 = null,
    service_base_uri: ?[]const u8 = null,
    service_group_id: ?u64 = null,
    service_table_name: ?[]const u8 = null,
    role: RuntimeRole = .combined,
    runtime_id: []const u8 = "graph-metric-maintenance",
    owner_id: []const u8 = "",
    worker_id: []const u8 = "graph-metric-worker",
    worker_ids: std.ArrayListUnmanaged([]const u8) = .empty,
    lease_owned: bool = true,
    lease_ttl_ms: u64 = 30_000,
    coordinator_start_background_builds: bool = true,
    max_ticks: usize = 1,
    ticks_set: bool = false,
    until_idle: bool = false,
    max_idle_ticks: ?usize = null,
    tick_interval_ms: u64 = 0,
    max_rounds: usize = 1,
    max_metrics_per_round: usize = 8,
    max_pages_per_round: usize = 1,
    test_now_ms: ?u64 = null,
    test_ready_file: ?[]const u8 = null,
    test_hold_after_run_ms: u64 = 0,
    summary_file: ?[]const u8 = null,
    local_db_writer_lock: bool = false,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.worker_ids.deinit(alloc);
        self.* = undefined;
    }

    fn runtimeConfig(self: CliConfig, clock: platform_clock.Clock) graph_metric_runtime_mod.Config {
        return .{
            .enabled = true,
            .start_background_loop = false,
            .role = self.role,
            .runtime_id = self.runtime_id,
            .lease_owned = self.lease_owned,
            .owner_id = self.owner_id,
            .lease_ttl_ms = self.lease_ttl_ms,
            .coordinator_start_background_builds = self.coordinator_start_background_builds,
            .planned_options = .{
                .worker_id = self.worker_id,
                .worker_ids = self.worker_ids.items,
                .max_rounds = self.max_rounds,
                .max_metrics_per_round = self.max_metrics_per_round,
                .max_pages_per_round = self.max_pages_per_round,
            },
            .clock = clock,
        };
    }

    fn serviceTarget(self: CliConfig) !?ServiceTarget {
        const has_any = self.service_base_uri != null or self.service_group_id != null or self.service_table_name != null;
        if (!has_any) return null;
        if (self.service_base_uri == null or self.service_group_id == null or self.service_table_name == null) {
            return error.InvalidArguments;
        }
        return .{
            .base_uri = self.service_base_uri.?,
            .group_id = self.service_group_id.?,
            .table_name = self.service_table_name.?,
        };
    }
};

const ServiceTarget = struct {
    base_uri: []const u8,
    group_id: u64,
    table_name: []const u8,
};

const ServiceMaintenanceRequestWire = struct {
    action: ServiceMaintenanceAction = .tick,
    role: RuntimeRole,
    runtime_id: []const u8,
    owner_id: []const u8,
    lease_owned: bool = false,
    lease_ttl_ms: u64 = 30_000,
    worker_id: ?[]const u8 = null,
    worker_ids: ?[]const []const u8 = null,
    start_background_builds: bool = true,
    max_rounds: usize = 1,
    max_metrics_per_round: usize = 8,
    max_pages_per_round: usize = 1,
    preserve_lease_after_tick: bool = false,
    now_ms: ?u64 = null,
};

const ServiceMaintenanceAction = enum {
    tick,
    status,
    release,
};

const ServiceMaintenanceResponseWire = struct {
    result: SweepResult = .{},
    stats: ?graph_metric_runtime_mod.Stats = null,
    released: bool = false,
    lease_owner_id_hash: u64 = 0,
    lease_expires_at_ms: u64 = 0,
};

const ServiceBoundaryRequester = struct {
    ptr: *anyopaque,
    request: *const fn (*anyopaque, std.mem.Allocator, ServiceTarget, []const u8) anyerror!HttpQueryResponse,
};

fn ServiceRequestAdapter(
    comptime Context: type,
    comptime request: fn (*Context, std.mem.Allocator, ServiceTarget, []const u8) anyerror!HttpQueryResponse,
) type {
    return struct {
        fn call(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: ServiceTarget,
            body: []const u8,
        ) !HttpQueryResponse {
            const context: *Context = @ptrCast(@alignCast(ptr));
            return try request(context, alloc, target, body);
        }
    };
}

const ServiceMaintenanceBoundary = struct {
    alloc: std.mem.Allocator,
    target: ServiceTarget,
    base_cli: CliConfig,
    requester: ServiceBoundaryRequester,
    stats: *graph_metric_runtime_mod.Stats,
    last_response_had_stats: bool = false,

    fn boundary(self: *ServiceMaintenanceBoundary) graph_metric_runtime_mod.MaintenanceBoundary {
        return graph_metric_runtime_mod.MaintenanceBoundary.init(self, &service_boundary_vtable);
    }

    fn requestRole(
        self: *ServiceMaintenanceBoundary,
        role: RuntimeRole,
        worker_id: []const u8,
        worker_ids: []const []const u8,
        start_background_builds: bool,
        max_rounds: usize,
        max_metrics: usize,
        max_pages: usize,
        now_ms: ?u64,
    ) !SweepResult {
        const body = try serviceRequestJsonFromFieldsAlloc(
            self.alloc,
            self.base_cli,
            .tick,
            role,
            worker_id,
            worker_ids,
            start_background_builds,
            max_rounds,
            max_metrics,
            max_pages,
            now_ms,
        );
        defer self.alloc.free(body);

        var response = try self.requester.request(self.requester.ptr, self.alloc, self.target, body);
        defer response.deinit(self.alloc);
        const parsed = try parseServiceMaintenanceResponse(self.alloc, response.body);
        self.last_response_had_stats = parsed.stats != null;
        if (parsed.stats) |server_stats| {
            mergeServiceStats(self.stats, server_stats);
        }
        return parsed.result;
    }
};

const service_boundary_vtable = graph_metric_runtime_mod.MaintenanceBoundary.VTable{
    .run_combined = serviceBoundaryRunCombined,
    .run_coordinator = serviceBoundaryRunCoordinator,
    .run_worker = serviceBoundaryRunWorker,
    .run_worker_pool = serviceBoundaryRunWorkerPool,
};

fn serviceBoundaryContext(ptr: *anyopaque) *ServiceMaintenanceBoundary {
    return @ptrCast(@alignCast(ptr));
}

fn serviceBoundaryRunCombined(
    ptr: *anyopaque,
    options: antfly.db.IndexManager.GraphMetricPlannedMaintenanceOptions,
) !SweepResult {
    const self = serviceBoundaryContext(ptr);
    return try self.requestRole(
        .combined,
        options.worker_id,
        options.worker_ids,
        true,
        options.max_rounds,
        options.max_metrics_per_round,
        options.max_pages_per_round,
        options.now_ms,
    );
}

fn serviceBoundaryRunCoordinator(
    ptr: *anyopaque,
    options: antfly.db.IndexManager.GraphMetricPlannedSchedulerSweepOptions,
) !SweepResult {
    const self = serviceBoundaryContext(ptr);
    return try self.requestRole(
        .coordinator,
        self.base_cli.worker_id,
        &.{},
        options.start_background_builds,
        self.base_cli.max_rounds,
        options.max_metrics,
        self.base_cli.max_pages_per_round,
        options.now_ms,
    );
}

fn serviceBoundaryRunWorker(
    ptr: *anyopaque,
    options: antfly.db.IndexManager.GraphMetricPlannedWorkerSweepOptions,
) !SweepResult {
    const self = serviceBoundaryContext(ptr);
    return try self.requestRole(
        .worker,
        options.worker_id,
        &.{},
        false,
        self.base_cli.max_rounds,
        self.base_cli.max_metrics_per_round,
        options.max_pages,
        options.now_ms,
    );
}

fn serviceBoundaryRunWorkerPool(
    ptr: *anyopaque,
    options: graph_metric_runtime_mod.MaintenanceBoundary.WorkerPoolSweepOptions,
) !SweepResult {
    const self = serviceBoundaryContext(ptr);
    return try self.requestRole(
        .worker_pool,
        options.worker_id,
        options.worker_ids,
        false,
        self.base_cli.max_rounds,
        self.base_cli.max_metrics_per_round,
        options.max_pages,
        options.now_ms,
    );
}

const RunSummary = struct {
    role: RuntimeRole,
    ticks_executed: usize,
    idle_streak: usize,
    exit_reason: ExitReason,
    durable_progressed: bool,
    result: SweepResult,
    stats: graph_metric_runtime_mod.Stats,
};

const SupervisorConfig = struct {
    db_path: ?[]const u8 = null,
    service_base_uri: ?[]const u8 = null,
    service_group_id: ?u64 = null,
    service_table_name: ?[]const u8 = null,
    executable: ?[]const u8 = null,
    coordinator_owner_id: []const u8 = "graph-metric-coordinator",
    worker_pool_owner_id: []const u8 = "graph-metric-worker-pool",
    worker_ids: std.ArrayListUnmanaged([]const u8) = .empty,
    lease_ttl_ms: u64 = 30_000,
    max_ticks: usize = 1024,
    max_idle_ticks: usize = 8,
    max_supervisor_rounds: usize = 1024,
    max_supervisor_idle_rounds: usize = 1,
    tick_interval_ms: u64 = 25,
    max_restarts: usize = 1,
    max_rounds: usize = 1,
    max_metrics_per_round: usize = 8,
    max_pages_per_round: usize = 2,
    summary_dir: []const u8 = ".zig-cache/tmp",
    help: bool = false,

    fn deinit(self: *SupervisorConfig, alloc: std.mem.Allocator) void {
        self.worker_ids.deinit(alloc);
        self.* = undefined;
    }

    fn ensureDefaultWorkers(self: *SupervisorConfig, alloc: std.mem.Allocator) !void {
        if (self.worker_ids.items.len != 0) return;
        try self.worker_ids.append(alloc, "graph-metric-worker-a");
        try self.worker_ids.append(alloc, "graph-metric-worker-b");
    }

    fn serviceTarget(self: SupervisorConfig) !?ServiceTarget {
        const has_any = self.service_base_uri != null or self.service_group_id != null or self.service_table_name != null;
        if (!has_any) return null;
        if (self.service_base_uri == null or self.service_group_id == null or self.service_table_name == null) {
            return error.InvalidArguments;
        }
        return .{
            .base_uri = self.service_base_uri.?,
            .group_id = self.service_group_id.?,
            .table_name = self.service_table_name.?,
        };
    }

    fn target(self: SupervisorConfig) !?SupervisorTarget {
        const service_target = try self.serviceTarget();
        if (self.db_path != null and service_target != null) return error.InvalidArguments;
        if (service_target) |target_value| return .{ .service = target_value };
        if (self.db_path) |db_path| return .{ .db_path = db_path };
        return null;
    }
};

const SupervisorTarget = union(enum) {
    db_path: []const u8,
    service: ServiceTarget,
};

const ChildRole = enum {
    coordinator,
    worker_pool,
};

const ChildExitSummary = struct {
    exited: bool,
    code: ?u8,
};

const ChildRuntimeTelemetry = struct {
    role: RuntimeRole,
    runtime_id_hash: u64,
    owner_id_hash: u64,
    lease_key_hash: u64,
    worker_id_hash: u64,
    worker_count: usize,
    lease_owned: bool,
    has_lease: bool,
    acquisition_count: u64,
    takeover_count: u64,
    lost_leases: u64,
    ticks_started: u64,
    ticks_completed: u64,
    idle_ticks: u64,
    error_ticks: u64,
    has_last_error: bool,
};

const ChildRunSummary = struct {
    exit: ChildExitSummary,
    durable_progressed: ?bool,
    telemetry: ?ChildRuntimeTelemetry = null,
    stdout_bytes: usize,
    stderr_bytes: usize,
};

const SupervisorSummary = struct {
    rounds_executed: usize,
    restarts: usize,
    idle_rounds: usize,
    exit_reason: SupervisorExitReason,
    succeeded: bool,
    coordinator: ChildRunSummary,
    worker_pool: ChildRunSummary,
};

const ChildArgv = struct {
    argv: std.ArrayListUnmanaged([]const u8) = .empty,
    owned: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *ChildArgv, alloc: std.mem.Allocator) void {
        for (self.owned.items) |item| alloc.free(item);
        self.owned.deinit(alloc);
        self.argv.deinit(alloc);
        self.* = undefined;
    }

    fn append(self: *ChildArgv, alloc: std.mem.Allocator, value: []const u8) !void {
        try self.argv.append(alloc, value);
    }

    fn appendOwned(self: *ChildArgv, alloc: std.mem.Allocator, comptime fmt: []const u8, args: anytype) !void {
        const value = try std.fmt.allocPrint(alloc, fmt, args);
        errdefer alloc.free(value);
        try self.owned.append(alloc, value);
        try self.argv.append(alloc, value);
    }
};

pub fn run(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const alloc = init.gpa;
    const first = args.next();
    if (first) |arg| {
        if (std.mem.eql(u8, arg, "supervise")) {
            var supervisor = try parseSupervisorCli(alloc, args);
            defer supervisor.deinit(alloc);
            if (supervisor.help) {
                printSupervisorUsage(argv0);
                return;
            }
            const target = (try supervisor.target()) orelse return error.InvalidArguments;
            const summary = try runSupervisorConfigured(init.io, alloc, argv0, target, supervisor);
            try writeJson(init.io, alloc, summary);
            return;
        }
        if (std.mem.eql(u8, arg, "launch")) {
            var launcher = try parseSupervisorCli(alloc, args);
            defer launcher.deinit(alloc);
            if (launcher.help) {
                printLaunchUsage(argv0);
                return;
            }
            const target = (try launcher.target()) orelse return error.InvalidArguments;
            const summary = try runLaunchedConfigured(init.io, alloc, argv0, target, launcher);
            try writeJson(init.io, alloc, summary);
            return;
        }
    }

    var cli = try parseCliWithFirst(alloc, args, first);
    defer cli.deinit(alloc);
    if (cli.help) {
        printUsage(argv0);
        return;
    }
    const summary = if (try cli.serviceTarget()) |target| blk: {
        if (cli.db_path != null) return error.InvalidArguments;
        break :blk try runServiceConfigured(alloc, target, cli);
    } else blk: {
        const db_path = cli.db_path orelse return error.InvalidArguments;
        break :blk try runConfigured(alloc, db_path, cli);
    };
    try writeJson(init.io, alloc, summary);
    if (cli.summary_file) |path| {
        try writeJsonFile(init.io, alloc, path, summary);
    }
}

fn runConfigured(alloc: std.mem.Allocator, db_path: []const u8, cli: CliConfig) !RunSummary {
    var local_db_writer_lock = if (cli.local_db_writer_lock)
        try acquireLocalDbWriterLock(alloc, db_path)
    else
        null;
    defer if (local_db_writer_lock) |*lock| lock.release();

    var manual_clock = platform_clock.ManualClock{};
    if (cli.test_now_ms) |now_ms| manual_clock.setRealtimeNs(now_ms * std.time.ns_per_ms);
    const clock = if (cli.test_now_ms != null) manual_clock.clock() else platform_clock.Clock.real();
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .writer_no_replay,
        .ttl_cleanup = .{ .enabled = false },
        .graph_metric_maintenance = cli.runtimeConfig(clock),
    });
    defer db.close();

    const runtime = db.graph_metric_runtime orelse return error.GraphMetricRuntimeNotInitialized;
    var total = SweepResult{};
    var ticks_executed: usize = 0;
    var idle_streak: usize = 0;
    var exit_reason: ExitReason = .max_ticks;
    while (ticks_executed < cli.max_ticks) {
        const tick = try runtime.runOnceDetailed();
        total.add(tick);
        ticks_executed += 1;
        if (tick.durableProgressed()) {
            idle_streak = 0;
        } else {
            idle_streak += 1;
            if (cli.max_idle_ticks) |max_idle_ticks| {
                if (idle_streak >= max_idle_ticks) {
                    exit_reason = .idle;
                    break;
                }
            }
        }
        if (ticks_executed < cli.max_ticks and cli.tick_interval_ms > 0) {
            platform.time.sleepNs(cli.tick_interval_ms * std.time.ns_per_ms);
        }
    }

    if (cli.test_ready_file) |path| {
        writeReadyFile(path);
    }
    if (cli.test_hold_after_run_ms > 0) {
        platform.time.sleepNs(cli.test_hold_after_run_ms * std.time.ns_per_ms);
    }

    return .{
        .role = cli.role,
        .ticks_executed = ticks_executed,
        .idle_streak = idle_streak,
        .exit_reason = exit_reason,
        .durable_progressed = total.durableProgressed(),
        .result = total,
        .stats = runtime.stats(),
    };
}

const RealServiceMaintenanceClient = struct {
    client: *antfly.public_api.ApiHttpClient,

    fn request(
        self: *RealServiceMaintenanceClient,
        alloc: std.mem.Allocator,
        target: ServiceTarget,
        body: []const u8,
    ) !HttpQueryResponse {
        _ = alloc;
        return try self.client.fetchGroupGraphMetricMaintenance(target.base_uri, target.group_id, target.table_name, body);
    }
};

fn runServiceConfigured(alloc: std.mem.Allocator, target: ServiceTarget, cli: CliConfig) !RunSummary {
    var executor = antfly.raft.transport.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = antfly.public_api.ApiHttpClient.init(alloc, executor.executor());
    var context = RealServiceMaintenanceClient{ .client = &client };
    return try runServiceConfiguredWithRequester(
        RealServiceMaintenanceClient,
        &context,
        RealServiceMaintenanceClient.request,
        alloc,
        target,
        cli,
    );
}

fn runServiceConfiguredWithRequester(
    comptime Context: type,
    context: *Context,
    comptime request: fn (*Context, std.mem.Allocator, ServiceTarget, []const u8) anyerror!HttpQueryResponse,
    alloc: std.mem.Allocator,
    target: ServiceTarget,
    cli: CliConfig,
) !RunSummary {
    var total = SweepResult{};
    var stats = try serviceInitialStats(alloc, cli);
    const RequestAdapter = ServiceRequestAdapter(Context, request);
    var boundary_context = ServiceMaintenanceBoundary{
        .alloc = alloc,
        .target = target,
        .base_cli = cli,
        .requester = .{
            .ptr = context,
            .request = RequestAdapter.call,
        },
        .stats = &stats,
    };
    const boundary = boundary_context.boundary();
    var manual_clock = platform_clock.ManualClock{};
    if (cli.test_now_ms) |now_ms| manual_clock.setRealtimeNs(now_ms * std.time.ns_per_ms);
    const clock = if (cli.test_now_ms != null) manual_clock.clock() else platform_clock.Clock.real();
    const runtime_config = cli.runtimeConfig(clock);
    var ticks_executed: usize = 0;
    var idle_streak: usize = 0;
    var exit_reason: ExitReason = .max_ticks;
    while (ticks_executed < cli.max_ticks) {
        stats.ticks_started += 1;
        boundary_context.last_response_had_stats = false;
        const tick = graph_metric_runtime_mod.runBoundaryTick(
            boundary,
            runtime_config,
            clock.nowRealtimeMs(),
        ) catch |err| {
            stats.error_ticks += 1;
            stats.last_error_name = @errorName(err);
            return err;
        };
        stats.ticks_completed += 1;
        if (!boundary_context.last_response_had_stats) {
            stats.last_result = tick;
            stats.total_result.add(tick);
        }
        total.add(tick);
        ticks_executed += 1;
        if (!boundary_context.last_response_had_stats and tick.durableProgressed()) {
            stats.durable_progress_ticks += 1;
            idle_streak = 0;
        } else if (boundary_context.last_response_had_stats and tick.durableProgressed()) {
            idle_streak = 0;
        } else {
            if (!boundary_context.last_response_had_stats) stats.idle_ticks += 1;
            idle_streak += 1;
            if (cli.max_idle_ticks) |max_idle_ticks| {
                if (idle_streak >= max_idle_ticks) {
                    exit_reason = .idle;
                    break;
                }
            }
        }
        if (ticks_executed < cli.max_ticks and cli.tick_interval_ms > 0) {
            platform.time.sleepNs(cli.tick_interval_ms * std.time.ns_per_ms);
        }
    }

    if (cli.test_ready_file) |path| {
        writeReadyFile(path);
    }
    if (cli.test_hold_after_run_ms > 0) {
        platform.time.sleepNs(cli.test_hold_after_run_ms * std.time.ns_per_ms);
    }

    if (cli.lease_owned) {
        try releaseServiceRuntimeOwner(Context, context, request, alloc, target, cli, &stats);
    }

    return .{
        .role = cli.role,
        .ticks_executed = ticks_executed,
        .idle_streak = idle_streak,
        .exit_reason = exit_reason,
        .durable_progressed = total.durableProgressed(),
        .result = total,
        .stats = stats,
    };
}

fn serviceRequestJsonAlloc(alloc: std.mem.Allocator, cli: CliConfig) ![]u8 {
    return try serviceRequestJsonFromFieldsAlloc(
        alloc,
        cli,
        .tick,
        cli.role,
        cli.worker_id,
        cli.worker_ids.items,
        cli.coordinator_start_background_builds,
        cli.max_rounds,
        cli.max_metrics_per_round,
        cli.max_pages_per_round,
        cli.test_now_ms,
    );
}

fn serviceRequestJsonFromFieldsAlloc(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    action: ServiceMaintenanceAction,
    role: RuntimeRole,
    worker_id: []const u8,
    worker_ids_slice: []const []const u8,
    start_background_builds: bool,
    max_rounds: usize,
    max_metrics_per_round: usize,
    max_pages_per_round: usize,
    now_ms: ?u64,
) ![]u8 {
    const request_worker_ids: ?[]const []const u8 = if (worker_ids_slice.len == 0) null else worker_ids_slice;
    return try std.json.Stringify.valueAlloc(alloc, ServiceMaintenanceRequestWire{
        .action = action,
        .role = role,
        .runtime_id = cli.runtime_id,
        .owner_id = if (cli.owner_id.len != 0) cli.owner_id else cli.runtime_id,
        .lease_owned = cli.lease_owned,
        .lease_ttl_ms = cli.lease_ttl_ms,
        .worker_id = worker_id,
        .worker_ids = request_worker_ids,
        .start_background_builds = start_background_builds,
        .max_rounds = max_rounds,
        .max_metrics_per_round = max_metrics_per_round,
        .max_pages_per_round = max_pages_per_round,
        .preserve_lease_after_tick = action == .tick and cli.test_hold_after_run_ms > 0,
        .now_ms = now_ms,
    }, .{});
}

fn releaseServiceRuntimeOwner(
    comptime Context: type,
    context: *Context,
    comptime request: fn (*Context, std.mem.Allocator, ServiceTarget, []const u8) anyerror!HttpQueryResponse,
    alloc: std.mem.Allocator,
    target: ServiceTarget,
    cli: CliConfig,
    stats: *graph_metric_runtime_mod.Stats,
) !void {
    const body = try serviceRequestJsonFromFieldsAlloc(
        alloc,
        cli,
        .release,
        cli.role,
        cli.worker_id,
        cli.worker_ids.items,
        cli.coordinator_start_background_builds,
        cli.max_rounds,
        cli.max_metrics_per_round,
        cli.max_pages_per_round,
        cli.test_now_ms,
    );
    defer alloc.free(body);

    var response = request(context, alloc, target, body) catch |err| {
        stats.error_ticks += 1;
        stats.last_error_name = @errorName(err);
        return err;
    };
    defer response.deinit(alloc);
    const parsed = parseServiceMaintenanceResponse(alloc, response.body) catch |err| {
        stats.error_ticks += 1;
        stats.last_error_name = @errorName(err);
        return err;
    };
    if (parsed.stats) |server_stats| {
        stats.lease_owned = server_stats.lease_owned;
        stats.has_lease = server_stats.has_lease;
        stats.last_acquired_ms = server_stats.last_acquired_ms;
    } else if (parsed.released) {
        stats.has_lease = false;
    }
    stats.shutdown = true;
}

fn parseServiceMaintenanceResponse(alloc: std.mem.Allocator, body: []const u8) !ServiceMaintenanceResponseWire {
    var parsed_value = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed_value.deinit();
    if (parsed_value.value == .object and
        (parsed_value.value.object.get("result") != null or
            parsed_value.value.object.get("stats") != null or
            parsed_value.value.object.get("released") != null))
    {
        var parsed = try std.json.parseFromSlice(ServiceMaintenanceResponseWire, alloc, body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        return parsed.value;
    }
    var parsed = try std.json.parseFromSlice(SweepResult, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return .{ .result = parsed.value, .stats = null };
}

fn mergeServiceStats(
    stats: *graph_metric_runtime_mod.Stats,
    server_stats: graph_metric_runtime_mod.Stats,
) void {
    stats.enabled = server_stats.enabled;
    stats.role = server_stats.role;
    stats.runtime_id_hash = server_stats.runtime_id_hash;
    stats.owner_id_hash = server_stats.owner_id_hash;
    stats.lease_key_hash = server_stats.lease_key_hash;
    stats.worker_id_hash = server_stats.worker_id_hash;
    stats.worker_count = server_stats.worker_count;
    stats.lease_owned = server_stats.lease_owned;
    stats.has_lease = server_stats.has_lease;
    stats.acquisition_count += server_stats.acquisition_count;
    stats.takeover_count += server_stats.takeover_count;
    stats.lease_acquire_failures += server_stats.lease_acquire_failures;
    stats.lost_leases += server_stats.lost_leases;
    stats.last_acquired_ms = server_stats.last_acquired_ms;
    stats.durable_progress_ticks += server_stats.durable_progress_ticks;
    stats.idle_ticks += server_stats.idle_ticks;
    stats.error_ticks += server_stats.error_ticks;
    stats.last_error_name = server_stats.last_error_name;
    stats.last_result = server_stats.last_result;
    stats.total_result.add(server_stats.total_result);
}

fn serviceInitialStats(alloc: std.mem.Allocator, cli: CliConfig) !graph_metric_runtime_mod.Stats {
    const worker_hash = serviceWorkerIdentityHash(cli);
    const lease_key_hash = try serviceRuntimeLeaseKeyHash(alloc, cli, worker_hash);
    return .{
        .enabled = true,
        .role = cli.role,
        .runtime_id_hash = identityHash(cli.runtime_id),
        .owner_id_hash = identityHash(if (cli.owner_id.len != 0) cli.owner_id else cli.runtime_id),
        .lease_key_hash = lease_key_hash,
        .worker_id_hash = worker_hash,
        .worker_count = configuredWorkerCount(cli),
        .lease_owned = cli.lease_owned,
        .has_lease = !cli.lease_owned,
    };
}

fn identityHash(value: []const u8) u64 {
    if (value.len == 0) return 0;
    return std.hash.Wyhash.hash(0, value);
}

fn configuredWorkerCount(cli: CliConfig) usize {
    if (cli.role == .coordinator) return 0;
    if (cli.worker_ids.items.len != 0) return cli.worker_ids.items.len;
    return if (cli.worker_id.len == 0) 0 else 1;
}

fn serviceWorkerIdentityHash(cli: CliConfig) u64 {
    if (cli.role == .coordinator) return 0;
    if (cli.worker_ids.items.len == 0) return identityHash(cli.worker_id);
    var xor_hash: u64 = 0;
    var sum_hash: u64 = 0;
    for (cli.worker_ids.items) |worker_id| {
        const item_hash = identityHash(worker_id);
        xor_hash ^= item_hash;
        sum_hash +%= item_hash;
    }
    const fingerprint_words = [_]u64{
        @intCast(cli.worker_ids.items.len),
        xor_hash,
        sum_hash,
    };
    return std.hash.Wyhash.hash(0, std.mem.asBytes(&fingerprint_words));
}

fn serviceRuntimeLeaseKeyHash(alloc: std.mem.Allocator, cli: CliConfig, worker_hash: u64) !u64 {
    const base_key = graph_metric_runtime_mod.defaultLeaseKey(cli.role);
    switch (cli.role) {
        .combined, .coordinator => return identityHash(base_key),
        .worker, .worker_pool => {
            if (configuredWorkerCount(cli) == 0) return identityHash(base_key);
            const lease_key = try std.fmt.allocPrint(alloc, "{s}:{x}", .{ base_key, worker_hash });
            defer alloc.free(lease_key);
            return identityHash(lease_key);
        },
    }
}

fn parseCli(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !CliConfig {
    return parseCliWithFirst(alloc, args, null);
}

fn parseCliWithFirst(alloc: std.mem.Allocator, args: *std.process.Args.Iterator, first_arg: ?[]const u8) !CliConfig {
    var cfg = CliConfig{};
    errdefer cfg.deinit(alloc);
    var pending_arg = first_arg;
    while (true) {
        const arg = blk: {
            if (pending_arg) |value| {
                pending_arg = null;
                break :blk value;
            }
            break :blk args.next() orelse break;
        };
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--db-path")) {
            cfg.db_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--base-uri") or std.mem.eql(u8, arg, "--service-base-uri")) {
            cfg.service_base_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--group-id")) {
            cfg.service_group_id = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--table-name")) {
            cfg.service_table_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--base-uri") or std.mem.eql(u8, arg, "--service-base-uri")) {
            cfg.service_base_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--group-id")) {
            cfg.service_group_id = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--table-name")) {
            cfg.service_table_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--role")) {
            cfg.role = try parseRole(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--runtime-id")) {
            cfg.runtime_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--owner-id")) {
            cfg.owner_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--worker-id")) {
            cfg.worker_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--worker-ids")) {
            try parseWorkerIds(alloc, &cfg.worker_ids, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--lease-owned")) {
            cfg.lease_owned = try parseBool(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--lease-ttl-ms")) {
            cfg.lease_ttl_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--coordinator-start-background-builds")) {
            cfg.coordinator_start_background_builds = try parseBool(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ticks")) {
            cfg.max_ticks = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            cfg.ticks_set = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--until-idle")) {
            cfg.until_idle = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-idle-ticks")) {
            cfg.max_idle_ticks = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--tick-ms")) {
            cfg.tick_interval_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-rounds")) {
            cfg.max_rounds = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-metrics")) {
            cfg.max_metrics_per_round = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-pages")) {
            cfg.max_pages_per_round = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--test-now-ms")) {
            cfg.test_now_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--test-ready-file")) {
            cfg.test_ready_file = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--test-hold-after-run-ms")) {
            cfg.test_hold_after_run_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--summary-file")) {
            cfg.summary_file = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--local-db-writer-lock")) {
            cfg.local_db_writer_lock = try parseBool(args.next() orelse return error.InvalidArguments);
            continue;
        }
        return error.InvalidArguments;
    }
    if (cfg.until_idle and cfg.max_idle_ticks == null) cfg.max_idle_ticks = 1;
    if (cfg.until_idle and !cfg.ticks_set) cfg.max_ticks = 1024;
    if (cfg.max_ticks == 0) return error.InvalidArguments;
    if (cfg.max_idle_ticks != null and cfg.max_idle_ticks.? == 0) return error.InvalidArguments;
    if (cfg.lease_ttl_ms == 0) return error.InvalidArguments;
    if (cfg.max_rounds == 0) return error.InvalidArguments;
    if (cfg.max_metrics_per_round == 0) return error.InvalidArguments;
    if (cfg.max_pages_per_round == 0) return error.InvalidArguments;
    if ((cfg.role == .coordinator or cfg.role == .worker) and cfg.worker_ids.items.len != 0) {
        return error.InvalidArguments;
    }
    const service_target = try cfg.serviceTarget();
    if (cfg.db_path != null and service_target != null) return error.InvalidArguments;
    return cfg;
}

fn writeReadyFile(path: []const u8) void {
    std.Io.Dir.cwd().writeFile(std.Io.Threaded.global_single_threaded.io(), .{
        .sub_path = path,
        .data = "ready\n",
    }) catch {};
}

fn parseSupervisorCli(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !SupervisorConfig {
    var cfg = SupervisorConfig{};
    errdefer cfg.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--db-path")) {
            cfg.db_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--executable")) {
            cfg.executable = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--base-uri") or std.mem.eql(u8, arg, "--service-base-uri")) {
            cfg.service_base_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--group-id")) {
            cfg.service_group_id = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--table-name")) {
            cfg.service_table_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--coordinator-owner-id")) {
            cfg.coordinator_owner_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--worker-pool-owner-id")) {
            cfg.worker_pool_owner_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--worker-ids")) {
            try parseWorkerIds(alloc, &cfg.worker_ids, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--lease-ttl-ms")) {
            cfg.lease_ttl_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ticks")) {
            cfg.max_ticks = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-idle-ticks")) {
            cfg.max_idle_ticks = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--supervisor-rounds")) {
            cfg.max_supervisor_rounds = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--supervisor-idle-rounds")) {
            cfg.max_supervisor_idle_rounds = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--tick-ms")) {
            cfg.tick_interval_ms = try parseInt(u64, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-restarts")) {
            cfg.max_restarts = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-rounds")) {
            cfg.max_rounds = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-metrics")) {
            cfg.max_metrics_per_round = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-pages")) {
            cfg.max_pages_per_round = try parseInt(usize, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--summary-dir")) {
            cfg.summary_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        return error.InvalidArguments;
    }

    if (cfg.max_ticks == 0) return error.InvalidArguments;
    if (cfg.max_idle_ticks == 0) return error.InvalidArguments;
    if (cfg.max_supervisor_rounds == 0) return error.InvalidArguments;
    if (cfg.max_supervisor_idle_rounds == 0) return error.InvalidArguments;
    if (cfg.lease_ttl_ms == 0) return error.InvalidArguments;
    if (cfg.max_rounds == 0) return error.InvalidArguments;
    if (cfg.max_metrics_per_round == 0) return error.InvalidArguments;
    if (cfg.max_pages_per_round == 0) return error.InvalidArguments;
    _ = try cfg.target();
    try cfg.ensureDefaultWorkers(alloc);
    return cfg;
}

fn parseRole(raw: []const u8) !RuntimeRole {
    if (std.mem.eql(u8, raw, "combined")) return .combined;
    if (std.mem.eql(u8, raw, "coordinator")) return .coordinator;
    if (std.mem.eql(u8, raw, "worker")) return .worker;
    if (std.mem.eql(u8, raw, "worker_pool") or std.mem.eql(u8, raw, "worker-pool")) return .worker_pool;
    return error.InvalidArguments;
}

fn parseBool(raw: []const u8) !bool {
    if (std.mem.eql(u8, raw, "true") or std.mem.eql(u8, raw, "1") or std.mem.eql(u8, raw, "yes")) return true;
    if (std.mem.eql(u8, raw, "false") or std.mem.eql(u8, raw, "0") or std.mem.eql(u8, raw, "no")) return false;
    return error.InvalidArguments;
}

fn parseInt(comptime T: type, raw: []const u8) !T {
    return std.fmt.parseInt(T, raw, 10) catch return error.InvalidArguments;
}

fn parseWorkerIds(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    raw: []const u8,
) !void {
    out.clearRetainingCapacity();
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |worker_id| {
        if (worker_id.len == 0) return error.InvalidArguments;
        for (out.items) |prior_worker_id| {
            if (std.mem.eql(u8, worker_id, prior_worker_id)) return error.InvalidArguments;
        }
        try out.append(alloc, worker_id);
    }
    if (out.items.len == 0) return error.InvalidArguments;
}

fn runSupervisorConfigured(
    io: std.Io,
    alloc: std.mem.Allocator,
    argv0: []const u8,
    target: SupervisorTarget,
    cfg: SupervisorConfig,
) !SupervisorSummary {
    var context = RealSupervisorChildRunner{};
    return runSupervisorConfiguredWithRunner(
        RealSupervisorChildRunner,
        &context,
        RealSupervisorChildRunner.run,
        io,
        alloc,
        argv0,
        target,
        cfg,
    );
}

fn runLaunchedConfigured(
    io: std.Io,
    alloc: std.mem.Allocator,
    argv0: []const u8,
    target: SupervisorTarget,
    cfg: SupervisorConfig,
) !SupervisorSummary {
    try std.Io.Dir.cwd().createDirPath(io, cfg.summary_dir);

    var rounds_executed: usize = 0;
    var restarts: usize = 0;
    var idle_rounds: usize = 0;
    var coordinator = ChildRunSummary{
        .exit = .{ .exited = false, .code = null },
        .durable_progressed = null,
        .telemetry = null,
        .stdout_bytes = 0,
        .stderr_bytes = 0,
    };
    var worker_pool = coordinator;

    while (rounds_executed < cfg.max_supervisor_rounds) {
        rounds_executed += 1;
        const coordinator_summary_path = try launchSummaryPathAlloc(alloc, cfg.summary_dir, .coordinator, rounds_executed);
        defer alloc.free(coordinator_summary_path);
        const worker_pool_summary_path = try launchSummaryPathAlloc(alloc, cfg.summary_dir, .worker_pool, rounds_executed);
        defer alloc.free(worker_pool_summary_path);
        const coordinator_stderr_path = try launchStderrPathAlloc(alloc, cfg.summary_dir, .coordinator, rounds_executed);
        defer alloc.free(coordinator_stderr_path);
        const worker_pool_stderr_path = try launchStderrPathAlloc(alloc, cfg.summary_dir, .worker_pool, rounds_executed);
        defer alloc.free(worker_pool_stderr_path);
        std.Io.Dir.cwd().deleteFile(io, coordinator_summary_path) catch {};
        std.Io.Dir.cwd().deleteFile(io, worker_pool_summary_path) catch {};
        std.Io.Dir.cwd().deleteFile(io, coordinator_stderr_path) catch {};
        std.Io.Dir.cwd().deleteFile(io, worker_pool_stderr_path) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_summary_path) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, worker_pool_summary_path) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, coordinator_stderr_path) catch {};
        defer std.Io.Dir.cwd().deleteFile(io, worker_pool_stderr_path) catch {};

        var coordinator_argv = try buildSupervisorChildArgv(alloc, argv0, target, cfg, .coordinator, coordinator_summary_path);
        defer coordinator_argv.deinit(alloc);
        var worker_pool_argv = try buildSupervisorChildArgv(alloc, argv0, target, cfg, .worker_pool, worker_pool_summary_path);
        defer worker_pool_argv.deinit(alloc);

        var coordinator_stderr = try std.Io.Dir.cwd().createFile(io, coordinator_stderr_path, .{ .truncate = true });
        defer coordinator_stderr.close(io);
        var worker_pool_stderr = try std.Io.Dir.cwd().createFile(io, worker_pool_stderr_path, .{ .truncate = true });
        defer worker_pool_stderr.close(io);

        var coordinator_child = try std.process.spawn(io, .{
            .argv = coordinator_argv.argv.items,
            .stdin = .ignore,
            .stdout = .ignore,
            .stderr = .{ .file = coordinator_stderr },
        });
        errdefer coordinator_child.kill(io);
        var worker_pool_child = try std.process.spawn(io, .{
            .argv = worker_pool_argv.argv.items,
            .stdin = .ignore,
            .stdout = .ignore,
            .stderr = .{ .file = worker_pool_stderr },
        });
        errdefer worker_pool_child.kill(io);

        const coordinator_term = try coordinator_child.wait(io);
        const worker_pool_term = try worker_pool_child.wait(io);
        coordinator = try childRunSummaryFromFile(alloc, io, coordinator_term, coordinator_summary_path, coordinator_stderr_path);
        worker_pool = try childRunSummaryFromFile(alloc, io, worker_pool_term, worker_pool_summary_path, worker_pool_stderr_path);

        const failed = !childRunSucceeded(coordinator) or !childRunSucceeded(worker_pool);
        if (failed) {
            reportLaunchChildFailure(alloc, io, "coordinator", coordinator, coordinator_stderr_path);
            reportLaunchChildFailure(alloc, io, "worker_pool", worker_pool, worker_pool_stderr_path);
            if (!shouldRestartSupervisorAttempt(true, restarts, cfg.max_restarts)) {
                return .{
                    .rounds_executed = rounds_executed,
                    .restarts = restarts,
                    .idle_rounds = idle_rounds,
                    .exit_reason = .restart_limit,
                    .succeeded = false,
                    .coordinator = coordinator,
                    .worker_pool = worker_pool,
                };
            }
            restarts += 1;
            idle_rounds = 0;
            continue;
        }

        const progressed = childRunDurableProgressed(coordinator) or childRunDurableProgressed(worker_pool);
        if (progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= cfg.max_supervisor_idle_rounds) {
                return .{
                    .rounds_executed = rounds_executed,
                    .restarts = restarts,
                    .idle_rounds = idle_rounds,
                    .exit_reason = .idle,
                    .succeeded = true,
                    .coordinator = coordinator,
                    .worker_pool = worker_pool,
                };
            }
        }
        if (cfg.tick_interval_ms > 0) {
            platform.time.sleepNs(cfg.tick_interval_ms * std.time.ns_per_ms);
        }
    }

    return .{
        .rounds_executed = rounds_executed,
        .restarts = restarts,
        .idle_rounds = idle_rounds,
        .exit_reason = .max_rounds,
        .succeeded = false,
        .coordinator = coordinator,
        .worker_pool = worker_pool,
    };
}

fn runSupervisorConfiguredWithRunner(
    comptime Context: type,
    context: *Context,
    comptime runChild: fn (*Context, std.mem.Allocator, std.Io, []const []const u8) anyerror!ChildRunSummary,
    io: std.Io,
    alloc: std.mem.Allocator,
    argv0: []const u8,
    target: SupervisorTarget,
    cfg: SupervisorConfig,
) !SupervisorSummary {
    var rounds_executed: usize = 0;
    var restarts: usize = 0;
    var idle_rounds: usize = 0;
    var coordinator = ChildRunSummary{
        .exit = .{ .exited = false, .code = null },
        .durable_progressed = null,
        .telemetry = null,
        .stdout_bytes = 0,
        .stderr_bytes = 0,
    };
    var worker_pool = coordinator;

    while (rounds_executed < cfg.max_supervisor_rounds) {
        rounds_executed += 1;
        var coordinator_argv = try buildSupervisorChildArgv(alloc, argv0, target, cfg, .coordinator, null);
        defer coordinator_argv.deinit(alloc);
        var worker_pool_argv = try buildSupervisorChildArgv(alloc, argv0, target, cfg, .worker_pool, null);
        defer worker_pool_argv.deinit(alloc);

        coordinator = try runChild(context, alloc, io, coordinator_argv.argv.items);
        worker_pool = try runChild(context, alloc, io, worker_pool_argv.argv.items);
        const failed = !childRunSucceeded(coordinator) or !childRunSucceeded(worker_pool);
        if (failed) {
            if (!shouldRestartSupervisorAttempt(true, restarts, cfg.max_restarts)) {
                return .{
                    .rounds_executed = rounds_executed,
                    .restarts = restarts,
                    .idle_rounds = idle_rounds,
                    .exit_reason = .restart_limit,
                    .succeeded = false,
                    .coordinator = coordinator,
                    .worker_pool = worker_pool,
                };
            }
            restarts += 1;
            idle_rounds = 0;
            continue;
        }

        const progressed = childRunDurableProgressed(coordinator) or childRunDurableProgressed(worker_pool);
        if (progressed) {
            idle_rounds = 0;
        } else {
            idle_rounds += 1;
            if (idle_rounds >= cfg.max_supervisor_idle_rounds) {
                return .{
                    .rounds_executed = rounds_executed,
                    .restarts = restarts,
                    .idle_rounds = idle_rounds,
                    .exit_reason = .idle,
                    .succeeded = true,
                    .coordinator = coordinator,
                    .worker_pool = worker_pool,
                };
            }
        }
        if (cfg.tick_interval_ms > 0) {
            platform.time.sleepNs(cfg.tick_interval_ms * std.time.ns_per_ms);
        }
    }

    return .{
        .rounds_executed = rounds_executed,
        .restarts = restarts,
        .idle_rounds = idle_rounds,
        .exit_reason = .max_rounds,
        .succeeded = false,
        .coordinator = coordinator,
        .worker_pool = worker_pool,
    };
}

const RealSupervisorChildRunner = struct {
    fn run(
        self: *RealSupervisorChildRunner,
        alloc: std.mem.Allocator,
        io: std.Io,
        argv: []const []const u8,
    ) !ChildRunSummary {
        _ = self;
        const result = try std.process.run(alloc, io, .{
            .argv = argv,
            .reserve_amount = 512,
        });
        defer alloc.free(result.stdout);
        defer alloc.free(result.stderr);
        return .{
            .exit = childExitFromTerm(result.term),
            .durable_progressed = parseChildDurableProgressed(alloc, result.stdout) catch null,
            .telemetry = parseChildRuntimeTelemetry(alloc, result.stdout) catch null,
            .stdout_bytes = result.stdout.len,
            .stderr_bytes = result.stderr.len,
        };
    }
};

fn buildSupervisorChildArgv(
    alloc: std.mem.Allocator,
    argv0: []const u8,
    target: SupervisorTarget,
    cfg: SupervisorConfig,
    role: ChildRole,
    summary_file: ?[]const u8,
) !ChildArgv {
    var out = ChildArgv{};
    errdefer out.deinit(alloc);

    try out.append(alloc, cfg.executable orelse argv0);
    try out.append(alloc, process_subcommand);
    try appendSupervisorTargetArgs(&out, alloc, target);
    try out.append(alloc, "--role");
    try out.append(alloc, switch (role) {
        .coordinator => "coordinator",
        .worker_pool => "worker_pool",
    });

    const owner_id = switch (role) {
        .coordinator => cfg.coordinator_owner_id,
        .worker_pool => cfg.worker_pool_owner_id,
    };
    try out.append(alloc, "--runtime-id");
    try out.append(alloc, owner_id);
    try out.append(alloc, "--owner-id");
    try out.append(alloc, owner_id);
    try out.append(alloc, "--worker-id");
    try out.append(alloc, switch (role) {
        .coordinator => "graph-metric-coordinator-unused",
        .worker_pool => "graph-metric-worker-pool-unused",
    });
    if (role == .worker_pool) {
        const worker_ids = try std.mem.join(alloc, ",", cfg.worker_ids.items);
        errdefer alloc.free(worker_ids);
        try out.owned.append(alloc, worker_ids);
        try out.append(alloc, "--worker-ids");
        try out.append(alloc, worker_ids);
    }

    try out.append(alloc, "--lease-owned");
    try out.append(alloc, "true");
    try out.append(alloc, "--lease-ttl-ms");
    try out.appendOwned(alloc, "{d}", .{cfg.lease_ttl_ms});
    try out.append(alloc, "--coordinator-start-background-builds");
    try out.append(alloc, if (role == .coordinator) "true" else "false");
    try out.append(alloc, "--ticks");
    try out.appendOwned(alloc, "{d}", .{cfg.max_ticks});
    try out.append(alloc, "--until-idle");
    try out.append(alloc, "--max-idle-ticks");
    try out.appendOwned(alloc, "{d}", .{cfg.max_idle_ticks});
    try out.append(alloc, "--tick-ms");
    try out.appendOwned(alloc, "{d}", .{cfg.tick_interval_ms});
    try out.append(alloc, "--max-rounds");
    try out.appendOwned(alloc, "{d}", .{cfg.max_rounds});
    try out.append(alloc, "--max-metrics");
    try out.appendOwned(alloc, "{d}", .{cfg.max_metrics_per_round});
    try out.append(alloc, "--max-pages");
    try out.appendOwned(alloc, "{d}", .{cfg.max_pages_per_round});
    if (summary_file) |path| {
        try out.append(alloc, "--summary-file");
        try out.append(alloc, path);
        if (std.meta.activeTag(target) == .db_path) {
            try out.append(alloc, "--local-db-writer-lock");
            try out.append(alloc, "true");
        }
    }

    return out;
}

fn appendSupervisorTargetArgs(out: *ChildArgv, alloc: std.mem.Allocator, target: SupervisorTarget) !void {
    switch (target) {
        .db_path => |db_path| {
            try out.append(alloc, "--db-path");
            try out.append(alloc, db_path);
        },
        .service => |service| {
            try out.append(alloc, "--base-uri");
            try out.append(alloc, service.base_uri);
            try out.append(alloc, "--group-id");
            try out.appendOwned(alloc, "{d}", .{service.group_id});
            try out.append(alloc, "--table-name");
            try out.append(alloc, service.table_name);
        },
    }
}

fn acquireLocalDbWriterLock(alloc: std.mem.Allocator, db_path: []const u8) !writer_lock_mod.WriterLock {
    const lock_path = try std.fmt.allocPrint(alloc, "{s}-graph-metric-maintenance", .{db_path});
    defer alloc.free(lock_path);

    var attempts: usize = 0;
    while (attempts < local_db_writer_lock_retries) : (attempts += 1) {
        return writer_lock_mod.acquire(lock_path) catch |err| switch (err) {
            error.WriterLocked => {
                platform.time.sleepNs(local_db_writer_lock_sleep_ms * std.time.ns_per_ms);
                continue;
            },
            else => return err,
        };
    }
    return error.WriterLocked;
}

fn launchSummaryPathAlloc(
    alloc: std.mem.Allocator,
    summary_dir: []const u8,
    role: ChildRole,
    round: usize,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/graph-metric-launch-{s}-{d}.json", .{
        summary_dir,
        switch (role) {
            .coordinator => "coordinator",
            .worker_pool => "worker-pool",
        },
        round,
    });
}

fn launchStderrPathAlloc(
    alloc: std.mem.Allocator,
    summary_dir: []const u8,
    role: ChildRole,
    round: usize,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/graph-metric-launch-{s}-{d}.stderr", .{
        summary_dir,
        switch (role) {
            .coordinator => "coordinator",
            .worker_pool => "worker-pool",
        },
        round,
    });
}

fn childRunSummaryFromFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    term: std.process.Child.Term,
    summary_path: []const u8,
    stderr_path: []const u8,
) !ChildRunSummary {
    const stderr_bytes = try childOutputFileSize(alloc, io, stderr_path);
    const raw = std.Io.Dir.cwd().readFileAlloc(io, summary_path, alloc, .limited(64 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return .{
            .exit = childExitFromTerm(term),
            .durable_progressed = null,
            .telemetry = null,
            .stdout_bytes = 0,
            .stderr_bytes = stderr_bytes,
        },
        else => return err,
    };
    defer alloc.free(raw);
    return .{
        .exit = childExitFromTerm(term),
        .durable_progressed = parseChildDurableProgressed(alloc, raw) catch null,
        .telemetry = parseChildRuntimeTelemetry(alloc, raw) catch null,
        .stdout_bytes = raw.len,
        .stderr_bytes = stderr_bytes,
    };
}

fn childOutputFileSize(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !usize {
    const raw = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(64 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    return raw.len;
}

fn reportLaunchChildFailure(
    alloc: std.mem.Allocator,
    io: std.Io,
    label: []const u8,
    summary: ChildRunSummary,
    stderr_path: []const u8,
) void {
    if (childRunSucceeded(summary)) return;
    const raw = std.Io.Dir.cwd().readFileAlloc(io, stderr_path, alloc, .limited(64 * 1024)) catch return;
    defer alloc.free(raw);
    if (raw.len == 0) return;
    std.debug.print("graph metric launched {s} stderr:\n{s}\n", .{ label, raw });
}

fn childExitFromTerm(term: std.process.Child.Term) ChildExitSummary {
    return switch (term) {
        .exited => |code| .{ .exited = true, .code = code },
        else => .{ .exited = false, .code = null },
    };
}

fn childExitSucceeded(summary: ChildExitSummary) bool {
    return summary.exited and summary.code != null and summary.code.? == 0;
}

fn childRunSucceeded(summary: ChildRunSummary) bool {
    return childExitSucceeded(summary.exit);
}

fn childRunDurableProgressed(summary: ChildRunSummary) bool {
    return summary.durable_progressed orelse false;
}

fn parseChildDurableProgressed(alloc: std.mem.Allocator, stdout: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stdout, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    const value = object.get("durable_progressed") orelse return error.InvalidArguments;
    return switch (value) {
        .bool => |b| b,
        else => error.InvalidArguments,
    };
}

fn parseChildRuntimeTelemetry(alloc: std.mem.Allocator, stdout: []const u8) !ChildRuntimeTelemetry {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stdout, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    const stats = switch (object.get("stats") orelse return error.InvalidArguments) {
        .object => |stats| stats,
        else => return error.InvalidArguments,
    };
    return .{
        .role = try parseJsonRole(stats.get("role") orelse return error.InvalidArguments),
        .runtime_id_hash = try parseJsonU64(stats.get("runtime_id_hash") orelse return error.InvalidArguments),
        .owner_id_hash = try parseJsonU64(stats.get("owner_id_hash") orelse return error.InvalidArguments),
        .lease_key_hash = try parseJsonU64(stats.get("lease_key_hash") orelse return error.InvalidArguments),
        .worker_id_hash = try parseJsonU64(stats.get("worker_id_hash") orelse return error.InvalidArguments),
        .worker_count = try parseJsonUsize(stats.get("worker_count") orelse return error.InvalidArguments),
        .lease_owned = try parseJsonBool(stats.get("lease_owned") orelse return error.InvalidArguments),
        .has_lease = try parseJsonBool(stats.get("has_lease") orelse return error.InvalidArguments),
        .acquisition_count = try parseJsonU64(stats.get("acquisition_count") orelse return error.InvalidArguments),
        .takeover_count = try parseJsonU64(stats.get("takeover_count") orelse return error.InvalidArguments),
        .lost_leases = try parseJsonU64(stats.get("lost_leases") orelse return error.InvalidArguments),
        .ticks_started = try parseJsonU64(stats.get("ticks_started") orelse return error.InvalidArguments),
        .ticks_completed = try parseJsonU64(stats.get("ticks_completed") orelse return error.InvalidArguments),
        .idle_ticks = try parseJsonU64(stats.get("idle_ticks") orelse return error.InvalidArguments),
        .error_ticks = try parseJsonU64(stats.get("error_ticks") orelse return error.InvalidArguments),
        .has_last_error = switch (stats.get("last_error_name") orelse return error.InvalidArguments) {
            .null => false,
            else => true,
        },
    };
}

fn telemetryFromRunSummary(summary: RunSummary) ChildRuntimeTelemetry {
    return .{
        .role = summary.stats.role,
        .runtime_id_hash = summary.stats.runtime_id_hash,
        .owner_id_hash = summary.stats.owner_id_hash,
        .lease_key_hash = summary.stats.lease_key_hash,
        .worker_id_hash = summary.stats.worker_id_hash,
        .worker_count = summary.stats.worker_count,
        .lease_owned = summary.stats.lease_owned,
        .has_lease = summary.stats.has_lease,
        .acquisition_count = summary.stats.acquisition_count,
        .takeover_count = summary.stats.takeover_count,
        .lost_leases = summary.stats.lost_leases,
        .ticks_started = summary.stats.ticks_started,
        .ticks_completed = summary.stats.ticks_completed,
        .idle_ticks = summary.stats.idle_ticks,
        .error_ticks = summary.stats.error_ticks,
        .has_last_error = summary.stats.last_error_name != null,
    };
}

fn parseJsonRole(value: std.json.Value) !RuntimeRole {
    return switch (value) {
        .string => |raw| parseRole(raw),
        else => error.InvalidArguments,
    };
}

fn parseJsonUsize(value: std.json.Value) !usize {
    return @intCast(try parseJsonU64(value));
}

fn parseJsonU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |int| if (int >= 0) @intCast(int) else error.InvalidArguments,
        .number_string => |raw| std.fmt.parseInt(u64, raw, 10) catch return error.InvalidArguments,
        else => error.InvalidArguments,
    };
}

fn parseJsonBool(value: std.json.Value) !bool {
    return switch (value) {
        .bool => |b| b,
        else => error.InvalidArguments,
    };
}

fn shouldRestartSupervisorAttempt(failed: bool, restarts: usize, max_restarts: usize) bool {
    return failed and restarts < max_restarts;
}

fn writeJson(io: std.Io, alloc: std.mem.Allocator, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{ .whitespace = .indent_2 });
    defer alloc.free(json);
    std.Io.File.stdout().writeStreamingAll(io, json) catch {};
    std.Io.File.stdout().writeStreamingAll(io, "\n") catch {};
}

fn writeJsonFile(io: std.Io, alloc: std.mem.Allocator, path: []const u8, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{ .whitespace = .indent_2 });
    defer alloc.free(json);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = path,
        .data = json,
    });
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} graph-metric-maintenance (--db-path <path> | --base-uri <uri> --group-id <id> --table-name <table>) [options]
        \\
        \\options:
        \\  --base-uri <uri>
        \\  --service-base-uri <uri>
        \\  --group-id <id>
        \\  --table-name <table>
        \\  --role <combined|coordinator|worker|worker_pool>
        \\  --runtime-id <id>
        \\  --owner-id <id>
        \\  --worker-id <id>
        \\  --worker-ids <id,id,...>
        \\  --lease-owned <true|false>
        \\  --lease-ttl-ms <ms>
        \\  --coordinator-start-background-builds <true|false>
        \\  --ticks <n>
        \\  --until-idle
        \\  --max-idle-ticks <n>
        \\  --tick-ms <ms>
        \\  --max-rounds <n>
        \\  --max-metrics <n>
        \\  --max-pages <n>
        \\  --test-now-ms <ms>
        \\  --test-ready-file <path>
        \\  --test-hold-after-run-ms <ms>
        \\  --summary-file <path>
        \\  --local-db-writer-lock <true|false>
        \\
        \\subcommands:
        \\  supervise
        \\  launch
        \\
    , .{argv0});
}

fn printSupervisorUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} graph-metric-maintenance supervise (--db-path <path> | --base-uri <uri> --group-id <id> --table-name <table>) [options]
        \\
        \\options:
        \\  --base-uri <uri>
        \\  --service-base-uri <uri>
        \\  --group-id <id>
        \\  --table-name <table>
        \\  --executable <path>
        \\  --coordinator-owner-id <id>
        \\  --worker-pool-owner-id <id>
        \\  --worker-ids <id,id,...>
        \\  --lease-ttl-ms <ms>
        \\  --ticks <n>
        \\  --max-idle-ticks <n>
        \\  --supervisor-rounds <n>
        \\  --supervisor-idle-rounds <n>
        \\  --tick-ms <ms>
        \\  --max-restarts <n>
        \\  --max-rounds <n>
        \\  --max-metrics <n>
        \\  --max-pages <n>
        \\  --summary-dir <path>
        \\
    , .{argv0});
}

fn printLaunchUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} graph-metric-maintenance launch (--db-path <path> | --base-uri <uri> --group-id <id> --table-name <table>) [options]
        \\
        \\options:
        \\  --base-uri <uri>
        \\  --service-base-uri <uri>
        \\  --group-id <id>
        \\  --table-name <table>
        \\  --executable <path>
        \\  --coordinator-owner-id <id>
        \\  --worker-pool-owner-id <id>
        \\  --worker-ids <id,id,...>
        \\  --lease-ttl-ms <ms>
        \\  --ticks <n>
        \\  --max-idle-ticks <n>
        \\  --supervisor-rounds <n>
        \\  --supervisor-idle-rounds <n>
        \\  --tick-ms <ms>
        \\  --max-restarts <n>
        \\  --max-rounds <n>
        \\  --max-metrics <n>
        \\  --max-pages <n>
        \\  --summary-dir <path>
        \\
    , .{argv0});
}

fn argvContains(argv: []const []const u8, needle: []const u8) bool {
    for (argv) |arg| {
        if (std.mem.eql(u8, arg, needle)) return true;
    }
    return false;
}

fn argvValueEquals(argv: []const []const u8, flag: []const u8, value: []const u8) bool {
    for (argv, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, flag)) {
            return i + 1 < argv.len and std.mem.eql(u8, argv[i + 1], value);
        }
    }
    return false;
}

fn argvContainsAny(argv: []const []const u8, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (argvContains(argv, needle)) return true;
    }
    return false;
}

const FakeSupervisorChildRunner = struct {
    sequence: []const ChildRunSummary,
    calls: usize = 0,

    fn run(
        self: *FakeSupervisorChildRunner,
        alloc: std.mem.Allocator,
        io: std.Io,
        argv: []const []const u8,
    ) !ChildRunSummary {
        _ = alloc;
        _ = io;
        _ = argv;
        if (self.calls >= self.sequence.len) return error.UnexpectedEndOfStream;
        const result = self.sequence[self.calls];
        self.calls += 1;
        return result;
    }
};

const FakeServiceMaintenanceClient = struct {
    responses: []const []const u8,
    calls: usize = 0,
    first_captured_body: ?[]u8 = null,
    captured_body: ?[]u8 = null,
    captured_target: ?ServiceTarget = null,
    expect_ready_file_before_release: ?[]const u8 = null,

    fn deinit(self: *FakeServiceMaintenanceClient, alloc: std.mem.Allocator) void {
        if (self.first_captured_body) |body| alloc.free(body);
        if (self.captured_body) |body| alloc.free(body);
        self.* = undefined;
    }

    fn request(
        self: *FakeServiceMaintenanceClient,
        alloc: std.mem.Allocator,
        target: ServiceTarget,
        body: []const u8,
    ) !HttpQueryResponse {
        if (self.calls >= self.responses.len) return error.UnexpectedEndOfStream;
        if (self.first_captured_body == null) {
            self.first_captured_body = try alloc.dupe(u8, body);
        }
        if (self.captured_body) |prior| alloc.free(prior);
        self.captured_body = try alloc.dupe(u8, body);
        self.captured_target = target;
        if (self.expect_ready_file_before_release) |ready_file| {
            if (std.mem.indexOf(u8, body, "\"action\":\"release\"") != null) {
                try std.Io.Dir.cwd().access(std.Io.Threaded.global_single_threaded.io(), ready_file, .{});
            }
        }
        const response_body = try alloc.dupe(u8, self.responses[self.calls]);
        self.calls += 1;
        return .{ .body = response_body };
    }
};

fn fakeChild(progressed: ?bool, code: u8) ChildRunSummary {
    return .{
        .exit = .{ .exited = true, .code = code },
        .durable_progressed = progressed,
        .telemetry = null,
        .stdout_bytes = if (progressed == null) 0 else 64,
        .stderr_bytes = 0,
    };
}

const LoopbackSupervisorChildRunner = struct {
    calls: usize = 0,

    fn run(
        self: *LoopbackSupervisorChildRunner,
        alloc: std.mem.Allocator,
        io: std.Io,
        argv: []const []const u8,
    ) !ChildRunSummary {
        _ = io;
        if (argv.len < 3) return error.InvalidArguments;
        if (!std.mem.eql(u8, argv[1], process_subcommand)) return error.InvalidArguments;
        const argv_z = try alloc.alloc([*:0]const u8, argv.len - 2);
        defer alloc.free(argv_z);
        var initialized: usize = 0;
        defer for (argv_z[0..initialized]) |arg| alloc.free(std.mem.span(arg));
        for (argv[2..], argv_z) |arg, *arg_z| {
            arg_z.* = (try alloc.dupeZ(u8, arg)).ptr;
            initialized += 1;
        }
        var args = std.process.Args.Iterator.init(.{ .vector = argv_z });
        var cli = try parseCli(alloc, &args);
        defer cli.deinit(alloc);
        const db_path = cli.db_path orelse return error.InvalidArguments;
        const summary = try runConfigured(alloc, db_path, cli);
        self.calls += 1;
        return .{
            .exit = .{ .exited = true, .code = 0 },
            .durable_progressed = summary.durable_progressed,
            .telemetry = telemetryFromRunSummary(summary),
            .stdout_bytes = 64,
            .stderr_bytes = 0,
        };
    }
};

const InProcessGraphMetricService = struct {
    db: *antfly.db.DB,
    bound: antfly.public_api.BoundTableWriteSource,
    calls: usize = 0,

    fn init(db: *antfly.db.DB) InProcessGraphMetricService {
        return .{
            .db = db,
            .bound = antfly.public_api.BoundTableWriteSource.init("docs", db),
        };
    }

    fn request(
        self: *InProcessGraphMetricService,
        alloc: std.mem.Allocator,
        target: ServiceTarget,
        body: []const u8,
    ) !HttpQueryResponse {
        const response_body = (try self.bound.source().graphMetricMaintenanceGroupLocal(
            alloc,
            target.group_id,
            target.table_name,
            body,
        )) orelse return error.InvalidArguments;
        self.calls += 1;
        return .{
            .content_type = try alloc.dupe(u8, "application/json"),
            .body = response_body,
        };
    }

    fn validateBatch(_: *anyopaque, _: []const u8, _: []const antfly.db.types.BatchWrite) !void {}

    fn validateTxn(_: *anyopaque, _: []const u8, _: []const antfly.db.types.TransactionWrite) !void {}
};

fn requestInProcessServiceMaintenanceForTest(
    alloc: std.mem.Allocator,
    service: *InProcessGraphMetricService,
    target: ServiceTarget,
    cli: CliConfig,
    action: ServiceMaintenanceAction,
) !ServiceMaintenanceResponseWire {
    const body = try serviceRequestJsonFromFieldsAlloc(
        alloc,
        cli,
        action,
        cli.role,
        cli.worker_id,
        cli.worker_ids.items,
        cli.coordinator_start_background_builds,
        cli.max_rounds,
        cli.max_metrics_per_round,
        cli.max_pages_per_round,
        cli.test_now_ms,
    );
    defer alloc.free(body);
    var response = try service.request(alloc, target, body);
    defer response.deinit(alloc);
    return try parseServiceMaintenanceResponse(alloc, response.body);
}

fn expectParseCliInvalid(alloc: std.mem.Allocator, argv: []const [*:0]const u8) !void {
    var args = std.process.Args.Iterator.init(.{ .vector = argv });
    try std.testing.expectError(error.InvalidArguments, parseCli(alloc, &args));
}

fn expectParseSupervisorInvalid(alloc: std.mem.Allocator, argv: []const [*:0]const u8) !void {
    var args = std.process.Args.Iterator.init(.{ .vector = argv });
    try std.testing.expectError(error.InvalidArguments, parseSupervisorCli(alloc, &args));
}

test "graph metric maintenance command parses worker pool config" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--db-path",    "/tmp/antfly-graph-metric-command-test",
        "--role",       "worker-pool",
        "--runtime-id", "runtime-a",
        "--owner-id",   "owner-a",
        "--worker-ids", "worker-a,worker-b",
        "--ticks",      "7",
        "--until-idle", "--max-idle-ticks",
        "3",            "--tick-ms",
        "5",            "--max-pages",
        "2",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(alloc, &args);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(RuntimeRole.worker_pool, parsed.role);
    try std.testing.expectEqual(@as(usize, 7), parsed.max_ticks);
    try std.testing.expect(parsed.until_idle);
    try std.testing.expectEqual(@as(usize, 3), parsed.max_idle_ticks.?);
    try std.testing.expectEqual(@as(u64, 5), parsed.tick_interval_ms);
    try std.testing.expectEqual(@as(usize, 2), parsed.worker_ids.items.len);
    try std.testing.expectEqualStrings("worker-a", parsed.worker_ids.items[0]);
    try std.testing.expectEqualStrings("worker-b", parsed.worker_ids.items[1]);
    const runtime_cfg = parsed.runtimeConfig(platform_clock.Clock.real());
    try std.testing.expect(runtime_cfg.enabled);
    try std.testing.expect(!runtime_cfg.start_background_loop);
    try std.testing.expectEqual(RuntimeRole.worker_pool, runtime_cfg.role);
    try std.testing.expectEqual(@as(usize, 2), runtime_cfg.planned_options.worker_ids.len);
    try std.testing.expectEqual(@as(usize, 2), runtime_cfg.planned_options.max_pages_per_round);
}

test "graph metric maintenance command parses service target config" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--base-uri",   "http://127.0.0.1:8080",
        "--group-id",   "7",
        "--table-name", "docs",
        "--role",       "worker-pool",
        "--runtime-id", "runtime-a",
        "--owner-id",   "owner-a",
        "--worker-ids", "worker-a,worker-b",
        "--max-pages",  "3",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(alloc, &args);
    defer parsed.deinit(alloc);

    const target = (try parsed.serviceTarget()) orelse return error.MissingServiceTarget;
    try std.testing.expectEqualStrings("http://127.0.0.1:8080", target.base_uri);
    try std.testing.expectEqual(@as(u64, 7), target.group_id);
    try std.testing.expectEqualStrings("docs", target.table_name);
    try std.testing.expectEqual(RuntimeRole.worker_pool, parsed.role);
    try std.testing.expectEqual(@as(usize, 2), parsed.worker_ids.items.len);
    try std.testing.expectEqual(@as(usize, 3), parsed.max_pages_per_round);
}

test "graph metric maintenance command rejects invalid service target combinations" {
    const alloc = std.testing.allocator;

    try expectParseCliInvalid(alloc, &.{
        "--db-path",    "/tmp/db",
        "--base-uri",   "http://127.0.0.1:8080",
        "--group-id",   "7",
        "--table-name", "docs",
    });
    try expectParseCliInvalid(alloc, &.{
        "--base-uri", "http://127.0.0.1:8080",
        "--group-id", "7",
    });
    try expectParseCliInvalid(alloc, &.{
        "--base-uri",   "http://127.0.0.1:8080",
        "--table-name", "docs",
    });
}

test "graph metric maintenance service request stays owner and budget scoped" {
    const alloc = std.testing.allocator;
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "worker-a", "worker-b" });

    const body = try serviceRequestJsonAlloc(alloc, .{
        .role = .worker_pool,
        .runtime_id = "runtime-a",
        .owner_id = "owner-a",
        .worker_id = "unused-worker",
        .worker_ids = worker_ids,
        .coordinator_start_background_builds = false,
        .max_rounds = 2,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 3,
        .test_now_ms = 42,
    });
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    try std.testing.expectEqualStrings("worker_pool", object.get("role").?.string);
    try std.testing.expectEqualStrings("tick", object.get("action").?.string);
    try std.testing.expectEqualStrings("unused-worker", object.get("worker_id").?.string);
    try std.testing.expectEqual(@as(usize, 2), object.get("worker_ids").?.array.items.len);
    try std.testing.expect(!object.get("start_background_builds").?.bool);
    try std.testing.expectEqual(@as(i64, 2), object.get("max_rounds").?.integer);
    try std.testing.expectEqual(@as(i64, 4), object.get("max_metrics_per_round").?.integer);
    try std.testing.expectEqual(@as(i64, 3), object.get("max_pages_per_round").?.integer);
    try std.testing.expectEqual(@as(i64, 42), object.get("now_ms").?.integer);
    try std.testing.expectEqualStrings("runtime-a", object.get("runtime_id").?.string);
    try std.testing.expectEqualStrings("owner-a", object.get("owner_id").?.string);
    try std.testing.expect(object.get("lease_owned").?.bool);
    try std.testing.expectEqual(@as(i64, 30_000), object.get("lease_ttl_ms").?.integer);
    try std.testing.expect(object.get("metric_name") == null);
    try std.testing.expect(object.get("target_generation") == null);
    try std.testing.expect(object.get("job_id") == null);
    try std.testing.expect(object.get("page_id") == null);
}

test "graph metric maintenance service runner aggregates remote ticks" {
    const alloc = std.testing.allocator;
    const runtime_hash = std.hash.Wyhash.hash(0, "service-runtime");
    const owner_hash = std.hash.Wyhash.hash(0, "service-owner");
    const response_a = try std.fmt.allocPrint(
        alloc,
        "{{\"result\":{{\"metrics_scanned\":1,\"builds_started\":1,\"budget_exhausted\":true}},\"stats\":{{\"enabled\":true,\"role\":\"coordinator\",\"runtime_id_hash\":{d},\"owner_id_hash\":{d},\"lease_key_hash\":1,\"worker_id_hash\":0,\"worker_count\":0,\"lease_owned\":true,\"has_lease\":true,\"acquisition_count\":1,\"takeover_count\":0,\"lease_acquire_failures\":0,\"lost_leases\":0,\"last_acquired_ms\":1000,\"started\":false,\"shutdown\":false,\"notified\":false,\"ticks_started\":1,\"ticks_completed\":1,\"durable_progress_ticks\":1,\"idle_ticks\":0,\"error_ticks\":0,\"last_error_name\":null,\"total_result\":{{\"metrics_scanned\":1,\"builds_started\":1,\"budget_exhausted\":true}},\"last_result\":{{\"metrics_scanned\":1,\"builds_started\":1,\"budget_exhausted\":true}}}}}}",
        .{ runtime_hash, owner_hash },
    );
    defer alloc.free(response_a);
    const response_b = try std.fmt.allocPrint(
        alloc,
        "{{\"result\":{{\"metrics_scanned\":1}},\"stats\":{{\"enabled\":true,\"role\":\"coordinator\",\"runtime_id_hash\":{d},\"owner_id_hash\":{d},\"lease_key_hash\":1,\"worker_id_hash\":0,\"worker_count\":0,\"lease_owned\":true,\"has_lease\":true,\"acquisition_count\":1,\"takeover_count\":0,\"lease_acquire_failures\":0,\"lost_leases\":0,\"last_acquired_ms\":1010,\"started\":false,\"shutdown\":false,\"notified\":false,\"ticks_started\":1,\"ticks_completed\":1,\"durable_progress_ticks\":0,\"idle_ticks\":1,\"error_ticks\":0,\"last_error_name\":null,\"total_result\":{{\"metrics_scanned\":1}},\"last_result\":{{\"metrics_scanned\":1}}}}}}",
        .{ runtime_hash, owner_hash },
    );
    defer alloc.free(response_b);
    const responses = [_][]const u8{
        response_a,
        response_b,
        "{\"released\":true,\"stats\":{\"enabled\":true,\"role\":\"coordinator\",\"lease_owned\":true,\"has_lease\":false}}",
    };
    var client = FakeServiceMaintenanceClient{ .responses = responses[0..] };
    defer client.deinit(alloc);

    const target = ServiceTarget{
        .base_uri = "http://127.0.0.1:8080",
        .group_id = 7,
        .table_name = "docs",
    };
    const summary = try runServiceConfiguredWithRequester(
        FakeServiceMaintenanceClient,
        &client,
        FakeServiceMaintenanceClient.request,
        alloc,
        target,
        .{
            .role = .coordinator,
            .runtime_id = "service-runtime",
            .owner_id = "service-owner",
            .lease_owned = true,
            .worker_id = "service-unused-worker",
            .max_ticks = 10,
            .until_idle = true,
            .max_idle_ticks = 1,
            .max_metrics_per_round = 4,
        },
    );

    try std.testing.expectEqual(@as(usize, 3), client.calls);
    try std.testing.expectEqualStrings(target.base_uri, client.captured_target.?.base_uri);
    try std.testing.expectEqual(target.group_id, client.captured_target.?.group_id);
    try std.testing.expectEqualStrings(target.table_name, client.captured_target.?.table_name);
    try std.testing.expectEqual(RuntimeRole.coordinator, summary.role);
    try std.testing.expectEqual(@as(usize, 2), summary.ticks_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.idle_streak);
    try std.testing.expectEqual(ExitReason.idle, summary.exit_reason);
    try std.testing.expect(summary.durable_progressed);
    try std.testing.expectEqual(@as(usize, 2), summary.result.metrics_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.result.builds_started);
    try std.testing.expect(summary.result.budget_exhausted);
    try std.testing.expect(summary.stats.enabled);
    try std.testing.expectEqual(RuntimeRole.coordinator, summary.stats.role);
    try std.testing.expectEqual(std.hash.Wyhash.hash(0, "service-runtime"), summary.stats.runtime_id_hash);
    try std.testing.expectEqual(std.hash.Wyhash.hash(0, "service-owner"), summary.stats.owner_id_hash);
    try std.testing.expect(summary.stats.lease_owned);
    try std.testing.expect(!summary.stats.has_lease);
    try std.testing.expect(summary.stats.shutdown);
    try std.testing.expectEqual(@as(u64, 2), summary.stats.acquisition_count);
    try std.testing.expectEqual(@as(u64, 2), summary.stats.ticks_started);
    try std.testing.expectEqual(@as(u64, 2), summary.stats.ticks_completed);
    try std.testing.expectEqual(@as(u64, 1), summary.stats.durable_progress_ticks);
    try std.testing.expectEqual(@as(u64, 1), summary.stats.idle_ticks);
    try std.testing.expectEqual(@as(u64, 0), summary.stats.error_ticks);
}

test "graph metric maintenance service boundary preserves worker pool owner request" {
    const alloc = std.testing.allocator;
    const responses = [_][]const u8{
        "{\"worker_steps\":2,\"pages_completed\":2}",
        "{\"released\":true,\"stats\":{\"enabled\":true,\"role\":\"worker_pool\",\"worker_count\":2,\"lease_owned\":true,\"has_lease\":false}}",
    };
    var client = FakeServiceMaintenanceClient{ .responses = responses[0..] };
    defer client.deinit(alloc);

    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "worker-a", "worker-b" });

    const target = ServiceTarget{
        .base_uri = "http://127.0.0.1:8080",
        .group_id = 7,
        .table_name = "docs",
    };
    const summary = try runServiceConfiguredWithRequester(
        FakeServiceMaintenanceClient,
        &client,
        FakeServiceMaintenanceClient.request,
        alloc,
        target,
        .{
            .role = .worker_pool,
            .runtime_id = "service-worker-pool-runtime",
            .owner_id = "service-worker-pool-owner",
            .lease_owned = true,
            .worker_id = "unused-worker",
            .worker_ids = worker_ids,
            .max_ticks = 1,
            .max_pages_per_round = 5,
            .test_now_ms = 700,
        },
    );

    try std.testing.expectEqual(@as(usize, 2), client.calls);
    try std.testing.expectEqual(RuntimeRole.worker_pool, summary.role);
    try std.testing.expectEqual(@as(usize, 2), summary.result.worker_steps);
    try std.testing.expectEqual(@as(usize, 2), summary.result.pages_completed);
    try std.testing.expectEqual(RuntimeRole.worker_pool, summary.stats.role);
    try std.testing.expectEqual(@as(usize, 2), summary.stats.worker_count);
    try std.testing.expect(summary.stats.lease_owned);
    try std.testing.expect(!summary.stats.has_lease);
    try std.testing.expect(summary.stats.shutdown);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, client.first_captured_body.?, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    try std.testing.expectEqualStrings("tick", object.get("action").?.string);
    try std.testing.expectEqualStrings("worker_pool", object.get("role").?.string);
    try std.testing.expectEqualStrings("unused-worker", object.get("worker_id").?.string);
    try std.testing.expectEqual(@as(usize, 2), object.get("worker_ids").?.array.items.len);
    try std.testing.expectEqual(@as(i64, 5), object.get("max_pages_per_round").?.integer);
    try std.testing.expectEqual(@as(i64, 700), object.get("now_ms").?.integer);
    try std.testing.expect(object.get("metric_name") == null);
    try std.testing.expect(object.get("job_id") == null);
    try std.testing.expect(object.get("page_id") == null);

    var release_parsed = try std.json.parseFromSlice(std.json.Value, alloc, client.captured_body.?, .{});
    defer release_parsed.deinit();
    const release_object = switch (release_parsed.value) {
        .object => |release_object| release_object,
        else => return error.InvalidArguments,
    };
    try std.testing.expectEqualStrings("release", release_object.get("action").?.string);
    try std.testing.expectEqualStrings("worker_pool", release_object.get("role").?.string);
    try std.testing.expectEqual(@as(usize, 2), release_object.get("worker_ids").?.array.items.len);
}

test "graph metric maintenance service test ready marker is written before clean release" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var ready_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const ready_path = try std.fmt.bufPrint(&ready_path_buf, ".zig-cache/tmp/{s}/graph-metric-service-ready-before-release", .{tmp.sub_path});
    std.Io.Dir.cwd().deleteFile(std.Io.Threaded.global_single_threaded.io(), ready_path) catch {};
    defer std.Io.Dir.cwd().deleteFile(std.Io.Threaded.global_single_threaded.io(), ready_path) catch {};

    const responses = [_][]const u8{
        "{\"metrics_scanned\":1}",
        "{\"released\":true,\"stats\":{\"enabled\":true,\"role\":\"coordinator\",\"lease_owned\":true,\"has_lease\":false}}",
    };
    var client = FakeServiceMaintenanceClient{
        .responses = responses[0..],
        .expect_ready_file_before_release = ready_path,
    };
    defer client.deinit(alloc);

    const target = ServiceTarget{
        .base_uri = "http://127.0.0.1:8080",
        .group_id = 7,
        .table_name = "docs",
    };
    const summary = try runServiceConfiguredWithRequester(
        FakeServiceMaintenanceClient,
        &client,
        FakeServiceMaintenanceClient.request,
        alloc,
        target,
        .{
            .role = .coordinator,
            .runtime_id = "service-ready-coordinator",
            .owner_id = "service-ready-coordinator",
            .lease_owned = true,
            .worker_id = "service-ready-unused",
            .max_ticks = 1,
            .max_metrics_per_round = 4,
            .test_ready_file = ready_path,
        },
    );

    try std.testing.expectEqual(@as(usize, 2), client.calls);
    try std.testing.expect(summary.stats.shutdown);
    try std.testing.expect(!summary.stats.has_lease);

    var release_parsed = try std.json.parseFromSlice(std.json.Value, alloc, client.captured_body.?, .{});
    defer release_parsed.deinit();
    const release_object = switch (release_parsed.value) {
        .object => |release_object| release_object,
        else => return error.InvalidArguments,
    };
    try std.testing.expectEqualStrings("release", release_object.get("action").?.string);
}

test "graph metric maintenance service owners drain degree through internal route" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-degree-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    var db = try antfly.db.DB.open(alloc, path, .{
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
    for (0..8) |i| {
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
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "service-worker-a", "service-worker-b" });

    var idle_rounds: usize = 0;
    var coordinator_summary = RunSummary{
        .role = .coordinator,
        .ticks_executed = 0,
        .idle_streak = 0,
        .exit_reason = .max_ticks,
        .durable_progressed = false,
        .result = .{},
        .stats = .{},
    };
    var worker_summary = coordinator_summary;
    for (0..80) |_| {
        coordinator_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            .{
                .role = .coordinator,
                .runtime_id = "service-degree-coordinator",
                .owner_id = "service-degree-coordinator",
                .lease_owned = true,
                .worker_id = "service-degree-coordinator-unused",
                .max_ticks = 1,
                .max_idle_ticks = 1,
                .tick_interval_ms = 0,
                .max_rounds = 1,
                .max_metrics_per_round = 4,
                .max_pages_per_round = 2,
            },
        );
        worker_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            .{
                .role = .worker_pool,
                .runtime_id = "service-degree-worker-pool",
                .owner_id = "service-degree-worker-pool",
                .lease_owned = true,
                .worker_id = "service-degree-worker-unused",
                .worker_ids = worker_ids,
                .coordinator_start_background_builds = false,
                .max_ticks = 1,
                .max_idle_ticks = 1,
                .tick_interval_ms = 0,
                .max_rounds = 1,
                .max_metrics_per_round = 4,
                .max_pages_per_round = 2,
            },
        );

        const progressed = coordinator_summary.durable_progressed or worker_summary.durable_progressed;
        idle_rounds = if (progressed) 0 else idle_rounds + 1;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(coordinator_summary.stats.shutdown);
            try std.testing.expect(worker_summary.stats.shutdown);
            try std.testing.expect(!coordinator_summary.stats.has_lease);
            try std.testing.expect(!worker_summary.stats.has_lease);
            try std.testing.expect(service.calls >= 4);
            return;
        }
        if (idle_rounds >= 4) break;
    }

    return error.GraphMetricBuildNotComplete;
}

test "graph metric maintenance service owners preserve degree freshness while rebuild is active" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-degree-active-freshness-db", .{tmp.sub_path});

    var db = try antfly.db.DB.open(alloc, path, .{
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
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
            .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}" },
        },
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());
    try db.runUntilIdle();

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 1), initial.graph_metric_results.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    const published_generation = initial.graph_metric_results[0].status.published_generation;
    try std.testing.expect(published_generation > 0);
    try std.testing.expectEqualStrings("doc:b", initial.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), initial.graph_metric_results[0].scores[0].score, 0.001);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:d",
            .value = "{\"title\":\"delta\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:b\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };
    const start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        .{
            .role = .coordinator,
            .runtime_id = "service-degree-freshness-coordinator",
            .owner_id = "service-degree-freshness-coordinator",
            .worker_id = "service-degree-freshness-unused",
            .max_rounds = 1,
            .max_metrics_per_round = 4,
            .max_pages_per_round = 1,
        },
        .tick,
    );
    try std.testing.expect(start.result.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), start.result.builds_started);

    var published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "degree",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "degree",
                .top_k = 2,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 1), published.graph_metric_results.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, published.graph_metric_results[0].status.state);
    try std.testing.expectEqual(published_generation, published.graph_metric_results[0].status.published_generation);
    try std.testing.expect(published.graph_metric_results[0].status.building_generation > published_generation);
    try std.testing.expectEqualStrings("doc:b", published.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), published.graph_metric_results[0].scores[0].score, 0.001);

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
    const traversal_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = traversal_query }},
        .limit = 0,
    });
    defer traversal.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", traversal.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), traversal.graph_results[0].nodes[0].metrics[0].score orelse return error.TestUnexpectedResult, 0.001);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].metric_status.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, traversal.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(published_generation, traversal.graph_results[0].metric_status[0].published_generation);

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "degree",
        .freshness = .fresh,
    }};
    var fresh_traversal_query = traversal_query;
    fresh_traversal_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_traversal_query }},
        .limit = 0,
    }));

    var rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    });
    defer rerank.deinit();
    try std.testing.expectEqualStrings("doc:b", rerank.hits[0].id);
    const rerank_status = rerank.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    const rerank_details = rerank.hits[0].score_details orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("degree", rerank_details.metric_name);
    try std.testing.expectEqual(published_generation, rerank_details.published_generation);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rerank_details.metric_score orelse return error.TestUnexpectedResult, 0.001);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "degree",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 4,
        .include_stored = false,
    }));
}

test "graph metric maintenance service owners fence abandoned degree leases and recover after ttl" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-degree-restart-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    var db = try antfly.db.DB.open(alloc, path, .{
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
    for (0..8) |i| {
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
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };

    var coordinator_a = CliConfig{
        .role = .coordinator,
        .runtime_id = "service-degree-restart-coordinator",
        .owner_id = "coordinator-a",
        .worker_id = "coordinator-unused",
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 2,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 1000,
    };
    const coordinator_a_start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_a,
        .tick,
    );
    const coordinator_a_start_stats = coordinator_a_start.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_a_start_stats.lease_owned);
    try std.testing.expect(coordinator_a_start_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_a_start_stats.acquisition_count);
    try std.testing.expect(coordinator_a_start.result.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), coordinator_a_start.result.builds_started);

    var coordinator_b = coordinator_a;
    coordinator_b.owner_id = "coordinator-b";
    coordinator_b.test_now_ms = 1100;
    const coordinator_b_fenced = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_b,
        .tick,
    );
    const coordinator_b_fenced_stats = coordinator_b_fenced.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_b_fenced_stats.lease_owned);
    try std.testing.expect(!coordinator_b_fenced_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_b_fenced_stats.lease_acquire_failures);
    try std.testing.expect(!coordinator_b_fenced.result.durableProgressed());

    coordinator_b.test_now_ms = 1301;
    const coordinator_b_takeover = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_b,
        .tick,
    );
    const coordinator_b_takeover_stats = coordinator_b_takeover.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_b_takeover_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_b_takeover_stats.takeover_count);
    try std.testing.expectEqual(identityHash("coordinator-b"), coordinator_b_takeover_stats.owner_id_hash);

    coordinator_a.test_now_ms = 1302;
    const stale_coordinator_release = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_a,
        .release,
    );
    try std.testing.expect(!stale_coordinator_release.released);
    try std.testing.expect(stale_coordinator_release.lease_owner_id_hash != 0);
    const stale_coordinator_release_stats = stale_coordinator_release.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!stale_coordinator_release_stats.has_lease);

    var worker_pool_a = CliConfig{
        .role = .worker_pool,
        .runtime_id = "service-degree-restart-worker-pool",
        .owner_id = "worker-pool-a",
        .worker_id = "worker-pool-unused",
        .coordinator_start_background_builds = false,
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 2,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 2000,
    };
    defer worker_pool_a.deinit(alloc);
    try worker_pool_a.worker_ids.appendSlice(alloc, &.{ "restart-worker-a", "restart-worker-b" });

    const worker_pool_a_start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_a,
        .tick,
    );
    const worker_pool_a_start_stats = worker_pool_a_start.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(worker_pool_a_start_stats.has_lease);
    try std.testing.expectEqual(@as(usize, 2), worker_pool_a_start_stats.worker_count);
    try std.testing.expect(worker_pool_a_start.result.durableProgressed());

    var worker_pool_b = CliConfig{
        .role = .worker_pool,
        .runtime_id = "service-degree-restart-worker-pool",
        .owner_id = "worker-pool-b",
        .worker_id = "worker-pool-unused",
        .coordinator_start_background_builds = false,
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 2,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 2100,
    };
    defer worker_pool_b.deinit(alloc);
    try worker_pool_b.worker_ids.appendSlice(alloc, &.{ "restart-worker-a", "restart-worker-b" });

    const worker_pool_b_fenced = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_b,
        .tick,
    );
    const worker_pool_b_fenced_stats = worker_pool_b_fenced.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!worker_pool_b_fenced_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_b_fenced_stats.lease_acquire_failures);
    try std.testing.expect(!worker_pool_b_fenced.result.durableProgressed());

    worker_pool_b.test_now_ms = 2301;
    const worker_pool_b_takeover = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_b,
        .tick,
    );
    const worker_pool_b_takeover_stats = worker_pool_b_takeover.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(worker_pool_b_takeover_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_b_takeover_stats.takeover_count);
    try std.testing.expectEqual(identityHash("worker-pool-b"), worker_pool_b_takeover_stats.owner_id_hash);

    worker_pool_a.test_now_ms = 2302;
    const stale_worker_pool_release = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_a,
        .release,
    );
    try std.testing.expect(!stale_worker_pool_release.released);
    try std.testing.expect(stale_worker_pool_release.lease_owner_id_hash != 0);
    const stale_worker_pool_release_stats = stale_worker_pool_release.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!stale_worker_pool_release_stats.has_lease);

    var idle_rounds: usize = 0;
    for (0..80) |round| {
        coordinator_b.test_now_ms = 2400 + @as(u64, @intCast(round));
        const coordinator_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            coordinator_b,
        );
        worker_pool_b.test_now_ms = 2500 + @as(u64, @intCast(round));
        const worker_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            worker_pool_b,
        );

        const progressed = coordinator_summary.durable_progressed or worker_summary.durable_progressed;
        idle_rounds = if (progressed) 0 else idle_rounds + 1;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(coordinator_summary.stats.shutdown);
            try std.testing.expect(worker_summary.stats.shutdown);
            try std.testing.expect(!coordinator_summary.stats.has_lease);
            try std.testing.expect(!worker_summary.stats.has_lease);
            return;
        }
        if (idle_rounds >= 4) break;
    }

    return error.GraphMetricBuildNotComplete;
}

test "graph metric maintenance service owners drain pagerank through internal route" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-pagerank-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    var db = try antfly.db.DB.open(alloc, path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });
    for (0..8) |i| {
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
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "service-pagerank-worker-a", "service-pagerank-worker-b" });

    var idle_rounds: usize = 0;
    var coordinator_summary = RunSummary{
        .role = .coordinator,
        .ticks_executed = 0,
        .idle_streak = 0,
        .exit_reason = .max_ticks,
        .durable_progressed = false,
        .result = .{},
        .stats = .{},
    };
    var worker_summary = coordinator_summary;
    for (0..160) |_| {
        coordinator_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            .{
                .role = .coordinator,
                .runtime_id = "service-pagerank-coordinator",
                .owner_id = "service-pagerank-coordinator",
                .lease_owned = true,
                .worker_id = "service-pagerank-coordinator-unused",
                .max_ticks = 1,
                .max_idle_ticks = 1,
                .tick_interval_ms = 0,
                .max_rounds = 1,
                .max_metrics_per_round = 4,
                .max_pages_per_round = 3,
            },
        );
        worker_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            .{
                .role = .worker_pool,
                .runtime_id = "service-pagerank-worker-pool",
                .owner_id = "service-pagerank-worker-pool",
                .lease_owned = true,
                .worker_id = "service-pagerank-worker-unused",
                .worker_ids = worker_ids,
                .coordinator_start_background_builds = false,
                .max_ticks = 1,
                .max_idle_ticks = 1,
                .tick_interval_ms = 0,
                .max_rounds = 1,
                .max_metrics_per_round = 4,
                .max_pages_per_round = 3,
            },
        );

        const progressed = coordinator_summary.durable_progressed or worker_summary.durable_progressed;
        idle_rounds = if (progressed) 0 else idle_rounds + 1;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(coordinator_summary.stats.shutdown);
            try std.testing.expect(worker_summary.stats.shutdown);
            try std.testing.expect(!coordinator_summary.stats.has_lease);
            try std.testing.expect(!worker_summary.stats.has_lease);
            try std.testing.expect(service.calls >= 4);
            const top = try graph_entry.index.graphMetricTopK("pagerank", 3);
            defer {
                for (top) |*score| score.deinit(alloc);
                alloc.free(top);
            }
            try std.testing.expect(top.len > 0);
            return;
        }
        if (idle_rounds >= 6) break;
    }

    return error.GraphMetricBuildNotComplete;
}

test "graph metric maintenance service owners preserve pagerank freshness while rebuild is active" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-pagerank-active-freshness-db", .{tmp.sub_path});

    var db = try antfly.db.DB.open(alloc, path, .{
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
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":3,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
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
    try db.runUntilIdle();

    var initial = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .fresh,
            },
        }},
        .limit = 0,
    });
    defer initial.deinit();
    try std.testing.expectEqual(@as(usize, 1), initial.graph_metric_results.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.fresh, initial.graph_metric_results[0].status.state);
    const published_generation = initial.graph_metric_results[0].status.published_generation;
    try std.testing.expect(published_generation > 0);
    try std.testing.expect(initial.graph_metric_results[0].scores.len > 0);
    const initial_top_node = initial.graph_metric_results[0].scores[0].node;
    const initial_top_score = initial.graph_metric_results[0].scores[0].score;

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:e",
            .value = "{\"title\":\"epsilon\",\"_edges\":{\"graph_idx\":{\"cites\":[{\"target\":\"doc:d\",\"weight\":1.0}]}}}",
        }},
        .sync_level = .write,
    });
    try db.runDerivedUntil(db.core.nextDerivedSequence());

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };
    const start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        .{
            .role = .coordinator,
            .runtime_id = "service-pagerank-freshness-coordinator",
            .owner_id = "service-pagerank-freshness-coordinator",
            .worker_id = "service-pagerank-freshness-unused",
            .max_rounds = 1,
            .max_metrics_per_round = 4,
            .max_pages_per_round = 1,
        },
        .tick,
    );
    try std.testing.expect(start.result.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), start.result.builds_started);

    var published = try db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
            .query = .{
                .index_name = "graph_idx",
                .metric_name = "pagerank",
                .top_k = 2,
                .freshness = .published,
            },
        }},
        .limit = 0,
    });
    defer published.deinit();
    try std.testing.expectEqual(@as(usize, 1), published.graph_metric_results.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, published.graph_metric_results[0].status.state);
    try std.testing.expectEqual(published_generation, published.graph_metric_results[0].status.published_generation);
    try std.testing.expect(published.graph_metric_results[0].status.building_generation > published_generation);
    try std.testing.expectEqualStrings(initial_top_node, published.graph_metric_results[0].scores[0].node);
    try std.testing.expectApproxEqAbs(initial_top_score, published.graph_metric_results[0].scores[0].score, 0.000001);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_metric_queries = &.{.{
            .name = "pagerank",
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
    const traversal_query = graph_query_mod.GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .params = .{ .edge_types = &.{"cites"}, .direction = .out, .max_depth = 1 },
        .metrics = &published_metric_reads,
        .include_metric_status = true,
    };
    var traversal = try db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = traversal_query }},
        .limit = 0,
    });
    defer traversal.deinit();
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results.len);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", traversal.graph_results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].nodes[0].metrics.len);
    try std.testing.expectEqualStrings("pagerank", traversal.graph_results[0].nodes[0].metrics[0].name);
    try std.testing.expect(traversal.graph_results[0].nodes[0].metrics[0].score != null);
    try std.testing.expectEqual(@as(usize, 1), traversal.graph_results[0].metric_status.len);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, traversal.graph_results[0].metric_status[0].state);
    try std.testing.expectEqual(published_generation, traversal.graph_results[0].metric_status[0].published_generation);

    const fresh_metric_reads = [_]graph_query_mod.GraphMetricRead{.{
        .name = "pagerank",
        .freshness = .fresh,
    }};
    var fresh_traversal_query = traversal_query;
    fresh_traversal_query.metrics = &fresh_metric_reads;
    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .graph_queries = &.{.{ .name = "neighbors", .query = fresh_traversal_query }},
        .limit = 0,
    }));

    var rerank = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .published,
            .weight = 1.0,
        },
        .limit = 5,
        .include_stored = false,
    });
    defer rerank.deinit();
    const rerank_status = rerank.graph_metric_rerank_status orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.building, rerank_status.state);
    try std.testing.expectEqual(published_generation, rerank_status.published_generation);
    const rerank_details = rerank.hits[0].score_details orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("pagerank", rerank_details.metric_name);
    try std.testing.expectEqual(published_generation, rerank_details.published_generation);
    try std.testing.expect(rerank_details.metric_score != null);

    try std.testing.expectError(error.MetricStale, db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match_all = {} },
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .weight = 1.0,
        },
        .limit = 5,
        .include_stored = false,
    }));
}

test "graph metric maintenance service owners fence abandoned pagerank leases and recover after ttl" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-service-pagerank-restart-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    var db = try antfly.db.DB.open(alloc, path, .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "graph_idx",
        .kind = .graph,
        .config_json = "{\"metrics\":{\"pagerank\":{\"enabled\":true,\"kind\":\"pagerank\",\"refresh\":\"background\",\"max_iterations\":2,\"tolerance\":0.000000001,\"edge_filter\":{\"types\":[\"cites\"]}}}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:hub", .value = "{\"title\":\"hub\"}" }},
        .sync_level = .write,
    });
    for (0..8) |i| {
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
    {
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        target_generation = graph_entry.index.edge_generation;
    }

    var service = InProcessGraphMetricService.init(&db);
    const target = ServiceTarget{
        .base_uri = "in-process://graph-metric-maintenance",
        .group_id = 7,
        .table_name = "docs",
    };

    var coordinator_a = CliConfig{
        .role = .coordinator,
        .runtime_id = "service-pagerank-restart-coordinator",
        .owner_id = "coordinator-a",
        .worker_id = "coordinator-unused",
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 3,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 1000,
    };
    const coordinator_a_start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_a,
        .tick,
    );
    const coordinator_a_start_stats = coordinator_a_start.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_a_start_stats.lease_owned);
    try std.testing.expect(coordinator_a_start_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_a_start_stats.acquisition_count);
    try std.testing.expect(coordinator_a_start.result.durableProgressed());
    try std.testing.expectEqual(@as(usize, 1), coordinator_a_start.result.builds_started);

    var coordinator_b = coordinator_a;
    coordinator_b.owner_id = "coordinator-b";
    coordinator_b.test_now_ms = 1100;
    const coordinator_b_fenced = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_b,
        .tick,
    );
    const coordinator_b_fenced_stats = coordinator_b_fenced.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_b_fenced_stats.lease_owned);
    try std.testing.expect(!coordinator_b_fenced_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_b_fenced_stats.lease_acquire_failures);
    try std.testing.expect(!coordinator_b_fenced.result.durableProgressed());

    coordinator_b.test_now_ms = 1301;
    const coordinator_b_takeover = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_b,
        .tick,
    );
    const coordinator_b_takeover_stats = coordinator_b_takeover.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(coordinator_b_takeover_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_b_takeover_stats.takeover_count);
    try std.testing.expectEqual(identityHash("coordinator-b"), coordinator_b_takeover_stats.owner_id_hash);

    coordinator_a.test_now_ms = 1302;
    const stale_coordinator_release = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        coordinator_a,
        .release,
    );
    try std.testing.expect(!stale_coordinator_release.released);
    try std.testing.expect(stale_coordinator_release.lease_owner_id_hash != 0);
    const stale_coordinator_release_stats = stale_coordinator_release.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!stale_coordinator_release_stats.has_lease);

    var worker_pool_a = CliConfig{
        .role = .worker_pool,
        .runtime_id = "service-pagerank-restart-worker-pool",
        .owner_id = "worker-pool-a",
        .worker_id = "worker-pool-unused",
        .coordinator_start_background_builds = false,
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 3,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 2000,
    };
    defer worker_pool_a.deinit(alloc);
    try worker_pool_a.worker_ids.appendSlice(alloc, &.{ "restart-pagerank-worker-a", "restart-pagerank-worker-b" });

    const worker_pool_a_start = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_a,
        .tick,
    );
    const worker_pool_a_start_stats = worker_pool_a_start.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(worker_pool_a_start_stats.has_lease);
    try std.testing.expectEqual(@as(usize, 2), worker_pool_a_start_stats.worker_count);
    try std.testing.expect(worker_pool_a_start.result.durableProgressed());

    var worker_pool_b = CliConfig{
        .role = .worker_pool,
        .runtime_id = "service-pagerank-restart-worker-pool",
        .owner_id = "worker-pool-b",
        .worker_id = "worker-pool-unused",
        .coordinator_start_background_builds = false,
        .lease_ttl_ms = 200,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 3,
        .test_hold_after_run_ms = 1,
        .test_now_ms = 2100,
    };
    defer worker_pool_b.deinit(alloc);
    try worker_pool_b.worker_ids.appendSlice(alloc, &.{ "restart-pagerank-worker-a", "restart-pagerank-worker-b" });

    const worker_pool_b_fenced = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_b,
        .tick,
    );
    const worker_pool_b_fenced_stats = worker_pool_b_fenced.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!worker_pool_b_fenced_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_b_fenced_stats.lease_acquire_failures);
    try std.testing.expect(!worker_pool_b_fenced.result.durableProgressed());

    worker_pool_b.test_now_ms = 2301;
    const worker_pool_b_takeover = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_b,
        .tick,
    );
    const worker_pool_b_takeover_stats = worker_pool_b_takeover.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(worker_pool_b_takeover_stats.has_lease);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_b_takeover_stats.takeover_count);
    try std.testing.expectEqual(identityHash("worker-pool-b"), worker_pool_b_takeover_stats.owner_id_hash);

    worker_pool_a.test_now_ms = 2302;
    const stale_worker_pool_release = try requestInProcessServiceMaintenanceForTest(
        alloc,
        &service,
        target,
        worker_pool_a,
        .release,
    );
    try std.testing.expect(!stale_worker_pool_release.released);
    try std.testing.expect(stale_worker_pool_release.lease_owner_id_hash != 0);
    const stale_worker_pool_release_stats = stale_worker_pool_release.stats orelse return error.MissingRuntimeStats;
    try std.testing.expect(!stale_worker_pool_release_stats.has_lease);

    var idle_rounds: usize = 0;
    for (0..160) |round| {
        coordinator_b.test_now_ms = 2400 + @as(u64, @intCast(round));
        const coordinator_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            coordinator_b,
        );
        worker_pool_b.test_now_ms = 2500 + @as(u64, @intCast(round));
        const worker_summary = try runServiceConfiguredWithRequester(
            InProcessGraphMetricService,
            &service,
            InProcessGraphMetricService.request,
            alloc,
            target,
            worker_pool_b,
        );

        const progressed = coordinator_summary.durable_progressed or worker_summary.durable_progressed;
        idle_rounds = if (progressed) 0 else idle_rounds + 1;
        const graph_entry = db.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("pagerank");
        defer status.deinit(alloc);
        if (status.state == .fresh) {
            try std.testing.expectEqual(target_generation, status.published_generation);
            try std.testing.expect(coordinator_summary.stats.shutdown);
            try std.testing.expect(worker_summary.stats.shutdown);
            try std.testing.expect(!coordinator_summary.stats.has_lease);
            try std.testing.expect(!worker_summary.stats.has_lease);
            const top = try graph_entry.index.graphMetricTopK("pagerank", 3);
            defer {
                for (top) |*score| score.deinit(alloc);
                alloc.free(top);
            }
            try std.testing.expect(top.len > 0);
            return;
        }
        if (idle_rounds >= 6) break;
    }

    return error.GraphMetricBuildNotComplete;
}

test "graph metric maintenance command rejects duplicate worker pool ids" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--db-path",    "/tmp/antfly-graph-metric-command-test",
        "--role",       "worker-pool",
        "--runtime-id", "runtime-a",
        "--owner-id",   "owner-a",
        "--worker-ids", "worker-a,worker-a",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.InvalidArguments, parseCli(alloc, &args));
}

test "graph metric maintenance command rejects worker id lists for single-owner roles" {
    const alloc = std.testing.allocator;
    const coordinator_argv = [_][*:0]const u8{
        "--db-path",    "/tmp/antfly-graph-metric-command-test",
        "--role",       "coordinator",
        "--runtime-id", "runtime-a",
        "--owner-id",   "owner-a",
        "--worker-ids", "worker-a,worker-b",
    };
    var coordinator_args = std.process.Args.Iterator.init(.{ .vector = coordinator_argv[0..] });
    try std.testing.expectError(error.InvalidArguments, parseCli(alloc, &coordinator_args));

    const worker_argv = [_][*:0]const u8{
        "--db-path",    "/tmp/antfly-graph-metric-command-test",
        "--role",       "worker",
        "--runtime-id", "runtime-a",
        "--owner-id",   "owner-a",
        "--worker-ids", "worker-a,worker-b",
    };
    var worker_args = std.process.Args.Iterator.init(.{ .vector = worker_argv[0..] });
    try std.testing.expectError(error.InvalidArguments, parseCli(alloc, &worker_args));
}

test "graph metric maintenance command rejects zero lease and maintenance budgets" {
    const alloc = std.testing.allocator;

    try expectParseCliInvalid(alloc, &.{
        "--db-path",      "/tmp/antfly-graph-metric-command-test",
        "--lease-ttl-ms", "0",
    });
    try expectParseCliInvalid(alloc, &.{
        "--db-path",    "/tmp/antfly-graph-metric-command-test",
        "--max-rounds", "0",
    });
    try expectParseCliInvalid(alloc, &.{
        "--db-path",     "/tmp/antfly-graph-metric-command-test",
        "--max-metrics", "0",
    });
    try expectParseCliInvalid(alloc, &.{
        "--db-path",   "/tmp/antfly-graph-metric-command-test",
        "--max-pages", "0",
    });
}

test "graph metric maintenance supervisor parses config and defaults workers" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--db-path",
        "/tmp/antfly-graph-metric-supervisor-test",
        "--executable",
        "/tmp/antfly",
        "--coordinator-owner-id",
        "coord-a",
        "--worker-pool-owner-id",
        "pool-a",
        "--ticks",
        "11",
        "--max-idle-ticks",
        "3",
        "--supervisor-rounds",
        "17",
        "--supervisor-idle-rounds",
        "2",
        "--tick-ms",
        "5",
        "--max-restarts",
        "4",
        "--summary-dir",
        "/tmp/summaries",
        "--max-pages",
        "7",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseSupervisorCli(alloc, &args);
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("/tmp/antfly-graph-metric-supervisor-test", parsed.db_path.?);
    try std.testing.expectEqualStrings("/tmp/antfly", parsed.executable.?);
    try std.testing.expectEqualStrings("coord-a", parsed.coordinator_owner_id);
    try std.testing.expectEqualStrings("pool-a", parsed.worker_pool_owner_id);
    try std.testing.expectEqual(@as(usize, 11), parsed.max_ticks);
    try std.testing.expectEqual(@as(usize, 3), parsed.max_idle_ticks);
    try std.testing.expectEqual(@as(usize, 17), parsed.max_supervisor_rounds);
    try std.testing.expectEqual(@as(usize, 2), parsed.max_supervisor_idle_rounds);
    try std.testing.expectEqual(@as(u64, 5), parsed.tick_interval_ms);
    try std.testing.expectEqual(@as(usize, 4), parsed.max_restarts);
    try std.testing.expectEqualStrings("/tmp/summaries", parsed.summary_dir);
    try std.testing.expectEqual(@as(usize, 7), parsed.max_pages_per_round);
    try std.testing.expectEqual(@as(usize, 2), parsed.worker_ids.items.len);
    try std.testing.expectEqualStrings("graph-metric-worker-a", parsed.worker_ids.items[0]);
    try std.testing.expectEqualStrings("graph-metric-worker-b", parsed.worker_ids.items[1]);
}

test "graph metric maintenance supervisor parses service target config" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--base-uri",
        "http://127.0.0.1:8080",
        "--group-id",
        "7",
        "--table-name",
        "docs",
        "--worker-ids",
        "worker-a,worker-b",
        "--max-pages",
        "5",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseSupervisorCli(alloc, &args);
    defer parsed.deinit(alloc);

    const target = (try parsed.target()) orelse return error.MissingServiceTarget;
    const service = switch (target) {
        .service => |service| service,
        .db_path => return error.InvalidArguments,
    };
    try std.testing.expectEqualStrings("http://127.0.0.1:8080", service.base_uri);
    try std.testing.expectEqual(@as(u64, 7), service.group_id);
    try std.testing.expectEqualStrings("docs", service.table_name);
    try std.testing.expectEqual(@as(usize, 2), parsed.worker_ids.items.len);
    try std.testing.expectEqual(@as(usize, 5), parsed.max_pages_per_round);
}

test "graph metric maintenance supervisor rejects duplicate worker pool ids" {
    const alloc = std.testing.allocator;
    const argv = [_][*:0]const u8{
        "--db-path",
        "/tmp/antfly-graph-metric-supervisor-test",
        "--worker-ids",
        "worker-a,worker-b,worker-a",
    };
    var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    try std.testing.expectError(error.InvalidArguments, parseSupervisorCli(alloc, &args));
}

test "graph metric maintenance supervisor rejects invalid service target combinations" {
    const alloc = std.testing.allocator;

    try expectParseSupervisorInvalid(alloc, &.{
        "--db-path",    "/tmp/db",
        "--base-uri",   "http://127.0.0.1:8080",
        "--group-id",   "7",
        "--table-name", "docs",
    });
    try expectParseSupervisorInvalid(alloc, &.{
        "--base-uri", "http://127.0.0.1:8080",
        "--group-id", "7",
    });
    try expectParseSupervisorInvalid(alloc, &.{
        "--base-uri",   "http://127.0.0.1:8080",
        "--table-name", "docs",
    });
}

test "graph metric maintenance supervisor rejects zero lease and maintenance budgets" {
    const alloc = std.testing.allocator;

    try expectParseSupervisorInvalid(alloc, &.{
        "--db-path",      "/tmp/antfly-graph-metric-supervisor-test",
        "--lease-ttl-ms", "0",
    });
    try expectParseSupervisorInvalid(alloc, &.{
        "--db-path",    "/tmp/antfly-graph-metric-supervisor-test",
        "--max-rounds", "0",
    });
    try expectParseSupervisorInvalid(alloc, &.{
        "--db-path",     "/tmp/antfly-graph-metric-supervisor-test",
        "--max-metrics", "0",
    });
    try expectParseSupervisorInvalid(alloc, &.{
        "--db-path",   "/tmp/antfly-graph-metric-supervisor-test",
        "--max-pages", "0",
    });
}

test "graph metric maintenance supervisor builds coordinator and worker pool argv" {
    const alloc = std.testing.allocator;
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "worker-a", "worker-b" });
    const cfg = SupervisorConfig{
        .executable = "/tmp/antfly",
        .coordinator_owner_id = "coord-owner",
        .worker_pool_owner_id = "pool-owner",
        .worker_ids = worker_ids,
        .lease_ttl_ms = 1234,
        .max_ticks = 11,
        .max_idle_ticks = 3,
        .tick_interval_ms = 5,
        .max_rounds = 2,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 7,
    };

    const db_target = SupervisorTarget{ .db_path = "/tmp/db" };
    var coordinator = try buildSupervisorChildArgv(alloc, "fallback-antfly", db_target, cfg, .coordinator, "/tmp/coordinator-summary.json");
    defer coordinator.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/antfly", coordinator.argv.items[0]);
    try std.testing.expectEqualStrings(process_subcommand, coordinator.argv.items[1]);
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--db-path", "/tmp/db"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--role", "coordinator"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--runtime-id", "coord-owner"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--owner-id", "coord-owner"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--coordinator-start-background-builds", "true"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--summary-file", "/tmp/coordinator-summary.json"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--local-db-writer-lock", "true"));
    try std.testing.expect(!argvContains(coordinator.argv.items, "--worker-ids"));

    var worker_pool = try buildSupervisorChildArgv(alloc, "fallback-antfly", db_target, cfg, .worker_pool, null);
    defer worker_pool.deinit(alloc);
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--role", "worker_pool"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--runtime-id", "pool-owner"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--owner-id", "pool-owner"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--worker-ids", "worker-a,worker-b"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--coordinator-start-background-builds", "false"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--lease-ttl-ms", "1234"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--ticks", "11"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-idle-ticks", "3"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--tick-ms", "5"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-rounds", "2"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-metrics", "4"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-pages", "7"));
}

test "graph metric maintenance supervisor builds service child argv without local writer guard" {
    const alloc = std.testing.allocator;
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "worker-a", "worker-b" });
    const cfg = SupervisorConfig{
        .executable = "/tmp/antfly",
        .coordinator_owner_id = "coord-owner",
        .worker_pool_owner_id = "pool-owner",
        .worker_ids = worker_ids,
        .max_ticks = 11,
        .max_idle_ticks = 3,
        .max_pages_per_round = 7,
    };
    const service_target = SupervisorTarget{ .service = .{
        .base_uri = "http://127.0.0.1:8080",
        .group_id = 7,
        .table_name = "docs",
    } };

    var coordinator = try buildSupervisorChildArgv(alloc, "fallback-antfly", service_target, cfg, .coordinator, "/tmp/coordinator-summary.json");
    defer coordinator.deinit(alloc);
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--base-uri", "http://127.0.0.1:8080"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--group-id", "7"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--table-name", "docs"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--role", "coordinator"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--summary-file", "/tmp/coordinator-summary.json"));
    try std.testing.expect(!argvContains(coordinator.argv.items, "--db-path"));
    try std.testing.expect(!argvContains(coordinator.argv.items, "--local-db-writer-lock"));

    var worker_pool = try buildSupervisorChildArgv(alloc, "fallback-antfly", service_target, cfg, .worker_pool, "/tmp/worker-summary.json");
    defer worker_pool.deinit(alloc);
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--base-uri", "http://127.0.0.1:8080"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--role", "worker_pool"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--worker-ids", "worker-a,worker-b"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-pages", "7"));
    try std.testing.expect(!argvContains(worker_pool.argv.items, "--db-path"));
    try std.testing.expect(!argvContains(worker_pool.argv.items, "--local-db-writer-lock"));
}

test "graph metric maintenance launched child argv stays owner and budget scoped" {
    const alloc = std.testing.allocator;
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, &.{ "worker-a", "worker-b" });
    const cfg = SupervisorConfig{
        .executable = "/tmp/antfly",
        .coordinator_owner_id = "coord-owner",
        .worker_pool_owner_id = "pool-owner",
        .worker_ids = worker_ids,
        .lease_ttl_ms = 1234,
        .max_ticks = 11,
        .max_idle_ticks = 3,
        .tick_interval_ms = 5,
        .max_rounds = 2,
        .max_metrics_per_round = 4,
        .max_pages_per_round = 7,
    };

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

    const db_target = SupervisorTarget{ .db_path = "/tmp/db" };
    var coordinator = try buildSupervisorChildArgv(alloc, "fallback-antfly", db_target, cfg, .coordinator, null);
    defer coordinator.deinit(alloc);
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--role", "coordinator"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--runtime-id", "coord-owner"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--owner-id", "coord-owner"));
    try std.testing.expect(argvValueEquals(coordinator.argv.items, "--worker-id", "graph-metric-coordinator-unused"));
    try std.testing.expect(!argvContains(coordinator.argv.items, "--worker-ids"));
    try std.testing.expect(!argvContainsAny(coordinator.argv.items, forbidden[0..]));

    var worker_pool = try buildSupervisorChildArgv(alloc, "fallback-antfly", db_target, cfg, .worker_pool, null);
    defer worker_pool.deinit(alloc);
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--role", "worker_pool"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--runtime-id", "pool-owner"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--owner-id", "pool-owner"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--worker-id", "graph-metric-worker-pool-unused"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--worker-ids", "worker-a,worker-b"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--lease-ttl-ms", "1234"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-rounds", "2"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-metrics", "4"));
    try std.testing.expect(argvValueEquals(worker_pool.argv.items, "--max-pages", "7"));
    try std.testing.expect(!argvContainsAny(worker_pool.argv.items, forbidden[0..]));
}

test "graph metric maintenance supervisor restart policy is bounded" {
    try std.testing.expect(shouldRestartSupervisorAttempt(true, 0, 1));
    try std.testing.expect(!shouldRestartSupervisorAttempt(true, 1, 1));
    try std.testing.expect(!shouldRestartSupervisorAttempt(false, 0, 1));
    try std.testing.expect(childExitSucceeded(.{ .exited = true, .code = 0 }));
    try std.testing.expect(!childExitSucceeded(.{ .exited = true, .code = 1 }));
    try std.testing.expect(!childExitSucceeded(.{ .exited = false, .code = null }));
}

test "graph metric maintenance supervisor loops until global idle" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const sequence = [_]ChildRunSummary{
        fakeChild(false, 0),
        fakeChild(true, 0),
        fakeChild(false, 0),
        fakeChild(false, 0),
    };
    var runner = FakeSupervisorChildRunner{ .sequence = sequence[0..] };
    var workers = std.ArrayListUnmanaged([]const u8).empty;
    defer workers.deinit(alloc);
    try workers.appendSlice(alloc, &.{ "worker-a", "worker-b" });
    const summary = try runSupervisorConfiguredWithRunner(
        FakeSupervisorChildRunner,
        &runner,
        FakeSupervisorChildRunner.run,
        io_impl.io(),
        alloc,
        "antfly",
        .{ .db_path = "/tmp/db" },
        .{
            .worker_ids = workers,
            .max_ticks = 1,
            .max_idle_ticks = 1,
            .max_supervisor_rounds = 4,
            .max_supervisor_idle_rounds = 1,
            .tick_interval_ms = 0,
            .max_restarts = 0,
        },
    );
    try std.testing.expectEqual(@as(usize, 4), runner.calls);
    try std.testing.expectEqual(@as(usize, 2), summary.rounds_executed);
    try std.testing.expectEqual(@as(usize, 0), summary.restarts);
    try std.testing.expectEqual(@as(usize, 1), summary.idle_rounds);
    try std.testing.expectEqual(SupervisorExitReason.idle, summary.exit_reason);
    try std.testing.expect(summary.succeeded);
}

test "graph metric maintenance supervisor drives degree through child role argv" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-supervised-degree-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    {
        var db = try antfly.db.DB.open(alloc, path, .{
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
        for (0..8) |i| {
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
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "supervised-worker-a", "supervised-worker-b" };
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, workers[0..]);

    var runner = LoopbackSupervisorChildRunner{};
    const summary = try runSupervisorConfiguredWithRunner(
        LoopbackSupervisorChildRunner,
        &runner,
        LoopbackSupervisorChildRunner.run,
        io_impl.io(),
        alloc,
        "antfly",
        .{ .db_path = path },
        .{
            .coordinator_owner_id = "supervised-degree-coordinator",
            .worker_pool_owner_id = "supervised-degree-worker-pool",
            .worker_ids = worker_ids,
            .max_ticks = 8,
            .max_idle_ticks = 2,
            .max_supervisor_rounds = 80,
            .max_supervisor_idle_rounds = 1,
            .tick_interval_ms = 0,
            .max_restarts = 0,
            .max_rounds = 1,
            .max_metrics_per_round = 4,
            .max_pages_per_round = 2,
        },
    );
    try std.testing.expect(summary.succeeded);
    try std.testing.expectEqual(SupervisorExitReason.idle, summary.exit_reason);
    try std.testing.expect(runner.calls >= 2);
    const coordinator_telemetry = summary.coordinator.telemetry orelse return error.MissingChildTelemetry;
    try std.testing.expectEqual(RuntimeRole.coordinator, coordinator_telemetry.role);
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "supervised-degree-coordinator"),
        coordinator_telemetry.runtime_id_hash,
    );
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "supervised-degree-coordinator"),
        coordinator_telemetry.owner_id_hash,
    );
    try std.testing.expect(coordinator_telemetry.lease_key_hash != 0);
    try std.testing.expectEqual(@as(u64, 0), coordinator_telemetry.worker_id_hash);
    try std.testing.expectEqual(@as(usize, 0), coordinator_telemetry.worker_count);
    try std.testing.expect(coordinator_telemetry.lease_owned);
    try std.testing.expect(coordinator_telemetry.has_lease);
    try std.testing.expectEqual(@as(u64, 1), coordinator_telemetry.acquisition_count);
    try std.testing.expectEqual(@as(u64, 0), coordinator_telemetry.error_ticks);
    try std.testing.expect(!coordinator_telemetry.has_last_error);

    const worker_pool_telemetry = summary.worker_pool.telemetry orelse return error.MissingChildTelemetry;
    try std.testing.expectEqual(RuntimeRole.worker_pool, worker_pool_telemetry.role);
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "supervised-degree-worker-pool"),
        worker_pool_telemetry.runtime_id_hash,
    );
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "supervised-degree-worker-pool"),
        worker_pool_telemetry.owner_id_hash,
    );
    try std.testing.expect(worker_pool_telemetry.lease_key_hash != 0);
    try std.testing.expect(worker_pool_telemetry.worker_id_hash != 0);
    try std.testing.expectEqual(@as(usize, 2), worker_pool_telemetry.worker_count);
    try std.testing.expect(worker_pool_telemetry.lease_owned);
    try std.testing.expect(worker_pool_telemetry.has_lease);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_telemetry.acquisition_count);
    try std.testing.expectEqual(@as(u64, 0), worker_pool_telemetry.error_ticks);
    try std.testing.expect(!worker_pool_telemetry.has_last_error);

    var reader = try antfly.db.DB.open(alloc, path, .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reader.close();
    const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
    var status = try graph_entry.index.graphMetricStatus("degree");
    defer status.deinit(alloc);
    try std.testing.expectEqual(antfly.graph.GraphIndex.GraphMetricState.fresh, status.state);
    try std.testing.expectEqual(target_generation, status.published_generation);
}

test "graph metric maintenance supervisor stops at restart limit" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const sequence = [_]ChildRunSummary{
        fakeChild(null, 17),
        fakeChild(false, 0),
        fakeChild(false, 0),
        fakeChild(null, 42),
    };
    var runner = FakeSupervisorChildRunner{ .sequence = sequence[0..] };
    var workers = std.ArrayListUnmanaged([]const u8).empty;
    defer workers.deinit(alloc);
    try workers.appendSlice(alloc, &.{ "worker-a", "worker-b" });
    const summary = try runSupervisorConfiguredWithRunner(
        FakeSupervisorChildRunner,
        &runner,
        FakeSupervisorChildRunner.run,
        io_impl.io(),
        alloc,
        "antfly",
        .{ .db_path = "/tmp/db" },
        .{
            .worker_ids = workers,
            .max_ticks = 1,
            .max_idle_ticks = 1,
            .max_supervisor_rounds = 4,
            .max_supervisor_idle_rounds = 1,
            .tick_interval_ms = 0,
            .max_restarts = 1,
        },
    );
    try std.testing.expectEqual(@as(usize, 4), runner.calls);
    try std.testing.expectEqual(@as(usize, 2), summary.rounds_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.restarts);
    try std.testing.expectEqual(SupervisorExitReason.restart_limit, summary.exit_reason);
    try std.testing.expect(!summary.succeeded);
    try std.testing.expectEqual(@as(?u8, 42), summary.worker_pool.exit.code);
}

test "graph metric maintenance supervisor parses child durable progress" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try parseChildDurableProgressed(
        alloc,
        "{ \"role\": \"worker_pool\", \"durable_progressed\": true }",
    ));
    try std.testing.expect(!try parseChildDurableProgressed(
        alloc,
        "{ \"role\": \"coordinator\", \"durable_progressed\": false }",
    ));
    try std.testing.expectError(error.InvalidArguments, parseChildDurableProgressed(
        alloc,
        "{ \"role\": \"coordinator\" }",
    ));
}

test "graph metric maintenance supervisor parses child runtime telemetry" {
    const alloc = std.testing.allocator;
    const telemetry = try parseChildRuntimeTelemetry(alloc,
        \\{
        \\  "role": "worker_pool",
        \\  "durable_progressed": false,
        \\  "stats": {
        \\    "enabled": true,
        \\    "role": "worker_pool",
        \\    "runtime_id_hash": 11,
        \\    "owner_id_hash": 12,
        \\    "lease_key_hash": 13,
        \\    "worker_id_hash": 14,
        \\    "worker_count": 2,
        \\    "lease_owned": true,
        \\    "has_lease": true,
        \\    "acquisition_count": 1,
        \\    "takeover_count": 0,
        \\    "lease_acquire_failures": 0,
        \\    "lost_leases": 0,
        \\    "last_acquired_ms": 123,
        \\    "started": false,
        \\    "shutdown": false,
        \\    "notified": false,
        \\    "ticks_started": 3,
        \\    "ticks_completed": 3,
        \\    "durable_progress_ticks": 1,
        \\    "idle_ticks": 2,
        \\    "error_ticks": 0,
        \\    "last_error_name": null
        \\  }
        \\}
    );
    try std.testing.expectEqual(RuntimeRole.worker_pool, telemetry.role);
    try std.testing.expectEqual(@as(u64, 11), telemetry.runtime_id_hash);
    try std.testing.expectEqual(@as(u64, 12), telemetry.owner_id_hash);
    try std.testing.expectEqual(@as(u64, 13), telemetry.lease_key_hash);
    try std.testing.expectEqual(@as(u64, 14), telemetry.worker_id_hash);
    try std.testing.expectEqual(@as(usize, 2), telemetry.worker_count);
    try std.testing.expect(telemetry.lease_owned);
    try std.testing.expect(telemetry.has_lease);
    try std.testing.expectEqual(@as(u64, 1), telemetry.acquisition_count);
    try std.testing.expectEqual(@as(u64, 0), telemetry.takeover_count);
    try std.testing.expectEqual(@as(u64, 0), telemetry.lost_leases);
    try std.testing.expectEqual(@as(u64, 3), telemetry.ticks_started);
    try std.testing.expectEqual(@as(u64, 3), telemetry.ticks_completed);
    try std.testing.expectEqual(@as(u64, 2), telemetry.idle_ticks);
    try std.testing.expectEqual(@as(u64, 0), telemetry.error_ticks);
    try std.testing.expect(!telemetry.has_last_error);
}

test "graph metric maintenance command exits after configured idle streak" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-command-idle-db", .{tmp.sub_path});
    {
        var db = try antfly.db.DB.open(alloc, path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
    }

    const summary = try runConfigured(alloc, path, .{
        .role = .coordinator,
        .runtime_id = "command-idle-coordinator",
        .owner_id = "command-idle-coordinator",
        .worker_id = "command-idle-unused",
        .max_ticks = 10,
        .until_idle = true,
        .max_idle_ticks = 2,
    });
    try std.testing.expectEqual(RuntimeRole.coordinator, summary.role);
    try std.testing.expectEqual(@as(usize, 2), summary.ticks_executed);
    try std.testing.expectEqual(@as(usize, 2), summary.idle_streak);
    try std.testing.expectEqual(ExitReason.idle, summary.exit_reason);
    try std.testing.expect(!summary.durable_progressed);
    try std.testing.expect(summary.stats.enabled);
    try std.testing.expect(!summary.stats.started);
}

test "graph metric maintenance command summary exposes ownership telemetry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-command-telemetry-db", .{tmp.sub_path});
    {
        var db = try antfly.db.DB.open(alloc, path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
    }

    const coordinator_summary = try runConfigured(alloc, path, .{
        .role = .coordinator,
        .runtime_id = "command-telemetry-coordinator-runtime",
        .owner_id = "command-telemetry-coordinator-owner",
        .worker_id = "command-telemetry-unused-worker",
        .max_ticks = 1,
    });
    try std.testing.expectEqual(RuntimeRole.coordinator, coordinator_summary.role);
    try std.testing.expectEqual(RuntimeRole.coordinator, coordinator_summary.stats.role);
    try std.testing.expect(coordinator_summary.stats.enabled);
    try std.testing.expect(coordinator_summary.stats.lease_owned);
    try std.testing.expect(coordinator_summary.stats.has_lease);
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "command-telemetry-coordinator-runtime"),
        coordinator_summary.stats.runtime_id_hash,
    );
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "command-telemetry-coordinator-owner"),
        coordinator_summary.stats.owner_id_hash,
    );
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, graph_metric_runtime_mod.defaultLeaseKey(.coordinator)),
        coordinator_summary.stats.lease_key_hash,
    );
    try std.testing.expectEqual(@as(u64, 0), coordinator_summary.stats.worker_id_hash);
    try std.testing.expectEqual(@as(usize, 0), coordinator_summary.stats.worker_count);
    try std.testing.expectEqual(@as(u64, 1), coordinator_summary.stats.acquisition_count);
    try std.testing.expectEqual(@as(u64, 0), coordinator_summary.stats.takeover_count);
    try std.testing.expectEqual(@as(u64, 0), coordinator_summary.stats.lost_leases);
    try std.testing.expectEqual(@as(u64, 1), coordinator_summary.stats.ticks_started);
    try std.testing.expectEqual(@as(u64, 1), coordinator_summary.stats.ticks_completed);
    try std.testing.expectEqual(@as(u64, 1), coordinator_summary.stats.idle_ticks);
    try std.testing.expectEqual(@as(u64, 0), coordinator_summary.stats.error_ticks);
    try std.testing.expectEqual(@as(?[]const u8, null), coordinator_summary.stats.last_error_name);

    const workers = [_][]const u8{ "command-telemetry-worker-a", "command-telemetry-worker-b" };
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, workers[0..]);

    const worker_pool_summary = try runConfigured(alloc, path, .{
        .role = .worker_pool,
        .runtime_id = "command-telemetry-worker-runtime",
        .owner_id = "command-telemetry-worker-owner",
        .worker_id = "command-telemetry-unused-single-worker",
        .worker_ids = worker_ids,
        .max_ticks = 1,
    });
    try std.testing.expectEqual(RuntimeRole.worker_pool, worker_pool_summary.role);
    try std.testing.expectEqual(RuntimeRole.worker_pool, worker_pool_summary.stats.role);
    try std.testing.expect(worker_pool_summary.stats.enabled);
    try std.testing.expect(worker_pool_summary.stats.lease_owned);
    try std.testing.expect(worker_pool_summary.stats.has_lease);
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "command-telemetry-worker-runtime"),
        worker_pool_summary.stats.runtime_id_hash,
    );
    try std.testing.expectEqual(
        std.hash.Wyhash.hash(0, "command-telemetry-worker-owner"),
        worker_pool_summary.stats.owner_id_hash,
    );
    try std.testing.expect(worker_pool_summary.stats.lease_key_hash != 0);
    try std.testing.expect(worker_pool_summary.stats.worker_id_hash != 0);
    try std.testing.expectEqual(@as(usize, 2), worker_pool_summary.stats.worker_count);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_summary.stats.acquisition_count);
    try std.testing.expectEqual(@as(u64, 0), worker_pool_summary.stats.takeover_count);
    try std.testing.expectEqual(@as(u64, 0), worker_pool_summary.stats.lost_leases);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_summary.stats.ticks_started);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_summary.stats.ticks_completed);
    try std.testing.expectEqual(@as(u64, 1), worker_pool_summary.stats.idle_ticks);
    try std.testing.expectEqual(@as(u64, 0), worker_pool_summary.stats.error_ticks);
    try std.testing.expectEqual(@as(?[]const u8, null), worker_pool_summary.stats.last_error_name);
}

test "graph metric maintenance command drives split degree through db-open runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/graph-metric-command-db", .{tmp.sub_path});

    var target_generation: u64 = 0;
    {
        var db = try antfly.db.DB.open(alloc, path, .{
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
        for (0..8) |i| {
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
        target_generation = graph_entry.index.edge_generation;
    }

    const workers = [_][]const u8{ "command-worker-a", "command-worker-b" };
    var worker_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer worker_ids.deinit(alloc);
    try worker_ids.appendSlice(alloc, workers[0..]);

    const coordinator = CliConfig{
        .role = .coordinator,
        .runtime_id = "command-coordinator",
        .owner_id = "command-coordinator",
        .worker_id = "command-coordinator-unused",
        .max_ticks = 1,
        .max_pages_per_round = 1,
    };
    const worker_pool = CliConfig{
        .role = .worker_pool,
        .runtime_id = "command-worker-pool",
        .owner_id = "command-worker-pool",
        .worker_id = "command-worker-unused",
        .worker_ids = worker_ids,
        .max_ticks = 1,
        .max_pages_per_round = 2,
    };

    var fresh = false;
    for (0..80) |_| {
        _ = try runConfigured(alloc, path, coordinator);
        const worker_summary = try runConfigured(alloc, path, worker_pool);
        try std.testing.expectEqual(RuntimeRole.worker_pool, worker_summary.role);
        try std.testing.expect(!worker_summary.stats.started);
        _ = try runConfigured(alloc, path, coordinator);

        var reader = try antfly.db.DB.open(alloc, path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reader.close();
        const graph_entry = reader.core.graphIndex("graph_idx") orelse return error.IndexNotFound;
        var status = try graph_entry.index.graphMetricStatus("degree");
        defer status.deinit(alloc);
        if (status.state == .fresh and status.published_generation == target_generation) {
            fresh = true;
            break;
        }
    }
    try std.testing.expect(fresh);
}
