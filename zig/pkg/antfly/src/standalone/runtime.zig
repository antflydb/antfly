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
const lease_executor = @import("lease_executor.zig");
const builtin = @import("builtin");
const platform_sync = @import("antfly_platform").sync;
const httpx = @import("httpx");
const antfly = @import("runtime_root.zig");
const group_ids = @import("../common/group_ids.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");
const fs_paths = @import("../common/fs_paths.zig");
const process_memory_budget = @import("../common/process_memory_budget.zig");
const preload_model_spec = @import("../common/preload_model_spec.zig");
const platform_time = @import("antfly_platform").time;
const platform = @import("antfly_platform");
const inference_bridge = @import("inference_bridge.zig");
const inference_connection_abi = @import("../inference_connection_abi.zig");
const internal_service_auth = @import("../api/internal_service_auth.zig");
const runtime_http_abi = @import("../runtime_http_abi.zig");
const inline_inference_codegen = builtin.is_test;
const inference_host = if (inline_inference_codegen) @import("inference_host.zig") else struct {};

const ApiHttpServer = antfly.public_api.ApiHttpServer;
const ApiKernelHandler = antfly.public_api.kernel_bridge.HttpxHandler;
const http_common = antfly.common.http;
const public_api_max_requests_per_connection: u32 = 64;
const public_api_max_body_size: usize = antfly.common.http.default_max_request_bytes;
const local_schema_migration_finalize_interval_ms: u64 = std.time.ms_per_s;

const LocalInferenceConnectionContext = struct {
    handle: *anyopaque,
};

const LocalSchemaProgressProvider = struct {
    ptr: *anyopaque,
    collect: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const antfly.metadata.TableRecord,
        ranges: []const antfly.metadata.RangeRecord,
    ) anyerror!antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot,
};
const default_public_port: u16 = 8080;
const cors_default_methods = [_][]const u8{ "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH" };
const cors_default_headers = [_][]const u8{ "Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin" };
const cors_default_exposed_headers = [_][]const u8{
    "X-Request-ID",
    "Retry-After",
    "Deprecation",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
};
const cors_default_max_age: u32 = 3600;
const antfarm_max_file_bytes: usize = 64 * 1024 * 1024;
const standalone_session_ttl_ns: u64 = std.time.ns_per_hour;
const standalone_session_cleanup_interval_ns: u64 = std.time.ns_per_min;
const standalone_session_max_count: usize = 1024;
const standalone_session_max_record_bytes: usize = 16 * 1024 * 1024;
const standalone_session_savepoint_limit: usize = 64;
const antfarm_installed_asset_root = "../share/antfly/antfarm";
const antfarm_asset_roots = [_][]const u8{
    "zig/pkg/antfly/antfarm",
    "pkg/antfly/antfarm",
    "/usr/share/antfly/antfarm",
    "antfarm",
};
const ha_lease_poll_interval_ns: u64 = 2 * std.time.ns_per_s;
const ha_lease_request_timeout_ms: u32 = 1_000;
const ha_lease_timing_jitter_ns: u64 = std.time.ns_per_s;
const ha_lease_min_grace_ms: u64 = 10_000;
const ha_lease_api_host_env = "ANTFLY_HA_LEASE_API_HOST";
const ha_lease_default_api_host = "kubernetes.default.svc";
const ha_lease_max_response_bytes: usize = 256 * 1024;
const internal_service_secret_key = "antfly.internal_service.secret";
const internal_service_issuer_key = "antfly.internal_service.issuer";

const StandaloneHttpContext = struct {
    api_server: ?*ApiHttpServer,
    cors_config: ?*const antfly.common.config.Config.CorsConfig = null,
};

const HALeaseAPIEndpoint = struct {
    host: []const u8,
    port: []const u8,
};

fn haLeaseAPIEndpoint(env: *const std.process.Environ.Map) !HALeaseAPIEndpoint {
    return .{
        .host = env.get(ha_lease_api_host_env) orelse ha_lease_default_api_host,
        .port = env.get("KUBERNETES_SERVICE_PORT_HTTPS") orelse env.get("KUBERNETES_SERVICE_PORT") orelse return error.HALeaseAPIPortMissing,
    };
}

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    experimental: bool = false,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    control_tick_ms: u64 = antfly.raft.RuntimeCadence.default_control_tick_ms,
    local_node_id: ?u64 = null,
    auth_enabled: ?bool = null,
    ard_base_url: ?[]const u8 = null,
    ard_publisher_domain: ?[]const u8 = null,
    ard_display_name: ?[]const u8 = null,
    ard_public_catalog_enabled: bool = false,
    inference_models_dir: ?[]const u8 = null,
    inference_ml_dir: ?[]const u8 = null,
    inference_host_budget_mb: usize = 0,
    inference_backend_budget_mb: usize = 0,
    inference_combined_budget_mb: usize = 0,
    inference_kv_budget_mb: usize = 0,
    inference_scratch_budget_mb: usize = 0,
    inference_process_memory_budget_mb: ?usize = null,
    inference_kernel_jit_mode: ?antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode = null,
    inference_preload_models: std.ArrayListUnmanaged(inference_bridge.WarmModel) = .empty,
    data_dir: ?[]const u8 = null,
    storage_engine: ?antfly.common.config.StorageEngine = null,
    storage_path: ?[]const u8 = null,
    storage_fsync: ?bool = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    extension_package_store_dir: ?[]const u8 = null,
    secret_store_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    ha_primary_log: ?[]const u8 = null,
    ha_primary_slots: ?[]const u8 = null,
    ha_primary_node_id: ?[]const u8 = null,
    ha_seed_capture_root: ?[]const u8 = null,
    ha_fence_wal: ?[]const u8 = null,
    ha_former_primary_log: ?[]const u8 = null,
    admin_token_env: ?[]const u8 = null,
    ha_retention_max_lag_lsn: ?u64 = null,
    ha_retention_max_retained_bytes: ?u64 = null,
    ha_retention_max_retained_age_ns: ?u64 = null,
    ha_sync_mode: ?antfly.ha.primary.DurabilityMode = null,
    ha_sync_selection: ?antfly.ha.primary.StandbySelection = null,
    ha_sync_required: ?usize = null,
    ha_sync_failure_policy: ?antfly.ha.primary.FailurePolicy = null,
    ha_sync_standby_names: std.ArrayListUnmanaged([]const u8) = .empty,
    ha_standby_log: ?[]const u8 = null,
    ha_standby_progress: ?[]const u8 = null,
    ha_standby_node_id: ?[]const u8 = null,
    ha_standby_upstream_url: ?[]const u8 = null,
    ha_standby_slot: ?[]const u8 = null,
    ha_startup_target_root: ?[]const u8 = null,
    ha_startup_topology_id: ?[]const u8 = null,
    ha_startup_topology_generation: ?u64 = null,
    ha_startup_generation: ?[]const u8 = null,
    ha_startup_target_pvc_name: ?[]const u8 = null,
    ha_startup_target_pvc_uid: ?[]const u8 = null,
    ha_startup_manifest_sha256: ?[]const u8 = null,
    ha_startup_aggregate_sha256: ?[]const u8 = null,
    ha_startup_seed_receipt_sha256: ?[]const u8 = null,
    ha_startup_capture_receipt_sha256: ?[]const u8 = null,
    ha_startup_materialized_receipt_sha256: ?[]const u8 = null,
    ha_startup_materialized_aggregate_sha256: ?[]const u8 = null,
    ha_startup_target_local_node_id: ?u64 = null,
    ha_startup_target_replica_id: ?u64 = null,
    ha_cluster_id: ?u64 = null,
    ha_shard_id: ?u64 = null,
    ha_table_id: ?u64 = null,
    ha_timeline_id: ?u64 = null,
    ha_epoch: ?u64 = null,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.secret_store_paths.deinit(alloc);
        self.ha_sync_standby_names.deinit(alloc);
        self.inference_preload_models.deinit(alloc);
        self.* = undefined;
    }

    fn primarySecretStorePath(self: *const CliConfig) ?[]const u8 {
        if (self.secret_store_paths.items.len == 0) return null;
        return self.secret_store_paths.items[0];
    }
};

// JSON is used only as a versioned, language-neutral payload inside the
// inference CreateContext. The distributed unit owns config parsing; the
// inference unit owns translation into inference runtime types.
const InferenceRuntimeConfigWire = struct {
    const KernelJit = struct {
        mode: antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode = .off,
        cache_dir: ?[]const u8 = null,
        max_cache_bytes_mb: usize = 1024,
        preload_budget_ms: u64 = 300_000,
    };
    const PromptCache = struct {
        enabled: bool = false,
        mode: antfly.common.config.Config.InferenceConfig.PromptCacheConfig.Mode = .block_hash,
        max_bytes_mb: usize = 512,
        min_tokens: usize = 64,
        ttl_ms: u64 = 300_000,
    };

    max_concurrent_requests: ?usize = null,
    kernel_jit: KernelJit = .{},
    prompt_cache: PromptCache = .{},
};

fn resolveKernelJitMode(
    configured: antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode,
    environment: ?[]const u8,
    cli: ?antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode,
) !antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode {
    if (cli) |mode| return mode;
    if (environment) |raw|
        return std.meta.stringToEnum(
            antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode,
            raw,
        ) orelse error.InvalidArguments;
    return configured;
}

const RuntimeLeaseWatchdog = struct {
    const ObservationFailureStage = enum { fetch, validation };

    watchdog: antfly.ha.kubernetes_lease_watchdog.Watchdog,
    io: std.Io,
    executor: lease_executor.LeaseExecutor,
    uri: []u8,
    token_path: []const u8,
    lease_name: []const u8,
    lease_namespace: []const u8,
    stable_topology_id: []const u8,
    node_id: []const u8,
    pod_uid: []const u8,
    process_boot_id: [64]u8,
    owned_data_generation: ?[]u8 = null,
    proof_active: std.atomic.Value(bool) = .init(false),
    proof_capability_deadline_ns: std.atomic.Value(u64) = .init(0),
    proof_authority_deadline_ns: std.atomic.Value(u64) = .init(0),
    proof_transitions: std.atomic.Value(u64) = .init(0),
    proof_mutex: std.atomic.Mutex = .unlocked,
    sentinel_persisted: bool = false,
    next_poll_ns: u64 = 0,
    fetch_failure_logged: bool = false,
    validation_failure_logged: bool = false,

    fn initFromEnv(
        alloc: std.mem.Allocator,
        io: std.Io,
        env: *const std.process.Environ.Map,
        cli: CliConfig,
        pod_uid: ?[]const u8,
    ) !?RuntimeLeaseWatchdog {
        const lease_name = env.get("ANTFLY_HA_LEASE_NAME") orelse return null;
        const namespace = env.get("ANTFLY_HA_LEASE_NAMESPACE") orelse return error.HALeaseNamespaceMissing;
        const api_endpoint = try haLeaseAPIEndpoint(env);
        const grace_raw = env.get("ANTFLY_HA_LEASE_GRACE_MS") orelse return error.HALeaseGraceMissing;
        const sentinel_path = env.get("ANTFLY_HA_LEASE_SENTINEL_PATH") orelse return error.HALeaseSentinelMissing;
        const topology_id = env.get("ANTFLY_HA_LEASE_TOPOLOGY_ID") orelse return error.HALeaseTopologyIDMissing;
        const resolved_pod_uid = pod_uid orelse return error.HALeasePodUIDMissing;
        const node_id = cli.ha_primary_node_id orelse cli.ha_standby_node_id orelse return error.HALeaseNodeIDMissing;
        const grace_ms = std.fmt.parseInt(u64, grace_raw, 10) catch return error.HALeaseGraceInvalid;
        if (grace_ms < ha_lease_min_grace_ms or grace_ms >= 30_000) return error.HALeaseGraceInvalid;
        const requested_generation = cli.ha_startup_generation orelse "initial";
        const sentinel_generation = try antfly.ha.kubernetes_lease_watchdog.loadSentinelGenerationAlloc(alloc, io, sentinel_path);
        defer if (sentinel_generation) |generation| alloc.free(generation);
        const repaired_generation = if (sentinel_generation != null)
            try antfly.ha.kubernetes_lease_watchdog.loadValidatedRepairGenerationAlloc(alloc, io, sentinel_path, topology_id, node_id)
        else
            null;
        errdefer if (repaired_generation) |generation| alloc.free(generation);
        if (repaired_generation) |generation| {
            try antfly.ha.kubernetes_lease_watchdog.rotateSentinelAfterValidatedRepair(
                alloc,
                io,
                sentinel_path,
                topology_id,
                node_id,
                generation,
            );
        }
        const data_generation = repaired_generation orelse requested_generation;
        var entropy: [32]u8 = undefined;
        try io.randomSecure(&entropy);
        const process_boot_id = std.fmt.bytesToHex(entropy, .lower);
        const scope = antfly.ha.kubernetes_lease_watchdog.Scope{
            .topology_id = topology_id,
            .node_id = node_id,
            .data_generation = data_generation,
            .process_boot_id = &process_boot_id,
        };
        var executor = try lease_executor.LeaseExecutor.init(
            alloc,
            io,
            env.get("ANTFLY_HA_LEASE_CA_PATH") orelse antfly.ha.kubernetes_lease_watchdog.service_account_ca_path,
            ha_lease_max_response_bytes,
        );
        errdefer executor.deinit();
        return .{
            .watchdog = try .init(.{
                .scope = scope,
                .grace_ns = grace_ms * std.time.ns_per_ms,
                .sentinel_path = sentinel_path,
            }, sentinel_generation, repaired_generation),
            .io = io,
            .executor = executor,
            .uri = try antfly.ha.kubernetes_lease_watchdog.leaseURLAlloc(alloc, api_endpoint.host, api_endpoint.port, namespace, lease_name),
            .token_path = env.get("ANTFLY_HA_LEASE_TOKEN_PATH") orelse antfly.ha.kubernetes_lease_watchdog.service_account_token_path,
            .lease_name = lease_name,
            .lease_namespace = namespace,
            .stable_topology_id = topology_id,
            .node_id = node_id,
            .pod_uid = resolved_pod_uid,
            .process_boot_id = process_boot_id,
            .owned_data_generation = repaired_generation,
            .sentinel_persisted = sentinel_generation != null and std.mem.eql(u8, sentinel_generation.?, data_generation),
        };
    }

    fn proofSource(self: *const RuntimeLeaseWatchdog) antfly.ha.http_admin.Server.AuthOptions.LeaseWatchdogProofSource {
        return .{ .ptr = self, .snapshot_fn = proofSnapshot };
    }

    fn repairReceiptSink(self: *RuntimeLeaseWatchdog) antfly.ha.http_admin.Server.AuthOptions.RepairReceiptSink {
        return .{ .ptr = self, .record_fn = recordRepairReceipt };
    }

    fn recordRepairReceipt(ptr: *anyopaque, result: antfly.ha.rejoin.RewindResult) !void {
        const self: *RuntimeLeaseWatchdog = @ptrCast(@alignCast(ptr));
        _ = try antfly.ha.kubernetes_lease_watchdog.persistRepairReceipt(
            self.executor.alloc,
            self.io,
            self.watchdog.cfg.sentinel_path,
            self.stable_topology_id,
            self.node_id,
            result.target_timeline_id,
            result.target_epoch,
            result.current_last_lsn,
            "",
        );
    }

    fn proofSnapshot(ptr: *const anyopaque, alloc: std.mem.Allocator) !?antfly.admin.HALeaseWatchdogProof {
        const self: *RuntimeLeaseWatchdog = @ptrCast(@alignCast(@constCast(ptr)));
        platform_sync.lockYielding(&self.proof_mutex);
        defer self.proof_mutex.unlock();
        const deadline = self.proof_authority_deadline_ns.load(.acquire);
        const capability_deadline = self.proof_capability_deadline_ns.load(.acquire);
        const now = platform_time.authorityNs();
        const authority_remaining_ms: i64 = if (deadline > now)
            @intCast(@min(
                (deadline - now) / std.time.ns_per_ms,
                self.watchdog.cfg.grace_ns / std.time.ns_per_ms,
            ))
        else
            0;
        return .{
            .capability_version = 1,
            .active = self.proof_active.load(.acquire) and capability_deadline != 0 and now < capability_deadline,
            // Rounding down makes the proof conservative at the sub-ms edge.
            .authority_granted = authority_remaining_ms > 0,
            .authority_remaining_ms = authority_remaining_ms,
            .lease_name = self.lease_name,
            .lease_namespace = self.lease_namespace,
            .stable_topology_id = self.stable_topology_id,
            .local_node_id = self.node_id,
            // The parsed JSON buffer is released after every poll. Return an
            // owned copy of the fixed watchdog snapshot so response encoding
            // can never race a later Lease observation.
            .observed_holder_node_id = try alloc.dupe(u8, self.watchdog.observedHolder()),
            .pod_uid = self.pod_uid,
            .process_boot_id = &self.process_boot_id,
            .observed_lease_transitions = @intCast(self.proof_transitions.load(.acquire)),
            .max_fence_latency_ms = @intCast(self.watchdog.cfg.grace_ns / std.time.ns_per_ms),
        };
    }

    fn deinit(self: *RuntimeLeaseWatchdog, alloc: std.mem.Allocator) void {
        self.executor.deinit();
        alloc.free(self.uri);
        if (self.owned_data_generation) |generation| alloc.free(generation);
        self.* = undefined;
    }

    fn poll(
        self: *RuntimeLeaseWatchdog,
        alloc: std.mem.Allocator,
        data_server: *antfly.data.runtime.DataServer,
    ) !void {
        const io = self.executor.io;
        const poll_started_ns = platform_time.authorityNs();
        if (poll_started_ns < self.next_poll_ns) return;
        self.next_poll_ns = poll_started_ns +| ha_lease_poll_interval_ns;
        const body = antfly.ha.kubernetes_lease_watchdog.fetchLeaseAlloc(
            alloc,
            io,
            self.executor.executor(),
            self.uri,
            self.token_path,
            ha_lease_request_timeout_ms,
        ) catch |err| {
            platform_sync.lockYielding(&self.proof_mutex);
            const failure = self.noteObservationFailureLocked(.fetch, err, platform_time.authorityNs());
            self.proof_mutex.unlock();
            return try self.applyDecision(alloc, io, data_server, failure);
        };
        defer alloc.free(body);
        const observed_monotonic_ns = platform_time.authorityNs();
        platform_sync.lockYielding(&self.proof_mutex);
        const decision = self.watchdog.observe(
            alloc,
            body,
            platform_time.realtimeNs(),
            observed_monotonic_ns,
        ) catch |err| {
            // A syntactically valid HTTP response that cannot prove the exact
            // topology/generation is not current capability evidence.
            const failure = self.noteObservationFailureLocked(.validation, err, platform_time.authorityNs());
            self.proof_mutex.unlock();
            return try self.applyDecision(alloc, io, data_server, failure);
        };
        // `active` is capability evidence, not write authority. A standby
        // reports active after validating the shared Lease while another node
        // still holds it, allowing the controller to certify the exact process
        // before an in-place promotion.
        if (decision == .observed or decision == .pending_authority or decision == .authorized or decision == .grace) {
            self.proof_transitions.store(self.watchdog.last_generation, .release);
            self.proof_active.store(true, .release);
            self.proof_capability_deadline_ns.store(observed_monotonic_ns +| self.watchdog.cfg.grace_ns, .release);
        } else if (decision == .waiting) {
            // In particular, an expired pre-transfer Lease must never refresh
            // the standby's Active proof.
            self.proof_active.store(false, .release);
            self.proof_capability_deadline_ns.store(0, .release);
        }
        self.proof_mutex.unlock();
        try self.applyDecision(alloc, io, data_server, decision);
    }

    const ObservationFailureTransition = struct {
        decision: antfly.ha.kubernetes_lease_watchdog.Decision,
        should_log: bool,
    };

    // Called with proof_mutex held. Separate the deterministic fail-closed
    // state transition from logging so tests can exercise expected failure
    // paths without emitting a real production error. The wrapper below
    // remains the sole logging boundary.
    fn transitionObservationFailureLocked(
        self: *RuntimeLeaseWatchdog,
        stage: ObservationFailureStage,
        now_ns: u64,
    ) ObservationFailureTransition {
        const should_log = switch (stage) {
            .fetch => first: {
                const first_failure = !self.fetch_failure_logged;
                self.fetch_failure_logged = true;
                break :first first_failure;
            },
            .validation => first: {
                self.proof_active.store(false, .release);
                self.proof_capability_deadline_ns.store(0, .release);
                const first_failure = !self.validation_failure_logged;
                self.validation_failure_logged = true;
                break :first first_failure;
            },
        };
        return .{
            .decision = self.watchdog.noteAPIFailure(now_ns),
            .should_log = should_log,
        };
    }

    // Called with proof_mutex held. Each failure stage logs at most once per
    // runtime process and includes only the Zig error name: bearer tokens,
    // request headers, and response bodies are never rendered.
    fn noteObservationFailureLocked(
        self: *RuntimeLeaseWatchdog,
        stage: ObservationFailureStage,
        err: anyerror,
        now_ns: u64,
    ) antfly.ha.kubernetes_lease_watchdog.Decision {
        const transition = self.transitionObservationFailureLocked(stage, now_ns);
        if (transition.should_log) switch (stage) {
            .fetch => std.log.err("HA Lease watchdog Kubernetes Lease fetch failed err={s}", .{@errorName(err)}),
            .validation => std.log.err("HA Lease watchdog Lease response validation failed err={s}", .{@errorName(err)}),
        };
        return transition.decision;
    }

    fn runIndependent(
        self: *RuntimeLeaseWatchdog,
        alloc: std.mem.Allocator,
        io: std.Io,
        data_server: *antfly.data.runtime.DataServer,
        stop: *const std.atomic.Value(bool),
        failed: *std.atomic.Value(bool),
    ) void {
        while (!stop.load(.acquire)) {
            self.poll(alloc, data_server) catch {
                failed.store(true, .release);
                return;
            };
            io.sleep(std.Io.Duration.fromMilliseconds(50), .awake) catch {
                failed.store(true, .release);
                return;
            };
        }
    }

    fn applyDecision(
        self: *RuntimeLeaseWatchdog,
        alloc: std.mem.Allocator,
        io: std.Io,
        data_server: *antfly.data.runtime.DataServer,
        decision: antfly.ha.kubernetes_lease_watchdog.Decision,
    ) !void {
        switch (decision) {
            .waiting, .observed, .pending_authority, .grace => {},
            .authorized => {
                self.proof_transitions.store(self.watchdog.last_generation, .release);
                self.proof_active.store(true, .release);
                self.proof_authority_deadline_ns.store(self.watchdog.local_deadline_ns, .release);
                data_server.ha_public_gate_state.publishExternalAuthorityUntil(true, self.watchdog.local_deadline_ns);
            },
            .fence => {
                // Fence transitions wait for every mutation that passed the
                // preflight authority gate to finish its local commit and HA
                // append before freezing the durable tail.
                var mutation_lease = data_server.ha_mutation_barrier.acquireExclusive();
                defer mutation_lease.release();
                platform_sync.lockYielding(&data_server.ha_state_mutex);
                defer data_server.ha_state_mutex.unlock();
                self.proof_active.store(false, .release);
                self.proof_capability_deadline_ns.store(0, .release);
                self.proof_authority_deadline_ns.store(0, .release);
                data_server.ha_public_gate_state.publishExternalAuthority(false);
                data_server.ha_public_gate_state.publishPrimaryFence(true);
                if (!self.sentinel_persisted) {
                    try self.watchdog.persistFence(alloc, io);
                    self.sentinel_persisted = true;
                }
            },
        }
    }
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    local_metadata_catalog_path: []u8,
    snapshot_root_dir: []u8,
    extension_package_store_dir: []u8,
    secret_store_path: []u8,
    auth_store_root_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.local_metadata_catalog_path);
        alloc.free(self.snapshot_root_dir);
        alloc.free(self.extension_package_store_dir);
        alloc.free(self.secret_store_path);
        alloc.free(self.auth_store_root_dir);
    }
};

const StandaloneHealthSource = struct {
    data_server: *antfly.data.runtime.DataServer,
    unified_api_ready: *const std.atomic.Value(bool),
    supervisor: *const antfly.common.runtime_lifecycle.RuntimeSupervisor,
    startup_checkpoint_lsn: ?u64 = null,
    handler: *const ApiKernelHandler,
    unified_lifecycle: *UnifiedServerLifecycle,

    fn readiness(self: *StandaloneHealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    fn metricsWriter(self: *StandaloneHealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *StandaloneHealthSource = @ptrCast(@alignCast(ptr));
        if (self.supervisor.currentState() != .ready) return false;
        switch (self.data_server.ha_public_gate_state.currentRole()) {
            .transitioning, .fenced_primary => return false,
            .disabled, .standby, .primary => {},
        }
        if (self.data_server.http_server) |*api_server| {
            if (api_server.storageMaintenanceExclusiveActive()) return false;
        }
        if (self.startup_checkpoint_lsn) |checkpoint_lsn| {
            self.data_server.ha_public_gate_state.checkRead(.{
                .consistency = .at_least_lsn,
                .required_lsn = checkpoint_lsn,
            }) catch return false;
        }
        if (self.unified_lifecycle.httpRuntimeStats()) |http_runtime| {
            if (!http_runtime.healthy) return false;
        }
        return standaloneReadyFromState(
            self.data_server.http_server != null,
            self.unified_api_ready.load(.acquire),
        );
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *StandaloneHealthSource = @ptrCast(@alignCast(ptr));
        var data_health = antfly.data.runtime.HealthSource{ .data_server = self.data_server };
        try data_health.metricsWriter().writeMetrics(writer);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_runtime_supervisor_state", "gauge", "Runtime supervisor phase (0 starting, 1 ready, 2 quiescing, 3 failed, 4 stopped)", @intFromEnum(self.supervisor.currentState()));
        try antfly.common.health_server.appendPromMetric(writer, "antfly_runtime_supervisor_cancelled", "gauge", "Whether process-level runtime cancellation has been requested", @intFromBool(self.supervisor.token().isCancelled()));

        const handler = antfly.public_api.kernel_bridge.handlerStats(self.handler);
        try antfly.common.request_admission.appendPrometheusMetrics(writer, .query, .{
            .capacity = handler.query_capacity,
            .in_flight = handler.query_in_flight,
            .peak_in_flight = handler.query_peak_in_flight,
            .rejected_total = handler.query_rejected_total,
        });
        try antfly.common.request_admission.appendPrometheusMetrics(writer, .write, .{
            .capacity = handler.write_capacity,
            .in_flight = handler.write_in_flight,
            .peak_in_flight = handler.write_peak_in_flight,
            .rejected_total = handler.write_rejected_total,
        });
        try antfly.common.request_admission.appendPrometheusMetrics(writer, .inference, .{
            .capacity = handler.inference_capacity,
            .in_flight = handler.inference_in_flight,
            .peak_in_flight = handler.inference_peak_in_flight,
            .rejected_total = handler.inference_rejected_total,
        });
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_capacity", "gauge", "Maximum concurrent streaming H2 query bodies", handler.query_body_capacity);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_bodies_in_flight", "gauge", "Streaming H2 query bodies currently admitted", handler.query_body_in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_peak_in_flight", "gauge", "Peak concurrent streaming H2 query bodies since process start", handler.query_body_peak_in_flight);
        try antfly.common.health_server.appendPromMetric(writer, "antfly_query_body_rejected_total", "counter", "Streaming H2 query bodies rejected by admission control", handler.query_body_rejected_total);
        if (self.unified_lifecycle.runtimeStats()) |http| {
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_connection_limit", "gauge", "Maximum concurrent public HTTP connections", http.max_connections);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_connections", "gauge", "Currently active public HTTP connections", http.active_connections);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_requests", "gauge", "Currently active public HTTP requests", http.active_requests);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_accept_errors_total", "counter", "Public HTTP listener accept failures", http.accept_errors_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_connection_dispatch_rejections_total", "counter", "Accepted public HTTP connections closed because concurrent execution was unavailable", http.connection_dispatch_rejections_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_request_dispatch_rejections_total", "counter", "HTTP requests rejected before application execution because listener or runtime request capacity was unavailable", http.request_dispatch_rejections_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_h2_stream_dispatch_rejections_total", "counter", "HTTP/2 streams reset before application execution because bounded handler execution was unavailable", http.h2_stream_dispatch_rejections_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_request_cancellations_total", "counter", "Public HTTP requests terminated by application cancellation", http.request_cancellations_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_capacity_bytes", "gauge", "Aggregate HTTP request-body buffer capacity", http.body_buffer_capacity_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_in_use_bytes", "gauge", "HTTP request-body bytes admitted across HTTP/1 and HTTP/2", http.body_buffer_in_use_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_peak_bytes", "gauge", "Peak admitted HTTP request-body bytes since process start", http.body_buffer_peak_bytes);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_body_buffer_rejected_total", "counter", "HTTP request bodies rejected by aggregate memory admission", http.body_buffer_rejected_total);
        }
        if (self.unified_lifecycle.httpRuntimeStats()) |http_runtime| {
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_listener_capacity", "gauge", "Maximum concurrent long-lived HTTP listeners", http_runtime.listener_capacity);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_listener_leases", "gauge", "Long-lived HTTP listeners currently owned by the shared runtime", http_runtime.active_listener_leases);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_transport_connection_capacity", "gauge", "Shared HTTP transport connection-task capacity", http_runtime.connection_capacity);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_transport_reserved_connections", "gauge", "HTTP transport connection-task capacity reserved by live listeners", http_runtime.reserved_connection_capacity);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_request_task_capacity", "gauge", "Shared HTTP application request-task capacity", http_runtime.request_capacity);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_request_task_reserved", "gauge", "HTTP request-task capacity reserved by live listeners", http_runtime.reserved_request_capacity);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_cancellation_watcher_start_failures_total", "counter", "Public requests rejected because transport cancellation observation could not be registered", http_runtime.h1_cancellation_registration_failures_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_hard_disconnect_cancellations_total", "counter", "Public requests cancelled after a hard transport failure", http_runtime.h1_hard_disconnect_cancellations_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_peer_observer_failures_total", "counter", "Public requests cancelled after transport cancellation observation failed", http_runtime.h1_cancellation_observer_failures_total);
            try antfly.common.health_server.appendPromMetric(writer, "antfly_http_active_peer_observers", "gauge", "HTTP/1 request sockets currently registered for hard-disconnect observation", http_runtime.active_h1_cancellation_observers);
        }
    }
};

fn standaloneReadyFromState(api_server_initialized: bool, unified_api_ready: bool) bool {
    return api_server_initialized and unified_api_ready;
}

fn startupCheckpointSatisfied(progress: antfly.ha.standby.Progress, checkpoint_lsn: u64) bool {
    return progress.applied_lsn >= checkpoint_lsn and progress.safe_read_lsn >= checkpoint_lsn;
}

const UnifiedServerLifecycle = antfly.common.runtime_lifecycle.HttpServerLifecycle;

const LocalStandaloneMetadata = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    manager: antfly.metadata.TableManager,
    extension_catalog: antfly.extensions.ExtensionCatalog,
    local_node_id: u64,
    store_id: u64,
    api_url: []const u8,
    replica_root_dir: []const u8,
    catalog_path: []const u8,
    catalog_store: ?*antfly.storage_backend_erased.Store,
    backend_runtime: *antfly.db.background_runtime.BackendRuntime,
    storage_engine: antfly.common.config.StorageEngine = .local,
    epoch: u64 = 1,
    last_schema_migration_finalize_at_ms: u64 = 0,
    local_schema_progress_provider: ?LocalSchemaProgressProvider = null,

    const PersistedCatalog = struct {
        epoch: u64 = 1,
        tables: []const antfly.metadata.TableRecord = &.{},
        ranges: []const antfly.metadata.RangeRecord = &.{},
        extension_packages: []const antfly.extensions.PackageManifest = &.{},
        installed_extensions: []const antfly.extensions.InstalledExtension = &.{},
        extension_members: []const antfly.extensions.ExtensionMember = &.{},
        extension_dependencies: []const antfly.extensions.ExtensionDependency = &.{},
    };

    const CatalogMutation = struct {
        previous_manager: antfly.metadata.TableManager,
        previous_extensions: antfly.extensions.ExtensionCatalog,
        previous_epoch: u64,
        committed: bool = false,

        fn commit(self: *CatalogMutation, metadata: *LocalStandaloneMetadata) !void {
            try metadata.persistLocked();
            self.committed = true;
        }

        fn deinit(self: *CatalogMutation, metadata: *LocalStandaloneMetadata) void {
            if (self.committed) {
                self.previous_extensions.deinit();
                self.previous_manager.deinit();
                return;
            }
            metadata.extension_catalog.deinit();
            metadata.manager.deinit();
            metadata.manager = self.previous_manager;
            metadata.extension_catalog = self.previous_extensions;
            metadata.epoch = self.previous_epoch;
        }
    };

    fn beginCatalogMutationLocked(self: *LocalStandaloneMetadata) !CatalogMutation {
        var manager = antfly.metadata.TableManager.init(self.alloc);
        errdefer manager.deinit();
        const tables = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        defer self.manager.freeRanges(self.alloc, ranges);
        _ = try manager.replaceProjectedTopology(tables, ranges);
        return .{
            .previous_manager = manager,
            .previous_extensions = try self.cloneExtensionCatalogLocked(),
            .previous_epoch = self.epoch,
        };
    }

    fn init(
        alloc: std.mem.Allocator,
        local_node_id: u64,
        store_id: u64,
        api_url: []const u8,
        replica_root_dir: []const u8,
        catalog_path: []const u8,
        backend_runtime: *antfly.db.background_runtime.BackendRuntime,
        catalog_store: ?*antfly.storage_backend_erased.Store,
        storage_engine: antfly.common.config.StorageEngine,
    ) !LocalStandaloneMetadata {
        var owned_api_url: ?[]u8 = try alloc.dupe(u8, api_url);
        errdefer if (owned_api_url) |value| alloc.free(value);
        var owned_replica_root_dir: ?[]u8 = try alloc.dupe(u8, replica_root_dir);
        errdefer if (owned_replica_root_dir) |value| alloc.free(value);
        var owned_catalog_path: ?[]u8 = try alloc.dupe(u8, catalog_path);
        errdefer if (owned_catalog_path) |value| alloc.free(value);
        var self = LocalStandaloneMetadata{
            .alloc = alloc,
            .manager = antfly.metadata.TableManager.init(alloc),
            .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
            .local_node_id = local_node_id,
            .store_id = store_id,
            .api_url = owned_api_url.?,
            .replica_root_dir = owned_replica_root_dir.?,
            .catalog_path = owned_catalog_path.?,
            .catalog_store = catalog_store,
            .backend_runtime = backend_runtime,
            .storage_engine = storage_engine,
        };
        owned_api_url = null;
        owned_replica_root_dir = null;
        owned_catalog_path = null;
        errdefer self.deinit();
        try self.loadPersistedCatalog();
        return self;
    }

    fn deinit(self: *LocalStandaloneMetadata) void {
        self.extension_catalog.deinit();
        self.manager.deinit();
        self.alloc.free(self.catalog_path);
        self.alloc.free(self.replica_root_dir);
        self.alloc.free(self.api_url);
        self.* = undefined;
    }

    fn setApiUrl(self: *LocalStandaloneMetadata, api_url: []const u8) !void {
        const owned_api_url = try self.alloc.dupe(u8, api_url);
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.alloc.free(self.api_url);
        self.api_url = owned_api_url;
    }

    fn catalogSource(self: *LocalStandaloneMetadata) antfly.public_api.table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = catalogAdminSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
            },
        };
    }

    fn statusSource(self: *LocalStandaloneMetadata) antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = catalogAdminSnapshot,
                .cached_admin_snapshot = cachedAdminSnapshot,
                .linearizable_snapshot = linearizableSnapshot,
                .free_admin_snapshot = catalogFreeAdminSnapshot,
                .create_table = createTable,
                .replace_table_definition = replaceTableDefinition,
                .restore_table = restoreTable,
                .drop_table = dropTable,
                .drop_table_expected = dropTableExpected,
                .update_schema = updateSchema,
                .update_schema_versioned = updateSchemaVersioned,
                .create_index = createIndex,
                .drop_index = dropIndex,
                .put_artifact_enrichment = putArtifactEnrichment,
                .delete_artifact_enrichment = deleteArtifactEnrichment,
                .wait_table_lifecycle = waitTableLifecycle,
                .wait_table_projection = waitTableProjection,
                .run_round = runRound,
                .install_extension = installExtension,
                .update_extension = updateExtension,
                .drop_extension = dropExtension,
                .enable_extension = enableExtension,
                .disable_extension = disableExtension,
                .configure_extension = configureExtension,
                .restore_extensions = restoreExtensions,
            },
        };
    }

    fn status(ptr: *anyopaque) !antfly.metadata_api.MetadataStatus {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return .{
            .metadata_group_id = group_ids.main_metadata_group_id,
            .metadata_epoch = self.epoch,
            .metadata_raft_role = "disabled",
            .projected_tables = self.manager.tables.count(),
            .projected_extension_packages = self.extension_catalog.packages.items.len,
            .projected_installed_extensions = self.extension_catalog.installed.items.len,
            .projected_extension_members = self.extension_catalog.members.items.len,
            .projected_extension_dependencies = self.extension_catalog.dependencies.items.len,
            .projected_ranges = self.manager.ranges.count(),
            .projected_stores = 1,
            .projected_placement_intents = self.manager.ranges.count(),
            .metrics = .{},
        };
    }

    fn cachedAdminSnapshot(ptr: *anyopaque) !?antfly.metadata_api.AdminSnapshot {
        return try catalogAdminSnapshot(ptr);
    }

    fn linearizableSnapshot(
        ptr: *anyopaque,
        request: antfly.public_api.operation.RequestContext,
    ) !?antfly.metadata_api.AdminSnapshot {
        // Standalone catalog mutations and snapshots share the same mutex, so
        // the locked clone itself is the linearization point. Preserve the
        // request lifecycle contract on both sides of the potentially large
        // allocation just like the Raft-backed coherent snapshot path does.
        try request.ensureActive();
        var snapshot = try catalogAdminSnapshot(ptr);
        errdefer catalogFreeAdminSnapshot(ptr, &snapshot);
        try request.ensureActive();
        return snapshot;
    }

    fn catalogAdminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const tables = try self.manager.listTables(self.alloc);
        errdefer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        errdefer self.manager.freeRanges(self.alloc, ranges);
        const extension_packages = try self.extension_catalog.listPackages(self.alloc);
        errdefer self.extension_catalog.freePackages(self.alloc, extension_packages);
        const installed_extensions = try self.extension_catalog.listInstalled(self.alloc);
        errdefer self.extension_catalog.freeInstalled(self.alloc, installed_extensions);
        const extension_members = try self.extension_catalog.listMembers(self.alloc);
        errdefer self.extension_catalog.freeMembers(self.alloc, extension_members);
        const extension_dependencies = try self.extension_catalog.listDependencies(self.alloc);
        errdefer self.extension_catalog.freeDependencies(self.alloc, extension_dependencies);

        const stores = try self.alloc.alloc(antfly.metadata.StoreRecord, 1);
        errdefer self.alloc.free(stores);
        stores[0] = try antfly.metadata.table_manager.cloneStore(self.alloc, .{
            .store_id = self.store_id,
            .node_id = self.local_node_id,
            .api_url = self.api_url,
            .role = "data",
            .health_class = "healthy",
            .live = true,
        });
        errdefer antfly.metadata.table_manager.freeStore(self.alloc, stores[0]);

        const placement_intents = try self.alloc.alloc(antfly.raft.PlacementIntent, ranges.len);
        errdefer self.alloc.free(placement_intents);
        for (ranges, 0..) |range, i| {
            placement_intents[i] = .{
                .record = .{
                    .group_id = range.group_id,
                    .replica_id = 1,
                    .local_node_id = self.local_node_id,
                    .bootstrap_mode = .persisted,
                    .metadata_version = self.epoch,
                },
                .store_id = self.store_id,
                .peer_node_ids = &.{},
            };
        }

        return .{
            .status = .{
                .metadata_group_id = group_ids.main_metadata_group_id,
                .metadata_epoch = self.epoch,
                .metadata_raft_role = "disabled",
                .projected_tables = tables.len,
                .projected_extension_packages = extension_packages.len,
                .projected_installed_extensions = installed_extensions.len,
                .projected_extension_members = extension_members.len,
                .projected_extension_dependencies = extension_dependencies.len,
                .projected_ranges = ranges.len,
                .projected_stores = stores.len,
                .projected_placement_intents = placement_intents.len,
                .metrics = .{},
            },
            .tables = tables,
            .ranges = ranges,
            .stores = stores,
            .placement_intents = placement_intents,
            .extension_packages = extension_packages,
            .installed_extensions = installed_extensions,
            .extension_members = extension_members,
            .extension_dependencies = extension_dependencies,
            .split_transitions = try self.alloc.alloc(antfly.metadata.SplitTransitionRecord, 0),
            .merge_transitions = try self.alloc.alloc(antfly.metadata.MergeTransitionRecord, 0),
        };
    }

    fn catalogFreeAdminSnapshot(ptr: *anyopaque, snapshot: *antfly.metadata_api.AdminSnapshot) void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        self.manager.freeTables(self.alloc, snapshot.tables);
        self.manager.freeRanges(self.alloc, snapshot.ranges);
        for (snapshot.stores) |store| antfly.metadata.table_manager.freeStore(self.alloc, store);
        self.alloc.free(snapshot.stores);
        self.alloc.free(snapshot.placement_intents);
        self.extension_catalog.freePackages(self.alloc, snapshot.extension_packages);
        self.extension_catalog.freeInstalled(self.alloc, snapshot.installed_extensions);
        self.extension_catalog.freeMembers(self.alloc, snapshot.extension_members);
        self.extension_catalog.freeDependencies(self.alloc, snapshot.extension_dependencies);
        self.alloc.free(snapshot.split_transitions);
        self.alloc.free(snapshot.merge_transitions);
        snapshot.* = undefined;
    }

    fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: antfly.public_api.tables.CreateTableRequest) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        const table = try deriveStandaloneTableRecord(self.storage_engine, table_name, req);
        const ranges = try antfly.public_api.tables.deriveInitialRanges(alloc, table);
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked(table_name) != null) return error.TableAlreadyExists;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn adoptEmbeddedLiteRootIfNeeded(self: *LocalStandaloneMetadata, backend: *antfly.lite.backend.Handle) !void {
        const existing = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, existing);
        if (existing.len != 0) return;
        if (!(try backend.isEmbeddedArtifact()) and !(try backend.embeddedRootHasUserDocuments())) return;

        const table = try deriveStandaloneTableRecord(.lite, "default", .{});
        const ranges = try antfly.public_api.tables.deriveInitialRanges(self.alloc, table);
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(self.alloc, record);
            self.alloc.free(ranges);
        }
        if (ranges.len != 1) return error.InvalidCreateTableRequest;
        const namespace = try std.fmt.allocPrint(self.alloc, "group-{d}/table-db", .{ranges[0].group_id});
        defer self.alloc.free(namespace);

        // The durable alias is published first. If catalog publication fails,
        // startup fails and the next attempt safely retries the idempotent
        // catalog adoption; it can never publish a table that points at an
        // empty namespace.
        try backend.adoptEmbeddedRootAsNamespace(namespace);

        // Embedded Lite is created with the deterministic identity of the
        // future standalone `default` table. Verify that invariant before
        // publishing metadata; never perform an O(live documents) identity
        // rewrite during startup or silently accept a mismatched artifact.
        const target_identity = antfly.db.DocIdentityNamespace{
            .table_id = table.table_id,
            .shard_id = ranges[0].group_id,
            .range_id = ranges[0].range_id,
        };
        try verifyAdoptedLiteIdentity(self.alloc, backend, namespace, target_identity);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked("default") != null) return;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn replaceTableDefinition(ptr: *anyopaque, expected: antfly.metadata.TableRecord, replacement: antfly.metadata.TableRecord) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current = self.findTableByNameLocked(replacement.name) orelse return error.TableNotFound;
        if (!antfly.metadata.table_manager.tableDefinitionsEqual(current.*, expected) or replacement.table_id != expected.table_id) return error.TableGenerationChanged;
        const previous = try antfly.metadata.table_manager.cloneTable(self.alloc, current.*);
        defer antfly.metadata.table_manager.freeTable(self.alloc, previous);
        const previous_epoch = self.epoch;

        try self.manager.upsertTable(replacement);
        self.epoch +|= 1;
        self.persistLocked() catch |err| {
            self.manager.upsertTable(previous) catch |rollback_err| {
                std.log.err("local metadata table definition rollback failed table={s} err={s}", .{ replacement.name, @errorName(rollback_err) });
            };
            self.epoch = previous_epoch;
            return err;
        };
    }

    fn restoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        artifact_backup_id: []const u8,
        manifest: *const antfly.public_api.backups.TableBackupManifest,
    ) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        try antfly.public_api.backups.validateTableManifest(alloc, manifest, manifest.backup_id);
        if (!std.mem.eql(u8, manifest.table_name, table_name)) return error.InvalidBackupRequest;
        var table = try antfly.public_api.backups.deriveRestoreTableRecord(alloc, table_name, location_uri, manifest);
        defer antfly.metadata.table_manager.freeTable(alloc, table);
        const ranges = try antfly.public_api.backups.deriveRestoreRanges(
            alloc,
            table.table_id,
            location_uri,
            connection,
            artifact_backup_id,
            manifest,
        );
        defer {
            for (ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }
        if (self.storage_engine == .lite and ranges.len != 1) return error.InvalidBackupRequest;
        table.desired_replica_count = 1;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.findTableByNameLocked(table_name) != null) return error.TableAlreadyExists;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(table);
        for (ranges) |range| try self.manager.upsertRange(range);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        _ = self.manager.removeTableTopology(table.table_id);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn dropTableExpected(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        expected_table_id: u64,
    ) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableGenerationChanged;
        if (table.table_id != expected_table_id) return error.TableGenerationChanged;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        _ = self.manager.removeTableTopology(expected_table_id);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        _ = try updateSchemaVersioned(ptr, alloc, table_name, schema_json);
    }

    fn updateSchemaVersioned(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !u32 {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const updated = try antfly.public_api.tables.applySchemaUpdateRecord(alloc, table, schema_json);
        defer antfly.metadata.table_manager.freeTable(alloc, updated);
        const version = try antfly.public_api.tables.schemaVersion(updated.schema_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
        return version;
    }

    fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try antfly.public_api.indexes.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        try antfly.public_api.indexes.validateArtifactEnrichmentsForTableIndexesJson(alloc, updated.indexes_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const indexes_json = (try antfly.public_api.indexes.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn putArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8, enrichment_json: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try antfly.public_api.indexes.addEnrichmentToTableIndexesJson(alloc, table.indexes_json, artifact_name, enrichment_json);
        defer alloc.free(updated.indexes_json);
        try antfly.public_api.indexes.validateArtifactEnrichmentsForTableIndexesJson(alloc, updated.indexes_json);
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn deleteArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableNotFound;
        const indexes_json = (try antfly.public_api.indexes.removeEnrichmentFromTableIndexesJson(alloc, table.indexes_json, artifact_name)) orelse return error.EnrichmentNotFound;
        defer alloc.free(indexes_json);
        try antfly.public_api.indexes.validateArtifactEnrichmentsForTableIndexesJson(alloc, indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.manager.upsertTable(updated);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn waitTableLifecycle(_: *anyopaque, _: []const u8, _: antfly.public_api.http_server.TableVisibility) !void {}

    fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const table = self.findTableByNameLocked(table_name) orelse return error.TableVisibilityTimeout;
        if (schema_json) |expected| {
            if (!std.mem.eql(u8, table.schema_json, expected)) return error.TableVisibilityTimeout;
        }
        if (indexes_json) |expected| {
            if (!std.mem.eql(u8, table.indexes_json, expected)) return error.TableVisibilityTimeout;
        }
    }

    fn runRound(ptr: *anyopaque) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        self.finalizeReadySchemaMigrations() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LsmRootWriterAlreadyOpen, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
    }

    fn installExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.InstallExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const installed_at_ms: i64 = @intCast(@divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms));
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            var planned = try catalog.installManifestOnly(extension_name, extension_name, persisted_req, installed_at_ms);
            defer planned.deinitOwned(self.alloc);
            return try antfly.extensions.cloneInstalledExtensionAlloc(alloc, planned);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var installed = try self.extension_catalog.installManifestOnly(extension_name, extension_name, persisted_req, installed_at_ms);
        defer installed.deinitOwned(self.alloc);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn updateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.UpdateExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            var planned = try catalog.updateManifestOnly(extension_name, persisted_req);
            defer planned.deinitOwned(self.alloc);
            return try antfly.extensions.cloneInstalledExtensionAlloc(alloc, planned);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var installed = try self.extension_catalog.updateManifestOnly(extension_name, persisted_req);
        defer installed.deinitOwned(self.alloc);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn dropExtension(ptr: *anyopaque, _: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.DropExtensionRequest) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var persisted_req = req;
        persisted_req.dry_run = false;
        if (req.dry_run) {
            var catalog = try self.cloneExtensionCatalogLocked();
            defer catalog.deinit();
            return try catalog.dropInstalledWithMode(extension_name, persisted_req);
        }
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.dropInstalledWithMode(extension_name, persisted_req);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn enableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.enableInstalled(extension_name);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn disableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.disableInstalled(extension_name);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn configureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, extension_name: []const u8, req: antfly.extensions.ConfigureExtensionRequest) !antfly.extensions.InstalledExtension {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        try self.extension_catalog.configureInstalled(extension_name, req);
        self.epoch +|= 1;
        try mutation.commit(self);
        return try self.extension_catalog.getInstalledAlloc(alloc, extension_name);
    }

    fn restoreExtensions(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        installed: []const antfly.extensions.InstalledExtension,
        members: []const antfly.extensions.ExtensionMember,
        dependencies: []const antfly.extensions.ExtensionDependency,
    ) !void {
        const self: *LocalStandaloneMetadata = @ptrCast(@alignCast(ptr));
        if (installed.len == 0 and members.len == 0 and dependencies.len == 0) return;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        for (installed) |extension| try self.extension_catalog.upsertInstalled(extension);
        for (members) |member| try self.extension_catalog.upsertMember(member);
        for (dependencies) |dependency| try self.extension_catalog.upsertDependency(dependency);
        self.epoch +|= 1;
        try mutation.commit(self);
    }

    fn cloneExtensionCatalogLocked(self: *LocalStandaloneMetadata) !antfly.extensions.ExtensionCatalog {
        var catalog = antfly.extensions.ExtensionCatalog.init(self.alloc);
        errdefer catalog.deinit();
        try catalog.loadProjectedRows(
            self.extension_catalog.packages.items,
            self.extension_catalog.installed.items,
            self.extension_catalog.members.items,
            self.extension_catalog.dependencies.items,
        );
        return catalog;
    }

    fn syncExtensionPackageStore(self: *LocalStandaloneMetadata, io: std.Io, root_path: []const u8) !usize {
        const entries = try antfly.extensions.scanPackageStoreAlloc(self.alloc, io, root_path);
        defer antfly.extensions.freePackageStoreEntries(self.alloc, entries);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var mutation = if (entries.len > 0) try self.beginCatalogMutationLocked() else null;
        defer if (mutation) |*active| active.deinit(self);
        for (entries) |entry| try self.extension_catalog.registerPackage(entry.manifest);
        if (entries.len > 0) {
            self.epoch +|= 1;
            try mutation.?.commit(self);
        }
        return entries.len;
    }

    fn finalizeReadySchemaMigrations(self: *LocalStandaloneMetadata) !void {
        const now_ms = monotonicMs();
        const snapshot = blk: {
            lockAtomic(&self.mutex);
            defer self.mutex.unlock();
            var active_migration = false;
            var table_it = self.manager.tables.valueIterator();
            while (table_it.next()) |table| {
                if (table.read_schema_json.len > 0) {
                    active_migration = true;
                    break;
                }
            }
            if (!active_migration) return;
            if (now_ms -| self.last_schema_migration_finalize_at_ms < local_schema_migration_finalize_interval_ms) return;
            self.last_schema_migration_finalize_at_ms = now_ms;

            const tables = try self.manager.listTables(self.alloc);
            errdefer self.manager.freeTables(self.alloc, tables);
            const ranges = try self.manager.listRanges(self.alloc);
            break :blk .{ .tables = tables, .ranges = ranges };
        };
        defer self.manager.freeRanges(self.alloc, snapshot.ranges);
        defer self.manager.freeTables(self.alloc, snapshot.tables);

        const hosted_group_ids = try self.alloc.alloc(u64, snapshot.ranges.len);
        defer self.alloc.free(hosted_group_ids);
        for (snapshot.ranges, 0..) |range, i| hosted_group_ids[i] = range.group_id;

        var runtime_progress: ?antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot = null;
        defer if (runtime_progress) |*progress| progress.deinit(self.alloc);
        var filesystem_progress: ?[]antfly.metadata.SchemaProgressRecord = null;
        defer if (filesystem_progress) |progress| self.alloc.free(progress);
        const progress: []const antfly.metadata.SchemaProgressRecord = progress: {
            if (self.local_schema_progress_provider) |provider| {
                runtime_progress = try provider.collect(provider.ptr, self.alloc, snapshot.tables, snapshot.ranges);
                if (runtime_progress.?.records.len != 0) break :progress runtime_progress.?.records;
                // A complete runtime observation is authoritative even while
                // not ready. Do not contend with its live writer by reopening
                // the same root through the filesystem fallback.
                if (runtime_progress.?.runtime_coverage_complete) return;
            }
            filesystem_progress = try antfly.metadata.table_provisioner.collectLocalSchemaProgressWithOptions(
                self.alloc,
                self.replica_root_dir,
                group_ids.main_metadata_group_id,
                self.local_node_id,
                hosted_group_ids,
                snapshot.tables,
                snapshot.ranges,
                .{
                    .backend_runtime = self.backend_runtime,
                },
            );
            break :progress filesystem_progress.?;
        };
        if (progress.len == 0) return;

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        var mutation = try self.beginCatalogMutationLocked();
        defer mutation.deinit(self);
        var changed = false;
        for (progress) |record| {
            const table = self.manager.tables.get(record.table_id) orelse continue;
            if (table.read_schema_json.len == 0) continue;

            const target_version = try localSchemaVersion(self.alloc, table.schema_json);
            if (record.schema_version != target_version) continue;

            var updated = try antfly.metadata.table_manager.cloneTable(self.alloc, table);
            defer antfly.metadata.table_manager.freeTable(self.alloc, updated);

            const read_version = try localSchemaVersion(self.alloc, updated.read_schema_json);
            if (read_version != target_version) {
                const next_indexes_json = try dropFullTextIndexForVersion(self.alloc, updated.indexes_json, read_version);
                self.alloc.free(updated.indexes_json);
                updated.indexes_json = next_indexes_json;
            }
            self.alloc.free(updated.read_schema_json);
            updated.read_schema_json = try self.alloc.dupe(u8, "");

            try self.manager.upsertTable(updated);
            changed = true;
        }

        if (changed) {
            self.epoch +|= 1;
            try mutation.commit(self);
        } else {
            mutation.committed = true;
        }
    }

    fn findTableByNameLocked(self: *LocalStandaloneMetadata, table_name: []const u8) ?*const antfly.metadata.TableRecord {
        var it = self.manager.tables.valueIterator();
        while (it.next()) |table| {
            if (std.mem.eql(u8, table.name, table_name)) return table;
        }
        return null;
    }

    fn loadPersistedCatalog(self: *LocalStandaloneMetadata) !void {
        const raw = if (self.catalog_store) |store| blk: {
            var txn = try store.beginRead();
            defer txn.abort();
            const value = txn.get("catalog") catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            break :blk try self.alloc.dupe(u8, value);
        } else readFileAlloc(
            self.alloc,
            self.backend_runtime.io() orelse std.Options.debug_io,
            self.catalog_path,
            64 * 1024 * 1024,
        ) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer self.alloc.free(raw);

        var parsed = try std.json.parseFromSlice(PersistedCatalog, self.alloc, raw, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        _ = try self.manager.replaceProjectedTopology(parsed.value.tables, parsed.value.ranges);
        try self.extension_catalog.loadProjectedRows(
            parsed.value.extension_packages,
            parsed.value.installed_extensions,
            parsed.value.extension_members,
            parsed.value.extension_dependencies,
        );
        self.epoch = @max(parsed.value.epoch, 1);
    }

    fn persistLocked(self: *LocalStandaloneMetadata) !void {
        const tables = try self.manager.listTables(self.alloc);
        defer self.manager.freeTables(self.alloc, tables);
        const ranges = try self.manager.listRanges(self.alloc);
        defer self.manager.freeRanges(self.alloc, ranges);
        const extension_packages = try self.extension_catalog.listPackages(self.alloc);
        defer self.extension_catalog.freePackages(self.alloc, extension_packages);
        const installed_extensions = try self.extension_catalog.listInstalled(self.alloc);
        defer self.extension_catalog.freeInstalled(self.alloc, installed_extensions);
        const extension_members = try self.extension_catalog.listMembers(self.alloc);
        defer self.extension_catalog.freeMembers(self.alloc, extension_members);
        const extension_dependencies = try self.extension_catalog.listDependencies(self.alloc);
        defer self.extension_catalog.freeDependencies(self.alloc, extension_dependencies);

        const encoded = try std.json.Stringify.valueAlloc(self.alloc, PersistedCatalog{
            .epoch = self.epoch,
            .tables = tables,
            .ranges = ranges,
            .extension_packages = extension_packages,
            .installed_extensions = installed_extensions,
            .extension_members = extension_members,
            .extension_dependencies = extension_dependencies,
        }, .{ .emit_null_optional_fields = false });
        defer self.alloc.free(encoded);

        if (self.catalog_store) |store| {
            var txn = try store.beginWrite();
            errdefer txn.abort();
            try txn.put("catalog", encoded);
            try txn.commit();
            try store.sync(true);
        } else {
            try writeFileAtomically(
                self.alloc,
                self.backend_runtime.io() orelse std.Options.debug_io,
                self.catalog_path,
                encoded,
            );
        }
    }
};

fn verifyAdoptedLiteIdentity(
    alloc: std.mem.Allocator,
    backend: *antfly.lite.backend.Handle,
    namespace: []const u8,
    target_identity: antfly.db.DocIdentityNamespace,
) !void {
    if (!target_identity.eql(antfly.lite.connection.embeddedRootIdentity())) {
        return error.InvalidEmbeddedLiteIdentity;
    }
    var db_opts = antfly.db.OpenOptions{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .ttl_cleanup = .{ .enabled = false },
        .identity_namespace = target_identity,
    };
    try backend.configureDbOpenOptionsForNamespace(&db_opts, namespace);
    var adopted_db = try antfly.db.DB.open(alloc, namespace, db_opts);
    defer adopted_db.close();
    if (!adopted_db.core.identity_namespace.eql(target_identity)) return error.InvalidEmbeddedLiteIdentity;
}

fn deriveStandaloneTableRecord(
    storage_engine: antfly.common.config.StorageEngine,
    table_name: []const u8,
    req: antfly.public_api.tables.CreateTableRequest,
) !antfly.metadata.TableRecord {
    if (storage_engine == .lite and (req.num_shards orelse 1) != 1) {
        return error.InvalidCreateTableRequest;
    }
    var table = antfly.public_api.tables.deriveTableRecord(table_name, req);
    // A standalone process owns the only replica regardless of whether its
    // local persistence is directory-backed or Lite single-file storage.
    table.desired_replica_count = 1;
    return table;
}

fn localSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableSchema,
    };
    const version_value = object.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| blk: {
            if (value < 0) return error.InvalidTableSchema;
            break :blk std.math.cast(u32, value) orelse return error.InvalidTableSchema;
        },
        else => return error.InvalidTableSchema,
    };
}

fn dropFullTextIndexForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var versioned_name_buf: [64]u8 = undefined;
    const stale_name = if (version == 0)
        antfly.public_api.tables.default_full_text_index_name
    else
        try std.fmt.bufPrint(&versioned_name_buf, "full_text_index_v{d}", .{version});
    _ = object.swapRemove(stale_name);
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

fn monotonicMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_standalone";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    _: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    var cli = try parseCli(alloc, args);
    defer cli.deinit(alloc);
    if (cli.help) {
        printUsage();
        return;
    }

    var termination_signals = antfly.common.runtime_lifecycle.ProcessSignalScope.install();
    defer termination_signals.deinit();
    var supervisor = antfly.common.runtime_lifecycle.RuntimeSupervisor.init(30_000);
    defer supervisor.markStopped();
    var setup_io = std.Io.Threaded.init(alloc, .{});
    defer setup_io.deinit();

    var secret_store: antfly.common.secrets.FileStore = undefined;
    var secret_store_initialized = false;
    defer if (secret_store_initialized) secret_store.deinit();

    if (cli.secret_store_paths.items.len > 0) {
        secret_store = try initLayeredSecretStore(alloc, setup_io.io(), cli.secret_store_paths.items);
        secret_store_initialized = true;
    }

    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |config_path|
        try antfly.common.config.loadFromPathWithSecretsForDeploymentWithIo(
            alloc,
            setup_io.io(),
            config_path,
            if (secret_store_initialized) &secret_store else null,
            .standalone,
        )
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();

    antfly.common.config.Config.validateServerTlsConfig(if (loaded_config) |*cfg| cfg.tls else null) catch |err| {
        std.log.err("standalone startup rejected configured tls: built-in server TLS is unsupported; terminate TLS at a trusted reverse proxy", .{});
        return err;
    };
    validateCorsConfig(configuredCors(if (loaded_config) |*cfg| cfg else null)) catch |err| {
        std.log.err("standalone startup rejected invalid cors configuration err={}", .{err});
        return err;
    };

    var remote_content_runtime: antfly.common.remote_content_runtime.Runtime = undefined;
    var remote_content_runtime_initialized = false;
    defer if (remote_content_runtime_initialized) remote_content_runtime.deinit();
    var remote_content_facade = antfly.common.config.Config.RemoteContentConfig{};
    const remote_content = if (cli.config_path) |config_path| blk: {
        remote_content_runtime = try antfly.common.remote_content_runtime.Runtime.initWithIo(
            alloc,
            setup_io.io(),
            config_path,
            if (secret_store_initialized) &secret_store else null,
            .standalone,
        );
        remote_content_runtime_initialized = true;
        remote_content_runtime.attach(&remote_content_facade);
        break :blk &remote_content_facade;
    } else if (loaded_config) |*cfg|
        if (cfg.remote_content) |*configured| configured else null
    else
        null;

    const storage_engine = cli.storage_engine orelse if (loaded_config) |*cfg| cfg.storage.engine else .local;
    if (storage_engine == .object) return error.UnsupportedStandaloneStorageEngine;
    const lite_path = if (storage_engine == .lite)
        (cli.storage_path orelse if (loaded_config) |*cfg| cfg.storage.lite_path else null) orelse return error.MissingLiteStoragePath
    else
        null;
    const lite_fsync = cli.storage_fsync orelse if (loaded_config) |*cfg| cfg.storage.lite_fsync else true;
    try validateEffectiveStandaloneStorage(cli, storage_engine, lite_path, if (loaded_config) |*cfg| cfg else null);
    if (loaded_config) |*cfg| cfg.deployment_mode = .standalone;

    const data_dir = try resolveLocalBaseDir(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer alloc.free(data_dir);
    if (storage_engine == .local) try antfly.common.data_format.ensureCompatible(alloc, setup_io.io(), data_dir);
    // Validate and freeze the HA role before any startup helper can mutate a
    // primary-local sidecar that is not part of the continuous HA WAL.
    try validateHARole(cli);
    const ha_role_requested = haPrimaryRequested(cli) or haStandbyRequested(cli);
    const ha_mutation_guard_enabled = haContinuousMutationGuardEnabled(cli);

    const resolved = try resolvePaths(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer resolved.deinit(alloc);

    try ensureDirPath(setup_io.io(), resolved.replica_root_dir);
    try ensureParent(setup_io.io(), resolved.replica_catalog_path);
    try ensureParent(setup_io.io(), resolved.local_metadata_catalog_path);
    try ensureDirPath(setup_io.io(), resolved.snapshot_root_dir);
    try ensureParent(setup_io.io(), resolved.secret_store_path);
    try ensureDirPath(setup_io.io(), resolved.auth_store_root_dir);

    var node_backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer node_backend_runtime.deinit();
    // The linked inference archive retains std.Io for its full node lifetime.
    // Keep the corresponding host lane lease until after node destruction.
    var inference_lane_lease = try node_backend_runtime.ptr().acquireInferenceLane();
    defer inference_lane_lease.release();
    const inference_io = inference_lane_lease.io();
    var lite_backend: ?antfly.lite.backend.Handle = if (lite_path) |path|
        try antfly.lite.backend.Handle.openOrCreate(alloc, path, .{ .no_sync = !lite_fsync })
    else
        null;
    defer if (lite_backend) |*backend| backend.deinit();
    if (lite_backend) |*backend| {
        node_backend_runtime.ptr().db_open_configurator = backend.dbOpenConfigurator();
    }

    // Restore jobs are storage-engine state. Local standalone keeps them in a
    // dedicated LSM root; Lite keeps them in its single-file reserved
    // namespace. Supplying the engine store during API construction avoids a
    // legacy LMDB sidecar and makes production LSM-only startup self-contained.
    const restore_job_root = if (lite_backend == null)
        try std.fmt.allocPrint(alloc, "{s}/api-restore-jobs", .{resolved.replica_root_dir})
    else
        null;
    defer if (restore_job_root) |path| alloc.free(path);
    var restore_job_backend: ?antfly.lsm_backend.BackendHandle = if (restore_job_root) |path|
        try antfly.lsm_backend.BackendHandle.open(alloc, path, .{})
    else
        null;
    defer if (restore_job_backend) |*backend| backend.close();
    var local_restore_job_store: ?antfly.storage_backend_erased.Store = if (restore_job_backend) |*backend|
        try backend.backend.runtimeStore(alloc, .{ .name = "system/api-restore-jobs" })
    else
        null;
    defer if (local_restore_job_store) |*store| store.deinit();
    const restore_job_store = if (lite_backend) |*backend|
        try backend.runtimeStoreForNamespace("system/api-restore-jobs")
    else
        &local_restore_job_store.?;
    // Incoming reverse-route observations are an exact, fenced directory, not
    // disposable cache state: retain one latest generation per logical graph
    // key so restarts and L1 eviction do not reintroduce all-shard probes.
    const incoming_graph_route_root = if (lite_backend == null)
        try std.fmt.allocPrint(alloc, "{s}/incoming-graph-routes", .{resolved.replica_root_dir})
    else
        null;
    defer if (incoming_graph_route_root) |path| alloc.free(path);
    var incoming_graph_route_backend: ?antfly.lsm_backend.BackendHandle = if (incoming_graph_route_root) |path|
        try antfly.lsm_backend.BackendHandle.open(alloc, path, .{})
    else
        null;
    defer if (incoming_graph_route_backend) |*backend| backend.close();
    var local_incoming_graph_route_store: ?antfly.storage_backend_erased.Store = if (incoming_graph_route_backend) |*backend|
        try backend.backend.runtimeStore(alloc, .{ .name = "system/incoming-graph-routes" })
    else
        null;
    defer if (local_incoming_graph_route_store) |*store| store.deinit();
    const incoming_graph_route_store = if (lite_backend) |*backend|
        try backend.runtimeStoreForNamespace("system/incoming-graph-routes")
    else
        &local_incoming_graph_route_store.?;
    var storage_maintenance = try antfly.storage_maintenance.Coordinator.init(
        alloc,
        if (lite_backend) |*backend| backend.maintenanceSource() else antfly.storage_maintenance.localSource,
        node_backend_runtime.ptr(),
    );
    defer storage_maintenance.deinit();

    // Standalone always owns a local Antfly node. In production its heavy
    // implementation is code-generated in the inference archive and reached
    // through an opaque internal ABI; the shipped artifact remains one binary.
    const loaded_cfg = if (loaded_config) |*cfg| cfg else null;
    const process_memory_resolution = resolveProcessMemoryBudget(
        cli,
        init.environ_map,
    ) catch |err| {
        std.log.err("invalid process memory budget; expected a MiB value representable on this platform", .{});
        return err;
    };
    const process_memory_limit_bytes = process_memory_resolution.limit_bytes;
    const configured_preload = if (loaded_cfg) |cfg| cfg.inference.preload else &.{};
    const loaded_preload = if (cli.inference_preload_models.items.len == 0 and configured_preload.len != 0) blk: {
        const out = try alloc.alloc(inference_bridge.WarmModel, configured_preload.len);
        for (configured_preload, 0..) |model, i| {
            out[i] = .{
                .kind = inference_bridge.String.init(model.kind),
                .name = inference_bridge.String.init(model.name),
                .backend = inference_bridge.OptionalString.init(model.backend),
                .format = inference_bridge.OptionalString.init(model.format),
                .quantization = inference_bridge.OptionalString.init(model.quantization),
                .residency_mode = switch (model.residency_mode orelse .auto) {
                    .auto => .auto,
                    .resident => .resident,
                    .streamed => .streamed,
                },
                .memory_budget_mb = model.memory_budget_mb orelse 0,
            };
        }
        break :blk out;
    } else &.{};
    defer if (loaded_preload.len != 0) alloc.free(loaded_preload);
    const active_preload = if (cli.inference_preload_models.items.len != 0)
        cli.inference_preload_models.items
    else
        loaded_preload;
    const content_security_json = if (loaded_cfg) |cfg|
        if (cfg.effectiveAntflyContentSecurity()) |security|
            try std.json.Stringify.valueAlloc(alloc, security.*, .{})
        else
            null
    else
        null;
    defer if (content_security_json) |json| alloc.free(json);
    const s3_credentials_json = if (loaded_cfg) |cfg|
        if (cfg.inference.s3_credentials) |credentials|
            try std.json.Stringify.valueAlloc(alloc, credentials, .{})
        else
            null
    else
        null;
    defer if (s3_credentials_json) |json| alloc.free(json);

    const configured_inference: antfly.common.config.Config.InferenceConfig =
        if (loaded_cfg) |cfg| cfg.inference else .{};
    const effective_kernel_jit_mode = try resolveKernelJitMode(
        configured_inference.kernel_jit.mode,
        platform.env.getenv("ANTFLY_INFERENCE_KERNEL_JIT_MODE"),
        cli.inference_kernel_jit_mode,
    );
    const inference_runtime_config_json = try std.json.Stringify.valueAlloc(alloc, InferenceRuntimeConfigWire{
        .max_concurrent_requests = resolveInferenceMaxConcurrentRequests(loaded_cfg),
        .kernel_jit = .{
            .mode = effective_kernel_jit_mode,
            .cache_dir = configured_inference.kernel_jit.cache_dir,
            .max_cache_bytes_mb = configured_inference.kernel_jit.max_cache_bytes_mb,
            .preload_budget_ms = configured_inference.kernel_jit.preload_budget_ms,
        },
        .prompt_cache = .{
            .enabled = configured_inference.prompt_cache.enabled,
            .mode = configured_inference.prompt_cache.mode,
            .max_bytes_mb = configured_inference.prompt_cache.max_bytes_mb,
            .min_tokens = configured_inference.prompt_cache.min_tokens,
            .ttl_ms = configured_inference.prompt_cache.ttl_ms,
        },
    }, .{});
    defer alloc.free(inference_runtime_config_json);

    var handle: ?*anyopaque = null;
    const inference_create_context = inference_bridge.CreateContext{
        .abi_version = inference_bridge.abi_version,
        .data_dir_ptr = data_dir.ptr,
        .data_dir_len = data_dir.len,
        .models_dir = inference_bridge.OptionalString.init(cli.inference_models_dir orelse if (loaded_cfg) |cfg| cfg.inference.models_dir else null),
        .ml_dir = inference_bridge.OptionalString.init(cli.inference_ml_dir orelse if (loaded_cfg) |cfg| cfg.inference.ml_dir else null),
        .host_limit_bytes = try mibToBytes(cli.inference_host_budget_mb),
        .backend_limit_bytes = try mibToBytes(cli.inference_backend_budget_mb),
        .combined_limit_bytes = try mibToBytes(cli.inference_combined_budget_mb),
        .kv_limit_bytes = try mibToBytes(cli.inference_kv_budget_mb),
        .scratch_limit_bytes = try mibToBytes(cli.inference_scratch_budget_mb),
        .process_memory_limit_bytes = process_memory_limit_bytes,
        .process_memory_limit_provenance = inferenceMemoryLimitProvenance(
            process_memory_resolution.effective_source,
        ),
        .preload_ptr = if (active_preload.len == 0) null else active_preload.ptr,
        .preload_len = active_preload.len,
        .keep_alive = inference_bridge.OptionalString.init(if (loaded_cfg) |cfg| cfg.inference.keep_alive else null),
        .max_loaded_models = if (loaded_cfg) |cfg| cfg.inference.max_loaded_models orelse 0 else 0,
        .has_max_loaded_models = if (loaded_cfg) |cfg| @intFromBool(cfg.inference.max_loaded_models != null) else 0,
        .content_security_json = inference_bridge.OptionalString.init(content_security_json),
        .s3_credentials_json = inference_bridge.OptionalString.init(s3_credentials_json),
        .runtime_config_json = inference_bridge.String.init(inference_runtime_config_json),
        .executor = .init(&inference_io),
        .out_handle = &handle,
    };
    const antfly_node = if (comptime inline_inference_codegen) blk: {
        break :blk try inference_host.linkedInferenceCreate(&inference_create_context);
    } else blk: {
        const inference_api = try linkedInferenceApi(
            inference_bridge.Capability.provider |
                inference_bridge.Capability.route_manifest |
                inference_bridge.Capability.resource_budget |
                inference_bridge.Capability.request_admission,
        );
        const status = inference_api.create(&inference_create_context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
        break :blk handle orelse return error.InferenceRuntimeStartupFailed;
    };
    var local_inference_connection_context = LocalInferenceConnectionContext{
        .handle = antfly_node,
    };
    // Until DataServer exists, error cleanup is owned here. Once its
    // ResourceManager is attached below, the regular defer is registered
    // after DataServer's so tokenizer budget callbacks are torn down first.
    var antfly_node_needs_errdeinit = true;
    errdefer if (antfly_node_needs_errdeinit) {
        if (comptime inline_inference_codegen)
            inference_host.linkedInferenceDestroy(antfly_node)
        else
            linkedInferenceApiInfallible().destroy(antfly_node);
    };

    var active_audio_runtime = try antfly.common.audio_runtime.ActiveRuntime.init(
        alloc,
        setup_io.io(),
        if (loaded_config) |*cfg| cfg else null,
    );
    defer active_audio_runtime.deinit();

    if (!secret_store_initialized) {
        secret_store = try antfly.common.secrets.FileStore.initWithIo(alloc, setup_io.io(), resolved.secret_store_path);
        secret_store_initialized = true;
    }

    const internal_service_secret = try secret_store.getOwned(alloc, internal_service_secret_key);
    defer if (internal_service_secret) |value| alloc.free(value);
    const internal_service_issuer = try secret_store.getOwned(alloc, internal_service_issuer_key);
    defer if (internal_service_issuer) |value| alloc.free(value);
    if (internal_service_secret != null or internal_service_issuer != null) {
        internal_service_auth.validateRuntimeConfig(
            internal_service_secret,
            null,
            internal_service_issuer,
        ) catch |err| {
            std.log.err(
                "standalone internal service credential is incomplete or invalid: configure {s} with at least {d} bytes and a printable {s}; err={s}",
                .{ internal_service_secret_key, internal_service_auth.minimum_secret_bytes, internal_service_issuer_key, @errorName(err) },
            );
            return err;
        };
    }

    const auth_enabled = resolveAuthEnabled(cli, if (loaded_config) |*cfg| cfg else null);
    var auth_backend: ?antfly.lsm_backend.BackendHandle = null;
    var auth_runtime: ?antfly.storage_backend_erased.NamespaceStore = null;
    var auth_user_store: ?antfly.usermgr.StorageUserStore = null;
    var auth_casbin_store: ?antfly.usermgr.StorageCasbinAdapter = null;
    var user_manager: ?antfly.usermgr.UserManager = null;
    if (auth_enabled) {
        auth_backend = try antfly.lsm_backend.BackendHandle.open(alloc, resolved.auth_store_root_dir, .{});
        errdefer if (auth_backend) |*backend| backend.close();
        auth_runtime = try auth_backend.?.backend.runtimeNamespaceStore(alloc);
        errdefer if (auth_runtime) |*runtime| runtime.deinit();
        auth_user_store = antfly.usermgr.StorageUserStore.init(alloc, auth_runtime.?);
        auth_casbin_store = antfly.usermgr.StorageCasbinAdapter.init(alloc, auth_runtime.?);
        user_manager = try antfly.usermgr.UserManager.initWithIo(
            alloc,
            setup_io.io(),
            auth_user_store.?.iface(),
            try antfly.usermgr.initDefaultEnforcer(alloc, auth_casbin_store.?.iface()),
        );
        errdefer if (user_manager) |*manager| manager.deinit();
        if (ha_role_requested) {
            // Auth is carried by the portable seed, not the continuous HA WAL.
            // Creating a local default admin on either HA role after seeding
            // would acknowledge credentials that disappear on promotion.
            var seeded_admin = user_manager.?.getUser("admin") catch |err| switch (err) {
                error.UserNotFound => return error.HAAuthSeedMissing,
                else => return err,
            };
            seeded_admin.deinit(alloc);
        } else {
            // This seeds only the local auth store and must remain auth-gated.
            // Raft-backed metadata writes during metadata bootstrap can block
            // clustered startup before raft listeners are running.
            try antfly.usermgr.ensureDefaultAdminUser(&user_manager.?);
        }
    }
    defer if (user_manager) |*manager| manager.deinit();
    defer if (auth_runtime) |*runtime| runtime.deinit();
    defer if (auth_backend) |*backend| backend.close();

    const public_listener = resolvePublicListener(cli);
    const local_node_id = cli.local_node_id orelse 1;
    const public_api_url = try std.fmt.allocPrint(
        alloc,
        "http://{s}:{d}",
        .{ public_listener.bind_host, public_listener.bind_port },
    );
    defer alloc.free(public_api_url);

    var local_metadata = LocalStandaloneMetadata.init(
        alloc,
        local_node_id,
        1,
        public_api_url,
        resolved.replica_root_dir,
        resolved.local_metadata_catalog_path,
        node_backend_runtime.ptr(),
        if (lite_backend) |*backend| try backend.runtimeStoreForNamespace("system/metadata") else null,
        storage_engine,
    ) catch |err| {
        std.log.err("standalone startup failed step=local_metadata_init err={}", .{err});
        return err;
    };
    defer local_metadata.deinit();
    if (lite_backend) |*backend| {
        try local_metadata.adoptEmbeddedLiteRootIfNeeded(backend);
        // Mark only after embedded adoption and metadata publication succeed.
        // Offline root-only backup must reject every standalone artifact, not
        // merely artifacts that originated in the embedded profile.
        try backend.markStandaloneArtifact();
    }
    // API transaction sessions are engine state, not a sidecar. Keeping them
    // in a reserved Lite namespace makes a copied/reopened .aflite file a
    // complete database and preserves staged multi-request transactions.
    var lite_session_store = if (lite_backend) |*backend|
        antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        )
    else
        null;
    const synced_extension_packages = if (ha_role_requested)
        0
    else
        local_metadata.syncExtensionPackageStore(setup_io.io(), resolved.extension_package_store_dir) catch |err| {
            std.log.err("standalone startup failed step=sync_extension_packages err={}", .{err});
            return err;
        };
    if (synced_extension_packages > 0) {
        std.log.info("standalone synced extension package store path={s} packages={d}", .{ resolved.extension_package_store_dir, synced_extension_packages });
    }

    try validateHAPathsUnderRoot(cli, data_dir);
    const ha_startup_expectation = try haStartupExpectationFromCli(cli);
    const ha_startup_checkpoint_lsn = if (ha_startup_expectation) |expectation|
        antfly.ha.seed_activation.validateActivatedGeneration(alloc, expectation) catch |err| {
            std.log.err("standalone startup failed step=validate_ha_active_generation err={}", .{err});
            return err;
        }
    else
        null;
    // A reseed may rotate a persisted Lease fence only after the complete,
    // immutable activation chain on this exact target volume has validated.
    // A generic checkpoint or caller-selected startup generation never reaches
    // this receipt writer.
    if (ha_startup_checkpoint_lsn) |checkpoint_lsn| {
        if (ha_startup_expectation) |expectation| {
            if (init.environ_map.get("ANTFLY_HA_LEASE_SENTINEL_PATH")) |sentinel_path| {
                if (init.environ_map.get("ANTFLY_HA_LEASE_TOPOLOGY_ID")) |topology_id| {
                    if (!std.mem.eql(u8, topology_id, expectation.binding.topology_id)) return error.HALeaseSentinelScopeMismatch;
                    const existing = try antfly.ha.kubernetes_lease_watchdog.loadValidatedRepairGenerationAlloc(
                        alloc,
                        setup_io.io(),
                        sentinel_path,
                        topology_id,
                        expectation.binding.node_id,
                    );
                    defer if (existing) |generation| alloc.free(generation);
                    if (existing == null and try antfly.ha.kubernetes_lease_watchdog.sentinelExists(setup_io.io(), sentinel_path)) {
                        _ = try antfly.ha.kubernetes_lease_watchdog.persistRepairReceipt(
                            alloc,
                            setup_io.io(),
                            sentinel_path,
                            topology_id,
                            expectation.binding.node_id,
                            expectation.expected.identity.timeline_id,
                            expectation.expected.identity.epoch,
                            checkpoint_lsn,
                            expectation.materialized_receipt_sha256.?,
                        );
                    }
                }
            }
        }
    }
    var ha_sync_policy = try haSyncPolicyFromCli(alloc, cli);
    defer ha_sync_policy.deinit(alloc);
    const ha_retention_policy = try haRetentionPolicyFromCli(cli);
    var ha_primary = openHAPrimaryFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_primary err={}", .{err});
        return err;
    };
    defer if (ha_primary) |*primary| primary.close();
    var ha_standby = openHAStandbyFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_standby err={}", .{err});
        return err;
    };
    defer if (ha_standby) |*standby| standby.close();
    var ha_fence_store = openHAFenceStoreFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_fence err={}", .{err});
        return err;
    };
    defer if (ha_fence_store) |*store| store.close();
    var ha_former_primary_log = openHAFormerPrimaryLogFromCli(alloc, setup_io.io(), cli) catch |err| {
        std.log.err("standalone startup failed step=open_ha_former_primary err={}", .{err});
        return err;
    };
    defer if (ha_former_primary_log) |*log| log.close();
    const admin_bearer_token = try resolveAdminBearerTokenFromCli(alloc, cli);
    defer if (admin_bearer_token) |token| alloc.free(token);
    const ha_pod_uid = try resolveHAPodUID(alloc);
    defer if (ha_pod_uid) |pod_uid| alloc.free(pod_uid);
    var ha_lease_watchdog = try RuntimeLeaseWatchdog.initFromEnv(
        alloc,
        setup_io.io(),
        init.environ_map,
        cli,
        ha_pod_uid,
    );
    defer if (ha_lease_watchdog) |*watchdog| watchdog.deinit(alloc);

    // Initialize DataServer without starting its listener — the unified
    // httpx.Server will serve the public API instead.
    var data_server = antfly.data.runtime.DataServer.initFromLocalMetadataSources(alloc, .{
        .bind_host = public_listener.bind_host,
        .bind_port = public_listener.bind_port,
        .enable_data_raft = false,
        .replica_root_dir = resolved.replica_root_dir,
        .replica_catalog_path = resolved.replica_catalog_path,
        .snapshot_root_dir = resolved.snapshot_root_dir,
        .process_memory_limit_bytes = process_memory_limit_bytes,
        .process_memory_limit_source = storageMemoryLimitSource(process_memory_resolution.effective_source),
        .store_registration = .{
            .node_id = local_node_id,
            .store_id = 1,
            .api_url = public_api_url,
            .role = "data",
        },
        .api_server_cfg = .{
            .ha_failover_safe_mutations_only = ha_mutation_guard_enabled,
            .ha_remote_apply_mutations_enabled = haRemoteApplyMutationsEnabled(ha_sync_policy.policy),
            .auth_enabled = auth_enabled,
            .experimental = cli.experimental,
            .mcp_max_tool_result_bytes = if (loaded_config) |*cfg| cfg.mcp.max_tool_result_bytes else antfly.common.config.default_mcp_max_tool_result_bytes,
            .query_max_concurrent_requests = if (loaded_config) |*cfg| cfg.admission.query.max_concurrent_requests else antfly.common.config.default_query_max_concurrent_requests,
            .graph_execution_limits = if (loaded_config) |*cfg| cfg.graph_execution else .{},
            .write_max_concurrent_requests = if (loaded_config) |*cfg| cfg.admission.write.max_concurrent_requests else antfly.common.config.default_write_max_concurrent_requests,
            .inference_max_concurrent_requests = if (loaded_config) |*cfg| cfg.admission.inference.max_concurrent_requests else antfly.common.config.default_inference_max_concurrent_requests,
            .inference_request_admission_source = .{
                .ptr = antfly_node,
                .try_acquire_fn = tryAcquireEmbeddedInferenceRequest,
                .release_fn = releaseEmbeddedInferenceRequest,
                .stats_fn = embeddedInferenceRequestStats,
            },
            .local_inference_connection_target = .{
                .capabilities = inference_connection_abi.Capability.streaming_response,
                .context = &local_inference_connection_context,
                .invoke = invokeLocalInferenceConnection,
            },
            .ard_base_url = cli.ard_base_url,
            .ard_publisher_domain = cli.ard_publisher_domain orelse "antfly.local",
            .ard_display_name = cli.ard_display_name orelse "Antfly",
            .ard_public_catalog_enabled = cli.ard_public_catalog_enabled,
            .internal_service_secret = internal_service_secret,
            .internal_service_issuer = internal_service_issuer,
            .deployment_mode = .standalone,
            .storage_maintenance = &storage_maintenance,
            .admin_bearer_token = admin_bearer_token,
            .secret_store = &secret_store,
            .remote_content = remote_content,
            .inference_api_key = if (loaded_config) |*cfg| if (cfg.inference.api_key) |value| value else null else null,
            .extension_package_store_dir = resolved.extension_package_store_dir,
            .node_config = if (loaded_config) |*cfg| cfg else null,
            .user_manager = if (user_manager) |*manager| manager else null,
            .session_store = if (lite_session_store) |*store| store else null,
            .restore_job_store = restore_job_store,
            .incoming_graph_route_store = incoming_graph_route_store,
            .session_ttl_ns = if (loaded_config) |*cfg| cfg.transaction_sessions.ttl_seconds * std.time.ns_per_s else standalone_session_ttl_ns,
            .session_cleanup_interval_ns = if (loaded_config) |*cfg| cfg.transaction_sessions.cleanup_interval_seconds * std.time.ns_per_s else standalone_session_cleanup_interval_ns,
            .session_max_count = if (loaded_config) |*cfg| cfg.transaction_sessions.max_count else standalone_session_max_count,
            .session_max_record_bytes = if (loaded_config) |*cfg| cfg.transaction_sessions.max_record_bytes else standalone_session_max_record_bytes,
            .session_savepoint_limit = if (loaded_config) |*cfg| cfg.transaction_sessions.max_savepoints else standalone_session_savepoint_limit,
        },
        .ha = if (ha_primary != null or ha_standby != null or ha_fence_store != null or ha_former_primary_log != null) .{
            .admin_context = .{
                .primary = if (ha_primary) |*primary| primary else null,
                .primary_node_id = cli.ha_primary_node_id,
                .standby = if (ha_standby) |*standby| standby else null,
                .standby_node_id = cli.ha_standby_node_id,
                .fence_store = if (ha_fence_store) |*store| store else null,
                .former_primary_log = if (ha_former_primary_log) |*log| log else null,
            },
            .standby_owner = if (ha_standby != null) &ha_standby else null,
            .admin_bearer_token = admin_bearer_token,
            .seed_capture_root = cli.ha_seed_capture_root,
            .seed_activation_root = cli.ha_startup_target_root,
            .pod_uid = ha_pod_uid,
            .lease_watchdog_proof = if (ha_lease_watchdog) |*watchdog| watchdog.proofSource() else null,
            .repair_receipt = if (ha_lease_watchdog) |*watchdog| watchdog.repairReceiptSink() else null,
            .internal_primary = if (ha_primary) |*primary| primary else null,
            .primary_retention_policy = ha_retention_policy,
            .primary_sync_policy = ha_sync_policy.policy,
            .standby_replication = try haStandbyReplicationConfigFromCliWithBearerToken(cli, admin_bearer_token),
        } else .{},
        .backend_runtime = node_backend_runtime.ptr(),
    }, local_metadata.catalogSource(), local_metadata.statusSource());
    defer data_server.deinitWithDeadline(supervisor.deadline());
    const managed_memory = data_server.provisioned_storage.resource_manager.snapshot().memory;
    std.log.info(
        "process memory policy operator_source={s} effective_source={s} configured_limit_bytes={d} effective_limit_bytes={d} managed_hard_limit_bytes={d}",
        .{
            @tagName(process_memory_resolution.source),
            @tagName(process_memory_resolution.effective_source),
            process_memory_resolution.configured_limit_bytes,
            data_server.provisioned_storage.effective_memory_limit_bytes,
            managed_memory.hard_limit_bytes,
        },
    );
    var inference_resource_owner = InferenceResourceBudgetOwner{
        .alloc = alloc,
        .manager = &data_server.provisioned_storage.resource_manager,
    };
    // This defer is registered before node teardown, so the node releases all
    // opaque admission leases before the owner validates its registry.
    defer inference_resource_owner.deinit();
    antfly_node_needs_errdeinit = false;
    defer {
        // DataServer sources, recovery workers, and durable API jobs retain the
        // embedded provider. Drain them while the node is valid, then release
        // tokenizer reservations while DataServer's ResourceManager is valid.
        // The earlier data_server.deinit defer performs final storage teardown.
        data_server.quiesceBackgroundWorkWithDeadline(supervisor.deadline());
        if (comptime inline_inference_codegen)
            inference_host.linkedInferenceDestroy(antfly_node)
        else
            linkedInferenceApiInfallible().destroy(antfly_node);
    }

    // Health, metrics, and watchdog supervision share the isolated control
    // lane but own and join their individual futures before releasing it.
    var control_lane_lease = try node_backend_runtime.ptr().acquireControlLane();
    defer control_lane_lease.release();
    const control_io = control_lane_lease.io();

    if (ha_lease_watchdog) |*watchdog| {
        data_server.ha_public_gate_state.requireExternalAuthority();
        if (watchdog.watchdog.latched) {
            data_server.ha_public_gate_state.publishPrimaryFence(true);
        } else {
            // The public listener is not created until this bounded first
            // authority attempt has completed. Failure leaves the primary
            // gate closed and is retried from the main runtime loop.
            try watchdog.poll(alloc, &data_server);
        }
    }
    var ha_watchdog_stop = std.atomic.Value(bool).init(false);
    var ha_watchdog_failed = std.atomic.Value(bool).init(false);
    var ha_watchdog_future = if (ha_lease_watchdog) |*watchdog|
        try control_io.concurrent(RuntimeLeaseWatchdog.runIndependent, .{
            watchdog,
            alloc,
            control_io,
            &data_server,
            &ha_watchdog_stop,
            &ha_watchdog_failed,
        })
    else
        null;
    defer if (ha_watchdog_future) |*future| {
        ha_watchdog_stop.store(true, .release);
        _ = future.await(control_io);
    };

    var inference_resource_budget = inference_bridge.ResourceBudget{
        .abi_version = inference_bridge.abi_version,
        .context = &inference_resource_owner,
        .retain_context = retainInferenceResourceOwner,
        .release_context = releaseInferenceResourceOwner,
        .reserve_admission = reserveInferenceResources,
        .retain_admission = retainInferenceResources,
        .release_admission = releaseInferenceResources,
        .observe_prompt_cache = observeInferencePromptCache,
        .observe_tokenizer_cache = observeInferenceTokenizerCache,
    };
    const configure_context = inference_bridge.ConfigureContext{
        .abi_version = inference_bridge.abi_version,
        .handle = antfly_node,
        .resource_budget = &inference_resource_budget,
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceConfigure(&configure_context);
    } else {
        const configure_status = (try linkedInferenceApi(
            inference_bridge.Capability.resource_budget,
        )).configure(&configure_context);
        if (!configure_status.isOk()) return inference_bridge.errorFromStatus(configure_status);
    }
    data_server.setAntflyProvider(inferenceBoundaryProvider(antfly_node));

    // Initialize API server (wires caches + sources) without binding a listener.
    try data_server.initApiServer();
    local_metadata.local_schema_progress_provider = localSchemaProgressProvider(&data_server);
    const api_server = &data_server.http_server.?;
    // Recovery is a startup concern: enqueue durable work before the listener is
    // marked ready instead of waiting for an unrelated request to arrive.
    try api_server.resumeRestoreJobsOnce();
    data_server.registerNodeIfConfigured() catch |err| {
        std.log.err("standalone startup failed step=register_node err={}", .{err});
        return err;
    };
    data_server.requestProvisionedStartupCatchUpNow() catch |err| {
        std.log.warn("standalone startup provisioned startup catch-up skipped err={}", .{err});
    };
    data_server.requestProvisionedCacheWarmup() catch |err| {
        std.log.warn("standalone startup provisioned cache warmup skipped err={}", .{err});
    };

    // ---------------------------------------------------------------
    // Unified httpx.Server — all routes on a single port
    // ---------------------------------------------------------------

    var handler = try antfly.public_api.kernel_bridge.createHandler(api_server);
    handler.initRuntime(alloc) catch |err| {
        antfly.public_api.kernel_bridge.deinitHandler(&handler);
        return err;
    };
    defer antfly.public_api.kernel_bridge.deinitHandler(&handler);

    const bind_host = public_listener.bind_host;
    const bind_port = public_listener.bind_port;
    const cors_config = configuredCors(api_server.cfg.node_config);

    var unified_api_ready = std.atomic.Value(bool).init(false);

    var unified_lifecycle = UnifiedServerLifecycle.init(control_io);
    const public_http_config = publicHttpServerConfig(bind_host, bind_port);
    var http_runtime = httpx.HttpRuntime.init(alloc, .{
        .max_active_h1_requests = public_http_config.max_connections,
        .max_active_connections = @as(usize, public_http_config.max_connections) +| antfly.common.health_server.max_connections,
        .max_active_requests = @as(usize, public_http_config.max_request_tasks) +| antfly.common.health_server.max_connections,
    });
    defer http_runtime.deinit();
    var standalone_health = StandaloneHealthSource{
        .data_server = &data_server,
        .unified_api_ready = &unified_api_ready,
        .supervisor = &supervisor,
        .startup_checkpoint_lsn = ha_startup_checkpoint_lsn,
        .handler = &handler,
        .unified_lifecycle = &unified_lifecycle,
    };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = antfly.common.health_server.HealthServer.startIfConfiguredOnHostWithRuntime(
        alloc,
        control_io,
        "standalone",
        public_listener.bind_host,
        health_port,
        standalone_health.readiness(),
        standalone_health.metricsWriter(),
        &http_runtime,
    ) catch |err| {
        std.log.err("standalone startup failed step=health_server err={}", .{err});
        return err;
    };
    defer if (health_server) |hs| hs.deinitWithDeadline(supervisor.deadline());

    var api_lane_lease = try node_backend_runtime.ptr().acquireApiLane();
    defer api_lane_lease.release();
    const public_io = api_lane_lease.io();
    var unified_future = (if (comptime inline_inference_codegen)
        control_io.concurrent(serveUnifiedWithInference, .{
            alloc,
            public_io,
            public_http_config,
            cors_config,
            &handler,
            antfly_node,
            api_server,
            &local_metadata,
            &unified_api_ready,
            &unified_lifecycle,
            &http_runtime,
        })
    else
        control_io.concurrent(serveUnifiedWithLinkedInference, .{
            alloc,
            public_io,
            public_http_config,
            cors_config,
            &handler,
            antfly_node,
            api_server,
            &local_metadata,
            &unified_api_ready,
            &unified_lifecycle,
            &http_runtime,
        })) catch |err| {
        std.log.err("standalone startup failed step=schedule_unified_http err={}", .{err});
        return err;
    };
    var future_awaited = false;
    defer if (!future_awaited) {
        unified_lifecycle.stop();
        _ = unified_future.await(control_io);
    };
    unified_lifecycle.waitForStartup(supervisor.startupDeadline(), termination_signals.token()) catch |err| {
        std.log.err("standalone startup failed step=bind_unified_http err={}", .{err});
        return err;
    };

    // Print only after the public listener has successfully bound.
    std.debug.print("standalone local metadata enabled (raft disabled)\n", .{});

    const runtime_cadence = antfly.raft.RuntimeCadence.fromMillis(
        antfly.raft.RuntimeCadence.default_raft_tick_ms,
        cli.control_tick_ms,
    ) catch return error.InvalidArguments;
    const tick_ms = @divExact(runtime_cadence.control_tick_ns, std.time.ns_per_ms);
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    try supervisor.publishReady();
    while (!supervisor.shouldStop(termination_signals.cancellationRequested())) {
        if (unified_lifecycle.runtimeFailure()) |err| return supervisor.fail("standalone", "unified-http", err);
        if (ha_watchdog_failed.load(.acquire)) return supervisor.fail("standalone", "ha-watchdog", error.HALeaseWatchdogWorkerFailed);
        data_server.runRound() catch |err| switch (err) {
            error.LsmRootWriterAlreadyOpen, error.WriterLocked => std.log.warn("standalone data round skipped err={}", .{err}),
            else => return supervisor.fail("standalone", "data-round", err),
        };
        if (!ha_role_requested) {
            LocalStandaloneMetadata.runRound(&local_metadata) catch |err| switch (err) {
                error.LsmRootWriterAlreadyOpen, error.WriterLocked => std.log.warn("standalone metadata round skipped err={}", .{err}),
                else => return supervisor.fail("standalone", "metadata-round", err),
            };
        }
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => {},
            .INTR => continue,
            else => return supervisor.fail("standalone", "control-wait", std.posix.unexpectedErrno(err)),
        }
    }

    const process_shutdown_deadline = supervisor.deadline();
    unified_lifecycle.shutdown(process_shutdown_deadline);
    _ = unified_future.await(control_io);
    future_awaited = true;
    if (unified_lifecycle.runtimeFailure()) |err| return supervisor.fail("standalone", "unified-http", err);
}

fn validateEffectiveStandaloneStorage(
    cli: CliConfig,
    storage_engine: antfly.common.config.StorageEngine,
    lite_path: ?[]const u8,
    loaded_config: ?*const antfly.common.config.Config,
) !void {
    if (storage_engine == .object) return error.UnsupportedStandaloneStorageEngine;
    if (storage_engine != .lite) {
        if (cli.storage_path != null or cli.storage_fsync != null) return error.InvalidArguments;
        return;
    }
    const path = lite_path orelse return error.MissingLiteStoragePath;
    if (!std.mem.endsWith(u8, path, ".aflite")) return error.InvalidLiteStoragePath;
    if (loaded_config) |cfg| {
        if (cfg.metadata.orchestration_urls.len != 0 or cfg.metadata.raft_urls.len != 0) {
            return error.LiteExternalMetadataUnsupported;
        }
        if (cfg.shard_allocation.default_shards_per_table != 1 or
            cfg.shard_allocation.min_shards_per_table != 1 or
            !cfg.shard_allocation.disable_shard_alloc)
        {
            return error.LiteHorizontalShardingUnsupported;
        }
    }
}

pub fn runLite(
    init: std.process.Init,
    path: []const u8,
    host: []const u8,
    port: u16,
    fsync: bool,
    extra_args: []const []const u8,
) !void {
    const path_z = try init.gpa.dupeZ(u8, path);
    defer init.gpa.free(path_z);
    const host_z = try init.gpa.dupeZ(u8, host);
    defer init.gpa.free(host_z);
    var port_buf: [16]u8 = undefined;
    const port_z = try std.fmt.bufPrintZ(&port_buf, "{d}", .{port});
    var argv = std.ArrayListUnmanaged([*:0]const u8).empty;
    defer argv.deinit(init.gpa);
    try argv.appendSlice(init.gpa, &.{
        "--storage-engine",
        "lite",
        "--storage-path",
        path_z.ptr,
        "--host",
        host_z.ptr,
        "--port",
        port_z.ptr,
        if (fsync) "--fsync=true" else "--fsync=false",
    });
    const owned_extra = try init.gpa.alloc([:0]u8, extra_args.len);
    var owned_extra_count: usize = 0;
    defer {
        for (owned_extra[0..owned_extra_count]) |value| init.gpa.free(value);
        init.gpa.free(owned_extra);
    }
    for (extra_args, 0..) |arg, i| {
        owned_extra[i] = try init.gpa.dupeZ(u8, arg);
        owned_extra_count += 1;
        try argv.append(init.gpa, owned_extra[i].ptr);
    }
    var args = std.process.Args.Iterator.init(.{ .vector = argv.items });
    try runFromIterator(init, "antfly standalone", &args);
}

// Unified server task
// ---------------------------------------------------------------

fn serveUnifiedWithInference(
    alloc: std.mem.Allocator,
    io: std.Io,
    public_http_config: httpx.ServerConfig,
    cors_config: ?*const antfly.common.config.Config.CorsConfig,
    handler: *ApiKernelHandler,
    antfly_node: *anyopaque,
    api_server: *ApiHttpServer,
    local_metadata: *LocalStandaloneMetadata,
    unified_api_ready: *std.atomic.Value(bool),
    lifecycle: *UnifiedServerLifecycle,
    http_runtime: *httpx.HttpRuntime,
) void {
    serveUnifiedInner(true, alloc, io, public_http_config, cors_config, handler, antfly_node, api_server, local_metadata, unified_api_ready, lifecycle, http_runtime) catch |err| {
        unified_api_ready.store(false, .release);
        lifecycle.publishFailure(err);
        std.debug.print("unified server error: {}\n", .{err});
        return;
    };
    unified_api_ready.store(false, .release);
    lifecycle.publishStopped();
}

fn serveUnifiedWithLinkedInference(
    alloc: std.mem.Allocator,
    io: std.Io,
    public_http_config: httpx.ServerConfig,
    cors_config: ?*const antfly.common.config.Config.CorsConfig,
    handler: *ApiKernelHandler,
    inference_handle: *anyopaque,
    api_server: *ApiHttpServer,
    local_metadata: *LocalStandaloneMetadata,
    unified_api_ready: *std.atomic.Value(bool),
    lifecycle: *UnifiedServerLifecycle,
    http_runtime: *httpx.HttpRuntime,
) void {
    serveUnifiedInner(false, alloc, io, public_http_config, cors_config, handler, inference_handle, api_server, local_metadata, unified_api_ready, lifecycle, http_runtime) catch |err| {
        unified_api_ready.store(false, .release);
        lifecycle.publishFailure(err);
        std.debug.print("unified server error: {}\n", .{err});
        return;
    };
    unified_api_ready.store(false, .release);
    lifecycle.publishStopped();
}

fn serveUnifiedInner(
    comptime inline_inference: bool,
    alloc: std.mem.Allocator,
    io: std.Io,
    public_http_config: httpx.ServerConfig,
    cors_config: ?*const antfly.common.config.Config.CorsConfig,
    handler: *ApiKernelHandler,
    antfly_node: *anyopaque,
    api_server: *ApiHttpServer,
    local_metadata: *LocalStandaloneMetadata,
    unified_api_ready: *std.atomic.Value(bool),
    lifecycle: *UnifiedServerLifecycle,
    http_runtime: *httpx.HttpRuntime,
) !void {
    var server_config = public_http_config;
    server_config.http_runtime = http_runtime;
    var server = httpx.Server.initWithConfig(alloc, io, server_config);
    defer server.deinit();
    var route_context = StandaloneHttpContext{
        .api_server = api_server,
        .cors_config = cors_config,
    };
    try lifecycle.attach(&server);
    defer lifecycle.detach(&server);

    if (corsEnabled(cors_config)) try server.use(corsMiddleware(&route_context));
    try server.use(inferenceAuthMiddleware(&route_context));
    try server.use(interactiveGenerateMiddleware());

    // Register inference AI routes under /ai/v1 and Traditional ML routes under /ml/v1.
    var linked_inference_routes: std.ArrayListUnmanaged(*LinkedInferenceRoute) = .empty;
    defer {
        for (linked_inference_routes.items) |route| alloc.destroy(route);
        linked_inference_routes.deinit(alloc);
    }
    if (comptime inline_inference) {
        try inference_host.linkedInferenceRegisterRoutesOn(antfly_node, &server);
    } else {
        const functions = try linkedInferenceApi(inference_bridge.Capability.route_manifest);
        try registerLinkedInferenceManifest(
            alloc,
            &server,
            antfly_node,
            functions,
            &linked_inference_routes,
        );
    }

    // Runtime roles consume the shared direct/linked kernel registrar instead
    // of carrying copies of the generated route manifest. Standalone supplies
    // its stronger Kubernetes-style root readiness contract locally.
    try server.get(antfly.public_api.http_routes.Routes.healthz, healthzHandler);
    try server.get(
        antfly.public_api.http_routes.Routes.readyz,
        httpx.Handler.bind(&route_context, readyzHandler),
    );
    try handler.registerRoutesWithoutProbes(&server);

    try server.use(httpx.Middleware.bind("storage-maintenance-admission", &route_context, storageMaintenanceAdmission));
    try registerAntfarmRoutes(&server);

    var listener_task = httpx.ListenerTask.init(&server);
    listener_task.start() catch |err| {
        const stats = http_runtime.stats();
        std.log.err(
            "standalone public listener admission failed err={s} requested_connections={d} requested_requests={d} runtime_connections={d} reserved_connections={d} runtime_requests={d} reserved_requests={d}",
            .{
                @errorName(err),
                public_http_config.max_connections,
                public_http_config.max_request_tasks,
                stats.connection_capacity,
                stats.reserved_connection_capacity,
                stats.request_capacity,
                stats.reserved_request_capacity,
            },
        );
        return err;
    };
    var listener_joined = false;
    defer if (!listener_joined) {
        listener_task.requestStop();
        listener_task.join() catch {};
    };
    if (public_http_config.port == 0) {
        const bound_address = server.boundAddress() orelse return error.PublicListenerAddressUnavailable;
        const bound_port = switch (bound_address) {
            .ip4 => |address| address.port,
            .ip6 => |address| address.port,
        };
        const bound_api_url = try std.fmt.allocPrint(
            alloc,
            "http://{s}:{d}",
            .{ public_http_config.host, bound_port },
        );
        defer alloc.free(bound_api_url);
        try local_metadata.setApiUrl(bound_api_url);
    }
    unified_api_ready.store(true, .release);
    try lifecycle.publishReady();

    if (server.boundAddress()) |addr| {
        std.debug.print("standalone public api listening on http://{f}\n", .{addr});
    }

    try listener_task.join();
    listener_joined = true;
}

const LinkedInferenceRoute = struct {
    functions: *const inference_bridge.FunctionTable,
    kernel_route_handle: *anyopaque,
    request_body: runtime_http_abi.RequestBodyMode,
    streaming_response: bool,
};

fn registerLinkedInferenceManifest(
    alloc: std.mem.Allocator,
    server: *httpx.Server,
    inference_handle: *anyopaque,
    functions: *const inference_bridge.FunctionTable,
    owned_routes: *std.ArrayListUnmanaged(*LinkedInferenceRoute),
) !void {
    var entries_ptr: ?[*]const inference_bridge.RouteManifestEntry = null;
    var entries_len: usize = 0;
    const status = functions.route_manifest(&.{
        .abi_version = inference_bridge.abi_version,
        .handle = inference_handle,
        .out_entries = &entries_ptr,
        .out_len = &entries_len,
    });
    if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    const entries = if (entries_ptr) |ptr| ptr[0..entries_len] else &.{};
    for (entries) |entry| {
        const route = try alloc.create(LinkedInferenceRoute);
        errdefer alloc.destroy(route);
        route.* = .{
            .functions = functions,
            .kernel_route_handle = entry.route_handle,
            .request_body = entry.request_body,
            .streaming_response = entry.streaming_response != 0,
        };
        try owned_routes.append(alloc, route);
        errdefer _ = owned_routes.pop();
        try server.routeWithData(switch (entry.method) {
            .get => .GET,
            .post => .POST,
            .put => .PUT,
            .delete => .DELETE,
        }, entry.path.slice(), linkedInferenceHttpHandler, route);
    }
}

fn linkedInferenceHttpHandler(context: *httpx.Context) anyerror!httpx.Response {
    const route: *const LinkedInferenceRoute = @ptrCast(@alignCast(context.route_data orelse return error.InferenceRuntimeUnavailable));
    const source_headers = context.request.headers.iterator();
    const headers = try context.allocator.alloc(runtime_http_abi.HeaderView, source_headers.len);
    defer context.allocator.free(headers);
    for (source_headers, 0..) |header, i| {
        headers[i] = .{
            .name = runtime_http_abi.Bytes.init(header.name),
            .value = runtime_http_abi.Bytes.init(header.value),
        };
    }
    const params = try context.allocator.alloc(runtime_http_abi.RouteParamView, context.params.len);
    defer context.allocator.free(params);
    for (context.params, 0..) |param, i| {
        params[i] = .{
            .name = runtime_http_abi.Bytes.init(param.name),
            .value = runtime_http_abi.Bytes.init(param.value),
        };
    }

    const request_view: runtime_http_abi.HttpRequestView = .{
        .method = switch (context.request.method) {
            .GET => .get,
            .POST => .post,
            .PUT => .put,
            .DELETE => .delete,
            else => return error.MethodNotAllowed,
        },
        .path = runtime_http_abi.Bytes.init(context.request.uri.path),
        .query = runtime_http_abi.OptionalBytes.init(context.request.uri.query),
        .headers_ptr = if (headers.len == 0) null else headers.ptr,
        .headers_len = headers.len,
        .params_ptr = if (params.len == 0) null else params.ptr,
        .params_len = params.len,
        .body = runtime_http_abi.OptionalBytes.init(context.request.body),
        .authorization = runtime_http_abi.OptionalBytes.init(context.request.headers.get("Authorization")),
        .content_type = runtime_http_abi.OptionalBytes.init(context.request.headers.get("Content-Type")),
    };
    var transport = @import("../runtime_http_bridge.zig").Outbound{ .context = context };
    const body_source = if (route.request_body == .buffered) transport.bodySource() else runtime_http_abi.RequestBodySource{};
    var response_handle: ?*anyopaque = null;
    var response_view: runtime_http_abi.HttpResponseView = undefined;
    const status = route.functions.handle_http(&.{
        .abi_version = inference_bridge.abi_version,
        .route_handle = route.kernel_route_handle,
        .request = &request_view,
        .cancellation = transport.cancellation(),
        .body_source = body_source,
        .stream = if (route.streaming_response) transport.stream() else .{},
        .out_response_handle = &response_handle,
        .out_response = &response_view,
    });
    if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    const owned_response_handle = response_handle orelse return error.RuntimeBoundaryFailure;
    defer route.functions.destroy_http_response(owned_response_handle);

    var response = httpx.Response.init(context.allocator, response_view.status);
    errdefer response.deinit();
    if (response_view.content_type.slice()) |content_type|
        try response.headers.set("Content-Type", content_type);
    const response_headers = if (response_view.headers_ptr) |ptr| ptr[0..response_view.headers_len] else &.{};
    for (response_headers) |header| {
        if (response_view.content_type.slice() != null and
            std.ascii.eqlIgnoreCase(header.name.slice(), "Content-Type")) continue;
        try response.headers.append(header.name.slice(), header.value.slice());
    }
    response.body = try context.allocator.dupe(u8, response_view.body.slice());
    response.body_owned = true;
    return response;
}

const public_http_connection_ceiling: u32 = 256;
const public_http_max_h1_inflight_bodies: u32 = 32;

fn publicHttpConnectionLimitForFdSoftLimit(fd_soft_limit: u64) u32 {
    // Public inbound sockets may use at most one quarter of the process FD
    // budget. Storage files, Raft, metadata, outbound providers, logs, and an
    // operator shell retain the rest even when the deployment lowers RLIMIT.
    const proportional_limit = @max(@as(u64, 1), fd_soft_limit / 4);
    return @intCast(@min(proportional_limit, public_http_connection_ceiling));
}

fn configuredPublicHttpConnectionLimit() u32 {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return public_http_connection_ceiling;
    const limit = std.posix.getrlimit(.NOFILE) catch return public_http_connection_ceiling;
    if (limit.cur == std.math.maxInt(@TypeOf(limit.cur))) return public_http_connection_ceiling;
    return publicHttpConnectionLimitForFdSoftLimit(@intCast(limit.cur));
}

fn publicHttpServerConfig(bind_host: []const u8, bind_port: u16) httpx.ServerConfig {
    return (httpx.ServerConfig{
        .host = bind_host,
        .port = bind_port,
        .max_body_size = antfly.public_api.http_server.public_api_max_request_body_bytes,
        // Bound aggregate request-body buffering across HTTP/1 and HTTP/2.
        // Four maximum-sized public requests may complete while excess uploads
        // are shed before allocator pressure becomes systemic.
        .request_body_buffer_budget_bytes = 256 * 1024 * 1024,
        // This is a transport safeguard for every H1 request body. Keep it
        // independent from admission.query.max_concurrent_requests.
        .max_h1_inflight_bodies = public_http_max_h1_inflight_bodies,
        .header_read_timeout_ms = 300_000,
        .body_read_timeout_ms = 300_000,
        .response_write_timeout_ms = 300_000,
        // Keep a large process-wide FD reserve for storage, Raft, outbound
        // clients, and diagnostics. This prevents the historical 1,000-socket
        // cliff under the common 1,024 descriptor soft limit.
        .max_connections = configuredPublicHttpConnectionLimit(),
        .accept_error_backoff_initial_ms = 5,
        .accept_error_backoff_max_ms = 1_000,
        .max_requests_per_connection = public_api_max_requests_per_connection,
        // httpx keeps SO_REUSEADDR separate from the opt-in SO_REUSEPORT flag,
        // preserving fast restarts without allowing two live runtimes to share
        // the public bind tuple.
        .reuse_address = true,
        .reuse_port = false,
    }).normalized();
}

fn healthzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return ctx.json(.{ .status = "ok" });
}

fn readyzHandler(route_context: *StandaloneHttpContext, ctx: *httpx.Context) anyerror!httpx.Response {
    const server = route_context.api_server orelse {
        try ctx.setHeader("Retry-After", "1");
        return ctx.status(503).json(.{ .status = "not_ready" });
    };
    if (server.storageMaintenanceExclusiveActive()) {
        try ctx.setHeader("Retry-After", "1");
        return ctx.status(503).json(.{ .status = "maintenance" });
    }
    server.checkReady() catch {
        try ctx.setHeader("Retry-After", "1");
        return ctx.status(503).json(.{ .status = "not_ready" });
    };
    return ctx.json(.{ .status = "ready" });
}

fn storageMaintenanceAdmission(route_context: *StandaloneHttpContext, ctx: *httpx.Context, next: *httpx.Next) anyerror!httpx.Response {
    const api_server = route_context.api_server orelse return next.call(ctx);
    if (!api_server.storageMaintenanceExclusiveActive()) return next.call(ctx);
    const path = ctx.request.uri.path;
    if (std.mem.eql(u8, path, "/healthz") or
        std.mem.eql(u8, path, "/readyz") or
        std.mem.startsWith(u8, path, antfly.admin.routes.maintenance ++ "/"))
    {
        return next.call(ctx);
    }
    _ = ctx.status(503);
    return ctx.text("storage maintenance in progress");
}

fn inferenceAuthMiddleware(route_context: *StandaloneHttpContext) httpx.Middleware {
    return httpx.Middleware.bind("inference_auth", route_context, inferenceAuth);
}

fn inferenceAuth(route_context: *StandaloneHttpContext, ctx: *httpx.Context, next: *httpx.Next) anyerror!httpx.Response {
    if (!isInferenceApiPath(ctx.request.uri.path)) return next.call(ctx);

    const server = route_context.api_server orelse return inferenceNotReadyResponse(ctx);
    const permission: antfly.public_api.kernel_abi.InferencePermission = switch (ctx.request.method) {
        .GET, .HEAD, .OPTIONS => .read,
        else => .write,
    };
    const decision = server.authorizeInferenceRequest(.{
        .authorization = ctx.header("authorization"),
        .trusted_principal = ctx.header(antfly.public_api.http_server.trusted_principal_header),
    }, permission) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return inferenceNotReadyResponse(ctx),
    };
    return switch (decision) {
        .allowed => next.call(ctx),
        .unauthorized => inferenceUnauthorizedResponse(ctx),
        .forbidden => inferenceForbiddenResponse(ctx, permission),
        .not_ready => inferenceNotReadyResponse(ctx),
    };
}

fn interactiveGenerateMiddleware() httpx.Middleware {
    return .{
        .name = "interactive_generate",
        .handler = struct {
            fn handler(ctx: *httpx.Context, next: *httpx.Next) anyerror!httpx.Response {
                if (!isInteractiveGeneratePath(ctx.request.uri.path)) return next.call(ctx);
                _ = antfly.db.enrichment_types.interactive_generate_inflight.fetchAdd(1, .monotonic);
                defer _ = antfly.db.enrichment_types.interactive_generate_inflight.fetchSub(1, .monotonic);
                return next.call(ctx);
            }
        }.handler,
    };
}

fn corsMiddleware(route_context: *StandaloneHttpContext) httpx.Middleware {
    return httpx.Middleware.bind("cors", route_context, corsRequest);
}

fn corsRequest(route_context: *StandaloneHttpContext, ctx: *httpx.Context, next: *httpx.Next) anyerror!httpx.Response {
    const config = route_context.cors_config orelse return next.call(ctx);
    if (!(config.enabled orelse true)) return next.call(ctx);

    const origin = ctx.header("origin") orelse return next.call(ctx);
    const requested_method = if (ctx.request.method == .OPTIONS)
        ctx.header("access-control-request-method")
    else
        null;
    const is_preflight = requested_method != null;
    const allowed_origin = corsAllowedOrigin(config, origin);

    if (allowed_origin == null or
        (is_preflight and !corsMethodAllowed(config, requested_method.?)) or
        (is_preflight and !corsRequestHeadersAllowed(config, ctx.header("access-control-request-headers"))) or
        (!is_preflight and !corsMethodAllowed(config, ctx.request.method.toString())))
    {
        if (!is_preflight) {
            try ctx.response.headers.append("Vary", "Origin");
            return next.call(ctx);
        }
        try appendCorsPreflightVary(&ctx.response.headers, true);
        return ctx.status(403).text("CORS request denied");
    }

    try applyCorsOriginHeaders(&ctx.response.headers, config, allowed_origin.?);
    if (!is_preflight) {
        try applyCorsExposedHeaders(ctx, config);
        return next.call(ctx);
    }

    try appendCorsPreflightVary(&ctx.response.headers, false);
    try applyCorsPreflightHeaders(ctx, config);
    return ctx.status(204).text("");
}

fn applyCorsOriginHeaders(
    headers: *httpx.Headers,
    config: *const antfly.common.config.Config.CorsConfig,
    allowed_origin: []const u8,
) !void {
    try headers.set("Access-Control-Allow-Origin", allowed_origin);
    if (!std.mem.eql(u8, allowed_origin, "*")) try headers.append("Vary", "Origin");
    if (config.allow_credentials orelse false) try headers.set("Access-Control-Allow-Credentials", "true");
}

fn appendCorsPreflightVary(headers: *httpx.Headers, include_origin: bool) !void {
    if (include_origin) try headers.append("Vary", "Origin");
    try headers.append("Vary", "Access-Control-Request-Method");
    try headers.append("Vary", "Access-Control-Request-Headers");
}

fn applyCorsExposedHeaders(ctx: *httpx.Context, config: *const antfly.common.config.Config.CorsConfig) !void {
    const exposed = config.exposed_headers orelse &cors_default_exposed_headers;
    if (exposed.len == 0) return;
    const joined = try joinCorsValues(ctx.allocator, exposed);
    defer ctx.allocator.free(joined);
    try ctx.response.headers.set("Access-Control-Expose-Headers", joined);
}

fn applyCorsPreflightHeaders(ctx: *httpx.Context, config: *const antfly.common.config.Config.CorsConfig) !void {
    const methods = if (config.allowed_methods) |values|
        try joinCorsValues(ctx.allocator, values)
    else
        try joinCorsValues(ctx.allocator, &cors_default_methods);
    defer ctx.allocator.free(methods);
    try ctx.response.headers.set("Access-Control-Allow-Methods", methods);

    // With credentials, Fetch treats `*` as the literal header name rather
    // than a wildcard. The request list has already been token-validated, so
    // reflect it explicitly to preserve the configured "allow any" intent.
    const credentialed_wildcard_headers = (config.allow_credentials orelse false) and corsAllowsAnyHeader(config);
    const headers = if (credentialed_wildcard_headers and ctx.header("access-control-request-headers") != null)
        try ctx.allocator.dupe(u8, ctx.header("access-control-request-headers").?)
    else if (config.allowed_headers) |values|
        try joinCorsValues(ctx.allocator, values)
    else
        try joinCorsValues(ctx.allocator, &cors_default_headers);
    defer ctx.allocator.free(headers);
    try ctx.response.headers.set("Access-Control-Allow-Headers", headers);

    var max_age_buf: [10]u8 = undefined;
    const max_age = try std.fmt.bufPrint(&max_age_buf, "{d}", .{config.max_age orelse cors_default_max_age});
    try ctx.response.headers.set("Access-Control-Max-Age", max_age);
}

fn joinCorsValues(alloc: std.mem.Allocator, values: anytype) ![]u8 {
    var size: usize = 0;
    for (values, 0..) |value, i| size += value.len + @as(usize, if (i == 0) 0 else 2);
    const joined = try alloc.alloc(u8, size);
    var offset: usize = 0;
    for (values, 0..) |value, i| {
        if (i != 0) {
            @memcpy(joined[offset..][0..2], ", ");
            offset += 2;
        }
        @memcpy(joined[offset..][0..value.len], value);
        offset += value.len;
    }
    return joined;
}

fn corsAllowedOrigin(config: *const antfly.common.config.Config.CorsConfig, origin: []const u8) ?[]const u8 {
    if (!isSafeCorsOrigin(origin)) return null;
    if (config.allowed_origins) |origins| {
        if (origins.len != 0) {
            for (origins) |allowed| if (std.mem.eql(u8, allowed, "*")) return "*";
            for (origins) |allowed| {
                if (std.mem.eql(u8, allowed, origin)) return origin;
            }
            return null;
        }
    }
    return "*";
}

fn corsMethodAllowed(config: *const antfly.common.config.Config.CorsConfig, method: []const u8) bool {
    if (config.allowed_methods) |methods| {
        for (methods) |allowed| if (std.mem.eql(u8, allowed, method)) return true;
        return false;
    }
    for (cors_default_methods) |allowed| if (std.mem.eql(u8, allowed, method)) return true;
    return false;
}

fn corsRequestHeadersAllowed(config: *const antfly.common.config.Config.CorsConfig, requested: ?[]const u8) bool {
    const raw = requested orelse return true;
    var values = std.mem.splitScalar(u8, raw, ',');
    while (values.next()) |value| {
        const name = std.mem.trim(u8, value, " \t");
        if (!isHttpToken(name) or !corsHeaderAllowed(config, name)) return false;
    }
    return true;
}

fn corsHeaderAllowed(config: *const antfly.common.config.Config.CorsConfig, name: []const u8) bool {
    if (config.allowed_headers) |headers| {
        for (headers) |allowed| {
            if (std.mem.eql(u8, allowed, "*") or std.ascii.eqlIgnoreCase(allowed, name)) return true;
        }
        return false;
    }
    for (cors_default_headers) |allowed| if (std.ascii.eqlIgnoreCase(allowed, name)) return true;
    return false;
}

fn corsAllowsAnyHeader(config: *const antfly.common.config.Config.CorsConfig) bool {
    const headers = config.allowed_headers orelse return false;
    for (headers) |allowed| if (std.mem.eql(u8, allowed, "*")) return true;
    return false;
}

fn configuredCors(config: ?*const antfly.common.config.Config) ?*const antfly.common.config.Config.CorsConfig {
    const loaded = config orelse return null;
    return if (loaded.cors) |*cors| cors else null;
}

fn corsEnabled(config: ?*const antfly.common.config.Config.CorsConfig) bool {
    const cors = config orelse return false;
    return cors.enabled orelse true;
}

fn validateCorsConfig(config: ?*const antfly.common.config.Config.CorsConfig) !void {
    const cors = config orelse return;
    if (!(cors.enabled orelse true)) return;

    const allow_credentials = cors.allow_credentials orelse false;
    if (cors.allowed_origins) |origins| {
        if (origins.len == 0 and allow_credentials) return error.CorsCredentialsWithWildcardOrigin;
        for (origins) |origin| {
            if (!isSafeCorsOrigin(origin)) return error.InvalidCorsOrigin;
            if (allow_credentials and std.mem.eql(u8, origin, "*")) return error.CorsCredentialsWithWildcardOrigin;
            if (allow_credentials and std.mem.eql(u8, origin, "null")) return error.CorsCredentialsWithOpaqueOrigin;
        }
    } else if (allow_credentials) {
        return error.CorsCredentialsWithWildcardOrigin;
    }

    if (cors.allowed_methods) |methods| for (methods) |method| {
        if (httpx.Method.fromString(method) == null) return error.InvalidCorsMethod;
    };
    if (cors.allowed_headers) |headers| for (headers) |header| {
        if (!isHttpToken(header)) return error.InvalidCorsHeader;
    };
    if (cors.exposed_headers) |headers| for (headers) |header| {
        if (!isHttpToken(header)) return error.InvalidCorsHeader;
        if (allow_credentials and std.mem.eql(u8, header, "*")) return error.CorsCredentialsWithWildcardExposedHeaders;
    };
}

fn isSafeCorsOrigin(origin: []const u8) bool {
    if (std.mem.eql(u8, origin, "*") or std.mem.eql(u8, origin, "null")) return true;
    const scheme_end = std.mem.indexOf(u8, origin, "://") orelse return false;
    if (scheme_end == 0 or scheme_end + 3 == origin.len or !std.ascii.isAlphabetic(origin[0])) return false;
    for (origin[1..scheme_end]) |char| {
        if (!std.ascii.isAlphanumeric(char) and char != '+' and char != '-' and char != '.') return false;
    }
    for (origin[scheme_end + 3 ..]) |char| {
        if (char <= ' ' or char >= 0x7f or char == '/' or char == '?' or char == '#' or char == '@' or char == ',') return false;
    }
    return true;
}

fn isHttpToken(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |char| switch (char) {
        'a'...'z', 'A'...'Z', '0'...'9', '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~' => {},
        else => return false,
    };
    return true;
}

fn isInferenceApiPath(path: []const u8) bool {
    return hasPathComponentPrefix(path, inference_bridge.ai_api_prefix) or
        hasPathComponentPrefix(path, inference_bridge.public_api_prefix);
}

fn isInteractiveGeneratePath(path: []const u8) bool {
    for ([_][]const u8{ inference_bridge.ai_api_prefix, inference_bridge.public_api_prefix }) |prefix| {
        if (!std.mem.startsWith(u8, path, prefix)) continue;
        const suffix = path[prefix.len..];
        if (std.mem.eql(u8, suffix, "/generate") or
            std.mem.eql(u8, suffix, "/generate/batch") or
            std.mem.eql(u8, suffix, "/chat/completions")) return true;
    }
    return false;
}

fn hasPathComponentPrefix(path: []const u8, prefix: []const u8) bool {
    return std.mem.eql(u8, path, prefix) or
        (std.mem.startsWith(u8, path, prefix) and path.len > prefix.len and path[prefix.len] == '/');
}

fn inferenceUnauthorizedResponse(ctx: *httpx.Context) !httpx.Response {
    try ctx.setHeader("WWW-Authenticate", "Basic realm=\"antfly\", Bearer realm=\"antfly\", ApiKey realm=\"antfly\"");
    return ctx.status(401).json(.{
        .@"error" = "unauthorized",
        .message = "valid Basic, Bearer, or ApiKey credentials are required",
        .retryable = false,
    });
}

fn inferenceForbiddenResponse(
    ctx: *httpx.Context,
    permission: antfly.public_api.kernel_abi.InferencePermission,
) !httpx.Response {
    return ctx.status(403).json(.{
        .@"error" = "forbidden",
        .message = switch (permission) {
            .read => "inference read permission is required",
            .write => "inference write permission is required",
        },
        .retryable = false,
    });
}

fn inferenceNotReadyResponse(ctx: *httpx.Context) !httpx.Response {
    try ctx.setHeader("Retry-After", "1");
    return ctx.status(503).json(.{
        .@"error" = "not_ready",
        .message = "inference authentication is not ready",
        .retryable = true,
    });
}

fn registerAntfarmRoutes(server: anytype) !void {
    try server.get("/", antfarmIndexHandler);
    try server.get("/assets/*", antfarmAssetHandler);
    try server.get("/fonts/*", antfarmFontHandler);
    try server.get("/*", antfarmSpaHandler);
}

fn antfarmIndexHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmFile(ctx, "index.html");
}

fn antfarmAssetHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmPrefixedFile(ctx, "/assets/", "assets/");
}

fn antfarmFontHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    return serveAntfarmPrefixedFile(ctx, "/fonts/", "fonts/");
}

fn antfarmSpaHandler(ctx: *httpx.Context) anyerror!httpx.Response {
    const path = ctx.request.uri.path;
    if (isAntfarmReservedPath(path)) {
        return ctx.status(404).text("not found");
    }
    if (!std.mem.startsWith(u8, path, "/")) {
        return ctx.status(400).text("invalid path");
    }

    const rel_path = path[1..];
    if (rel_path.len > 0 and std.mem.indexOfScalar(u8, rel_path, '.') != null) {
        return serveAntfarmFile(ctx, rel_path);
    }
    return serveAntfarmFile(ctx, "index.html");
}

fn serveAntfarmPrefixedFile(ctx: *httpx.Context, prefix: []const u8, rel_prefix: []const u8) anyerror!httpx.Response {
    const path = ctx.request.uri.path;
    if (!std.mem.startsWith(u8, path, prefix)) {
        return ctx.status(400).text("invalid path");
    }
    const suffix = path[prefix.len..];
    if (suffix.len == 0) {
        return ctx.status(404).text("not found");
    }

    var rel_buf: [1024]u8 = undefined;
    const rel_path = std.fmt.bufPrint(&rel_buf, "{s}{s}", .{ rel_prefix, suffix }) catch {
        return ctx.status(414).text("path too long");
    };
    return serveAntfarmFile(ctx, rel_path);
}

fn serveAntfarmFile(ctx: *httpx.Context, rel_path: []const u8) anyerror!httpx.Response {
    if (hasUnsafeStaticPath(rel_path)) {
        return ctx.status(400).text("invalid path");
    }

    if (try serveInstalledAntfarmFile(ctx, rel_path)) |resp| {
        return resp;
    }

    for (antfarm_asset_roots) |root| {
        var full_path_buf: [4096]u8 = undefined;
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ root, rel_path }) catch continue;
        if (try serveAntfarmPath(ctx, rel_path, full_path)) |resp| return resp;
    }

    return ctx.status(404).text("not found");
}

fn serveInstalledAntfarmFile(ctx: *httpx.Context, rel_path: []const u8) anyerror!?httpx.Response {
    const exe_dir = std.process.executableDirPathAlloc(ctx.io, ctx.allocator) catch return null;
    defer ctx.allocator.free(exe_dir);

    var full_path_buf: [4096]u8 = undefined;
    const full_path = std.fmt.bufPrint(
        &full_path_buf,
        "{s}/{s}/{s}",
        .{ exe_dir, antfarm_installed_asset_root, rel_path },
    ) catch return null;

    return try serveAntfarmPath(ctx, rel_path, full_path);
}

fn serveAntfarmPath(ctx: *httpx.Context, rel_path: []const u8, full_path: []const u8) anyerror!?httpx.Response {
    const body = std.Io.Dir.cwd().readFileAlloc(
        ctx.io,
        full_path,
        ctx.allocator,
        std.Io.Limit.limited(antfarm_max_file_bytes),
    ) catch |err| switch (err) {
        error.FileNotFound, error.NotDir, error.IsDir => return null,
        error.StreamTooLong => return try ctx.status(413).text("file too large"),
        else => return err,
    };

    var resp = httpx.Response.init(ctx.allocator, 200);
    errdefer resp.deinit();
    try resp.headers.set("Content-Type", antfarmContentType(rel_path));
    resp.body = body;
    resp.body_owned = true;
    return resp;
}

fn hasUnsafeStaticPath(path: []const u8) bool {
    if (path.len == 0) return true;
    if (path[0] == '/') return true;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return true;
    if (std.mem.indexOfScalar(u8, path, '\\') != null) return true;
    if (std.mem.indexOf(u8, path, "..") != null) return true;
    var i: usize = 0;
    while (i + 2 < path.len) : (i += 1) {
        if (path[i] != '%' or path[i + 1] != '2') continue;
        if (path[i + 2] == 'f' or path[i + 2] == 'F' or path[i + 2] == 'e' or path[i + 2] == 'E') return true;
    }
    return false;
}

fn isAntfarmReservedPath(path: []const u8) bool {
    const reserved = [_][]const u8{
        "/api",
        "/db",
        "/ai",
        "/ml",
        "/antfly",
        "/metadata",
        "/admin",
        "/internal",
        "/mcp",
        "/a2a",
        "/.well-known",
        "/extensions",
        "/healthz",
        "/readyz",
        "/registry",
    };
    for (reserved) |prefix| {
        if (std.mem.eql(u8, path, prefix)) return true;
        if (path.len > prefix.len and std.mem.startsWith(u8, path, prefix) and path[prefix.len] == '/') return true;
    }
    return isVersionedApiPath(path);
}

fn isVersionedApiPath(path: []const u8) bool {
    if (path.len < 5 or path[0] != '/') return false;
    const first_separator = std.mem.indexOfScalarPos(u8, path, 1, '/') orelse return false;
    if (first_separator == 1 or first_separator + 2 >= path.len or path[first_separator + 1] != 'v') return false;

    var cursor = first_separator + 2;
    const digits_start = cursor;
    while (cursor < path.len and std.ascii.isDigit(path[cursor])) : (cursor += 1) {}
    if (cursor == digits_start) return false;
    return cursor == path.len or path[cursor] == '/';
}

fn antfarmContentType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js") or std.mem.endsWith(u8, path, ".mjs")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".ico")) return "image/x-icon";
    if (std.mem.endsWith(u8, path, ".webp")) return "image/webp";
    if (std.mem.endsWith(u8, path, ".woff")) return "font/woff";
    if (std.mem.endsWith(u8, path, ".woff2")) return "font/woff2";
    if (std.mem.endsWith(u8, path, ".ttf")) return "font/ttf";
    if (std.mem.endsWith(u8, path, ".wasm")) return "application/wasm";
    if (std.mem.endsWith(u8, path, ".map")) return "application/json";
    if (std.mem.endsWith(u8, path, ".txt")) return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

fn localReplicaRootReconcileHook(data_server: *antfly.data.runtime.DataServer) antfly.metadata_service.LocalReplicaRootReconcileHook {
    return .{
        .ptr = data_server,
        .vtable = &.{
            .run = runLocalReplicaRootReconcileHook,
        },
    };
}

fn localSchemaProgressProvider(data_server: *antfly.data.runtime.DataServer) LocalSchemaProgressProvider {
    return .{
        .ptr = data_server,
        .collect = collectLocalSchemaProgress,
    };
}

fn collectLocalSchemaProgress(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    tables: []const antfly.metadata.TableRecord,
    ranges: []const antfly.metadata.RangeRecord,
) !antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return try data_server.collectLocalSchemaProgressSnapshot(alloc, tables, ranges);
}

fn localReplicaRootReconcilePermitHook(data_server: *antfly.data.runtime.DataServer) antfly.metadata_service.LocalReplicaRootReconcilePermitHook {
    return .{
        .ptr = data_server,
        .vtable = &.{
            .should_reconcile = runLocalReplicaRootReconcilePermitHook,
        },
    };
}

fn runLocalReplicaRootReconcileHook(
    ptr: *anyopaque,
    request: antfly.metadata_service.LocalReplicaRootReconcileHook.Request,
) !antfly.metadata.table_provisioner.ProvisionSummary {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return try data_server.reconcileVisibleProvisionedReplicaStateFromSnapshot(
        request.metadata_group_id,
        request.group_ids,
        request.tables,
        request.ranges,
    );
}

fn runLocalReplicaRootReconcilePermitHook(ptr: *anyopaque) bool {
    const data_server: *antfly.data.runtime.DataServer = @ptrCast(@alignCast(ptr));
    return !data_server.shouldDeferProvisionedReplicaRootReconcile();
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn readFileAlloc(alloc: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize) ![]u8 {
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bytes));
}

fn writeFileAtomically(alloc: std.mem.Allocator, io: std.Io, path: []const u8, contents: []const u8) !void {
    // Catalog mutations are serialized by LocalStandaloneMetadata.mutex.
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-standalone-metadata", .{path});
    defer alloc.free(tmp_path);

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    try fs_paths.syncDirPortable(io, parent);
}

// ---------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------

fn validPreloadModelKind(value: []const u8) bool {
    return std.mem.eql(u8, value, "embedder") or
        std.mem.eql(u8, value, "reranker") or
        std.mem.eql(u8, value, "generator") or
        std.mem.eql(u8, value, "chunker") or
        std.mem.eql(u8, value, "classifier") or
        std.mem.eql(u8, value, "rewriter") or
        std.mem.eql(u8, value, "reader") or
        std.mem.eql(u8, value, "transcriber") or
        std.mem.eql(u8, value, "extractor");
}

fn parsePreloadModelFlag(value: []const u8) !inference_bridge.WarmModel {
    const spec = try preload_model_spec.parse(value);
    return .{
        .kind = inference_bridge.String.init(if (validPreloadModelKind(spec.kind)) spec.kind else return error.InvalidArguments),
        .name = inference_bridge.String.init(spec.name),
        .backend = inference_bridge.OptionalString.init(spec.backend),
    };
}

fn parseCli(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    errdefer cfg.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--experimental")) {
            cfg.experimental = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--config")) {
            cfg.config_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--id")) {
            cfg.local_node_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--host")) {
            cfg.bind_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--port")) {
            cfg.bind_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--health-port")) {
            cfg.health_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--health")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.health_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--health=")) {
            cfg.health_enabled = parseBoolFlag(arg["--health=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--control-tick-ms")) {
            cfg.control_tick_ms = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--auth")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.auth_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--auth=")) {
            cfg.auth_enabled = parseBoolFlag(arg["--auth=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-publisher-domain")) {
            cfg.ard_publisher_domain = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-base-url")) {
            cfg.ard_base_url = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-display-name")) {
            cfg.ard_display_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-public-catalog")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.ard_public_catalog_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--ard-public-catalog=")) {
            cfg.ard_public_catalog_enabled = parseBoolFlag(arg["--ard-public-catalog=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--models-dir")) {
            cfg.inference_models_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ml-dir")) {
            cfg.inference_ml_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-host-budget-mb")) {
            cfg.inference_host_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-backend-budget-mb")) {
            cfg.inference_backend_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-combined-budget-mb")) {
            cfg.inference_combined_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-kv-budget-mb")) {
            cfg.inference_kv_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--inference-scratch-budget-mb")) {
            cfg.inference_scratch_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--process-memory-budget-mb") or
            std.mem.eql(u8, arg, "--inference-process-memory-budget-mb"))
        {
            cfg.inference_process_memory_budget_mb = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--kernel-jit-mode")) {
            cfg.inference_kernel_jit_mode = std.meta.stringToEnum(
                antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode,
                args.next() orelse return error.InvalidArguments,
            ) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--preload-model")) {
            try cfg.inference_preload_models.append(alloc, try parsePreloadModelFlag(args.next() orelse return error.InvalidArguments));
            continue;
        }
        if (std.mem.eql(u8, arg, "--data-dir")) {
            cfg.data_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--storage-engine")) {
            cfg.storage_engine = parseStorageEngine(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--storage-engine=")) {
            cfg.storage_engine = parseStorageEngine(arg["--storage-engine=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--storage-path")) {
            cfg.storage_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--storage-path=")) {
            cfg.storage_path = arg["--storage-path=".len..];
            continue;
        }
        if (std.mem.eql(u8, arg, "--fsync")) {
            cfg.storage_fsync = parseBoolFlag(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--fsync=")) {
            cfg.storage_fsync = parseBoolFlag(arg["--fsync=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-root-dir")) {
            cfg.replica_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-catalog-path")) {
            cfg.replica_catalog_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--snapshot-root-dir")) {
            cfg.snapshot_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--extension-package-store")) {
            cfg.extension_package_store_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            try cfg.secret_store_paths.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-log")) {
            cfg.ha_primary_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-slots")) {
            cfg.ha_primary_slots = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-primary-node-id")) {
            cfg.ha_primary_node_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-seed-capture-root")) {
            cfg.ha_seed_capture_root = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-fence-wal")) {
            cfg.ha_fence_wal = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-former-primary-log")) {
            cfg.ha_former_primary_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--admin-token-env")) {
            cfg.admin_token_env = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-lag-lsn")) {
            cfg.ha_retention_max_lag_lsn = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-retained-bytes")) {
            cfg.ha_retention_max_retained_bytes = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-retention-max-retained-age-ns")) {
            cfg.ha_retention_max_retained_age_ns = try parsePositiveU64(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-mode")) {
            cfg.ha_sync_mode = try parseHASyncDurabilityMode(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-selection")) {
            cfg.ha_sync_selection = try parseHASyncStandbySelection(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-required")) {
            cfg.ha_sync_required = try parsePositiveUsize(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-standby")) {
            try cfg.ha_sync_standby_names.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-sync-failure")) {
            cfg.ha_sync_failure_policy = try parseHASyncFailurePolicy(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-log")) {
            cfg.ha_standby_log = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-progress")) {
            cfg.ha_standby_progress = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-node-id")) {
            cfg.ha_standby_node_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-upstream-url")) {
            cfg.ha_standby_upstream_url = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-standby-slot")) {
            cfg.ha_standby_slot = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-target-root")) {
            cfg.ha_startup_target_root = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-topology-id")) {
            cfg.ha_startup_topology_id = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-topology-generation")) {
            cfg.ha_startup_topology_generation = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-generation")) {
            cfg.ha_startup_generation = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-target-pvc-name")) {
            cfg.ha_startup_target_pvc_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-target-pvc-uid")) {
            cfg.ha_startup_target_pvc_uid = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-manifest-sha256")) {
            cfg.ha_startup_manifest_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-aggregate-sha256")) {
            cfg.ha_startup_aggregate_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-seed-receipt-sha256")) {
            cfg.ha_startup_seed_receipt_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-capture-receipt-sha256")) {
            cfg.ha_startup_capture_receipt_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-materialized-receipt-sha256")) {
            cfg.ha_startup_materialized_receipt_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-materialized-aggregate-sha256")) {
            cfg.ha_startup_materialized_aggregate_sha256 = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-target-local-node-id")) {
            cfg.ha_startup_target_local_node_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-startup-target-replica-id")) {
            cfg.ha_startup_target_replica_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-cluster-id")) {
            cfg.ha_cluster_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-shard-id")) {
            cfg.ha_shard_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-table-id")) {
            cfg.ha_table_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-timeline-id")) {
            cfg.ha_timeline_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ha-epoch")) {
            cfg.ha_epoch = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
}

fn parseStorageEngine(value: []const u8) ?antfly.common.config.StorageEngine {
    if (std.mem.eql(u8, value, "local")) return .local;
    if (std.mem.eql(u8, value, "lite")) return .lite;
    if (std.mem.eql(u8, value, "object")) return .object;
    return null;
}

fn resolveLocalBaseDir(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) ![]u8 {
    if (cli.data_dir) |path| return try normalizeResolvedPathAlloc(alloc, path);
    return try antfly.common.config.resolveLocalBaseDir(alloc, cfg);
}

fn resolvePaths(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedPaths {
    const local_base = try resolveLocalBaseDir(alloc, cli, cfg);
    defer alloc.free(local_base);
    const data_base = try std.fmt.allocPrint(alloc, "{s}/data", .{local_base});
    defer alloc.free(data_base);
    const metadata_base = try std.fmt.allocPrint(alloc, "{s}/metadata", .{local_base});
    defer alloc.free(metadata_base);

    const replica_root_dir = if (cli.replica_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/replicas", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_root_dir);
    const replica_catalog_path = if (cli.replica_catalog_path) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_catalog_path);
    const local_metadata_catalog_path = blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/local-metadata.json", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(local_metadata_catalog_path);
    const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{data_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(snapshot_root_dir);
    const extension_package_store_dir = try resolveExtensionPackageStoreDir(alloc, cli.extension_package_store_dir, local_base);
    errdefer alloc.free(extension_package_store_dir);
    const secret_store_path = if (cli.primarySecretStorePath()) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/secrets.json", .{local_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(secret_store_path);
    const auth_store_root_dir = blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/auth", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(auth_store_root_dir);

    return .{
        .replica_root_dir = replica_root_dir,
        .replica_catalog_path = replica_catalog_path,
        .local_metadata_catalog_path = local_metadata_catalog_path,
        .snapshot_root_dir = snapshot_root_dir,
        .extension_package_store_dir = extension_package_store_dir,
        .secret_store_path = secret_store_path,
        .auth_store_root_dir = auth_store_root_dir,
    };
}

fn initLayeredSecretStore(
    alloc: std.mem.Allocator,
    io: std.Io,
    raw_paths: []const []const u8,
) !antfly.common.secrets.FileStore {
    var normalized_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (normalized_paths.items) |path| alloc.free(path);
        normalized_paths.deinit(alloc);
    }
    for (raw_paths) |raw_path| {
        const normalized_path = try normalizeResolvedPathAlloc(alloc, raw_path);
        errdefer alloc.free(normalized_path);
        try normalized_paths.append(alloc, normalized_path);
    }
    return try antfly.common.secrets.FileStore.initLayeredWithIo(alloc, io, normalized_paths.items);
}

fn resolveExtensionPackageStoreDir(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
) ![]u8 {
    const env_var_z = try alloc.dupeZ(u8, antfly.extensions.wasmtime_runtime.package_store_env);
    defer alloc.free(env_var_z);
    return try resolveExtensionPackageStoreDirWithEnv(
        alloc,
        cli_path,
        local_base,
        platform.env.getenvSlice(env_var_z),
    );
}

fn resolveExtensionPackageStoreDirWithEnv(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
    env_path: ?[]const u8,
) ![]u8 {
    if (cli_path) |path| return try normalizeResolvedPathAlloc(alloc, path);
    if (env_path) |path| {
        if (std.mem.trim(u8, path, " \t\r\n").len > 0) {
            return try normalizeResolvedPathAlloc(alloc, path);
        }
    }

    const raw = try std.fmt.allocPrint(alloc, "{s}/extensions", .{local_base});
    defer alloc.free(raw);
    return try normalizeResolvedPathAlloc(alloc, raw);
}

fn normalizeResolvedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try alloc.dupe(u8, path);

    var probe = path;
    while (true) {
        const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(std.Options.debug_io, probe, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
        if (resolved_z) |resolved| {
            defer alloc.free(resolved);
            const resolved_prefix = resolved[0..resolved.len];
            if (probe.len == path.len) return try alloc.dupe(u8, resolved_prefix);

            const suffix_start: usize = if (probe.len == 1) 1 else probe.len + 1;
            const suffix = path[suffix_start..];
            return try std.fs.path.join(alloc, &.{ resolved_prefix, suffix });
        }

        const parent = std.fs.path.dirname(probe) orelse return try alloc.dupe(u8, path);
        if (parent.len == probe.len) return try alloc.dupe(u8, path);
        probe = parent;
    }
}

fn resolveMetadataRaftListener(
    cli: CliConfig,
    local_node_id: u64,
    cfg: ?*const antfly.common.config.Config,
) antfly.metadata.runtime.ListenerConfig {
    if (cfg) |loaded| {
        if (antfly.metadata.runtime.metadataClusterPeerUrl(loaded, local_node_id)) |url| {
            return antfly.metadata.runtime.parseHostPort(url) catch .{ .bind_host = cli.bind_host orelse "127.0.0.1", .bind_port = 0 };
        }
    }
    return antfly.metadata.runtime.resolveListener(cli.bind_host, null, cfg);
}

fn resolveMetadataApiListener(
    cfg: ?*const antfly.common.config.Config,
    local_node_id: u64,
    fallback_host: []const u8,
) antfly.metadata.runtime.ListenerConfig {
    if (cfg) |loaded| {
        if (antfly.metadata.runtime.metadataOrchestrationPeerUrl(loaded, local_node_id)) |url| {
            return antfly.metadata.runtime.parseHostPort(url) catch .{ .bind_host = fallback_host, .bind_port = 0 };
        }
    }
    return .{ .bind_host = fallback_host, .bind_port = 0 };
}

fn resolveMetadataClusterPeers(
    alloc: std.mem.Allocator,
    cfg: ?*const antfly.common.config.Config,
) ![]antfly.metadata.runtime.MetadataClusterPeer {
    if (cfg) |loaded| return try antfly.metadata.runtime.metadataClusterPeersFromConfig(alloc, loaded);
    return &.{};
}

fn resolvePublicListener(cli: CliConfig) antfly.metadata.runtime.ListenerConfig {
    return .{
        .bind_host = cli.bind_host orelse "127.0.0.1",
        .bind_port = cli.bind_port orelse default_public_port,
    };
}

fn haPrimaryRequested(cli: CliConfig) bool {
    return cli.ha_primary_log != null or
        cli.ha_primary_slots != null or
        cli.ha_primary_node_id != null;
}

fn haStandbyRequested(cli: CliConfig) bool {
    return cli.ha_standby_log != null or
        cli.ha_standby_progress != null or
        cli.ha_standby_node_id != null or
        cli.ha_standby_upstream_url != null or
        cli.ha_standby_slot != null;
}

fn haContinuousMutationGuardEnabled(cli: CliConfig) bool {
    // A standby can never acknowledge public state changes: its only legal
    // mutation source is the authenticated replication stream. A primary,
    // however, has a supported catalog-bootstrap phase before a table identity
    // exists. Its continuous WAL is table-scoped, so enabling the fail-closed
    // ingress guard before both identity components are configured would make
    // it impossible to create the table whose identity must be supplied on the
    // HA restart.
    if (haStandbyRequested(cli)) return true;
    return haPrimaryRequested(cli) and
        cli.ha_shard_id != null and
        cli.ha_table_id != null;
}

fn haRemoteApplyMutationsEnabled(policy: antfly.ha.primary.SyncPolicy) bool {
    return policy.mode == .remote_apply and
        policy.failure_policy == .block and
        policy.standby_names.len > 0;
}

fn haIdentityRequested(cli: CliConfig) bool {
    return cli.ha_cluster_id != null or
        cli.ha_shard_id != null or
        cli.ha_table_id != null or
        cli.ha_timeline_id != null or
        cli.ha_epoch != null;
}

fn haStartupGateRequested(cli: CliConfig) bool {
    return cli.ha_startup_target_root != null or
        cli.ha_startup_topology_id != null or
        cli.ha_startup_topology_generation != null or
        cli.ha_startup_generation != null or
        cli.ha_startup_target_pvc_name != null or
        cli.ha_startup_target_pvc_uid != null or
        cli.ha_startup_manifest_sha256 != null or
        cli.ha_startup_aggregate_sha256 != null or
        cli.ha_startup_seed_receipt_sha256 != null or
        cli.ha_startup_capture_receipt_sha256 != null or
        cli.ha_startup_materialized_receipt_sha256 != null or
        cli.ha_startup_materialized_aggregate_sha256 != null or
        cli.ha_startup_target_local_node_id != null or
        cli.ha_startup_target_replica_id != null;
}

fn haSyncPolicyRequested(cli: CliConfig) bool {
    return cli.ha_sync_mode != null or
        cli.ha_sync_selection != null or
        cli.ha_sync_required != null or
        cli.ha_sync_failure_policy != null or
        cli.ha_sync_standby_names.items.len > 0;
}

fn haRetentionPolicyRequested(cli: CliConfig) bool {
    return cli.ha_retention_max_lag_lsn != null or
        cli.ha_retention_max_retained_bytes != null or
        cli.ha_retention_max_retained_age_ns != null;
}

fn validateHARole(cli: CliConfig) !void {
    const primary_requested = haPrimaryRequested(cli);
    const standby_requested = haStandbyRequested(cli);
    if (primary_requested and standby_requested) return error.HAMultipleRolesConfigured;
    if (haIdentityRequested(cli) and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_fence_wal != null and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_former_primary_log != null and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (cli.ha_seed_capture_root != null and !primary_requested and !standby_requested) return error.HARoleMissing;
    if (haStartupGateRequested(cli) and !standby_requested) return error.HAStartupGateRequiresStandby;
    if (cli.ha_former_primary_log != null) {
        _ = try requireHAPath(cli.ha_former_primary_log, error.HAFormerPrimaryLogInvalid, error.HAFormerPrimaryLogInvalid);
    }
    if (cli.admin_token_env) |env_var| {
        switch (antfly.ha.validation.classifyHAString(env_var)) {
            .ok => {},
            .missing => return error.AdminTokenEnvMissing,
            .padded => return error.AdminTokenEnvInvalid,
        }
        if (!antfly.ha.validation.isEnvVarName(env_var)) return error.AdminTokenEnvInvalid;
    }
    if (primary_requested or standby_requested) {
        _ = try requireHAPath(cli.ha_fence_wal, error.HAFenceWalMissing, error.HAFenceWalInvalid);
    }
    if (primary_requested or standby_requested) try validateHAIdentity(cli);
    if (primary_requested) try validateHAPrimaryRoleComplete(cli);
    if (standby_requested) try validateHAStandbyRoleComplete(cli);
    if (haRetentionPolicyRequested(cli) and !primary_requested) return error.HARetentionPolicyRequiresPrimary;
    // A standby must preload the policy it will enforce if promotion opens a
    // primary in place. The mirror remains inactive while the standby owns the
    // runtime; it becomes authoritative only after the promoted-primary
    // handoff. Sync flags without any HA role are still invalid.
    if (haSyncPolicyRequested(cli) and !primary_requested and !standby_requested) return error.HASyncPolicyRequiresPrimary;
}

fn validateHAIdentity(cli: CliConfig) !void {
    if (cli.ha_cluster_id == null) return error.HAClusterIdMissing;
    if (cli.ha_timeline_id == null) return error.HATimelineIdMissing;
    if (cli.ha_epoch == null) return error.HAEpochMissing;
}

fn requireHAString(value: ?[]const u8, comptime missing_err: anyerror, comptime padded_err: anyerror) ![]const u8 {
    switch (antfly.ha.validation.classifyHAString(value)) {
        .ok => return value.?,
        .missing => return missing_err,
        .padded => return padded_err,
    }
}

fn requireHAPath(value: ?[]const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAString(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isAbsoluteNormalizedPath(raw)) return invalid_err;
    return raw;
}

fn requireHAPathWithinRoot(value: ?[]const u8, root: []const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAPath(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isAbsoluteNormalizedPathWithinRoot(raw, root)) return invalid_err;
    return raw;
}

fn requireHAIdentifier(value: ?[]const u8, comptime missing_err: anyerror, comptime invalid_err: anyerror) ![]const u8 {
    const raw = try requireHAString(value, missing_err, invalid_err);
    if (!antfly.ha.validation.isIdentifier(raw)) return invalid_err;
    return raw;
}

fn validateHAPrimaryRoleComplete(cli: CliConfig) !void {
    _ = try requireHAPath(cli.ha_primary_log, error.HAPrimaryLogMissing, error.HAPrimaryLogInvalid);
    _ = try requireHAPath(cli.ha_primary_slots, error.HAPrimarySlotsMissing, error.HAPrimarySlotsInvalid);
    _ = try requireHAIdentifier(cli.ha_primary_node_id, error.HAPrimaryNodeIdMissing, error.HAPrimaryNodeIdInvalid);
}

fn validateHAStandbyRoleComplete(cli: CliConfig) !void {
    _ = try requireHAPath(cli.ha_standby_log, error.HAStandbyLogMissing, error.HAStandbyLogInvalid);
    _ = try requireHAPath(cli.ha_standby_progress, error.HAStandbyProgressMissing, error.HAStandbyProgressInvalid);
    _ = try requireHAIdentifier(cli.ha_standby_node_id, error.HAStandbyNodeIdMissing, error.HAStandbyNodeIdInvalid);
}

fn validateHAPathsUnderRoot(cli: CliConfig, data_root: []const u8) !void {
    if (cli.ha_former_primary_log != null) {
        _ = try requireHAPathWithinRoot(cli.ha_former_primary_log, data_root, error.HAFormerPrimaryLogInvalid, error.HAFormerPrimaryLogInvalid);
    }
    if (haPrimaryRequested(cli) or haStandbyRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_fence_wal, data_root, error.HAFenceWalMissing, error.HAFenceWalInvalid);
        if (cli.ha_seed_capture_root != null) {
            _ = try requireHAPathWithinRoot(cli.ha_seed_capture_root, data_root, error.HASeedCaptureRootMissing, error.HASeedCaptureRootInvalid);
        }
    }
    if (haPrimaryRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_primary_log, data_root, error.HAPrimaryLogMissing, error.HAPrimaryLogInvalid);
        _ = try requireHAPathWithinRoot(cli.ha_primary_slots, data_root, error.HAPrimarySlotsMissing, error.HAPrimarySlotsInvalid);
    }
    if (haStandbyRequested(cli)) {
        _ = try requireHAPathWithinRoot(cli.ha_standby_log, data_root, error.HAStandbyLogMissing, error.HAStandbyLogInvalid);
        _ = try requireHAPathWithinRoot(cli.ha_standby_progress, data_root, error.HAStandbyProgressMissing, error.HAStandbyProgressInvalid);
        if (haStartupGateRequested(cli)) {
            _ = try requireHAPathWithinRoot(cli.ha_startup_target_root, data_root, error.HAStartupTargetRootMissing, error.HAStartupTargetRootInvalid);
        }
    }
}

fn haStartupExpectationFromCli(cli: CliConfig) !?antfly.ha.seed_activation.StartupExpectation {
    if (!haStartupGateRequested(cli)) return null;
    if (!haStandbyRequested(cli)) return error.HAStartupGateRequiresStandby;
    const binding = antfly.ha.seed_activation.ActivationBinding{
        .topology_id = try requireHAIdentifier(cli.ha_startup_topology_id, error.HAStartupTopologyIdMissing, error.HAStartupTopologyIdInvalid),
        .topology_generation = cli.ha_startup_topology_generation orelse return error.HAStartupTopologyGenerationMissing,
        .node_id = try requireHAIdentifier(cli.ha_standby_node_id, error.HAStandbyNodeIdMissing, error.HAStandbyNodeIdInvalid),
        .target_pvc_name = try requireHAIdentifier(cli.ha_startup_target_pvc_name, error.HAStartupTargetPVCNameMissing, error.HAStartupTargetPVCNameInvalid),
        .target_pvc_uid = try requireHAIdentifier(cli.ha_startup_target_pvc_uid, error.HAStartupTargetPVCUIDMissing, error.HAStartupTargetPVCUIDInvalid),
    };
    const capture_receipt_sha256 = (try optionalHAStartupDigest(cli.ha_startup_capture_receipt_sha256)) orelse
        return error.HAStartupCaptureReceiptSHA256Missing;
    const materialized_receipt_sha256 = (try optionalHAStartupDigest(cli.ha_startup_materialized_receipt_sha256)) orelse
        return error.HAStartupMaterializedReceiptSHA256Missing;
    const materialized_aggregate_sha256 = (try optionalHAStartupDigest(cli.ha_startup_materialized_aggregate_sha256)) orelse
        return error.HAStartupMaterializedAggregateSHA256Missing;
    const target_local_node_id = cli.ha_startup_target_local_node_id orelse
        return error.HAStartupTargetLocalNodeIDMissing;
    if (target_local_node_id == 0) return error.HAStartupTargetLocalNodeIDInvalid;
    if (target_local_node_id != (cli.local_node_id orelse 1)) return error.HAStartupTargetLocalNodeIDMismatch;
    const target_replica_id = cli.ha_startup_target_replica_id orelse
        return error.HAStartupTargetReplicaIDMissing;
    if (target_replica_id == 0) return error.HAStartupTargetReplicaIDInvalid;
    // Standalone owns one local replica whose identity is fixed at 1. Opening
    // a generation materialized for any other replica would silently point the
    // catalog at a topology this runtime cannot own.
    if (target_replica_id != 1) return error.HAStartupTargetReplicaIDMismatch;
    return .{
        .target_root = try requireHAPath(cli.ha_startup_target_root, error.HAStartupTargetRootMissing, error.HAStartupTargetRootInvalid),
        .expected = .{
            .generation = try requireHAIdentifier(cli.ha_startup_generation, error.HAStartupGenerationMissing, error.HAStartupGenerationInvalid),
            .slot_name = try requireHAIdentifier(cli.ha_standby_slot, error.HAStandbySlotMissing, error.HAStandbySlotInvalid),
            .identity = try haStandbyIdentity(cli),
            .binding = binding,
            .capture_receipt_sha256 = capture_receipt_sha256,
        },
        .binding = binding,
        .manifest_sha256 = try optionalHAStartupDigest(cli.ha_startup_manifest_sha256),
        .aggregate_sha256 = try optionalHAStartupDigest(cli.ha_startup_aggregate_sha256),
        .seed_receipt_sha256 = try optionalHAStartupDigest(cli.ha_startup_seed_receipt_sha256),
        .capture_receipt_sha256 = capture_receipt_sha256,
        .materialized_receipt_sha256 = materialized_receipt_sha256,
        .materialized_aggregate_sha256 = materialized_aggregate_sha256,
        .target_local_node_id = target_local_node_id,
        .target_replica_id = target_replica_id,
    };
}

fn optionalHAStartupDigest(value: ?[]const u8) !?[]const u8 {
    const digest = value orelse return null;
    if (digest.len != 64) return error.HAStartupDigestInvalid;
    for (digest) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.HAStartupDigestInvalid;
    }
    return digest;
}

fn haStandbyReplicationConfigFromCli(cli: CliConfig) !?antfly.data.runtime.HAStandbyReplicationConfig {
    return try haStandbyReplicationConfigFromCliWithBearerToken(cli, null);
}

fn haStandbyReplicationConfigFromCliWithBearerToken(
    cli: CliConfig,
    bearer_token: ?[]const u8,
) !?antfly.data.runtime.HAStandbyReplicationConfig {
    if (cli.ha_standby_upstream_url == null and cli.ha_standby_slot == null) return null;
    const upstream = try requireHAString(cli.ha_standby_upstream_url, error.HAStandbyUpstreamUrlMissing, error.HAStandbyUpstreamUrlInvalid);
    const slot = try requireHAIdentifier(cli.ha_standby_slot, error.HAStandbySlotMissing, error.HAStandbySlotInvalid);
    const parsed = antfly.ha.validation.parseURLNoHiddenWhitespace(upstream) catch return error.HAStandbyUpstreamUrlInvalid;
    if (!isHAReplicationUpstreamScheme(parsed)) return error.HAStandbyUpstreamUrlInvalid;
    if (parsed.host == null) return error.HAStandbyUpstreamUrlInvalid;
    return .{
        .upstream_base_uri = upstream,
        .slot_name = slot,
        .bearer_token = bearer_token,
        .standby_log_path = cli.ha_standby_log,
        .standby_progress_path = cli.ha_standby_progress,
    };
}

fn isHAReplicationUpstreamScheme(parsed: std.Uri) bool {
    return std.mem.eql(u8, parsed.scheme, "http") or std.mem.eql(u8, parsed.scheme, "https");
}

const OwnedHASyncPolicy = struct {
    policy: antfly.ha.primary.SyncPolicy = .{},
    standby_names: []const []const u8 = &.{},

    fn deinit(self: *OwnedHASyncPolicy, alloc: std.mem.Allocator) void {
        if (self.standby_names.len > 0) alloc.free(self.standby_names);
        self.* = undefined;
    }
};

fn haSyncPolicyFromCli(alloc: std.mem.Allocator, cli: CliConfig) !OwnedHASyncPolicy {
    if (!haSyncPolicyRequested(cli)) return .{};
    if (!haPrimaryRequested(cli) and !haStandbyRequested(cli)) return error.HASyncPolicyRequiresPrimary;

    const names = try alloc.alloc([]const u8, cli.ha_sync_standby_names.items.len);
    errdefer alloc.free(names);
    @memcpy(names, cli.ha_sync_standby_names.items);
    const selection = cli.ha_sync_selection orelse .any;
    if (selection == .all and cli.ha_sync_required != null) return error.InvalidHASyncPolicy;

    const policy = antfly.ha.primary.SyncPolicy{
        .mode = cli.ha_sync_mode orelse .remote_write,
        .selection = selection,
        .required = if (selection == .all) names.len else cli.ha_sync_required orelse 1,
        .standby_names = names,
        .failure_policy = cli.ha_sync_failure_policy orelse .block,
    };
    try validateHASyncPolicy(policy);

    return .{
        .policy = policy,
        .standby_names = names,
    };
}

fn haRetentionPolicyFromCli(cli: CliConfig) !antfly.ha.slot_store.RetentionPolicy {
    if (!haRetentionPolicyRequested(cli)) return .{};
    if (!haPrimaryRequested(cli)) return error.HARetentionPolicyRequiresPrimary;
    return .{
        .max_lag_lsn = cli.ha_retention_max_lag_lsn orelse 0,
        .max_retained_bytes = cli.ha_retention_max_retained_bytes orelse 0,
        .max_retained_age_ns = cli.ha_retention_max_retained_age_ns orelse 0,
    };
}

fn validateHASyncPolicy(policy: antfly.ha.primary.SyncPolicy) !void {
    if (policy.required == 0) return error.InvalidHASyncPolicy;
    if (policy.mode == .async) return;
    if (policy.standby_names.len == 0) return error.InvalidHASyncPolicy;
    if (policy.selection != .all and policy.required > policy.standby_names.len) {
        return error.InvalidHASyncPolicy;
    }
}

fn haPrimaryIdentity(cli: CliConfig) !antfly.ha.primary.Identity {
    return .{
        .cluster_id = cli.ha_cluster_id orelse return error.HAClusterIdMissing,
        .shard_id = cli.ha_shard_id orelse 0,
        .table_id = cli.ha_table_id orelse 0,
        .timeline_id = cli.ha_timeline_id orelse return error.HATimelineIdMissing,
        .epoch = cli.ha_epoch orelse return error.HAEpochMissing,
    };
}

fn openHAPrimaryFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.primary.Primary {
    if (!haPrimaryRequested(cli)) return null;
    const log_path = cli.ha_primary_log orelse return error.HAPrimaryLogMissing;
    const slots_path = cli.ha_primary_slots orelse return error.HAPrimarySlotsMissing;
    if (cli.ha_primary_node_id == null) return error.HAPrimaryNodeIdMissing;

    try ensureParent(io, log_path);
    try ensureParent(io, slots_path);

    const log_z = try alloc.dupeZ(u8, log_path);
    defer alloc.free(log_z);
    const slots_z = try alloc.dupeZ(u8, slots_path);
    defer alloc.free(slots_z);

    return try antfly.ha.primary.Primary.open(alloc, log_z.ptr, slots_z.ptr, try haPrimaryIdentity(cli), .{});
}

fn haStandbyIdentity(cli: CliConfig) !antfly.ha.standby.Identity {
    return .{
        .cluster_id = cli.ha_cluster_id orelse return error.HAClusterIdMissing,
        .shard_id = cli.ha_shard_id orelse 0,
        .table_id = cli.ha_table_id orelse 0,
        .timeline_id = cli.ha_timeline_id orelse return error.HATimelineIdMissing,
        .epoch = cli.ha_epoch orelse return error.HAEpochMissing,
    };
}

fn openHAStandbyFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.standby.Standby {
    if (!haStandbyRequested(cli)) return null;
    const log_path = cli.ha_standby_log orelse return error.HAStandbyLogMissing;
    const progress_path = cli.ha_standby_progress orelse return error.HAStandbyProgressMissing;
    if (cli.ha_standby_node_id == null) return error.HAStandbyNodeIdMissing;

    try ensureParent(io, log_path);
    try ensureParent(io, progress_path);

    const log_z = try alloc.dupeZ(u8, log_path);
    defer alloc.free(log_z);
    const progress_z = try alloc.dupeZ(u8, progress_path);
    defer alloc.free(progress_z);

    return try antfly.ha.standby.Standby.open(alloc, log_z.ptr, progress_z.ptr, try haStandbyIdentity(cli), .{});
}

fn openHAFenceStoreFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.fencing.Store {
    const fence_wal_path = cli.ha_fence_wal orelse return null;
    if (!haPrimaryRequested(cli) and !haStandbyRequested(cli)) return error.HARoleMissing;

    try ensureParent(io, fence_wal_path);

    const fence_wal_z = try alloc.dupeZ(u8, fence_wal_path);
    defer alloc.free(fence_wal_z);

    return try antfly.ha.fencing.Store.open(alloc, fence_wal_z.ptr, .{});
}

fn openHAFormerPrimaryLogFromCli(alloc: std.mem.Allocator, io: std.Io, cli: CliConfig) !?antfly.ha.replication_log.ReplicationLog {
    const former_primary_log_path = cli.ha_former_primary_log orelse return null;
    if (!haPrimaryRequested(cli) and !haStandbyRequested(cli)) return error.HARoleMissing;
    if (cli.ha_primary_log) |primary_log_path| {
        if (std.mem.eql(u8, former_primary_log_path, primary_log_path)) return null;
    }
    if (cli.ha_standby_log) |standby_log_path| {
        if (std.mem.eql(u8, former_primary_log_path, standby_log_path)) return null;
    }

    try ensureParent(io, former_primary_log_path);

    const former_primary_log_z = try alloc.dupeZ(u8, former_primary_log_path);
    defer alloc.free(former_primary_log_z);

    return try antfly.ha.replication_log.ReplicationLog.open(former_primary_log_z.ptr, .{});
}

fn resolveAdminBearerTokenFromCli(alloc: std.mem.Allocator, cli: CliConfig) !?[]u8 {
    const raw_env_var = cli.admin_token_env orelse return null;
    const env_var = std.mem.trim(u8, raw_env_var, " \t\r\n");
    if (env_var.len == 0) return error.AdminTokenEnvMissing;
    if (!antfly.ha.validation.isEnvVarName(env_var)) return error.AdminTokenEnvInvalid;

    const env_var_z = try alloc.dupeZ(u8, env_var);
    defer alloc.free(env_var_z);

    const raw_token_z = std.c.getenv(env_var_z.ptr) orelse return error.AdminTokenMissing;
    const token = std.mem.trim(u8, std.mem.span(raw_token_z), " \t\r\n");
    if (token.len == 0) return error.AdminTokenMissing;
    return try alloc.dupe(u8, token);
}

fn resolveHAPodUID(alloc: std.mem.Allocator) !?[]u8 {
    const raw_z = std.c.getenv("ANTFLY_POD_UID") orelse return null;
    const pod_uid = std.mem.trim(u8, std.mem.span(raw_z), " \t\r\n");
    if (!antfly.ha.validation.isIdentifier(pod_uid)) return error.HAPodUIDInvalid;
    return try alloc.dupe(u8, pod_uid);
}

fn ensureDirPath(io: std.Io, dir_path: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(io, dir_path);
}

fn ensureParent(io: std.Io, file_path: []const u8) !void {
    if (std.fs.path.dirname(file_path)) |parent| {
        var dir = std.Io.Dir.cwd().openDir(io, parent, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                try std.Io.Dir.cwd().createDirPath(io, parent);
                return;
            },
            else => return err,
        };
        dir.close(io);
    }
}

fn resolveAuthEnabled(cli: CliConfig, cfg: ?*const antfly.common.config.Config) bool {
    if (cli.auth_enabled) |value| return value;
    if (cfg) |loaded| return loaded.auth_enabled;
    return false;
}

fn resolveInferenceModelsDir(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ?[]const u8 {
    if (cli.inference_models_dir) |value| return value;
    if (cfg) |loaded| return loaded.inference.models_dir;
    return null;
}

fn resolveInferenceMaxConcurrentRequests(cfg: ?*const antfly.common.config.Config) u32 {
    return if (cfg) |config|
        config.admission.inference.max_concurrent_requests
    else
        antfly.common.config.default_inference_max_concurrent_requests;
}

fn resolveInferenceMlDir(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ?[]const u8 {
    if (cli.inference_ml_dir) |value| return value;
    if (cfg) |loaded| return loaded.inference.ml_dir;
    return null;
}

const InferenceBudgetOverrides = struct {
    host_limit_bytes: usize,
    backend_limit_bytes: usize,
    combined_limit_bytes: usize,
    kv_limit_bytes: usize,
    scratch_limit_bytes: usize,
};

fn resolveInferenceBudgetOverrides(cli: CliConfig) !InferenceBudgetOverrides {
    return .{
        .host_limit_bytes = try mibToBytes(cli.inference_host_budget_mb),
        .backend_limit_bytes = try mibToBytes(cli.inference_backend_budget_mb),
        .combined_limit_bytes = try mibToBytes(cli.inference_combined_budget_mb),
        .kv_limit_bytes = try mibToBytes(cli.inference_kv_budget_mb),
        .scratch_limit_bytes = try mibToBytes(cli.inference_scratch_budget_mb),
    };
}

fn inferenceBoundaryProvider(handle: *anyopaque) antfly.inference.managed_embedder.AntflyProvider {
    return .{
        .ptr = handle,
        .embed_dense_texts = inferenceProviderEmbedDenseTexts,
        .embed_dense_texts_with_context = inferenceProviderEmbedDenseTextsWithContext,
        .embed_sparse_texts = inferenceProviderEmbedSparseTexts,
        .embed_dense_parts = inferenceProviderEmbedDenseParts,
        .embed_dense_parts_with_context = inferenceProviderEmbedDensePartsWithContext,
        .rerank_texts = inferenceProviderRerankTexts,
        .generate_text = inferenceProviderGenerateText,
        .generate_messages = inferenceProviderGenerateMessages,
        .read_images = inferenceProviderReadImages,
        .transcribe_audio = inferenceProviderTranscribeAudio,
        .extract = inferenceProviderExtract,
        .list_models_json = inferenceProviderListModelsJson,
    };
}

fn invokeInferenceProvider(
    comptime Result: type,
    alloc: std.mem.Allocator,
    handle: *anyopaque,
    operation: inference_bridge.ProviderOperation,
    request: anytype,
    deadline_ns: ?u64,
) !Result {
    const request_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(request_json);
    var response_handle: ?*anyopaque = null;
    var response_json: inference_bridge.String = undefined;
    const effective_deadline_ns = deadline_ns orelse platform_time.monotonicNs() +| 5 * std.time.ns_per_min;
    const context = inference_bridge.ProviderInvokeContext{
        .abi_version = inference_bridge.abi_version,
        .handle = handle,
        .operation = @intFromEnum(operation),
        .request_json = inference_bridge.String.init(request_json),
        .deadline_ns = effective_deadline_ns,
        .has_deadline = 1,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceInvokeProvider(&context);
    } else {
        const status = (try linkedInferenceApi(
            inference_bridge.Capability.provider,
        )).invoke_provider(&context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    }
    const owned_response = response_handle orelse return error.InferenceRuntimeResponseMissing;
    defer if (comptime inline_inference_codegen)
        inference_host.linkedInferenceDestroyProviderResponse(owned_response)
    else
        linkedInferenceApiInfallible().destroy_provider_response(owned_response);
    return try std.json.parseFromSliceLeaky(Result, alloc, response_json.slice(), .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn linkedInferenceApi(required_capabilities: u64) !*const inference_bridge.FunctionTable {
    const table = inference_bridge.antfly_standalone_inference_get_function_table();
    if (!inference_bridge.validFunctionTable(table, required_capabilities))
        return error.UnsupportedVersion;
    return table;
}

fn linkedInferenceApiInfallible() *const inference_bridge.FunctionTable {
    return linkedInferenceApi(0) catch @panic("linked inference ABI changed after startup");
}

/// Dispatch the runtime-reserved local-inference connection through the same
/// embedded route handler used by the public inference API. This preserves the
/// destination's validation and admission semantics without opening a second
/// connection to our own listener.
fn invokeLocalInferenceConnection(context: *const inference_connection_abi.InvokeContext) callconv(.c) inference_connection_abi.Status {
    invokeLocalInferenceConnectionFallible(context) catch |err| {
        std.log.err("local inference connection failed err={}", .{err});
        return inference_connection_abi.statusFromError(err);
    };
    return .ok;
}

const LocalInferenceInvocationLifetime = struct {
    upstream: runtime_http_abi.CancellationView,
    deadline_ns: u64,

    fn expired(self: *const LocalInferenceInvocationLifetime) bool {
        return self.deadline_ns != 0 and platform_time.monotonicNs() >= self.deadline_ns;
    }

    fn check(self: *const LocalInferenceInvocationLifetime) !void {
        if (self.upstream.requested()) return error.Canceled;
        if (self.expired()) return error.Timeout;
    }

    fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const self: *const LocalInferenceInvocationLifetime = @ptrCast(@alignCast(raw orelse return 1));
        return @intFromBool(self.upstream.requested() or self.expired());
    }

    fn cancellation(self: *const LocalInferenceInvocationLifetime) runtime_http_abi.CancellationView {
        return .{ .context = self, .is_cancelled = isCancelled };
    }
};

test "standalone local inference lifetime distinguishes deadline from upstream cancellation" {
    const expired = LocalInferenceInvocationLifetime{
        .upstream = .{},
        .deadline_ns = platform_time.monotonicNs(),
    };
    try std.testing.expectError(error.Timeout, expired.check());
    try std.testing.expect(expired.cancellation().requested());

    const Cancelled = struct {
        fn requested(_: ?*const anyopaque) callconv(.c) u8 {
            return 1;
        }
    };
    const canceled = LocalInferenceInvocationLifetime{
        .upstream = .{ .context = &expired, .is_cancelled = Cancelled.requested },
        .deadline_ns = std.math.maxInt(u64),
    };
    try std.testing.expectError(error.Canceled, canceled.check());
}

fn ownedInferenceConnectionBytes(alloc: std.mem.Allocator, value: []const u8) !inference_connection_abi.OwnedBytes {
    const owned = try alloc.dupe(u8, value);
    return .{
        .ptr = if (owned.len == 0) null else owned.ptr,
        .len = owned.len,
    };
}

fn optionalOwnedInferenceConnectionBytes(
    alloc: std.mem.Allocator,
    value: ?[]const u8,
) !inference_connection_abi.OptionalOwnedBytes {
    const present = value orelse return .{};
    return .{
        .bytes = try ownedInferenceConnectionBytes(alloc, present),
        .present = 1,
    };
}

fn invokeLocalInferenceConnectionFallible(context: *const inference_connection_abi.InvokeContext) !void {
    if (!inference_connection_abi.validInvokeContext(context)) return error.UnsupportedVersion;
    const local_context: *LocalInferenceConnectionContext = @ptrCast(@alignCast(context.target_context));
    const alloc = context.allocator.asStd();
    const operation = context.operation.slice();
    const body = context.body.slice();
    var lifetime = LocalInferenceInvocationLifetime{
        .upstream = context.cancellation,
        .deadline_ns = context.deadline_ns,
    };
    try lifetime.check();
    const functions: ?*const inference_bridge.FunctionTable = if (comptime inline_inference_codegen)
        null
    else
        try linkedInferenceApi(inference_bridge.Capability.route_manifest);

    var entries_ptr: ?[*]const inference_bridge.RouteManifestEntry = null;
    var entries_len: usize = 0;
    const manifest_context = inference_bridge.RouteManifestContext{
        .abi_version = inference_bridge.abi_version,
        .handle = local_context.handle,
        .out_entries = &entries_ptr,
        .out_len = &entries_len,
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceRouteManifest(&manifest_context);
    } else {
        const status = functions.?.route_manifest(&manifest_context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    }

    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ inference_bridge.ai_api_prefix, operation });
    defer alloc.free(path);
    const interactive_generate = isInteractiveGeneratePath(path);
    if (interactive_generate)
        _ = antfly.db.enrichment_types.interactive_generate_inflight.fetchAdd(1, .monotonic);
    defer {
        if (interactive_generate)
            _ = antfly.db.enrichment_types.interactive_generate_inflight.fetchSub(1, .monotonic);
    }
    const entries = if (entries_ptr) |ptr| ptr[0..entries_len] else &.{};
    const route_handle = for (entries) |entry| {
        if (entry.method == .post and std.mem.eql(u8, entry.path.slice(), path))
            break entry.route_handle;
    } else return error.UnsupportedInferenceOperation;

    const headers = [_]runtime_http_abi.HeaderView{.{
        .name = runtime_http_abi.Bytes.init("Content-Type"),
        .value = runtime_http_abi.Bytes.init("application/json"),
    }};
    const request = runtime_http_abi.HttpRequestView{
        .method = .post,
        .path = runtime_http_abi.Bytes.init(path),
        .headers_ptr = &headers,
        .headers_len = headers.len,
        .body = runtime_http_abi.OptionalBytes.init(body),
        .content_type = runtime_http_abi.OptionalBytes.init("application/json"),
    };
    var response_handle: ?*anyopaque = null;
    var response_view: runtime_http_abi.HttpResponseView = undefined;
    const handle_context = inference_bridge.HttpHandleContext{
        .abi_version = inference_bridge.abi_version,
        .route_handle = route_handle,
        .request = &request,
        .cancellation = lifetime.cancellation(),
        .stream = context.stream,
        .out_response_handle = &response_handle,
        .out_response = &response_view,
    };
    if (comptime inline_inference_codegen) {
        inference_host.linkedInferenceHandleHttp(&handle_context) catch |err| {
            try lifetime.check();
            return err;
        };
    } else {
        const status = functions.?.handle_http(&handle_context);
        if (!status.isOk()) {
            try lifetime.check();
            return inference_bridge.errorFromStatus(status);
        }
    }
    try lifetime.check();
    const owned_response = response_handle orelse return error.InferenceRuntimeResponseMissing;
    defer if (comptime inline_inference_codegen)
        inference_host.linkedInferenceDestroyHttpResponse(owned_response)
    else
        functions.?.destroy_http_response(owned_response);

    var response: inference_connection_abi.InvokeResponse = .{
        .status = response_view.status,
        .body = try ownedInferenceConnectionBytes(alloc, response_view.body.slice()),
    };
    errdefer alloc.free(response.body.slice());
    var retry_after: ?[]const u8 = null;
    const response_headers = if (response_view.headers_ptr) |ptr| ptr[0..response_view.headers_len] else &.{};
    for (response_headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name.slice(), "Retry-After")) {
            retry_after = header.value.slice();
            break;
        }
    }
    response.retry_after = try optionalOwnedInferenceConnectionBytes(alloc, retry_after);
    errdefer if (response.retry_after.present != 0) alloc.free(response.retry_after.bytes.slice());
    response.content_type = try optionalOwnedInferenceConnectionBytes(alloc, response_view.content_type.slice());
    context.out_response.* = response;
}

fn tryAcquireEmbeddedInferenceRequest(handle: *anyopaque) bool {
    if (comptime inline_inference_codegen) {
        return inference_host.linkedInferenceTryAcquireRequest(handle);
    }
    return linkedInferenceApiInfallible().try_acquire_request(handle) != 0;
}

fn releaseEmbeddedInferenceRequest(handle: *anyopaque) void {
    if (comptime inline_inference_codegen) {
        inference_host.linkedInferenceReleaseRequest(handle);
        return;
    }
    linkedInferenceApiInfallible().release_request(handle);
}

fn embeddedInferenceRequestStats(handle: *anyopaque) antfly.common.request_admission.RequestAdmission.Stats {
    const stats = if (comptime inline_inference_codegen)
        inference_host.linkedInferenceRequestAdmissionStats(handle)
    else blk: {
        var result: inference_bridge.RequestAdmissionStats = undefined;
        linkedInferenceApiInfallible().request_admission_stats(handle, &result);
        break :blk result;
    };
    return .{
        .capacity = stats.capacity,
        .in_flight = stats.in_flight,
        .peak_in_flight = stats.peak_in_flight,
        .rejected_total = stats.rejected_total,
    };
}

fn inferenceProviderEmbedDenseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![][]f32 {
    return try invokeInferenceProvider([][]f32, alloc, handle, .embed_dense_texts, .{
        .model = model,
        .texts = texts,
    }, null);
}

fn inferenceProviderEmbedDenseTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try invokeInferenceProvider([][]f32, alloc, handle, .embed_dense_texts_with_context, .{
        .model = model,
        .texts = texts,
    }, context.deadline_ns);
}

fn inferenceProviderEmbedSparseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![]antfly.db.embedder.SparseEmbedding {
    return try invokeInferenceProvider([]antfly.db.embedder.SparseEmbedding, alloc, handle, .embed_sparse_texts, .{
        .model = model,
        .texts = texts,
    }, null);
}

fn inferenceProviderEmbedDenseParts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
) anyerror![][]f32 {
    return try invokeInferenceProvider([][]f32, alloc, handle, .embed_dense_parts, .{
        .model = model,
        .parts = parts,
    }, null);
}

fn inferenceProviderEmbedDensePartsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const antfly.template.ContentPart,
    context: antfly.inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try invokeInferenceProvider([][]f32, alloc, handle, .embed_dense_parts_with_context, .{
        .model = model,
        .parts = parts,
    }, context.deadline_ns);
}

fn inferenceProviderRerankTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
) anyerror![]f32 {
    return try invokeInferenceProvider([]f32, alloc, handle, .rerank_texts, .{
        .model = model,
        .query = query,
        .documents = documents,
    }, null);
}

fn inferenceProviderGenerateText(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_text, .{
        .model = model,
        .roles = roles,
        .contents = contents,
    }, null);
}

fn inferenceProviderGenerateMessages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const antfly.inference.ChatMessage,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_messages, .{
        .model = model,
        .messages = messages,
    }, null);
}

fn inferenceProviderReadImages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.readers.Request,
) anyerror![]antfly.readers.Result {
    return try invokeInferenceProvider([]antfly.readers.Result, alloc, handle, .read_images, .{
        .model = model,
        .request = request,
    }, null);
}

fn inferenceProviderTranscribeAudio(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.transcribing.Request,
) anyerror!antfly.transcribing.Response {
    return try invokeInferenceProvider(antfly.transcribing.Response, alloc, handle, .transcribe_audio, .{
        .model = model,
        .request = request,
    }, null);
}

fn inferenceProviderExtract(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: antfly.extracting.Request,
) anyerror!antfly.extracting.Response {
    const json = try invokeInferenceProvider([]u8, alloc, handle, .extract, .{
        .model = model,
        .request = request,
    }, null);
    return .{ .allocator = alloc, .json = json };
}

fn inferenceProviderListModelsJson(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .list_models_json, .{}, null);
}

fn inferenceResourceSlices(amounts: *const inference_bridge.AdmissionAmounts) ![3]antfly.resource_manager.SliceAmount {
    return .{
        .{
            .slice = .inference_model_residency,
            .bytes = @intCast(try std.math.add(usize, amounts.host_weight_bytes, amounts.backend_weight_bytes)),
        },
        .{
            .slice = .inference_kv_working_set,
            .bytes = @intCast(try std.math.add(usize, amounts.host_kv_bytes, amounts.backend_kv_bytes)),
        },
        .{
            .slice = .inference_scratch_working_set,
            .bytes = @intCast(try std.math.add(usize, amounts.host_scratch_bytes, amounts.backend_scratch_bytes)),
        },
    };
}

fn inferenceHostCharge(amounts: *const inference_bridge.AdmissionAmounts) !u64 {
    var total = try std.math.add(usize, amounts.host_weight_bytes, amounts.host_kv_bytes);
    total = try std.math.add(usize, total, amounts.host_scratch_bytes);
    if (builtin.os.tag == .macos) {
        total = try std.math.add(usize, total, amounts.backend_weight_bytes);
        total = try std.math.add(usize, total, amounts.backend_kv_bytes);
        total = try std.math.add(usize, total, amounts.backend_scratch_bytes);
    }
    return @intCast(total);
}

const InferenceResourceBudgetOwner = struct {
    alloc: std.mem.Allocator,
    manager: *antfly.resource_manager.ResourceManager,
    lease_pool_mutex: std.atomic.Mutex = .unlocked,
    free_leases: ?*InferenceAdmissionLease = null,
    active_leases: std.AutoHashMapUnmanaged(usize, *InferenceAdmissionLease) = .empty,
    next_lease_token: usize = 1,
    observer_mutex: std.atomic.Mutex = .unlocked,
    prompt_cache_observers: std.AutoHashMapUnmanaged(usize, u64) = .empty,
    tokenizer_mutex: std.atomic.Mutex = .unlocked,
    tokenizer_cache_observers: std.AutoHashMapUnmanaged(usize, u64) = .empty,
    outstanding_admission_leases: std.atomic.Value(usize) = .init(0),
    references: std.atomic.Value(usize) = .init(1),
    closing: std.atomic.Value(bool) = .init(false),
    lifetime_mutex: std.atomic.Mutex = .unlocked,

    fn deinit(self: *@This()) void {
        lockAtomic(&self.lifetime_mutex);
        defer self.lifetime_mutex.unlock();
        if (self.closing.swap(true, .acq_rel))
            @panic("inference resource owner closed twice");
        if (self.references.load(.acquire) != 1)
            @panic("inference resource owner closed with retained capability contexts");
        if (self.outstanding_admission_leases.load(.acquire) != 0 or
            self.active_leases.count() != 0)
            @panic("inference resource owner closed with active admission leases");
        self.active_leases.deinit(self.alloc);
        self.active_leases = .empty;
        var current = self.free_leases;
        while (current) |lease| {
            current = lease.next_free;
            self.alloc.destroy(lease);
        }
        self.free_leases = null;
        if (self.prompt_cache_observers.count() != 0)
            @panic("inference resource owner closed with prompt cache observers");
        self.prompt_cache_observers.deinit(self.alloc);
        self.prompt_cache_observers = .empty;
        if (self.tokenizer_cache_observers.count() != 0)
            @panic("inference resource owner closed with tokenizer cache observers");
        self.tokenizer_cache_observers.deinit(self.alloc);
        self.tokenizer_cache_observers = .empty;
    }

    fn acquireLease(self: *@This()) !*InferenceAdmissionLease {
        lockAtomic(&self.lease_pool_mutex);
        if (self.free_leases) |lease| {
            self.free_leases = lease.next_free;
            self.lease_pool_mutex.unlock();
            lease.next_free = null;
            return lease;
        }
        self.lease_pool_mutex.unlock();
        return try self.alloc.create(InferenceAdmissionLease);
    }

    fn recycleLease(self: *@This(), lease: *InferenceAdmissionLease) void {
        lockAtomic(&self.lease_pool_mutex);
        defer self.lease_pool_mutex.unlock();
        lease.next_free = self.free_leases;
        self.free_leases = lease;
    }

    fn registerLease(self: *@This(), lease: *InferenceAdmissionLease) !usize {
        lockAtomic(&self.lease_pool_mutex);
        defer self.lease_pool_mutex.unlock();

        // Tokens are monotonically advanced and never expose the recycled
        // pointer. On integer wrap, skip zero and any token still active.
        // This prevents a delayed duplicate release from targeting a newer
        // reservation that happens to reuse the same pool slot.
        var token = self.next_lease_token;
        while (token == 0 or self.active_leases.contains(token)) {
            token +%= 1;
        }
        self.next_lease_token = token +% 1;
        if (self.next_lease_token == 0) self.next_lease_token = 1;
        try self.active_leases.put(self.alloc, token, lease);
        return token;
    }

    fn retainLease(
        self: *@This(),
        token: usize,
        retained: []const antfly.resource_manager.SliceAmount,
        retained_host_charge_bytes: u64,
    ) !void {
        lockAtomic(&self.lease_pool_mutex);
        defer self.lease_pool_mutex.unlock();
        const lease = self.active_leases.get(token) orelse {
            self.manager.recordAccountingError();
            return error.InvalidArguments;
        };
        try lease.reservation.retain(retained, retained_host_charge_bytes);
    }

    fn takeLease(self: *@This(), token: usize) ?*InferenceAdmissionLease {
        lockAtomic(&self.lease_pool_mutex);
        defer self.lease_pool_mutex.unlock();
        const removed = self.active_leases.fetchRemove(token) orelse return null;
        return removed.value;
    }

    fn observePromptCache(
        self: *@This(),
        observer_id: usize,
        previous: u64,
        next: u64,
    ) bool {
        return self.observeCacheUsage(
            &self.observer_mutex,
            &self.prompt_cache_observers,
            .inference_prompt_cache,
            observer_id,
            previous,
            next,
            false,
        );
    }

    fn observeTokenizerCache(
        self: *@This(),
        observer_id: usize,
        previous: u64,
        next: u64,
    ) bool {
        return self.observeCacheUsage(
            &self.tokenizer_mutex,
            &self.tokenizer_cache_observers,
            .inference_tokenizer_cache,
            observer_id,
            previous,
            next,
            true,
        );
    }

    fn observeCacheUsage(
        self: *@This(),
        mutex: *std.atomic.Mutex,
        observers: *std.AutoHashMapUnmanaged(usize, u64),
        slice: antfly.resource_manager.Slice,
        observer_id: usize,
        previous: u64,
        next: u64,
        enforce_growth_limits: bool,
    ) bool {
        lockAtomic(mutex);
        defer mutex.unlock();

        if (observer_id == 0) {
            self.manager.recordAccountingError();
            return false;
        }
        var inserted = false;
        const current = observers.getPtr(observer_id) orelse blk: {
            if (previous != 0) {
                self.manager.recordAccountingError();
                return false;
            }
            if (next == 0) return true;
            const entry = observers.getOrPut(
                self.alloc,
                observer_id,
            ) catch {
                self.manager.recordAccountingError();
                return false;
            };
            entry.value_ptr.* = 0;
            inserted = true;
            break :blk entry.value_ptr;
        };
        if (current.* != previous) {
            self.manager.recordAccountingError();
            return false;
        }
        const accepted = if (enforce_growth_limits)
            self.manager.tryAdjustUsageIdentity(slice, observer_id, current.*, next)
        else
            self.manager.tryObserveUsageIdentity(slice, observer_id, current.*, next);
        if (!accepted) {
            if (inserted) _ = observers.remove(observer_id);
            return false;
        }
        current.* = next;
        if (next == 0) _ = observers.remove(observer_id);
        return true;
    }
};

const InferenceAdmissionLease = struct {
    reservation: antfly.resource_manager.BatchReservation,
    next_free: ?*InferenceAdmissionLease = null,
};

fn retainInferenceResourceOwner(context: *anyopaque) callconv(.c) u8 {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    lockAtomic(&owner.lifetime_mutex);
    defer owner.lifetime_mutex.unlock();
    if (owner.closing.load(.acquire)) return 0;
    const current = owner.references.load(.acquire);
    if (current == 0 or current == std.math.maxInt(usize)) return 0;
    owner.references.store(current + 1, .release);
    return 1;
}

fn releaseInferenceResourceOwner(context: *anyopaque) callconv(.c) void {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    lockAtomic(&owner.lifetime_mutex);
    defer owner.lifetime_mutex.unlock();
    // The standalone runtime owns the base reference until node destruction
    // completes; capability consumers may never release that final edge.
    const current = owner.references.load(.acquire);
    if (current <= 1)
        @panic("inference resource capability released its owner's base reference");
    owner.references.store(current - 1, .release);
}

fn reserveInferenceResources(
    context: *anyopaque,
    amounts: *const inference_bridge.AdmissionAmounts,
    out_lease: *usize,
) callconv(.c) inference_bridge.Status {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    out_lease.* = 0;
    const slices = inferenceResourceSlices(amounts) catch |err| return inference_bridge.statusFromError(err);
    const host_charge = inferenceHostCharge(amounts) catch |err| return inference_bridge.statusFromError(err);
    const lease = owner.acquireLease() catch |err|
        return inference_bridge.statusFromError(err);
    lease.* = .{
        .reservation = owner.manager.reserveBatchClassifiedWithHostCharge(
            &slices,
            host_charge,
        ) catch |err| {
            owner.recycleLease(lease);
            return inference_bridge.statusFromError(err);
        },
    };
    const lease_token = owner.registerLease(lease) catch |err| {
        lease.reservation.release();
        owner.recycleLease(lease);
        return inference_bridge.statusFromError(err);
    };
    _ = owner.outstanding_admission_leases.fetchAdd(1, .acq_rel);
    out_lease.* = lease_token;
    return .ok;
}

fn retainInferenceResources(
    context: *anyopaque,
    lease_token: usize,
    retained: *const inference_bridge.AdmissionAmounts,
) callconv(.c) inference_bridge.Status {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    if (lease_token == 0) return inference_bridge.statusFromError(error.InvalidArguments);
    const slices = inferenceResourceSlices(retained) catch |err| return inference_bridge.statusFromError(err);
    const host_charge = inferenceHostCharge(retained) catch |err| return inference_bridge.statusFromError(err);
    owner.retainLease(lease_token, &slices, host_charge) catch |err|
        return inference_bridge.statusFromError(err);
    return .ok;
}

fn releaseInferenceResources(context: *anyopaque, lease_token: usize) callconv(.c) void {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    if (lease_token == 0) return;
    const lease = owner.takeLease(lease_token) orelse {
        owner.manager.recordAccountingError();
        return;
    };
    lease.reservation.release();
    owner.recycleLease(lease);
    _ = owner.outstanding_admission_leases.fetchSub(1, .acq_rel);
}

fn observeInferencePromptCache(
    context: *anyopaque,
    observer_id: usize,
    previous: u64,
    next: u64,
) callconv(.c) u8 {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    return @intFromBool(owner.observePromptCache(observer_id, previous, next));
}

fn observeInferenceTokenizerCache(
    context: *anyopaque,
    observer_id: usize,
    previous: u64,
    next: u64,
) callconv(.c) u8 {
    const owner: *InferenceResourceBudgetOwner = @ptrCast(@alignCast(context));
    return @intFromBool(owner.observeTokenizerCache(observer_id, previous, next));
}

fn mibToBytes(value: usize) !usize {
    return process_memory_budget.mibToBytes(value);
}

fn resolveProcessMemoryBudget(
    cli: CliConfig,
    env: *const std.process.Environ.Map,
) !process_memory_budget.EffectiveResolution {
    return process_memory_budget.resolveSystemDetailed(
        cli.inference_process_memory_budget_mb,
        env.get(process_memory_budget.canonical_env),
        env.get(process_memory_budget.inference_compat_env),
    );
}

fn storageMemoryLimitSource(
    source: process_memory_budget.EffectiveSource,
) antfly.public_api.MemoryLimitSource {
    return switch (source) {
        .explicit => .explicit,
        .cgroup_v2 => .cgroup_v2,
        .cgroup_v1 => .cgroup_v1,
        .host => .host,
        .unavailable => .unavailable,
    };
}

fn inferenceMemoryLimitProvenance(
    source: process_memory_budget.EffectiveSource,
) inference_bridge.ProcessMemoryLimitProvenance {
    return switch (source) {
        .explicit => .explicit,
        .cgroup_v2 => .cgroup_v2,
        .cgroup_v1 => .cgroup_v1,
        .host => .host,
        .unavailable => .unavailable,
    };
}

fn printUsage() void {
    std.debug.print(
        \\Usage: antfly standalone [options]
        \\
        \\Options:
        \\  --config <path>                       JSON common config file
        \\  --host <host>                         Public API host (default: 127.0.0.1)
        \\  --port <port>                         Public API port (default: 8080)
        \\  --auth <true|false>                   Enable authentication for public APIs (default: false)
        \\  --id <node-id>                        Local node id (default: 1)
        \\  --health <true|false>                 Enable health/metrics server (default: true)
        \\  --health-port <port>                  Dedicated health/metrics port on --host (default: 4200)
        \\  --experimental                        Enable experimental A2A protocol surfaces
        \\  --ard-base-url <url>                  Absolute public base URL for ARD catalog artifact links
        \\  --ard-publisher-domain <name>         ARD did:web publisher domain (default: antfly.local)
        \\  --ard-display-name <name>             ARD catalog host display name (default: Antfly)
        \\  --ard-public-catalog <bool>           Publish anonymous /.well-known ARD bootstrap when auth is enabled
        \\  --control-tick-ms <ms>                Control scheduling interval, 1-60000 (default: 100)
        \\  --models-dir <path>                   Embedded AI models directory (default: ~/.antfly/inference/models)
        \\  --ml-dir <path>                       Embedded Traditional ML directory (default: ~/.antfly/inference/ml)
        \\  --inference-host-budget-mb <n>        Embedded inference native generation host budget override
        \\  --inference-backend-budget-mb <n>     Embedded inference native generation backend budget override
        \\  --inference-combined-budget-mb <n>    Embedded inference native generation combined budget override
        \\  --process-memory-budget-mb <n>        Whole-process host-memory envelope (0: auto-detect)
        \\  --inference-kv-budget-mb <n>          Embedded inference native generation KV cache budget override
        \\  --inference-scratch-budget-mb <n>     Embedded inference native generation scratch budget override
        \\  --inference-process-memory-budget-mb <n> Compatibility alias for --process-memory-budget-mb
        \\  --kernel-jit-mode <off|shadow|on|required> Embedded inference runtime JIT mode override
        \\  --preload-model <kind:name|kind:backend:name> Preload and warm an embedded model before serving
        \\  --data-dir <path>                     Local Antfly data directory root
        \\  --storage-engine lite                 Use the single-file Lite engine
        \\  --storage-path <path.aflite>          Lite database path (required with Lite)
        \\  --fsync <true|false>                  Lite commit durability (default: true)
        \\  --replica-root-dir <path>             Replica root directory
        \\  --replica-catalog-path <path>         Replica catalog file path
        \\  --snapshot-root-dir <path>            Snapshot root directory
        \\  --extension-package-store <path>      Extension package store directory
        \\  --secret-store-path <path>            Antfly secrets.json file path; repeat for fallback layers
        \\  --ha-primary-log <path>               Enable HA primary WAL/admin API with this replication log path
        \\  --ha-primary-slots <path>             HA primary replication slot store path
        \\  --ha-primary-node-id <id>             HA primary node id for typed admin receipts
        \\  --ha-seed-capture-root <path>          Durable runtime-owned immutable seed generation root
        \\  --ha-fence-wal <path>                 Durable HA promotion fence WAL path
        \\  --ha-former-primary-log <path>        Durable HA log used by former-primary rewind admin workflows
        \\  --admin-token-env <name>              Require Authorization: Bearer token from this environment variable for admin and HA APIs
        \\  --ha-retention-max-lag-lsn <n>        HA primary marks slots reseed-required after this LSN retention lag
        \\  --ha-retention-max-retained-bytes <n> HA primary marks oldest slots reseed-required above this retained WAL byte cap
        \\  --ha-retention-max-retained-age-ns <n> HA primary marks oldest slots reseed-required above this retained WAL age cap
        \\  --ha-sync-mode <mode>                 HA primary sync mode: async, remote-write, remote-apply
        \\  --ha-sync-selection <selection>       HA sync standby selection: any, first, all
        \\  --ha-sync-required <n>                HA sync required standby acknowledgements
        \\  --ha-sync-standby <name>              HA sync standby name; repeat for multiple standbys
        \\  --ha-sync-failure <policy>            HA sync failure policy: block, fail-closed, degrade-to-async
        \\  --ha-standby-log <path>               Enable HA standby admin API with this received replication log path
        \\  --ha-standby-progress <path>          HA standby durable receive/apply progress WAL path
        \\  --ha-standby-node-id <id>             HA standby node id for typed admin receipts
        \\  --ha-standby-upstream-url <url>       Upstream primary URL for continuous standby pull/apply
        \\  --ha-standby-slot <name>              Upstream replication slot name for continuous standby pull/apply
        \\  --ha-startup-target-root <path>       Activated standby generation root; requires the complete startup evidence set
        \\  --ha-startup-topology-id <id>         Exact topology id bound into the activation receipt
        \\  --ha-startup-topology-generation <n>  Exact topology generation bound into the activation receipt
        \\  --ha-startup-generation <id>          Exact activated seed generation
        \\  --ha-startup-target-pvc-name <name>   Exact target PVC name bound into the activation receipt
        \\  --ha-startup-target-pvc-uid <uid>     Exact target PVC UID bound into the activation receipt
        \\  --ha-startup-capture-receipt-sha256 <sha256> Exact runtime capture authority digest
        \\  --ha-startup-materialized-receipt-sha256 <sha256> Exact materialized topology receipt digest
        \\  --ha-startup-materialized-aggregate-sha256 <sha256> Exact materialized file aggregate digest
        \\  --ha-startup-target-local-node-id <id> Exact local node id used to materialize the live generation
        \\  --ha-startup-target-replica-id <id>   Exact replica id used to materialize the live generation
        \\  --ha-cluster-id <id>                  HA replicated cluster id
        \\  --ha-shard-id <id>                    HA replicated shard id (default: 0)
        \\  --ha-table-id <id>                    HA replicated table id (default: 0)
        \\  --ha-timeline-id <id>                 HA primary timeline id
        \\  --ha-epoch <id>                       HA primary epoch
        \\  -h, --help                            Show this help
        \\
    , .{});
}

fn parseBoolFlag(raw: []const u8) ?bool {
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "false")) return false;
    return null;
}

fn parseHASyncDurabilityMode(raw: []const u8) !antfly.ha.primary.DurabilityMode {
    if (std.mem.eql(u8, raw, "async")) return .async;
    if (std.mem.eql(u8, raw, "remote_write") or std.mem.eql(u8, raw, "remote-write")) return .remote_write;
    if (std.mem.eql(u8, raw, "remote_apply") or std.mem.eql(u8, raw, "remote-apply")) return .remote_apply;
    return error.InvalidHASyncMode;
}

fn parseHASyncStandbySelection(raw: []const u8) !antfly.ha.primary.StandbySelection {
    if (std.mem.eql(u8, raw, "any")) return .any;
    if (std.mem.eql(u8, raw, "first")) return .first;
    if (std.mem.eql(u8, raw, "all")) return .all;
    return error.InvalidHASyncSelection;
}

fn parseHASyncFailurePolicy(raw: []const u8) !antfly.ha.primary.FailurePolicy {
    if (std.mem.eql(u8, raw, "block")) return .block;
    if (std.mem.eql(u8, raw, "fail_closed") or std.mem.eql(u8, raw, "fail-closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "degrade_to_async") or std.mem.eql(u8, raw, "degrade-to-async")) return .degrade_to_async;
    return error.InvalidHASyncFailurePolicy;
}

fn parsePositiveUsize(raw: []const u8) !usize {
    const value = std.fmt.parseInt(usize, raw, 10) catch return error.InvalidArguments;
    if (value == 0) return error.InvalidHASyncPolicy;
    return value;
}

fn parsePositiveU64(raw: []const u8) !u64 {
    const value = std.fmt.parseInt(u64, raw, 10) catch return error.InvalidArguments;
    if (value == 0) return error.InvalidHARetentionPolicy;
    return value;
}

const RecordingRouteMethod = enum {
    get,
    post,
    put,
    delete,
};

const RecordingRoute = struct {
    method: RecordingRouteMethod,
    path: []u8,
};

const RecordingServer = struct {
    allocator: std.mem.Allocator,
    routes: std.ArrayListUnmanaged(RecordingRoute) = .empty,

    fn deinit(self: *@This()) void {
        for (self.routes.items) |route| self.allocator.free(route.path);
        self.routes.deinit(self.allocator);
    }

    fn append(self: *@This(), method: RecordingRouteMethod, comptime path: []const u8) !void {
        try self.routes.append(self.allocator, .{
            .method = method,
            .path = try self.allocator.dupe(u8, path),
        });
    }

    pub fn get(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.get, path);
    }

    pub fn post(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.post, path);
    }

    pub fn put(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.put, path);
    }

    pub fn delete(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.delete, path);
    }

    fn hasRoute(self: *const @This(), method: RecordingRouteMethod, path: []const u8) bool {
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) return true;
        }
        return false;
    }
};

test "standalone runtime module compiles" {
    _ = run;
    _ = runFromIterator;
    try std.testing.expect(isInteractiveGeneratePath("/ai/v1/generate"));
    try std.testing.expect(isInteractiveGeneratePath("/ai/v1/chat/completions"));
    try std.testing.expect(isInteractiveGeneratePath("/ml/v1/generate/batch"));
    try std.testing.expect(!isInteractiveGeneratePath("/ai/v1/embed"));
    try std.testing.expect(!isInteractiveGeneratePath("/ai/v10/generate"));
}

test "HA Lease minimum grace contains poll request and scheduling margin" {
    const minimum_grace_ns = ha_lease_min_grace_ms * std.time.ns_per_ms;
    const request_timeout_ns = @as(u64, ha_lease_request_timeout_ms) * std.time.ns_per_ms;
    try std.testing.expect(ha_lease_poll_interval_ns + request_timeout_ns + ha_lease_timing_jitter_ns < minimum_grace_ns);
}

test "standalone Lite enforces one shard and one replica" {
    const lite = try deriveStandaloneTableRecord(.lite, "docs", .{});
    try std.testing.expectEqual(@as(u32, 1), lite.min_ranges);
    try std.testing.expectEqual(@as(u32, 1), lite.desired_replica_count);
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        deriveStandaloneTableRecord(.lite, "split", .{ .num_shards = 2 }),
    );

    const local = try deriveStandaloneTableRecord(.local, "local", .{ .num_shards = 2 });
    try std.testing.expectEqual(@as(u32, 2), local.min_ranges);
    try std.testing.expectEqual(@as(u32, 1), local.desired_replica_count);
}

test "standalone Lite adoption preserves deterministic embedded document identity" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/identity-adoption.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var embedded = try antfly.lite.connection.Connection.create(alloc, path, true);
        defer embedded.close();
        try embedded.db.batch(.{ .writes = &.{.{ .key = "doc:portable", .value = "{\"body\":\"portable\"}" }} });
    }

    var backend = try antfly.lite.backend.Handle.open(alloc, path, .{});
    defer backend.deinit();
    const target = antfly.lite.connection.embeddedRootIdentity();
    const default_table = try deriveStandaloneTableRecord(.lite, "default", .{});
    const default_range = antfly.public_api.tables.deriveInitialRange(default_table);
    try std.testing.expectEqual(default_table.table_id, target.table_id);
    try std.testing.expectEqual(default_range.group_id, target.shard_id);
    try std.testing.expectEqual(default_range.range_id, target.range_id);
    const namespace = try std.fmt.allocPrint(alloc, "group-{d}/table-db", .{target.shard_id});
    defer alloc.free(namespace);
    try backend.adoptEmbeddedRootAsNamespace(namespace);
    try verifyAdoptedLiteIdentity(alloc, &backend, namespace, target);
    // Retry after a crash boundary is a no-op.
    try verifyAdoptedLiteIdentity(alloc, &backend, namespace, target);

    var opts = antfly.db.OpenOptions{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .start_optional_runtimes = false,
        .identity_namespace = target,
    };
    const restored_namespace = try std.fmt.allocPrint(alloc, "/restored/root/{s}", .{namespace});
    defer alloc.free(restored_namespace);
    try backend.configureDbOpenOptionsForNamespace(&opts, restored_namespace);
    var adopted = try antfly.db.DB.open(alloc, namespace, opts);
    defer adopted.close();
    try std.testing.expect(adopted.core.identity_namespace.eql(target));
    const value = (try adopted.get(alloc, "doc:portable")) orelse return error.MissingAdoptedDocument;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"body\":\"portable\"}", value);
}

test "standalone validates effective Lite CLI and config settings" {
    const lite_cli = CliConfig{ .storage_engine = .lite, .storage_path = "data.aflite" };
    try validateEffectiveStandaloneStorage(lite_cli, .lite, "data.aflite", null);
    try std.testing.expectError(
        error.InvalidLiteStoragePath,
        validateEffectiveStandaloneStorage(lite_cli, .lite, "data.db", null),
    );
    try std.testing.expectError(
        error.InvalidArguments,
        validateEffectiveStandaloneStorage(.{ .storage_path = "unused.aflite" }, .local, null, null),
    );

    var distributed = try antfly.common.config.Config.parseFromSlice(std.testing.allocator,
        \\{"deployment_mode":"distributed","storage":{"engine":"local","local":{"base_dir":"data"}}}
    );
    defer distributed.deinit();
    try std.testing.expectError(
        error.LiteHorizontalShardingUnsupported,
        validateEffectiveStandaloneStorage(lite_cli, .lite, "data.aflite", &distributed),
    );
}

test "standalone runtime local generator accepts media url data uris" {
    const alloc = std.testing.allocator;
    const messages = [_]antfly.inference.ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "describe" },
            .{ .media = .{
                .url = "data:image/png;base64,AQI=",
                .mime_type = "image/png",
            } },
        } },
    }};

    const preflight = try inference_host.preflightLocalGenerateMessages(&messages);
    var converted = try inference_host.convertLocalGenerateMessages(alloc, &messages, preflight.decoded_media_bytes);
    defer converted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), converted.messages.len);
    const message = converted.messages[0];
    try std.testing.expectEqualStrings("describe", message.content);
    try std.testing.expectEqual(@as(usize, 1), message.image_bytes.?.len);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, message.image_bytes.?[0]);
    try std.testing.expectEqual(@as(usize, 2), message.content_parts.?.len);
    try std.testing.expectEqual(@as(usize, 0), message.content_parts.?[1].image);
}

test "standalone runtime local dense embed preserves borrowed binary media" {
    const raw = [_]u8{ 1, 2, 3 };
    const parts = [_]antfly.template.ContentPart{
        .{ .text = "caption" },
        .{ .media_url = "data:image/png;base64,AA==" },
        .{ .binary = .{ .mime_type = "image/png", .data = &raw } },
    };
    const direct = try inference_host.localAntflyDirectDenseParts(std.testing.allocator, &parts);
    defer std.testing.allocator.free(direct);

    try std.testing.expectEqual(@as(usize, 3), direct.len);
    try std.testing.expectEqualStrings("caption", direct[0].text);
    try std.testing.expectEqualStrings("data:image/png;base64,AA==", direct[1].image_url);
    try std.testing.expectEqualStrings("image/png", direct[2].media.mime_type);
    try std.testing.expectEqual(@intFromPtr(raw[0..].ptr), @intFromPtr(direct[2].media.data.ptr));
    try std.testing.expectEqualSlices(u8, &raw, direct[2].media.data);
}

test "standalone runtime local generator preflights mixed resident media exactly" {
    const messages = [_]antfly.inference.ChatMessage{.{
        .role = .user,
        .content = .{ .parts = &.{
            .{ .text = "listen" },
            .{ .media = .{
                .data = "AQID",
                .mime_type = "audio/wav",
            } },
            .{ .image_url = .{ .url = "data:image/png;base64,BAU=" } },
        } },
    }};

    const preflight = try inference_host.preflightLocalGenerateMessages(&messages);
    try std.testing.expectEqual(@as(usize, "listen".len), preflight.text_bytes);
    try std.testing.expectEqual(
        @as(usize, "AQID".len + "data:image/png;base64,BAU=".len),
        preflight.encoded_media_bytes,
    );
    try std.testing.expectEqual(@as(usize, 5), preflight.decoded_media_bytes);
    try std.testing.expectEqual(@as(usize, 2), preflight.media_count);
    try std.testing.expectEqual(@as(usize, 1), preflight.image_count);
    try std.testing.expect(preflight.has_audio);
}

test "standalone runtime local generator refuses decode allocation beyond preflight" {
    var no_storage: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&no_storage);
    var budget = inference_host.LocalGenerateDecodeBudget{ .remaining_bytes = 1 };

    try std.testing.expectError(
        error.RemoteContentTooLarge,
        inference_host.decodeLocalGenerateDataUri(
            fixed.allocator(),
            "data:image/png;base64,AQI=",
            null,
            &budget,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), budget.remaining_bytes);
}

test "standalone runtime leaves auth disabled unless config or cli enables it" {
    try std.testing.expect(!resolveAuthEnabled(.{}, null));
    try std.testing.expect(resolveAuthEnabled(.{ .auth_enabled = true }, null));
    try std.testing.expect(!resolveAuthEnabled(.{ .auth_enabled = false }, null));
}

test "standalone continuous HA mutation guard follows role lifecycle" {
    try std.testing.expect(!haContinuousMutationGuardEnabled(.{}));
    try std.testing.expect(!haContinuousMutationGuardEnabled(.{ .ha_primary_log = "/ha/primary.wal" }));
    try std.testing.expect(!haContinuousMutationGuardEnabled(.{
        .ha_primary_log = "/ha/primary.wal",
        .ha_shard_id = 10,
    }));
    try std.testing.expect(haContinuousMutationGuardEnabled(.{
        .ha_primary_log = "/ha/primary.wal",
        .ha_shard_id = 10,
        .ha_table_id = 20,
    }));
    try std.testing.expect(haContinuousMutationGuardEnabled(.{ .ha_standby_log = "/ha/standby.wal" }));
    try std.testing.expect(!haRemoteApplyMutationsEnabled(.{}));
    try std.testing.expect(!haRemoteApplyMutationsEnabled(.{
        .mode = .remote_write,
        .failure_policy = .block,
        .standby_names = &.{"standby-a"},
    }));
    try std.testing.expect(haRemoteApplyMutationsEnabled(.{
        .mode = .remote_apply,
        .failure_policy = .block,
        .standby_names = &.{"standby-a"},
    }));
}

test "standalone runtime parses experimental flag" {
    const argv = [_][*:0]const u8{"--experimental"};
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(std.testing.allocator, &iter);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expect(parsed.experimental);
}

test "standalone inference middleware reuses public API authentication" {
    const alloc = std.testing.allocator;
    const Harness = struct {
        fn next(_: *httpx.Next, ctx: *httpx.Context) anyerror!httpx.Response {
            return ctx.status(204).text("next");
        }

        fn expect(
            middleware: httpx.Middleware,
            path: []const u8,
            authorization: ?[]const u8,
            expected_status: u16,
        ) !void {
            return expectMethod(middleware, .GET, path, authorization, expected_status);
        }

        fn expectMethod(
            middleware: httpx.Middleware,
            method: httpx.Method,
            path: []const u8,
            authorization: ?[]const u8,
            expected_status: u16,
        ) !void {
            var request = try httpx.Request.init(std.testing.allocator, method, path);
            defer request.deinit();
            if (authorization) |value| try request.setHeader("authorization", value);

            var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
            defer ctx.deinit();
            var next_handler = httpx.Next{ ._call = next };
            var response = try middleware.invoke(&ctx, &next_handler);
            defer response.deinit();

            try std.testing.expectEqual(expected_status, response.status.code);
            if (expected_status == 401) {
                try std.testing.expectEqualStrings(
                    "{\"error\":\"unauthorized\",\"message\":\"valid Basic, Bearer, or ApiKey credentials are required\",\"retryable\":false}",
                    response.body.?,
                );
                try std.testing.expectEqualStrings(
                    "Basic realm=\"antfly\", Bearer realm=\"antfly\", ApiKey realm=\"antfly\"",
                    response.headers.get("WWW-Authenticate").?,
                );
            } else if (expected_status == 403) {
                const required = if (method == .GET or method == .HEAD or method == .OPTIONS) "read" else "write";
                const expected = try std.fmt.allocPrint(
                    std.testing.allocator,
                    "{{\"error\":\"forbidden\",\"message\":\"inference {s} permission is required\",\"retryable\":false}}",
                    .{required},
                );
                defer std.testing.allocator.free(expected);
                try std.testing.expectEqualStrings(
                    expected,
                    response.body.?,
                );
            } else if (expected_status == 503) {
                try std.testing.expectEqualStrings(
                    "{\"error\":\"not_ready\",\"message\":\"inference authentication is not ready\",\"retryable\":true}",
                    response.body.?,
                );
                try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
            }
        }
    };

    var route_context = StandaloneHttpContext{ .api_server = null };
    try Harness.expect(inferenceAuthMiddleware(&route_context), "/ai/v1/models", null, 503);

    var store = antfly.usermgr.MemoryStore.init(alloc);
    defer store.deinit();
    var policy_store = antfly.casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var manager = try antfly.usermgr.UserManager.init(
        alloc,
        store.iface(),
        try antfly.usermgr.initDefaultEnforcer(alloc, policy_store.iface()),
    );
    defer manager.deinit();
    var user = try manager.createUser("admin", "admin", &.{});
    defer user.deinit(alloc);

    var api_server = antfly.public_api.http_server.ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &manager,
    }, .{ .ptr = undefined, .vtable = undefined }, null, null);
    defer api_server.deinit();

    route_context.api_server = &api_server;
    const middleware = inferenceAuthMiddleware(&route_context);
    var table_read = try antfly.usermgr.Permission.initOwned(alloc, .table, "documents", .read);
    defer table_read.deinit(alloc);
    try manager.addPermissionToUser("admin", table_read);
    for ([_][]const u8{ "/ai/v1/models", "/ml/v1/metrics" }) |path| {
        try Harness.expect(middleware, path, null, 401);
        try Harness.expect(middleware, path, "Basic YWRtaW46d3Jvbmc=", 401);
        try Harness.expect(middleware, path, "Basic YWRtaW46YWRtaW4=", 403);
    }

    var inference_read = try antfly.usermgr.Permission.initOwned(alloc, .inference, "*", .read);
    defer inference_read.deinit(alloc);
    try manager.addPermissionToUser("admin", inference_read);
    for ([_][]const u8{ "/ai/v1/models", "/ml/v1/metrics" }) |path| {
        try Harness.expect(middleware, path, "Basic YWRtaW46YWRtaW4=", 204);
    }
    try Harness.expectMethod(middleware, .POST, "/ai/v1/generate", "Basic YWRtaW46YWRtaW4=", 403);

    var inference_write = try antfly.usermgr.Permission.initOwned(alloc, .inference, "*", .write);
    defer inference_write.deinit(alloc);
    try manager.addPermissionToUser("admin", inference_write);
    try Harness.expectMethod(middleware, .POST, "/ai/v1/generate", "Basic YWRtaW46YWRtaW4=", 204);

    var global_read = try antfly.usermgr.Permission.initOwned(alloc, .@"*", "*", .read);
    defer global_read.deinit(alloc);
    try std.testing.expect(antfly.public_api.http_server.permissionsAllow(&.{global_read}, .inference, "*", .read));

    for ([_][]const u8{ "/ai/v10/models", "/ml/v1evil/metrics", "/healthz", "/auth/v1/login" }) |path| {
        try Harness.expect(middleware, path, null, 204);
    }

    api_server.cfg.user_manager = null;
    try Harness.expect(middleware, "/ai/v1/models", null, 503);

    api_server.cfg.auth_enabled = false;
    api_server.cfg.user_manager = &manager;
    try Harness.expect(middleware, "/ai/v1/models", null, 204);

    api_server.cfg.user_manager = null;
    api_server.cfg.trusted_principal_secret = "test-secret";
    try Harness.expect(middleware, "/ai/v1/models", null, 401);
}

test "standalone CORS middleware enforces dynamic configuration" {
    const Harness = struct {
        fn next(_: *httpx.Next, ctx: *httpx.Context) anyerror!httpx.Response {
            return ctx.status(209).text("next");
        }

        fn execute(
            config: *const antfly.common.config.Config.CorsConfig,
            method: httpx.Method,
            origin: ?[]const u8,
            requested_method: ?[]const u8,
            requested_headers: ?[]const u8,
        ) !httpx.Response {
            var request = try httpx.Request.init(std.testing.allocator, method, "/ai/v1/models");
            defer request.deinit();
            if (origin) |value| try request.setHeader("origin", value);
            if (requested_method) |value| try request.setHeader("access-control-request-method", value);
            if (requested_headers) |value| try request.setHeader("access-control-request-headers", value);

            var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
            defer ctx.deinit();
            var next_handler = httpx.Next{ ._call = next };
            var route_context = StandaloneHttpContext{ .api_server = null, .cors_config = config };
            return corsMiddleware(&route_context).invoke(&ctx, &next_handler);
        }
    };

    var defaults: antfly.common.config.Config.CorsConfig = .{};
    try validateCorsConfig(&defaults);
    {
        var response = try Harness.execute(&defaults, .GET, "https://any.example", null, null);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 209), response.status.code);
        try std.testing.expectEqualStrings("*", response.headers.get("Access-Control-Allow-Origin").?);
        try std.testing.expectEqualStrings(
            "X-Request-ID, Retry-After, Deprecation, X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset",
            response.headers.get("Access-Control-Expose-Headers").?,
        );
    }
    {
        var response = try Harness.execute(
            &defaults,
            .OPTIONS,
            "https://any.example",
            "POST",
            "content-type, AUTHORIZATION",
        );
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 204), response.status.code);
        try std.testing.expectEqualStrings("GET, POST, PUT, DELETE, OPTIONS, PATCH", response.headers.get("Access-Control-Allow-Methods").?);
        try std.testing.expectEqualStrings("Content-Type, Authorization, X-Requested-With, Accept, Origin", response.headers.get("Access-Control-Allow-Headers").?);
        try std.testing.expectEqualStrings("3600", response.headers.get("Access-Control-Max-Age").?);
    }
    {
        var response = try Harness.execute(&defaults, .OPTIONS, "https://any.example", "BREW", null);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 403), response.status.code);
        try std.testing.expect(response.headers.get("Access-Control-Allow-Origin") == null);
    }
    {
        var response = try Harness.execute(&defaults, .OPTIONS, null, "POST", null);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 209), response.status.code);
    }

    var exact_origin = "https://allowed.example".*;
    var post_method = "POST".*;
    var allowed_header = "X-Token".*;
    var exposed_header = "X-Request-Id".*;
    var exact_origins = [_][]u8{exact_origin[0..]};
    var post_methods = [_][]u8{post_method[0..]};
    var allowed_headers = [_][]u8{allowed_header[0..]};
    var exposed_headers = [_][]u8{exposed_header[0..]};
    var exact = antfly.common.config.Config.CorsConfig{
        .allowed_origins = &exact_origins,
        .allowed_methods = &post_methods,
        .allowed_headers = &allowed_headers,
        .exposed_headers = &exposed_headers,
        .allow_credentials = true,
        .max_age = 7,
    };
    try validateCorsConfig(&exact);
    {
        var response = try Harness.execute(&exact, .POST, exact_origin[0..], null, null);
        defer response.deinit();
        try std.testing.expectEqualStrings(exact_origin[0..], response.headers.get("Access-Control-Allow-Origin").?);
        try std.testing.expectEqualStrings("true", response.headers.get("Access-Control-Allow-Credentials").?);
        try std.testing.expectEqualStrings("X-Request-Id", response.headers.get("Access-Control-Expose-Headers").?);
    }
    {
        var response = try Harness.execute(&exact, .POST, "https://denied.example", null, null);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 209), response.status.code);
        try std.testing.expect(response.headers.get("Access-Control-Allow-Origin") == null);
        try std.testing.expectEqualStrings("Origin", response.headers.get("Vary").?);
    }
    {
        var response = try Harness.execute(&exact, .GET, exact_origin[0..], null, null);
        defer response.deinit();
        try std.testing.expect(response.headers.get("Access-Control-Allow-Origin") == null);
        try std.testing.expectEqualStrings("Origin", response.headers.get("Vary").?);
    }
    {
        var response = try Harness.execute(&exact, .OPTIONS, exact_origin[0..], "POST", "x-token");
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 204), response.status.code);
        try std.testing.expectEqualStrings("POST", response.headers.get("Access-Control-Allow-Methods").?);
        try std.testing.expectEqualStrings("X-Token", response.headers.get("Access-Control-Allow-Headers").?);
        try std.testing.expectEqualStrings("7", response.headers.get("Access-Control-Max-Age").?);
    }

    var wildcard = "*".*;
    var wildcard_origins = [_][]u8{wildcard[0..]};
    var wildcard_credentials = antfly.common.config.Config.CorsConfig{
        .allowed_origins = &wildcard_origins,
        .allow_credentials = true,
    };
    try std.testing.expectError(error.CorsCredentialsWithWildcardOrigin, validateCorsConfig(&wildcard_credentials));
    var default_wildcard_credentials = antfly.common.config.Config.CorsConfig{ .allow_credentials = true };
    try std.testing.expectError(error.CorsCredentialsWithWildcardOrigin, validateCorsConfig(&default_wildcard_credentials));
    var opaque_origin = "null".*;
    var opaque_origins = [_][]u8{opaque_origin[0..]};
    var opaque_credentials = antfly.common.config.Config.CorsConfig{
        .allowed_origins = &opaque_origins,
        .allow_credentials = true,
    };
    try std.testing.expectError(error.CorsCredentialsWithOpaqueOrigin, validateCorsConfig(&opaque_credentials));

    var wildcard_header = "*".*;
    var wildcard_headers = [_][]u8{wildcard_header[0..]};
    var credentialed_any_header = antfly.common.config.Config.CorsConfig{
        .allowed_origins = &exact_origins,
        .allowed_methods = &post_methods,
        .allowed_headers = &wildcard_headers,
        .allow_credentials = true,
    };
    try validateCorsConfig(&credentialed_any_header);
    {
        var response = try Harness.execute(
            &credentialed_any_header,
            .OPTIONS,
            exact_origin[0..],
            "POST",
            "X-Trace-Id, X-Client-Version",
        );
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 204), response.status.code);
        try std.testing.expectEqualStrings(
            "X-Trace-Id, X-Client-Version",
            response.headers.get("Access-Control-Allow-Headers").?,
        );
    }

    var credentialed_wildcard_exposed = credentialed_any_header;
    credentialed_wildcard_exposed.exposed_headers = &wildcard_headers;
    try std.testing.expectError(
        error.CorsCredentialsWithWildcardExposedHeaders,
        validateCorsConfig(&credentialed_wildcard_exposed),
    );

    var injected_origin = "https://allowed.example\r\nX-Injected: true".*;
    var injected_origins = [_][]u8{injected_origin[0..]};
    var unsafe = antfly.common.config.Config.CorsConfig{ .allowed_origins = &injected_origins };
    try std.testing.expectError(error.InvalidCorsOrigin, validateCorsConfig(&unsafe));
    unsafe.enabled = false;
    try validateCorsConfig(&unsafe);

    var injected_header = "X-Safe\r\nX-Injected".*;
    var injected_headers = [_][]u8{injected_header[0..]};
    var unsafe_header = antfly.common.config.Config.CorsConfig{ .allowed_headers = &injected_headers };
    try std.testing.expectError(error.InvalidCorsHeader, validateCorsConfig(&unsafe_header));
}

test "standalone runtime local replica reconcile permit blocks only active startup catch-up" {
    var data_server = antfly.data.runtime.DataServer{
        .alloc = std.testing.allocator,
        .provisioned_storage = undefined,
        .read_source = undefined,
        .write_source = undefined,
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(1),
        .listener_cfg = undefined,
    };

    data_server.provisioned_startup_catch_up_active.store(false, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(true, .monotonic);
    data_server.last_provision_metadata_epoch = null;
    data_server.last_provision_fingerprint = null;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.last_provision_metadata_epoch = 17;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.last_provision_fingerprint = 99;
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(true, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(!runLocalReplicaRootReconcilePermitHook(&data_server));

    data_server.provisioned_startup_catch_up_active.store(false, .monotonic);
    data_server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try std.testing.expect(runLocalReplicaRootReconcilePermitHook(&data_server));
}

test "standalone runtime registers antfarm static routes" {
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try registerAntfarmRoutes(&server);

    try std.testing.expect(server.hasRoute(.get, "/"));
    try std.testing.expect(server.hasRoute(.get, "/assets/*"));
    try std.testing.expect(server.hasRoute(.get, "/fonts/*"));
    try std.testing.expect(server.hasRoute(.get, "/*"));
}

test "standalone runtime antfarm path guards keep api routes reserved" {
    try std.testing.expect(isAntfarmReservedPath("/db/v1/tables"));
    try std.testing.expect(isAntfarmReservedPath("/ai/v1/models"));
    try std.testing.expect(isAntfarmReservedPath("/antfly/readyz"));
    try std.testing.expect(isAntfarmReservedPath(antfly.admin.routes.ha_primary_status));
    try std.testing.expect(isAntfarmReservedPath("/a2a"));
    try std.testing.expect(isAntfarmReservedPath("/.well-known/agent-card.json"));
    try std.testing.expect(isAntfarmReservedPath("/extensions/v1/packages"));
    try std.testing.expect(isAntfarmReservedPath("/unknown/v1/status"));
    try std.testing.expect(isAntfarmReservedPath("/unknown/v12/status"));
    try std.testing.expect(!isAntfarmReservedPath("/models/version/status"));
    try std.testing.expect(!isAntfarmReservedPath("/models"));
    try std.testing.expect(hasUnsafeStaticPath("../index.html"));
    try std.testing.expect(hasUnsafeStaticPath("%2e%2e/index.html"));
    try std.testing.expect(!hasUnsafeStaticPath("assets/index.js"));
}

test "parse cli accepts config path" {
    var argv = [_][*:0]const u8{ "--config", "antfly.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("antfly.json", cfg.config_path.?);
}

test "parse cli accepts secret store path" {
    var argv = [_][*:0]const u8{ "--secret-store-path", "/run/antfly/secrets/secrets.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_paths.items[0]);
}

test "parse cli accepts extension package store path" {
    var argv = [_][*:0]const u8{ "--extension-package-store", "/opt/antfly/extensions" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", cfg.extension_package_store_dir.?);
}

test "parse cli accepts ARD identity flags" {
    var argv = [_][*:0]const u8{
        "--ard-publisher-domain",
        "tenant.example.com",
        "--ard-base-url",
        "https://tenant.example.com",
        "--ard-display-name",
        "Tenant Antfly",
        "--ard-public-catalog",
        "true",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("https://tenant.example.com", cfg.ard_base_url.?);
    try std.testing.expectEqualStrings("tenant.example.com", cfg.ard_publisher_domain.?);
    try std.testing.expectEqualStrings("Tenant Antfly", cfg.ard_display_name.?);
    try std.testing.expect(cfg.ard_public_catalog_enabled);
}

test "parse cli accepts canonical host port and models dir flags" {
    var argv = [_][*:0]const u8{
        "--host",
        "127.0.0.1",
        "--port",
        "8080",
        "--models-dir",
        "/tmp/models",
        "--ml-dir",
        "/tmp/ml",
        "--preload-model",
        "generator:metal:gemma-e2b",
        "--data-dir",
        "/tmp/antfly-data",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("127.0.0.1", cfg.bind_host.?);
    try std.testing.expectEqual(@as(u16, 8080), cfg.bind_port.?);
    try std.testing.expectEqualStrings("/tmp/models", cfg.inference_models_dir.?);
    try std.testing.expectEqualStrings("/tmp/ml", cfg.inference_ml_dir.?);
    try std.testing.expectEqual(@as(usize, 1), cfg.inference_preload_models.items.len);
    try std.testing.expectEqualStrings("generator", cfg.inference_preload_models.items[0].kind.slice());
    try std.testing.expectEqualStrings("gemma-e2b", cfg.inference_preload_models.items[0].name.slice());
    try std.testing.expectEqualStrings("metal", cfg.inference_preload_models.items[0].backend.slice().?);
    try std.testing.expectEqualStrings("/tmp/antfly-data", cfg.data_dir.?);
}

test "parse cli preserves registry variants and recognizes explicit preload backends" {
    var argv = [_][*:0]const u8{
        "--preload-model",
        "embedder:owner/model:i8",
        "--preload-model",
        "generator:metal:owner/model:Q4_K_M",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), cfg.inference_preload_models.items.len);
    try std.testing.expectEqualStrings("owner/model:i8", cfg.inference_preload_models.items[0].name.slice());
    try std.testing.expect(cfg.inference_preload_models.items[0].backend.slice() == null);
    try std.testing.expectEqualStrings("owner/model:Q4_K_M", cfg.inference_preload_models.items[1].name.slice());
    try std.testing.expectEqualStrings("metal", cfg.inference_preload_models.items[1].backend.slice().?);
}

test "parse cli accepts HA primary runtime flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-seed-capture-root",
        "/tmp/ha-seed-captures",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-former-primary-log",
        "/tmp/ha-primary.log",
        "--admin-token-env",
        "ANTFLY_HA_ADMIN_TOKEN",
        "--ha-retention-max-lag-lsn",
        "500",
        "--ha-retention-max-retained-bytes",
        "8192",
        "--ha-retention-max-retained-age-ns",
        "1000000",
        "--ha-cluster-id",
        "100",
        "--ha-shard-id",
        "10",
        "--ha-table-id",
        "20",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expect(haPrimaryRequested(cfg));
    try std.testing.expectEqualStrings("/tmp/ha-primary.log", cfg.ha_primary_log.?);
    try std.testing.expectEqualStrings("/tmp/ha-slots.wal", cfg.ha_primary_slots.?);
    try std.testing.expectEqualStrings("primary-a", cfg.ha_primary_node_id.?);
    try std.testing.expectEqualStrings("/tmp/ha-seed-captures", cfg.ha_seed_capture_root.?);
    try std.testing.expectEqualStrings("/tmp/ha-fence.wal", cfg.ha_fence_wal.?);
    try std.testing.expectEqualStrings("/tmp/ha-primary.log", cfg.ha_former_primary_log.?);
    try std.testing.expectEqualStrings("ANTFLY_HA_ADMIN_TOKEN", cfg.admin_token_env.?);
    try std.testing.expectEqual(@as(u64, 500), cfg.ha_retention_max_lag_lsn.?);
    try std.testing.expectEqual(@as(u64, 8192), cfg.ha_retention_max_retained_bytes.?);
    try std.testing.expectEqual(@as(u64, 1000000), cfg.ha_retention_max_retained_age_ns.?);
    try std.testing.expectEqual(@as(u64, 100), cfg.ha_cluster_id.?);
    try std.testing.expectEqual(@as(u64, 10), cfg.ha_shard_id.?);
    try std.testing.expectEqual(@as(u64, 20), cfg.ha_table_id.?);
    try std.testing.expectEqual(@as(u64, 3), cfg.ha_timeline_id.?);
    try std.testing.expectEqual(@as(u64, 4), cfg.ha_epoch.?);
}

test "parse cli accepts HA primary sync policy flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-sync-mode",
        "remote-apply",
        "--ha-sync-selection",
        "first",
        "--ha-sync-required",
        "2",
        "--ha-sync-standby",
        "standby-a",
        "--ha-sync-standby",
        "standby-b",
        "--ha-sync-failure",
        "fail-closed",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    var sync_policy = try haSyncPolicyFromCli(std.testing.allocator, cfg);
    defer sync_policy.deinit(std.testing.allocator);

    try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_apply, sync_policy.policy.mode);
    try std.testing.expectEqual(antfly.ha.primary.StandbySelection.first, sync_policy.policy.selection);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.required);
    try std.testing.expectEqual(antfly.ha.primary.FailurePolicy.fail_closed, sync_policy.policy.failure_policy);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.standby_names.len);
    try std.testing.expectEqualStrings("standby-a", sync_policy.policy.standby_names[0]);
    try std.testing.expectEqualStrings("standby-b", sync_policy.policy.standby_names[1]);
}

test "parse cli treats ALL HA sync policy as all named standbys" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-primary.slots",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-sync-mode",
        "remote-apply",
        "--ha-sync-selection",
        "all",
        "--ha-sync-standby",
        "standby-a",
        "--ha-sync-standby",
        "standby-b",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    var sync_policy = try haSyncPolicyFromCli(std.testing.allocator, cfg);
    defer sync_policy.deinit(std.testing.allocator);

    try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_apply, sync_policy.policy.mode);
    try std.testing.expectEqual(antfly.ha.primary.StandbySelection.all, sync_policy.policy.selection);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.required);
    try std.testing.expectEqual(@as(usize, 2), sync_policy.policy.standby_names.len);

    cfg.ha_sync_required = 1;
    try std.testing.expectError(error.InvalidHASyncPolicy, haSyncPolicyFromCli(std.testing.allocator, cfg));
}

test "parse cli accepts HA primary retention policy flags" {
    var argv = [_][*:0]const u8{
        "--ha-primary-log",
        "/tmp/ha-primary.log",
        "--ha-primary-slots",
        "/tmp/ha-slots.wal",
        "--ha-primary-node-id",
        "primary-a",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-cluster-id",
        "100",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
        "--ha-retention-max-lag-lsn",
        "50",
        "--ha-retention-max-retained-bytes",
        "4096",
        "--ha-retention-max-retained-age-ns",
        "1000000",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);

    try validateHARole(cfg);
    const retention_policy = try haRetentionPolicyFromCli(cfg);
    try std.testing.expectEqual(@as(u64, 50), retention_policy.max_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4096), retention_policy.max_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1000000), retention_policy.max_retained_age_ns);
}

test "parse cli accepts HA standby runtime flags" {
    var argv = [_][*:0]const u8{
        "--id",
        "7",
        "--ha-standby-log",
        "/tmp/ha-standby.log",
        "--ha-standby-progress",
        "/tmp/ha-standby-progress.wal",
        "--ha-standby-node-id",
        "standby-a",
        "--ha-seed-capture-root",
        "/tmp/ha-seed-captures",
        "--ha-fence-wal",
        "/tmp/ha-fence.wal",
        "--ha-standby-upstream-url",
        "http://primary.antfly.svc:8080",
        "--ha-standby-slot",
        "standby-a",
        "--ha-startup-target-root",
        "/tmp/active",
        "--ha-startup-topology-id",
        "topology-a",
        "--ha-startup-topology-generation",
        "3",
        "--ha-startup-generation",
        "generation-a",
        "--ha-startup-target-pvc-name",
        "standby-a-data",
        "--ha-startup-target-pvc-uid",
        "pvc-uid-1",
        "--ha-startup-capture-receipt-sha256",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--ha-startup-materialized-receipt-sha256",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "--ha-startup-materialized-aggregate-sha256",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "--ha-startup-target-local-node-id",
        "7",
        "--ha-startup-target-replica-id",
        "1",
        "--ha-cluster-id",
        "100",
        "--ha-shard-id",
        "10",
        "--ha-table-id",
        "20",
        "--ha-timeline-id",
        "3",
        "--ha-epoch",
        "4",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try validateHARole(cfg);
    try std.testing.expect(!haPrimaryRequested(cfg));
    try std.testing.expect(haStandbyRequested(cfg));
    try std.testing.expectEqualStrings("/tmp/ha-standby.log", cfg.ha_standby_log.?);
    try std.testing.expectEqualStrings("/tmp/ha-standby-progress.wal", cfg.ha_standby_progress.?);
    try std.testing.expectEqualStrings("standby-a", cfg.ha_standby_node_id.?);
    try std.testing.expectEqualStrings("/tmp/ha-seed-captures", cfg.ha_seed_capture_root.?);
    try std.testing.expectEqualStrings("/tmp/ha-fence.wal", cfg.ha_fence_wal.?);
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", cfg.ha_standby_upstream_url.?);
    try std.testing.expectEqualStrings("standby-a", cfg.ha_standby_slot.?);
    try std.testing.expectEqualStrings("/tmp/active", cfg.ha_startup_target_root.?);
    try std.testing.expectEqualStrings("topology-a", cfg.ha_startup_topology_id.?);
    try std.testing.expectEqual(@as(u64, 3), cfg.ha_startup_topology_generation.?);
    try std.testing.expectEqualStrings("generation-a", cfg.ha_startup_generation.?);
    try std.testing.expectEqualStrings("standby-a-data", cfg.ha_startup_target_pvc_name.?);
    try std.testing.expectEqualStrings("pvc-uid-1", cfg.ha_startup_target_pvc_uid.?);
    try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", cfg.ha_startup_capture_receipt_sha256.?);
    try std.testing.expectEqualStrings("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", cfg.ha_startup_materialized_receipt_sha256.?);
    try std.testing.expectEqualStrings("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", cfg.ha_startup_materialized_aggregate_sha256.?);
    try std.testing.expectEqual(@as(u64, 7), cfg.ha_startup_target_local_node_id.?);
    try std.testing.expectEqual(@as(u64, 1), cfg.ha_startup_target_replica_id.?);
    try std.testing.expectEqual(@as(u64, 100), cfg.ha_cluster_id.?);
    try std.testing.expectEqual(@as(u64, 10), cfg.ha_shard_id.?);
    try std.testing.expectEqual(@as(u64, 20), cfg.ha_table_id.?);
    try std.testing.expectEqual(@as(u64, 3), cfg.ha_timeline_id.?);
    try std.testing.expectEqual(@as(u64, 4), cfg.ha_epoch.?);

    const replication_cfg = (try haStandbyReplicationConfigFromCliWithBearerToken(cfg, "runtime-secret-token")) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", replication_cfg.upstream_base_uri);
    try std.testing.expectEqualStrings("standby-a", replication_cfg.slot_name);
    try std.testing.expectEqualStrings("runtime-secret-token", replication_cfg.bearer_token orelse return error.TestExpectedEqual);
    const startup = (try haStartupExpectationFromCli(cfg)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("/tmp/active", startup.target_root);
    try std.testing.expectEqualStrings("topology-a", startup.binding.topology_id);
    try std.testing.expectEqual(@as(u64, 3), startup.binding.topology_generation);
    try std.testing.expectEqualStrings("generation-a", startup.expected.generation);
    try std.testing.expectEqualStrings(startup.capture_receipt_sha256.?, startup.expected.capture_receipt_sha256.?);
    try std.testing.expectEqualStrings("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", startup.materialized_receipt_sha256.?);
    try std.testing.expectEqualStrings("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", startup.materialized_aggregate_sha256.?);
    try std.testing.expectEqual(@as(u64, 7), startup.target_local_node_id.?);
    try std.testing.expectEqual(@as(u64, 1), startup.target_replica_id.?);

    var missing_capture_authority = cfg;
    missing_capture_authority.ha_startup_capture_receipt_sha256 = null;
    try std.testing.expectError(error.HAStartupCaptureReceiptSHA256Missing, haStartupExpectationFromCli(missing_capture_authority));

    var missing_materialized_receipt = cfg;
    missing_materialized_receipt.ha_startup_materialized_receipt_sha256 = null;
    try std.testing.expectError(error.HAStartupMaterializedReceiptSHA256Missing, haStartupExpectationFromCli(missing_materialized_receipt));

    var missing_materialized_aggregate = cfg;
    missing_materialized_aggregate.ha_startup_materialized_aggregate_sha256 = null;
    try std.testing.expectError(error.HAStartupMaterializedAggregateSHA256Missing, haStartupExpectationFromCli(missing_materialized_aggregate));

    var missing_target_local_node = cfg;
    missing_target_local_node.ha_startup_target_local_node_id = null;
    try std.testing.expectError(error.HAStartupTargetLocalNodeIDMissing, haStartupExpectationFromCli(missing_target_local_node));

    var missing_target_replica = cfg;
    missing_target_replica.ha_startup_target_replica_id = null;
    try std.testing.expectError(error.HAStartupTargetReplicaIDMissing, haStartupExpectationFromCli(missing_target_replica));

    var wrong_target_local_node = cfg;
    wrong_target_local_node.ha_startup_target_local_node_id = 8;
    try std.testing.expectError(error.HAStartupTargetLocalNodeIDMismatch, haStartupExpectationFromCli(wrong_target_local_node));

    var wrong_target_replica = cfg;
    wrong_target_replica.ha_startup_target_replica_id = 2;
    try std.testing.expectError(error.HAStartupTargetReplicaIDMismatch, haStartupExpectationFromCli(wrong_target_replica));
}

test "standalone HA standby replication flags require upstream and slot" {
    try std.testing.expectError(error.HAStandbySlotMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = " \t ",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotMissing, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = " \t ",
    }));

    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "  http://primary.antfly.svc:8080 \n",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080/\treplication",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = " standby-a\t",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "primary.antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http:///replication",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbyUpstreamUrlInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "file:///tmp/primary",
        .ha_standby_slot = "standby-a",
    }));
    try std.testing.expectError(error.HAStandbySlotInvalid, haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = "standby a",
    }));

    const replication_cfg = (try haStandbyReplicationConfigFromCli(.{
        .ha_standby_upstream_url = "http://primary.antfly.svc:8080",
        .ha_standby_slot = "standby-a",
    })) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("http://primary.antfly.svc:8080", replication_cfg.upstream_base_uri);
    try std.testing.expectEqualStrings("standby-a", replication_cfg.slot_name);
}

test "standalone HA string classifier distinguishes missing padded and valid values" {
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(null));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(""));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.missing, antfly.ha.validation.classifyHAString(" \t\r\n"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.padded, antfly.ha.validation.classifyHAString(" standby-a"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.padded, antfly.ha.validation.classifyHAString("standby-a\n"));
    try std.testing.expectEqual(antfly.ha.validation.HAStringValidation.ok, antfly.ha.validation.classifyHAString("standby-a"));

    try std.testing.expectError(error.HAStandbySlotMissing, requireHAString(null, error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectError(error.HAStandbySlotMissing, requireHAString(" \t", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectError(error.HAStandbySlotInvalid, requireHAString(" standby-a ", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
    try std.testing.expectEqualStrings("standby-a", try requireHAString("standby-a", error.HAStandbySlotMissing, error.HAStandbySlotInvalid));
}

test "standalone HA primary identity defaults shard and table to whole instance" {
    const identity = try haPrimaryIdentity(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    });
    try std.testing.expectEqual(@as(u64, 100), identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), identity.table_id);
    try std.testing.expectEqual(@as(u64, 3), identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 4), identity.epoch);
}

test "standalone HA standby identity defaults shard and table to whole instance" {
    const identity = try haStandbyIdentity(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    });
    try std.testing.expectEqual(@as(u64, 100), identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), identity.table_id);
    try std.testing.expectEqual(@as(u64, 3), identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 4), identity.epoch);
}

test "standalone HA runtime rejects ambiguous role flags" {
    try std.testing.expectError(error.HAMultipleRolesConfigured, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_standby_log = "/tmp/standby.log",
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_fence_wal = "/tmp/fence.wal",
    }));
    try std.testing.expectError(error.HARoleMissing, validateHARole(.{
        .ha_former_primary_log = "/tmp/former-primary.wal",
    }));
    try validateHARole(.{
        .admin_token_env = "ANTFLY_HA_ADMIN_TOKEN",
    });
    try std.testing.expectError(error.AdminTokenEnvMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = " \t ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = " ANTFLY_HA_ADMIN_TOKEN ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = "bad-token-env",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .admin_token_env = "9TOKEN",
    }));
    try std.testing.expectError(error.HAFenceWalMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
    }));
    try std.testing.expectError(error.HAFenceWalMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = " \t ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = " /tmp/fence.wal ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_former_primary_log = " /tmp/former-primary.wal ",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_former_primary_log = "/tmp/../former-primary.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAClusterIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HATimelineIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAEpochMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
    }));
    try std.testing.expectError(error.HAPrimaryLogMissing, validateHARole(.{
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdMissing, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = " /tmp/primary.log ",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHARole(.{
        .ha_primary_log = "primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = " /tmp/slots.wal ",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp//slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = " primary-a ",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAPrimaryNodeIdInvalid, validateHARole(.{
        .ha_primary_log = "/tmp/primary.log",
        .ha_primary_slots = "/tmp/slots.wal",
        .ha_primary_node_id = "primary a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAClusterIdMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogMissing, validateHARole(.{
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdMissing, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogInvalid, validateHARole(.{
        .ha_standby_log = " /tmp/standby.log ",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyLogInvalid, validateHARole(.{
        .ha_standby_log = "standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = " /tmp/progress.wal ",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/../progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = " standby-a ",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HAStandbyNodeIdInvalid, validateHARole(.{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_lag_lsn = 50,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_retained_bytes = 4096,
    }));
    try std.testing.expectError(error.HARetentionPolicyRequiresPrimary, validateHARole(.{
        .ha_retention_max_retained_age_ns = 1000000,
    }));
    try std.testing.expectError(error.InvalidHARetentionPolicy, parsePositiveU64("0"));
    try std.testing.expectError(error.HASyncPolicyRequiresPrimary, validateHARole(.{
        .ha_sync_mode = .remote_write,
    }));

    var promoted_policy_cli = CliConfig{
        .ha_standby_log = "/tmp/standby.log",
        .ha_standby_progress = "/tmp/progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
        .ha_sync_mode = .remote_apply,
        .ha_sync_required = 1,
        .ha_sync_failure_policy = .block,
    };
    defer promoted_policy_cli.deinit(std.testing.allocator);
    try promoted_policy_cli.ha_sync_standby_names.append(std.testing.allocator, "primary-a");
    try validateHARole(promoted_policy_cli);
    var promoted_policy = try haSyncPolicyFromCli(std.testing.allocator, promoted_policy_cli);
    defer promoted_policy.deinit(std.testing.allocator);
    try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_apply, promoted_policy.policy.mode);
    try std.testing.expectEqual(@as(usize, 1), promoted_policy.policy.required);
    try std.testing.expectEqualStrings("primary-a", promoted_policy.policy.standby_names[0]);
    try std.testing.expectEqual(antfly.ha.primary.FailurePolicy.block, promoted_policy.policy.failure_policy);
    try std.testing.expectError(error.InvalidHASyncPolicy, haSyncPolicyFromCli(std.testing.allocator, .{
        .ha_primary_log = "/tmp/primary.log",
        .ha_fence_wal = "/tmp/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
        .ha_sync_mode = .remote_write,
        .ha_sync_required = 1,
    }));
}

test "standalone HA runtime requires HA paths under resolved data root" {
    const root = "/tmp/antfly-data-root";
    const primary_cfg = CliConfig{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_former_primary_log = root ++ "/ha/primary.log",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    };
    try validateHARole(primary_cfg);
    try validateHAPathsUnderRoot(primary_cfg, root);

    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = "/tmp/outside/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAPrimarySlotsInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = "/tmp/outside/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAFenceWalInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = "/tmp/outside/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAFormerPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = root ++ "/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_former_primary_log = "/tmp/outside/former-primary.log",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));

    const standby_cfg = CliConfig{
        .ha_standby_log = root ++ "/ha/standby.log",
        .ha_standby_progress = root ++ "/ha/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    };
    try validateHARole(standby_cfg);
    try validateHAPathsUnderRoot(standby_cfg, root);

    try std.testing.expectError(error.HAStandbyLogInvalid, validateHAPathsUnderRoot(.{
        .ha_standby_log = "/tmp/outside/standby.log",
        .ha_standby_progress = root ++ "/ha/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAStandbyProgressInvalid, validateHAPathsUnderRoot(.{
        .ha_standby_log = root ++ "/ha/standby.log",
        .ha_standby_progress = "/tmp/outside/standby-progress.wal",
        .ha_standby_node_id = "standby-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
    try std.testing.expectError(error.HAPrimaryLogInvalid, validateHAPathsUnderRoot(.{
        .ha_primary_log = "/tmp/antfly-data-root2/ha/primary.log",
        .ha_primary_slots = root ++ "/ha/slots.wal",
        .ha_primary_node_id = "primary-a",
        .ha_fence_wal = root ++ "/ha/fence.wal",
        .ha_cluster_id = 100,
        .ha_timeline_id = 3,
        .ha_epoch = 4,
    }, root));
}

test "standalone HA runtime validates bearer token env name before lookup" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const env_name = "ANTFLY_HA_ADMIN_TOKEN_TEST_VALUE";

    try std.testing.expect((try resolveAdminBearerTokenFromCli(alloc, .{})) == null);
    try std.testing.expectError(error.AdminTokenEnvMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = " \t ",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "bad-token-env",
    }));
    try std.testing.expectError(error.AdminTokenEnvInvalid, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "9TOKEN",
    }));
    try std.testing.expectError(error.AdminTokenMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = "ANTFLY_HA_ADMIN_TOKEN_SHOULD_NOT_EXIST",
    }));

    try std.testing.expectEqual(@as(c_int, 0), c.setenv(env_name, " secret-token\n", 1));
    defer _ = c.unsetenv(env_name);
    const token = try resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = env_name,
    });
    defer alloc.free(token.?);
    try std.testing.expectEqualStrings("secret-token", token.?);

    try std.testing.expectEqual(@as(c_int, 0), c.setenv(env_name, " \t\n", 1));
    try std.testing.expectError(error.AdminTokenMissing, resolveAdminBearerTokenFromCli(alloc, .{
        .admin_token_env = env_name,
    }));
}

test "standalone runtime defaults public listener to antfarm port" {
    const listener = resolvePublicListener(.{});
    try std.testing.expectEqualStrings("127.0.0.1", listener.bind_host);
    try std.testing.expectEqual(@as(u16, default_public_port), listener.bind_port);
}

test "standalone public HTTP server is restart-safe and uses public API request body limit" {
    const cfg = publicHttpServerConfig("127.0.0.1", 8080);
    try std.testing.expect(cfg.reuse_address);
    try std.testing.expect(!cfg.reuse_port);
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_body_size);
    try std.testing.expectEqual(@as(usize, 256 * 1024 * 1024), cfg.request_body_buffer_budget_bytes);
    try std.testing.expect(cfg.max_connections >= 1);
    try std.testing.expect(cfg.max_connections <= public_http_connection_ceiling);
    try std.testing.expectEqual(cfg.max_connections, cfg.max_request_tasks);
    try std.testing.expectEqual(public_http_max_h1_inflight_bodies, cfg.max_h1_inflight_bodies);
    try std.testing.expectEqual(@as(u32, 5), cfg.accept_error_backoff_initial_ms);
    try std.testing.expectEqual(@as(u32, 1_000), cfg.accept_error_backoff_max_ms);
    try std.testing.expectEqual(@as(u32, 256), publicHttpConnectionLimitForFdSoftLimit(1024));
    try std.testing.expectEqual(@as(u32, 128), publicHttpConnectionLimitForFdSoftLimit(512));
    try std.testing.expectEqual(@as(u32, 32), publicHttpConnectionLimitForFdSoftLimit(128));
    try std.testing.expectEqual(@as(u32, 1), publicHttpConnectionLimitForFdSoftLimit(3));
}

test "standalone rejects configured server TLS instead of serving plaintext" {
    try antfly.common.config.Config.validateServerTlsConfig(null);
    try std.testing.expectError(error.ServerTlsUnsupported, antfly.common.config.Config.validateServerTlsConfig(.{}));
}

test "standalone Lite transaction sessions survive file reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/sessions.aflite", .{tmp.sub_path});
    defer alloc.free(path);

    var txn_id: antfly.db.types.TxnId = undefined;
    {
        var backend = try antfly.lite.backend.Handle.openOrCreate(alloc, path, .{ .no_sync = false });
        defer backend.deinit();
        var durable = antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        );
        var registry = antfly.public_api.transactions.SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        txn_id = (try registry.begin(alloc, .{ .sync_level = .write }, 1)).txn_id;
    }

    {
        var backend = try antfly.lite.backend.Handle.open(alloc, path, .{});
        defer backend.deinit();
        var durable = antfly.public_api.transactions.DurableSessionStore.initRuntime(
            alloc,
            try backend.runtimeStoreForNamespace("system/api-transaction-sessions"),
        );
        var registry = antfly.public_api.transactions.SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const restored = registry.getInfo(txn_id) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(txn_id, restored.txn_id);
        try std.testing.expectEqual(antfly.db.types.SyncLevel.write, restored.sync_level);
    }
}

test "antfly config uses cli override before common config" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .inference = .{
            .api_url = try alloc.dupe(u8, "http://127.0.0.1:9000"),
            .models_dir = try alloc.dupe(u8, "/tmp/from-config"),
            .ml_dir = try alloc.dupe(u8, "/tmp/ml-from-config"),
        },
    };
    defer cfg.deinit();

    const cli = CliConfig{
        .inference_models_dir = "/tmp/from-cli",
        .inference_ml_dir = "/tmp/ml-from-cli",
        .inference_backend_budget_mb = 8192,
    };
    try std.testing.expectEqualStrings("/tmp/from-cli", resolveInferenceModelsDir(cli, &cfg).?);
    try std.testing.expectEqualStrings("/tmp/ml-from-cli", resolveInferenceMlDir(cli, &cfg).?);
    try std.testing.expectEqual(@as(usize, 8192 * 1024 * 1024), (try resolveInferenceBudgetOverrides(cli)).backend_limit_bytes);
}

test "standalone memory budget conversion rejects overflow" {
    try std.testing.expectError(error.InvalidArguments, mibToBytes(std.math.maxInt(usize)));
    try std.testing.expectEqual(
        @as(usize, 0),
        try process_memory_budget.resolve(0, "invalid", "14000"),
    );
}

test "standalone public api caps keep alive request reuse" {
    try std.testing.expect(public_api_max_requests_per_connection > 0);
    try std.testing.expect(public_api_max_requests_per_connection < 1000);
}

test "standalone public api body limit matches common http listener" {
    try std.testing.expectEqual(antfly.common.http.default_max_request_bytes, public_api_max_body_size);
}

test "standalone readiness follows api initialization and unified listener" {
    try std.testing.expect(!standaloneReadyFromState(false, false));
    try std.testing.expect(!standaloneReadyFromState(false, true));
    try std.testing.expect(!standaloneReadyFromState(true, false));
    try std.testing.expect(standaloneReadyFromState(true, true));
}

test "standalone startup checkpoint readiness requires applied and safe-read progress" {
    try std.testing.expect(!startupCheckpointSatisfied(.{ .received_lsn = 11, .applied_lsn = 10, .safe_read_lsn = 10 }, 11));
    try std.testing.expect(!startupCheckpointSatisfied(.{ .received_lsn = 11, .applied_lsn = 11, .safe_read_lsn = 10 }, 11));
    try std.testing.expect(startupCheckpointSatisfied(.{ .received_lsn = 11, .applied_lsn = 11, .safe_read_lsn = 11 }, 11));
}

test "standalone public ready endpoint fails closed before API initialization" {
    var route_context = StandaloneHttpContext{ .api_server = null };

    var request = try httpx.Request.init(std.testing.allocator, .GET, "/readyz");
    defer request.deinit();
    var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer ctx.deinit();
    var response = try readyzHandler(&route_context, &ctx);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("{\"status\":\"not_ready\"}", response.body.?);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
}

test "parse cli accepts inference budget overrides" {
    var argv = [_][*:0]const u8{
        "--inference-host-budget-mb",
        "4096",
        "--inference-backend-budget-mb",
        "12288",
        "--inference-combined-budget-mb",
        "16384",
        "--inference-kv-budget-mb",
        "2048",
        "--inference-scratch-budget-mb",
        "1024",
        "--process-memory-budget-mb",
        "14000",
        "--kernel-jit-mode",
        "required",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4096), cfg.inference_host_budget_mb);
    try std.testing.expectEqual(@as(usize, 12288), cfg.inference_backend_budget_mb);
    try std.testing.expectEqual(@as(usize, 16384), cfg.inference_combined_budget_mb);
    try std.testing.expectEqual(@as(usize, 2048), cfg.inference_kv_budget_mb);
    try std.testing.expectEqual(@as(usize, 1024), cfg.inference_scratch_budget_mb);
    try std.testing.expectEqual(@as(?usize, 14000), cfg.inference_process_memory_budget_mb);
    try std.testing.expectEqual(antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode.required, cfg.inference_kernel_jit_mode.?);
}

test "standalone preserves effective process envelope provenance for inference" {
    const Case = struct {
        source: process_memory_budget.EffectiveSource,
        expected: inference_bridge.ProcessMemoryLimitProvenance,
    };
    inline for ([_]Case{
        .{ .source = .explicit, .expected = .explicit },
        .{ .source = .cgroup_v2, .expected = .cgroup_v2 },
        .{ .source = .cgroup_v1, .expected = .cgroup_v1 },
        .{ .source = .host, .expected = .host },
        .{ .source = .unavailable, .expected = .unavailable },
    }) |case| {
        try std.testing.expectEqual(
            case.expected,
            inferenceMemoryLimitProvenance(case.source),
        );
    }
}

test "standalone kernel JIT mode precedence is CLI then environment then config" {
    const Mode = antfly.common.config.Config.InferenceConfig.KernelJitConfig.Mode;
    try std.testing.expectEqual(Mode.on, try resolveKernelJitMode(.shadow, "on", null));
    try std.testing.expectEqual(Mode.required, try resolveKernelJitMode(.shadow, "invalid", .required));
    try std.testing.expectError(error.InvalidArguments, resolveKernelJitMode(.shadow, "invalid", null));
}

test "inference config falls back to common config" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .admission = .{
            .inference = .{ .max_concurrent_requests = 0 },
        },
        .inference = .{
            .api_url = try alloc.dupe(u8, "http://127.0.0.1:8089"),
            .models_dir = try alloc.dupe(u8, "/tmp/antfly-models"),
            .ml_dir = try alloc.dupe(u8, "/tmp/antfly-ml"),
            .kernel_jit = .{
                .mode = .shadow,
                .cache_dir = try alloc.dupe(u8, "/tmp/antfly-jit"),
                .max_cache_bytes_mb = 256,
                .preload_budget_ms = 120_000,
            },
            .prompt_cache = .{
                .enabled = true,
                .mode = .simple,
                .max_bytes_mb = 256,
                .min_tokens = 48,
                .ttl_ms = 120_000,
            },
            .preload = try alloc.dupe(antfly.common.config.Config.InferenceConfig.WarmModelConfig, &.{
                .{
                    .kind = try alloc.dupe(u8, "generator"),
                    .name = try alloc.dupe(u8, "antflydb/gemma-e2b"),
                    .backend = try alloc.dupe(u8, "metal"),
                    .format = try alloc.dupe(u8, "gguf"),
                    .quantization = try alloc.dupe(u8, "q4_k"),
                },
            }),
        },
    };
    defer cfg.deinit();

    try std.testing.expectEqualStrings("/tmp/antfly-models", resolveInferenceModelsDir(.{}, &cfg).?);
    try std.testing.expectEqualStrings("/tmp/antfly-ml", resolveInferenceMlDir(.{}, &cfg).?);
    try std.testing.expectEqual(@as(u32, 0), resolveInferenceMaxConcurrentRequests(&cfg));
    try std.testing.expectEqual(
        antfly.common.config.default_inference_max_concurrent_requests,
        resolveInferenceMaxConcurrentRequests(null),
    );
    try std.testing.expectEqual(@as(usize, 1), cfg.inference.preload.len);
    try std.testing.expectEqualStrings("generator", cfg.inference.preload[0].kind);
    try std.testing.expectEqualStrings("antflydb/gemma-e2b", cfg.inference.preload[0].name);
    try std.testing.expectEqualStrings("metal", cfg.inference.preload[0].backend.?);
    try std.testing.expectEqualStrings("gguf", cfg.inference.preload[0].format.?);
    try std.testing.expectEqualStrings("q4_k", cfg.inference.preload[0].quantization.?);
}

test "inference admission bridge charges combined native residency to resource manager" {
    var budgets = antfly.resource_manager.Options.defaultBudgets();
    budgets[@intFromEnum(antfly.resource_manager.Slice.inference_model_residency)] =
        .{ .hard_limit_bytes = 100 };
    var manager = antfly.resource_manager.ResourceManager.init(.{ .budgets = budgets });
    var owner = InferenceResourceBudgetOwner{
        .alloc = std.testing.allocator,
        .manager = &manager,
    };
    defer owner.deinit();
    try std.testing.expectEqual(@as(u8, 1), retainInferenceResourceOwner(&owner));
    try std.testing.expectEqual(@as(usize, 2), owner.references.load(.acquire));
    releaseInferenceResourceOwner(&owner);
    try std.testing.expectEqual(@as(usize, 1), owner.references.load(.acquire));

    const oversized = inference_bridge.AdmissionAmounts{
        .host_weight_bytes = 80,
        .backend_weight_bytes = 30,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    };
    var lease_token: usize = 0;
    try std.testing.expectEqual(
        error.ResourceRequestTooLarge,
        inference_bridge.errorFromStatus(reserveInferenceResources(&owner, &oversized, &lease_token)),
    );
    try std.testing.expectEqual(@as(usize, 0), lease_token);
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    const admitted = inference_bridge.AdmissionAmounts{
        .host_weight_bytes = 60,
        .backend_weight_bytes = 30,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    };
    try std.testing.expect(reserveInferenceResources(&owner, &admitted, &lease_token).isOk());
    try std.testing.expect(lease_token != 0);
    try std.testing.expectEqual(
        @as(u64, 90),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
    try std.testing.expectEqual(
        if (builtin.os.tag == .macos) @as(u64, 90) else @as(u64, 60),
        manager.snapshot().memory.used_bytes,
    );

    const retained = inference_bridge.AdmissionAmounts{
        .host_weight_bytes = 40,
        .backend_weight_bytes = 30,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    };
    try std.testing.expect(retainInferenceResources(&owner, lease_token, &retained).isOk());
    try std.testing.expectEqual(
        @as(u64, 70),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );

    const unavailable = inference_bridge.AdmissionAmounts{
        .host_weight_bytes = 31,
        .backend_weight_bytes = 0,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    };
    var unavailable_lease: usize = 0;
    try std.testing.expectEqual(
        error.ResourceTemporarilyUnavailable,
        inference_bridge.errorFromStatus(reserveInferenceResources(&owner, &unavailable, &unavailable_lease)),
    );
    try std.testing.expectEqual(@as(usize, 0), unavailable_lease);

    releaseInferenceResources(&owner, lease_token);
    try std.testing.expectEqual(
        @as(u64, 0),
        manager.sliceStats(.inference_model_residency).used_bytes,
    );
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    var replacement_token: usize = 0;
    const replacement = inference_bridge.AdmissionAmounts{
        .host_weight_bytes = 10,
        .backend_weight_bytes = 0,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    };
    try std.testing.expect(reserveInferenceResources(&owner, &replacement, &replacement_token).isOk());
    try std.testing.expect(replacement_token != lease_token);
    releaseInferenceResources(&owner, lease_token);
    try std.testing.expectEqual(@as(u64, 10), manager.snapshot().memory.used_bytes);
    try std.testing.expect(!retainInferenceResources(&owner, lease_token, &replacement).isOk());
    try std.testing.expectEqual(@as(u64, 10), manager.snapshot().memory.used_bytes);
    releaseInferenceResources(&owner, replacement_token);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    try std.testing.expectEqual(@as(u8, 1), observeInferencePromptCache(&owner, 1, 0, 10));
    try std.testing.expectEqual(@as(u8, 1), observeInferencePromptCache(&owner, 2, 0, 20));
    try std.testing.expectEqual(@as(u8, 0), observeInferencePromptCache(&owner, 1, 0, 0));
    try std.testing.expectEqual(@as(u64, 30), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 1), observeInferencePromptCache(&owner, 1, 10, 0));
    try std.testing.expectEqual(@as(u8, 1), observeInferencePromptCache(&owner, 2, 20, 0));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    try std.testing.expectEqual(@as(u8, 1), observeInferenceTokenizerCache(&owner, 11, 0, 10));
    try std.testing.expectEqual(@as(u8, 1), observeInferenceTokenizerCache(&owner, 22, 0, 10));
    try std.testing.expectEqual(@as(u8, 0), observeInferenceTokenizerCache(&owner, 11, 0, 0));
    try std.testing.expectEqual(@as(u64, 20), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 1), observeInferenceTokenizerCache(&owner, 11, 10, 0));
    try std.testing.expectEqual(@as(u64, 10), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u8, 1), observeInferenceTokenizerCache(&owner, 22, 10, 0));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "standalone tokenizer bridge enforces growth and permits exact teardown" {
    var budgets = antfly.resource_manager.Options.defaultBudgets();
    budgets[@intFromEnum(antfly.resource_manager.Slice.inference_tokenizer_cache)] =
        .{ .hard_limit_bytes = 16 };
    var manager = antfly.resource_manager.ResourceManager.init(.{
        .memory_budget = .{ .hard_limit_bytes = 20 },
        .budgets = budgets,
        .identity_allocator = std.testing.allocator,
    });
    defer manager.deinit(std.testing.allocator);
    var owner = InferenceResourceBudgetOwner{
        .alloc = std.testing.allocator,
        .manager = &manager,
    };
    defer owner.deinit();

    try std.testing.expectEqual(
        @as(u8, 1),
        observeInferenceTokenizerCache(&owner, 101, 0, 12),
    );
    try std.testing.expectEqual(
        @as(u8, 0),
        observeInferenceTokenizerCache(&owner, 101, 12, 18),
    );
    try std.testing.expectEqual(
        @as(u64, 12),
        manager.sliceStats(.inference_tokenizer_cache).used_bytes,
    );
    try std.testing.expectEqual(
        @as(u8, 1),
        observeInferenceTokenizerCache(&owner, 101, 12, 0),
    );
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "standalone runtime resolves paths from common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{},
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
        .inference = .{},
    };
    defer cfg.deinit();

    const resolved = try resolvePaths(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    const expected_data_base = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/data");
    defer alloc.free(expected_data_base);
    const expected_metadata_base = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/metadata");
    defer alloc.free(expected_metadata_base);
    const expected_replica_root = try std.fs.path.join(alloc, &.{ expected_data_base, "replicas" });
    defer alloc.free(expected_replica_root);
    const expected_replica_catalog = try std.fs.path.join(alloc, &.{ expected_data_base, "catalog.txt" });
    defer alloc.free(expected_replica_catalog);
    const expected_local_metadata = try std.fs.path.join(alloc, &.{ expected_metadata_base, "local-metadata.json" });
    defer alloc.free(expected_local_metadata);
    const expected_snapshot_root = try std.fs.path.join(alloc, &.{ expected_data_base, "snapshots" });
    defer alloc.free(expected_snapshot_root);
    const expected_extension_store = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/extensions");
    defer alloc.free(expected_extension_store);
    try std.testing.expectEqualStrings(expected_replica_root, resolved.replica_root_dir);
    try std.testing.expectEqualStrings(expected_replica_catalog, resolved.replica_catalog_path);
    try std.testing.expectEqualStrings(expected_local_metadata, resolved.local_metadata_catalog_path);
    try std.testing.expectEqualStrings(expected_snapshot_root, resolved.snapshot_root_dir);
    try std.testing.expectEqualStrings(expected_extension_store, resolved.extension_package_store_dir);
    const expected_secret_store = try normalizeResolvedPathAlloc(alloc, "/tmp/antflydb/secrets.json");
    defer alloc.free(expected_secret_store);
    try std.testing.expectEqualStrings(expected_secret_store, resolved.secret_store_path);
}

test "standalone runtime resolves explicit extension package store path" {
    const alloc = std.testing.allocator;
    const resolved = try resolvePaths(alloc, .{ .extension_package_store_dir = "/opt/antfly/extensions" }, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", resolved.extension_package_store_dir);
}

test "standalone runtime resolves extension package store env before local default" {
    const alloc = std.testing.allocator;

    const env_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, null, "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(env_resolved);
    try std.testing.expectEqualStrings("/antfly-extension-env", env_resolved);

    const cli_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, "/antfly-cli-extensions", "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(cli_resolved);
    try std.testing.expectEqualStrings("/antfly-cli-extensions", cli_resolved);
}

test "standalone runtime resolves explicit secret store path" {
    const alloc = std.testing.allocator;
    var cli = CliConfig{};
    defer cli.deinit(alloc);
    try cli.secret_store_paths.append(alloc, "/run/antfly/secrets/secrets.json");
    const resolved = try resolvePaths(alloc, cli, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", resolved.secret_store_path);
}

test "standalone runtime data dir overrides common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{},
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/from-config"),
        },
        .inference = .{},
    };
    defer cfg.deinit();

    const local_base = try resolveLocalBaseDir(alloc, .{ .data_dir = "/tmp/from-cli" }, &cfg);
    defer alloc.free(local_base);
    try std.testing.expectEqualStrings("/tmp/from-cli", local_base);

    const resolved = try resolvePaths(alloc, .{ .data_dir = "/tmp/from-cli" }, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/replicas", resolved.replica_root_dir);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/catalog.txt", resolved.replica_catalog_path);
    try std.testing.expectEqualStrings("/tmp/from-cli/metadata/local-metadata.json", resolved.local_metadata_catalog_path);
    try std.testing.expectEqualStrings("/tmp/from-cli/data/snapshots", resolved.snapshot_root_dir);
}

test "standalone metadata rolls back an undurable catalog mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/catalog-directory", .{tmp.sub_path});
    defer alloc.free(catalog_dir);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try ensureDirPath(io_impl.io(), catalog_dir);

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, catalog_dir),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();

    {
        var mutation = try metadata.beginCatalogMutationLocked();
        defer mutation.deinit(&metadata);
        const table = antfly.public_api.tables.deriveTableRecord("docs", .{});
        try metadata.manager.upsertTable(table);
        metadata.epoch = 9;
        var persist_failed = false;
        mutation.commit(&metadata) catch {
            persist_failed = true;
        };
        try std.testing.expect(persist_failed);
    }
    try std.testing.expectEqual(@as(u64, 1), metadata.epoch);
    try std.testing.expect(metadata.findTableByNameLocked("docs") == null);
}

test "standalone metadata advertises a linearizable owned snapshot" {
    const alloc = std.testing.allocator;
    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, ".zig-cache/unused-linearizable-snapshot-catalog"),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();
    try metadata.manager.upsertTable(.{ .table_id = 7, .name = "docs" });
    metadata.epoch = 9;

    const source = metadata.statusSource();
    var snapshot = (try source.linearizableSnapshot(.{})) orelse return error.TestUnexpectedResult;
    defer source.freeAdminSnapshot(&snapshot);
    try std.testing.expectEqual(@as(u64, 9), snapshot.status.metadata_epoch);
    try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
    try std.testing.expectEqualStrings("docs", snapshot.tables[0].name);

    try metadata.setApiUrl("http://127.0.0.1:49152");
    var rebound_snapshot = (try source.linearizableSnapshot(.{})) orelse return error.TestUnexpectedResult;
    defer source.freeAdminSnapshot(&rebound_snapshot);
    try std.testing.expectEqual(@as(usize, 1), rebound_snapshot.stores.len);
    try std.testing.expectEqualStrings("http://127.0.0.1:49152", rebound_snapshot.stores[0].api_url);
}

test "standalone metadata conditional drop preserves a replacement identity" {
    const alloc = std.testing.allocator;
    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, ".zig-cache/unused-conditional-drop-catalog"),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();
    try metadata.manager.upsertTable(.{ .table_id = 8, .name = "docs" });

    const source = metadata.statusSource();
    try std.testing.expect(source.supportsExpectedTableDrop());
    try std.testing.expectError(error.TableGenerationChanged, source.dropTableExpected(alloc, "docs", 7));
    try std.testing.expectEqual(@as(u64, 8), metadata.findTableByNameLocked("docs").?.table_id);
}

test "standalone metadata rejects corrupt catalog without double-freeing owned paths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/corrupt-catalog.json", .{tmp.sub_path});
    defer alloc.free(catalog_path);
    try writeFileAtomically(alloc, std.Options.debug_io, catalog_path, "{not-json");

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    const result = LocalStandaloneMetadata.init(
        alloc,
        1,
        1,
        "http://127.0.0.1:8080",
        ".",
        catalog_path,
        backend_runtime.ptr(),
        null,
        .local,
    );
    if (result) |value| {
        var metadata = value;
        defer metadata.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}

test "standalone metadata finalizes schema migration from resident runtime evidence" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/catalog.json", .{tmp.sub_path});
    defer alloc.free(catalog_path);

    var backend_runtime = try antfly.db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer backend_runtime.deinit();
    var metadata = LocalStandaloneMetadata{
        .alloc = alloc,
        .manager = antfly.metadata.TableManager.init(alloc),
        .extension_catalog = antfly.extensions.ExtensionCatalog.init(alloc),
        .local_node_id = 1,
        .store_id = 1,
        .api_url = try alloc.dupe(u8, "http://127.0.0.1:8080"),
        .replica_root_dir = try alloc.dupe(u8, "."),
        .catalog_path = try alloc.dupe(u8, catalog_path),
        .catalog_store = null,
        .backend_runtime = backend_runtime.ptr(),
    };
    defer metadata.deinit();
    try metadata.manager.upsertTable(.{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    });
    try metadata.manager.upsertRange(.{
        .group_id = 70,
        .table_id = 7,
        .start_key = "",
    });

    const Provider = struct {
        fn collect(
            _: *anyopaque,
            provider_alloc: std.mem.Allocator,
            _: []const antfly.metadata.TableRecord,
            _: []const antfly.metadata.RangeRecord,
        ) !antfly.data.runtime.DataServer.LocalSchemaProgressSnapshot {
            const records = try provider_alloc.alloc(antfly.metadata.SchemaProgressRecord, 1);
            records[0] = .{ .table_id = 7, .node_id = 1, .schema_version = 1 };
            return .{ .records = records, .runtime_coverage_complete = true };
        }
    };
    metadata.local_schema_progress_provider = .{
        .ptr = undefined,
        .collect = Provider.collect,
    };

    try metadata.finalizeReadySchemaMigrations();
    const table = metadata.findTableByNameLocked("docs") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("", table.read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "full_text_index_v0") == null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "full_text_index_v1") != null);
}

test "standalone unified server lifecycle propagates startup failure" {
    var lifecycle = UnifiedServerLifecycle.init(std.testing.io);
    lifecycle.publishFailure(error.AddressInUse);
    var cancellation = antfly.common.runtime_lifecycle.CancellationSource{};
    try std.testing.expectError(
        error.AddressInUse,
        lifecycle.waitForStartup(
            antfly.common.runtime_lifecycle.ShutdownDeadline.afterMilliseconds(100),
            cancellation.token(),
        ),
    );
    try std.testing.expectEqual(error.AddressInUse, lifecycle.runtimeFailure().?);
}

test "runtime lease watchdog fetch and validation failures publish no bootstrap capability" {
    inline for ([_]RuntimeLeaseWatchdog.ObservationFailureStage{ .fetch, .validation }) |stage| {
        var runtime_watchdog = RuntimeLeaseWatchdog{
            .watchdog = try antfly.ha.kubernetes_lease_watchdog.Watchdog.init(.{
                .scope = .{
                    .topology_id = "topology-7",
                    .node_id = "primary-a",
                    .data_generation = "initial",
                },
                .grace_ns = 10 * std.time.ns_per_s,
                .sentinel_path = "/tmp/lease-fenced",
            }, null, null),
            .io = std.testing.io,
            .executor = undefined,
            .uri = undefined,
            .token_path = "",
            .lease_name = "topology-ha-fence",
            .lease_namespace = "default",
            .stable_topology_id = "topology-7",
            .node_id = "primary-a",
            .pod_uid = "primary-pod-uid",
            .process_boot_id = [_]u8{'a'} ** 64,
        };
        platform_sync.lockYielding(&runtime_watchdog.proof_mutex);
        const transition = runtime_watchdog.transitionObservationFailureLocked(stage, 1);
        const repeated_transition = runtime_watchdog.transitionObservationFailureLocked(stage, 2);
        runtime_watchdog.proof_mutex.unlock();
        try std.testing.expectEqual(antfly.ha.kubernetes_lease_watchdog.Decision.waiting, transition.decision);
        try std.testing.expect(transition.should_log);
        try std.testing.expectEqual(antfly.ha.kubernetes_lease_watchdog.Decision.waiting, repeated_transition.decision);
        try std.testing.expect(!repeated_transition.should_log);
        try std.testing.expectEqual(stage == .fetch, runtime_watchdog.fetch_failure_logged);
        try std.testing.expectEqual(stage == .validation, runtime_watchdog.validation_failure_logged);

        const proof = (try RuntimeLeaseWatchdog.proofSnapshot(&runtime_watchdog, std.testing.allocator)).?;
        defer std.testing.allocator.free(proof.observed_holder_node_id);
        try std.testing.expect(!proof.active);
        try std.testing.expect(!proof.authority_granted);
        try std.testing.expectEqual(@as(i64, 0), proof.authority_remaining_ms);
        try std.testing.expectEqual(@as(i64, 0), proof.observed_lease_transitions);
        try std.testing.expectEqual(@as(usize, 0), proof.observed_holder_node_id.len);
    }
}

test "runtime lease watchdog retains a bounded Kubernetes response budget" {
    try std.testing.expectEqual(@as(usize, 256 * 1024), ha_lease_max_response_bytes);
}

test "runtime lease watchdog prefers a DNS-verified Kubernetes API host and retains the injected port" {
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("KUBERNETES_SERVICE_HOST", "10.96.0.1");
    try env.put("KUBERNETES_SERVICE_PORT_HTTPS", "443");

    const default_endpoint = try haLeaseAPIEndpoint(&env);
    try std.testing.expectEqualStrings(ha_lease_default_api_host, default_endpoint.host);
    try std.testing.expectEqualStrings("443", default_endpoint.port);

    try env.put(ha_lease_api_host_env, "kubernetes.default.svc.cluster.local");
    const overridden_endpoint = try haLeaseAPIEndpoint(&env);
    try std.testing.expectEqualStrings("kubernetes.default.svc.cluster.local", overridden_endpoint.host);
    try std.testing.expectEqualStrings("443", overridden_endpoint.port);
}
