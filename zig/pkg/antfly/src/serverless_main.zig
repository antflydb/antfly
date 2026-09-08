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
const antfly = @import("cli_root.zig");

const serverless = antfly.serverless;
const serverless_default_max_request_bytes: usize = antfly.public_api.http_server.public_api_max_request_body_bytes;
const serverless_default_max_connection_threads: u32 = 64;
const serverless_default_query_cache_max_bytes: u64 = 4 * 1024 * 1024 * 1024;
const serverless_default_query_cache_payload_max_bytes: u64 = 64 * 1024 * 1024;

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    secret_store_path: ?[]const u8 = null,
    artifacts_uri: ?[]const u8 = null,
    manifests_uri: ?[]const u8 = null,
    wal_uri: ?[]const u8 = null,
    progress_uri: ?[]const u8 = null,
    catalog_uri: ?[]const u8 = null,
    query_cache_dir: ?[]const u8 = null,
    query_cache_max_bytes: ?u64 = null,
    query_cache_payload_max_bytes: ?u64 = null,
    embedding_indexes_json: ?[]const u8 = null,
    sparse_embedding_index_name: ?[]const u8 = null,
    chunk_embedding_index_name: ?[]const u8 = null,
    chunk_embedding_dimensions: ?u32 = null,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_port: ?u16 = null,
    max_request_bytes: ?usize = null,
    max_connection_threads: ?u32 = null,
    role: ?[]const u8 = null,
    tick_ms: ?u64 = null,
    publish_enabled: ?bool = null,
    compaction_enabled: ?bool = null,
    prune_enabled: ?bool = null,
    enrichment_enabled: ?bool = null,
    remote_content_block_private_ips: ?bool = null,
    help: bool = false,
};

pub fn run(
    init: std.process.Init,
    forced_role: ?serverless.RuntimeRole,
    forced_listener: ?bool,
    forced_combined_mode: ?bool,
) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_serverless";
    return try runFromIterator(init, argv0, &args, forced_role, forced_listener, forced_combined_mode);
}

pub fn runFromIterator(
    init: std.process.Init,
    argv0: []const u8,
    args: *std.process.Args.Iterator,
    forced_role: ?serverless.RuntimeRole,
    forced_listener: ?bool,
    forced_combined_mode: ?bool,
) !void {
    const alloc = init.gpa;
    var termination_signals = antfly.common.runtime_lifecycle.ProcessSignalScope.install();
    defer termination_signals.deinit();
    const cli = try parseCli(args);
    if (cli.help) {
        printUsage(argv0);
        return;
    }
    var supervisor = antfly.common.runtime_lifecycle.RuntimeSupervisor.init(30_000);
    defer supervisor.markStopped();

    var secret_store: ?antfly.common.secrets.FileStore = if (cli.secret_store_path orelse init.environ_map.get("ANTFLY_SECRET_STORE_PATH")) |path|
        try antfly.common.secrets.FileStore.init(alloc, path)
    else
        null;
    defer if (secret_store) |*store| store.deinit();
    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |path|
        try antfly.common.config.loadFromPathWithSecretsForDeployment(alloc, path, if (secret_store) |*store| store else null, .serverless)
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();
    antfly.common.config.Config.validateServerTlsConfig(if (loaded_config) |*cfg| cfg.tls else null) catch |err| {
        std.log.err("serverless startup rejected configured tls: built-in server TLS is unsupported; terminate TLS at a trusted reverse proxy", .{});
        return err;
    };
    var configured_uris = try ConfiguredStorageUris.init(
        alloc,
        if (loaded_config) |*cfg| cfg else null,
        if (secret_store) |*store| store else null,
    );
    defer configured_uris.deinit(alloc);
    if (loaded_config != null) try rejectStorageUriOverrides(init.environ_map, cli);

    var remote_content: ?antfly.common.config.Config.RemoteContentConfig = null;
    if (cli.remote_content_block_private_ips orelse try parseEnvOptionalBool(init.environ_map, "ANTFLY_SERVERLESS_REMOTE_CONTENT_BLOCK_PRIVATE_IPS")) |block_private_ips| {
        remote_content = .{
            .security = .{ .block_private_ips = block_private_ips },
        };
    } else if (loaded_config) |*cfg| {
        if (cfg.remote_content) |value| remote_content = value;
    }

    const bootstrap = serverless.BootstrapConfig{
        .artifacts_uri = try resolveRequired(init.environ_map, cli.artifacts_uri, configured_uris.artifacts, "ANTFLY_SERVERLESS_ARTIFACTS_URI"),
        .manifests_uri = try resolveRequired(init.environ_map, cli.manifests_uri, configured_uris.manifests, "ANTFLY_SERVERLESS_MANIFESTS_URI"),
        .wal_uri = try resolveRequired(init.environ_map, cli.wal_uri, configured_uris.wal, "ANTFLY_SERVERLESS_WAL_URI"),
        .progress_uri = try resolveRequired(init.environ_map, cli.progress_uri, configured_uris.progress, "ANTFLY_SERVERLESS_PROGRESS_URI"),
        .catalog_uri = try resolveRequired(init.environ_map, cli.catalog_uri, configured_uris.catalog, "ANTFLY_SERVERLESS_CATALOG_URI"),
        .s3_options = configured_uris.s3_options,
        .gcs_options = configured_uris.gcs_options,
        .query_cache_dir = cli.query_cache_dir orelse init.environ_map.get("ANTFLY_SERVERLESS_QUERY_CACHE_DIR"),
        .query_cache_max_bytes = cli.query_cache_max_bytes orelse try parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES", serverless_default_query_cache_max_bytes),
        .query_cache_payload_max_bytes = cli.query_cache_payload_max_bytes orelse try parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES", serverless_default_query_cache_payload_max_bytes),
        .embedding_indexes_json = cli.embedding_indexes_json orelse init.environ_map.get("ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON"),
        .sparse_embedding_index_name = cli.sparse_embedding_index_name orelse init.environ_map.get("ANTFLY_SERVERLESS_SPARSE_EMBEDDING_INDEX_NAME") orelse "serverless_sparse",
        .chunk_embedding_index_name = cli.chunk_embedding_index_name orelse init.environ_map.get("ANTFLY_SERVERLESS_CHUNK_EMBEDDING_INDEX_NAME") orelse "serverless_chunk",
        .chunk_embedding_dimensions = cli.chunk_embedding_dimensions orelse try parseEnvIntOrDefault(init.environ_map, u32, "ANTFLY_SERVERLESS_CHUNK_EMBEDDING_DIMS", 8),
        .tick_interval_ms = cli.tick_ms orelse try parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_TICK_INTERVAL_MS", 25),
        .role = forced_role orelse try parseRuntimeRole(cli.role orelse init.environ_map.get("ANTFLY_SERVERLESS_ROLE") orelse "combined"),
        .combined_mode = forced_combined_mode orelse false,
        .publish_enabled = cli.publish_enabled orelse try parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_PUBLISH_ENABLED", true),
        .compaction_enabled = cli.compaction_enabled orelse try parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_COMPACTION_ENABLED", true),
        .prune_enabled = cli.prune_enabled orelse try parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_PRUNE_ENABLED", true),
        .enrichment_enabled = cli.enrichment_enabled orelse try parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_ENRICHMENT_ENABLED", true),
        .remote_content = if (remote_content) |*cfg| cfg else null,
        .query_max_concurrent_requests = if (loaded_config) |*cfg| cfg.admission.query.max_concurrent_requests else antfly.common.config.default_query_max_concurrent_requests,
        .graph_execution_limits = if (loaded_config) |*cfg| cfg.graph_execution else .{},
        .write_max_concurrent_requests = if (loaded_config) |*cfg| cfg.admission.write.max_concurrent_requests else antfly.common.config.default_write_max_concurrent_requests,
    };
    const listener_enabled = forced_listener orelse listenerEnabledForRole(bootstrap.role);
    const listener = if (listener_enabled) try serverless_serverConfigFromEnv(init.environ_map, cli) else null;

    var srv = serverless.ServerlessServer.init(alloc, init.io, .{
        .bootstrap = bootstrap,
        .listener = listener,
    }) catch |err| {
        reportStartupError(err);
        return err;
    };
    defer srv.deinitWithDeadline(supervisor.deadline());
    srv.start() catch |err| {
        reportStartupError(err);
        return err;
    };
    defer srv.stopWithDeadline(supervisor.deadline());

    if (listener_enabled) {
        const base_uri = try srv.baseUri(alloc);
        defer alloc.free(base_uri);
        std.debug.print("serverless listening on {s}\n", .{base_uri});
    } else {
        std.debug.print("serverless maintenance runtime started without listener\n", .{});
    }
    printRuntimeStatusSummary(srv.runtimeStatus());

    var health_source = ServerlessHealthSource{ .srv = &srv, .supervisor = &supervisor };
    const health_port = cli.health_port orelse try parseEnvOptionalInt(init.environ_map, u16, "ANTFLY_SERVERLESS_HEALTH_PORT");
    const health_bind_host = cli.bind_host orelse init.environ_map.get("ANTFLY_SERVERLESS_BIND_HOST") orelse "127.0.0.1";
    const health_server = try antfly.common.health_server.HealthServer.startIfConfiguredOnHostWithRuntime(
        alloc,
        init.io,
        "serverless",
        health_bind_host,
        health_port,
        health_source.readiness(),
        health_source.metricsWriter(),
        srv.httpRuntime(),
    );
    defer if (health_server) |hs| hs.deinitWithDeadline(supervisor.deadline());

    try supervisor.publishReady();
    while (!supervisor.shouldStop(termination_signals.cancellationRequested())) {
        if (srv.listenerFailure()) |err| return supervisor.fail("serverless", "public-http", err);
        if (health_server) |hs| if (hs.runtimeFailure()) |err| return supervisor.fail("health", "http", err);
        // A short interruptible wait bounds graceful termination latency even
        // on platforms whose clock sleep is automatically restarted.
        sleepMs(init.io, 250);
    }
}

const ServerlessHealthSource = struct {
    srv: *serverless.ServerlessServer,
    supervisor: *const antfly.common.runtime_lifecycle.RuntimeSupervisor,

    fn readiness(self: *ServerlessHealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    fn metricsWriter(self: *ServerlessHealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *ServerlessHealthSource = @ptrCast(@alignCast(ptr));
        return self.supervisor.currentState() == .ready and self.srv.runtimeStatus().validated;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *ServerlessHealthSource = @ptrCast(@alignCast(ptr));
        try antfly.common.prometheus.appendPromMetric(writer, "antfly_runtime_supervisor_state", "gauge", "Runtime supervisor phase (0 starting, 1 ready, 2 quiescing, 3 failed, 4 stopped)", @intFromEnum(self.supervisor.currentState()));
        try antfly.common.prometheus.appendPromMetric(writer, "antfly_runtime_supervisor_cancelled", "gauge", "Whether process-level runtime cancellation has been requested", @intFromBool(self.supervisor.token().isCancelled()));
        const run_stats = self.srv.stack.runtime.metricsSnapshot();
        const query_metrics = self.srv.stack.query.metricsSnapshot();
        try writeServerlessPrometheus(writer, run_stats, query_metrics);
    }
};

fn writeServerlessPrometheus(writer: *std.Io.Writer, run_stats: anytype, query_metrics: anytype) !void {
    const append = antfly.common.prometheus.appendPromMetric;

    try append(writer, "antfly_serverless_published_namespaces_total", "counter", "Namespaces published by the maintenance runtime", @intCast(run_stats.published_namespaces));
    try append(writer, "antfly_serverless_compacted_namespaces_total", "counter", "Namespaces compacted by the maintenance runtime", @intCast(run_stats.compacted_namespaces));
    try append(writer, "antfly_serverless_pruned_namespaces_total", "counter", "Namespaces pruned by the maintenance runtime", @intCast(run_stats.pruned_namespaces));
    try append(writer, "antfly_serverless_deleted_versions_total", "counter", "Manifest versions deleted by pruning", @intCast(run_stats.deleted_versions));
    try append(writer, "antfly_serverless_enriched_documents_total", "counter", "Documents successfully enriched", @intCast(run_stats.enriched_documents));
    try append(writer, "antfly_serverless_enrichment_failed_documents_total", "counter", "Documents for which enrichment failed", @intCast(run_stats.enrichment_failed_documents));
    try append(writer, "antfly_serverless_queries_total", "counter", "Total query executions", query_metrics.total_queries);
    try append(writer, "antfly_serverless_vector_queries_total", "counter", "Vector query executions", query_metrics.vector_queries);
    try append(writer, "antfly_serverless_hybrid_queries_total", "counter", "Hybrid query executions", query_metrics.hybrid_queries);
    try append(writer, "antfly_serverless_sparse_queries_total", "counter", "Sparse query executions", query_metrics.sparse_queries);
    try antfly.db.query_metrics.writePrometheus(writer);
    try antfly.db.enrichment_utf8_text.writePrometheus(writer);
}

fn parseEnvOptionalInt(
    env_map: *std.process.Environ.Map,
    comptime T: type,
    env_name: []const u8,
) !?T {
    const raw = env_map.get(env_name) orelse return null;
    return std.fmt.parseInt(T, raw, 10) catch return invalidEnvironmentValue(env_name, raw, "an unsigned base-10 integer");
}

fn parseCli(args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--config")) {
            cfg.config_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            cfg.secret_store_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--artifacts-uri")) {
            cfg.artifacts_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--manifests-uri")) {
            cfg.manifests_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--wal-uri")) {
            cfg.wal_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--progress-uri")) {
            cfg.progress_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--catalog-uri")) {
            cfg.catalog_uri = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--query-cache-dir")) {
            cfg.query_cache_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--query-cache-max-bytes")) {
            cfg.query_cache_max_bytes = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--query-cache-payload-max-bytes")) {
            cfg.query_cache_payload_max_bytes = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--embedding-indexes-json")) {
            cfg.embedding_indexes_json = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--sparse-embedding-index-name")) {
            cfg.sparse_embedding_index_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--chunk-embedding-index-name")) {
            cfg.chunk_embedding_index_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--chunk-embedding-dims")) {
            cfg.chunk_embedding_dimensions = try std.fmt.parseInt(u32, args.next() orelse return error.InvalidArguments, 10);
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
        if (std.mem.eql(u8, arg, "--max-request-bytes")) {
            cfg.max_request_bytes = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--max-connection-threads")) {
            cfg.max_connection_threads = try std.fmt.parseInt(u32, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--role")) {
            cfg.role = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--tick-ms")) {
            cfg.tick_ms = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--publish-enabled")) {
            cfg.publish_enabled = try parseBoolArg(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--compaction-enabled")) {
            cfg.compaction_enabled = try parseBoolArg(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--prune-enabled")) {
            cfg.prune_enabled = try parseBoolArg(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--enrichment-enabled")) {
            cfg.enrichment_enabled = try parseBoolArg(args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--remote-content-block-private-ips")) {
            cfg.remote_content_block_private_ips = try parseBoolArg(args.next() orelse return error.InvalidArguments);
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
}

fn resolveRequired(
    env_map: *std.process.Environ.Map,
    cli_value: ?[]const u8,
    config_value: ?[]const u8,
    env_name: []const u8,
) ![]const u8 {
    if (cli_value) |value| return value;
    if (env_map.get(env_name)) |value| return value;
    return config_value orelse {
        std.debug.print("missing required config: {s}\n", .{env_name});
        return error.MissingConfiguration;
    };
}

fn rejectStorageUriOverrides(env_map: *std.process.Environ.Map, cli: CliConfig) !void {
    if (cli.artifacts_uri != null or env_map.get("ANTFLY_SERVERLESS_ARTIFACTS_URI") != null or
        cli.manifests_uri != null or env_map.get("ANTFLY_SERVERLESS_MANIFESTS_URI") != null or
        cli.wal_uri != null or env_map.get("ANTFLY_SERVERLESS_WAL_URI") != null or
        cli.progress_uri != null or env_map.get("ANTFLY_SERVERLESS_PROGRESS_URI") != null or
        cli.catalog_uri != null or env_map.get("ANTFLY_SERVERLESS_CATALOG_URI") != null)
    {
        std.debug.print("serverless storage URI overrides cannot be combined with --config; select per-lane connection, bucket, and prefix under storage.object.lanes\n", .{});
        return error.ConflictingStorageConfiguration;
    }
}

const ConfiguredStorageUris = struct {
    artifacts: ?[]u8 = null,
    manifests: ?[]u8 = null,
    wal: ?[]u8 = null,
    progress: ?[]u8 = null,
    catalog: ?[]u8 = null,
    s3_options: [5]?serverless.BootstrapConfig.S3Options = .{ null, null, null, null, null },
    gcs_options: [5]?serverless.BootstrapConfig.GcsOptions = .{ null, null, null, null, null },
    resolved_credentials: [5]?antfly.common.config.Config.ResolvedExternalIoCredentials = .{ null, null, null, null, null },

    fn init(
        alloc: std.mem.Allocator,
        cfg: ?*const antfly.common.config.Config,
        secret_store: ?*antfly.common.secrets.FileStore,
    ) !ConfiguredStorageUris {
        const config = cfg orelse return .{};
        if (config.deployment_mode != .serverless or config.storage.engine != .object) return error.InvalidServerlessStorageConfig;
        const connection = config.storage.object_connection orelse return error.InvalidServerlessStorageConfig;
        const bucket = config.storage.object_bucket orelse return error.InvalidServerlessStorageConfig;
        const prefix = config.storage.object_prefix orelse "";
        var out: ConfiguredStorageUris = .{};
        errdefer out.deinit(alloc);
        const lanes = [_]antfly.common.config.Config.ObjectStorageLocation{
            config.storage.object_lanes.artifacts,
            config.storage.object_lanes.manifests,
            config.storage.object_lanes.wal,
            config.storage.object_lanes.progress,
            config.storage.object_lanes.catalog,
        };
        const names = [_][]const u8{ "artifacts", "manifests", "wal", "progress", "catalog" };
        const uris = [_]*?[]u8{ &out.artifacts, &out.manifests, &out.wal, &out.progress, &out.catalog };
        for (lanes, names, 0..) |lane, name, index| {
            const lane_connection = lane.connection orelse connection;
            const lane_bucket = lane.bucket orelse bucket;
            const external = try storageExternalIo(config, lane_connection);
            const scheme: []const u8 = switch (external.protocol) {
                .s3 => "s3",
                .gcs => "gs",
                else => return error.InvalidServerlessStorageConfig,
            };
            uris[index].* = if (lane.prefix) |lane_prefix|
                try objectUriAlloc(alloc, scheme, lane_bucket, lane_prefix)
            else
                try objectLaneUriAlloc(alloc, scheme, lane_bucket, prefix, name);
            switch (external.protocol) {
                .s3 => out.s3_options[index] = try storageS3Options(alloc, external, secret_store, &out.resolved_credentials[index]),
                .gcs => out.gcs_options[index] = try storageGcsOptions(alloc, external, secret_store, &out.resolved_credentials[index]),
                else => unreachable,
            }
        }
        return out;
    }

    fn deinit(self: *ConfiguredStorageUris, alloc: std.mem.Allocator) void {
        if (self.artifacts) |value| alloc.free(value);
        if (self.manifests) |value| alloc.free(value);
        if (self.wal) |value| alloc.free(value);
        if (self.progress) |value| alloc.free(value);
        if (self.catalog) |value| alloc.free(value);
        for (&self.resolved_credentials) |*maybe_credentials| {
            if (maybe_credentials.*) |*credentials| credentials.deinit(alloc);
        }
        self.* = .{};
    }
};

fn storageExternalIo(
    config: *const antfly.common.config.Config,
    connection_id: []const u8,
) !antfly.common.config.Config.ExternalIoConnectionConfig {
    const connection = config.connections.get(connection_id) orelse return error.InvalidServerlessStorageConfig;
    if (connection.kind != .external_io) return error.InvalidServerlessStorageConfig;
    return connection.external_io orelse return error.InvalidServerlessStorageConfig;
}

fn storageS3Options(
    alloc: std.mem.Allocator,
    configured_external: antfly.common.config.Config.ExternalIoConnectionConfig,
    secret_store: ?*antfly.common.secrets.FileStore,
    resolved_credentials: *?antfly.common.config.Config.ResolvedExternalIoCredentials,
) !serverless.BootstrapConfig.S3Options {
    if (configured_external.protocol != .s3) return error.InvalidServerlessStorageConfig;
    resolved_credentials.* = try antfly.common.config.Config.resolveExternalIoCredentials(alloc, configured_external, secret_store);
    const external = resolved_credentials.*.?.apply(configured_external);
    var options = serverless.BootstrapConfig.S3Options{
        .endpoint = external.endpoint,
        .region = external.region,
        .use_ssl = external.use_ssl orelse true,
        .addressing_style = switch (external.addressing_style) {
            .path => .path,
            .virtual_hosted => .virtual_hosted,
        },
        .create_bucket = external.bucket_provisioning == .create_if_missing,
    };
    switch (external.credentials.source) {
        .default => options.credential_source = .default,
        .static => {
            options.access_key_id = external.credentials.access_key_id;
            options.secret_access_key = external.credentials.secret_access_key;
            options.session_token = external.credentials.session_token;
        },
        .profile => options.credential_source = .{ .profile = .{
            .name = external.credentials.profile.?,
            .shared_credentials_file = external.credentials.shared_credentials_file,
        } },
        .web_identity => options.credential_source = .{ .web_identity = .{
            .role_arn = external.credentials.role_arn.?,
            .token_file = external.credentials.token_file.?,
            .session_name = external.credentials.session_name orelse "antfly-serverless",
            .sts_endpoint = external.credentials.sts_endpoint,
        } },
    }
    return options;
}

fn storageGcsOptions(
    alloc: std.mem.Allocator,
    configured_external: antfly.common.config.Config.ExternalIoConnectionConfig,
    secret_store: ?*antfly.common.secrets.FileStore,
    resolved_credentials: *?antfly.common.config.Config.ResolvedExternalIoCredentials,
) !serverless.BootstrapConfig.GcsOptions {
    if (configured_external.protocol != .gcs) return error.InvalidServerlessStorageConfig;
    resolved_credentials.* = try antfly.common.config.Config.resolveExternalIoCredentials(alloc, configured_external, secret_store);
    const external = resolved_credentials.*.?.apply(configured_external);
    return .{
        .endpoint = external.endpoint,
        .upload_endpoint = external.upload_endpoint,
        .project_id = external.project_id,
        .credential_source = switch (external.gcs_credentials.source) {
            .default => .default,
            .bearer_token => .bearer_token,
            .service_account => .service_account,
        },
        .bearer_token = external.gcs_credentials.bearer_token,
        .service_account_json = external.gcs_credentials.service_account_json,
        .credentials_path = external.gcs_credentials.credentials_path,
        .scope = external.gcs_credentials.scope,
        .create_bucket = external.bucket_provisioning == .create_if_missing,
    };
}

fn objectUriAlloc(alloc: std.mem.Allocator, scheme: []const u8, bucket: []const u8, raw_prefix: []const u8) ![]u8 {
    const prefix = std.mem.trim(u8, raw_prefix, "/");
    return if (prefix.len == 0)
        try std.fmt.allocPrint(alloc, "{s}://{s}", .{ scheme, bucket })
    else
        try std.fmt.allocPrint(alloc, "{s}://{s}/{s}", .{ scheme, bucket, prefix });
}

fn objectLaneUriAlloc(alloc: std.mem.Allocator, scheme: []const u8, bucket: []const u8, raw_prefix: []const u8, lane: []const u8) ![]u8 {
    const prefix = std.mem.trim(u8, raw_prefix, "/");
    return if (prefix.len == 0)
        try std.fmt.allocPrint(alloc, "{s}://{s}/{s}", .{ scheme, bucket, lane })
    else
        try std.fmt.allocPrint(alloc, "{s}://{s}/{s}/{s}", .{ scheme, bucket, prefix, lane });
}

fn parseEnvIntOrDefault(
    env_map: *std.process.Environ.Map,
    comptime T: type,
    env_name: []const u8,
    default: T,
) !T {
    const raw = env_map.get(env_name) orelse return default;
    return std.fmt.parseInt(T, raw, 10) catch return invalidEnvironmentValue(env_name, raw, "an unsigned base-10 integer");
}

fn parseEnvBoolOrDefault(
    env_map: *std.process.Environ.Map,
    env_name: []const u8,
    default: bool,
) !bool {
    const raw = env_map.get(env_name) orelse return default;
    return parseBool(raw) catch return invalidEnvironmentValue(env_name, raw, "true/false, yes/no, or 1/0");
}

fn parseEnvOptionalBool(
    env_map: *std.process.Environ.Map,
    env_name: []const u8,
) !?bool {
    const raw = env_map.get(env_name) orelse return null;
    return parseBool(raw) catch return invalidEnvironmentValue(env_name, raw, "true/false, yes/no, or 1/0");
}

fn invalidEnvironmentValue(name: []const u8, value: []const u8, expected: []const u8) error{InvalidEnvironmentValue} {
    std.log.err("invalid environment variable {s}={s}; expected {s}", .{ name, value, expected });
    return error.InvalidEnvironmentValue;
}

fn serverless_serverConfigFromEnv(
    env_map: *std.process.Environ.Map,
    cli: CliConfig,
) !serverless.ListenerConfig {
    return .{
        .bind_host = cli.bind_host orelse env_map.get("ANTFLY_SERVERLESS_BIND_HOST") orelse "127.0.0.1",
        .bind_port = cli.bind_port orelse try parseEnvIntOrDefault(env_map, u16, "ANTFLY_SERVERLESS_BIND_PORT", 8080),
        .max_request_bytes = cli.max_request_bytes orelse try parseEnvIntOrDefault(
            env_map,
            usize,
            "ANTFLY_SERVERLESS_MAX_REQUEST_BYTES",
            serverless_default_max_request_bytes,
        ),
        .max_connections = cli.max_connection_threads orelse try parseEnvIntOrDefault(
            env_map,
            u32,
            "ANTFLY_SERVERLESS_MAX_CONNECTION_THREADS",
            serverless_default_max_connection_threads,
        ),
    };
}

fn listenerEnabledForRole(role: serverless.RuntimeRole) bool {
    return switch (role) {
        .combined, .api_only, .query_only => true,
        .maintenance_only => false,
    };
}

fn parseRuntimeRole(raw: []const u8) !serverless.RuntimeRole {
    if (std.mem.eql(u8, raw, "combined")) return .combined;
    if (std.mem.eql(u8, raw, "api") or std.mem.eql(u8, raw, "api_only")) return .api_only;
    if (std.mem.eql(u8, raw, "query") or std.mem.eql(u8, raw, "query_only")) return .query_only;
    if (std.mem.eql(u8, raw, "maintenance") or std.mem.eql(u8, raw, "maintenance_only")) return .maintenance_only;
    return error.InvalidRuntimeRole;
}

fn parseBoolArg(raw: []const u8) !bool {
    return parseBool(raw);
}

fn parseBool(raw: []const u8) !bool {
    if (std.mem.eql(u8, raw, "1") or std.ascii.eqlIgnoreCase(raw, "true") or std.ascii.eqlIgnoreCase(raw, "yes")) return true;
    if (std.mem.eql(u8, raw, "0") or std.ascii.eqlIgnoreCase(raw, "false") or std.ascii.eqlIgnoreCase(raw, "no")) return false;
    return error.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} [options]
        \\
        \\options:
        \\  --config <path>                 JSON common config with deployment_mode=serverless and storage.engine=object
        \\  --secret-store-path <path>      Resolve ${{secret:...}} references from this protected JSON store
        \\  --artifacts-uri <uri>
        \\  --manifests-uri <uri>
        \\  --wal-uri <uri>
        \\  --progress-uri <uri>
        \\  --catalog-uri <uri>
        \\  --query-cache-dir <path>
        \\  --query-cache-max-bytes <bytes>
        \\  --query-cache-payload-max-bytes <bytes>
        \\  --host <host>
        \\  --port <port>
        \\  --health-port <port>
        \\  --max-request-bytes <bytes>
        \\  --max-connection-threads <count>
        \\  --role <combined|api|query|maintenance>
        \\  --tick-ms <milliseconds>
        \\  --publish-enabled <true|false>
        \\  --compaction-enabled <true|false>
        \\  --prune-enabled <true|false>
        \\  --enrichment-enabled <true|false>
        \\  --remote-content-block-private-ips <true|false>
        \\  --help
        \\
        \\supported uri schemes:
        \\  file://...
        \\  s3://bucket/prefix
        \\  gs://bucket/prefix
        \\
        \\environment:
        \\  ANTFLY_SERVERLESS_ARTIFACTS_URI
        \\  ANTFLY_SERVERLESS_MANIFESTS_URI
        \\  ANTFLY_SERVERLESS_WAL_URI
        \\  ANTFLY_SERVERLESS_PROGRESS_URI
        \\  ANTFLY_SERVERLESS_CATALOG_URI
        \\  ANTFLY_SECRET_STORE_PATH
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_DIR
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES default: 4294967296
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES default: 67108864
        \\  ANTFLY_SERVERLESS_BIND_HOST      default: 127.0.0.1
        \\  ANTFLY_SERVERLESS_BIND_PORT      default: 8080
        \\  ANTFLY_SERVERLESS_HEALTH_PORT    default: unset (disables dedicated health server)
        \\  ANTFLY_SERVERLESS_MAX_REQUEST_BYTES default: 33554432
        \\  ANTFLY_SERVERLESS_MAX_CONNECTION_THREADS default: 64 (0 unbounded)
        \\  ANTFLY_SERVERLESS_ROLE           default: combined
        \\  ANTFLY_SERVERLESS_TICK_INTERVAL_MS default: 25
        \\  ANTFLY_SERVERLESS_PUBLISH_ENABLED default: true
        \\  ANTFLY_SERVERLESS_COMPACTION_ENABLED default: true
        \\  ANTFLY_SERVERLESS_PRUNE_ENABLED default: true
        \\  ANTFLY_SERVERLESS_ENRICHMENT_ENABLED default: true
        \\  ANTFLY_SERVERLESS_REMOTE_CONTENT_BLOCK_PRIVATE_IPS default: unset
        \\
    ,
        .{argv0},
    );
}

fn reportStartupError(err: anyerror) void {
    if (startupErrorHint(err)) |hint| {
        std.debug.print("serverless startup failed: {s}\n", .{hint});
    }
}

fn startupErrorHint(err: anyerror) ?[]const u8 {
    return switch (err) {
        error.UnsupportedRemoteUri => "unsupported storage URI scheme; expected file://..., s3://bucket/prefix, or gs://bucket/prefix",
        error.InvalidRemoteUri => "invalid storage URI; expected a non-empty path or bucket/prefix",
        error.InvalidTickInterval => "invalid tick interval; ANTFLY_SERVERLESS_TICK_INTERVAL_MS must be greater than zero",
        error.InvalidQueryCacheDir => "invalid query cache dir; ANTFLY_SERVERLESS_QUERY_CACHE_DIR must be non-empty when set",
        error.InvalidQueryCacheBudget => "invalid query cache budget; ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES must be greater than zero when the cache is enabled",
        error.InvalidQueryCachePayloadBudget => "invalid query cache payload budget; ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES must be greater than zero when the cache is enabled",
        error.QueryCachePayloadExceedsBudget => "invalid query cache budgets; the per-payload limit cannot exceed the total cache limit",
        error.InvalidRuntimeRole => "invalid runtime role; expected combined, api, query, or maintenance",
        error.InvalidEnvironmentValue => "invalid serverless environment configuration; see the preceding variable-specific error",
        error.MissingEndpoint => "missing S3-compatible endpoint; configure the storage connection endpoint or AWS_ENDPOINT_URL",
        error.MissingAccessKeyId => "missing S3 access key; configure the storage connection secret or AWS_ACCESS_KEY_ID",
        error.MissingSecretAccessKey => "missing S3 secret key; configure the storage connection secret or AWS_SECRET_ACCESS_KEY",
        error.BucketNotFound => "configured object-storage bucket does not exist; provision it or set bucket_provisioning=create_if_missing for development",
        error.MissingServiceAccount => "missing GCS auth; set GCS_BEARER_TOKEN, GOOGLE_OAUTH_ACCESS_TOKEN, GOOGLE_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS for gs:// backends",
        error.MissingProjectId => "missing GCS project id; set GOOGLE_CLOUD_PROJECT or GCLOUD_PROJECT, or use a service account that includes project_id",
        else => null,
    };
}

fn sleepMs(io: std.Io, ms: u64) void {
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(if (ms == 0) @as(u64, 1) else ms)),
    }, io) catch {};
}

fn printRuntimeStatusSummary(status: *const serverless.ServerlessRuntimeStatus) void {
    std.debug.print("serverless bootstrap role={s} combined_mode={any} validated={any} tick_ms={d}\n", .{
        @tagName(status.role),
        status.combined_mode,
        status.validated,
        status.tick_interval_ms,
    });
    std.debug.print("  maintenance: publish={any} compact={any} prune={any} enrich={any}\n", .{
        status.publish_enabled,
        status.compaction_enabled,
        status.prune_enabled,
        status.enrichment_enabled,
    });
    for (status.targets) |target| {
        std.debug.print("  {s}: {s} ({s})\n", .{
            target.lane,
            backendSummary(target),
            target.uri,
        });
    }
}

fn backendSummary(target: serverless.RuntimeStorageTarget) []const u8 {
    return switch (target.backend) {
        .file => target.path orelse "file",
        .s3, .gs => target.prefix orelse target.bucket orelse target.uri,
    };
}

test "serverless main module compiles" {
    _ = CliConfig;
}

test "serverless metrics include shared enrichment utf8 repair counter" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try writeServerlessPrometheus(
        &writer.writer,
        .{
            .published_namespaces = 0,
            .compacted_namespaces = 0,
            .pruned_namespaces = 0,
            .deleted_versions = 0,
            .enriched_documents = 0,
            .enrichment_failed_documents = 0,
        },
        .{
            .total_queries = 0,
            .vector_queries = 0,
            .hybrid_queries = 0,
            .sparse_queries = 0,
        },
    );

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "# TYPE antfly_enrichment_invalid_utf8_repairs_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "antfly_enrichment_invalid_utf8_repairs_total ") != null);
}

test "serverless main startup hint covers backend config errors" {
    try std.testing.expectEqualStrings(
        "missing S3-compatible endpoint; configure the storage connection endpoint or AWS_ENDPOINT_URL",
        startupErrorHint(error.MissingEndpoint).?,
    );
    try std.testing.expectEqualStrings(
        "missing GCS auth; set GCS_BEARER_TOKEN, GOOGLE_OAUTH_ACCESS_TOKEN, GOOGLE_SERVICE_ACCOUNT_JSON, or GOOGLE_APPLICATION_CREDENTIALS for gs:// backends",
        startupErrorHint(error.MissingServiceAccount).?,
    );
    try std.testing.expectEqualStrings(
        "invalid runtime role; expected combined, api, query, or maintenance",
        startupErrorHint(error.InvalidRuntimeRole).?,
    );
    try std.testing.expect(startupErrorHint(error.UnexpectedHttpStatus) == null);
}

test "serverless main parses runtime roles" {
    try std.testing.expectEqual(serverless.RuntimeRole.combined, try parseRuntimeRole("combined"));
    try std.testing.expectEqual(serverless.RuntimeRole.api_only, try parseRuntimeRole("api"));
    try std.testing.expectEqual(serverless.RuntimeRole.query_only, try parseRuntimeRole("query"));
    try std.testing.expectEqual(serverless.RuntimeRole.maintenance_only, try parseRuntimeRole("maintenance_only"));
}

test "serverless main enables listener only for query-serving roles" {
    try std.testing.expect(listenerEnabledForRole(.combined));
    try std.testing.expect(listenerEnabledForRole(.api_only));
    try std.testing.expect(listenerEnabledForRole(.query_only));
    try std.testing.expect(!listenerEnabledForRole(.maintenance_only));
}

test "serverless main listener config defaults request limit to public API limit" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();

    const cfg = try serverless_serverConfigFromEnv(&env_map, .{});
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_request_bytes);
    try std.testing.expectEqual(serverless_default_max_connection_threads, cfg.max_connections);
}

test "serverless main listener config allows env and cli listener limit overrides" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();
    try env_map.put("ANTFLY_SERVERLESS_MAX_REQUEST_BYTES", "4194304");
    try env_map.put("ANTFLY_SERVERLESS_MAX_CONNECTION_THREADS", "7");

    const env_cfg = try serverless_serverConfigFromEnv(&env_map, .{});
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), env_cfg.max_request_bytes);
    try std.testing.expectEqual(@as(u32, 7), env_cfg.max_connections);

    const cli_cfg = try serverless_serverConfigFromEnv(&env_map, .{
        .max_request_bytes = 8 * 1024 * 1024,
        .max_connection_threads = 11,
    });
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), cli_cfg.max_request_bytes);
    try std.testing.expectEqual(@as(u32, 11), cli_cfg.max_connections);
}

test "serverless main rejects malformed explicit environment values" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();
    try env_map.put("ANTFLY_SERVERLESS_MAX_CONNECTION_THREADS", "many");
    try std.testing.expectError(error.InvalidEnvironmentValue, serverless_serverConfigFromEnv(&env_map, .{}));

    _ = env_map.remove("ANTFLY_SERVERLESS_MAX_CONNECTION_THREADS");
    try env_map.put("ANTFLY_SERVERLESS_PRUNE_ENABLED", "sometimes");
    try std.testing.expectError(
        error.InvalidEnvironmentValue,
        parseEnvBoolOrDefault(&env_map, "ANTFLY_SERVERLESS_PRUNE_ENABLED", true),
    );
}

test "serverless main parses maintenance booleans" {
    try std.testing.expect(try parseBoolArg("true"));
    try std.testing.expect(try parseBoolArg("1"));
    try std.testing.expect(!(try parseBoolArg("false")));
    try std.testing.expectError(error.InvalidArguments, parseBoolArg("maybe"));
}

test "serverless main rejects location-only overrides with connection config" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();
    try std.testing.expectError(error.ConflictingStorageConfiguration, rejectStorageUriOverrides(&env_map, .{ .wal_uri = "s3://other/wal" }));
    try env_map.put("ANTFLY_SERVERLESS_CATALOG_URI", "s3://other/catalog");
    try std.testing.expectError(error.ConflictingStorageConfiguration, rejectStorageUriOverrides(&env_map, .{}));
}

test "serverless main backend summary prefers parsed location" {
    try std.testing.expectEqualStrings("artifacts/dev", backendSummary(.{
        .lane = @constCast("artifacts"),
        .uri = @constCast("s3://bucket/artifacts/dev"),
        .backend = .s3,
        .bucket = @constCast("bucket"),
        .prefix = @constCast("artifacts/dev"),
    }));
    try std.testing.expectEqualStrings("/tmp/antfly-artifacts", backendSummary(.{
        .lane = @constCast("artifacts"),
        .uri = @constCast("file:///tmp/antfly-artifacts"),
        .backend = .file,
        .path = @constCast("/tmp/antfly-artifacts"),
    }));
}

test "serverless main derives multi-bucket lanes and per-connection credentials" {
    const alloc = std.testing.allocator;
    const store_path = ".zig-cache/test-serverless-storage-secrets.json";
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};
    }
    var secret_store = try antfly.common.secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();
    inline for (.{
        .{ "data.key", "data-key" },
        .{ "data.secret", "data-secret" },
        .{ "wal.key", "wal-key" },
        .{ "wal.secret", "wal-secret" },
    }) |entry| {
        var stored = try secret_store.put(alloc, entry[0], entry[1]);
        stored.deinit(alloc);
    }
    var cfg = try antfly.common.config.Config.parseFromSliceWithSecrets(alloc,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "region": "us-west-2", "buckets": ["data-bucket"], "credentials": { "source": "static", "access_key_id": "${secret:data.key}", "secret_access_key": "${secret:data.secret}" } } },
        \\    "wal": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "s3", "endpoint": "minio:9000", "use_ssl": false, "buckets": ["wal-bucket"], "credentials": { "source": "static", "access_key_id": "${secret:wal.key}", "secret_access_key": "${secret:wal.secret}" } } }
        \\  },
        \\  "storage": { "engine": "object", "object": { "connection": "data", "bucket": "data-bucket", "prefix": "/prod/", "lanes": { "wal": { "connection": "wal", "bucket": "wal-bucket", "prefix": "/durable/" } } } }
        \\}
    , &secret_store);
    defer cfg.deinit();
    var configured = try ConfiguredStorageUris.init(alloc, &cfg, &secret_store);
    defer configured.deinit(alloc);
    try std.testing.expectEqualStrings("s3://data-bucket/prod/artifacts", configured.artifacts.?);
    try std.testing.expectEqualStrings("s3://wal-bucket/durable", configured.wal.?);
    try std.testing.expectEqualStrings("data-key", configured.s3_options[0].?.access_key_id.?);
    try std.testing.expectEqualStrings("wal-key", configured.s3_options[2].?.access_key_id.?);
    try std.testing.expect(!configured.s3_options[2].?.use_ssl);
}

test "serverless main derives GCS lanes and configured credentials" {
    const alloc = std.testing.allocator;
    var cfg = try antfly.common.config.Config.parseFromSlice(alloc,
        \\{
        \\  "deployment_mode": "serverless",
        \\  "connections": {
        \\    "gcs-data": { "kind": "external_io", "capabilities": ["storage.primary"], "external_io": { "protocol": "gcs", "project_id": "antfly-prod", "endpoint": "https://storage.example/v1", "upload_endpoint": "https://storage.example/upload/v1", "buckets": ["data-bucket"], "credentials": { "source": "bearer_token", "bearer_token": "gcs-token" } } }
        \\  },
        \\  "storage": { "engine": "object", "object": { "connection": "gcs-data", "bucket": "data-bucket", "prefix": "/prod/" } }
        \\}
    );
    defer cfg.deinit();
    var configured = try ConfiguredStorageUris.init(alloc, &cfg, null);
    defer configured.deinit(alloc);

    try std.testing.expectEqualStrings("gs://data-bucket/prod/artifacts", configured.artifacts.?);
    try std.testing.expect(configured.s3_options[0] == null);
    try std.testing.expectEqual(.bearer_token, configured.gcs_options[0].?.credential_source);
    try std.testing.expectEqualStrings("gcs-token", configured.gcs_options[0].?.bearer_token.?);
    try std.testing.expectEqualStrings("antfly-prod", configured.gcs_options[0].?.project_id.?);
    try std.testing.expectEqualStrings("https://storage.example/v1", configured.gcs_options[0].?.endpoint.?);
}
