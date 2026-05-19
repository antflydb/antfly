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

const serverless = antfly.serverless;
const serverless_default_max_request_bytes: usize = antfly.raft.transport.std_http_listener.default_max_request_bytes;

const CliConfig = struct {
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
    role: ?[]const u8 = null,
    tick_ms: ?u64 = null,
    publish_enabled: ?bool = null,
    compaction_enabled: ?bool = null,
    prune_enabled: ?bool = null,
    enrichment_enabled: ?bool = null,
    help: bool = false,
};

pub fn run(
    init: std.process.Init,
    forced_role: ?serverless.RuntimeRole,
    forced_listener: ?bool,
    forced_swarm_mode: ?bool,
) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_serverless";
    return try runFromIterator(init, argv0, &args, forced_role, forced_listener, forced_swarm_mode);
}

pub fn runFromIterator(
    init: std.process.Init,
    argv0: []const u8,
    args: *std.process.Args.Iterator,
    forced_role: ?serverless.RuntimeRole,
    forced_listener: ?bool,
    forced_swarm_mode: ?bool,
) !void {
    const alloc = init.gpa;
    const cli = try parseCli(args);
    if (cli.help) {
        printUsage(argv0);
        return;
    }

    const bootstrap = serverless.BootstrapConfig{
        .artifacts_uri = try resolveRequired(init.environ_map, cli.artifacts_uri, "ANTFLY_SERVERLESS_ARTIFACTS_URI"),
        .manifests_uri = try resolveRequired(init.environ_map, cli.manifests_uri, "ANTFLY_SERVERLESS_MANIFESTS_URI"),
        .wal_uri = try resolveRequired(init.environ_map, cli.wal_uri, "ANTFLY_SERVERLESS_WAL_URI"),
        .progress_uri = try resolveRequired(init.environ_map, cli.progress_uri, "ANTFLY_SERVERLESS_PROGRESS_URI"),
        .catalog_uri = try resolveRequired(init.environ_map, cli.catalog_uri, "ANTFLY_SERVERLESS_CATALOG_URI"),
        .query_cache_dir = cli.query_cache_dir orelse init.environ_map.get("ANTFLY_SERVERLESS_QUERY_CACHE_DIR"),
        .query_cache_max_bytes = cli.query_cache_max_bytes orelse parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES", 0),
        .query_cache_payload_max_bytes = cli.query_cache_payload_max_bytes orelse parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES", 0),
        .embedding_indexes_json = cli.embedding_indexes_json orelse init.environ_map.get("ANTFLY_SERVERLESS_EMBEDDING_INDEXES_JSON"),
        .sparse_embedding_index_name = cli.sparse_embedding_index_name orelse init.environ_map.get("ANTFLY_SERVERLESS_SPARSE_EMBEDDING_INDEX_NAME") orelse "serverless_sparse",
        .chunk_embedding_index_name = cli.chunk_embedding_index_name orelse init.environ_map.get("ANTFLY_SERVERLESS_CHUNK_EMBEDDING_INDEX_NAME") orelse "serverless_chunk",
        .chunk_embedding_dimensions = cli.chunk_embedding_dimensions orelse parseEnvIntOrDefault(init.environ_map, u32, "ANTFLY_SERVERLESS_CHUNK_EMBEDDING_DIMS", 8),
        .tick_interval_ms = cli.tick_ms orelse parseEnvIntOrDefault(init.environ_map, u64, "ANTFLY_SERVERLESS_TICK_INTERVAL_MS", 25),
        .role = forced_role orelse try parseRuntimeRole(cli.role orelse init.environ_map.get("ANTFLY_SERVERLESS_ROLE") orelse "combined"),
        .swarm_mode = forced_swarm_mode orelse false,
        .publish_enabled = cli.publish_enabled orelse parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_PUBLISH_ENABLED", true),
        .compaction_enabled = cli.compaction_enabled orelse parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_COMPACTION_ENABLED", true),
        .prune_enabled = cli.prune_enabled orelse parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_PRUNE_ENABLED", true),
        .enrichment_enabled = cli.enrichment_enabled orelse parseEnvBoolOrDefault(init.environ_map, "ANTFLY_SERVERLESS_ENRICHMENT_ENABLED", true),
    };
    const listener_enabled = forced_listener orelse listenerEnabledForRole(bootstrap.role);
    const listener = if (listener_enabled) serverless_serverConfigFromEnv(init.environ_map, cli) else null;

    var srv = serverless.ServerlessServer.init(alloc, .{
        .bootstrap = bootstrap,
        .listener = listener,
    }) catch |err| {
        reportStartupError(err);
        return err;
    };
    defer srv.deinit();
    srv.start() catch |err| {
        reportStartupError(err);
        return err;
    };
    defer srv.stop();

    if (listener_enabled) {
        const base_uri = try srv.baseUri(alloc);
        defer alloc.free(base_uri);
        std.debug.print("serverless listening on {s}\n", .{base_uri});
    } else {
        std.debug.print("serverless maintenance runtime started without listener\n", .{});
    }
    printRuntimeStatusSummary(srv.runtimeStatus());

    var health_source = ServerlessHealthSource{ .srv = &srv };
    const health_port = cli.health_port orelse parseEnvOptionalInt(init.environ_map, u16, "ANTFLY_SERVERLESS_HEALTH_PORT");
    const health_server = try antfly.common.health_server.HealthServer.startIfConfigured(
        alloc,
        "serverless",
        health_port,
        health_source.readiness(),
        health_source.metricsWriter(),
    );
    defer if (health_server) |hs| hs.deinit();

    while (true) {
        sleepMs(init.io, 60_000);
    }
}

const ServerlessHealthSource = struct {
    srv: *serverless.ServerlessServer,

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
        return self.srv.runtimeStatus().validated;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *ServerlessHealthSource = @ptrCast(@alignCast(ptr));
        const run_stats = self.srv.stack.runtime.metricsSnapshot();
        const query_metrics = self.srv.stack.query.metricsSnapshot();
        const append = antfly.common.health_server.appendPromMetric;

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
    }
};

fn parseEnvOptionalInt(
    env_map: *std.process.Environ.Map,
    comptime T: type,
    env_name: []const u8,
) ?T {
    const raw = env_map.get(env_name) orelse return null;
    return std.fmt.parseInt(T, raw, 10) catch null;
}

fn parseCli(args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
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
        return error.InvalidArguments;
    }
    return cfg;
}

fn resolveRequired(
    env_map: *std.process.Environ.Map,
    cli_value: ?[]const u8,
    env_name: []const u8,
) ![]const u8 {
    if (cli_value) |value| return value;
    return env_map.get(env_name) orelse {
        std.debug.print("missing required config: {s}\n", .{env_name});
        return error.MissingConfiguration;
    };
}

fn parseEnvIntOrDefault(
    env_map: *std.process.Environ.Map,
    comptime T: type,
    env_name: []const u8,
    default: T,
) T {
    const raw = env_map.get(env_name) orelse return default;
    return std.fmt.parseInt(T, raw, 10) catch default;
}

fn parseEnvBoolOrDefault(
    env_map: *std.process.Environ.Map,
    env_name: []const u8,
    default: bool,
) bool {
    const raw = env_map.get(env_name) orelse return default;
    return parseBool(raw) catch default;
}

fn serverless_serverConfigFromEnv(
    env_map: *std.process.Environ.Map,
    cli: CliConfig,
) antfly.raft.transport.StdHttpListenerConfig {
    return .{
        .bind_host = cli.bind_host orelse env_map.get("ANTFLY_SERVERLESS_BIND_HOST") orelse "127.0.0.1",
        .bind_port = cli.bind_port orelse parseEnvIntOrDefault(env_map, u16, "ANTFLY_SERVERLESS_BIND_PORT", 8080),
        .max_request_bytes = cli.max_request_bytes orelse parseEnvIntOrDefault(
            env_map,
            usize,
            "ANTFLY_SERVERLESS_MAX_REQUEST_BYTES",
            serverless_default_max_request_bytes,
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
        \\  --role <combined|api|query|maintenance>
        \\  --tick-ms <milliseconds>
        \\  --publish-enabled <true|false>
        \\  --compaction-enabled <true|false>
        \\  --prune-enabled <true|false>
        \\  --enrichment-enabled <true|false>
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
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_DIR
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_MAX_BYTES default: 0 (unbounded)
        \\  ANTFLY_SERVERLESS_QUERY_CACHE_PAYLOAD_MAX_BYTES default: 0 (unbounded)
        \\  ANTFLY_SERVERLESS_BIND_HOST      default: 127.0.0.1
        \\  ANTFLY_SERVERLESS_BIND_PORT      default: 8080
        \\  ANTFLY_SERVERLESS_HEALTH_PORT    default: unset (disables dedicated health server)
        \\  ANTFLY_SERVERLESS_MAX_REQUEST_BYTES default: 33554432
        \\  ANTFLY_SERVERLESS_ROLE           default: combined
        \\  ANTFLY_SERVERLESS_TICK_INTERVAL_MS default: 25
        \\  ANTFLY_SERVERLESS_PUBLISH_ENABLED default: true
        \\  ANTFLY_SERVERLESS_COMPACTION_ENABLED default: true
        \\  ANTFLY_SERVERLESS_PRUNE_ENABLED default: true
        \\  ANTFLY_SERVERLESS_ENRICHMENT_ENABLED default: true
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
        error.InvalidRuntimeRole => "invalid runtime role; expected combined, api, query, or maintenance",
        error.MissingEndpoint => "missing S3-compatible endpoint; set AWS_ENDPOINT_URL for s3:// backends",
        error.MissingAccessKeyId => "missing S3-compatible access key; set AWS_ACCESS_KEY_ID for s3:// backends",
        error.MissingSecretAccessKey => "missing S3-compatible secret; set AWS_SECRET_ACCESS_KEY for s3:// backends",
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
    std.debug.print("serverless bootstrap role={s} swarm_mode={any} validated={any} tick_ms={d}\n", .{
        @tagName(status.role),
        status.swarm_mode,
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

test "serverless main startup hint covers backend config errors" {
    try std.testing.expectEqualStrings(
        "missing S3-compatible endpoint; set AWS_ENDPOINT_URL for s3:// backends",
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

test "serverless main listener config defaults request limit to 32 MiB" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();

    const cfg = serverless_serverConfigFromEnv(&env_map, .{});
    try std.testing.expectEqual(serverless_default_max_request_bytes, cfg.max_request_bytes);
}

test "serverless main listener config allows env and cli request limit overrides" {
    var env_map = std.process.Environ.Map.init(std.testing.allocator);
    defer env_map.deinit();
    try env_map.put("ANTFLY_SERVERLESS_MAX_REQUEST_BYTES", "4194304");

    const env_cfg = serverless_serverConfigFromEnv(&env_map, .{});
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), env_cfg.max_request_bytes);

    const cli_cfg = serverless_serverConfigFromEnv(&env_map, .{ .max_request_bytes = 8 * 1024 * 1024 });
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), cli_cfg.max_request_bytes);
}

test "serverless main parses maintenance booleans" {
    try std.testing.expect(try parseBoolArg("true"));
    try std.testing.expect(try parseBoolArg("1"));
    try std.testing.expect(!(try parseBoolArg("false")));
    try std.testing.expectError(error.InvalidArguments, parseBoolArg("maybe"));
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
