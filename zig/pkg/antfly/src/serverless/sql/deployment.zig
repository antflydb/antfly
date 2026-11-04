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

//! Process-owned wiring for API gateways, query pools and maintenance pools.
const std = @import("std");
const local = @import("antfly_local_sources");
const runtime = @import("runtime.zig");
const transport = @import("transport.zig");
const A = std.mem.Allocator;
const Role = @import("../api/types.zig").RuntimeRole;
const Opened = local.serverless_object_store_support.OpenedObjectStore;
const ArtifactStore = @import("../../api/lake_index_store.zig").Store;
pub const Config = struct {
    state_uri: []const u8,
    exchange_uri: []const u8,
    workers: []const []const u8,
    discovery_name: ?[]const u8 = null,
    state_connection: ?[]const u8 = null,
    exchange_connection: ?[]const u8 = null,
    authentication: @import("../../api/internal_service_auth.zig").Config,
    task_memory: u64 = 128 * 1024 * 1024,
    concurrency: u16 = 8,
    query_concurrency: u16 = 64,
    query_limits: @import("plan.zig").Limits = .{},
    spill_dir: ?[]const u8 = null,
    spill_bytes: u64 = 8 * 1024 * 1024 * 1024,
    pub fn limits(self: Config) @import("plan.zig").Limits {
        var result = self.query_limits;
        result.task_memory = self.task_memory;
        result.concurrency = self.query_concurrency;
        result.task_output_bytes = @min(result.task_output_bytes, self.task_memory / 4);
        return result;
    }
    pub fn validate(self: Config) !void {
        if (self.task_memory < 1024 * 1024 or self.task_memory > 1024 * 1024 * 1024 or self.concurrency == 0 or self.concurrency > 256 or self.query_concurrency == 0 or self.query_concurrency > 256 or self.workers.len == 0 or self.workers.len > 256) return error.InvalidSqlDeployment;
        try self.limits().validate();
        try @import("../../api/internal_service_auth.zig").validateRuntimeConfig(self.authentication.secret, null, self.authentication.issuer);
        for ([_][]const u8{ self.state_uri, self.exchange_uri }) |uri| {
            if (!(std.mem.startsWith(u8, uri, "s3://") or std.mem.startsWith(u8, uri, "gs://") or std.mem.startsWith(u8, uri, "file://"))) return error.InvalidSqlDeployment;
        }
        if (self.discovery_name) |name| _ = std.Io.net.HostName.init(name) catch return error.InvalidSqlDeployment;
        for (self.workers) |url| {
            const parsed = std.Uri.parse(url) catch return error.InvalidSqlDeployment;
            if ((!std.mem.eql(u8, parsed.scheme, "http") and !std.mem.eql(u8, parsed.scheme, "https")) or parsed.host == null or parsed.user != null or parsed.password != null or parsed.query != null or parsed.fragment != null) return error.InvalidSqlDeployment;
        }
    }
    /// Secret values come from the existing keystore or Secret-backed env.
    /// No credential is persisted in the operator's ConfigMap.
    pub fn fromEnv(a: A, env: *const std.process.Environ.Map, secrets: ?*local.common_secrets.FileStore) !?Config {
        const state = env.get("ANTFLY_SQL_STATE_URI") orelse {
            if (env.get("ANTFLY_SQL_EXCHANGE_URI") != null or env.get("ANTFLY_SQL_QUERY_WORKERS") != null) return error.InvalidSqlDeployment;
            return null;
        };
        const exchange = env.get("ANTFLY_SQL_EXCHANGE_URI") orelse return error.InvalidSqlDeployment;
        const urls = env.get("ANTFLY_SQL_QUERY_WORKERS") orelse return error.InvalidSqlDeployment;
        const secret = env.get("ANTFLY_SQL_SERVICE_SECRET") orelse if (secrets) |store| (try store.getOwned(a, "antfly.internal_service.secret")) orelse env.get("ANTFLY_INTERNAL_SERVICE_SECRET") else env.get("ANTFLY_INTERNAL_SERVICE_SECRET");
        const issuer = env.get("ANTFLY_SQL_SERVICE_ISSUER") orelse if (secrets) |store| (try store.getOwned(a, "antfly.internal_service.issuer")) orelse env.get("ANTFLY_INTERNAL_SERVICE_ISSUER") else env.get("ANTFLY_INTERNAL_SERVICE_ISSUER");
        const result: Config = .{
            .discovery_name = env.get("ANTFLY_SQL_QUERY_DNS"),
            .state_connection = env.get("ANTFLY_SQL_STATE_CONNECTION"),
            .exchange_connection = env.get("ANTFLY_SQL_EXCHANGE_CONNECTION"),
            .state_uri = state,
            .exchange_uri = exchange,
            .workers = try std.json.parseFromSliceLeaky([]const []const u8, a, urls, .{ .allocate = .alloc_always }),
            .authentication = .{ .secret = secret orelse return error.InvalidSqlDeployment, .issuer = issuer orelse return error.InvalidSqlDeployment },
            .task_memory = if (env.get("ANTFLY_SQL_TASK_MEMORY_BYTES")) |value| try std.fmt.parseInt(u64, value, 10) else 128 * 1024 * 1024,
            .spill_dir = env.get("ANTFLY_SQL_SPILL_DIR"),
            .query_limits = if (env.get("ANTFLY_SQL_QUERY_LIMITS")) |json| try std.json.parseFromSliceLeaky(@import("plan.zig").Limits, a, json, .{}) else .{},
            .query_concurrency = if (env.get("ANTFLY_SQL_QUERY_CONCURRENCY")) |value| try std.fmt.parseInt(u16, value, 10) else 64,
            .concurrency = if (env.get("ANTFLY_SQL_CONCURRENCY")) |value| try std.fmt.parseInt(u16, value, 10) else 8,
        };
        try result.validate();
        return result;
    }
};
fn openExecution(a: A, node: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, uri: []const u8, connection: ?[]const u8, file_bucket: []const u8) !Opened {
    if (connection) |name| {
        var scoped = (node orelse return error.InvalidSqlDeployment).*;
        var parsed = try local.serverless_remote_uri.parseAlloc(a, uri);
        defer switch (parsed) {
            .file => |value| a.free(value),
            inline .s3, .gcs => |*value| value.deinit(a),
        };
        const location = switch (parsed) {
            .file => return error.InvalidSqlDeployment,
            inline .s3, .gcs => |value| value,
        };
        const configured = scoped.connections.get(name) orelse return error.InvalidSqlDeployment;
        const external = configured.external_io orelse return error.InvalidSqlDeployment;
        if ((parsed == .s3 and external.protocol != .s3) or (parsed == .gcs and external.protocol != .gcs)) return error.InvalidSqlDeployment;
        scoped.storage.artifacts = .{ .connection = @constCast(name), .bucket = location.bucket, .prefix = location.prefix };
        return @import("../configured_object_store_support.zig").openNativeArtifactObjectStoreAlloc(a, &scoped, secrets, false);
    }
    return Opened.initRemoteUriWithOptions(a, uri, file_bucket, .{ .ensure_bucket = false });
}
pub const Owned = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    state: Opened,
    exchange: Opened,
    artifacts: ?ArtifactStore,
    artifact_port: @import("../artifacts/store.zig").ArtifactStore,
    host: ?@import("lake_source.zig").Host,
    executor: @import("../../common/http/std_http_executor.zig").StdHttpExecutor,
    client: transport.Client,
    service: runtime.Service,
    bridge: @import("bridge.zig").Bridge,
    driver: @import("driver.zig").Driver,
    admission: local.common_request_admission.RequestAdmission,
    config: Config,
    cache: local.serverless_query_lake_serving_cache.Cache,
    spill: local.sql_spill.Manager,
    pub fn init(a: A, io: std.Io, cfg: Config, role: Role, node: ?*const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, deployment: local.common_config.DeploymentMode, local_base: ?[]const u8) !*Owned {
        try cfg.validate();
        const self = try a.create(Owned);
        errdefer a.destroy(self);
        self.a = a;
        self.arena = .init(a);
        errdefer self.arena.deinit();
        const scratch = self.arena.allocator();
        // Deep-copy borrowed configuration, including credentials, for shutdown.
        self.config = try std.json.parseFromSliceLeaky(Config, scratch, try std.json.Stringify.valueAlloc(scratch, cfg, .{}), .{ .allocate = .alloc_always });
        self.state = try openExecution(a, node, secrets, cfg.state_uri, cfg.state_connection, "sql-state");
        errdefer self.state.deinit();
        self.exchange = try openExecution(a, node, secrets, cfg.exchange_uri, cfg.exchange_connection orelse cfg.state_connection, "sql-exchange");
        errdefer self.exchange.deinit();
        // Maintenance owns only execution expiry collection. Opening source
        // artifacts would unnecessarily require table/source credentials.
        self.artifacts = if (role == .maintenance_only) null else try ArtifactStore.openNative(a, node, secrets, false, deployment, local_base);
        errdefer if (self.artifacts) |*store| store.deinit();
        self.artifact_port = if (self.artifacts) |*store| store.artifactStore() else undefined;
        self.cache = local.serverless_query_lake_serving_cache.Cache.initWithMemoryLimit(a, 64 * 1024 * 1024);
        errdefer self.cache.deinit();
        self.spill = .{ .alloc = a, .io = io, .root = self.config.spill_dir orelse "", .max_bytes = cfg.spill_bytes, .context = self, .checkpoint = struct {
            fn check(_: *anyopaque) !void {}
        }.check };
        if (cfg.spill_dir) |path| {
            if (!std.fs.path.isAbsolute(path)) return error.InvalidSqlDeployment;
            try std.Io.Dir.cwd().createDirPath(io, path);
        }
        self.host = if (self.artifacts) |*store| .{ .cache = &self.cache, .artifacts = &self.artifact_port, .store_identity = try @import("../../api/native_retained_cut.zig").storeIdentity(scratch, store.locator), .options = .{ .node_config = node, .secret_store = secrets }, .io = io } else null;
        self.executor.initInPlace(a, .{});
        errdefer self.executor.deinit();
        self.client = .{ .a = a, .executor = self.executor.executor(), .authentication = self.config.authentication, .workers = self.config.workers, .io = io, .discovery = if (self.config.discovery_name) |name| .{ .name = name, .io = io, .port = 12390 } else null };
        var nonce: [16]u8 = undefined;
        try io.randomSecure(&nonce);
        const owner = try scratch.dupe(u8, &std.fmt.bytesToHex(nonce, .lower));
        const seed = local.serverless_external_source_mod.lake_catalog.types.digestHex(try std.fmt.allocPrint(scratch, "{s}\x00{s}\x00{s}", .{ cfg.state_uri, cfg.exchange_uri, cfg.authentication.issuer }));
        self.service = .{ .a = a, .state_store = .{ .client = self.state.client, .bucket = self.state.bucket, .prefix = if (self.state.prefix.len == 0) "sql" else self.state.prefix, .now_ms = 0 }, .exchange_store = .{ .client = self.exchange.client, .bucket = self.exchange.bucket, .prefix = if (self.exchange.prefix.len == 0) "sql" else self.exchange.prefix }, .tenant = seed, .owner = owner, .role = role, .source = if (self.host) |*host| host.source() else .{ .ptr = self, .open = noSource }, .retention = if (self.host) |*host| host.retention() else .{ .ptr = self, .ensure = noRetention }, .context = .{ .io = io }, .task_memory_ceiling = cfg.task_memory, .spill = if (cfg.spill_dir != null) &self.spill else null };
        self.bridge = .{ .service = self.service, .dispatcher = self.client.dispatcher(), .gateway = &self.client, .principal_scopes = true, .limits = cfg.limits() };
        self.driver = .{ .service = self.service, .dispatcher = self.client.dispatcher(), .principal_scopes = true };
        if (role == .query_only or role == .combined) {
            self.service.notification = self.driver.notification();
            self.bridge.service.notification = self.service.notification;
        }
        self.admission = .init(cfg.concurrency);
        return self;
    }
    fn noSource(_: *anyopaque, _: A, _: @import("plan.zig").Digest, _: @import("plan.zig").Input, _: u32, _: ?*const local.sql_dynamic_filter.Filter, _: local.serverless_query_lake_read_context.Context) !@import("runner.zig").Stream {
        return error.SqlQueryRoleRequired;
    }
    fn noRetention(_: *anyopaque, _: A, _: @import("plan.zig").Plan, _: local.serverless_query_lake_read_context.Context) !void {
        return error.SqlQueryRoleRequired;
    }
    fn open(raw: *anyopaque, _: A, tenant: @import("plan.zig").Digest, context: local.serverless_query_lake_read_context.Context) !runtime.Service {
        const self: *Owned = @ptrCast(@alignCast(raw));
        try @import("state.zig").validateDigest(tenant);
        var service = self.service;
        service.tenant = tenant;
        service.context = context;
        return service;
    }
    pub fn factory(self: *Owned) runtime.Factory {
        return .{ .ptr = self, .open = open, .dispatcher = self.client.dispatcher() };
    }
    pub fn collector(self: *Owned) @import("collector.zig").Collector {
        return .{ .state_store = self.service.state_store, .exchange_store = self.service.exchange_store };
    }
    pub fn attachApi(self: *Owned, server: *@import("../../api/http_server.zig").ApiHttpServer) void {
        server.cfg.distributed_sql_join = self.bridge.port();
        server.cfg.distributed_sql_scope_seed = self.service.tenant;
    }
    pub fn attachServer(self: *Owned, server: *@import("../server.zig").ServerlessServer) void {
        server.sql_owned = self;
        server.owned_http_server.cfg.sql_configured = true;
        switch (self.service.role) {
            .query_only, .combined => {
                server.owned_http_server.cfg.distributed_sql = .{ .factory = self.factory(), .authentication = self.config.authentication, .admission = &self.admission };
                server.sql_driver = &self.driver;
            },
            else => {},
        }
        switch (self.service.role) {
            .maintenance_only, .combined => server.stack.runtime.cfg.sql_execution_gc = self.collector(),
            else => {},
        }
    }
    pub fn deinit(self: *Owned) void {
        self.driver.stop();
        self.executor.deinit();
        self.spill.deinit();
        self.cache.deinit();
        self.artifact_port = undefined;
        if (self.artifacts) |*store| store.deinit();
        self.exchange.deinit();
        self.state.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
};

test "distributed SQL deployment separates cluster fan-out from pod admission and bounds output" {
    const config: Config = .{ .state_uri = "file:///state", .exchange_uri = "file:///exchange", .workers = &.{"https://query.example"}, .authentication = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "sql" }, .task_memory = 8 * 1024 * 1024, .concurrency = 2, .query_concurrency = 64, .query_limits = .{ .query_exchange_bytes = 8 * 1024 * 1024 * 1024 } };
    try config.validate();
    const policy = config.limits();
    try std.testing.expectEqual(@as(u16, 64), policy.concurrency);
    try std.testing.expectEqual(@as(u16, 2), config.concurrency);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024), policy.task_output_bytes);
    try std.testing.expectEqual(@as(u64, 8 * 1024 * 1024 * 1024), policy.query_exchange_bytes);
    var invalid = config;
    invalid.query_limits.task_scan_rows = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidSqlDistributedPlan, invalid.validate());
}
