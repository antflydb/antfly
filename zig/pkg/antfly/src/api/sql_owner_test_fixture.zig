// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Mounted SQL primary-key rewrite, including metadata-owned donor routing.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const http_server = @import("http_server.zig");
const test_helpers = @import("../public_test_helpers.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

/// The fixture is the cluster administrator for this disposable deployment.
/// Registration only advertises a key; it must not silently approve a root.
/// Use the real signed enrollment and Raft apply path before campaigns need it.
fn enrollOwnedTestRoot(ctx: *Context, root_dir: []const u8) !void {
    const local = @import("antfly_local_sources");
    const enrollment = @import("../metadata/store_root_enrollment.zig");
    const root = try local.storage_db_root_signing_identity.load(ctx.alloc, ctx.io, root_dir);
    const registration = ctx.data.store_registration orelse return error.StoreRegistrationUnavailable;
    const identity: enrollment.Identity = .{
        .metadata_incarnation = try ctx.metadata.server.svc.metadataIncarnation() orelse return error.MetadataIncarnationUnavailable,
        .node_id = registration.node_id,
        .store_id = registration.store_id,
        .root_incarnation = root.root_incarnation,
        .public_key = root.public_key,
    };
    const source = http_server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const context: local.api_operation.RequestContext = .{ .setting_admin = true, .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s };
    const response = source.systemCatalog(ctx.alloc, context, .{ .store_root_enroll = try enrollment.Request.sign(identity, root.seed) }) catch |err| switch (err) {
        error.MetadataMutationOutcomeUnknown => null,
        else => return err,
    };
    if (response) |bytes| ctx.alloc.free(bytes);
    // Unknown admission is observed, not replayed or assumed successful.
    const observed = try source.systemCatalog(ctx.alloc, context, .{ .store_root_enrollment_status = identity });
    defer ctx.alloc.free(observed);
    var confirmed = try std.json.parseFromSlice(enrollment.Identity, ctx.alloc, observed, .{});
    defer confirmed.deinit();
    try std.testing.expectEqualDeep(identity, confirmed.value);
}

/// Wait on the same read-only ownership proof ALTER consumes, never retry an
/// admitted mutation or manufacture a publication receipt in the fixture.
pub fn waitRelationPublication(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, name: []const u8) !void {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const lookup: domain.ResolveMany = .{
        .relations = &.{.{ .database = "default", .namespace = "public", .name = name }},
        .include_query_definitions = true,
    };
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const bytes = source.systemCatalog(alloc, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup }) catch |err| switch (err) {
            error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer alloc.free(bytes);
        var snapshot = try std.json.parseFromSlice(domain.ResolvedMany, alloc, bytes, .{});
        defer snapshot.deinit();
        try snapshot.value.validateRelations(lookup);
        if (snapshot.value.relations[0] == null) return error.MissingSourceRelation;
        return;
    }
    return error.CatalogPublicationNotReady;
}

fn basicAuthorization(alloc: std.mem.Allocator, username: []const u8, password: []const u8) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ username, password });
    defer alloc.free(raw);
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(raw.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, raw);
    return std.fmt.allocPrint(alloc, "Basic {s}", .{encoded});
}

fn metadataRaft(ptr: *anyopaque) !void {
    const server: *metadata_runtime.Server = @ptrCast(@alignCast(ptr));
    try server.runRaftRoundOnly();
}
fn metadataControl(ptr: *anyopaque) !void {
    const server: *metadata_runtime.Server = @ptrCast(@alignCast(ptr));
    try server.runControlRoundOnly();
    try server.runCdcRound();
}
fn dataRaft(ptr: *anyopaque) !void {
    const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
    try server.runRaftRoundOnly();
}
fn dataControl(ptr: *anyopaque) !void {
    const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
    try server.runControlRoundOnly();
}

pub fn request(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8, method: http.Method, body: ?[]const u8) !http.HttpResponse {
    return requestWithTimeout(alloc, transport, headers, base, suffix, method, body, if (method == .GET) 1_000 else null);
}
pub fn requestWithTimeout(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8, method: http.Method, body: ?[]const u8, timeout_ms: ?u32) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, suffix });
    defer alloc.free(uri);
    // The transport owns Authorization separately and strips it from ordinary
    // headers; keep the Basic credential on its canonical request field.
    const authorization = if (headers.len > 0 and std.ascii.eqlIgnoreCase(headers[0].name, "authorization")) headers[0].value else null;
    return transport.execute(alloc, .{ .method = method, .uri = uri, .authorization = authorization, .headers = if (authorization != null) headers[1..] else headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = timeout_ms });
}
pub fn sql(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, statement: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    return request(alloc, transport, headers, base, "/db/v1/sql", .POST, body);
}
pub fn table(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}", .{name});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
pub fn batch(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, body: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/batch", .{name});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .POST, body);
}
pub fn document(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, key: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/documents/{s}", .{ name, key });
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
pub fn job(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, id: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/restore/jobs/{s}", .{id});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
pub fn field(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.UnexpectedSqlResponse;
    return value.object.get(name) orelse error.UnexpectedSqlResponse;
}
pub fn sqlInteger(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| number,
        .number_string, .string => |number| std.fmt.parseInt(i64, number, 10),
        else => error.UnexpectedSqlResponse,
    };
}

pub fn waitForDonorRoute(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, data: *data_runtime.DataServer, table_id: u64) !void {
    const public_server = metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    const router = public_server.cfg.session_router orelse return error.MissingDonorRouter;
    const catalog = table_catalog.CatalogSource.fromMetadataHttpService(metadata.server.svc);
    const reader = if (data.http_server) |*server| server.table_reads orelse return error.MissingOwnerReadSource else return error.MissingOwnerReadSource;
    var last_status_error: ?anyerror = null;
    var last_read_error: ?anyerror = null;
    var no_route: usize = 0;
    for (0..300) |_| {
        data.runStoreStatusRoundOnly() catch |err| {
            last_status_error = err;
        };
        var snapshot = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&snapshot);
        const range = for (snapshot.ranges) |entry| {
            if (entry.table_id == table_id) break entry;
        } else {
            try io.sleep(.fromMilliseconds(20), .awake);
            continue;
        };
        const physical_name = for (snapshot.tables) |entry| {
            if (entry.table_id == table_id) break entry.name;
        } else {
            try io.sleep(.fromMilliseconds(20), .awake);
            continue;
        };
        const resolved = try table_router.resolveGroupRoute(alloc, catalog, router, range.group_id, .prefer_leader);
        if (resolved) |value| {
            var route = value;
            defer route.deinit(alloc);
            switch (route) {
                .remote => |remote| if (remote.node_id == 9) {
                    const observed = reader.lookup(alloc, physical_name, range.start_key, .{
                        .relational_topology_json = "{\"mode\":\"identity\"}",
                        .execution_deadline_ns = platform.time.monotonicNs() +| 500 * std.time.ns_per_ms,
                    }, .read_index) catch |err| {
                        last_read_error = err;
                        try io.sleep(.fromMilliseconds(20), .awake);
                        continue;
                    };
                    if (observed) |value_response| {
                        var response = value_response;
                        defer response.deinit(alloc);
                        const Identity = struct { namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace };
                        var identity = try std.json.parseFromSlice(Identity, alloc, response.json, .{ .ignore_unknown_fields = true });
                        defer identity.deinit();
                        if (identity.value.namespace.table_id == table_id and identity.value.namespace.shard_id == metadata_table_manager.rangeDocIdentityShardId(range) and identity.value.namespace.range_id == metadata_table_manager.rangeDocIdentityRangeId(range)) return;
                    }
                },
                .local => {},
            }
        }
        no_route += 1;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    std.debug.print("PK donor/owner readiness unavailable table={d} no_route={d} last_status_error={s} last_read_error={s}\n", .{ table_id, no_route, if (last_status_error) |err| @errorName(err) else "none", if (last_read_error) |err| @errorName(err) else "none" });
    return error.DonorRouteUnavailable;
}

pub const Context = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    metadata: *metadata_runtime.Server,
    data: *data_runtime.DataServer,
    transport: http.RequestExecutor,
    headers: []const http.RequestHeader,
    admin_headers: []const http.RequestHeader,
    reader_headers: []const http.RequestHeader,
    metadata_uri: []const u8,
    base: []const u8,
    progress: [4]*raft.ManagedProgressDriver,

    pub fn checkDrivers(self: *const Context) !void {
        for (self.progress) |driver| try driver.check();
    }
};

/// Observe the serving owner's schema as well as metadata publication. A
/// routing/catalog read alone does not prove data-owner propagation finished.
pub const ServingSchema = struct { table_id: u64, version: u32 };

pub fn waitServingSchema(ctx: *Context, name: []const u8, minimum: u32) !ServingSchema {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    const lookup: domain.ResolveMany = .{ .relations = &.{.{ .name = name }}, .include_query_definitions = true };
    const metadata = http_server.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = metadata.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup }) catch |err| switch (err) {
            error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => {
                try ctx.io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer ctx.alloc.free(bytes);
        var resolved = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, bytes, .{});
        defer resolved.deinit();
        try resolved.value.validateRelations(lookup);
        if (resolved.value.relations[0]) |relation| {
            if (relation.owner.schema_version >= minimum) {
                const observed = reader.lookup(ctx.alloc, relation.table.name, "", .{
                    .relational_topology_json = "{\"mode\":\"public_schema\"}",
                    .execution_deadline_ns = deadline,
                }, .read_index) catch |err| switch (err) {
                    error.GroupLeaderUnavailable, error.LeaderUnavailable, error.NotLeader, error.UnknownGroup, error.ReadIndexTimeout, error.Timeout, error.ReadUnavailable, error.StorageReadTemporarilyUnavailable, error.TableNotFound, error.IntegrityCatalogChanged, error.CatalogRoutingUnavailable => null,
                    else => return err,
                };
                if (observed) |value| {
                    var response = value;
                    defer response.deinit(ctx.alloc);
                    var public = try std.json.parseFromSlice([]const u8, ctx.alloc, response.json, .{});
                    defer public.deinit();
                    var schema = try std.json.parseFromSlice(struct { version: u32 }, ctx.alloc, public.value, .{ .ignore_unknown_fields = true });
                    defer schema.deinit();
                    if (schema.value.version == relation.owner.schema_version) return .{ .table_id = relation.owner.table_id, .version = schema.value.version };
                }
            }
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlOwnerSchemaTimeout;
}

pub fn run(comptime Campaign: type) !void {
    return runWithState(Campaign, .{});
}

/// Runtime campaign data avoids compiling the same mounted owner and restart
/// machinery once per original contract. Each run still owns isolated roots.
pub fn runWithState(comptime Campaign: type, initial: Campaign) !void {
    const alloc = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(alloc);
    const internal_secret = "pk-rewrite-e2e-internal-service-secret-v1";
    const trusted_secret = "pk-rewrite-e2e-trusted-principal-secret-v1";
    const issuer = "pk-rewrite-e2e";
    const rewrite_username = "pk-rewriter";
    const rewrite_password = "pk-rewrite-durable-password";
    var auth_store = usermgr.MemoryStore.init(alloc);
    defer auth_store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var auth_manager = try usermgr.UserManager.init(alloc, auth_store.iface(), try usermgr.initDefaultEnforcer(alloc, policy_store.iface()));
    defer auth_manager.deinit();
    var rewrite_permission = try usermgr.Permission.initOwned(alloc, .table, "*", .admin);
    defer rewrite_permission.deinit(alloc);
    var rewrite_user = try auth_manager.createUser(rewrite_username, rewrite_password, &.{rewrite_permission});
    defer rewrite_user.deinit(alloc);
    var reader_permission = try usermgr.Permission.initOwned(alloc, .table, "*", .read);
    defer reader_permission.deinit(alloc);
    var reader_user = try auth_manager.createUser("pk-reader", "pk-reader-durable-password", &.{reader_permission});
    defer reader_user.deinit(alloc);
    if (@hasDecl(Campaign, "configureAuth")) try Campaign.configureAuth(&auth_manager);
    const rewrite_authorization = try basicAuthorization(alloc, rewrite_username, rewrite_password);
    defer alloc.free(rewrite_authorization);
    const rewrite_headers = [_]http.RequestHeader{.{ .name = "authorization", .value = rewrite_authorization }};
    const reader_authorization = try basicAuthorization(alloc, "pk-reader", "pk-reader-durable-password");
    defer alloc.free(reader_authorization);
    const reader_headers = [_]http.RequestHeader{.{ .name = "authorization", .value = reader_authorization }};
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const meta_root = try std.fmt.allocPrint(alloc, "{s}/metadata", .{root});
    defer alloc.free(meta_root);
    const data_root = try std.fmt.allocPrint(alloc, "{s}/data", .{root});
    defer alloc.free(data_root);
    const meta_catalog = try std.fmt.allocPrint(alloc, "{s}/metadata-catalog", .{root});
    defer alloc.free(meta_catalog);
    const data_catalog = try std.fmt.allocPrint(alloc, "{s}/data-catalog", .{root});
    defer alloc.free(data_catalog);
    const snapshots = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{root});
    defer alloc.free(snapshots);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    // Session campaigns opt in to the same native durable registry used by
    // production, not a mock or the legacy filesystem DocStore fallback.
    const native_sessions = if (@hasDecl(Campaign, "native_sessions")) Campaign.native_sessions else false;
    const session_path = try std.fmt.allocPrint(alloc, "{s}/sql-sessions", .{root});
    defer alloc.free(session_path);
    const local = @import("antfly_local_sources");
    var session_backend: ?local.storage_lsm_backend.BackendHandle = if (native_sessions) try local.storage_lsm_backend.BackendHandle.open(alloc, session_path, .{}) else null;
    defer if (session_backend) |*backend| backend.close();
    var session_store: ?local.storage_backend_erased.Store = if (session_backend) |*backend| try backend.backend.runtimeStore(alloc, .{ .name = "system/api-transaction-sessions" }) else null;
    defer if (session_store) |*store| store.deinit();
    var sessions = if (session_store) |*store| @import("transactions.zig").DurableSessionStore.initRuntime(alloc, store) else null;
    var metadata = try metadata_runtime.Server.init(process_alloc, .{
        .local_node_id = 1,
        .metadata_group_id = 2188,
        .replica_root_dir = meta_root,
        .replica_catalog_path = meta_catalog,
        .snapshot_root_dir = snapshots,
        .observe_local_replica_root = true,
        .api_server_cfg = .{ .auth_enabled = true, .user_manager = &auth_manager, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    });
    defer metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2188, 1);
    var meta_raft = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &metadata, .run_once = metadataRaft }, std.time.ns_per_ms);
    defer meta_raft.deinit();
    try meta_raft.start();
    var meta_control = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &metadata, .run_once = metadataControl }, std.time.ns_per_ms);
    defer meta_control.deinit();
    try meta_control.start();
    for (0..600) |_| {
        if (try metadata.server.svc.metadataIncarnation() != null) break;
        try io_impl.io().sleep(.fromMilliseconds(10), .awake);
    } else return error.MetadataIncarnationUnavailable;
    const metadata_uri = try metadata.adminBaseUri(alloc);
    defer alloc.free(metadata_uri);
    var data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce", .session_store = if (sessions) |*store| store else null },
    }, metadata_uri);
    var data_live = true;
    defer if (data_live) data.deinit();
    try data.start();
    for (0..32) |_| {
        data.registerNodeIfConfigured() catch |err| switch (err) {
            error.StoreRegistrationNotVisible => {
                try io_impl.io().sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
        break;
    } else return error.StoreRegistrationNotVisible;
    var data_raft = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    var data_raft_live = true;
    defer if (data_raft_live) data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    var data_control_live = true;
    defer if (data_control_live) data_control.deinit();
    try data_control.start();
    const base = try data.baseUri(alloc);
    defer alloc.free(base);
    var executor = executor_mod.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const transport = executor.executor();
    const now: i64 = @intCast(@divFloor(platform.time.realtimeNs(), std.time.ns_per_s));
    const claims = try std.fmt.allocPrint(alloc,
        \\{{"iss":"{s}","sub":"user:pk-rewrite-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};
    var context: Context = .{
        .alloc = alloc,
        .io = io_impl.io(),
        .metadata = &metadata,
        .data = &data,
        .transport = transport,
        .headers = &headers,
        .admin_headers = &rewrite_headers,
        .reader_headers = &reader_headers,
        .metadata_uri = metadata_uri,
        .base = base,
        .progress = .{ &meta_raft, &meta_control, &data_raft, &data_control },
    };
    var campaign = initial;
    try enrollOwnedTestRoot(&context, data_root);
    try campaign.beforeRestart(&context);
    try context.checkDrivers();
    // Recover the same durable owner storage while metadata keeps serving.
    data_control.deinit();
    data_control_live = false;
    data_raft.deinit();
    data_raft_live = false;
    data.deinit();
    data_live = false;
    data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce", .session_store = if (sessions) |*store| store else null },
    }, metadata_uri);
    data_live = true;
    try data.start();
    for (0..32) |_| {
        data.registerNodeIfConfigured() catch |err| switch (err) {
            error.StoreRegistrationNotVisible => {
                try io_impl.io().sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
        break;
    } else return error.StoreRegistrationNotVisible;
    data_raft = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_live = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_live = true;
    try data_control.start();
    const restarted_base = try data.baseUri(alloc);
    defer alloc.free(restarted_base);
    context.base = restarted_base;
    try campaign.afterRestart(&context);
    try context.checkDrivers();
}
