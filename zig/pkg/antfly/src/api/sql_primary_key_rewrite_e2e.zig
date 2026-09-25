// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

fn request(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8, method: http.Method, body: ?[]const u8) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, suffix });
    defer alloc.free(uri);
    // The transport owns Authorization separately and strips it from ordinary
    // headers; keep the Basic credential on its canonical request field.
    const authorization = if (headers.len > 0 and std.ascii.eqlIgnoreCase(headers[0].name, "authorization")) headers[0].value else null;
    return transport.execute(alloc, .{ .method = method, .uri = uri, .authorization = authorization, .headers = if (authorization != null) headers[1..] else headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = if (method == .GET) 1_000 else null });
}
fn sql(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, statement: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    return request(alloc, transport, headers, base, "/db/v1/sql", .POST, body);
}
fn table(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}", .{name});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
fn batch(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, body: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/batch", .{name});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .POST, body);
}
fn document(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, key: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/documents/{s}", .{ name, key });
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
fn job(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, id: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/restore/jobs/{s}", .{id});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
fn field(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.UnexpectedSqlResponse;
    return value.object.get(name) orelse error.UnexpectedSqlResponse;
}
fn sqlInteger(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| number,
        .number_string, .string => |number| std.fmt.parseInt(i64, number, 10),
        else => error.UnexpectedSqlResponse,
    };
}

/// A local ReadIndex does not prove metadata can route the private donor RPC.
/// Await the exact installed metadata router's projected store-status route.
fn waitForDonorRoute(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, data: *data_runtime.DataServer, table_id: u64) !void {
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
                        const Identity = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace };
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

test "mounted SQL ADD PRIMARY KEY guards publication until rewrite validates" {
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
    const rewrite_authorization = try basicAuthorization(alloc, rewrite_username, rewrite_password);
    defer alloc.free(rewrite_authorization);
    const rewrite_headers = [_]http.RequestHeader{.{ .name = "authorization", .value = rewrite_authorization }};
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
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    defer data.deinit();
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
    defer data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    defer data_control.deinit();
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
    const cases = [_]struct { name: []const u8, rows: []const u8, succeeds: bool }{
        .{ .name = "pk_good", .rows = "{\"inserts\":{\"row-a\":{\"id\":1,\"note\":\"first\"},\"row-b\":{\"id\":2,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = true },
        .{ .name = "pk_null", .rows = "{\"inserts\":{\"row-null\":{\"id\":null,\"note\":\"missing\"}},\"sync_level\":\"full_text\"}", .succeeds = false },
        .{ .name = "pk_duplicate", .rows = "{\"inserts\":{\"row-a\":{\"id\":7,\"note\":\"first\"},\"row-b\":{\"id\":7,\"note\":\"second\"}},\"sync_level\":\"full_text\"}", .succeeds = false },
    };
    for (cases) |case| {
        const create = try std.fmt.allocPrint(alloc, "CREATE TABLE {s} (id BIGINT, note TEXT)", .{case.name});
        defer alloc.free(create);
        var created = try sql(alloc, transport, &headers, base, create);
        defer created.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), created.status);
        var before_response = try table(alloc, transport, &headers, base, case.name);
        defer before_response.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), before_response.status);
        var before = try std.json.parseFromSlice(std.json.Value, alloc, before_response.body, .{});
        defer before.deinit();
        const before_id = (try field(before.value, "table_id")).string;
        try waitForDonorRoute(alloc, io_impl.io(), &metadata, &data, try std.fmt.parseUnsigned(u64, before_id, 10));
        var basic_read = try table(alloc, transport, &rewrite_headers, metadata_uri, case.name);
        defer basic_read.deinit(alloc);
        if (basic_read.status != 200) std.debug.print("PK Basic metadata read table={s} status={d} body={s}\n", .{ case.name, basic_read.status, basic_read.body });
        try std.testing.expectEqual(@as(u16, 200), basic_read.status);
        var inserted = blk: {
            for (0..600) |_| {
                var attempt = try batch(alloc, transport, &headers, base, case.name, case.rows);
                if (attempt.status != 503) break :blk attempt;
                attempt.deinit(alloc);
                try io_impl.io().sleep(.fromMilliseconds(10), .awake);
            }
            return error.TablePlacementTimeout;
        };
        defer inserted.deinit(alloc);
        if (inserted.status != 201) std.debug.print("PK setup batch table={s} status={d} body={s}\n", .{ case.name, inserted.status, inserted.body });
        try std.testing.expectEqual(@as(u16, 201), inserted.status);
        if (case.succeeds) {
            for ([_][]const u8{ "row-a", "row-b" }) |key| {
                var fetched = try document(alloc, transport, &headers, base, case.name, key);
                defer fetched.deinit(alloc);
                std.debug.print("PK source GET key={s} status={d} body={s}\n", .{ key, fetched.status, fetched.body });
                if (fetched.status != 200) return error.UnexpectedSourceLookup;
            }
            var selected = try sql(alloc, transport, &headers, base, "SELECT id FROM pk_good ORDER BY id");
            defer selected.deinit(alloc);
            if (selected.status != 200) {
                std.debug.print("PK source SELECT status={d} body={s}\n", .{ selected.status, selected.body });
                var metadata_selected = try sql(alloc, transport, &headers, metadata_uri, "SELECT id FROM pk_good ORDER BY id");
                defer metadata_selected.deinit(alloc);
                std.debug.print("PK metadata SELECT status={d} body={s}\n", .{ metadata_selected.status, metadata_selected.body });
                return error.UnexpectedSqlResponse;
            }
            var selected_json = try std.json.parseFromSlice(std.json.Value, alloc, selected.body, .{});
            defer selected_json.deinit();
            const rows = try field(selected_json.value, "rows");
            const first = if (rows == .array and rows.array.items.len == 2 and rows.array.items[0] == .array and rows.array.items[0].array.items.len == 1)
                sqlInteger(rows.array.items[0].array.items[0]) catch null
            else
                null;
            const second = if (rows == .array and rows.array.items.len == 2 and rows.array.items[1] == .array and rows.array.items[1].array.items.len == 1)
                sqlInteger(rows.array.items[1].array.items[0]) catch null
            else
                null;
            if (first != 1 or second != 2) {
                std.debug.print("PK source SELECT unexpected rows body={s}\n", .{selected.body});
                return error.UnexpectedSqlRows;
            }
        }
        const alter = try std.fmt.allocPrint(alloc, "ALTER TABLE {s} ADD CONSTRAINT {s}_key PRIMARY KEY (id)", .{ case.name, case.name });
        defer alloc.free(alter);
        var admitted = try sql(alloc, transport, &rewrite_headers, metadata_uri, alter);
        defer admitted.deinit(alloc);
        if (admitted.status == 501) {
            // This target remains useful while production keeps the rewrite
            // guard on: prove existing rows are readable and no generation was
            // published. Once the guard lifts, continue through all three
            // positive/negative lifecycle cases below.
            try std.testing.expect(case.succeeds);
            var guarded_response = try table(alloc, transport, &headers, base, case.name);
            defer guarded_response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), guarded_response.status);
            var guarded = try std.json.parseFromSlice(std.json.Value, alloc, guarded_response.body, .{});
            defer guarded.deinit();
            try std.testing.expectEqualStrings(before_id, (try field(guarded.value, "table_id")).string);
            const guarded_schema = try field(guarded.value, "schema");
            const guarded_constraints = if (guarded_schema == .object) guarded_schema.object.get("unique_constraints") else null;
            try std.testing.expect(guarded_constraints == null or guarded_constraints.? == .null or (guarded_constraints.? == .array and guarded_constraints.?.array.items.len == 0));
            return;
        }
        if (admitted.status != 202 and admitted.status != 409) std.debug.print("PK ALTER table={s} status={d} body={s}\n", .{ case.name, admitted.status, admitted.body });
        try std.testing.expect(admitted.status == 202 or admitted.status == 409);
        var admitted_json = try std.json.parseFromSlice(std.json.Value, alloc, admitted.body, .{});
        defer admitted_json.deinit();
        const receipt = try field(admitted_json.value, "ddl_receipt");
        try std.testing.expectEqualStrings(if (admitted.status == 202) "pending" else "admission_unknown", (try field(receipt, "state")).string);
        const job_id = (try field(receipt, "restore_job_id")).string;
        if (admitted.status == 409) {
            // This is a read-only recovery handle. Never replay the ALTER.
            var first_status = try job(alloc, transport, &rewrite_headers, metadata_uri, job_id);
            defer first_status.deinit(alloc);
            std.debug.print("PK uncertain admission job status table={s} status={d} body={s}\n", .{ case.name, first_status.status, first_status.body });
            if (first_status.status != 200) return error.RewriteAdmissionUnknownWithoutVisibleJob;
        }
        var terminal = false;
        var last_phase: [64]u8 = undefined;
        var last_phase_len: usize = 0;
        const started_ns = platform.time.monotonicNs();
        const deadline_ns = started_ns +| 30 * std.time.ns_per_s;
        for (0..300) |_| {
            if (platform.time.monotonicNs() >= deadline_ns) break;
            try meta_raft.check();
            try meta_control.check();
            try data_raft.check();
            try data_control.check();
            var response = try job(alloc, transport, &rewrite_headers, metadata_uri, job_id);
            defer response.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer parsed.deinit();
            const phase = (try field(parsed.value, "phase")).string;
            last_phase_len = @min(phase.len, last_phase.len);
            @memcpy(last_phase[0..last_phase_len], phase[0..last_phase_len]);
            if (std.mem.eql(u8, phase, if (case.succeeds) "succeeded" else "failed")) {
                std.debug.print("PK job terminal table={s} body={s}\n", .{ case.name, response.body });
                terminal = true;
                break;
            }
            if (std.mem.eql(u8, phase, if (case.succeeds) "failed" else "succeeded")) return error.UnexpectedRewriteOutcome;
            try io_impl.io().sleep(.fromMilliseconds(10), .awake);
        }
        if (!terminal) std.debug.print("PK job nonterminal table={s} last_phase={s}\n", .{ case.name, last_phase[0..last_phase_len] });
        try std.testing.expect(terminal);
        var after_response = try table(alloc, transport, &headers, base, case.name);
        defer after_response.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), after_response.status);
        var after = try std.json.parseFromSlice(std.json.Value, alloc, after_response.body, .{});
        defer after.deinit();
        const after_id = (try field(after.value, "table_id")).string;
        try std.testing.expectEqual(case.succeeds, !std.mem.eql(u8, before_id, after_id));
        const schema = try field(after.value, "schema");
        const constraints = if (schema == .object) schema.object.get("unique_constraints") else null;
        try std.testing.expectEqual(case.succeeds, constraints != null and constraints.? == .array and constraints.?.array.items.len == 1);
        if (case.succeeds) {
            var duplicate = try batch(alloc, transport, &headers, base, case.name, "{\"inserts\":{\"row-duplicate\":{\"id\":1,\"note\":\"duplicate\"}},\"sync_level\":\"full_text\"}");
            defer duplicate.deinit(alloc);
            try std.testing.expectEqual(@as(u16, 409), duplicate.status);
            try std.testing.expect(std.mem.indexOf(u8, duplicate.body, "UniqueConstraintViolation") != null);
        }
    }
}
