// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Opt-in mounted self-FK publication proof. The normal target checks the
//! public guard; the diagnostic target is only run during a bounded guard lift.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const test_helpers = @import("../public_test_helpers.zig");

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
    return transport.execute(alloc, .{ .method = method, .uri = uri, .headers = headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = 3_000 });
}
fn sql(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, statement: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    return request(alloc, transport, headers, base, "/db/v1/sql", .POST, body);
}
fn table(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !http.HttpResponse {
    return request(alloc, transport, headers, base, "/db/v1/tables/nodes", .GET, null);
}
fn batch(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, body: []const u8) !http.HttpResponse {
    return request(alloc, transport, headers, base, "/db/v1/tables/nodes/batch", .POST, body);
}
fn awaitTable(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !u64 {
    for (0..600) |_| {
        var response = try table(alloc, transport, headers, base);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer parsed.deinit();
            const value = parsed.value.object.get("table_id") orelse return error.UnexpectedTableResponse;
            return std.fmt.parseUnsigned(u64, value.string, 10);
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
}
fn awaitStoreRegistration(io: std.Io, data: *data_runtime.DataServer) !void {
    // registerNodeIfConfigured verifies the exact store record in a fresh
    // metadata snapshot. A newly restarted store may need another metadata
    // control round before that record is visible.
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        data.registerNodeIfConfigured() catch |err| switch (err) {
            error.StoreRegistrationNotVisible => {
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        };
        return;
    }
    return error.StoreRegistrationNotVisible;
}
fn hasSelfFk(alloc: std.mem.Allocator, response: http.HttpResponse) !bool {
    if (response.status != 200) return error.UnexpectedTableResponse;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
    defer parsed.deinit();
    const schema = parsed.value.object.get("schema") orelse return error.UnexpectedTableResponse;
    const keys = schema.object.get("foreign_keys") orelse return false;
    return keys == .array and keys.array.items.len != 0;
}
fn awaitBatch(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, body: []const u8) !http.HttpResponse {
    for (0..600) |_| {
        var response = try batch(alloc, transport, headers, base, body);
        if (response.status != 503) return response;
        response.deinit(alloc);
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
}
fn awaitPublication(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, table_id: u64) !void {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const encoded = try source.systemCatalog(alloc, .{
            .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
            .fk_generation_publication_authority = true,
        }, .{ .fk_generation_publication_status = table_id });
        defer alloc.free(encoded);
        var status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        try status.value.validateState(alloc);
        if (status.value.phase == .published) {
            try std.testing.expectEqual(@as(usize, 1), status.value.child_fenced.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_activated.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_acknowledged.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.child_installed.len);
            return;
        }
        if (status.value.phase == .canceled) return error.UnexpectedPublicationCancel;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.PublicationTimeout;
}

fn mountedSelfFk(activated: bool) !void {
    const alloc = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(alloc);
    const trusted_secret = "hosted-self-fk-trusted-v1";
    const internal_secret = "hosted-self-fk-internal-v1";
    const issuer = "hosted-self-fk";
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
    const io = io_impl.io();
    var metadata = try metadata_runtime.Server.init(process_alloc, .{
        .local_node_id = 1,
        .metadata_group_id = 2297,
        .replica_root_dir = meta_root,
        .replica_catalog_path = meta_catalog,
        .snapshot_root_dir = snapshots,
        .observe_local_replica_root = true,
        .api_server_cfg = .{ .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    });
    defer metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2297, 1);
    var meta_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataRaft }, std.time.ns_per_ms);
    defer meta_raft.deinit();
    try meta_raft.start();
    var meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, std.time.ns_per_ms);
    defer meta_control.deinit();
    try meta_control.start();
    for (0..600) |_| {
        if (try metadata.server.svc.metadataIncarnation() != null) break;
        try io.sleep(.fromMilliseconds(10), .awake);
    } else return error.MetadataIncarnationUnavailable;
    const metadata_uri = try metadata.adminBaseUri(alloc);
    defer alloc.free(metadata_uri);
    var data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    var data_live = true;
    defer if (data_live) data.deinit();
    try data.start();
    try awaitStoreRegistration(io, &data);
    var data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    var data_raft_live = true;
    defer if (data_raft_live) data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
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
        \\{{"iss":"{s}","sub":"user:hosted-self-fk-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};

    var create = try sql(alloc, transport, &headers, base, "CREATE TABLE nodes (id BIGINT PRIMARY KEY, parent_id BIGINT)");
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), create.status);
    const table_id = try awaitTable(alloc, io, transport, &headers, base);
    const add_statement = "ALTER TABLE nodes ADD CONSTRAINT self_parent FOREIGN KEY (parent_id) REFERENCES nodes(id)";
    var add = try sql(alloc, transport, &headers, metadata_uri, add_statement);
    defer add.deinit(alloc);
    if (!activated) {
        try std.testing.expectEqual(@as(u16, 501), add.status);
        try std.testing.expect(std.mem.indexOf(u8, add.body, "0A000") != null);
        try std.testing.expect(std.mem.indexOf(u8, add.body, "No schema publication was admitted") != null);
        var unchanged = try table(alloc, transport, &headers, base);
        defer unchanged.deinit(alloc);
        try std.testing.expect(!try hasSelfFk(alloc, unchanged));
        return;
    }
    if (add.status != 202) std.debug.print("mounted self-FK ADD status={d} body={s}\n", .{ add.status, add.body });
    try std.testing.expectEqual(@as(u16, 202), add.status);
    try awaitPublication(alloc, io, &metadata, table_id);
    var added = try table(alloc, transport, &headers, base);
    defer added.deinit(alloc);
    try std.testing.expect(try hasSelfFk(alloc, added));
    var parent_row = try awaitBatch(alloc, io, transport, &headers, base, "{\"inserts\":{\"p\":{\"id\":1}},\"sync_level\":\"full_text\"}");
    defer parent_row.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), parent_row.status);
    var child_row = try awaitBatch(alloc, io, transport, &headers, base, "{\"inserts\":{\"c\":{\"id\":2,\"parent_id\":1}},\"sync_level\":\"full_text\"}");
    defer child_row.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), child_row.status);
    var blocked = try awaitBatch(alloc, io, transport, &headers, base, "{\"deletes\":[\"p\"],\"sync_level\":\"full_text\"}");
    defer blocked.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), blocked.status);
    var drop = try sql(alloc, transport, &headers, metadata_uri, "ALTER TABLE nodes DROP CONSTRAINT self_parent");
    defer drop.deinit(alloc);
    if (drop.status != 202) std.debug.print("mounted self-FK DROP status={d} body={s}\n", .{ drop.status, drop.body });
    try std.testing.expectEqual(@as(u16, 202), drop.status);
    try awaitPublication(alloc, io, &metadata, table_id);
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
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    data_live = true;
    try data.start();
    try awaitStoreRegistration(io, &data);
    data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_live = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_live = true;
    try data_control.start();
    const restarted_base = try data.baseUri(alloc);
    defer alloc.free(restarted_base);
    try std.testing.expectEqual(table_id, try awaitTable(alloc, io, transport, &headers, restarted_base));
    var after_drop = try table(alloc, transport, &headers, restarted_base);
    defer after_drop.deinit(alloc);
    try std.testing.expect(!try hasSelfFk(alloc, after_drop));
    var released = try awaitBatch(alloc, io, transport, &headers, restarted_base, "{\"deletes\":[\"p\"],\"sync_level\":\"full_text\"}");
    defer released.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), released.status);
}

test "mounted hosted self-FK public admission remains guarded" {
    try mountedSelfFk(false);
}

test "mounted hosted self-FK ADD DROP restart diagnostic" {
    try mountedSelfFk(true);
}
