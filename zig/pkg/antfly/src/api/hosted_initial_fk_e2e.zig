// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Mounted metadata/data owner setup for hosted initial-FK publication.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
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

test "mounted hosted FK parent owner is read-index ready" {
    const alloc = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(alloc);
    const internal_secret = "hosted-fk-internal-service-secret-v1";
    const trusted_secret = "hosted-fk-trusted-principal-secret-v1";
    const issuer = "hosted-fk-test";
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
        .metadata_group_id = 2193,
        .replica_root_dir = meta_root,
        .replica_catalog_path = meta_catalog,
        .snapshot_root_dir = snapshots,
        .observe_local_replica_root = true,
        .api_server_cfg = .{
            .trusted_principal_secret = trusted_secret,
            .trusted_principal_issuer = issuer,
            .internal_service_secret = internal_secret,
            .internal_service_issuer = issuer,
            .internal_service_auth_capability = "v1; mode=enforce",
        },
    });
    defer metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2193, 1);
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
        .api_server_cfg = .{
            .deployment_mode = .distributed,
            .trusted_principal_secret = trusted_secret,
            .trusted_principal_issuer = issuer,
            .internal_service_secret = internal_secret,
            .internal_service_issuer = issuer,
            .internal_service_auth_capability = "v1; mode=enforce",
        },
    }, metadata_uri);
    defer data.deinit();
    try data.start();
    const owner_transport = data.data_raft.?.host.http_host.request_executor;
    try std.testing.expect(data.http_server.?.cfg.session_executor != null);
    try std.testing.expect(data.http_server.?.cfg.session_executor.?.ptr == owner_transport.ptr);
    for (0..32) |_| {
        data.registerNodeIfConfigured() catch |err| switch (err) {
            error.StoreRegistrationNotVisible => {
                try io.sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
        break;
    } else return error.StoreRegistrationNotVisible;
    var data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    defer data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    defer data_control.deinit();
    try data_control.start();

    const base = try data.baseUri(alloc);
    defer alloc.free(base);
    var executor = executor_mod.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const transport = executor.executor();
    const now: i64 = @intCast(@divFloor(platform.time.realtimeNs(), std.time.ns_per_s));
    const claims = try std.fmt.allocPrint(alloc,
        \\{{"iss":"{s}","sub":"user:hosted-fk-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};
    const parent_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/parents", .{base});
    defer alloc.free(parent_uri);
    const parent_body =
        \\{"num_shards":1,"schema":{"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"parent_key","columns":["a","b"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}}}}}
    ;
    var created = try transport.execute(alloc, .{ .method = .POST, .uri = parent_uri, .headers = &headers, .content_type = "application/json", .body = parent_body });
    defer created.deinit(alloc);
    if (created.status != 200) std.debug.print("linked hosted parent CREATE status={} body={s}\n", .{ created.status, created.body });
    try std.testing.expectEqual(@as(u16, 200), created.status);

    var parent_name: ?[]u8 = null;
    defer if (parent_name) |name| alloc.free(name);
    var parent_start: ?[]u8 = null;
    defer if (parent_start) |key| alloc.free(key);
    var parent_table_id: u64 = 0;
    var parent_shard_id: u64 = 0;
    var parent_range_id: u64 = 0;
    for (0..600) |_| {
        try data.runStoreStatusRoundOnly();
        var snapshot = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&snapshot);
        if (snapshot.tables.len == 1 and snapshot.ranges.len == 1) {
            parent_name = try alloc.dupe(u8, snapshot.tables[0].name);
            parent_start = try alloc.dupe(u8, snapshot.ranges[0].start_key);
            parent_table_id = snapshot.tables[0].table_id;
            parent_shard_id = metadata_table_manager.rangeDocIdentityShardId(snapshot.ranges[0]);
            parent_range_id = metadata_table_manager.rangeDocIdentityRangeId(snapshot.ranges[0]);
            break;
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    try std.testing.expect(parent_name != null);
    const reader = if (data.http_server) |*server| server.table_reads orelse return error.Unavailable else return error.Unavailable;
    var ready = false;
    var last_error: ?anyerror = null;
    for (0..128) |_| {
        const observed = reader.lookup(alloc, parent_name.?, parent_start.?, .{
            .relational_topology_json = "{\"mode\":\"identity\"}",
            .execution_deadline_ns = platform.time.monotonicNs() +| 500 * std.time.ns_per_ms,
        }, .read_index) catch |err| {
            last_error = err;
            try io.sleep(.fromMilliseconds(20), .awake);
            continue;
        };
        if (observed) |value| {
            var response = value;
            defer response.deinit(alloc);
            const Identity = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace, catalog_digest: [32]u8, next_epoch: u64 };
            var identity = try std.json.parseFromSlice(Identity, alloc, response.json, .{ .ignore_unknown_fields = true });
            defer identity.deinit();
            try std.testing.expectEqual(parent_table_id, identity.value.namespace.table_id);
            try std.testing.expectEqual(parent_shard_id, identity.value.namespace.shard_id);
            try std.testing.expectEqual(parent_range_id, identity.value.namespace.range_id);
            ready = true;
            break;
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    if (!ready) std.debug.print("linked hosted parent read-index unavailable err={s}\n", .{if (last_error) |err| @errorName(err) else "none"});
    try std.testing.expect(ready);

    // A rejected initial MATCH PARTIAL create must not leave a hidden child
    // or an orphaned generation publication behind. Keep this assertion even
    // while the coordinated publication path remains publicly guarded.
    const child_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/children", .{base});
    defer alloc.free(child_uri);
    const child_body =
        \\{"num_shards":1,"schema":{"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"partial_parent","child_columns":["pa","pb"],"parent_table":"parents","parent_columns":["a","b"],"match":"partial"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"pa":{"type":"integer","nullable":true},"pb":{"type":"integer","nullable":true}},"required":["id"],"additionalProperties":false}}}}}
    ;
    var child_response = try transport.execute(alloc, .{
        .method = .POST,
        .uri = child_uri,
        .headers = &headers,
        .content_type = "application/json",
        .body = child_body,
    });
    defer child_response.deinit(alloc);
    if (child_response.status == 202) {
        const publication = @import("../metadata/fk_generation_publication.zig");
        const Accepted = struct { table_id: []const u8, publication_id: []const u8, state: []const u8 };
        var accepted = try std.json.parseFromSlice(Accepted, alloc, child_response.body, .{ .ignore_unknown_fields = true });
        defer accepted.deinit();
        try std.testing.expectEqualStrings("pending", accepted.value.state);
        try std.testing.expectEqual(@as(usize, 32), accepted.value.publication_id.len);
        const child_table_id = try std.fmt.parseInt(u64, accepted.value.table_id, 10);
        const Status = struct {
            revision: u64,
            phase: publication.InitialPhase,
            child_provisioned: []const publication.Receipt = &.{},
            parent_staged: []const publication.Receipt = &.{},
            parent_activated: []const publication.Receipt = &.{},
            parent_acknowledged: []const publication.Receipt = &.{},
            child_released: []const publication.Receipt = &.{},
        };
        const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
        var last_phase: publication.InitialPhase = .provisioning_child;
        var last_revision: u64 = 0;
        var published = false;
        for (0..1500) |_| {
            const status_json = try source.systemCatalog(alloc, .{
                .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
                .fk_generation_publication_authority = true,
            }, .{ .fk_initial_create_status = child_table_id });
            defer alloc.free(status_json);
            var status = try std.json.parseFromSlice(Status, alloc, status_json, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            last_phase = status.value.phase;
            last_revision = status.value.revision;
            if (last_phase == .published) {
                try std.testing.expectEqual(@as(usize, 1), status.value.child_provisioned.len);
                try std.testing.expectEqual(@as(usize, 1), status.value.parent_staged.len);
                try std.testing.expectEqual(@as(usize, 1), status.value.parent_activated.len);
                try std.testing.expectEqual(@as(usize, 1), status.value.parent_acknowledged.len);
                try std.testing.expectEqual(@as(usize, 1), status.value.child_released.len);
                published = true;
                break;
            }
            try io.sleep(.fromMilliseconds(20), .awake);
        }
        if (!published) std.debug.print("linked hosted initial FK stalled phase={s} revision={}\n", .{ @tagName(last_phase), last_revision });
        try std.testing.expect(published);
        var visible = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&visible);
        try std.testing.expectEqual(@as(usize, 2), visible.tables.len);
        var child_visible = false;
        for (visible.tables) |table| if (table.table_id == child_table_id) {
            child_visible = true;
            break;
        };
        try std.testing.expect(child_visible);
        return;
    }
    if (child_response.status != 422) std.debug.print("linked hosted child guard status={} body={s}\n", .{ child_response.status, child_response.body });
    try std.testing.expectEqual(@as(u16, 422), child_response.status);
    const GuardError = struct { @"error": []const u8 };
    var guard_error = try std.json.parseFromSlice(GuardError, alloc, child_response.body, .{ .ignore_unknown_fields = true });
    defer guard_error.deinit();
    try std.testing.expectEqualStrings(
        "initial MATCH PARTIAL foreign keys require atomic parent support-index publication; create the table without that constraint, then add it with ALTER TABLE",
        guard_error.value.@"error",
    );

    var after_rejection = try metadata.server.svc.adminSnapshot();
    defer metadata.server.svc.freeAdminSnapshot(&after_rejection);
    try std.testing.expectEqual(@as(usize, 1), after_rejection.tables.len);
    try std.testing.expectEqual(parent_table_id, after_rejection.tables[0].table_id);
    const work_json = try http_server.StatusSource.fromMetadataHttpService(metadata.server.svc).systemCatalog(alloc, .{
        .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
        .fk_generation_publication_authority = true,
    }, .{ .fk_initial_create_work = 0 });
    defer alloc.free(work_json);
    var work = try std.json.parseFromSlice(?@import("../metadata/fk_generation_publication.zig").InitialWork, alloc, work_json, .{});
    defer work.deinit();
    try std.testing.expect(work.value == null);
}
