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

fn awaitBatch(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, table: []const u8, body: []const u8) !http.HttpResponse {
    // Poll only read-only readiness. A 503 from a mutation is not proof that
    // admission failed, so never replay a write to wait for a cold owner.
    const status_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/{s}/constraints/status", .{ base, table });
    defer alloc.free(status_uri);
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        var response = transport.execute(alloc, .{ .method = .GET, .uri = status_uri, .headers = headers, .timeout_ms = 3_000 }) catch |err| switch (err) {
            error.Timeout, error.ConnectionRefused, error.ConnectionResetByPeer => {
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        defer response.deinit(alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(struct { state: []const u8, ranges: []const std.json.Value }, alloc, response.body, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.ranges.len > 0 and std.mem.eql(u8, parsed.value.state, "enforced")) break;
        } else if (response.status != 409 and response.status != 503 and response.status != 504) {
            return error.ConstraintCoverageUnavailable;
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    } else return error.ConstraintCoverageUnavailable;
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/{s}/batch", .{ base, table });
    defer alloc.free(uri);
    return transport.execute(alloc, .{ .method = .POST, .uri = uri, .headers = headers, .content_type = "application/json", .body = body, .timeout_ms = 15_000 });
}

pub fn mountedInitialFault(leadership_transfer: bool) !void {
    return mountedInitialScenario(if (leadership_transfer) .transfer else .restart);
}

pub const Scenario = enum { restart, transfer, cancel_offline, published_obsolete };

pub fn mountedInitialScenario(scenario: Scenario) !void {
    const leadership_transfer = scenario != .restart;
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
    var metadata_alive = true;
    defer if (metadata_alive) metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2193, 1);
    var meta_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataRaft }, raft.RuntimeCadence.default_raft_tick_ms * std.time.ns_per_ms);
    var meta_raft_alive = true;
    defer if (meta_raft_alive) meta_raft.deinit();
    try meta_raft.start();
    var meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, raft.RuntimeCadence.default_control_tick_ms * std.time.ns_per_ms);
    var meta_control_alive = true;
    defer if (meta_control_alive) meta_control.deinit();
    try meta_control.start();
    for (0..600) |_| {
        if (try metadata.server.svc.metadataIncarnation() != null) break;
        try io.sleep(.fromMilliseconds(10), .awake);
    } else return error.MetadataIncarnationUnavailable;
    var metadata_uri = try metadata.adminBaseUri(alloc);
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
    var data_alive = true;
    defer if (data_alive) data.deinit();
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
    var data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, raft.RuntimeCadence.default_raft_tick_ms * std.time.ns_per_ms);
    var data_raft_alive = true;
    defer if (data_raft_alive) data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, raft.RuntimeCadence.default_control_tick_ms * std.time.ns_per_ms);
    var data_control_alive = true;
    defer if (data_control_alive) data_control.deinit();
    try data_control.start();

    const peers_api = @import("hosted_self_fk_integration_test.zig");
    var peers: [2]?*peers_api.DataPeer = .{ null, null };
    defer {
        if (meta_control_alive) {
            meta_control.deinit();
            meta_control_alive = false;
        }
        for (peers) |peer| if (peer) |active| active.destroy(alloc);
    }
    if (leadership_transfer) {
        peers[0] = try peers_api.DataPeer.create(alloc, process_alloc, io, root, metadata_uri, 10, trusted_secret, internal_secret, issuer);
        peers[1] = try peers_api.DataPeer.create(alloc, process_alloc, io, root, metadata_uri, 11, trusted_secret, internal_secret, issuer);
    }

    var base = try data.baseUri(alloc);
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
    // Hosted owner receipts require explicit administrator enrollment of the
    // physical store key; ordinary service registration cannot authorize it.
    const signing_root = try @import("antfly_local_sources").storage_db_root_signing_identity.load(alloc, io, data_root);
    const proof = try @import("../metadata/store_root_enrollment.zig").Request.sign(.{
        .metadata_incarnation = (try metadata.server.svc.metadataIncarnation()) orelse return error.MetadataIncarnationUnavailable,
        .node_id = 9,
        .store_id = 9,
        .root_incarnation = signing_root.root_incarnation,
        .public_key = signing_root.public_key,
    }, signing_root.seed);
    const proof_body = try @import("store_root_enrollment_http.zig").encodeAlloc(alloc, proof);
    defer alloc.free(proof_body);
    const enrollment_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/store-roots/enroll", .{base});
    defer alloc.free(enrollment_uri);
    var enrolled = try transport.execute(alloc, .{ .method = .POST, .uri = enrollment_uri, .headers = &headers, .content_type = "application/json", .body = proof_body });
    defer enrolled.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), enrolled.status);
    for (peers, 0..) |peer, index| if (peer) |active| {
        const peer_root = try @import("antfly_local_sources").storage_db_root_signing_identity.load(alloc, io, active.replica_root);
        const node_id: u64 = 10 + @as(u64, @intCast(index));
        const peer_proof = try @import("../metadata/store_root_enrollment.zig").Request.sign(.{
            .metadata_incarnation = proof.identity.metadata_incarnation,
            .node_id = node_id,
            .store_id = node_id,
            .root_incarnation = peer_root.root_incarnation,
            .public_key = peer_root.public_key,
        }, peer_root.seed);
        const peer_body = try @import("store_root_enrollment_http.zig").encodeAlloc(alloc, peer_proof);
        defer alloc.free(peer_body);
        var peer_enrolled = try transport.execute(alloc, .{ .method = .POST, .uri = enrollment_uri, .headers = &headers, .content_type = "application/json", .body = peer_body });
        defer peer_enrolled.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), peer_enrolled.status);
    };
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
    const mounted_api = if (data.http_server) |*server| server else return error.Unavailable;
    const reader = mounted_api.table_reads orelse return error.Unavailable;
    var ready = false;
    var last_error: ?anyerror = null;
    // A production-cadence election can exceed 128 short polling sleeps.
    // Bound this read-only readiness barrier by elapsed time instead.
    const ready_deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < ready_deadline) {
        const observed = reader.lookup(alloc, parent_name.?, parent_start.?, .{
            .relational_topology_json = "{\"mode\":\"identity\"}",
            .execution_deadline_ns = @min(ready_deadline, platform.time.monotonicNs() +| 500 * std.time.ns_per_ms),
        }, .read_index) catch |err| {
            last_error = err;
            try io.sleep(.fromMilliseconds(20), .awake);
            continue;
        };
        if (observed) |value| {
            var response = value;
            defer response.deinit(alloc);
            const Identity = struct { namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace, catalog_digest: [32]u8, next_epoch: u64 };
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
    const driver = http_server.ApiHttpServer.FkInitialCreateTestDriver;
    var meta_api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
    try driver.pauseBackground(meta_api, io);
    try driver.pauseBackground(&data.http_server.?, io);
    for (peers) |peer| if (peer) |active| try driver.pauseBackground(&active.server.http_server.?, io);
    var child_response = try transport.execute(alloc, .{
        .method = .POST,
        .uri = child_uri,
        .headers = &headers,
        .content_type = "application/json",
        .body = child_body,
    });
    defer child_response.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 202), child_response.status);
    var accepted = try std.json.parseFromSlice(struct { table_id: []const u8 }, alloc, child_response.body, .{ .ignore_unknown_fields = true });
    defer accepted.deinit();
    const child_id = try std.fmt.parseInt(u64, accepted.value.table_id, 10);
    var restarted = false;
    var transferred = false;
    var hidden_voters_ready = false;
    var offline_peer_index: ?usize = null;
    var lost_owner: usize = 0;
    var lost_metadata: usize = 0;
    for (0..24) |_| {
        const before = try position(alloc, &metadata, child_id);
        if (before.phase == .published or before.phase == .canceled) break;
        if (leadership_transfer and !hidden_voters_ready and before.phase == .provisioning_child) {
            _ = try peers_api.awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, before.group_id);
            hidden_voters_ready = true;
        }
        if (offline_peer_index == null and ((scenario == .cancel_offline and before.phase == .staging_parents) or
            (scenario == .published_obsolete and before.phase == .publishing_child)))
        {
            const leader = try peers_api.awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, before.group_id);
            const index: usize = if (peers[0].?.server.data_raft.?.raftStatus(before.group_id).?.id != leader) 0 else 1;
            const node_id: u64 = 10 + @as(u64, @intCast(index));
            const active = peers[index].?;
            const expected_phase: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Phase = if (scenario == .cancel_offline) .hidden else .released;
            const applied_deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < applied_deadline) {
                if (try active.server.readHiddenInitialChildRecord(before.group_id, child_id)) |cold| {
                    if (cold.phase == expected_phase) break;
                }
                try io.sleep(.fromMilliseconds(20), .awake);
            } else return error.OfflineReceiptNotApplied;
            // Drain is a metadata decision; the offline physical root remains
            // on disk and cannot receive the later logical cancellation.
            try metadata.server.svc.requestNodeShutdown(node_id);
            active.destroy(alloc);
            peers[index] = null;
            offline_peer_index = index;
            const removal_deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < removal_deadline) {
                var cut = try metadata.server.svc.adminSnapshot();
                defer metadata.server.svc.freeAdminSnapshot(&cut);
                const existing = for (cut.placement_intents) |intent| {
                    if (intent.record.group_id == before.group_id and intent.record.local_node_id == node_id) break intent;
                } else break;
                try metadata.server.svc.removeReplicaIntent(before.group_id, node_id, existing.record.metadata_version);
                try io.sleep(.fromMilliseconds(20), .awake);
            } else return error.OfflinePlacementRemovalTimeout;
            if (scenario == .cancel_offline) {
                const canceled = try meta_api.source.systemCatalog(alloc, .{
                    .deadline_ns = platform.time.monotonicNs() +| 10 * std.time.ns_per_s,
                    .setting_admin = true,
                    .fk_generation_publication_authority = true,
                }, .{ .fk_initial_create_mutate = .{
                    .plan_id = before.plan_id,
                    .child_table_id = child_id,
                    .expected_revision = before.revision,
                    .action = .cancel,
                } });
                alloc.free(canceled);
                continue;
            }
        }
        if (leadership_transfer and !transferred and before.phase == .published_hidden) {
            const old_leader = try peers_api.awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, before.group_id);
            try peers_api.transferOwnerLeadership(io, &data, .{ peers[0].?, peers[1].? }, before.group_id);
            const new_leader = try peers_api.awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, before.group_id);
            try std.testing.expect(old_leader != new_leader);
            try std.testing.expectEqualDeep(before, try position(alloc, &metadata, child_id));
            transferred = true;
        }
        if (!leadership_transfer and !restarted and before.phase == .published_hidden) {
            const cold_before = (try data.readHiddenInitialChildRecord(before.group_id, child_id)) orelse return error.MissingHiddenReceipt;
            try std.testing.expectEqual(@import("antfly_local_sources").storage_db_relational_initial_child_publication.Phase.hidden, cold_before.phase);
            data_control.deinit();
            data_control_alive = false;
            data_raft.deinit();
            data_raft_alive = false;
            data.deinit();
            data_alive = false;
            meta_control.deinit();
            meta_control_alive = false;
            meta_raft.deinit();
            meta_raft_alive = false;
            metadata.deinit();
            metadata_alive = false;
            metadata = try metadata_runtime.Server.init(process_alloc, .{
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
            metadata_alive = true;
            try metadata.start();
            try metadata.bootstrapLocal(2193, 1);
            meta_api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
            try driver.pauseBackground(meta_api, io);
            meta_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataRaft }, raft.RuntimeCadence.default_raft_tick_ms * std.time.ns_per_ms);
            meta_raft_alive = true;
            try meta_raft.start();
            meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, raft.RuntimeCadence.default_control_tick_ms * std.time.ns_per_ms);
            meta_control_alive = true;
            try meta_control.start();
            const reopen_deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < reopen_deadline) {
                const reopened = position(alloc, &metadata, child_id) catch {
                    try io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                };
                try std.testing.expectEqualDeep(before, reopened);
                break;
            } else return error.MetadataPublicationReopenTimeout;
            const next_metadata_uri = try metadata.adminBaseUri(alloc);
            alloc.free(metadata_uri);
            metadata_uri = next_metadata_uri;
            data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
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
            data_alive = true;
            try data.start();
            try driver.pauseBackground(&data.http_server.?, io);
            const registration_deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
            while (platform.time.monotonicNs() < registration_deadline) {
                data.registerNodeIfConfigured() catch |err| switch (err) {
                    error.StoreRegistrationNotVisible => {
                        try io.sleep(.fromMilliseconds(10), .awake);
                        continue;
                    },
                    else => return err,
                };
                break;
            } else return error.StoreRegistrationNotVisible;
            data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, raft.RuntimeCadence.default_raft_tick_ms * std.time.ns_per_ms);
            data_raft_alive = true;
            try data_raft.start();
            data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, raft.RuntimeCadence.default_control_tick_ms * std.time.ns_per_ms);
            data_control_alive = true;
            try data_control.start();
            const next_base = try data.baseUri(alloc);
            alloc.free(base);
            base = next_base;
            try std.testing.expectEqualDeep(before, try position(alloc, &metadata, child_id));
            driver.forgetVolatileCursor(meta_api);
            restarted = true;
        }
        // First commit the owner effect but discard its response before the
        // metadata CAS. Repeating the exact durable plan must be idempotent.
        try stepLost(alloc, io, &metadata, child_id, meta_api, before, .before_metadata_mutate, &.{ &meta_raft, &meta_control, &data_raft, &data_control });
        try std.testing.expectEqualDeep(before, try position(alloc, &metadata, child_id));
        lost_owner += 1;
        driver.forgetVolatileCursor(meta_api);
        try stepLost(alloc, io, &metadata, child_id, meta_api, before, .after_metadata_mutate, &.{ &meta_raft, &meta_control, &data_raft, &data_control });
        const after = try position(alloc, &metadata, child_id);
        try std.testing.expectEqual(before.revision + 1, after.revision);
        lost_metadata += 1;
        driver.forgetVolatileCursor(meta_api);
    } else return error.InitialPublicationDidNotFinish;
    const completed = try position(alloc, &metadata, child_id);
    if (scenario == .cancel_offline or scenario == .published_obsolete) {
        try std.testing.expectEqual(if (scenario == .cancel_offline) publication.InitialPhase.canceled else .published, completed.phase);
        const index = offline_peer_index orelse return error.OfflineReplicaWasNotStopped;
        const node_id: u64 = 10 + @as(u64, @intCast(index));
        const retired_ticket = (try pendingTicket(alloc, &metadata, node_id, completed.group_id)) orelse return error.MissingOfflineRetirementTicket;
        try std.testing.expectEqual(if (scenario == .cancel_offline) @as(@TypeOf(retired_ticket.replica.retirement_authority), .canceled_plan) else .published_obsolete, retired_ticket.replica.retirement_authority);
        // Keep a published obsolete slot draining until its exact old root is
        // ACKed; otherwise the planner may immediately create its replacement.
        if (scenario == .cancel_offline) try metadata.server.svc.cancelNodeShutdown(node_id);
        peers[index] = try peers_api.DataPeer.create(alloc, process_alloc, io, root, metadata_uri, node_id, trusted_secret, internal_secret, issuer);
        const returned = peers[index].?;
        try driver.pauseBackground(&returned.server.http_server.?, io);
        const root_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ returned.replica_root, completed.group_id });
        defer alloc.free(root_path);
        const retirement_deadline = platform.time.monotonicNs() +| 90 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < retirement_deadline) {
            try returned.raft_driver.checkFailure();
            try returned.control_driver.checkFailure();
            const pending = try pendingTicket(alloc, &metadata, node_id, completed.group_id);
            const exists = if (std.Io.Dir.cwd().statFile(io, root_path, .{ .follow_symlinks = false })) |_| true else |err| switch (err) {
                error.FileNotFound => false,
                else => return err,
            };
            const worker = @import("../data/fk_retirement_worker.zig");
            if (pending == null and !exists and try worker.blocksReplica(alloc, io, returned.replica_root, .{
                .group_id = completed.group_id,
                .replica_id = retired_ticket.replica.replica_id,
                .node_id = node_id,
                .root_generation = retired_ticket.replica.root_generation,
            })) break;
            try io.sleep(.fromMilliseconds(20), .awake);
        } else return error.OfflineRetirementAckTimeout;
        // Cleanup authority is exact: the existing published parent remains
        // public, while the canceled child never appears in the catalog.
        var cut = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&cut);
        var found_parent = false;
        var found_child = false;
        for (cut.tables) |table| {
            if (table.table_id == child_id) found_child = true;
            if (table.table_id == parent_table_id) found_parent = true;
        }
        try std.testing.expect(found_parent);
        try std.testing.expectEqual(scenario == .published_obsolete, found_child);
        if (scenario == .published_obsolete) {
            // The surviving current owner remains released. Retirement of a
            // historical root must neither cancel nor unlink this public root.
            const current = (try data.readHiddenInitialChildRecord(completed.group_id, child_id)) orelse return error.CurrentPublishedOwnerRemoved;
            try std.testing.expectEqual(@import("antfly_local_sources").storage_db_relational_initial_child_publication.Phase.released, current.phase);
            try std.testing.expect((try pendingTicket(alloc, &metadata, 9, completed.group_id)) == null);
            try metadata.server.svc.cancelNodeShutdown(node_id);
        }
        return;
    }
    try std.testing.expectEqual(publication.InitialPhase.published, completed.phase);
    try std.testing.expect((if (leadership_transfer) transferred and hidden_voters_ready else restarted) and lost_owner >= 5 and lost_metadata == lost_owner);
    try std.testing.expectEqual(@as(usize, 1), completed.released);
    if (leadership_transfer) _ = try peers_api.awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, completed.group_id);
    const released = (try data.readHiddenInitialChildRecord(completed.group_id, child_id)) orelse return error.MissingHiddenReceipt;
    try std.testing.expectEqual(@import("antfly_local_sources").storage_db_relational_initial_child_publication.Phase.released, released.phase);
    var visible = try metadata.server.svc.adminSnapshot();
    defer metadata.server.svc.freeAdminSnapshot(&visible);
    var children: usize = 0;
    for (visible.tables) |table| {
        if (table.table_id == child_id) children += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), children);
}

test "mounted initial FK lost replies and cold owner restart before release" {
    try mountedInitialFault(false);
}

const publication = @import("../metadata/fk_generation_publication.zig");
fn pendingTicket(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, store_id: u64, group_id: u64) !?@import("../metadata/fk_initial_retirement_contract.zig").Ticket {
    const store = metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try store.fkInitialRetirementTicketPageJson(alloc, 2193, .{ .store_id = store_id, .limit = 128 });
    defer alloc.free(bytes);
    var page = try std.json.parseFromSlice(@import("../metadata/fk_initial_retirement_wire.zig").PageResponse, alloc, bytes, .{});
    defer page.deinit();
    for (page.value.items) |ticket| if (ticket.replica.group_id == group_id) return ticket;
    return null;
}
const Position = struct {
    plan_id: publication.Id,
    phase: publication.InitialPhase,
    revision: u64,
    group_id: u64,
    released: usize,
};

fn position(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, child_id: u64) !Position {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const bytes = try source.systemCatalog(alloc, .{
        .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
        .fk_generation_publication_authority = true,
    }, .{ .fk_initial_create_status = child_id });
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(publication.InitialPublication, alloc, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return .{
        .plan_id = parsed.value.plan.id,
        .phase = parsed.value.phase,
        .revision = parsed.value.revision,
        .group_id = parsed.value.plan.child_ranges[0].group_id,
        .released = parsed.value.child_released.len,
    };
}

fn stepLost(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, child_id: u64, server: *http_server.ApiHttpServer, before: Position, fault: http_server.ApiHttpServer.FkInitialCreateTestDriver.Fault, drivers: []const *raft.ManagedProgressDriver) !void {
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        for (drivers, 0..) |progress, index| progress.checkFailure() catch |err| {
            std.debug.print("initial FK fault progress driver={} phase={s} error={s}\n", .{ index, @tagName(before.phase), @errorName(err) });
            return err;
        };
        http_server.ApiHttpServer.FkInitialCreateTestDriver.step(server, fault) catch |err| switch (err) {
            error.InjectedPublicationReplyLoss => return,
            error.MetadataLinearizableReadTimeout, error.ForeignKeyParentSchemaPending, error.GenerationAdmissionPending, error.RaftProposalPending, error.RaftGroupNotFound, error.NotLeader, error.NoLeader, error.ReadIndexNotReady, error.StoreRegistrationNotVisible => {
                try std.testing.expectEqualDeep(before, try position(alloc, metadata, child_id));
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            error.MetadataMutationOutcomeUnknown => {
                const after = try position(alloc, metadata, child_id);
                if (fault == .after_metadata_mutate and std.mem.eql(u8, &after.plan_id, &before.plan_id) and after.revision == before.revision + 1) return;
                return err;
            },
            else => {
                std.debug.print("initial FK fault phase={s} revision={} error={s}\n", .{ @tagName(before.phase), before.revision, @errorName(err) });
                diagnosePlacement(alloc, metadata, before);
                return err;
            },
        };
        return error.ExpectedInjectedPublicationReplyLoss;
    }
    diagnosePlacement(alloc, metadata, before);
    return error.InitialPublicationFaultTimeout;
}

fn diagnosePlacement(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, expected: Position) void {
    var projection = metadata.server.svc.captureProvisioningCatalog(alloc) catch |err| {
        std.debug.print("initial FK fault provisioning capture={s}\n", .{@errorName(err)});
        return;
    };
    defer projection.deinit(alloc);
    std.debug.print("initial FK fault provisioning tables={} ranges={} hidden_owners={} expected_group={}\n", .{ projection.tables.len, projection.ranges.len, projection.initial_fk_owners.len, expected.group_id });
    for (projection.initial_fk_owners) |owner| std.debug.print("initial FK fault hidden owner group={}\n", .{owner.child_group_id});
    var snapshot = metadata.server.svc.adminSnapshot() catch |err| {
        std.debug.print("initial FK fault admin snapshot={s}\n", .{@errorName(err)});
        return;
    };
    defer metadata.server.svc.freeAdminSnapshot(&snapshot);
    for (snapshot.placement_intents) |intent| std.debug.print("initial FK fault placement group={} node={} store={}\n", .{ intent.record.group_id, intent.record.local_node_id, intent.store_id });
}
