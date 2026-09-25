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

fn awaitBatch(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, table: []const u8, body: []const u8) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/{s}/batch", .{ base, table });
    defer alloc.free(uri);
    for (0..600) |_| {
        var response = try transport.execute(alloc, .{ .method = .POST, .uri = uri, .headers = headers, .content_type = "application/json", .body = body, .timeout_ms = 3_000 });
        if (response.status != 503) return response;
        response.deinit(alloc);
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
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
    var data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    var data_raft_alive = true;
    defer if (data_raft_alive) data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    var data_control_alive = true;
    defer if (data_control_alive) data_control.deinit();
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
    const mounted_api = if (data.http_server) |*server| server else return error.Unavailable;
    const reader = mounted_api.table_reads orelse return error.Unavailable;
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
        var last_phase: publication.InitialPhase = .preparing_support;
        var last_revision: u64 = 0;
        var last_receipts: [5]usize = .{ 0, 0, 0, 0, 0 };
        var published = false;
        const publication_deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
        while (platform.time.monotonicNs() < publication_deadline) {
            const status_json = try source.systemCatalog(alloc, .{
                .deadline_ns = @min(publication_deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
                .fk_generation_publication_authority = true,
            }, .{ .fk_initial_create_status = child_table_id });
            defer alloc.free(status_json);
            var status = try std.json.parseFromSlice(Status, alloc, status_json, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            last_phase = status.value.phase;
            last_revision = status.value.revision;
            last_receipts = .{
                status.value.child_provisioned.len,
                status.value.parent_staged.len,
                status.value.parent_activated.len,
                status.value.parent_acknowledged.len,
                status.value.child_released.len,
            };
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
        if (!published) {
            var stalled_snapshot = try metadata.server.svc.adminSnapshot();
            defer metadata.server.svc.freeAdminSnapshot(&stalled_snapshot);
            const durable_json = try source.systemCatalog(alloc, .{
                .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
                .fk_generation_publication_authority = true,
            }, .{ .fk_initial_create_status = child_table_id });
            defer alloc.free(durable_json);
            var durable = try std.json.parseFromSlice(publication.InitialPublication, alloc, durable_json, .{ .ignore_unknown_fields = true });
            defer durable.deinit();
            for (stalled_snapshot.tables) |table| if (table.table_id == parent_table_id) {
                const pending_parent = durable.value.plan.parents[0];
                const after = pending_parent.support_after orelse pending_parent.table;
                std.debug.print("linked hosted initial FK stalled phase={s} revision={} receipts={any} schema_v={} read_v={} support_after_v={} matches_after={} v0_index={} v1_index={}\n", .{
                    @tagName(last_phase),                                       last_revision,                                                                                                         last_receipts,
                    try @import("tables.zig").schemaVersion(table.schema_json), if (table.read_schema_json.len == 0) @as(u32, 0) else try @import("tables.zig").schemaVersion(table.read_schema_json), try @import("tables.zig").schemaVersion(after.schema_json),
                    metadata_table_manager.tableDefinitionsEqual(table, after), std.mem.indexOf(u8, table.indexes_json, "full_text_index_v0") != null,                                                 std.mem.indexOf(u8, table.indexes_json, "full_text_index_v1") != null,
                });
                break;
            };
            for (stalled_snapshot.schema_progresses) |progress| if (progress.table_id == parent_table_id)
                std.debug.print("linked hosted parent schema progress node={} version={}\n", .{ progress.node_id, progress.schema_version });
            for (stalled_snapshot.placement_intents) |intent|
                std.debug.print("linked hosted placement group={} node={} store={}\n", .{ intent.record.group_id, intent.record.local_node_id, intent.store_id });
            const hidden_group_id = durable.value.plan.child_ranges[0].group_id;
            var hidden_status_seen = false;
            for (stalled_snapshot.merged_group_statuses) |status| if (status.group_id == hidden_group_id) {
                hidden_status_seen = true;
                std.debug.print("linked hosted hidden raft group={} leader_known={} leader_store={} healthy_voters={}\n", .{
                    hidden_group_id, status.leader_known, status.leader_store_id, status.healthy_voter_reports,
                });
            };
            if (!hidden_status_seen) std.debug.print("linked hosted hidden raft group={} status=absent\n", .{hidden_group_id});
            const hidden_record = data.readHiddenInitialChildRecord(hidden_group_id, child_table_id) catch |err| blk: {
                std.debug.print("linked hosted hidden owner read err={s}\n", .{@errorName(err)});
                break :blk null;
            };
            if (hidden_record) |record|
                std.debug.print("linked hosted hidden owner phase={s} provision={}/{}\n", .{ @tagName(record.phase), record.provision_term, record.provision_index })
            else
                std.debug.print("linked hosted hidden owner record=absent\n", .{});
            for (stalled_snapshot.stores) |store| for (store.runtime_statuses) |runtime| {
                if (runtime.table_id != parent_table_id) continue;
                std.debug.print("linked hosted parent runtime node={} group={} freshness={s} identity_live={} indexes={}\n", .{
                    store.node_id, runtime.group_id, runtime.freshness, runtime.doc_identity.live_ordinals, runtime.indexes.len,
                });
                for (runtime.indexes) |index| if (std.mem.eql(u8, index.name, "full_text_index_v1"))
                    std.debug.print("linked hosted parent v1 index docs={} backfill={} replay={}/{} load_error={s}\n", .{
                        index.doc_count, index.backfill_active, index.replay_applied_sequence, index.replay_target_sequence, index.load_error orelse "none",
                    });
            };
        }
        try std.testing.expect(published);
        var visible = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&visible);
        try std.testing.expectEqual(@as(usize, 2), visible.tables.len);
        var child_name: ?[]const u8 = null;
        for (visible.tables) |table| if (table.table_id == child_table_id) {
            child_name = table.name;
            break;
        };
        try std.testing.expect(child_name != null);
        const parent_support = for (visible.tables) |table| {
            if (table.table_id == parent_table_id) break table;
        } else return error.TestExpectedParentSupport;
        try std.testing.expectEqual(@as(usize, 0), parent_support.read_schema_json.len);
        try std.testing.expect(std.mem.indexOf(u8, parent_support.indexes_json, "full_text_index_v1") != null);
        const child_group = for (visible.ranges) |range| {
            if (range.table_id == child_table_id) break range.group_id;
        } else return error.TestExpectedPublishedChildRange;
        const released_before = (try data.readHiddenInitialChildRecord(child_group, child_table_id)) orelse return error.TestExpectedHiddenChildReceipt;
        try std.testing.expectEqual(@import("../storage/db/relational_initial_child_publication.zig").Phase.released, released_before.phase);
        const parent_probe_started_ns = platform.time.monotonicNs();
        var parent_catalog_probe = (try reader.lookup(alloc, parent_name.?, "", .{
            .relational_integrity_catalog = true,
            .execution_deadline_ns = parent_probe_started_ns +| 5 * std.time.ns_per_s,
        }, .read_index)) orelse return error.TestExpectedParentIntegrityCatalog;
        defer parent_catalog_probe.deinit(alloc);
        std.debug.print("linked hosted parent integrity catalog read-index elapsed_ms={}\n", .{(platform.time.monotonicNs() -| parent_probe_started_ns) / std.time.ns_per_ms});
        const parent_metadata_started_ns = platform.time.monotonicNs();
        var parent_metadata = try transport.execute(alloc, .{ .method = .GET, .uri = parent_uri, .headers = &headers, .timeout_ms = 5_000 });
        defer parent_metadata.deinit(alloc);
        std.debug.print("linked hosted parent metadata GET status={} elapsed_ms={}\n", .{ parent_metadata.status, (platform.time.monotonicNs() -| parent_metadata_started_ns) / std.time.ns_per_ms });
        try std.testing.expectEqual(@as(u16, 200), parent_metadata.status);
        const parent_batch_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/parents/batch", .{base});
        defer alloc.free(parent_batch_uri);
        const parent_insert_started_ns = platform.time.monotonicNs();
        var parent_insert = transport.execute(alloc, .{
            .method = .POST,
            .uri = parent_batch_uri,
            .headers = &headers,
            .content_type = "application/json",
            .body = "{\"inserts\":{\"parent-row\":{\"a\":1,\"b\":2}},\"sync_level\":\"full_text\"}",
            .timeout_ms = 10_000,
        }) catch |err| {
            std.debug.print("linked hosted parent insert err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| parent_insert_started_ns) / std.time.ns_per_ms });
            return err;
        };
        defer parent_insert.deinit(alloc);
        std.debug.print("linked hosted parent insert status={} elapsed_ms={}\n", .{ parent_insert.status, (platform.time.monotonicNs() -| parent_insert_started_ns) / std.time.ns_per_ms });
        try std.testing.expectEqual(@as(u16, 201), parent_insert.status);
        // Probe the exact indexed read used by MATCH PARTIAL before issuing a
        // mutation. This distinguishes an unavailable support index from a
        // later transaction-coordination stall without retrying a write whose
        // outcome is uncertain.
        var parent_epoch = try std.json.parseFromSlice(struct { schema_version: u32 }, alloc, parent_catalog_probe.json, .{ .ignore_unknown_fields = true });
        defer parent_epoch.deinit();
        var parent_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, parent_support.schema_json);
        defer parent_schema.deinit(alloc);
        const Condition = struct { column: []const u8, op: []const u8 = "eq", value: std.json.Value };
        const conditions = [_]Condition{.{ .column = "a", .value = .{ .integer = 1 } }};
        const support = try @import("../schema/relational_witness_indexes.zig").select(alloc, parent_schema, &conditions);
        defer alloc.free(support.values);
        const scan_query = try std.json.Stringify.valueAlloc(alloc, .{
            .schema_version = parent_epoch.value.schema_version,
            .fields = &.{ "a", "b" },
            .index = support.name,
            .lower = .{ .values = support.values },
            .upper = .{ .values = support.values },
            .conditions = &conditions,
        }, .{});
        defer alloc.free(scan_query);
        const support_scan_started_ns = platform.time.monotonicNs();
        const support_scan = reader.scan(alloc, parent_name.?, "", "", .{
            .relational_query_json = scan_query,
            .include_documents = true,
            .limit = 4096,
            .execution_deadline_ns = support_scan_started_ns +| 5 * std.time.ns_per_s,
        }, .read_index) catch |err| {
            std.debug.print("linked hosted parent support scan index={s} err={s} elapsed_ms={}\n", .{ support.name, @errorName(err), (platform.time.monotonicNs() -| support_scan_started_ns) / std.time.ns_per_ms });
            return err;
        };
        if (support_scan) |value| {
            var response = value;
            defer response.deinit(alloc);
            std.debug.print("linked hosted parent support scan index={s} rows={} elapsed_ms={}\n", .{ support.name, std.mem.count(u8, response.ndjson, "\n"), (platform.time.monotonicNs() -| support_scan_started_ns) / std.time.ns_per_ms });
        } else return error.TestExpectedParentSupportScan;
        const child_target = try @import("../system_catalog/domain.zig").Target.literal("children");
        const direct_resolve_started_ns = platform.time.monotonicNs();
        const direct_resolve = source.systemCatalog(alloc, .{ .deadline_ns = direct_resolve_started_ns +| 5 * std.time.ns_per_s }, .{ .resolve = child_target }) catch |err| {
            std.debug.print("linked hosted direct child resolve err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| direct_resolve_started_ns) / std.time.ns_per_ms });
            return err;
        };
        defer alloc.free(direct_resolve);
        std.debug.print("linked hosted direct child resolve bytes={} elapsed_ms={}\n", .{ direct_resolve.len, (platform.time.monotonicNs() -| direct_resolve_started_ns) / std.time.ns_per_ms });
        const direct_status_started_ns = platform.time.monotonicNs();
        const direct_status = source.systemCatalog(alloc, .{ .deadline_ns = direct_status_started_ns +| 5 * std.time.ns_per_s }, .{ .table_status = .{ .logical = child_target } }) catch |err| {
            std.debug.print("linked hosted direct child table_status err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| direct_status_started_ns) / std.time.ns_per_ms });
            return err;
        };
        defer alloc.free(direct_status);
        std.debug.print("linked hosted direct child table_status bytes={} elapsed_ms={}\n", .{ direct_status.len, (platform.time.monotonicNs() -| direct_status_started_ns) / std.time.ns_per_ms });
        const routed_resolve_started_ns = platform.time.monotonicNs();
        const routed_resolve = mounted_api.source.systemCatalog(alloc, .{ .deadline_ns = routed_resolve_started_ns +| 5 * std.time.ns_per_s }, .{ .resolve = child_target }) catch |err| {
            std.debug.print("linked hosted routed child resolve err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| routed_resolve_started_ns) / std.time.ns_per_ms });
            return err;
        };
        defer alloc.free(routed_resolve);
        std.debug.print("linked hosted routed child resolve bytes={} elapsed_ms={}\n", .{ routed_resolve.len, (platform.time.monotonicNs() -| routed_resolve_started_ns) / std.time.ns_per_ms });
        const child_metadata_started_ns = platform.time.monotonicNs();
        var child_metadata = try transport.execute(alloc, .{ .method = .GET, .uri = child_uri, .headers = &headers, .timeout_ms = 5_000 });
        defer child_metadata.deinit(alloc);
        std.debug.print("linked hosted child metadata GET status={} elapsed_ms={}\n", .{ child_metadata.status, (platform.time.monotonicNs() -| child_metadata_started_ns) / std.time.ns_per_ms });
        try std.testing.expectEqual(@as(u16, 200), child_metadata.status);
        const validation_started_ns = platform.time.monotonicNs();
        mounted_api.validateTableWritesAgainstSchemaWithContext(.{ .deadline_ns = validation_started_ns +| 5 * std.time.ns_per_s }, child_name.?, &.{@import("../storage/db/types.zig").BatchWrite{
            .key = "valid-child",
            .value = "{\"id\":7,\"pa\":1,\"pb\":null}",
        }}) catch |err| {
            std.debug.print("linked hosted valid child validation err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| validation_started_ns) / std.time.ns_per_ms });
            return err;
        };
        std.debug.print("linked hosted valid child validation elapsed_ms={}\n", .{(platform.time.monotonicNs() -| validation_started_ns) / std.time.ns_per_ms});
        // Preparation is read-only. If this bounded probe completes, the
        // remaining public timeout is in authorization or distributed commit.
        const preparation_started_ns = platform.time.monotonicNs();
        var prepared = @import("relational_integrity_commit.zig").prepareWithCoverageControlled(alloc, reader, visible.tables, visible.ranges, &.{.{
            .table_name = child_name.?,
            .writes = &.{.{ .key = "valid-child", .value = "{\"id\":7,\"pa\":1,\"pb\":null}" }},
        }}, .{ .deadline_ns = preparation_started_ns +| 5 * std.time.ns_per_s }) catch |err| {
            std.debug.print("linked hosted valid child preparation err={s} elapsed_ms={}\n", .{ @errorName(err), (platform.time.monotonicNs() -| preparation_started_ns) / std.time.ns_per_ms });
            return err;
        };
        defer prepared.deinit();
        std.debug.print("linked hosted valid child preparation participants={} elapsed_ms={}\n", .{ prepared.tables.len, (platform.time.monotonicNs() -| preparation_started_ns) / std.time.ns_per_ms });
        const valid_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/children/batch", .{base});
        defer alloc.free(valid_uri);
        const valid_started_ns = platform.time.monotonicNs();
        var valid_child = transport.execute(alloc, .{
            .method = .POST,
            .uri = valid_uri,
            .headers = &headers,
            .content_type = "application/json",
            .body = "{\"inserts\":{\"valid-child\":{\"id\":7,\"pa\":1,\"pb\":null}}}",
            .timeout_ms = 10_000,
        }) catch |err| {
            const observed_owner = data.readHiddenInitialChildRecord(child_group, child_table_id) catch null;
            std.debug.print("linked hosted valid MATCH PARTIAL insert err={s} elapsed_ms={} metadata_phase={s} owner_phase={s}\n", .{
                @errorName(err),
                (platform.time.monotonicNs() -| valid_started_ns) / std.time.ns_per_ms,
                @tagName(last_phase),
                if (observed_owner) |record| @tagName(record.phase) else "unavailable",
            });
            return err;
        };
        defer valid_child.deinit(alloc);
        if (valid_child.status != 201) std.debug.print("linked hosted valid MATCH PARTIAL insert status={} elapsed_ms={} body={s}\n", .{ valid_child.status, (platform.time.monotonicNs() -| valid_started_ns) / std.time.ns_per_ms, valid_child.body });
        // A distributed commit can be durable before all participants report
        // propagation. A 202 is not an invitation to replay the mutation:
        // wait for an authoritative public read of the committed row instead.
        try std.testing.expect(valid_child.status == 201 or valid_child.status == 202);
        if (valid_child.status == 202) {
            var acknowledgement = try std.json.parseFromSlice(struct { status: []const u8 }, alloc, valid_child.body, .{ .ignore_unknown_fields = true });
            defer acknowledgement.deinit();
            try std.testing.expectEqualStrings("committed_pending", acknowledgement.value.status);
        }
        const lookup_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/children/documents/valid-child", .{base});
        defer alloc.free(lookup_uri);
        const visible_deadline_ns = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
        while (true) {
            var lookup = try transport.execute(alloc, .{ .method = .GET, .uri = lookup_uri, .headers = &headers, .timeout_ms = 3_000 });
            const status = lookup.status;
            lookup.deinit(alloc);
            if (status == 200) break;
            if (status != 404 and status != 503) return error.ValidChildLookupFailed;
            if (platform.time.monotonicNs() >= visible_deadline_ns) return error.ValidChildVisibilityTimeout;
            try io.sleep(.fromMilliseconds(20), .awake);
        }
        var orphan_child = try awaitBatch(alloc, io, transport, &headers, base, "children", "{\"inserts\":{\"orphan-child\":{\"id\":8,\"pa\":999,\"pb\":null}},\"sync_level\":\"full_text\"}");
        defer orphan_child.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), orphan_child.status);
        var blocked_parent_delete = try awaitBatch(alloc, io, transport, &headers, base, "parents", "{\"deletes\":[\"parent-row\"],\"sync_level\":\"full_text\"}");
        defer blocked_parent_delete.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), blocked_parent_delete.status);
        const unauthorized_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/unauthorized-children", .{base});
        defer alloc.free(unauthorized_uri);
        var unauthorized = try transport.execute(alloc, .{ .method = .POST, .uri = unauthorized_uri, .content_type = "application/json", .body = child_body });
        defer unauthorized.deinit(alloc);
        try std.testing.expect(unauthorized.status == 401 or unauthorized.status == 403);
        const limited_claims = try std.fmt.allocPrint(alloc,
            \\{{"iss":"{s}","sub":"user:hosted-fk-limited","tenant":"test","admin":false,"iat":{d},"exp":{d}}}
        , .{ issuer, now, now + 3600 });
        defer alloc.free(limited_claims);
        const limited_token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, limited_claims);
        defer alloc.free(limited_token);
        const limited_headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = limited_token }};
        var limited = try transport.execute(alloc, .{ .method = .POST, .uri = unauthorized_uri, .headers = &limited_headers, .content_type = "application/json", .body = child_body });
        defer limited.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 403), limited.status);
        var after_denial = try metadata.server.svc.adminSnapshot();
        defer metadata.server.svc.freeAdminSnapshot(&after_denial);
        try std.testing.expectEqual(@as(usize, 2), after_denial.tables.len);

        data_control.deinit();
        data_control_alive = false;
        data_raft.deinit();
        data_raft_alive = false;
        data.deinit();
        data_alive = false;
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
        data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
        data_raft_alive = true;
        try data_raft.start();
        data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
        data_control_alive = true;
        try data_control.start();
        const released_after = for (0..200) |_| {
            const value = data.readHiddenInitialChildRecord(child_group, child_table_id) catch |err| switch (err) {
                error.StorageReadTemporarilyUnavailable => {
                    try io.sleep(.fromMilliseconds(10), .awake);
                    continue;
                },
                else => return err,
            };
            if (value) |record| break record;
            try io.sleep(.fromMilliseconds(10), .awake);
        } else return error.TestExpectedRecoveredHiddenChildReceipt;
        try std.testing.expectEqual(@import("../storage/db/relational_initial_child_publication.zig").Phase.released, released_after.phase);
        try std.testing.expectEqual(released_before.provision_term, released_after.provision_term);
        try std.testing.expectEqual(released_before.provision_index, released_after.provision_index);
        const restarted_base = try data.baseUri(alloc);
        defer alloc.free(restarted_base);
        const restarted_lookup_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/children/documents/valid-child", .{restarted_base});
        defer alloc.free(restarted_lookup_uri);
        const recovered_visible_deadline_ns = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
        while (true) {
            var lookup = try transport.execute(alloc, .{ .method = .GET, .uri = restarted_lookup_uri, .headers = &headers, .timeout_ms = 3_000 });
            const status = lookup.status;
            lookup.deinit(alloc);
            if (status == 200) break;
            if (status != 404 and status != 503) return error.RecoveredChildLookupFailed;
            if (platform.time.monotonicNs() >= recovered_visible_deadline_ns) return error.RecoveredChildVisibilityTimeout;
            try io.sleep(.fromMilliseconds(20), .awake);
        }
        var still_orphan = try awaitBatch(alloc, io, transport, &headers, restarted_base, "children", "{\"inserts\":{\"orphan-after-restart\":{\"id\":9,\"pa\":999,\"pb\":null}},\"sync_level\":\"full_text\"}");
        defer still_orphan.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), still_orphan.status);
        var still_blocked = try awaitBatch(alloc, io, transport, &headers, restarted_base, "parents", "{\"deletes\":[\"parent-row\"],\"sync_level\":\"full_text\"}");
        defer still_blocked.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), still_blocked.status);
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

    // Exercise the private publication protocol while public MATCH PARTIAL
    // CREATE remains guarded. This uses the same admitted plan and supervisor
    // as ingress, without making an unproven route available to users.
    var internal_request = try @import("tables.zig").parseCreateTableRequest(alloc, child_body);
    defer internal_request.deinit(alloc);
    // Public ingress resolves the logical FK target before planning. The
    // internal test bypasses ingress, so provide the same physical target.
    const physical_parent = try std.fmt.allocPrint(alloc, "\"parent_table\":\"{s}\"", .{parent_name.?});
    defer alloc.free(physical_parent);
    const resolved_schema = try std.mem.replaceOwned(u8, alloc, internal_request.schema_json.?, "\"parent_table\":\"parents\"", physical_parent);
    alloc.free(internal_request.schema_json.?);
    internal_request.schema_json = resolved_schema;
    const begin = if (data.http_server) |*server| try server.beginFkInitialCreate(
        alloc,
        .{ .deadline_ns = platform.time.monotonicNs() +| 20 * std.time.ns_per_s },
        null,
        try @import("../system_catalog/domain.zig").Target.literal("children"),
        internal_request,
    ) else return error.Unavailable;
    try std.testing.expect(begin.state == .pending);
    const private_source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const private_deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    var private_phase: @import("../metadata/fk_generation_publication.zig").InitialPhase = .preparing_support;
    var private_receipts: usize = 0;
    while (platform.time.monotonicNs() < private_deadline) {
        const status_json = try private_source.systemCatalog(alloc, .{
            .deadline_ns = @min(private_deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
            .fk_generation_publication_authority = true,
        }, .{ .fk_initial_create_status = begin.child_table_id });
        defer alloc.free(status_json);
        var status = try std.json.parseFromSlice(@import("../metadata/fk_generation_publication.zig").InitialPublication, alloc, status_json, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        private_phase = status.value.phase;
        private_receipts = status.value.child_provisioned.len;
        if (private_phase == .published) break;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    if (private_phase != .published)
        std.debug.print("hosted private initial FK stalled phase={s} child_receipts={}\n", .{ @tagName(private_phase), private_receipts });
    try std.testing.expectEqual(@import("../metadata/fk_generation_publication.zig").InitialPhase.published, private_phase);
    var published_snapshot = try metadata.server.svc.adminSnapshot();
    defer metadata.server.svc.freeAdminSnapshot(&published_snapshot);
    try std.testing.expectEqual(@as(usize, 2), published_snapshot.tables.len);
    const published_child = for (published_snapshot.tables) |table| {
        if (table.table_id == begin.child_table_id) break table;
    } else return error.TestExpectedPublishedChild;
    try std.testing.expect(published_child.name.len != 0);
    const private_group = for (published_snapshot.ranges) |range| {
        if (range.table_id == begin.child_table_id) break range.group_id;
    } else return error.TestExpectedPublishedChildRange;
    const hidden_record = (try data.readHiddenInitialChildRecord(private_group, begin.child_table_id)) orelse
        return error.TestExpectedHiddenChildReceipt;
    try std.testing.expect(hidden_record.phase == .released);
    try std.testing.expect(hidden_record.provision_term != 0 and hidden_record.provision_index != 0);

    // A completed owner receipt and the published table must survive loss of
    // the data process, not merely a live coordinator cursor. Stop workers
    // before closing the owner, then reopen the same Raft and LSM roots.
    data_control.deinit();
    data_control_alive = false;
    data_raft.deinit();
    data_raft_alive = false;
    data.deinit();
    data_alive = false;
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
    data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_alive = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_alive = true;
    try data_control.start();
    const recovered = for (0..200) |_| {
        const value = data.readHiddenInitialChildRecord(private_group, begin.child_table_id) catch |err| switch (err) {
            error.StorageReadTemporarilyUnavailable => {
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        };
        if (value) |record| break record;
        try io.sleep(.fromMilliseconds(10), .awake);
    } else return error.TestExpectedRecoveredHiddenChildReceipt;
    try std.testing.expect(recovered.phase == .released);
    try std.testing.expectEqual(hidden_record.provision_term, recovered.provision_term);
    try std.testing.expectEqual(hidden_record.provision_index, recovered.provision_index);
}
