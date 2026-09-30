// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Mounted external-parent FK generation retirement through public SQL.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const staging = @import("../metadata/restore_staging.zig");
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
    return requestWithTimeout(alloc, transport, headers, base, suffix, method, body, 3_000);
}
fn requestWithTimeout(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8, method: http.Method, body: ?[]const u8, timeout_ms: u32) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, suffix });
    defer alloc.free(uri);
    const authorization = if (headers.len > 0 and std.ascii.eqlIgnoreCase(headers[0].name, "authorization")) headers[0].value else null;
    return transport.execute(alloc, .{ .method = method, .uri = uri, .authorization = authorization, .headers = if (authorization != null) headers[1..] else headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = timeout_ms });
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
fn batch(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, body: []const u8, timeout_ms: u32) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/batch", .{name});
    defer alloc.free(suffix);
    return requestWithTimeout(alloc, transport, headers, base, suffix, .POST, body, timeout_ms);
}
fn awaitEnforced(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}/constraints/status", .{name});
    defer alloc.free(path);
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        var response = try request(alloc, transport, headers, base, path, .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var status = try std.json.parseFromSlice(struct { state: []const u8, ranges: []const std.json.Value }, alloc, response.body, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            if (status.value.ranges.len != 0 and std.mem.eql(u8, status.value.state, "enforced")) return;
        } else if (response.status != 409 and response.status != 503 and response.status != 504) return error.ConstraintCoverageUnavailable;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ConstraintCoverageUnavailable;
}

fn job(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, id: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/restore/jobs/{s}", .{id});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
fn field(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.UnexpectedResponse;
    return value.object.get(name) orelse error.UnexpectedResponse;
}
fn tableId(alloc: std.mem.Allocator, response: http.HttpResponse) !u64 {
    if (response.status != 200) return error.UnexpectedTableResponse;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
    defer parsed.deinit();
    return std.fmt.parseUnsigned(u64, (try field(parsed.value, "table_id")).string, 10);
}
fn hasForeignKey(alloc: std.mem.Allocator, response: http.HttpResponse) !bool {
    if (response.status != 200) return error.UnexpectedTableResponse;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
    defer parsed.deinit();
    const schema = try field(parsed.value, "schema");
    if (schema != .object) return error.UnexpectedTableResponse;
    const keys = schema.object.get("foreign_keys") orelse return false;
    return keys == .array and keys.array.items.len != 0;
}
fn awaitTable(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !u64 {
    for (0..600) |_| {
        var response = try table(alloc, transport, headers, base, name);
        defer response.deinit(alloc);
        if (response.status == 200) return tableId(alloc, response);
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
}
fn awaitTableId(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, expected: u64) !void {
    for (0..600) |_| {
        var response = try table(alloc, transport, headers, base, name);
        defer response.deinit(alloc);
        if (response.status == 200) if (try tableId(alloc, response) == expected) return;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
}
const ParentRoute = struct {
    group_id: u64,
    table_name: []u8,

    fn deinit(self: ParentRoute, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
    }
};

fn awaitParentAcknowledged(alloc: std.mem.Allocator, io: std.Io, reader: @import("table_reads.zig").TableReadSource, parent: ParentRoute) ![32]u8 {
    const Receipt = struct { digest: [32]u8, term: u64, index: u64 };
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    var missing: usize = 0;
    var completed = false;
    var activation_index: u64 = 0;
    var acknowledged_index: u64 = 0;
    var matching = false;
    while (platform.time.monotonicNs() < deadline) {
        const lookup = reader.lookupGroupLocal(alloc, parent.group_id, parent.table_name, "parent-row", .{ .relational_topology_json = "{\"mode\":\"generation_publication\"}" }, .read_index) catch |err| {
            std.debug.print("hosted FK parent ACK lookup failed group_id={} err={s}\n", .{ parent.group_id, @errorName(err) });
            return err;
        };
        if (lookup) |value| {
            var response = value;
            defer response.deinit(alloc);
            var status = try std.json.parseFromSlice(struct {
                completed: ?std.json.Value,
                activation_receipt: ?Receipt,
                acknowledged_receipt: ?Receipt,
            }, alloc, response.json, .{ .ignore_unknown_fields = true });
            defer status.deinit();
            completed = status.value.completed != null;
            activation_index = if (status.value.activation_receipt) |receipt| receipt.index else 0;
            acknowledged_index = if (status.value.acknowledged_receipt) |receipt| receipt.index else 0;
            if (status.value.completed != null) {
                if (status.value.activation_receipt) |activation| {
                    if (status.value.acknowledged_receipt) |acknowledged| {
                        matching = std.mem.eql(u8, &activation.digest, &acknowledged.digest);
                        if (activation.term != 0 and activation.index != 0 and acknowledged.term != 0 and acknowledged.index != 0 and
                            std.mem.eql(u8, &activation.digest, &acknowledged.digest)) return acknowledged.digest;
                    }
                }
            }
        } else missing += 1;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    std.debug.print("hosted FK parent ACK missing group_id={} lookup_missing={} completed={} activation_index={} acknowledged_index={} matching={}\n", .{ parent.group_id, missing, completed, activation_index, acknowledged_index, matching });
    return error.ParentGenerationAcknowledgementMissing;
}
fn awaitPublication(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, child_id: u64, expect_fk: bool) !ParentRoute {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    var last_phase: publication.Phase = .fencing_child;
    while (platform.time.monotonicNs() < deadline) {
        const encoded = try source.systemCatalog(alloc, .{
            .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
            .fk_generation_publication_authority = true,
        }, .{ .fk_generation_publication_status = child_id });
        defer alloc.free(encoded);
        var status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        last_phase = status.value.phase;
        try status.value.validateState(alloc);
        if (status.value.phase == .publishing_child or status.value.phase == .installing_child or status.value.phase == .published) {
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_acknowledged.len);
        } else {
            // The child declaration stays unchanged while the parent has not
            // acknowledged its exact accepted generation.
            var snapshot = try metadata.server.svc.adminSnapshot();
            defer metadata.server.svc.freeAdminSnapshot(&snapshot);
            const current = for (snapshot.tables) |entry| {
                if (entry.table_id == child_id) break entry;
            } else return error.ChildTableMissing;
            const contains_fk = std.mem.indexOf(u8, current.schema_json, "child_parent") != null;
            try std.testing.expectEqual(!expect_fk, contains_fk);
        }
        if (status.value.phase == .published) {
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_staged.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_activated.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_acknowledged.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.child_installed.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.plan.parents.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.plan.parents[0].ranges.len);
            const parent = status.value.plan.parents[0];
            const range = parent.ranges[0];
            if (status.value.parent_acknowledged[0].group_id != range.group_id) return error.ParentAcknowledgementRouteChanged;
            var snapshot = try metadata.server.svc.adminSnapshot();
            defer metadata.server.svc.freeAdminSnapshot(&snapshot);
            var table_present = false;
            for (snapshot.tables) |entry| {
                if (entry.table_id == parent.table.table_id and std.mem.eql(u8, entry.name, parent.table.name)) {
                    table_present = true;
                    break;
                }
            }
            if (!table_present) return error.ParentCatalogRouteChanged;
            for (snapshot.ranges) |entry| {
                if (entry.table_id == parent.table.table_id and entry.group_id == range.group_id and entry.range_id == range.range_id) {
                    return .{ .group_id = range.group_id, .table_name = try alloc.dupe(u8, parent.table.name) };
                }
            }
            return error.ParentCatalogRouteChanged;
        }
        if (status.value.phase == .canceled) return error.UnexpectedPublicationCancel;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    std.debug.print("hosted FK publication stalled phase={s}\n", .{@tagName(last_phase)});
    return error.PublicationTimeout;
}

fn awaitTruncate(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, metadata: *metadata_runtime.Server, old_child_id: u64, receipt: http.HttpResponse) !u64 {
    if (receipt.status != 202 and receipt.status != 409) return error.UnexpectedTruncateStatus;
    var accepted = try std.json.parseFromSlice(std.json.Value, alloc, receipt.body, .{});
    defer accepted.deinit();
    const ddl_receipt = try field(accepted.value, "ddl_receipt");
    const job_id_text = (try field(ddl_receipt, "restore_job_id")).string;
    const job_id = try std.fmt.parseUnsigned(u64, job_id_text, 10);
    const plan_id = try staging.idForAttempt(job_id, 1);
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    var last_phase: ?staging.State = null;
    while (platform.time.monotonicNs() < deadline) {
        if (try source.getRestoreStaging(alloc, plan_id, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) })) |encoded| {
            defer alloc.free(encoded);
            var staged = try std.json.parseFromSlice(staging.Job, alloc, encoded, .{ .ignore_unknown_fields = true });
            defer staged.deinit();
            if (last_phase == null or last_phase.? != staged.value.state) {
                std.debug.print("truncate staging phase={s}\n", .{@tagName(staged.value.state)});
                last_phase = staged.value.state;
            }
            if (staged.value.plan.external_fk_parents.len != 1 or staged.value.plan.external_fk_parents[0].ranges.len != 1) return error.TruncateParentPlanMismatch;
            if (staged.value.state == .published) {
                const parent_group = staged.value.plan.external_fk_parents[0].ranges[0].group_id;
                const activation_receipt = try source.getRestoreStagingReceipt(alloc, plan_id, .activating, parent_group, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) }) orelse return error.TruncateParentActivationReceiptMissing;
                alloc.free(activation_receipt);
            } else {
                var public_child = try table(alloc, transport, headers, base, "children");
                defer public_child.deinit(alloc);
                if (try tableId(alloc, public_child) != old_child_id) {
                    // Publication may race these two read-index requests;
                    // confirm metadata advanced rather than accepting an
                    // early child descriptor under an unacknowledged parent.
                    const current_bytes = (try source.getRestoreStaging(alloc, plan_id, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) })) orelse return error.TruncatePublicationOrderViolation;
                    defer alloc.free(current_bytes);
                    var current = try std.json.parseFromSlice(staging.Job, alloc, current_bytes, .{ .ignore_unknown_fields = true });
                    defer current.deinit();
                    if (current.value.state != .published) return error.TruncatePublicationOrderViolation;
                }
            }
        }
        var response = try job(alloc, transport, headers, base, job_id_text);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var state = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer state.deinit();
            const phase = (try field(state.value, "phase")).string;
            if (std.mem.eql(u8, phase, "failed")) return error.TruncateJobFailed;
            if (std.mem.eql(u8, phase, "succeeded")) {
                // Job completion can race the preceding staging read. Prove
                // the durable terminal state, not whether polling saw it.
                const terminal_bytes = (try source.getRestoreStaging(alloc, plan_id, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) })) orelse return error.TruncatePublicationUnobserved;
                defer alloc.free(terminal_bytes);
                var terminal = try std.json.parseFromSlice(staging.Job, alloc, terminal_bytes, .{ .ignore_unknown_fields = true });
                defer terminal.deinit();
                if (terminal.value.state != .published or terminal.value.plan.external_fk_parents.len != 1 or terminal.value.plan.external_fk_parents[0].ranges.len != 1)
                    return error.TruncatePublicationUnobserved;
                const parent_group = terminal.value.plan.external_fk_parents[0].ranges[0].group_id;
                const activation = (try source.getRestoreStagingReceipt(alloc, plan_id, .activating, parent_group, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) })) orelse return error.TruncateParentActivationReceiptMissing;
                alloc.free(activation);
                var child = try table(alloc, transport, headers, base, "children");
                defer child.deinit(alloc);
                const new_id = try tableId(alloc, child);
                if (new_id == old_child_id) return error.TruncateGenerationUnchanged;
                try std.testing.expect(try hasForeignKey(alloc, child));
                return new_id;
            }
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.TruncateJobTimeout;
}

const recovery_fixture = @import("hosted_truncate_recovery_fixture.zig");
const MountedMode = enum { drop, truncate, truncate_fault };

fn awaitRetirementState(alloc: std.mem.Allocator, io: std.Io, data: *data_runtime.DataServer, route: ParentRoute, acknowledged: bool, drivers: []const *raft.ManagedProgressDriver) !void {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    const reader = (if (data.http_server) |*server| server.table_reads else null) orelse return error.ParentReadSourceMissing;
    while (platform.time.monotonicNs() < deadline) {
        for (drivers) |driver| try driver.checkFailure();
        var response = reader.lookup(alloc, route.table_name, "", .{
            .relational_topology_json = "{\"mode\":\"parent_activation\"}",
            .execution_deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
        }, .read_index) catch |err| switch (err) {
            error.NotLeader, error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerUnavailable, error.Timeout, error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.DeadlineExceeded => {
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        if (response) |*value| {
            defer value.deinit(alloc);
            var parsed = try std.json.parseFromSlice(?@import("../storage/db/relational_integrity_generation_retirement.zig").OwnerStatus, alloc, value.json, .{});
            defer parsed.deinit();
            if (parsed.value) |status| {
                try std.testing.expectEqual(route.group_id, status.fence.owner_group_id);
                if (status.completed and status.acknowledged == acknowledged) return;
            }
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ParentRetirementStateTimeout;
}

fn mountedHostedExternalParent(mode: MountedMode) !void {
    const alloc = std.testing.allocator;
    const usermgr = @import("../usermgr/mod.zig");
    const casbin = @import("antfly_casbin");
    var auth_store = usermgr.MemoryStore.init(alloc);
    defer auth_store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var auth_manager = try usermgr.UserManager.init(alloc, auth_store.iface(), try usermgr.initDefaultEnforcer(alloc, policy_store.iface()));
    defer auth_manager.deinit();
    var permission = try usermgr.Permission.initOwned(alloc, .table, "*", .admin);
    defer permission.deinit(alloc);
    var admin = try auth_manager.createUser("truncate-admin", "truncate-password", &.{permission});
    defer admin.deinit(alloc);
    const admin_headers = [_]http.RequestHeader{.{ .name = "Authorization", .value = "Basic dHJ1bmNhdGUtYWRtaW46dHJ1bmNhdGUtcGFzc3dvcmQ=" }};
    const process_alloc = platform.allocator.processAllocator(alloc);
    const trusted_secret = "hosted-fk-drop-trusted-v1";
    const internal_secret = "hosted-fk-drop-internal-v1";
    const issuer = "hosted-fk-drop";
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
        .metadata_group_id = 2197,
        .replica_root_dir = meta_root,
        .replica_catalog_path = meta_catalog,
        .snapshot_root_dir = snapshots,
        .observe_local_replica_root = true,
        .api_server_cfg = .{ .auth_enabled = true, .user_manager = &auth_manager, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    });
    defer metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2197, 1);
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
    var data_raft_live = true;
    defer if (data_raft_live) data_raft.deinit();
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    var data_control_live = true;
    defer if (data_control_live) data_control.deinit();
    try data_control.start();
    const restarter: recovery_fixture.DataRestart = .{
        .alloc = process_alloc,
        .io = io,
        .server = &data,
        .server_live = &data_live,
        .raft_driver = &data_raft,
        .raft_live = &data_raft_live,
        .control_driver = &data_control,
        .control_live = &data_control_live,
        .metadata_uri = metadata_uri,
        .config = .{
            .replica_root_dir = data_root,
            .replica_catalog_path = data_catalog,
            .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
            .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
        },
    };
    const base = try data.baseUri(alloc);
    defer alloc.free(base);
    var executor = executor_mod.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const transport = executor.executor();
    const now: i64 = @intCast(@divFloor(platform.time.realtimeNs(), std.time.ns_per_s));
    const claims = try std.fmt.allocPrint(alloc,
        \\{{"iss":"{s}","sub":"user:hosted-fk-drop-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};

    for ([_][]const u8{
        "CREATE TABLE parents (id BIGINT PRIMARY KEY)",
        "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT)",
    }) |statement| {
        var response = try sql(alloc, transport, &headers, base, statement);
        defer response.deinit(alloc);
        if (response.status != 200) std.debug.print("hosted FK create status={d}\n", .{response.status});
        try std.testing.expectEqual(@as(u16, 200), response.status);
    }
    const parent_id = try awaitTable(alloc, io, transport, &headers, base, "parents");
    const child_id = try awaitTable(alloc, io, transport, &headers, base, "children");
    var parent_insert = try batch(alloc, transport, &headers, base, "parents", "{\"inserts\":{\"parent-row\":{\"id\":1}},\"sync_level\":\"full_text\"}", 3_000);
    defer parent_insert.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), parent_insert.status);
    var add = try sql(alloc, transport, &headers, metadata_uri, "ALTER TABLE children ADD CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES parents(id)");
    defer add.deinit(alloc);
    if (add.status != 202) std.debug.print("hosted FK ADD status={d}\n", .{add.status});
    try std.testing.expectEqual(@as(u16, 202), add.status);
    const parent_route = try awaitPublication(alloc, io, &metadata, child_id, true);
    defer parent_route.deinit(alloc);
    const prior_parent_receipt = try awaitParentAcknowledged(alloc, io, (if (data.http_server) |*server| server.table_reads else null) orelse return error.ParentReadSourceMissing, parent_route);
    var after_add = try table(alloc, transport, &headers, base, "children");
    defer after_add.deinit(alloc);
    try std.testing.expect(try hasForeignKey(alloc, after_add));
    awaitEnforced(alloc, io, transport, &headers, base, "children") catch |err| {
        try meta_raft.checkFailure();
        try meta_control.checkFailure();
        try data_raft.checkFailure();
        try data_control.checkFailure();
        for ([_][]const u8{ "parents", "children" }) |name| {
            var visible = try table(alloc, transport, &headers, base, name);
            defer visible.deinit(alloc);
            std.debug.print("activation timeout table={s} status={d} body={s}\n", .{ name, visible.status, visible.body });
        }
        return err;
    };
    var child_insert = try batch(alloc, transport, &headers, base, "children", "{\"inserts\":{\"child-row\":{\"id\":7,\"parent_id\":1}},\"sync_level\":\"full_text\"}", 10_000);
    defer child_insert.deinit(alloc);
    if (child_insert.status != 201) std.debug.print("hosted FK child insert status={d}\n", .{child_insert.status});
    try std.testing.expectEqual(@as(u16, 201), child_insert.status);
    const delete_started_ns = platform.time.monotonicNs();
    var blocked_delete = try batch(alloc, transport, &headers, base, "parents", "{\"deletes\":[\"parent-row\"],\"sync_level\":\"full_text\"}", 10_000);
    std.debug.print("hosted FK pre-DROP parent DELETE elapsed_ms={d}\n", .{(platform.time.monotonicNs() -| delete_started_ns) / std.time.ns_per_ms});
    defer blocked_delete.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), blocked_delete.status);

    const next_child_id = switch (mode) {
        .drop => blk: {
            var drop = try sql(alloc, transport, &headers, metadata_uri, "ALTER TABLE children DROP CONSTRAINT child_parent");
            defer drop.deinit(alloc);
            if (drop.status != 202) std.debug.print("hosted FK DROP status={d}\n", .{drop.status});
            try std.testing.expectEqual(@as(u16, 202), drop.status);
            const retired_route = try awaitPublication(alloc, io, &metadata, child_id, false);
            defer retired_route.deinit(alloc);
            try std.testing.expectEqual(parent_route.group_id, retired_route.group_id);
            try std.testing.expectEqualStrings(parent_route.table_name, retired_route.table_name);
            var after_drop = try table(alloc, transport, &headers, base, "children");
            defer after_drop.deinit(alloc);
            try std.testing.expectEqual(child_id, try tableId(alloc, after_drop));
            try std.testing.expect(!try hasForeignKey(alloc, after_drop));
            break :blk child_id;
        },
        .truncate, .truncate_fault => blk: {
            const coordinator = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
            const hooks = http_server.ApiHttpServer.TruncateTestDriver;
            defer hooks.release(coordinator);
            if (mode == .truncate_fault) hooks.arm(coordinator, .parent_activated);
            var truncate = try sql(alloc, transport, &admin_headers, metadata_uri, "TRUNCATE children");
            defer truncate.deinit(alloc);
            if (truncate.status != 202 and truncate.status != 409) std.debug.print("hosted FK TRUNCATE status={d}\n", .{truncate.status});
            try std.testing.expectEqual(@as(u16, 202), truncate.status);
            if (mode == .truncate_fault) {
                const drivers = [_]*raft.ManagedProgressDriver{ &meta_raft, &meta_control, &data_raft, &data_control };
                try recovery_fixture.awaitBoundary(io, coordinator, &drivers);
                try awaitRetirementState(alloc, io, &data, parent_route, false, &drivers);
                // The parent activation committed, but its coordinator receipt
                // was lost. Reopen the same roots before allowing its retry.
                try restarter.restart();
                try awaitRetirementState(alloc, io, &data, parent_route, false, &drivers);
                hooks.arm(coordinator, .before_parent_ack);
                try recovery_fixture.awaitBoundary(io, coordinator, &drivers);
                try awaitRetirementState(alloc, io, &data, parent_route, false, &drivers);
                var published_child = try table(alloc, transport, &admin_headers, metadata_uri, "children");
                defer published_child.deinit(alloc);
                try std.testing.expect(try tableId(alloc, published_child) != child_id);
                hooks.arm(coordinator, .after_parent_ack);
                try recovery_fixture.awaitBoundary(io, coordinator, &drivers);
                try awaitRetirementState(alloc, io, &data, parent_route, true, &drivers);
                // Lose the ACK reply and process-local owner caches. Metadata
                // and the owner must recover the original job, not a new SQL.
                try restarter.restart();
                try awaitRetirementState(alloc, io, &data, parent_route, true, &drivers);
                hooks.release(coordinator);
            }
            break :blk try awaitTruncate(alloc, io, transport, &admin_headers, metadata_uri, &metadata, child_id, truncate);
        },
    };

    if (mode != .truncate_fault) try restarter.restart();
    const restarted_base = try data.baseUri(alloc);
    defer alloc.free(restarted_base);
    try awaitTableId(alloc, io, transport, &headers, restarted_base, "children", next_child_id);
    var restarted_child = try table(alloc, transport, &headers, restarted_base, "children");
    defer restarted_child.deinit(alloc);
    try std.testing.expectEqual(mode != .drop, try hasForeignKey(alloc, restarted_child));
    try awaitTableId(alloc, io, transport, &headers, restarted_base, "parents", parent_id);
    const parent_server = if (data.http_server) |*server| server else return error.ParentReadSourceMissing;
    const parent_reader = parent_server.table_reads orelse return error.ParentReadSourceMissing;
    if (mode == .drop) {
        const current_parent_receipt = try awaitParentAcknowledged(alloc, io, parent_reader, parent_route);
        try std.testing.expect(!std.mem.eql(u8, &prior_parent_receipt, &current_parent_receipt));
    }
    // Cold restart readiness is a linearizable read, never a retried mutation.
    const parent_ready_deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (true) {
        var ready = parent_reader.lookup(alloc, parent_route.table_name, "", .{
            .relational_integrity_catalog = true,
            .execution_deadline_ns = @min(parent_ready_deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
        }, .read_index) catch |err| switch (err) {
            error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerUnavailable, error.Timeout, error.ReadIndexTimeout, error.CatalogRoutingSnapshotTimeout, error.DeadlineExceeded => {
                if (platform.time.monotonicNs() >= parent_ready_deadline) return err;
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        if (ready) |*response| {
            response.deinit(alloc);
            break;
        }
        if (platform.time.monotonicNs() >= parent_ready_deadline) return error.ParentReadSourceMissing;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    var released_delete = try batch(alloc, transport, &headers, restarted_base, "parents", "{\"deletes\":[\"parent-row\"],\"sync_level\":\"full_text\"}", 10_000);
    defer released_delete.deinit(alloc);
    if (released_delete.status != 201) std.debug.print("hosted FK post-retirement delete status={d}\n", .{released_delete.status});
    try std.testing.expectEqual(@as(u16, 201), released_delete.status);
    if (mode != .drop) {
        try awaitEnforced(alloc, io, transport, &headers, restarted_base, "children");
        var orphan = try batch(alloc, transport, &headers, restarted_base, "children", "{\"inserts\":{\"orphan\":{\"id\":8,\"parent_id\":1}},\"sync_level\":\"full_text\"}", 10_000);
        defer orphan.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), orphan.status);
        var new_parent = try batch(alloc, transport, &headers, restarted_base, "parents", "{\"inserts\":{\"new-parent\":{\"id\":2}},\"sync_level\":\"full_text\"}", 10_000);
        defer new_parent.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 201), new_parent.status);
        var new_child = try batch(alloc, transport, &headers, restarted_base, "children", "{\"inserts\":{\"new-child\":{\"id\":9,\"parent_id\":2}},\"sync_level\":\"full_text\"}", 10_000);
        defer new_child.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 201), new_child.status);
        var protected = try batch(alloc, transport, &headers, restarted_base, "parents", "{\"deletes\":[\"new-parent\"],\"sync_level\":\"full_text\"}", 10_000);
        defer protected.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 409), protected.status);
    }
}

test "mounted hosted external-parent FK DROP publishes after parent ACK" {
    try mountedHostedExternalParent(.drop);
}

test "mounted hosted external-parent FK TRUNCATE retires old references across restart" {
    try mountedHostedExternalParent(.truncate);
}

test "mounted hosted external-parent FK TRUNCATE recovers lost activation and ACK replies across cold owners" {
    try mountedHostedExternalParent(.truncate_fault);
}
