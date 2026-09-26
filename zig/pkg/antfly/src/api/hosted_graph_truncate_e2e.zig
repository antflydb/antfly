// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Mounted graph TRUNCATE through public SQL with a test-only admission permit.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const sql_truncate = @import("sql_truncate.zig");
const staging = @import("../metadata/restore_staging.zig");
const fk_publication = @import("../metadata/fk_generation_publication.zig");
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
    const authorization = if (headers.len > 0 and std.ascii.eqlIgnoreCase(headers[0].name, "authorization")) headers[0].value else null;
    return transport.execute(alloc, .{ .method = method, .uri = uri, .authorization = authorization, .headers = if (authorization != null) headers[1..] else headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = 10_000 });
}
fn sql(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, statement: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    return request(alloc, transport, headers, base, "/db/v1/sql", .POST, body);
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
fn awaitNamedTableId(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8, expected: ?u64, excluded: ?u64) !u64 {
    const path = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}", .{name});
    defer alloc.free(path);
    for (0..600) |_| {
        var response = try request(alloc, transport, headers, base, path, .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200) {
            const id = try tableId(alloc, response);
            if ((expected == null or id == expected.?) and (excluded == null or id != excluded.?)) return id;
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.GraphTablePlacementTimeout;
}
fn awaitTableId(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, expected: ?u64, excluded: ?u64) !u64 {
    return awaitNamedTableId(alloc, io, transport, headers, base, "docs", expected, excluded);
}
fn awaitChildForeignKey(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, child_id: u64) !void {
    for (0..1200) |_| {
        var response = try request(alloc, transport, headers, base, "/db/v1/tables/children", .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200 and try tableId(alloc, response) == child_id and std.mem.indexOf(u8, response.body, "child_parent") != null) return;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.GraphCascadeForeignKeyTimeout;
}
fn awaitChildFkPublication(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, child_id: u64) !void {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const encoded = try source.systemCatalog(alloc, .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s), .fk_generation_publication_authority = true }, .{ .fk_generation_publication_status = child_id });
        defer alloc.free(encoded);
        var publication = try std.json.parseFromSlice(fk_publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
        defer publication.deinit();
        try publication.value.validateState(alloc);
        if (publication.value.phase == .published) {
            if (publication.value.child_installed.len != 1 or publication.value.parent_acknowledged.len != 1)
                return error.GraphCascadeForeignKeyReceiptMissing;
            return;
        }
        if (publication.value.phase == .canceled) return error.GraphCascadeForeignKeyCanceled;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.GraphCascadeForeignKeyTimeout;
}
fn awaitChildConstraintEnforced(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !void {
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        var response = try request(alloc, transport, headers, base, "/db/v1/tables/children/constraints/status", .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var status = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer status.deinit();
            const state = (try field(status.value, "state")).string;
            if (std.mem.eql(u8, state, "enforced")) return;
            if (std.mem.eql(u8, state, "invalid")) return error.GraphCascadeConstraintInvalid;
        } else if (response.status != 409 and response.status != 503 and response.status != 504) return error.GraphCascadeConstraintStatusUnavailable;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.GraphCascadeConstraintActivationTimeout;
}
fn awaitChildDocumentVisible(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, child_id: u64) !void {
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        if (try awaitNamedTableId(alloc, io, transport, headers, base, "children", child_id, null) != child_id)
            return error.GraphCascadeChildRouteChanged;
        var row = try request(alloc, transport, headers, base, "/db/v1/tables/children/documents/child-c", .GET, null);
        defer row.deinit(alloc);
        if (row.status == 200) return;
        if (row.status != 404 and row.status != 409 and row.status != 503 and row.status != 504)
            return error.GraphCascadeChildReadUnavailable;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.GraphCascadeChildCommitNotVisible;
}
fn awaitIndex(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !void {
    for (0..600) |_| {
        var response = try request(alloc, transport, headers, base, "/db/v1/tables/docs/indexes/links", .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200) return;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.GraphIndexPlacementTimeout;
}

fn awaitGraphTruncate(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, metadata: *metadata_runtime.Server, old_ids: []const u64, receipt: http.HttpResponse) !u64 {
    if (receipt.status != 202) {
        std.log.warn("graph truncate admission rejected status={d}", .{receipt.status});
        return error.UnexpectedGraphTruncateStatus;
    }
    var accepted = try std.json.parseFromSlice(std.json.Value, alloc, receipt.body, .{});
    defer accepted.deinit();
    const ddl_receipt = try field(accepted.value, "ddl_receipt");
    const job_text = (try field(ddl_receipt, "restore_job_id")).string;
    const job_id = try std.fmt.parseUnsigned(u64, job_text, 10);
    const job_suffix = try std.fmt.allocPrint(alloc, "/db/v1/restore/jobs/{s}", .{job_text});
    defer alloc.free(job_suffix);
    const plan_id = try staging.idForAttempt(job_id, 1);
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    var published = false;
    while (platform.time.monotonicNs() < deadline) {
        const context: @import("operation.zig").RequestContext = .{ .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s) };
        if (try source.getRestoreStaging(alloc, plan_id, context)) |encoded| {
            defer alloc.free(encoded);
            var staged = try std.json.parseFromSlice(staging.Job, alloc, encoded, .{ .ignore_unknown_fields = true });
            defer staged.deinit();
            if (staged.value.plan.targets.len != old_ids.len) return error.GraphTruncatePlanMismatch;
            var graph_targets: usize = 0;
            for (staged.value.plan.targets) |target| {
                const old = target.replace orelse return error.GraphTruncatePlanMismatch;
                if (old.ranges.len != 1 or target.table.table_id == old.table.table_id or
                    std.mem.indexOfScalar(u64, old_ids, old.table.table_id) == null) return error.GraphTruncatePlanMismatch;
                if (target.graph_retirement_digest != null) graph_targets += 1;
            }
            if (graph_targets != 1) return error.GraphTruncatePlanMismatch;
            if (staged.value.state == .published) {
                for (staged.value.plan.targets) |target| {
                    const old_range = target.replace.?.ranges[0];
                    const handoff = (try staging.generationHandoffForOldRange(target, old_range)) orelse return error.GraphTruncatePlanMismatch;
                    const fence = for (target.replace.?.fences) |item| {
                        if (item.owner_group_id == old_range.group_id) break item;
                    } else return error.GraphTruncatePlanMismatch;
                    const preflight = (try source.getRestoreStagingReceipt(alloc, plan_id, .validating, old_range.group_id, context)) orelse return error.GenerationHandoffPreflightMissing;
                    defer alloc.free(preflight);
                    const expected_preflight = try staging.generationHandoffPreflightDigest(fence, staged.value.plan_digest, handoff);
                    if (!std.mem.eql(u8, preflight, &expected_preflight)) return error.GenerationHandoffPreflightMismatch;
                    const raw_receipt = (try source.getRestoreStagingReceipt(alloc, plan_id, .cutover, old_range.group_id, context)) orelse return error.GraphSealReceiptMissing;
                    defer alloc.free(raw_receipt);
                    const graph_digest: ?[32]u8 = if (try staging.graphSealScopeForOldRange(alloc, staged.value.plan, staged.value.plan_digest, target, old_range)) |seal| try seal.sealDigest() else null;
                    const handoff_digest = try staging.generationHandoffSealDigest(fence, staged.value.plan_digest, handoff);
                    const expected = try staging.generationHandoffOldFenceDigest(fence, handoff_digest, graph_digest);
                    if (raw_receipt.len != expected.len or !std.mem.eql(u8, raw_receipt, &expected))
                        return error.GraphSealReceiptMismatch;
                    const target_range = target.ranges[0];
                    const installed = (try source.getRestoreStagingReceipt(alloc, plan_id, .activating, target_range.group_id, context)) orelse return error.GenerationHandoffInstallMissing;
                    defer alloc.free(installed);
                    const mapped = (try staging.mappedEmptyGenerationHandoffForGroup(alloc, staged.value.plan, staged.value.plan_digest, target_range.group_id)) orelse return error.GraphTruncatePlanMismatch;
                    if (!std.mem.eql(u8, installed, &mapped.expected_receipt_digest)) return error.GenerationHandoffInstallMismatch;
                }
                published = true;
            } else {
                var current = try request(alloc, transport, headers, base, "/db/v1/tables/docs", .GET, null);
                defer current.deinit(alloc);
                if (try tableId(alloc, current) != old_ids[0]) {
                    const fresh = (try source.getRestoreStaging(alloc, plan_id, context)) orelse return error.GraphPublicationOrderViolation;
                    defer alloc.free(fresh);
                    var after = try std.json.parseFromSlice(staging.Job, alloc, fresh, .{ .ignore_unknown_fields = true });
                    defer after.deinit();
                    if (after.value.state != .published) return error.GraphPublicationOrderViolation;
                }
            }
        }
        var response = try request(alloc, transport, headers, base, job_suffix, .GET, null);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var state = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer state.deinit();
            const phase = (try field(state.value, "phase")).string;
            if (std.mem.eql(u8, phase, "failed")) return error.GraphTruncateJobFailed;
            if (std.mem.eql(u8, phase, "succeeded")) {
                // The HTTP job projection can become visible one control
                // tick before this fixture observes the authoritative
                // metadata publication. Keep waiting under the same bound;
                // never replay the already admitted TRUNCATE.
                if (!published) {
                    try io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                }
                return awaitTableId(alloc, io, transport, headers, base, null, old_ids[0]);
            }
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    const diagnostic_context: @import("operation.zig").RequestContext = .{ .deadline_ns = platform.time.monotonicNs() +| 2 * std.time.ns_per_s };
    const diagnostic = source.getRestoreStaging(alloc, plan_id, diagnostic_context) catch |err| blk: {
        std.log.warn("graph truncate staging diagnostic unavailable err={s}", .{@errorName(err)});
        break :blk null;
    };
    if (diagnostic) |encoded| {
        defer alloc.free(encoded);
        var stalled = std.json.parseFromSlice(staging.Job, alloc, encoded, .{ .ignore_unknown_fields = true }) catch return error.GraphTruncateJobTimeout;
        defer stalled.deinit();
        std.log.warn("graph truncate stalled state={s} revision={d} completed={d}", .{ @tagName(stalled.value.state), stalled.value.revision, stalled.value.completed_owners });
        for (stalled.value.plan.targets) |target| {
            const old = target.replace orelse continue;
            for (old.ranges) |range| {
                const checked = source.getRestoreStagingReceipt(alloc, plan_id, .validating, range.group_id, diagnostic_context) catch null;
                defer if (checked) |receipt_bytes| alloc.free(receipt_bytes);
                const sealed = source.getRestoreStagingReceipt(alloc, plan_id, .cutover, range.group_id, diagnostic_context) catch null;
                defer if (sealed) |receipt_bytes| alloc.free(receipt_bytes);
                std.log.warn("graph truncate old group={d} preflight={} sealed={}", .{ range.group_id, checked != null, sealed != null });
            }
            for (target.ranges) |range| {
                const installed = source.getRestoreStagingReceipt(alloc, plan_id, .activating, range.group_id, diagnostic_context) catch null;
                defer if (installed) |receipt_bytes| alloc.free(receipt_bytes);
                std.log.warn("graph truncate target group={d} installed={}", .{ range.group_id, installed != null });
            }
        }
    } else std.log.warn("graph truncate staging diagnostic missing plan", .{});
    if (request(alloc, transport, headers, base, job_suffix, .GET, null) catch null) |job_response| {
        var response = job_response;
        defer response.deinit(alloc);
        std.log.warn("graph truncate job diagnostic status={d}", .{response.status});
    }
    return error.GraphTruncateJobTimeout;
}

fn assertHandoffInstallSurvivesRestart(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, accepted_response: http.HttpResponse) !void {
    var accepted = try std.json.parseFromSlice(std.json.Value, alloc, accepted_response.body, .{});
    defer accepted.deinit();
    const job_text = (try field(try field(accepted.value, "ddl_receipt"), "restore_job_id")).string;
    const plan_id = try staging.idForAttempt(try std.fmt.parseUnsigned(u64, job_text, 10), 1);
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const context: @import("operation.zig").RequestContext = .{ .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s };
    const encoded = (try source.getRestoreStaging(alloc, plan_id, context)) orelse return error.GraphTruncatePlanMismatch;
    defer alloc.free(encoded);
    var staged = try std.json.parseFromSlice(staging.Job, alloc, encoded, .{ .ignore_unknown_fields = true });
    defer staged.deinit();
    if (staged.value.state != .published) return error.GraphPublicationUnobserved;
    // Once published, the hidden restore descriptor is intentionally retired.
    // Reach the new owner through its public catalog fence, but keep the
    // read-indexed control mode service-internal and compare its durable fact
    // to the immutable Plan rather than trusting routing alone.
    // A group-local source is only valid on the hosting data node. This
    // metadata process has a local kernel source too, but probing a data-group
    // ID through it would create/read a different physical root. Use the
    // public table lookup so placement routing reaches the actual owner.
    const reads = (metadata.server.owned_public_read_source orelse return error.GraphTruncateReadSourceMissing).source();
    for (staged.value.plan.targets) |target| {
        if (target.ranges.len != 1 or target.ranges[0].start_key.len != 0 or target.ranges[0].end_key != null)
            return error.GraphTruncatePlanMismatch;
        const range = target.ranges[0];
        const mapped = (try staging.mappedEmptyGenerationHandoffForGroup(alloc, staged.value.plan, staged.value.plan_digest, range.group_id)) orelse return error.GraphTruncatePlanMismatch;
        var first: ?@import("../storage/db/restore_staging_contract.zig").GenerationAdmissionReceipt = null;
        for (0..2) |_| {
            var response = (try reads.lookup(alloc, target.table.name, "", .{
                .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
                .execution_deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
            }, .read_index)) orelse return error.GenerationHandoffInstallMissing;
            defer response.deinit(alloc);
            var parsed = try std.json.parseFromSlice(?@import("../storage/db/restore_staging_contract.zig").GenerationAdmissionReceipt, alloc, response.json, .{});
            defer parsed.deinit();
            const installed = parsed.value orelse return error.GenerationHandoffInstallMissing;
            if (installed.applied_term == 0 or installed.applied_index == 0 or
                !std.mem.eql(u8, &installed.scope, &mapped.command.scope) or
                !std.mem.eql(u8, &installed.logical_digest, &mapped.expected_receipt_digest)) return error.GenerationHandoffInstallMismatch;
            if (first) |prior| if (prior.applied_term != installed.applied_term or prior.applied_index != installed.applied_index) return error.GenerationHandoffInstallReplayChanged;
            first = installed;
        }
    }
}

test "mounted hosted graph TRUNCATE seals and CASCADE retires FK cohort across restart" {
    const alloc = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(alloc);
    const trusted_secret = "hosted-graph-truncate-trusted-v1";
    const internal_secret = "hosted-graph-truncate-internal-v1";
    const issuer = "hosted-graph-truncate";
    const admin_username = "graph-truncate-admin";
    const admin_password = "graph-truncate-password";
    var auth_store = usermgr.MemoryStore.init(alloc);
    defer auth_store.deinit();
    var policy_store = casbin.MemoryAdapter.init(alloc);
    defer policy_store.deinit();
    var auth_manager = try usermgr.UserManager.init(alloc, auth_store.iface(), try usermgr.initDefaultEnforcer(alloc, policy_store.iface()));
    defer auth_manager.deinit();
    var admin_permission = try usermgr.Permission.initOwned(alloc, .table, "*", .admin);
    defer admin_permission.deinit(alloc);
    var admin_user = try auth_manager.createUser(admin_username, admin_password, &.{admin_permission});
    defer admin_user.deinit(alloc);
    const admin_authorization = try basicAuthorization(alloc, admin_username, admin_password);
    defer alloc.free(admin_authorization);
    const admin_headers = [_]http.RequestHeader{.{ .name = "authorization", .value = admin_authorization }};
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
        .api_server_cfg = .{ .auth_enabled = true, .user_manager = &auth_manager, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
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
        .store_registration = .{ .node_id = 19, .store_id = 19, .role = "data" },
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
    const base = try data.baseUri(alloc);
    defer alloc.free(base);
    var executor = executor_mod.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const transport = executor.executor();
    const now: i64 = @intCast(@divFloor(platform.time.realtimeNs(), std.time.ns_per_s));
    const claims = try std.fmt.allocPrint(alloc,
        \\{{"iss":"{s}","sub":"user:hosted-graph-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};

    var created = try sql(alloc, transport, &headers, base, "CREATE TABLE docs (id BIGINT PRIMARY KEY)");
    defer created.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), created.status);
    const old_id = try awaitTableId(alloc, io, transport, &headers, base, null, null);
    var index = try request(alloc, transport, &headers, base, "/db/v1/tables/docs/indexes/links", .POST, "{\"name\":\"links\",\"type\":\"graph\"}");
    defer index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), index.status);
    try awaitIndex(alloc, io, transport, &headers, base);
    var inserted = try request(alloc, transport, &headers, base, "/db/v1/tables/docs/batch", .POST, "{\"inserts\":{\"doc-a\":{\"id\":1}},\"sync_level\":\"full_text\"}");
    defer inserted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), inserted.status);

    // Public graph TRUNCATE is closed before any durable job admission.
    var guarded = try sql(alloc, transport, &admin_headers, metadata_uri, "TRUNCATE docs");
    defer guarded.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), guarded.status);
    try std.testing.expectEqual(old_id, try awaitTableId(alloc, io, transport, &headers, base, null, null));
    try sql_truncate.grantGraphTruncateForTest(old_id);
    defer sql_truncate.clearGraphTruncateForTest();
    var truncate = try sql(alloc, transport, &admin_headers, metadata_uri, "TRUNCATE docs");
    defer truncate.deinit(alloc);
    const new_id = try awaitGraphTruncate(alloc, io, transport, &admin_headers, metadata_uri, &metadata, &.{old_id}, truncate);
    try awaitIndex(alloc, io, transport, &headers, base);
    var old_document = try request(alloc, transport, &headers, base, "/db/v1/tables/docs/documents/doc-a", .GET, null);
    defer old_document.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), old_document.status);

    data_control.deinit();
    data_control_live = false;
    data_raft.deinit();
    data_raft_live = false;
    data.deinit();
    data_live = false;
    data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 19, .store_id = 19, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    data_live = true;
    try data.start();
    try data.registerNodeIfConfigured();
    data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_live = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_live = true;
    try data_control.start();
    const restarted_base = try data.baseUri(alloc);
    defer alloc.free(restarted_base);
    _ = try awaitTableId(alloc, io, transport, &headers, restarted_base, new_id, null);
    try awaitIndex(alloc, io, transport, &headers, restarted_base);
    var after_restart = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/docs/documents/doc-a", .GET, null);
    defer after_restart.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), after_restart.status);

    // Exercise the same graph barrier as part of a dependency-closed FK
    // cohort. ADD FK is the supported hosted publication path, so this does
    // not bypass any separate initial-child authority gate.
    var created_child = try sql(alloc, transport, &headers, restarted_base, "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT)");
    defer created_child.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), created_child.status);
    const old_child_id = try awaitNamedTableId(alloc, io, transport, &headers, restarted_base, "children", null, null);
    var parent_insert = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/docs/batch", .POST, "{\"inserts\":{\"doc-b\":{\"id\":2}},\"sync_level\":\"full_text\"}");
    defer parent_insert.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), parent_insert.status);
    var pre_fk_child = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/children/batch", .POST, "{\"inserts\":{\"child-b\":{\"id\":7,\"parent_id\":2}},\"sync_level\":\"full_text\"}");
    defer pre_fk_child.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), pre_fk_child.status);
    var add_fk = try sql(alloc, transport, &admin_headers, metadata_uri, "ALTER TABLE children ADD CONSTRAINT child_parent FOREIGN KEY (parent_id) REFERENCES docs(id)");
    defer add_fk.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), add_fk.status);
    try awaitChildFkPublication(alloc, io, &metadata, old_child_id);
    try awaitChildForeignKey(alloc, io, transport, &headers, restarted_base, old_child_id);
    try awaitChildConstraintEnforced(alloc, io, transport, &headers, restarted_base);
    var child_insert = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/children/batch", .POST, "{\"inserts\":{\"child-c\":{\"id\":8,\"parent_id\":2}},\"sync_level\":\"full_text\"}");
    defer child_insert.deinit(alloc);
    if (child_insert.status != 201 and child_insert.status != 202) return error.GraphCascadeChildCommitRejected;
    // A 202 is committed with propagation pending. Reconcile visibility by
    // exact key; never replay the mutation after an accepted response.
    try awaitChildDocumentVisible(alloc, io, transport, &headers, restarted_base, old_child_id);

    var guarded_cascade = try sql(alloc, transport, &admin_headers, metadata_uri, "TRUNCATE docs CASCADE");
    defer guarded_cascade.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), guarded_cascade.status);
    try std.testing.expectEqual(new_id, try awaitTableId(alloc, io, transport, &headers, restarted_base, null, null));
    try std.testing.expectEqual(old_child_id, try awaitNamedTableId(alloc, io, transport, &headers, restarted_base, "children", null, null));
    const cohort_ids = if (new_id < old_child_id) [_]u64{ new_id, old_child_id } else [_]u64{ old_child_id, new_id };
    try sql_truncate.grantGraphTruncateCohortForTest(&cohort_ids);
    var cascade = try sql(alloc, transport, &admin_headers, metadata_uri, "TRUNCATE docs CASCADE");
    defer cascade.deinit(alloc);
    const after_cascade_id = try awaitGraphTruncate(alloc, io, transport, &admin_headers, metadata_uri, &metadata, &.{ new_id, old_child_id }, cascade);
    const after_cascade_child_id = try awaitNamedTableId(alloc, io, transport, &headers, restarted_base, "children", null, old_child_id);
    if (after_cascade_child_id == old_child_id) return error.GraphCascadeChildNotReplaced;
    try awaitIndex(alloc, io, transport, &headers, restarted_base);
    var deleted_parent = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/docs/documents/doc-b", .GET, null);
    defer deleted_parent.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), deleted_parent.status);
    var deleted_child = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/children/documents/child-b", .GET, null);
    defer deleted_child.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), deleted_child.status);
    var deleted_post_fk_child = try request(alloc, transport, &headers, restarted_base, "/db/v1/tables/children/documents/child-c", .GET, null);
    defer deleted_post_fk_child.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), deleted_post_fk_child.status);
    try std.testing.expect(after_cascade_id != new_id);
    try assertHandoffInstallSurvivesRestart(alloc, &metadata, cascade);

    // The target receipt is a durable owner fact, not merely a metadata ACK.
    // Reopen both owners cold and repeat the exact hidden read-index lookup;
    // duplicate observation must retain the original Raft receipt.
    data_control.deinit();
    data_control_live = false;
    data_raft.deinit();
    data_raft_live = false;
    data.deinit();
    data_live = false;
    data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 19, .store_id = 19, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    data_live = true;
    try data.start();
    try data.registerNodeIfConfigured();
    data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_live = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_live = true;
    try data_control.start();
    const final_base = try data.baseUri(alloc);
    defer alloc.free(final_base);
    try assertHandoffInstallSurvivesRestart(alloc, &metadata, cascade);
    try std.testing.expectEqual(after_cascade_id, try awaitTableId(alloc, io, transport, &headers, final_base, null, null));
    try std.testing.expectEqual(after_cascade_child_id, try awaitNamedTableId(alloc, io, transport, &headers, final_base, "children", null, null));
}
