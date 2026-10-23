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

//! Catalog-only SQL qualification. Restart the actual metadata Raft owner and
//! SQL connection store; data-shard restarts cannot prove catalog recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const metadata = @import("../metadata/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const users = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");
const transactions = @import("transactions.zig");
pub const request = @import("sql_owner_test_fixture.zig").request;

pub const Context = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    metadata: *metadata.Server,
    transport: http.RequestExecutor,
    admin_headers: []const http.RequestHeader,
    base: []const u8,
};

fn progressRaft(raw: *anyopaque) !void {
    try @as(*metadata.Server, @ptrCast(@alignCast(raw))).runRaftRoundOnly();
}

fn progressControl(raw: *anyopaque) !void {
    try @as(*metadata.Server, @ptrCast(@alignCast(raw))).runControlRoundOnly();
}

pub fn runWithState(comptime Campaign: type, initial: Campaign) !void {
    const a = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(a);
    var user_store = users.MemoryStore.init(a);
    defer user_store.deinit();
    var policies = casbin.MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try users.UserManager.init(a, user_store.iface(), try users.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var permission = try users.Permission.initOwned(a, .@"*", "*", .admin);
    defer permission.deinit(a);
    var user = try manager.createUser("pk-rewriter", "catalog-owner-witness", &.{permission});
    defer user.deinit(a);
    if (@hasDecl(Campaign, "configureAuth")) try Campaign.configureAuth(&manager);
    const raw = "pk-rewriter:catalog-owner-witness";
    var authorization: [6 + std.base64.standard.Encoder.calcSize(raw.len)]u8 = undefined;
    @memcpy(authorization[0..6], "Basic ");
    _ = std.base64.standard.Encoder.encode(authorization[6..], raw);
    const headers = [_]http.RequestHeader{.{ .name = "authorization", .value = &authorization }};
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    const raft_root = try std.fmt.allocPrint(a, "{s}/metadata", .{root});
    defer a.free(raft_root);
    const catalog_root = try std.fmt.allocPrint(a, "{s}/metadata-catalog", .{root});
    defer a.free(catalog_root);
    const snapshot_root = try std.fmt.allocPrint(a, "{s}/snapshots", .{root});
    defer a.free(snapshot_root);
    const session_root = try std.fmt.allocPrint(a, "{s}/sql-connections", .{root});
    defer a.free(session_root);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var campaign = initial;
    // Each iteration independently opens and closes both durable owners.
    for (0..2) |restart| {
        var session_backend = try local.storage_lsm_backend.BackendHandle.open(a, session_root, .{});
        defer session_backend.close();
        var session_store = try session_backend.backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
        defer session_store.deinit();
        var sessions = transactions.DurableSessionStore.initRuntime(a, &session_store);
        var server = try metadata.Server.init(process_alloc, .{
            .local_node_id = 1,
            .metadata_group_id = 2188,
            .replica_root_dir = raft_root,
            .replica_catalog_path = catalog_root,
            .snapshot_root_dir = snapshot_root,
            .observe_local_replica_root = true,
            .api_server_cfg = .{
                .auth_enabled = true,
                .user_manager = &manager,
                .session_store = &sessions,
                .trusted_principal_secret = "catalog-owner-private-trusted-principal-secret",
                .trusted_principal_issuer = "catalog-owner-witness",
                .internal_service_secret = "catalog-owner-private-internal-service-secret",
                .internal_service_issuer = "catalog-owner-witness",
                .internal_service_auth_capability = "v1; mode=enforce",
            },
        });
        defer server.deinit();
        try server.start();
        try server.bootstrapLocal(2188, 1);
        var raft_progress = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &server, .run_once = progressRaft }, std.time.ns_per_ms);
        defer raft_progress.deinit();
        try raft_progress.start();
        var control_progress = raft.ManagedProgressDriver.init(io_impl.io(), .{ .ptr = &server, .run_once = progressControl }, std.time.ns_per_ms);
        defer control_progress.deinit();
        try control_progress.start();
        for (0..600) |_| {
            if (try server.server.svc.metadataIncarnation() != null) break;
            try io_impl.io().sleep(.fromMilliseconds(10), .awake);
        } else return error.MetadataIncarnationUnavailable;
        const uri = try server.adminBaseUri(a);
        defer a.free(uri);
        var network = executor.StdHttpExecutor.init(a, .{});
        defer network.deinit();
        var context: Context = .{ .alloc = a, .io = io_impl.io(), .metadata = &server, .transport = network.executor(), .admin_headers = &headers, .base = uri };
        if (restart == 0) try campaign.beforeRestart(&context) else try campaign.afterRestart(&context);
        try raft_progress.check();
        try control_progress.check();
    }
}
