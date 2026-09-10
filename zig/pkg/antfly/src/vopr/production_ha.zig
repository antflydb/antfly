// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! HA owners borrowed by the production cluster history. Replication applies
//! through DataServer into actual DBs; promotion crosses the authenticated
//! production admin surface and consumes the standby's durable WAL owner.
const std = @import("std");
const runtime = @import("../data/runtime.zig");
const ha = @import("../storage/ha/mod.zig");
const storage_io = @import("../storage/lsm_backend/storage_io.zig");
const wal = @import("../storage/wal.zig");
const http = @import("../common/http/http_common.zig");
const catalog = @import("../api/table_catalog.zig");
const api = @import("../api/http_server.zig");
const metadata_api = @import("../metadata/api.zig");
const table_manager = @import("../metadata/table_manager.zig");
const api_client = @import("../api/http_client.zig");
const background = @import("../storage/background_runtime.zig");

pub const Owners = struct {
    pub const token = "vopr-ha-scaling-admin";
    const identity: ha.primary.Identity = .{ .cluster_id = 6840, .shard_id = 0, .table_id = 0, .timeline_id = 1, .epoch = 1 };
    const log_path: [:0]const u8 = "/production-ha/receive";
    const progress_path: [:0]const u8 = "/production-ha/progress";
    const fence_path: [:0]const u8 = "/production-ha/fence";

    alloc: std.mem.Allocator,
    io: std.Io,
    storage: storage_io.IoStorage,
    replica_root: []u8,
    options: wal.WalOptions = .{},
    primary: ?ha.primary.Primary = null,
    promoted_lsn: u64 = 0,
    primary_server: ?runtime.DataServer = null,
    primary_uri: ?[]u8 = null,
    primary_root: []u8,
    standby: ?ha.standby.Standby = null,
    fences: ?ha.fencing.Store = null,
    server: ?runtime.DataServer = null,
    uri: ?[]u8 = null,
    boundary: u64 = 0,
    observed_progress: ha.standby.Progress = .{},
    promoted_sound: bool = false,

    pub fn create(alloc: std.mem.Allocator, io: std.Io, primary_root: []const u8) !*Owners {
        const self = try alloc.create(Owners);
        const replica_root = std.fmt.allocPrint(alloc, "{s}-standby", .{primary_root}) catch |err| {
            alloc.destroy(self);
            return err;
        };
        const owned_primary_root = std.fmt.allocPrint(alloc, "{s}-ha-primary", .{primary_root}) catch |err| {
            alloc.free(replica_root);
            alloc.destroy(self);
            return err;
        };
        self.* = .{ .alloc = alloc, .io = io, .storage = storage_io.IoStorage.init(io), .replica_root = replica_root, .primary_root = owned_primary_root };
        errdefer self.destroy();
        self.options = .{
            .storage = self.storage.storage(),
            .clock = .{ .ctx = self, .now_ns_fn = nowNs, .sleep_ns_fn = sleepNs },
        };
        self.primary = try ha.primary.Primary.open(alloc, "/production-ha/primary", "/production-ha/slots", identity, .{
            .replication_log_options = .{ .wal_options = self.options },
            .slot_store_options = .{ .wal_options = self.options },
        });
        try self.primary.?.createSlot("standby", 0);
        self.standby = try ha.standby.Standby.open(alloc, log_path, progress_path, identity, .{
            .receive_log_options = .{ .wal_options = self.options },
            .progress_wal_options = self.options,
        });
        self.fences = try ha.fencing.Store.open(alloc, fence_path, .{ .wal_options = self.options });
        return self;
    }

    fn nowNs(ptr: ?*anyopaque) u64 {
        const self: *Owners = @ptrCast(@alignCast(ptr.?));
        return @intCast(@max(0, std.Io.Clock.awake.now(self.io).nanoseconds));
    }

    fn sleepNs(ptr: ?*anyopaque, ns: u64) void {
        const self: *Owners = @ptrCast(@alignCast(ptr.?));
        self.io.sleep(.fromNanoseconds(@intCast(ns)), .awake) catch unreachable;
    }

    pub fn primaryConfig(self: *Owners) runtime.DataServerHAConfig {
        const primary = &self.primary.?;
        return .{
            .admin_context = .{ .primary = primary, .primary_node_id = "primary", .fence_store = &self.fences.? },
            .admin_bearer_token = token,
        };
    }

    // Standalone HA and quorum Raft are distinct production ownership modes.
    // Both participate in one scheduler history without stacking HA over a
    // Raft apply path, which deliberately bypasses primary HA publication.
    const tables = [_]table_manager.TableRecord{.{ .table_id = 6850, .name = "ha_docs", .placement_role = "data" }};
    const ranges = [_]table_manager.RangeRecord{.{ .table_id = 6850, .group_id = 6851, .range_id = 6851, .start_key = "", .end_key = null }};

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{ .metadata_group_id = 6849, .metrics = .{} };
    }
    fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        return .{ .status = try status(ptr), .tables = @constCast(&tables), .ranges = @constCast(&ranges), .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
    fn freeSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    fn routing(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
        return .{ .metadata_group_id = 6849, .tables = @constCast(&tables), .ranges = @constCast(&ranges) };
    }
    fn freeRouting(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
    fn catalogSource(self: *Owners) catalog.CatalogSource {
        return .{ .ptr = self, .io = @import("../runtime_io_abi.zig").Borrow.init(&self.io), .vtable = &.{ .admin_snapshot = snapshot, .free_admin_snapshot = freeSnapshot, .routing_snapshot = routing, .linearizable_routing_snapshot = routing, .free_routing_snapshot = freeRouting } };
    }
    fn statusSource(self: *Owners) api.StatusSource {
        return .{ .ptr = self, .vtable = &.{ .status = status, .admin_snapshot = snapshot, .free_admin_snapshot = freeSnapshot, .routing_snapshot = routing, .linearizable_routing_snapshot = routing, .free_routing_snapshot = freeRouting } };
    }
    pub fn startPrimary(self: *Owners, backend: *background.BackendRuntime) !void {
        self.primary_server = runtime.DataServer.initFromLocalMetadataSources(self.alloc, .{
            .replica_root_dir = self.primary_root,
            .backend_runtime = backend,
            .api_server_cfg = .{ .admin_bearer_token = token },
            .ha = self.primaryConfig(),
        }, self.catalogSource(), self.statusSource());
        try self.primary_server.?.startPublicHttp();
        self.primary_uri = try self.primary_server.?.baseUri(self.alloc);
    }
    pub fn write(self: *Owners, executor: http.RequestExecutor, uri: []const u8, body: []const u8) !void {
        var client = api_client.ApiHttpClient.init(self.alloc, executor);
        var response = try client.fetchBatchResponse(uri, "ha_docs", body);
        defer response.deinit(self.alloc);
        if (response.status != 201 and response.status != 202) return error.ProductionHAPublicWriteRejected;
    }
    pub fn verify(self: *Owners, executor: http.RequestExecutor, uri: []const u8, key: []const u8, value: []const u8) !void {
        // Standby reads explicitly request the supported stale-read policy;
        // their visible boundary is checked against durable apply progress.
        const path = try std.fmt.allocPrint(self.alloc, "{s}/tables/ha_docs/documents/{s}?consistency=stale", .{ uri, key });
        defer self.alloc.free(path);
        var response = try executor.execute(self.alloc, .{ .method = .GET, .uri = path });
        defer response.deinit(self.alloc);
        if (response.status != 200 or std.mem.indexOf(u8, response.body, value) == null) {
            std.debug.print("HA lookup key={s} status={d} body={s}\n", .{ key, response.status, response.body });
            return error.ProductionHALostAcknowledgedWrite;
        }
    }

    pub fn startStandby(self: *Owners, backend: *background.BackendRuntime) !void {
        self.server = runtime.DataServer.initFromLocalMetadataSources(self.alloc, .{
            .replica_root_dir = self.replica_root,
            .api_server_cfg = .{},
            .backend_runtime = backend,
            .ha = .{
                .admin_context = .{ .standby = &self.standby.?, .standby_node_id = "standby", .fence_store = &self.fences.? },
                .standby_owner = &self.standby,
                .admin_bearer_token = token,
                .standby_replication = .{ .upstream_base_uri = self.primary_uri.?, .slot_name = "standby", .standby_log_path = log_path, .standby_progress_path = progress_path },
            },
        }, self.catalogSource(), self.statusSource());
        try self.server.?.startPublicHttp();
        self.uri = try self.server.?.baseUri(self.alloc);
    }

    pub fn catchUp(self: *Owners, executor: http.RequestExecutor, upstream: []const u8) !void {
        _ = try self.server.?.replicateHAStandbyUntilCaughtUp(executor, upstream, "standby", .{ .max_records = 8 });
        if (self.primary.?.lastLsn() == 0) return error.ProductionHAEmptyReplicationStream;
        const progress = self.standby.?.currentProgress();
        self.observed_progress = progress;
        if (progress.applied_lsn != self.primary.?.lastLsn() or progress.safe_read_lsn != progress.applied_lsn)
            return error.ProductionHAStandbyNotSafe;
    }

    pub fn admin(self: *Owners, executor: http.RequestExecutor, uri: []const u8, body: []const u8, expected_status: u16) !void {
        var response = try executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .body = body,
            .content_type = "application/json",
            .authorization = "Bearer " ++ token,
        });
        defer response.deinit(self.alloc);
        if (response.status != expected_status) {
            std.log.err("production HA admin {s}: status={} body={s}", .{ uri, response.status, response.body });
            return error.ProductionHAAdminStatusMismatch;
        }
    }

    pub fn fence(self: *Owners, executor: http.RequestExecutor, upstream: []const u8) !void {
        const uri = try std.fmt.allocPrint(self.alloc, "{s}/admin/v1/ha/fence", .{upstream});
        defer self.alloc.free(uri);
        const body = try std.json.Stringify.valueAlloc(self.alloc, .{
            .identity = identity,
            .old_primary_id = "primary",
            .promoted_node_id = "standby",
            .new_timeline_id = @as(u64, 2),
            .new_epoch = @as(u64, 2),
            .generation = @as(u64, 1),
            .required_lsn = self.primary.?.lastLsn(),
            .observed_lsn = self.primary.?.lastLsn(),
            .force = false,
            .reason = "VOPR ownership cutover during automatic split",
        }, .{});
        defer self.alloc.free(body);
        try self.admin(executor, uri, body, 200);
        self.boundary = self.primary.?.lastLsn();
        if (self.boundary == 0) return error.ProductionHAEmptyPromotionBoundary;
    }

    pub fn standbyAdmin(self: *Owners, executor: http.RequestExecutor, expected_status: u16) !void {
        const uri = try std.fmt.allocPrint(self.alloc, "{s}/admin/v1/ha/promotion/current-fence", .{self.uri.?});
        defer self.alloc.free(uri);
        try self.admin(executor, uri, "{}", expected_status);
    }

    pub fn promote(self: *Owners, executor: http.RequestExecutor) !void {
        // Reconstruct the fence authority from durable storage before handing
        // ownership over. Both server contexts borrow this stable optional slot.
        self.fences.?.close();
        self.fences = null;
        self.fences = try ha.fencing.Store.open(self.alloc, fence_path, .{ .wal_options = self.options });
        try self.standbyAdmin(executor, 200);
        if (self.standby != null or self.server.?.ha_promoted_primary == null)
            return error.ProductionHAPromotionNotAdopted;
        const promoted = &self.server.?.ha_promoted_primary.?;
        self.promoted_lsn = promoted.lastLsn();
        self.promoted_sound = promoted.identity.timeline_id == 2 and
            promoted.identity.epoch == 2 and promoted.lastLsn() > self.boundary;
        if (!self.promoted_sound) return error.ProductionHAInvalidPromotion;
    }

    pub fn stopPrimary(self: *Owners) void {
        if (self.primary_server) |*server| {
            server.beginTeardown();
            server.quiesceBackgroundWork();
            server.deinit();
            self.primary_server = null;
        }
    }

    pub fn destroy(self: *Owners) void {
        self.stopPrimary();
        if (self.server) |*server| {
            server.beginTeardown();
            server.quiesceBackgroundWork();
            server.deinit();
        }
        if (self.uri) |uri| self.alloc.free(uri);
        if (self.standby) |*standby| standby.close();
        if (self.primary_uri) |uri| self.alloc.free(uri);
        self.alloc.free(self.primary_root);
        if (self.primary) |*primary| primary.close();
        if (self.fences) |*fences| fences.close();
        self.alloc.free(self.replica_root);
        self.alloc.destroy(self);
    }
};

test "production HA owners stream and promote through public HTTP on VoprIo" {
    const vopr = @import("vopr");
    var allocator: std.heap.DebugAllocator(.{ .stack_trace_frames = 0 }) = .init;
    defer std.debug.assert(allocator.deinit() == .ok);
    const alloc = allocator.allocator();
    var tmp = std.testing.tmpDir(.{}); // vopr-audit: allow(host_filesystem) namespace for unused ancillary API stores; modeled replication uses VoprIo
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ha", .{tmp.sub_path});
    defer alloc.free(root);
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{ .tasks = .{ .stack_size = 8 * 1024 * 1024 } });
    defer vopr_io.deinit();
    var backend = try background.BackendRuntimeHandle.init(alloc, .{ .backend = .manual, .borrowed_io = .{ .general = vopr_io.io() }, .filesystem_io = vopr_io.io() });
    defer backend.deinit();
    const Worker = struct {
        alloc: std.mem.Allocator,
        io: std.Io,
        backend: *background.BackendRuntime,
        root: []const u8,
        done: bool = false,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.runInner() catch |err| {
                self.failure = err;
                if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
            };
            self.done = true;
        }
        fn runInner(self: *@This()) !void {
            const owners = try Owners.create(self.alloc, self.io, self.root);
            defer owners.destroy();
            var client = @import("../common/http/io_http_executor.zig").IoHttpExecutor.init(self.alloc, self.io, .{ .keep_alive = false });
            defer client.deinit();
            try owners.startPrimary(self.backend);
            try owners.write(client.executor(), owners.primary_uri.?,
                \\{"inserts":{"before":{"title":"before-promotion"}},"sync_level":"write"}
            );
            try owners.startStandby(self.backend);
            try owners.catchUp(client.executor(), owners.primary_uri.?);
            try owners.verify(client.executor(), owners.uri.?, "before", "before-promotion");
            try owners.standbyAdmin(client.executor(), 409);
            try owners.fence(client.executor(), owners.primary_uri.?);
            try owners.catchUp(client.executor(), owners.primary_uri.?);
            owners.stopPrimary();
            try owners.promote(client.executor());
            try owners.write(client.executor(), owners.uri.?,
                \\{"inserts":{"after":{"title":"after-promotion"}},"sync_level":"write"}
            );
            try owners.verify(client.executor(), owners.uri.?, "before", "before-promotion");
            try owners.verify(client.executor(), owners.uri.?, "after", "after-promotion");
        }
    };
    var worker = Worker{ .alloc = alloc, .io = vopr_io.io(), .backend = backend.ptr(), .root = root };
    _ = vopr_io.io().async(Worker.run, .{&worker});
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    var choices = vopr.choice.PrefixedCooperativeSeeded.init(&.{}, 0xa17f_aa01);
    for (0..100_000) |step| {
        if (vopr_io.scheduler().quiescent()) break;
        enabled.items.clearRetainingCapacity();
        events.deinit(alloc);
        try vopr_io.scheduler().enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        if (enabled.items.items.len == 0) return error.ProductionHADeadlock;
        const selected = try choices.source().choose(.{
            .site_id = vopr.id.stable("choice", "production-ha.scheduler"),
            .site_name = "production-ha.scheduler",
            .occurrence = step,
            .enabled = enabled.items.items,
        });
        try vopr_io.scheduler().executeReady(selected, &events, alloc);
    } else return error.ProductionHATransitionBudgetExceeded;
    if (worker.failure) |err| return err;
    try std.testing.expect(worker.done);
    try vopr_io.ensureNoCapabilityViolation();
}
