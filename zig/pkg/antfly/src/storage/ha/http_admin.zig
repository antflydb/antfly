// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! HTTP adapter for HA admin commands.
//!
//! The storage HA command vocabulary lives in `admin_cli.zig`, and execution
//! lives in `admin_exec.zig`. This adapter gives operators and future node
//! servers a small HTTP surface without creating a second command contract.

const std = @import("std");
const Allocator = std.mem.Allocator;
const admin_api = @import("../../admin/mod.zig");
const http_common = @import("../../common/http/http_common.zig");
const ha_admin = @import("admin.zig");
const admin_cli = @import("admin_cli.zig");
const admin_exec = @import("admin_exec.zig");
const backup_manifest = @import("backup_manifest.zig");
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const rejoin = @import("rejoin.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Routes = struct {
    pub const health = "/ha/v1/health";
    pub const ready = "/ha/v1/ready";
    pub const command = "/ha/v1/admin/command";
};

pub const CommandRequest = struct {
    argv: []const []const u8,
};

pub const Server = struct {
    alloc: Allocator,
    ctx: admin_exec.Context,

    pub fn init(alloc: Allocator, ctx: admin_exec.Context) Server {
        return .{
            .alloc = alloc,
            .ctx = ctx,
        };
    }

    pub fn executor(self: *Server) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn deinit(self: *Server) void {
        self.* = undefined;
    }

    pub fn handle(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const path = requestPath(req.uri);
        switch (req.method) {
            .GET => {
                if (std.mem.eql(u8, path, Routes.health)) {
                    return try textResponse(self.alloc, 200, "ok");
                }
                if (std.mem.eql(u8, path, Routes.ready)) {
                    if (self.ready()) return try textResponse(self.alloc, 200, "ready");
                    return try textResponse(self.alloc, 503, "not ready");
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_primary_status)) {
                    return try self.handleAdminPrimaryStatus();
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_standby_status)) {
                    return try self.handleAdminStandbyStatus();
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_replication_slots)) {
                    return try self.handleAdminReplicationSlots();
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_fence_current)) {
                    return try self.handleAdminFenceCurrent();
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .POST => {
                if (std.mem.eql(u8, path, Routes.command)) {
                    return try self.handleCommand(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_replication_slots)) {
                    return try self.handleAdminCreateReplicationSlot(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_base_backups)) {
                    return try self.handleAdminBeginBaseBackup(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_base_backups_finish)) {
                    return try self.handleAdminFinishBaseBackup(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_standby_bootstrap)) {
                    return try self.handleAdminBootstrapStandby(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_fence)) {
                    return try self.handleAdminAcquireFence(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_promotion_assess)) {
                    return try self.handleAdminAssessPromotion(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_promotion_current_fence)) {
                    return try self.handleAdminPromoteCurrentFence();
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_promotion)) {
                    return try self.handleAdminPromote(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_rejoin_assess)) {
                    return try self.handleAdminAssessRejoin(req);
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .PUT => {
                if (admin_api.routes.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_pause_suffix)) |slot_name| {
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .pause);
                }
                if (admin_api.routes.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_resume_suffix)) |slot_name| {
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .@"resume");
                }
                if (knownFixedRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .DELETE => {
                if (admin_api.routes.replicationSlotNameFromPath(path, "")) |slot_name| {
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .drop);
                }
                if (knownFixedRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
        }
    }

    fn handleAdminPrimaryStatus(self: *Server) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .primary_status = .{} },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminStandbyStatus(self: *Server) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .standby_status = .{} },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminReplicationSlots(self: *Server) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .slot_list = .{} },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminCreateReplicationSlot(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA replication slot request");

        var parsed = std.json.parseFromSlice(
            admin_api.ReplicationSlotCreateRequest,
            self.alloc,
            req.body,
            .{ .ignore_unknown_fields = true },
        ) catch return try textResponse(self.alloc, 400, "invalid HA replication slot request");
        defer parsed.deinit();

        const initial_lsn: ?u64 = if (parsed.value.initial_lsn) |value| blk: {
            if (value < 0) return try textResponse(self.alloc, 400, "invalid HA replication slot request");
            break :blk @intCast(value);
        } else null;

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .slot = .{
                .action = .create,
                .request = ha_admin.SlotRequest{
                    .slot_name = parsed.value.slot_name,
                    .initial_lsn = initial_lsn,
                },
            } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminReplicationSlotLifecycle(
        self: *Server,
        slot_name: []const u8,
        action: ha_admin.SlotAction,
    ) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .slot = .{
                .action = action,
                .request = ha_admin.SlotRequest{
                    .slot_name = slot_name,
                },
            } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminBeginBaseBackup(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA base backup request");
        var parsed = admin_api.openapi.server.parseBeginHABaseBackupBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA base backup request");
        defer parsed.deinit();

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .seed = .{ .begin = .{
                .slot_name = parsed.value.slot_name,
                .manifest_id = parsed.value.manifest_id,
            } } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminFinishBaseBackup(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA base backup finish request");
        var parsed = admin_api.openapi.server.parseFinishHABaseBackupBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA base backup finish request");
        defer parsed.deinit();

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .seed = .{ .finish = .{
                .manifest_path = parsed.value.manifest_path,
            } } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminBootstrapStandby(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA standby bootstrap request");
        var parsed = admin_api.openapi.server.parseBootstrapHAStandbyBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA standby bootstrap request");
        defer parsed.deinit();

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .seed = .{ .bootstrap = .{
                .manifest_path = parsed.value.manifest_path,
                .content_root = parsed.value.content_root,
            } } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminFenceCurrent(self: *Server) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .fence_current,
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminAcquireFence(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence = self.parseFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA fence request");
        };
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .fence_acquire = fence },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminAssessPromotion(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        var command = admin_cli.PromoteAssessCommand{ .check = .{} };
        if (req.body.len != 0) {
            var parsed = admin_api.openapi.server.parseAssessHAPromotionBody(
                self.alloc,
                req.body,
            ) catch return try textResponse(self.alloc, 400, "invalid HA promotion assessment request");
            defer parsed.deinit();

            if (parsed.value.required_lsn) |value| {
                command.check.required_lsn = uint64FromJson(value) catch {
                    return try textResponse(self.alloc, 400, "invalid HA promotion assessment request");
                };
            }
            command.check.fencing_confirmed = parsed.value.fencing_confirmed orelse false;
            command.check.force = parsed.value.force orelse false;
            command.use_current_fence = parsed.value.use_current_fence orelse false;
        }

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .promote_assess = command },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminPromoteCurrentFence(self: *Server) !http_common.HttpResponse {
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .promote_current_fence,
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminPromote(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence = self.parseFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA promotion request");
        };
        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .promote = .{ .fence = fence } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn handleAdminAssessRejoin(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA rejoin assessment request");
        var parsed = admin_api.openapi.server.parseAssessHARejoinBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        defer parsed.deinit();

        const identity = adminIdentityFromOpenApi(parsed.value.identity) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        const last_lsn = uint64FromJson(parsed.value.last_lsn) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        const retained_from_lsn = uint64FromJson(parsed.value.retained_from_lsn) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        const receipt = if (parsed.value.receipt) |value|
            adminFenceReceiptFromOpenApi(value) catch {
                return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
            }
        else
            null;

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .rejoin_assess = .{
                .former = rejoin.FormerPrimaryState{
                    .node_id = parsed.value.node_id,
                    .identity = identity,
                    .last_lsn = last_lsn,
                },
                .receipt = receipt,
                .policy = rejoin.RejoinPolicy{
                    .retained_from_lsn = retained_from_lsn,
                    .allow_rewind_after_forced_promotion = parsed.value.allow_rewind_after_forced_promotion orelse false,
                },
            } },
        };
        defer plan.deinit(self.alloc);
        return try self.handleJsonPlan(plan);
    }

    fn parseFenceRequest(self: *Server, req: http_common.HttpRequest) !fencing.FenceRequest {
        if (req.body.len == 0) return error.InvalidAdminRequest;
        var parsed = admin_api.openapi.server.parseAcquireHAFenceBody(
            self.alloc,
            req.body,
        ) catch return error.InvalidAdminRequest;
        defer parsed.deinit();
        return adminFenceRequestFromOpenApi(parsed.value) catch return error.InvalidAdminRequest;
    }

    fn handleCommand(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA command request");

        var parsed = std.json.parseFromSlice(
            CommandRequest,
            self.alloc,
            req.body,
            .{ .ignore_unknown_fields = true },
        ) catch return try textResponse(self.alloc, 400, "invalid HA command request");
        defer parsed.deinit();

        var plan = admin_cli.parse(self.alloc, parsed.value.argv) catch return try textResponse(self.alloc, 400, "invalid HA command argv");
        defer plan.deinit(self.alloc);

        var rendered = admin_exec.executeAndRenderAlloc(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        errdefer rendered.deinit(self.alloc);

        return .{
            .status = 200,
            .content_type = try self.alloc.dupe(u8, rendered.content_type),
            .body = rendered.body,
        };
    }

    fn handleJsonPlan(self: *Server, plan: admin_cli.Plan) !http_common.HttpResponse {
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);

        return .{
            .status = 200,
            .content_type = try self.alloc.dupe(u8, "application/json"),
            .body = try admin_exec.renderJsonAlloc(self.alloc, result),
        };
    }

    fn execute(ptr: *anyopaque, _: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *Server = @ptrCast(@alignCast(ptr));
        return try self.handle(req);
    }

    fn ready(self: *const Server) bool {
        return self.ctx.primary != null or self.ctx.standby != null or self.ctx.fence_store != null;
    }
};

fn requestPath(uri: []const u8) []const u8 {
    const scheme_index = std.mem.indexOf(u8, uri, "://") orelse return uri;
    const authority_start = scheme_index + 3;
    const path_index = std.mem.indexOfScalarPos(u8, uri, authority_start, '/') orelse return "/";
    return uri[path_index..];
}

fn knownFixedRoute(path: []const u8) bool {
    return std.mem.eql(u8, path, Routes.health) or
        std.mem.eql(u8, path, Routes.ready) or
        std.mem.eql(u8, path, Routes.command) or
        std.mem.eql(u8, path, admin_api.routes.ha_primary_status) or
        std.mem.eql(u8, path, admin_api.routes.ha_standby_status) or
        std.mem.eql(u8, path, admin_api.routes.ha_replication_slots) or
        std.mem.eql(u8, path, admin_api.routes.ha_base_backups) or
        std.mem.eql(u8, path, admin_api.routes.ha_base_backups_finish) or
        std.mem.eql(u8, path, admin_api.routes.ha_standby_bootstrap) or
        std.mem.eql(u8, path, admin_api.routes.ha_fence) or
        std.mem.eql(u8, path, admin_api.routes.ha_fence_current) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion_assess) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion_current_fence) or
        std.mem.eql(u8, path, admin_api.routes.ha_rejoin_assess);
}

fn adminFenceRequestFromOpenApi(request: admin_api.FenceAcquireRequest) !fencing.FenceRequest {
    return .{
        .identity = try adminIdentityFromOpenApi(request.identity),
        .old_primary_id = request.old_primary_id,
        .promoted_node_id = request.promoted_node_id,
        .new_timeline_id = try positiveUint64FromJson(request.new_timeline_id),
        .new_epoch = try positiveUint64FromJson(request.new_epoch),
        .required_lsn = try positiveUint64FromJson(request.required_lsn),
        .observed_lsn = try uint64FromJson(request.observed_lsn),
        .force = request.force orelse false,
        .reason = request.reason orelse "",
    };
}

fn adminFenceReceiptFromOpenApi(receipt: admin_api.HAFenceReceipt) !fencing.Receipt {
    return .{
        .identity = try adminIdentityFromOpenApi(receipt.identity),
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .parent_timeline_id = try positiveUint64FromJson(receipt.parent_timeline_id),
        .parent_epoch = try positiveUint64FromJson(receipt.parent_epoch),
        .new_timeline_id = try positiveUint64FromJson(receipt.new_timeline_id),
        .new_epoch = try positiveUint64FromJson(receipt.new_epoch),
        .required_lsn = try positiveUint64FromJson(receipt.required_lsn),
        .observed_lsn = try uint64FromJson(receipt.observed_lsn),
        .generation = try positiveUint64FromJson(receipt.generation),
        .forced = receipt.forced,
        .token = receipt.token,
        .reason = receipt.reason,
    };
}

fn adminIdentityFromOpenApi(identity: admin_api.openapi.HAIdentity) !standby_mod.Identity {
    return .{
        .cluster_id = try positiveUint64FromJson(identity.cluster_id),
        .shard_id = try positiveUint64FromJson(identity.shard_id),
        .table_id = try positiveUint64FromJson(identity.table_id),
        .timeline_id = try positiveUint64FromJson(identity.timeline_id),
        .epoch = try positiveUint64FromJson(identity.epoch),
    };
}

fn uint64FromJson(value: i64) !u64 {
    if (value < 0) return error.InvalidAdminRequest;
    return @intCast(value);
}

fn positiveUint64FromJson(value: i64) !u64 {
    const parsed = try uint64FromJson(value);
    if (parsed == 0) return error.InvalidAdminRequest;
    return parsed;
}

fn commandErrorStatus(err: anyerror) u16 {
    return switch (err) {
        error.PrimaryUnavailable,
        error.StandbyUnavailable,
        error.FenceStoreUnavailable,
        error.FenceAlreadyHeld,
        error.FenceReceiptMissing,
        error.BaseBackupSlotInUse,
        error.BackupSlotNotRetained,
        error.SlotAlreadyExists,
        error.SlotInactive,
        error.SlotRequiresReseed,
        error.WalNoLongerRetained,
        error.MissingReceivedRecord,
        error.RecordAlreadyReceived,
        error.StandbyAlreadyBootstrapped,
        error.SyncPolicyUnsatisfied,
        => 409,
        error.SlotNotFound,
        error.BackupStartNotFound,
        error.BackupSlotNotFound,
        => 404,
        error.PrometheusUnsupportedForResult,
        error.InvalidSlotName,
        error.InvalidSlotProgress,
        error.InvalidReplicationError,
        error.InvalidReplicationStartLsn,
        error.ReplicationStartAheadOfPrimary,
        error.InvalidCheckpointLsn,
        error.InvalidBackupLsn,
        error.InvalidManifestId,
        error.ManifestIdTooLong,
        error.EmptyManifest,
        error.TooManyManifestFiles,
        error.InvalidManifestPath,
        error.ManifestPathTooLong,
        error.DuplicateManifestPath,
        error.ManifestFileSetMismatch,
        error.BackupStartNotDurable,
        error.BackupCheckpointNotDurable,
        error.BackupStartMismatch,
        error.InitialLsnAheadOfPrimary,
        error.ManifestPathMissing,
        error.ManifestFileTooLarge,
        error.ManifestFileMissing,
        error.ManifestFileCrcMismatch,
        error.ManifestFileSizeMismatch,
        error.InvalidOldPrimaryId,
        error.InvalidPromotedNodeId,
        error.InvalidTimelineSwitch,
        error.InvalidFenceLsn,
        error.FenceRequiresForce,
        error.FenceFieldTooLong,
        error.FencingRequired,
        error.PromotionRequiresForce,
        error.PromotionNotAllowed,
        error.StandbyAheadOfPrimary,
        error.TargetAheadOfPrimary,
        error.InvalidSyncPolicy,
        error.WrongCluster,
        error.WrongShard,
        error.WrongTable,
        error.WrongTimeline,
        error.WrongEpoch,
        => 400,
        else => 500,
    };
}

fn textResponse(alloc: Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,
    backup_root: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
        alloc.free(self.backup_root);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);
    const fence_wal = try allocPrintPath(alloc, name, "fence-wal", nonce);
    defer alloc.free(fence_wal);
    const backup_root = try allocPrintPath(alloc, name, "backup-root", nonce);
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
        .fence_wal = try alloc.dupeZ(u8, fence_wal),
        .backup_root = try alloc.dupeZ(u8, backup_root),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-http-admin-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn seedFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = bytes,
    });
}

test "storage.ha http admin serves health and command endpoint" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "command");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = Server.init(alloc, .{ .primary = &primary, .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
    try std.testing.expectEqualStrings("ok", health.body);

    var ready = try server.handle(.{ .method = .GET, .uri = Routes.ready });
    defer ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ready.status);
    try std.testing.expectEqualStrings("ready", ready.body);

    var create = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"slot\",\"create\",\"standby-a\",\"--initial-lsn\",\"0\"]}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), create.status);
    try std.testing.expectEqualStrings("application/json", create.content_type.?);
    try expectContains(create.body, "\"slot_name\":\"standby-a\"");

    var typed_status = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_primary_status,
    });
    defer typed_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_status.status);
    try std.testing.expectEqualStrings("application/json", typed_status.content_type.?);
    try expectContains(typed_status.body, "\"primary_status\"");
    try expectContains(typed_status.body, "\"current_lsn\":0");

    var typed_create = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-b\",\"initial_lsn\":0}",
    });
    defer typed_create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_create.status);
    try std.testing.expectEqualStrings("application/json", typed_create.content_type.?);
    try expectContains(typed_create.body, "\"slot_name\":\"standby-b\"");

    var typed_slots = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_replication_slots,
    });
    defer typed_slots.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_slots.status);
    try std.testing.expectEqualStrings("application/json", typed_slots.content_type.?);
    try expectContains(typed_slots.body, "\"slot_list\"");
    try expectContains(typed_slots.body, "\"name\":\"standby-a\"");
    try expectContains(typed_slots.body, "\"name\":\"standby-b\"");

    const typed_pause_uri = try admin_api.routes.replicationSlotPausePathAlloc(alloc, "standby-b");
    defer alloc.free(typed_pause_uri);
    var typed_pause = try server.handle(.{
        .method = .PUT,
        .uri = typed_pause_uri,
    });
    defer typed_pause.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_pause.status);
    try std.testing.expectEqualStrings("application/json", typed_pause.content_type.?);
    try expectContains(typed_pause.body, "\"slot_name\":\"standby-b\"");
    try expectContains(typed_pause.body, "\"active\":false");

    const typed_resume_uri = try admin_api.routes.replicationSlotResumePathAlloc(alloc, "standby-b");
    defer alloc.free(typed_resume_uri);
    var typed_resume = try server.handle(.{
        .method = .PUT,
        .uri = typed_resume_uri,
    });
    defer typed_resume.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_resume.status);
    try std.testing.expectEqualStrings("application/json", typed_resume.content_type.?);
    try expectContains(typed_resume.body, "\"slot_name\":\"standby-b\"");
    try expectContains(typed_resume.body, "\"active\":true");

    const typed_drop_uri = try admin_api.routes.replicationSlotPathAlloc(alloc, "standby-b");
    defer alloc.free(typed_drop_uri);
    var typed_drop = try server.handle(.{
        .method = .DELETE,
        .uri = typed_drop_uri,
    });
    defer typed_drop.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_drop.status);
    try std.testing.expectEqualStrings("application/json", typed_drop.content_type.?);
    try expectContains(typed_drop.body, "\"slot_name\":\"standby-b\"");

    var invalid_typed_create = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{}",
    });
    defer invalid_typed_create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_typed_create.status);
    try expectContains(invalid_typed_create.body, "invalid HA replication slot request");

    var duplicate_create = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"slot\",\"create\",\"standby-a\",\"--initial-lsn\",\"0\"]}",
    });
    defer duplicate_create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), duplicate_create.status);
    try expectContains(duplicate_create.body, "SlotAlreadyExists");

    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
    var stream = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"stream\",\"once\",\"--slot\",\"standby-a\"]}",
    });
    defer stream.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), stream.status);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", stream.content_type.?);
    try expectContains(stream.body, "result=stream_once\n");
    try expectContains(stream.body, "received_count=1\n");
    try expectContains(stream.body, "applied_lsn=1\n");

    var typed_standby_status = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_standby_status,
    });
    defer typed_standby_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_standby_status.status);
    try std.testing.expectEqualStrings("application/json", typed_standby_status.content_type.?);
    try expectContains(typed_standby_status.body, "\"standby_status\"");
    try expectContains(typed_standby_status.body, "\"received_lsn\":1");
    try expectContains(typed_standby_status.body, "\"applied_lsn\":1");

    var invalid_progress = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"standby\",\"ack\",\"--slot\",\"standby-a\",\"--timeline-id\",\"1\",\"--received-lsn\",\"1\",\"--applied-lsn\",\"2\"]}",
    });
    defer invalid_progress.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_progress.status);
    try expectContains(invalid_progress.body, "InvalidSlotProgress");

    var invalid_seed = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"seed\",\"begin\",\"--slot\",\"standby-a\",\"--manifest-id\",\"\"]}",
    });
    defer invalid_seed.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_seed.status);
    try expectContains(invalid_seed.body, "InvalidManifestId");

    try primary.pauseSlot("standby-a");
    var inactive_stream = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"stream\",\"--slot\",\"standby-a\",\"--from-lsn\",\"1\"]}",
    });
    defer inactive_stream.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), inactive_stream.status);
    try expectContains(inactive_stream.body, "SlotInactive");
    try primary.resumeSlot("standby-a");

    var fail_closed_append = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"commit\",\"append\",\"--payload\",\"two\",\"--sync-mode\",\"remote-write\",\"--sync-standby\",\"standby-a\",\"--sync-failure\",\"fail-closed\"]}",
    });
    defer fail_closed_append.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), fail_closed_append.status);
    try expectContains(fail_closed_append.body, "SyncPolicyUnsatisfied");
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var typed_fence = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_fence,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1,\"reason\":\"http-admin-test\"}",
    });
    defer typed_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_fence.status);
    try std.testing.expectEqualStrings("application/json", typed_fence.content_type.?);
    try expectContains(typed_fence.body, "\"fence_acquire\"");
    try expectContains(typed_fence.body, "\"promoted_node_id\":\"standby-a\"");

    var typed_current_fence = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_fence_current,
    });
    defer typed_current_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_current_fence.status);
    try std.testing.expectEqualStrings("application/json", typed_current_fence.content_type.?);
    try expectContains(typed_current_fence.body, "\"fence_current\"");
    try expectContains(typed_current_fence.body, "\"old_primary_id\":\"primary-a\"");

    var typed_rejoin_unfenced = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":1,\"retained_from_lsn\":0}",
    });
    defer typed_rejoin_unfenced.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rejoin_unfenced.status);
    try std.testing.expectEqualStrings("application/json", typed_rejoin_unfenced.content_type.?);
    try expectContains(typed_rejoin_unfenced.body, "\"rejoin_assess\"");
    try expectContains(typed_rejoin_unfenced.body, "\"action\":\"reject_unfenced\"");
    try expectContains(typed_rejoin_unfenced.body, "\"reason\":\"no_fence\"");

    var typed_rejoin_fenced = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":0,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer typed_rejoin_fenced.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rejoin_fenced.status);
    try std.testing.expectEqualStrings("application/json", typed_rejoin_fenced.content_type.?);
    try expectContains(typed_rejoin_fenced.body, "\"rejoin_assess\"");
    try expectContains(typed_rejoin_fenced.body, "\"action\":\"rewind\"");
    try expectContains(typed_rejoin_fenced.body, "\"target_timeline_id\":2");

    var typed_promote_assess = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_assess,
        .content_type = "application/json",
        .body = "{\"use_current_fence\":true}",
    });
    defer typed_promote_assess.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote_assess.status);
    try std.testing.expectEqualStrings("application/json", typed_promote_assess.content_type.?);
    try expectContains(typed_promote_assess.body, "\"promote_assess\"");
    try expectContains(typed_promote_assess.body, "\"can_promote\":true");

    var typed_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_current_fence,
        .content_type = "application/json",
    });
    defer typed_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote.status);
    try std.testing.expectEqualStrings("application/json", typed_promote.content_type.?);
    try expectContains(typed_promote.body, "\"promote_current_fence\"");
    try expectContains(typed_promote.body, "\"new_identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2");
}

test "storage.ha http admin serves typed base backup seed endpoints" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "seed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = Server.init(alloc, .{ .primary = &primary, .standby = &standby });
    defer server.deinit();

    var typed_begin = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_base_backups,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-seed\",\"manifest_id\":\"base-http\"}",
    });
    defer typed_begin.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_begin.status);
    try std.testing.expectEqualStrings("application/json", typed_begin.content_type.?);
    try expectContains(typed_begin.body, "\"seed_begin\"");
    try expectContains(typed_begin.body, "\"backup_lsn\":1");
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));

    const files = seedFiles();
    const encoded_manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = "base-http",
        .backup_lsn = 1,
        .checkpoint_lsn = 2,
        .files = &files,
    });
    defer alloc.free(encoded_manifest);

    const manifest_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "backup.afha" });
    defer alloc.free(manifest_path);
    const manifest_file_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "manifest" });
    defer alloc.free(manifest_file_path);
    const sstable_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "sst/0001" });
    defer alloc.free(sstable_path);
    try writeTestFile(manifest_path, encoded_manifest);
    try writeTestFile(manifest_file_path, "manifest");
    try writeTestFile(sstable_path, "sstable");

    const finish_body = try std.fmt.allocPrint(alloc, "{{\"manifest_path\":\"{s}\"}}", .{manifest_path});
    defer alloc.free(finish_body);
    var typed_finish = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_base_backups_finish,
        .content_type = "application/json",
        .body = finish_body,
    });
    defer typed_finish.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_finish.status);
    try std.testing.expectEqualStrings("application/json", typed_finish.content_type.?);
    try expectContains(typed_finish.body, "\"seed_finish\"");
    try expectContains(typed_finish.body, "\"manifest_id\":\"base-http\"");
    try expectContains(typed_finish.body, "\"end_record_lsn\":3");

    const bootstrap_body = try std.fmt.allocPrint(
        alloc,
        "{{\"manifest_path\":\"{s}\",\"content_root\":\"{s}\"}}",
        .{ manifest_path, paths.backup_root },
    );
    defer alloc.free(bootstrap_body);
    var typed_bootstrap = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_standby_bootstrap,
        .content_type = "application/json",
        .body = bootstrap_body,
    });
    defer typed_bootstrap.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_bootstrap.status);
    try std.testing.expectEqualStrings("application/json", typed_bootstrap.content_type.?);
    try expectContains(typed_bootstrap.body, "\"seed_bootstrap\"");
    try expectContains(typed_bootstrap.body, "\"manifest_id\":\"base-http\"");
    try expectContains(typed_bootstrap.body, "\"checkpoint_lsn\":2");
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());
}

test "storage.ha http admin returns route method and command errors" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var missing = try server.handle(.{ .method = .GET, .uri = "/ha/v1/missing" });
    defer missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing.status);

    var not_ready = try server.handle(.{ .method = .GET, .uri = Routes.ready });
    defer not_ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), not_ready.status);
    try std.testing.expectEqualStrings("not ready", not_ready.body);

    var wrong_method = try server.handle(.{ .method = .PUT, .uri = Routes.command });
    defer wrong_method.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), wrong_method.status);

    var bad_json = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{",
    });
    defer bad_json.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bad_json.status);

    var unavailable = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"identify\"]}",
    });
    defer unavailable.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), unavailable.status);
    try expectContains(unavailable.body, "PrimaryUnavailable");
}

test "storage.ha http admin exposes request executor" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();
    const executor = server.executor();
    var health = try executor.execute(alloc, .{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
}

test "storage.ha http admin accepts absolute URIs" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = "http://ha-admin.test/ha/v1/health" });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);

    var ready = try server.handle(.{ .method = .GET, .uri = "http://ha-admin.test/ha/v1/ready" });
    defer ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), ready.status);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
