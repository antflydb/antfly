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
const commit_gate = @import("commit_gate.zig");
const fencing = @import("fencing.zig");
const owner_job_gate = @import("owner_job_gate.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const replication_record = @import("replication_record.zig");
const rejoin = @import("rejoin.zig");
const standby_mod = @import("standby.zig");
const status_mod = @import("status.zig");
const write_gate = @import("write_gate.zig");

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
                    return try self.handleAdminPrimaryStatus(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_standby_status)) {
                    return try self.handleAdminStandbyStatus(req);
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
                if (std.mem.eql(u8, path, admin_api.routes.ha_commit_check)) {
                    return try self.handleAdminCommitCheck(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_commit_append)) {
                    return try self.handleAdminCommitAppend(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_read_check)) {
                    return try self.handleAdminReadCheck(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_write_check)) {
                    return try self.handleAdminWriteCheck(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_owner_job_check)) {
                    return try self.handleAdminOwnerJobCheck(req);
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

    fn handleAdminPrimaryStatus(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        const query = requestQuery(req.uri);
        const max_lag_lsn = if (queryValue(query, "max_lag_lsn")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA primary status request")
        else
            0;
        var sync = buildSyncPolicyFromQuery(self.alloc, query) catch
            return try textResponse(self.alloc, 400, "invalid HA primary status request");
        errdefer sync.deinit(self.alloc);
        defer sync.deinit(self.alloc);

        var snapshot = ha_admin.primaryStatus(self.alloc, primary, .{ .max_lag_lsn = max_lag_lsn }, sync.policy) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer snapshot.deinit(self.alloc);
        return try self.handleTypedJson(status_mod.primaryStatusDocument(snapshot));
    }

    fn handleAdminStandbyStatus(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const query = requestQuery(req.uri);
        const upstream_lsn = if (queryValue(query, "upstream_lsn")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA standby status request")
        else
            null;

        return try self.handleTypedJson(status_mod.standbyStatusDocument(
            ha_admin.standbyStatus(standby, upstream_lsn),
        ));
    }

    fn handleAdminReplicationSlots(self: *Server) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        var snapshot = ha_admin.primaryStatus(self.alloc, primary, .{}, null) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer snapshot.deinit(self.alloc);
        return try self.handleTypedJson(status_mod.primaryStatusDocument(snapshot));
    }

    fn handleAdminCreateReplicationSlot(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA replication slot request");

        var parsed = admin_api.openapi.server.parseCreateHAReplicationSlotBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA replication slot request");
        defer parsed.deinit();

        const initial_lsn: ?u64 = if (parsed.value.initial_lsn) |value| blk: {
            if (value < 0) return try textResponse(self.alloc, 400, "invalid HA replication slot request");
            break :blk @intCast(value);
        } else null;

        const result = ha_admin.applySlotAction(primary, .create, .{
            .slot_name = parsed.value.slot_name,
            .initial_lsn = initial_lsn,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return switch (result) {
            .create => |slot| try self.handleTypedJson(slotActionDocument("create", slot, null)),
            else => unreachable,
        };
    }

    fn handleAdminReplicationSlotLifecycle(
        self: *Server,
        slot_name: []const u8,
        action: ha_admin.SlotAction,
    ) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        const result = ha_admin.applySlotAction(primary, action, .{
            .slot_name = slot_name,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return switch (result) {
            .create => unreachable,
            .pause => |slot| try self.handleTypedJson(slotActionDocument("pause", slot, slot.dropped)),
            .@"resume" => |slot| try self.handleTypedJson(slotActionDocument("resume", slot, slot.dropped)),
            .drop => |slot| try self.handleTypedJson(slotActionDocument("drop", slot, slot.dropped)),
        };
    }

    fn handleAdminCommitCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA commit check request");
        var parsed = admin_api.openapi.server.parseCheckHACommitBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA commit check request");
        defer parsed.deinit();

        const target_lsn = uint64FromJson(parsed.value.target_lsn) catch {
            return try textResponse(self.alloc, 400, "invalid HA commit check request");
        };
        const policy = syncPolicyFromOpenApi(parsed.value.sync_policy) catch {
            return try textResponse(self.alloc, 400, "invalid HA commit check request");
        };
        const gate = ha_admin.evaluateCommit(primary, target_lsn, policy) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return try self.handleTypedJson(CommitCheckDocument{
            .gate = commitGateDocument(gate),
        });
    }

    fn handleAdminCommitAppend(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA commit append request");
        var parsed = admin_api.openapi.server.parseAppendHACommitBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA commit append request");
        defer parsed.deinit();

        const append = appendOptionsFromOpenApi(parsed.value) catch {
            return try textResponse(self.alloc, 400, "invalid HA commit append request");
        };
        const policy = syncPolicyFromOpenApi(parsed.value.sync_policy) catch {
            return try textResponse(self.alloc, 400, "invalid HA commit append request");
        };
        const result = commit_gate.appendAndEvaluate(primary, append, policy) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return try self.handleTypedJson(CommitAppendDocument{
            .lsn = result.lsn,
            .gate = commitGateDocument(result.gate),
        });
    }

    fn handleAdminReadCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const request = if (req.body.len == 0)
            read_gate.Request{}
        else blk: {
            var parsed = admin_api.openapi.server.parseCheckHAReadBody(
                self.alloc,
                req.body,
            ) catch return try textResponse(self.alloc, 400, "invalid HA read check request");
            defer parsed.deinit();
            break :blk readRequestFromOpenApi(parsed.value) catch {
                return try textResponse(self.alloc, 400, "invalid HA read check request");
            };
        };
        const decision = ha_admin.evaluateStandbyRead(standby, request) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return try self.handleTypedJson(ReadCheckDocument{ .decision = decision });
    }

    fn handleAdminWriteCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA write check request");
        var parsed = admin_api.openapi.server.parseCheckHAWriteBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA write check request");
        defer parsed.deinit();
        const request = writeRequestFromOpenApi(parsed.value) catch {
            return try textResponse(self.alloc, 400, "invalid HA write check request");
        };
        const decision = switch (request.role) {
            .primary => blk: {
                const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
                break :blk ha_admin.evaluatePrimaryWrite(primary, request.request) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
            .standby => blk: {
                const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
                break :blk ha_admin.evaluateStandbyWrite(standby, request.request) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
        };
        return try self.handleTypedJson(WriteCheckDocument{ .decision = decision });
    }

    fn handleAdminOwnerJobCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA owner job check request");
        var parsed = admin_api.openapi.server.parseCheckHAOwnerJobBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA owner job check request");
        defer parsed.deinit();
        const request = ownerJobRequestFromOpenApi(parsed.value) catch {
            return try textResponse(self.alloc, 400, "invalid HA owner job check request");
        };
        const decision = switch (request.role) {
            .primary => blk: {
                const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
                break :blk ha_admin.evaluatePrimaryOwnerJob(primary, request.request) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
            .standby => blk: {
                const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
                break :blk ha_admin.evaluateStandbyOwnerJob(standby, request.request) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
        };
        return try self.handleTypedJson(OwnerJobCheckDocument{ .decision = decision });
    }

    fn handleAdminBeginBaseBackup(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA base backup request");
        var parsed = admin_api.openapi.server.parseBeginHABaseBackupBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA base backup request");
        defer parsed.deinit();

        const result = ha_admin.beginBaseBackup(primary, .{
            .slot_name = parsed.value.slot_name,
            .manifest_id = parsed.value.manifest_id,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return try self.handleTypedJson(BaseBackupBeginDocument{
            .backup_lsn = result.backup_lsn,
            .start_record_lsn = result.start_record_lsn,
        });
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
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return switch (result) {
            .seed_finish => |seed| try self.handleTypedJson(BaseBackupFinishDocument{
                .manifest_id = seed.manifest_id,
                .backup_lsn = seed.backup_lsn,
                .end_record_lsn = seed.end_record_lsn,
            }),
            else => unreachable,
        };
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
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return switch (result) {
            .seed_bootstrap => |seed| try self.handleTypedJson(StandbyBootstrapDocument{
                .manifest_id = seed.manifest_id,
                .backup_lsn = seed.backup_lsn,
                .checkpoint_lsn = seed.checkpoint_lsn,
            }),
            else => unreachable,
        };
    }

    fn handleAdminFenceCurrent(self: *Server) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        var current = ha_admin.currentPromotionFence(self.alloc, fence_store) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer if (current) |*result| result.deinit(self.alloc);

        if (current) |result| {
            return try self.handleTypedJson(CurrentFenceDocument{
                .held = true,
                .receipt = result.receipt,
            });
        }
        return try self.handleTypedJson(CurrentFenceDocument{
            .held = false,
            .receipt = null,
        });
    }

    fn handleAdminAcquireFence(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        const fence = self.parseFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA fence request");
        };
        var result = ha_admin.acquirePromotionFence(self.alloc, fence_store, fence) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return try self.handleTypedJson(FenceDocument{ .receipt = result.receipt });
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
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return switch (result) {
            .promote_assess => |assessment| try self.handleTypedJson(PromotionAssessDocument{
                .assessment = assessment,
            }),
            else => unreachable,
        };
    }

    fn handleAdminPromoteCurrentFence(self: *Server) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        var result = ha_admin.promoteWithCurrentFence(self.alloc, fence_store, standby) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return try self.handleTypedJson(promotionDocument(result));
    }

    fn handleAdminPromote(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const fence = self.parseFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA promotion request");
        };
        var result = ha_admin.promoteWithFence(self.alloc, fence_store, standby, .{ .fence = fence }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return try self.handleTypedJson(promotionDocument(result));
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

        return try self.handleTypedJson(RejoinAssessDocument{
            .assessment = ha_admin.assessFormerPrimaryRejoin(.{
                .node_id = parsed.value.node_id,
                .identity = identity,
                .last_lsn = last_lsn,
            }, receipt, .{
                .retained_from_lsn = retained_from_lsn,
                .allow_rewind_after_forced_promotion = parsed.value.allow_rewind_after_forced_promotion orelse false,
            }),
        });
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

    fn handleTypedJson(self: *Server, value: anytype) !http_common.HttpResponse {
        return .{
            .status = 200,
            .content_type = try self.alloc.dupe(u8, "application/json"),
            .body = try std.json.Stringify.valueAlloc(self.alloc, value, .{}),
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

const SlotActionDocument = struct {
    schema_version: u32 = 1,
    slot_action: []const u8,
    slot: SlotDocument,
};

const BaseBackupBeginDocument = struct {
    schema_version: u32 = 1,
    backup_lsn: u64,
    start_record_lsn: u64,
};

const BaseBackupFinishDocument = struct {
    schema_version: u32 = 1,
    manifest_id: []const u8,
    backup_lsn: u64,
    end_record_lsn: u64,
};

const StandbyBootstrapDocument = struct {
    schema_version: u32 = 1,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
};

const CommitCheckDocument = struct {
    schema_version: u32 = 1,
    gate: CommitGateDocument,
};

const CommitAppendDocument = struct {
    schema_version: u32 = 1,
    lsn: u64,
    gate: CommitGateDocument,
};

const CommitGateDocument = struct {
    target_lsn: u64,
    action: commit_gate.Action,
    durability: primary_mod.DurabilityDecision,
};

const ReadCheckDocument = struct {
    schema_version: u32 = 1,
    decision: read_gate.Decision,
};

const WriteCheckDocument = struct {
    schema_version: u32 = 1,
    decision: write_gate.Decision,
};

const OwnerJobCheckDocument = struct {
    schema_version: u32 = 1,
    decision: owner_job_gate.Decision,
};

const FenceDocument = struct {
    schema_version: u32 = 1,
    receipt: fencing.Receipt,
};

const CurrentFenceDocument = struct {
    schema_version: u32 = 1,
    held: bool,
    receipt: ?fencing.Receipt,
};

const PromotionAssessDocument = struct {
    schema_version: u32 = 1,
    assessment: status_mod.PromotionAssessment,
};

const PromotionDocument = struct {
    schema_version: u32 = 1,
    assessment: status_mod.PromotionAssessment,
    promotion: standby_mod.PromotionResult,
    fence_generation: u64,
    fence_token: []const u8,
    forced: bool,
};

const RejoinAssessDocument = struct {
    schema_version: u32 = 1,
    assessment: rejoin.Assessment,
};

fn promotionDocument(result: ha_admin.FencedPromotionResult) PromotionDocument {
    return .{
        .assessment = result.assessment,
        .promotion = result.promotion,
        .fence_generation = result.fence_generation,
        .fence_token = result.fence_token,
        .forced = result.forced,
    };
}

fn commitGateDocument(gate: commit_gate.GateResult) CommitGateDocument {
    return .{
        .target_lsn = gate.target_lsn,
        .action = gate.action,
        .durability = gate.decision,
    };
}

const SlotDocument = struct {
    slot_name: []const u8,
    timeline_id: u64,
    restart_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    safe_read_lsn: u64,
    active: bool,
    reseed_required: bool,
    last_error: ?[]const u8,
    current_lsn: u64,
    dropped: ?bool = null,
};

fn slotActionDocument(action: []const u8, slot: anytype, dropped: ?bool) SlotActionDocument {
    return .{
        .slot_action = action,
        .slot = .{
            .slot_name = slot.slot_name,
            .timeline_id = slot.timeline_id,
            .restart_lsn = slot.restart_lsn,
            .received_lsn = slot.received_lsn,
            .applied_lsn = slot.applied_lsn,
            .safe_read_lsn = slot.safe_read_lsn,
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .last_error = slot.last_error,
            .current_lsn = slot.current_lsn,
            .dropped = dropped,
        },
    };
}

fn requestPath(uri: []const u8) []const u8 {
    const path_with_query = if (std.mem.indexOf(u8, uri, "://")) |scheme_index| blk: {
        const authority_start = scheme_index + 3;
        const path_index = std.mem.indexOfScalarPos(u8, uri, authority_start, '/') orelse return "/";
        break :blk uri[path_index..];
    } else uri;
    const query_index = std.mem.indexOfScalar(u8, path_with_query, '?') orelse return path_with_query;
    return path_with_query[0..query_index];
}

fn requestQuery(uri: []const u8) []const u8 {
    const query_index = std.mem.indexOfScalar(u8, uri, '?') orelse return "";
    const fragment_index = std.mem.indexOfScalarPos(u8, uri, query_index + 1, '#') orelse uri.len;
    return uri[query_index + 1 .. fragment_index];
}

fn knownFixedRoute(path: []const u8) bool {
    return std.mem.eql(u8, path, Routes.health) or
        std.mem.eql(u8, path, Routes.ready) or
        std.mem.eql(u8, path, Routes.command) or
        std.mem.eql(u8, path, admin_api.routes.ha_primary_status) or
        std.mem.eql(u8, path, admin_api.routes.ha_standby_status) or
        std.mem.eql(u8, path, admin_api.routes.ha_commit_check) or
        std.mem.eql(u8, path, admin_api.routes.ha_commit_append) or
        std.mem.eql(u8, path, admin_api.routes.ha_read_check) or
        std.mem.eql(u8, path, admin_api.routes.ha_write_check) or
        std.mem.eql(u8, path, admin_api.routes.ha_owner_job_check) or
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

const GateRole = enum {
    primary,
    standby,
};

const WriteGateRequest = struct {
    role: GateRole,
    request: write_gate.Request,
};

const OwnerJobGateRequest = struct {
    role: GateRole,
    request: owner_job_gate.Request,
};

fn syncPolicyFromOpenApi(policy: admin_api.HASyncPolicy) !primary_mod.SyncPolicy {
    const required = if (policy.required) |value| blk: {
        const parsed = try positiveUint64FromJson(value);
        if (parsed > std.math.maxInt(usize)) return error.InvalidAdminRequest;
        break :blk @as(usize, @intCast(parsed));
    } else 1;
    const standby_names = policy.standby_names orelse &.{};
    for (standby_names) |name| {
        if (name.len == 0) return error.InvalidAdminRequest;
    }

    return .{
        .mode = try parseDurabilityModeQuery(policy.mode),
        .selection = if (policy.selection) |raw| try parseStandbySelectionQuery(raw) else .any,
        .required = required,
        .standby_names = standby_names,
        .failure_policy = if (policy.failure_policy) |raw| try parseFailurePolicyQuery(raw) else .block,
    };
}

fn readRequestFromOpenApi(request: admin_api.ReadCheckRequest) !read_gate.Request {
    return .{
        .consistency = if (request.consistency) |raw| try parseReadConsistency(raw) else .stale_ok,
        .required_lsn = if (request.required_lsn) |value| try uint64FromJson(value) else null,
        .required_metadata_lsn = if (request.required_metadata_lsn) |value| try uint64FromJson(value) else null,
        .metadata_applied_lsn = if (request.metadata_applied_lsn) |value| try uint64FromJson(value) else null,
    };
}

fn writeRequestFromOpenApi(request: admin_api.WriteCheckRequest) !WriteGateRequest {
    return .{
        .role = try parseGateRole(request.role),
        .request = .{
            .expected_identity = if (request.expected_identity) |identity| try adminIdentityFromOpenApi(identity) else null,
        },
    };
}

fn ownerJobRequestFromOpenApi(request: admin_api.OwnerJobCheckRequest) !OwnerJobGateRequest {
    return .{
        .role = try parseGateRole(request.role),
        .request = .{
            .kind = try parseOwnerJobKind(request.kind),
            .expected_identity = if (request.expected_identity) |identity| try adminIdentityFromOpenApi(identity) else null,
        },
    };
}

fn parseReadConsistency(raw: []const u8) !read_gate.Consistency {
    if (std.mem.eql(u8, raw, "stale_ok") or std.mem.eql(u8, raw, "stale-ok")) return .stale_ok;
    if (std.mem.eql(u8, raw, "at_least_lsn") or std.mem.eql(u8, raw, "at-least-lsn")) return .at_least_lsn;
    if (std.mem.eql(u8, raw, "primary")) return .primary;
    return error.InvalidAdminRequest;
}

fn parseGateRole(raw: []const u8) !GateRole {
    if (std.mem.eql(u8, raw, "primary")) return .primary;
    if (std.mem.eql(u8, raw, "standby")) return .standby;
    return error.InvalidAdminRequest;
}

fn parseOwnerJobKind(raw: []const u8) !owner_job_gate.JobKind {
    if (std.mem.eql(u8, raw, "compaction_publish")) return .compaction_publish;
    if (std.mem.eql(u8, raw, "derived_effect_writer")) return .derived_effect_writer;
    if (std.mem.eql(u8, raw, "enrichment_writer")) return .enrichment_writer;
    if (std.mem.eql(u8, raw, "retention_advance")) return .retention_advance;
    return error.InvalidAdminRequest;
}

fn appendOptionsFromOpenApi(request: admin_api.CommitAppendRequest) !primary_mod.AppendOptions {
    return .{
        .kind = if (request.kind) |raw| try parseRecordKind(raw) else .batch_mutation,
        .payload_codec = if (request.payload_codec) |raw| try parsePayloadCodec(raw) else .raw,
        .shard_id = if (request.shard_id) |value| try uint64FromJson(value) else 0,
        .table_id = if (request.table_id) |value| try uint64FromJson(value) else 0,
        .commit_timestamp_ns = request.commit_timestamp_ns orelse 0,
        .payload = request.payload,
    };
}

fn parseRecordKind(raw: []const u8) !replication_record.RecordKind {
    if (std.mem.eql(u8, raw, "batch_mutation")) return .batch_mutation;
    if (std.mem.eql(u8, raw, "metadata_mutation")) return .metadata_mutation;
    if (std.mem.eql(u8, raw, "derived_effect")) return .derived_effect;
    if (std.mem.eql(u8, raw, "backup_start")) return .backup_start;
    if (std.mem.eql(u8, raw, "backup_end")) return .backup_end;
    if (std.mem.eql(u8, raw, "checkpoint")) return .checkpoint;
    if (std.mem.eql(u8, raw, "manifest")) return .manifest;
    if (std.mem.eql(u8, raw, "truncate")) return .truncate;
    if (std.mem.eql(u8, raw, "timeline_switch")) return .timeline_switch;
    return error.InvalidAdminRequest;
}

fn parsePayloadCodec(raw: []const u8) !replication_record.PayloadCodec {
    if (std.mem.eql(u8, raw, "raw")) return .raw;
    if (std.mem.eql(u8, raw, "json")) return .json;
    if (std.mem.eql(u8, raw, "binary")) return .binary;
    return error.InvalidAdminRequest;
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

const QuerySyncPolicy = struct {
    policy: ?primary_mod.SyncPolicy = null,
    owned_standby_names: []const []const u8 = &.{},

    fn deinit(self: *QuerySyncPolicy, alloc: Allocator) void {
        alloc.free(self.owned_standby_names);
        self.* = undefined;
    }
};

fn buildSyncPolicyFromQuery(alloc: Allocator, query: []const u8) !QuerySyncPolicy {
    var mode: ?primary_mod.DurabilityMode = null;
    var selection: primary_mod.StandbySelection = .any;
    var required: usize = 1;
    var failure_policy: primary_mod.FailurePolicy = .block;
    var names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer names.deinit(alloc);

    if (queryValue(query, "sync_mode")) |raw| mode = try parseDurabilityModeQuery(raw);
    if (queryValue(query, "sync_selection")) |raw| selection = try parseStandbySelectionQuery(raw);
    if (queryValue(query, "sync_required")) |raw| {
        const parsed = try uint64Text(raw);
        if (parsed == 0 or parsed > std.math.maxInt(usize)) return error.InvalidAdminRequest;
        required = @intCast(parsed);
    }
    if (queryValue(query, "sync_failure")) |raw| failure_policy = try parseFailurePolicyQuery(raw);

    var iter = std.mem.splitScalar(u8, query, '&');
    while (iter.next()) |part| {
        const key, const value = splitQueryPart(part);
        if (std.mem.eql(u8, key, "sync_standby")) {
            if (value.len == 0) return error.InvalidAdminRequest;
            try names.append(alloc, value);
        }
    }

    const configured = mode != null or
        selection != .any or
        required != 1 or
        failure_policy != .block or
        names.items.len > 0;
    if (!configured) return .{};

    const owned = try names.toOwnedSlice(alloc);
    names = .empty;
    return .{
        .policy = .{
            .mode = mode orelse .remote_write,
            .selection = selection,
            .required = required,
            .standby_names = owned,
            .failure_policy = failure_policy,
        },
        .owned_standby_names = owned,
    };
}

fn queryValue(query: []const u8, key: []const u8) ?[]const u8 {
    var iter = std.mem.splitScalar(u8, query, '&');
    while (iter.next()) |part| {
        const part_key, const part_value = splitQueryPart(part);
        if (std.mem.eql(u8, part_key, key)) return part_value;
    }
    return null;
}

fn splitQueryPart(part: []const u8) struct { []const u8, []const u8 } {
    if (std.mem.indexOfScalar(u8, part, '=')) |idx| return .{ part[0..idx], part[idx + 1 ..] };
    return .{ part, "" };
}

fn uint64Text(raw: []const u8) !u64 {
    if (raw.len == 0) return error.InvalidAdminRequest;
    return std.fmt.parseUnsigned(u64, raw, 10) catch error.InvalidAdminRequest;
}

fn parseDurabilityModeQuery(raw: []const u8) !primary_mod.DurabilityMode {
    if (std.mem.eql(u8, raw, "async")) return .async;
    if (std.mem.eql(u8, raw, "remote_write") or std.mem.eql(u8, raw, "remote-write")) return .remote_write;
    if (std.mem.eql(u8, raw, "remote_apply") or std.mem.eql(u8, raw, "remote-apply")) return .remote_apply;
    return error.InvalidAdminRequest;
}

fn parseStandbySelectionQuery(raw: []const u8) !primary_mod.StandbySelection {
    if (std.mem.eql(u8, raw, "any")) return .any;
    if (std.mem.eql(u8, raw, "first")) return .first;
    if (std.mem.eql(u8, raw, "all")) return .all;
    return error.InvalidAdminRequest;
}

fn parseFailurePolicyQuery(raw: []const u8) !primary_mod.FailurePolicy {
    if (std.mem.eql(u8, raw, "block")) return .block;
    if (std.mem.eql(u8, raw, "fail_closed") or std.mem.eql(u8, raw, "fail-closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "degrade_to_async") or std.mem.eql(u8, raw, "degrade-to-async")) return .degrade_to_async;
    return error.InvalidAdminRequest;
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
        error.RequiredLsnMissing,
        error.AppliedAheadOfReceived,
        error.SafeReadAheadOfApplied,
        error.MetadataAheadOfApplied,
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
    try expectContains(typed_status.body, "\"schema_version\":1");
    try expectContains(typed_status.body, "\"snapshot\"");
    try expectContains(typed_status.body, "\"role\":\"primary\"");
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
    try expectContains(typed_create.body, "\"slot_action\":\"create\"");
    try expectContains(typed_create.body, "\"slot_name\":\"standby-b\"");

    var typed_slots = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_replication_slots,
    });
    defer typed_slots.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_slots.status);
    try std.testing.expectEqualStrings("application/json", typed_slots.content_type.?);
    try expectContains(typed_slots.body, "\"snapshot\"");
    try expectContains(typed_slots.body, "\"slots\"");
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
    try expectContains(typed_pause.body, "\"slot_action\":\"pause\"");
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
    try expectContains(typed_resume.body, "\"slot_action\":\"resume\"");
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
    try expectContains(typed_drop.body, "\"slot_action\":\"drop\"");
    try expectContains(typed_drop.body, "\"slot_name\":\"standby-b\"");
    try expectContains(typed_drop.body, "\"dropped\":true");

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
    try expectContains(typed_standby_status.body, "\"snapshot\"");
    try expectContains(typed_standby_status.body, "\"role\":\"standby\"");
    try expectContains(typed_standby_status.body, "\"received_lsn\":1");
    try expectContains(typed_standby_status.body, "\"applied_lsn\":1");

    const typed_primary_policy_uri = try std.fmt.allocPrint(
        alloc,
        "{s}?max_lag_lsn=1&sync_mode=remote-apply&sync_standby=standby-a&sync_failure=fail-closed",
        .{admin_api.routes.ha_primary_status},
    );
    defer alloc.free(typed_primary_policy_uri);
    var typed_primary_policy_status = try server.handle(.{
        .method = .GET,
        .uri = typed_primary_policy_uri,
    });
    defer typed_primary_policy_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_primary_policy_status.status);
    try expectContains(typed_primary_policy_status.body, "\"snapshot\"");
    try expectContains(typed_primary_policy_status.body, "\"durability\"");
    try expectContains(typed_primary_policy_status.body, "\"mode\":\"remote_apply\"");
    try expectContains(typed_primary_policy_status.body, "\"status\":\"satisfied\"");

    const typed_standby_upstream_uri = try std.fmt.allocPrint(
        alloc,
        "{s}?upstream_lsn=2",
        .{admin_api.routes.ha_standby_status},
    );
    defer alloc.free(typed_standby_upstream_uri);
    var typed_standby_upstream_status = try server.handle(.{
        .method = .GET,
        .uri = typed_standby_upstream_uri,
    });
    defer typed_standby_upstream_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_standby_upstream_status.status);
    try expectContains(typed_standby_upstream_status.body, "\"snapshot\"");
    try expectContains(typed_standby_upstream_status.body, "\"upstream_lsn\":2");
    try expectContains(typed_standby_upstream_status.body, "\"write_lag_lsn\":1");

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
    try expectContains(typed_fence.body, "\"schema_version\":1");
    try expectContains(typed_fence.body, "\"receipt\"");
    try expectContains(typed_fence.body, "\"promoted_node_id\":\"standby-a\"");

    var typed_current_fence = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_fence_current,
    });
    defer typed_current_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_current_fence.status);
    try std.testing.expectEqualStrings("application/json", typed_current_fence.content_type.?);
    try expectContains(typed_current_fence.body, "\"held\":true");
    try expectContains(typed_current_fence.body, "\"receipt\"");
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
    try expectContains(typed_rejoin_unfenced.body, "\"schema_version\":1");
    try expectContains(typed_rejoin_unfenced.body, "\"assessment\"");
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
    try expectContains(typed_rejoin_fenced.body, "\"assessment\"");
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
    try expectContains(typed_promote_assess.body, "\"schema_version\":1");
    try expectContains(typed_promote_assess.body, "\"assessment\"");
    try expectContains(typed_promote_assess.body, "\"can_promote\":true");

    var typed_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_current_fence,
        .content_type = "application/json",
    });
    defer typed_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote.status);
    try std.testing.expectEqualStrings("application/json", typed_promote.content_type.?);
    try expectContains(typed_promote.body, "\"promotion\"");
    try expectContains(typed_promote.body, "\"fence_generation\":1");
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
    try expectContains(typed_begin.body, "\"schema_version\":1");
    try expectContains(typed_begin.body, "\"backup_lsn\":1");
    try expectContains(typed_begin.body, "\"start_record_lsn\":1");
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
    try expectContains(typed_finish.body, "\"schema_version\":1");
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
    try expectContains(typed_bootstrap.body, "\"schema_version\":1");
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
