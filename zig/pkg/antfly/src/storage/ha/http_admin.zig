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

//! HTTP adapter for HA admin operations.
//!
//! Typed `/admin/v1/ha` routes are the operator-facing control-plane contract.
//! The legacy command endpoint remains only for replication compatibility
//! commands that do not yet have a typed admin route.

const std = @import("std");
const Allocator = std.mem.Allocator;
const platform_sync = @import("antfly_platform").sync;
const admin_api = @import("../../admin/mod.zig");
const http_common = @import("../../common/http/http_common.zig");
const ha_admin = @import("admin.zig");
const admin_cli = @import("admin_cli.zig");
const admin_exec = @import("admin_exec.zig");
const backup_manifest = @import("backup_manifest.zig");
const commit_gate = @import("commit_gate.zig");
const fencing = @import("fencing.zig");
const lifecycle_receipt_ledger = @import("lifecycle_receipt_ledger.zig");
const owner_job_gate = @import("owner_job_gate.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const rejoin = @import("rejoin.zig");
const seed_artifact = @import("seed_artifact.zig");
const seed_capture = @import("seed_capture.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const status_mod = @import("status.zig");
const validation = @import("validation.zig");
const write_gate = @import("write_gate.zig");
const wal_mod = @import("../wal.zig");

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
    auth: AuthOptions = .{},

    pub const StandbyStatusExtras = struct {
        ptr: *anyopaque,
        last_error: *const fn (ptr: *anyopaque) ?[]const u8,
        last_attempt_ns: *const fn (ptr: *anyopaque) ?u64,
        last_success_ns: *const fn (ptr: *anyopaque) ?u64,
        replication_failures_total: *const fn (ptr: *anyopaque) ?u64,

        pub fn lastError(self: StandbyStatusExtras) ?[]const u8 {
            return self.last_error(self.ptr);
        }

        pub fn lastAttemptNs(self: StandbyStatusExtras) ?u64 {
            return self.last_attempt_ns(self.ptr);
        }

        pub fn lastSuccessNs(self: StandbyStatusExtras) ?u64 {
            return self.last_success_ns(self.ptr);
        }

        pub fn replicationFailuresTotal(self: StandbyStatusExtras) ?u64 {
            return self.replication_failures_total(self.ptr);
        }
    };

    pub const StateChangedHook = struct {
        ptr: *anyopaque,
        run_fn: *const fn (ptr: *anyopaque) void,

        pub fn run(self: StateChangedHook) void {
            self.run_fn(self.ptr);
        }
    };

    /// Runtime-owned capture deliberately bypasses the generic state-mutex
    /// wrapper below. Its callback acquires the mutation barrier first and the
    /// HA state mutex second, preserving the process-wide lock order.
    pub const SeedCaptureResult = struct {
        capture: seed_capture.CaptureResult,
        node_id: []u8,

        pub fn deinit(self: *SeedCaptureResult, alloc: Allocator) void {
            self.capture.deinit(alloc);
            alloc.free(self.node_id);
            self.* = undefined;
        }
    };

    pub const SeedCaptureHook = struct {
        ptr: *anyopaque,
        run_fn: *const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            slot_name: []const u8,
            generation: []const u8,
            binding: seed_artifact.LifecycleBinding,
        ) anyerror!SeedCaptureResult,

        pub fn run(
            self: SeedCaptureHook,
            alloc: Allocator,
            slot_name: []const u8,
            generation: []const u8,
            binding: seed_artifact.LifecycleBinding,
        ) !SeedCaptureResult {
            return self.run_fn(self.ptr, alloc, slot_name, generation, binding);
        }
    };

    pub const AuthOptions = struct {
        pub const LifecycleReceipts = struct {
            capture_root: ?[]const u8 = null,
            activation_root: ?[]const u8 = null,
            wal_options: wal_mod.WalOptions = .{},
            node_id: ?[]const u8 = null,
            role: lifecycle_receipt_ledger.RuntimeRole = .unknown,
            pod_uid: ?[]const u8 = null,
        };

        bearer_token: ?[]const u8 = null,
        standby_status_extras: ?StandbyStatusExtras = null,
        state_mutex: ?*std.atomic.Mutex = null,
        seed_capture: ?SeedCaptureHook = null,
        lifecycle_receipts: ?LifecycleReceipts = null,
        primary_fence_started: ?StateChangedHook = null,
        state_changed: ?StateChangedHook = null,
    };

    pub fn init(alloc: Allocator, ctx: admin_exec.Context) Server {
        return initWithOptions(alloc, ctx, .{});
    }

    pub fn initWithOptions(alloc: Allocator, ctx: admin_exec.Context, auth: AuthOptions) Server {
        return .{
            .alloc = alloc,
            .ctx = ctx,
            .auth = auth,
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
        if (isAdminAuthRequired(path) and !self.authorized(req)) {
            return try textResponse(self.alloc, 401, "unauthorized");
        }
        if (req.method == .GET and std.mem.eql(u8, path, Routes.health)) {
            return try textResponse(self.alloc, 200, "ok");
        }
        if (req.method == .POST and std.mem.eql(u8, path, admin_api.routes.ha_base_backups_capture)) {
            defer if (self.auth.state_changed) |hook| hook.run();
            return try self.handleAdminCaptureSeedArtifact(req);
        }
        if (std.mem.eql(u8, path, admin_api.routes.ha_seed_lifecycle_receipts)) {
            if (req.method != .GET) return try textResponse(self.alloc, 405, "method not allowed");
            return try self.handleAdminLifecycleReceipts(req);
        }
        if (self.auth.state_mutex) |mutex| {
            platform_sync.lockYielding(mutex);
            defer mutex.unlock();
        }
        defer if (self.auth.state_changed) |hook| hook.run();
        switch (req.method) {
            .GET => {
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
                if (knownRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
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
                if (std.mem.eql(u8, path, admin_api.routes.ha_base_backups_activate)) {
                    return try self.handleAdminActivateSeededSlot(req);
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
                if (std.mem.eql(u8, path, admin_api.routes.ha_rejoin_rewind)) {
                    return try self.handleAdminRewindRejoin(req);
                }
                if (std.mem.eql(u8, path, admin_api.routes.ha_rejoin_reseed)) {
                    return try self.handleAdminReseedRejoin(req);
                }
                if (knownRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .PUT => {
                if (self.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_pause_suffix) catch return try textResponse(self.alloc, 400, "invalid HA replication slot path")) |slot_name| {
                    defer self.alloc.free(slot_name);
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .pause);
                }
                if (self.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_resume_suffix) catch return try textResponse(self.alloc, 400, "invalid HA replication slot path")) |slot_name| {
                    defer self.alloc.free(slot_name);
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .@"resume");
                }
                if (knownRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .DELETE => {
                if (self.replicationSlotNameFromPath(path, "") catch return try textResponse(self.alloc, 400, "invalid HA replication slot path")) |slot_name| {
                    defer self.alloc.free(slot_name);
                    return try self.handleAdminReplicationSlotLifecycle(slot_name, .drop);
                }
                if (knownRoute(path)) {
                    return try textResponse(self.alloc, 405, "method not allowed");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
        }
    }

    fn authorized(self: *const Server, req: http_common.HttpRequest) bool {
        const raw_token = self.auth.bearer_token orelse return true;
        const token = std.mem.trim(u8, raw_token, " \t\r\n");
        if (token.len == 0) return false;
        const authorization = req.authorization orelse req.header("authorization") orelse return false;
        return bearerAuthorizationMatches(token, authorization);
    }

    fn replicationSlotNameFromPath(self: *Server, path: []const u8, suffix: []const u8) !?[]u8 {
        return try admin_api.routes.replicationSlotNameFromPathAlloc(self.alloc, path, suffix);
    }

    fn handleAdminPrimaryStatus(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        const query = requestQuery(req.uri);
        const max_lag_lsn = if (queryValue(query, "max_lag_lsn")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA primary status request")
        else
            0;
        const max_retained_bytes = if (queryValue(query, "max_retained_bytes")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA primary status request")
        else
            0;
        const max_retained_age_ns = if (queryValue(query, "max_retained_age_ns")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA primary status request")
        else
            0;
        var sync = buildSyncPolicyFromQuery(self.alloc, query) catch
            return try textResponse(self.alloc, 400, "invalid HA primary status request");
        errdefer sync.deinit(self.alloc);
        defer sync.deinit(self.alloc);

        var snapshot = ha_admin.primaryStatus(self.alloc, primary, .{
            .max_lag_lsn = max_lag_lsn,
            .max_retained_bytes = max_retained_bytes,
            .max_retained_age_ns = max_retained_age_ns,
        }, sync.policy) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer snapshot.deinit(self.alloc);
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
        const response = admin_api.HAPrimaryStatusResponse{
            .schema_version = 1,
            .snapshot = try adminPrimarySnapshot(self.alloc, snapshot, node_id),
        };
        defer self.alloc.free(response.snapshot.slots);
        return try self.handleTypedJson(response);
    }

    fn handleAdminStandbyStatus(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const node_id = self.standbyNodeID() orelse return try textResponse(self.alloc, 409, "StandbyNodeIDUnavailable");
        const query = requestQuery(req.uri);
        const upstream_lsn = if (queryValue(query, "upstream_lsn")) |raw|
            uint64Text(raw) catch return try textResponse(self.alloc, 400, "invalid HA standby status request")
        else
            null;

        var snapshot = ha_admin.standbyStatus(standby, upstream_lsn);
        if (self.auth.standby_status_extras) |extras| {
            snapshot.last_error = extras.lastError();
            snapshot.last_attempt_ns = extras.lastAttemptNs();
            snapshot.last_success_ns = extras.lastSuccessNs();
            snapshot.replication_failures_total = extras.replicationFailuresTotal();
        }
        return try self.handleTypedJson(admin_api.HAStandbyStatusResponse{
            .schema_version = 1,
            .snapshot = try adminStandbySnapshot(snapshot, node_id),
        });
    }

    fn handleAdminReplicationSlots(self: *Server) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        var snapshot = ha_admin.primaryStatus(self.alloc, primary, .{}, null) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer snapshot.deinit(self.alloc);
        const slots = try slotListDocuments(self.alloc, snapshot);
        defer self.alloc.free(slots);
        return try self.handleTypedJson(admin_api.HAReplicationSlotListResponse{
            .schema_version = 1,
            .slots = slots,
        });
    }

    fn handleAdminCreateReplicationSlot(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA replication slot request");

        var parsed = admin_api.server.parseCreateHAReplicationSlotBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA replication slot request");
        defer parsed.deinit();

        const initial_lsn: ?u64 = if (parsed.value.initial_lsn) |value| blk: {
            if (value < 0) return try textResponse(self.alloc, 400, "invalid HA replication slot request");
            break :blk @intCast(value);
        } else null;
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");

        const result = ha_admin.applySlotAction(primary, .create, .{
            .slot_name = parsed.value.slot_name,
            .initial_lsn = initial_lsn,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return switch (result) {
            .create => |slot| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "replication_slot_create:{s}", .{parsed.value.slot_name});
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(try slotActionDocument(action_id, "replication_slot_create", parsed.value.slot_name, node_id, "create", slot, null));
            },
            else => unreachable,
        };
    }

    fn handleAdminReplicationSlotLifecycle(
        self: *Server,
        slot_name: []const u8,
        action: ha_admin.SlotAction,
    ) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
        const result = ha_admin.applySlotAction(primary, action, .{
            .slot_name = slot_name,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        const action_kind = switch (action) {
            .create => "replication_slot_create",
            .pause => "replication_slot_pause",
            .@"resume" => "replication_slot_resume",
            .drop => "replication_slot_drop",
        };
        return switch (result) {
            .create => unreachable,
            .pause => |slot| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "{s}:{s}", .{ action_kind, slot_name });
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(try slotActionDocument(action_id, action_kind, slot_name, node_id, "pause", slot, slot.dropped));
            },
            .@"resume" => |slot| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "{s}:{s}", .{ action_kind, slot_name });
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(try slotActionDocument(action_id, action_kind, slot_name, node_id, "resume", slot, slot.dropped));
            },
            .drop => |slot| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "{s}:{s}", .{ action_kind, slot_name });
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(try slotActionDocument(action_id, action_kind, slot_name, node_id, "drop", slot, slot.dropped));
            },
        };
    }

    fn handleAdminCommitCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA commit check request");
        var parsed = admin_api.server.parseCheckHACommitBody(
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
        return try self.handleTypedJson(admin_api.HACommitCheckResponse{
            .schema_version = 1,
            .gate = try adminCommitGate(gate),
        });
    }

    fn handleAdminCommitAppend(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA commit append request");
        var parsed = admin_api.server.parseAppendHACommitBody(
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
        return try self.handleTypedJson(admin_api.HACommitAppendResponse{
            .schema_version = 1,
            .lsn = try adminI64(result.lsn),
            .gate = try adminCommitGate(result.gate),
        });
    }

    fn handleAdminReadCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const request = if (req.body.len == 0)
            read_gate.Request{}
        else blk: {
            var parsed = admin_api.server.parseCheckHAReadBody(
                self.alloc,
                req.body,
            ) catch return try textResponse(self.alloc, 400, "invalid HA read check request");
            defer parsed.deinit();
            break :blk readRequestFromOpenApi(parsed.value) catch {
                return try textResponse(self.alloc, 400, "invalid HA read check request");
            };
        };
        const effective_request = admin_exec.readRequestWithContext(self.ctx, request) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        const decision = ha_admin.evaluateStandbyRead(standby, effective_request) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        return try self.handleTypedJson(admin_api.HAReadCheckResponse{
            .schema_version = 1,
            .decision = try adminReadDecision(decision),
        });
    }

    fn handleAdminWriteCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA write check request");
        var parsed = admin_api.server.parseCheckHAWriteBody(
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
                const decision = if (self.ctx.fence_store) |fence_store|
                    if (self.primaryNodeID()) |node_id|
                        write_gate.evaluateFencedPrimary(.{
                            .primary = primary,
                            .fence_store = fence_store,
                            .node_id = node_id,
                        }, request.request)
                    else
                        return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable")
                else
                    ha_admin.evaluatePrimaryWrite(primary, request.request);
                break :blk decision catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
            .standby => blk: {
                const evaluated = if (self.ctx.standby) |standby|
                    ha_admin.evaluateStandbyWrite(standby, request.request)
                else if (self.ctx.primary) |primary|
                    if (self.ctx.promoted_standby_handoff) |handoff|
                        ha_admin.evaluatePromotedPrimaryWrite(primary, handoff, request.request)
                    else
                        return try textResponse(self.alloc, 409, "StandbyUnavailable")
                else
                    return try textResponse(self.alloc, 409, "StandbyUnavailable");
                break :blk evaluated catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
        };
        return try self.handleTypedJson(admin_api.HAWriteCheckResponse{
            .schema_version = 1,
            .decision = try adminWriteDecision(decision),
        });
    }

    fn handleAdminOwnerJobCheck(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA owner job check request");
        var parsed = admin_api.server.parseCheckHAOwnerJobBody(
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
                const evaluated = if (self.ctx.standby) |standby|
                    ha_admin.evaluateStandbyOwnerJob(standby, request.request)
                else if (self.ctx.primary) |primary|
                    if (self.ctx.promoted_standby_handoff) |handoff|
                        ha_admin.evaluatePromotedPrimaryOwnerJob(primary, handoff, request.request)
                    else
                        return try textResponse(self.alloc, 409, "StandbyUnavailable")
                else
                    return try textResponse(self.alloc, 409, "StandbyUnavailable");
                break :blk evaluated catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            },
        };
        return try self.handleTypedJson(admin_api.HAOwnerJobCheckResponse{
            .schema_version = 1,
            .decision = try adminOwnerJobDecision(decision),
        });
    }

    fn handleAdminBeginBaseBackup(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA base backup request");
        var parsed = admin_api.server.parseBeginHABaseBackupBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA base backup request");
        defer parsed.deinit();
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");

        const result = ha_admin.beginBaseBackup(primary, .{
            .slot_name = parsed.value.slot_name,
            .manifest_id = parsed.value.manifest_id,
        }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        const action_id = try std.fmt.allocPrint(self.alloc, "base_backup_begin:{s}", .{result.manifest_id});
        defer self.alloc.free(action_id);
        return try self.handleTypedJson(admin_api.HABaseBackupBeginResponse{
            .schema_version = 1,
            .action = .{
                .action_id = action_id,
                .action_kind = "base_backup_begin",
                .target = result.manifest_id,
                .state = "applied",
                .node_id = node_id,
            },
            .slot_name = result.slot_name,
            .manifest_id = result.manifest_id,
            .backup_lsn = try adminI64(result.backup_lsn),
            .start_record_lsn = try adminI64(result.start_record_lsn),
        });
    }

    fn handleAdminFinishBaseBackup(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA base backup finish request");
        var parsed = admin_api.server.parseFinishHABaseBackupBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA base backup finish request");
        defer parsed.deinit();
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
        const manifest_path = validateAdminHAPath(parsed.value.manifest_path, .manifest) catch |err| {
            return try textResponse(self.alloc, 400, @errorName(err));
        };

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .seed = .{ .finish = .{
                .manifest_path = manifest_path,
            } } },
        };
        defer plan.deinit(self.alloc);
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return switch (result) {
            .seed_finish => |seed| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "base_backup_finish:{s}", .{seed.manifest_id});
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(admin_api.HABaseBackupFinishResponse{
                    .schema_version = 1,
                    .action = .{
                        .action_id = action_id,
                        .action_kind = "base_backup_finish",
                        .target = seed.manifest_id,
                        .state = "applied",
                        .node_id = node_id,
                    },
                    .manifest_id = seed.manifest_id,
                    .backup_lsn = try adminI64(seed.backup_lsn),
                    .end_record_lsn = try adminI64(seed.end_record_lsn),
                });
            },
            else => unreachable,
        };
    }

    fn handleAdminCaptureSeedArtifact(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const hook = self.auth.seed_capture orelse return try textResponse(self.alloc, 409, "SeedCaptureUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA seed capture request");
        var parsed = admin_api.server.parseCaptureHASeedArtifactBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA seed capture request");
        defer parsed.deinit();
        if (!validation.isIdentifier(parsed.value.slot_name) or
            !validation.isIdentifier(parsed.value.generation) or
            !validation.isIdentifier(parsed.value.topology_id) or
            !validation.isIdentifier(parsed.value.node_id) or
            !validation.isIdentifier(parsed.value.target_pvc_name) or
            !validation.isIdentifier(parsed.value.target_pvc_uid))
        {
            return try textResponse(self.alloc, 400, "invalid HA seed capture identity");
        }
        const topology_generation = positiveUint64FromJson(parsed.value.topology_generation) catch
            return try textResponse(self.alloc, 400, "invalid HA seed capture topology generation");
        const binding = seed_artifact.LifecycleBinding{
            .topology_id = parsed.value.topology_id,
            .topology_generation = topology_generation,
            .node_id = parsed.value.node_id,
            .target_pvc_name = parsed.value.target_pvc_name,
            .target_pvc_uid = parsed.value.target_pvc_uid,
        };

        var result = hook.run(
            self.alloc,
            parsed.value.slot_name,
            parsed.value.generation,
            binding,
        ) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        var receipt = std.json.parseFromSlice(
            seed_capture.CaptureReceipt,
            self.alloc,
            result.capture.receipt_json,
            .{ .ignore_unknown_fields = true },
        ) catch return try textResponse(self.alloc, 500, "InvalidSeedCaptureReceipt");
        defer receipt.deinit();
        const value = receipt.value;
        if (!std.mem.eql(u8, value.slot_name, parsed.value.slot_name) or
            !std.mem.eql(u8, value.generation, parsed.value.generation) or
            !std.mem.eql(u8, value.topology_id, binding.topology_id) or
            value.topology_generation != binding.topology_generation or
            !std.mem.eql(u8, value.node_id, binding.node_id) or
            !std.mem.eql(u8, value.target_pvc_name, binding.target_pvc_name) or
            !std.mem.eql(u8, value.target_pvc_uid, binding.target_pvc_uid) or
            !validSha256Hex(value.source_plan_sha256) or
            !validSha256Hex(value.manifest_sha256))
        {
            return try textResponse(self.alloc, 500, "InvalidSeedCaptureReceipt");
        }

        const action_id = try std.fmt.allocPrint(self.alloc, "seed_capture:{s}", .{value.generation});
        defer self.alloc.free(action_id);
        return try self.handleTypedJson(admin_api.HASeedArtifactCaptureResponse{
            .schema_version = 1,
            .action = .{
                .action_id = action_id,
                .action_kind = "seed_capture",
                .target = value.generation,
                .state = if (result.capture.already_captured) "already_applied" else "applied",
                .node_id = result.node_id,
            },
            .slot_name = value.slot_name,
            .generation = value.generation,
            .topology_id = value.topology_id,
            .topology_generation = try adminI64(value.topology_generation),
            .node_id = value.node_id,
            .target_pvc_name = value.target_pvc_name,
            .target_pvc_uid = value.target_pvc_uid,
            .cluster_id = try adminI64(value.cluster_id),
            .shard_id = try adminI64(value.shard_id),
            .table_id = try adminI64(value.table_id),
            .timeline_id = try adminI64(value.timeline_id),
            .epoch = try adminI64(value.epoch),
            .manifest_id = value.manifest_id,
            .source_plan_sha256 = value.source_plan_sha256,
            .backup_lsn = try adminI64(value.backup_lsn),
            .checkpoint_lsn = try adminI64(value.checkpoint_lsn),
            .end_record_lsn = try adminI64(value.end_record_lsn),
            .manifest_sha256 = value.manifest_sha256,
            .file_count = try adminI64(@intCast(value.file_count)),
            .total_bytes = try adminI64(value.total_bytes),
            .generation_root = result.capture.generation_root,
            .content_root = result.capture.content_root,
            .manifest_path = result.capture.manifest_path,
            .already_captured = result.capture.already_captured,
        });
    }

    fn handleAdminLifecycleReceipts(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const config = self.auth.lifecycle_receipts orelse
            return try textResponse(self.alloc, 409, "LifecycleReceiptLedgerUnavailable");
        const query = parseLifecycleReceiptQuery(requestQuery(req.uri)) catch
            return try textResponse(self.alloc, 400, "invalid lifecycle receipt query");
        const authoritative_root = switch (query.kind) {
            .capture => config.capture_root,
            .activation => config.activation_root,
        } orelse return try textResponse(self.alloc, 409, "LifecycleReceiptLedgerUnavailable");

        var wal_options = config.wal_options;
        wal_options.read_only = true;
        var ledger = lifecycle_receipt_ledger.Ledger.open(self.alloc, authoritative_root, .{
            .wal_options = wal_options,
        }) catch |err| return try textResponse(self.alloc, 500, @errorName(err));
        defer ledger.close();

        const fenced = if (config.node_id) |node_id| blk: {
            const store = self.ctx.fence_store orelse break :blk false;
            const receipt = store.currentBorrowed() orelse break :blk false;
            break :blk std.mem.eql(u8, receipt.old_primary_id, node_id);
        } else false;
        var page = ledger.readPage(self.alloc, query.kind, .{
            .after = query.after,
            .limit = query.limit,
        }, .{
            .authoritative_root = authoritative_root,
            .runtime = .{
                .node_id = config.node_id,
                .role = config.role,
                .pod_uid = config.pod_uid,
                .fenced = fenced,
                .observed_at_unix_ns = @import("../../platform/time.zig").realtimeNs(),
            },
        }) catch |err| return try textResponse(self.alloc, 500, @errorName(err));
        defer page.deinit(self.alloc);

        const entries = try self.alloc.alloc(admin_api.HASeedLifecycleReceiptEvent, page.entries.len);
        defer self.alloc.free(entries);
        for (page.entries, 0..) |entry, index| {
            entries[index] = .{
                .cursor = try adminI64(entry.cursor),
                .kind = @tagName(entry.kind),
                .generation = entry.generation,
                .slot_name = entry.slot_name,
                .topology_id = entry.topology_id,
                .topology_generation = try adminI64(entry.topology_generation),
                .node_id = entry.node_id,
                .target_pvc_name = entry.target_pvc_name,
                .target_pvc_uid = entry.target_pvc_uid,
                .receipt_sha256 = entry.receipt_sha256,
                .receipt_json = entry.receipt_json,
                .recorded_at_unix_ns = try adminI64(entry.recorded_at_unix_ns),
                .pod_uid = entry.pod_uid,
                .authoritative_state = @tagName(entry.authoritative_state),
            };
        }
        return try self.handleTypedJson(admin_api.HASeedLifecycleReceiptInventoryResponse{
            .schema_version = 1,
            .entries = entries,
            .first_cursor = try adminI64(page.first_cursor),
            .end_cursor = try adminI64(page.end_cursor),
            .next_cursor = try adminI64(page.next_cursor),
            .history_truncated = page.history_truncated,
            .gap = page.gap,
            .has_more = page.has_more,
            .runtime = .{
                .node_id = page.runtime.node_id,
                .role = @tagName(page.runtime.role),
                .pod_uid = page.runtime.pod_uid,
                .fenced = page.runtime.fenced,
                .observed_at_unix_ns = try adminI64(page.runtime.observed_at_unix_ns),
            },
        });
    }

    fn handleAdminActivateSeededSlot(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const primary = self.ctx.primary orelse return try textResponse(self.alloc, 409, "PrimaryUnavailable");
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA seeded slot activation request");
        var parsed = admin_api.server.parseActivateHASeededSlotBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA seeded slot activation request");
        defer parsed.deinit();
        const request = parsed.value;
        if (!validation.isIdentifier(request.slot_name) or
            !validation.isIdentifier(request.generation) or
            request.manifest_id.len == 0 or
            !validSha256Hex(request.seed_receipt_sha256) or
            !validSha256Hex(request.manifest_sha256) or
            !validSha256Hex(request.aggregate_sha256))
        {
            return try textResponse(self.alloc, 400, "invalid HA seeded slot activation evidence");
        }
        const timeline_id = positiveUint64FromJson(request.timeline_id) catch
            return try textResponse(self.alloc, 400, "invalid HA seeded slot activation timeline");
        const checkpoint_lsn = positiveUint64FromJson(request.checkpoint_lsn) catch
            return try textResponse(self.alloc, 400, "invalid HA seeded slot activation checkpoint");
        const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
        const already_applied = if (primary.slot(request.slot_name)) |slot|
            slot.lifecycle == .streaming and slot.active and !slot.reseed_required and
                slot.timeline_id == timeline_id and slot.received_lsn >= checkpoint_lsn and
                slot.applied_lsn >= checkpoint_lsn and slot.safe_read_lsn >= checkpoint_lsn
        else
            false;

        ha_admin.activateSeededSlot(primary, request.slot_name, timeline_id, checkpoint_lsn) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        const action_id = try std.fmt.allocPrint(self.alloc, "seeded_slot_activate:{s}", .{request.generation});
        defer self.alloc.free(action_id);
        return try self.handleTypedJson(admin_api.HASeededSlotActivateResponse{
            .schema_version = 1,
            .action = .{
                .action_id = action_id,
                .action_kind = "seeded_slot_activate",
                .target = request.generation,
                .state = if (already_applied) "already_applied" else "applied",
                .node_id = node_id,
            },
            .slot_name = request.slot_name,
            .generation = request.generation,
            .manifest_id = request.manifest_id,
            .timeline_id = request.timeline_id,
            .checkpoint_lsn = request.checkpoint_lsn,
            .seed_receipt_sha256 = request.seed_receipt_sha256,
            .manifest_sha256 = request.manifest_sha256,
            .aggregate_sha256 = request.aggregate_sha256,
        });
    }

    fn handleAdminBootstrapStandby(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA standby bootstrap request");
        var parsed = admin_api.server.parseBootstrapHAStandbyBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA standby bootstrap request");
        defer parsed.deinit();
        const node_id = self.standbyNodeID() orelse return try textResponse(self.alloc, 409, "StandbyNodeIDUnavailable");
        const manifest_path = validateAdminHAPath(parsed.value.manifest_path, .manifest) catch |err| {
            return try textResponse(self.alloc, 400, @errorName(err));
        };
        const content_root = if (parsed.value.content_root) |root|
            validateAdminHAPath(root, .content_root) catch |err| {
                return try textResponse(self.alloc, 400, @errorName(err));
            }
        else
            null;

        var plan = admin_cli.Plan{
            .output = .json,
            .command = .{ .seed = .{ .bootstrap = .{
                .manifest_path = manifest_path,
                .content_root = content_root,
            } } },
        };
        defer plan.deinit(self.alloc);
        var result = admin_exec.execute(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        return switch (result) {
            .seed_bootstrap => |seed| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "standby_bootstrap:{s}", .{seed.manifest_id});
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(admin_api.HAStandbyBootstrapResponse{
                    .schema_version = 1,
                    .action = .{
                        .action_id = action_id,
                        .action_kind = "standby_bootstrap",
                        .target = seed.manifest_id,
                        .state = "applied",
                        .node_id = node_id,
                    },
                    .manifest_id = seed.manifest_id,
                    .backup_lsn = try adminI64(seed.backup_lsn),
                    .checkpoint_lsn = try adminI64(seed.checkpoint_lsn),
                });
            },
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
            return try self.handleTypedJson(admin_api.HACurrentFenceResponse{
                .schema_version = 1,
                .held = true,
                .receipt = try adminFenceReceipt(result.receipt),
            });
        }
        return try self.handleTypedJson(admin_api.HACurrentFenceResponse{
            .schema_version = 1,
            .held = false,
            .receipt = null,
        });
    }

    fn handleAdminAcquireFence(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        var fence = self.parseAcquireFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA fence request");
        };
        if (self.ctx.primary) |primary| {
            const primary_node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
            if (std.mem.eql(u8, primary_node_id, fence.old_primary_id) and
                primary.identity.cluster_id == fence.identity.cluster_id and
                primary.identity.shard_id == fence.identity.shard_id and
                primary.identity.table_id == fence.identity.table_id and
                primary.identity.timeline_id == fence.identity.timeline_id and
                primary.identity.epoch == fence.identity.epoch)
            {
                // The caller-provided LSN is only the last control-plane
                // observation. Freeze the live writer under the same mutex used
                // by final HA WAL append/ack, then bind the receipt to the
                // former primary's actual durable tail.
                if (self.auth.primary_fence_started) |hook| hook.run();
                const durable_tail = primary.lastLsn();
                if (durable_tail < fence.required_lsn) {
                    return try textResponse(self.alloc, 409, "FormerPrimaryBehindFenceBoundary");
                }
                fence.required_lsn = durable_tail;
                fence.observed_lsn = durable_tail;
            }
        }
        var result = ha_admin.acquirePromotionFence(self.alloc, fence_store, fence) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        const action_id = try std.fmt.allocPrint(self.alloc, "fence_acquire:{s}", .{result.receipt.promoted_node_id});
        defer self.alloc.free(action_id);
        const action_node_id = self.primaryNodeID() orelse self.standbyNodeID() orelse result.receipt.promoted_node_id;
        return try self.handleTypedJson(admin_api.HAFenceResponse{
            .schema_version = 1,
            .action = .{
                .action_id = action_id,
                .action_kind = "fence_acquire",
                .target = result.receipt.promoted_node_id,
                .state = "applied",
                .node_id = action_node_id,
            },
            .receipt = try adminFenceReceipt(result.receipt),
        });
    }

    fn handleAdminAssessPromotion(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        var command = admin_cli.PromoteAssessCommand{ .check = .{} };
        if (req.body.len != 0) {
            var parsed = admin_api.server.parseAssessHAPromotionBody(
                self.alloc,
                req.body,
            ) catch return try textResponse(self.alloc, 400, "invalid HA promotion assessment request");
            defer parsed.deinit();

            if (parsed.value.required_lsn) |value| {
                command.check.required_lsn = uint64FromJson(value) catch {
                    return try textResponse(self.alloc, 400, "invalid HA promotion assessment request");
                };
            }
            command.check.fencing_confirmed = parsed.value.fencing_confirmed;
            command.check.force = parsed.value.force;
            command.use_current_fence = parsed.value.use_current_fence;
        }
        const node_id = self.standbyNodeID() orelse return try textResponse(self.alloc, 409, "StandbyNodeIDUnavailable");

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
            .promote_assess => |assessment| blk: {
                const action_id = try std.fmt.allocPrint(self.alloc, "promotion_assess:{s}", .{node_id});
                defer self.alloc.free(action_id);
                break :blk try self.handleTypedJson(admin_api.HAPromotionAssessResponse{
                    .schema_version = 1,
                    .action = .{
                        .action_id = action_id,
                        .action_kind = "promotion_assess",
                        .target = node_id,
                        .state = "assessed",
                        .node_id = node_id,
                    },
                    .assessment = try adminPromotionAssessment(assessment),
                });
            },
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
        const document = try promotionDocument(self.alloc, result);
        defer self.alloc.free(document.action.action_id);
        return try self.handleTypedJson(document);
    }

    fn handleAdminPromote(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const fence_store = self.ctx.fence_store orelse return try textResponse(self.alloc, 409, "FenceStoreUnavailable");
        const standby = self.ctx.standby orelse return try textResponse(self.alloc, 409, "StandbyUnavailable");
        const fence = self.parsePromoteFenceRequest(req) catch {
            return try textResponse(self.alloc, 400, "invalid HA promotion request");
        };
        var result = ha_admin.promoteWithFence(self.alloc, fence_store, standby, .{ .fence = fence }) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        defer result.deinit(self.alloc);
        const document = try promotionDocument(self.alloc, result);
        defer self.alloc.free(document.action.action_id);
        return try self.handleTypedJson(document);
    }

    fn handleAdminAssessRejoin(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        return try self.handleAdminRejoin(req, null);
    }

    fn handleAdminRewindRejoin(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        return try self.handleAdminRejoin(req, .rewind);
    }

    fn handleAdminReseedRejoin(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        return try self.handleAdminRejoin(req, .reseed);
    }

    fn handleAdminRejoin(self: *Server, req: http_common.HttpRequest, expected_action: ?rejoin.Action) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA rejoin assessment request");
        var parsed = admin_api.server.parseAssessHARejoinBody(
            self.alloc,
            req.body,
        ) catch return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        defer parsed.deinit();

        const identity = adminIdentityFromOpenApi(parsed.value.identity) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        var last_lsn = uint64FromJson(parsed.value.last_lsn) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        const retained_from_lsn = uint64FromJson(parsed.value.retained_from_lsn) catch {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        };
        if (!validation.isIdentifier(parsed.value.node_id)) {
            return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
        }
        const receipt = if (parsed.value.receipt) |value|
            adminFenceReceiptFromOpenApi(value) catch {
                return try textResponse(self.alloc, 400, "invalid HA rejoin assessment request");
            }
        else
            null;

        if (self.localFormerPrimaryLog(parsed.value.node_id)) |log| {
            last_lsn = log.lastLsn();
        } else if (expected_action != null and expected_action.? == .rewind) {
            return try textResponse(self.alloc, 409, "FormerPrimaryLogUnavailable");
        }

        if (expected_action != null) {
            if (receipt) |fence| {
                self.validateRejoinReceiptBinding(parsed.value.node_id, identity, fence) catch {
                    return try textResponse(self.alloc, 400, "invalid HA rejoin receipt binding");
                };
            }
        }

        const assessment = ha_admin.assessFormerPrimaryRejoin(.{
            .node_id = parsed.value.node_id,
            .identity = identity,
            .last_lsn = last_lsn,
        }, receipt, .{
            .retained_from_lsn = retained_from_lsn,
            .allow_rewind_after_forced_promotion = parsed.value.allow_rewind_after_forced_promotion,
        });

        if (expected_action) |expected| {
            if (assessment.action != expected) {
                const message = switch (expected) {
                    .rewind => "HA rejoin assessment does not allow rewind",
                    .reseed => "HA rejoin assessment does not allow reseed",
                    else => "HA rejoin assessment does not allow requested action",
                };
                return try textResponse(self.alloc, 409, message);
            }

            if (receipt) |fence| {
                self.recordVerifiedLocalRejoinReceipt(parsed.value.node_id, identity, fence) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
            }

            if (expected == .rewind) {
                const log = self.rejoinRewindLog() orelse
                    return try textResponse(self.alloc, 409, "FormerPrimaryLogUnavailable");
                const rewind = ha_admin.rewindFormerPrimaryReplicationLog(self.alloc, log, assessment) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
                const action_id = try std.fmt.allocPrint(self.alloc, "rejoin_rewind:{s}", .{assessment.former_node_id});
                defer self.alloc.free(action_id);
                return try self.handleTypedJson(admin_api.HARejoinAssessResponse{
                    .schema_version = 1,
                    .action = .{
                        .action_id = action_id,
                        .action_kind = "rejoin_rewind",
                        .target = assessment.former_node_id,
                        .state = "applied",
                        .node_id = assessment.former_node_id,
                    },
                    .assessment = try adminRejoinAssessment(assessment),
                    .rewind = try adminRejoinRewindResult(rewind),
                });
            }
            if (expected == .reseed) {
                const primary = self.ctx.primary orelse
                    return try textResponse(self.alloc, 409, "PrimaryUnavailable");
                const node_id = self.primaryNodeID() orelse return try textResponse(self.alloc, 409, "PrimaryNodeIDUnavailable");
                const reseed = ha_admin.markFormerPrimaryForReseed(primary, assessment) catch |err| {
                    return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
                };
                const action_id = try std.fmt.allocPrint(self.alloc, "rejoin_reseed:{s}", .{assessment.former_node_id});
                defer self.alloc.free(action_id);
                return try self.handleTypedJson(admin_api.HARejoinAssessResponse{
                    .schema_version = 1,
                    .action = .{
                        .action_id = action_id,
                        .action_kind = "rejoin_reseed",
                        .target = assessment.former_node_id,
                        .state = "applied",
                        .node_id = node_id,
                    },
                    .assessment = try adminRejoinAssessment(assessment),
                    .reseed = try adminRejoinReseedResult(reseed),
                });
            }
        }

        const action_id = try std.fmt.allocPrint(self.alloc, "rejoin_assess:{s}", .{assessment.former_node_id});
        defer self.alloc.free(action_id);
        return try self.handleTypedJson(admin_api.HARejoinAssessResponse{
            .schema_version = 1,
            .action = .{
                .action_id = action_id,
                .action_kind = "rejoin_assess",
                .target = assessment.former_node_id,
                .state = "assessed",
                .node_id = assessment.former_node_id,
            },
            .assessment = try adminRejoinAssessment(assessment),
        });
    }

    fn validateRejoinReceiptBinding(self: *Server, node_id: []const u8, identity: standby_mod.Identity, receipt: fencing.Receipt) !void {
        _ = self;
        try fencing.validateReceiptBinding(receipt, .{
            .old_primary_id = node_id,
            .parent_identity = identity,
        });
    }

    fn recordVerifiedLocalRejoinReceipt(self: *Server, node_id: []const u8, identity: standby_mod.Identity, receipt: fencing.Receipt) !void {
        const local_node_id = self.ctx.primary_node_id orelse return;
        if (!std.mem.eql(u8, local_node_id, node_id)) return;
        const fence_store = self.ctx.fence_store orelse return;
        try fence_store.recordVerifiedReceipt(receipt, .{
            .old_primary_id = node_id,
            .parent_identity = identity,
        });
    }

    fn rejoinRewindLog(self: *Server) ?*replication_log.ReplicationLog {
        if (self.ctx.former_primary_log) |log| return log;
        if (self.ctx.primary) |primary| return &primary.log;
        return null;
    }

    fn localFormerPrimaryLog(self: *Server, node_id: []const u8) ?*replication_log.ReplicationLog {
        const local_node_id = self.ctx.primary_node_id orelse return null;
        if (!std.mem.eql(u8, local_node_id, node_id)) return null;
        return self.rejoinRewindLog();
    }

    fn parseAcquireFenceRequest(self: *Server, req: http_common.HttpRequest) !fencing.FenceRequest {
        if (req.body.len == 0) return error.InvalidAdminRequest;
        var parsed = admin_api.server.parseAcquireHAFenceBody(
            self.alloc,
            req.body,
        ) catch return error.InvalidAdminRequest;
        defer parsed.deinit();
        return adminFenceRequestFromOpenApi(parsed.value) catch return error.InvalidAdminRequest;
    }

    fn parsePromoteFenceRequest(self: *Server, req: http_common.HttpRequest) !fencing.FenceRequest {
        if (req.body.len == 0) return error.InvalidAdminRequest;
        var parsed = admin_api.server.parsePromoteHABody(
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
        if (!legacyCommandEndpointAllowed(plan.command, plan.output)) {
            return try textResponse(self.alloc, 409, "TypedAdminAPIRequired");
        }

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

    fn primaryNodeID(self: *const Server) ?[]const u8 {
        if (self.ctx.primary_node_id) |node_id| {
            if (validation.isIdentifier(node_id)) return node_id;
        }
        return null;
    }

    fn standbyNodeID(self: *const Server) ?[]const u8 {
        if (self.ctx.standby_node_id) |node_id| {
            if (validation.isIdentifier(node_id)) return node_id;
        }
        return null;
    }
};

const bearer_prefix = "Bearer ";

const AdminHAPathField = enum {
    manifest,
    content_root,
};

fn validateAdminHAPath(raw: []const u8, field: AdminHAPathField) ![]const u8 {
    switch (validation.classifyHAString(raw)) {
        .ok => {},
        .missing => return switch (field) {
            .manifest => error.ManifestPathMissing,
            .content_root => error.ContentRootMissing,
        },
        .padded => return adminHAPathInvalidError(field),
    }
    if (!validation.isAbsoluteNormalizedPath(raw)) return adminHAPathInvalidError(field);
    return raw;
}

fn adminHAPathInvalidError(field: AdminHAPathField) anyerror {
    return switch (field) {
        .manifest => error.ManifestPathInvalid,
        .content_root => error.ContentRootInvalid,
    };
}

fn bearerAuthorizationMatches(expected_token: []const u8, authorization: []const u8) bool {
    if (!std.mem.startsWith(u8, authorization, bearer_prefix)) return false;
    const provided_token = authorization[bearer_prefix.len..];
    return timingSafeEql(expected_token, provided_token);
}

fn timingSafeEql(expected: []const u8, provided: []const u8) bool {
    if (expected.len != provided.len) return false;
    var diff: u8 = 0;
    for (expected, provided) |expected_byte, provided_byte| {
        diff |= expected_byte ^ provided_byte;
    }
    return diff == 0;
}

fn legacyCommandEndpointAllowed(command: admin_cli.Command, output: admin_cli.OutputFormat) bool {
    if (output == .prometheus) return false;
    return switch (command) {
        .identify_system,
        .start_replication,
        .stream_once,
        .standby_status_update,
        => true,
        else => false,
    };
}

const ActionReceiptDocument = struct {
    action_id: []const u8,
    action_kind: []const u8,
    target: []const u8,
    state: []const u8,
    node_id: []const u8,

    fn deinit(self: *ActionReceiptDocument, alloc: Allocator) void {
        alloc.free(self.action_id);
        alloc.free(self.target);
        self.* = undefined;
    }
};

fn actionReceiptAlloc(
    alloc: Allocator,
    action_kind: []const u8,
    target: []const u8,
    state: []const u8,
    node_id: []const u8,
) !ActionReceiptDocument {
    return .{
        .action_id = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ action_kind, target }),
        .action_kind = action_kind,
        .target = try alloc.dupe(u8, target),
        .state = state,
        .node_id = node_id,
    };
}

fn promotionDocument(alloc: Allocator, result: ha_admin.FencedPromotionResult) !admin_api.HAPromotionResponse {
    const target = result.promoted_node_id;
    const action_id = try std.fmt.allocPrint(alloc, "promotion:{s}", .{target});
    errdefer alloc.free(action_id);
    return .{
        .schema_version = 1,
        .action = .{
            .action_id = action_id,
            .action_kind = "promotion",
            .target = target,
            .state = "applied",
            .node_id = result.promoted_node_id,
        },
        .assessment = try adminPromotionAssessment(result.assessment),
        .promotion = .{
            .node_id = result.promoted_node_id,
            .switch_lsn = try adminI64(result.promotion.switch_lsn),
            .old_identity = try adminIdentity(result.promotion.old_identity),
            .new_identity = try adminIdentity(result.promotion.new_identity),
            .forced = result.promotion.forced,
            .data_loss_possible = result.promotion.data_loss_possible,
        },
        .fence_generation = try adminI64(result.fence_generation),
        .fence_token = result.fence_token,
        .forced = result.forced,
    };
}

fn adminCommitGate(gate: commit_gate.GateResult) !admin_api.HACommitGate {
    return .{
        .target_lsn = try adminI64(gate.target_lsn),
        .action = @tagName(gate.action),
        .durability = try adminDurabilityDecision(gate.decision),
    };
}

fn adminDurabilityDecision(decision: primary_mod.DurabilityDecision) !admin_api.HADurabilityDecision {
    return .{
        .status = @tagName(decision.status),
        .mode = @tagName(decision.mode),
        .selection = @tagName(decision.selection),
        .target_lsn = try adminI64(decision.target_lsn),
        .progress_lsn = try adminI64(decision.progress_lsn),
        .missing_lsn_count = try adminI64(decision.missing_lsn_count),
        .satisfied_count = try adminI64(decision.satisfied_count),
        .required_count = try adminI64(decision.required_count),
        .candidate_count = try adminI64(decision.candidate_count),
    };
}

fn adminReadDecision(decision: read_gate.Decision) !admin_api.HAReadDecision {
    return .{
        .action = @tagName(decision.action),
        .consistency = @tagName(decision.consistency),
        .required_lsn = if (decision.required_lsn) |value| try adminI64(value) else null,
        .required_metadata_lsn = if (decision.required_metadata_lsn) |value| try adminI64(value) else null,
        .received_lsn = try adminI64(decision.received_lsn),
        .applied_lsn = try adminI64(decision.applied_lsn),
        .safe_read_lsn = try adminI64(decision.safe_read_lsn),
        .metadata_applied_lsn = if (decision.metadata_applied_lsn) |value| try adminI64(value) else null,
        .serve_lsn = if (decision.serve_lsn) |value| try adminI64(value) else null,
        .missing_lsn_count = try adminI64(decision.missing_lsn_count),
        .metadata_missing_lsn_count = try adminI64(decision.metadata_missing_lsn_count),
    };
}

fn adminWriteDecision(decision: write_gate.Decision) !admin_api.HAWriteDecision {
    return .{
        .role = @tagName(decision.role),
        .action = @tagName(decision.action),
        .identity = try adminIdentity(decision.identity),
        .durable_lsn = try adminI64(decision.durable_lsn),
        .next_lsn = try adminI64(decision.next_lsn),
        .promotion_handoff = if (decision.promotion_handoff) |handoff| try adminPromotionHandoff(handoff) else null,
    };
}

fn adminOwnerJobDecision(decision: owner_job_gate.Decision) !admin_api.HAOwnerJobDecision {
    return .{
        .kind = @tagName(decision.kind),
        .role = @tagName(decision.role),
        .action = @tagName(decision.action),
        .identity = try adminIdentity(decision.identity),
        .durable_lsn = try adminI64(decision.durable_lsn),
        .next_lsn = try adminI64(decision.next_lsn),
        .promotion_handoff = if (decision.promotion_handoff) |handoff| try adminPromotionHandoff(handoff) else null,
    };
}

fn adminPromotionHandoff(handoff: standby_mod.PromotionHandoff) !admin_api.HAPromotionHandoff {
    return .{
        .identity = try adminIdentity(handoff.identity),
        .switch_lsn = try adminI64(handoff.switch_lsn),
        .next_lsn = try adminI64(handoff.next_lsn),
    };
}

fn adminPrimarySnapshot(alloc: Allocator, snapshot: status_mod.PrimarySnapshot, node_id: []const u8) !admin_api.HAPrimarySnapshot {
    return .{
        .role = @tagName(snapshot.role),
        .node_id = node_id,
        .identity = try adminIdentity(snapshot.identity),
        .current_lsn = try adminI64(snapshot.current_lsn),
        .slots = try adminSlotSnapshots(alloc, snapshot.slots),
        .retention = try adminRetentionSnapshot(snapshot.retention),
        .durability = if (snapshot.durability) |decision| try adminDurabilityDecision(decision) else null,
    };
}

fn adminStandbySnapshot(snapshot: status_mod.StandbySnapshot, node_id: []const u8) !admin_api.HAStandbySnapshot {
    return .{
        .role = @tagName(snapshot.role),
        .node_id = node_id,
        .identity = try adminIdentity(snapshot.identity),
        .received_lsn = try adminI64(snapshot.received_lsn),
        .applied_lsn = try adminI64(snapshot.applied_lsn),
        .safe_read_lsn = try adminI64(snapshot.safe_read_lsn),
        .upstream_lsn = if (snapshot.upstream_lsn) |value| try adminI64(value) else null,
        .write_lag_lsn = if (snapshot.write_lag_lsn) |value| try adminI64(value) else null,
        .receive_lag_lsn = if (snapshot.receive_lag_lsn) |value| try adminI64(value) else null,
        .apply_lag_lsn = if (snapshot.apply_lag_lsn) |value| try adminI64(value) else null,
        .last_error = snapshot.last_error,
        .last_attempt_ns = if (snapshot.last_attempt_ns) |value| try adminI64(value) else null,
        .last_success_ns = if (snapshot.last_success_ns) |value| try adminI64(value) else null,
        .replication_failures_total = if (snapshot.replication_failures_total) |value| try adminI64(value) else null,
        .unapplied_lsn_count = try adminI64(snapshot.unapplied_lsn_count),
        .caught_up_to_received = snapshot.caught_up_to_received,
        .can_serve_safe_reads = snapshot.can_serve_safe_reads,
    };
}

fn adminSlotSnapshots(alloc: Allocator, slots: []const status_mod.SlotSnapshot) ![]admin_api.HASlotSnapshot {
    const admin_slots = try alloc.alloc(admin_api.HASlotSnapshot, slots.len);
    errdefer alloc.free(admin_slots);
    for (slots, 0..) |slot, idx| {
        admin_slots[idx] = .{
            .name = slot.name,
            .timeline_id = try adminI64(slot.timeline_id),
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .restart_lsn = try adminI64(slot.restart_lsn),
            .received_lsn = try adminI64(slot.received_lsn),
            .applied_lsn = try adminI64(slot.applied_lsn),
            .safe_read_lsn = try adminI64(slot.safe_read_lsn),
            .write_lag_lsn = try adminI64(slot.write_lag_lsn),
            .apply_lag_lsn = try adminI64(slot.apply_lag_lsn),
            .safe_read_lag_lsn = try adminI64(slot.safe_read_lag_lsn),
            .retention_lag_lsn = try adminI64(slot.retention_lag_lsn),
            .status = @tagName(slot.status),
            .last_error = slot.last_error,
        };
    }
    return admin_slots;
}

fn adminRetentionSnapshot(snapshot: slot_store.RetentionSnapshot) !admin_api.HARetentionSnapshot {
    return .{
        .primary_lsn = try adminI64(snapshot.primary_lsn),
        .oldest_restart_lsn = try adminI64(snapshot.oldest_restart_lsn),
        .retained_lsn_count = try adminI64(snapshot.retained_lsn_count),
        .retained_byte_count = try adminI64(snapshot.retained_byte_count),
        .retained_age_ns = try adminI64(snapshot.retained_age_ns),
        .active_slots = try adminI64(snapshot.active_slots),
        .reseed_recommended = try adminI64(snapshot.reseed_recommended),
    };
}

fn adminPromotionAssessment(assessment: status_mod.PromotionAssessment) !admin_api.HAPromotionAssessment {
    return .{
        .required_lsn = try adminI64(assessment.required_lsn),
        .received_lsn = try adminI64(assessment.received_lsn),
        .applied_lsn = try adminI64(assessment.applied_lsn),
        .has_required_lsn = assessment.has_required_lsn,
        .caught_up_to_received = assessment.caught_up_to_received,
        .fencing_confirmed = assessment.fencing_confirmed,
        .force = assessment.force,
        .mode = @tagName(assessment.mode),
        .data_loss_possible = assessment.data_loss_possible,
        .safe = assessment.safe,
        .requires_fencing = assessment.requires_fencing,
        .requires_force = assessment.requires_force,
        .can_promote = assessment.can_promote,
    };
}

fn adminRejoinAssessment(assessment: rejoin.Assessment) !admin_api.HARejoinAssessment {
    return .{
        .action = @tagName(assessment.action),
        .reason = @tagName(assessment.reason),
        .former_node_id = assessment.former_node_id,
        .target_timeline_id = try adminI64(assessment.target_timeline_id),
        .target_epoch = try adminI64(assessment.target_epoch),
        .parent_cluster_id = try adminI64(assessment.parent_cluster_id),
        .parent_shard_id = try adminI64(assessment.parent_shard_id),
        .parent_table_id = try adminI64(assessment.parent_table_id),
        .parent_timeline_id = try adminI64(assessment.parent_timeline_id),
        .parent_epoch = try adminI64(assessment.parent_epoch),
        .fork_lsn = try adminI64(assessment.fork_lsn),
        .former_last_lsn = try adminI64(assessment.former_last_lsn),
        .retained_from_lsn = try adminI64(assessment.retained_from_lsn),
        .data_loss_discarded = assessment.data_loss_discarded,
    };
}

fn adminRejoinRewindResult(result: rejoin.RewindResult) !admin_api.HARejoinRewindResult {
    return .{
        .node_id = result.node_id,
        .fork_lsn = try adminI64(result.fork_lsn),
        .previous_last_lsn = try adminI64(result.previous_last_lsn),
        .current_last_lsn = try adminI64(result.current_last_lsn),
        .next_lsn = try adminI64(result.next_lsn),
        .discarded_lsn_count = try adminI64(result.discarded_lsn_count),
        .target_timeline_id = try adminI64(result.target_timeline_id),
        .target_epoch = try adminI64(result.target_epoch),
        .data_loss_discarded = result.data_loss_discarded,
    };
}

fn adminRejoinReseedResult(result: rejoin.ReseedResult) !admin_api.HARejoinReseedResult {
    return .{
        .node_id = result.node_id,
        .slot_name = result.slot_name,
        .target_timeline_id = try adminI64(result.target_timeline_id),
        .target_epoch = try adminI64(result.target_epoch),
        .fork_lsn = try adminI64(result.fork_lsn),
        .former_last_lsn = try adminI64(result.former_last_lsn),
        .reseed_required = result.reseed_required,
        .base_backup_required = result.base_backup_required,
    };
}

fn adminIdentity(identity: standby_mod.Identity) !admin_api.HAIdentity {
    return .{
        .cluster_id = try adminI64(identity.cluster_id),
        .shard_id = try adminI64(identity.shard_id),
        .table_id = try adminI64(identity.table_id),
        .timeline_id = try adminI64(identity.timeline_id),
        .epoch = try adminI64(identity.epoch),
    };
}

fn adminFenceReceipt(receipt: fencing.Receipt) !admin_api.HAFenceReceipt {
    return .{
        .identity = try adminIdentity(receipt.identity),
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .parent_timeline_id = try adminI64(receipt.parent_timeline_id),
        .parent_epoch = try adminI64(receipt.parent_epoch),
        .new_timeline_id = try adminI64(receipt.new_timeline_id),
        .new_epoch = try adminI64(receipt.new_epoch),
        .required_lsn = try adminI64(receipt.required_lsn),
        .observed_lsn = try adminI64(receipt.observed_lsn),
        .generation = try adminI64(receipt.generation),
        .forced = receipt.forced,
        .token = receipt.token,
        .reason = receipt.reason,
    };
}

fn adminI64(value: u64) !i64 {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return error.AdminOpenAPIIntegerOverflow;
    return @intCast(value);
}

fn slotListDocuments(alloc: Allocator, snapshot: status_mod.PrimarySnapshot) ![]admin_api.HAReplicationSlot {
    const slots = try alloc.alloc(admin_api.HAReplicationSlot, snapshot.slots.len);
    errdefer alloc.free(slots);
    for (snapshot.slots, 0..) |slot, idx| {
        slots[idx] = .{
            .slot_name = slot.name,
            .timeline_id = try adminI64(slot.timeline_id),
            .restart_lsn = try adminI64(slot.restart_lsn),
            .received_lsn = try adminI64(slot.received_lsn),
            .applied_lsn = try adminI64(slot.applied_lsn),
            .safe_read_lsn = try adminI64(slot.safe_read_lsn),
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .last_error = slot.last_error,
            .current_lsn = try adminI64(snapshot.current_lsn),
        };
    }
    return slots;
}

fn slotActionDocument(
    action_id: []const u8,
    action_kind: []const u8,
    target: []const u8,
    node_id: []const u8,
    action: []const u8,
    slot: anytype,
    dropped: ?bool,
) !admin_api.HAReplicationSlotActionResponse {
    return .{
        .schema_version = 1,
        .action = .{
            .action_id = action_id,
            .action_kind = action_kind,
            .target = target,
            .state = "applied",
            .node_id = node_id,
        },
        .slot_action = action,
        .slot = .{
            .slot_name = slot.slot_name,
            .timeline_id = try adminI64(slot.timeline_id),
            .restart_lsn = try adminI64(slot.restart_lsn),
            .received_lsn = try adminI64(slot.received_lsn),
            .applied_lsn = try adminI64(slot.applied_lsn),
            .safe_read_lsn = try adminI64(slot.safe_read_lsn),
            .active = slot.active,
            .reseed_required = slot.reseed_required,
            .last_error = slot.last_error,
            .current_lsn = try adminI64(slot.current_lsn),
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

const LifecycleReceiptQuery = struct {
    kind: lifecycle_receipt_ledger.Kind,
    after: u64 = 0,
    limit: usize = 100,
};

fn parseLifecycleReceiptQuery(query: []const u8) !LifecycleReceiptQuery {
    if (query.len == 0 or std.mem.indexOfScalar(u8, query, '%') != null) return error.InvalidLifecycleReceiptQuery;
    var kind: ?lifecycle_receipt_ledger.Kind = null;
    var after: ?u64 = null;
    var limit: ?usize = null;
    var fields = std.mem.splitScalar(u8, query, '&');
    while (fields.next()) |field| {
        if (field.len == 0) return error.InvalidLifecycleReceiptQuery;
        const separator = std.mem.indexOfScalar(u8, field, '=') orelse return error.InvalidLifecycleReceiptQuery;
        if (separator == 0 or separator + 1 >= field.len or
            std.mem.indexOfScalarPos(u8, field, separator + 1, '=') != null)
            return error.InvalidLifecycleReceiptQuery;
        const key = field[0..separator];
        const value = field[separator + 1 ..];
        if (std.mem.eql(u8, key, "kind")) {
            if (kind != null) return error.InvalidLifecycleReceiptQuery;
            kind = if (std.mem.eql(u8, value, "capture"))
                .capture
            else if (std.mem.eql(u8, value, "activation"))
                .activation
            else
                return error.InvalidLifecycleReceiptQuery;
        } else if (std.mem.eql(u8, key, "after")) {
            if (after != null) return error.InvalidLifecycleReceiptQuery;
            after = try parseUnsignedQueryValue(u64, value);
        } else if (std.mem.eql(u8, key, "limit")) {
            if (limit != null) return error.InvalidLifecycleReceiptQuery;
            const parsed = try parseUnsignedQueryValue(usize, value);
            if (parsed == 0 or parsed > lifecycle_receipt_ledger.max_page_limit)
                return error.InvalidLifecycleReceiptQuery;
            limit = parsed;
        } else {
            return error.InvalidLifecycleReceiptQuery;
        }
    }
    return .{
        .kind = kind orelse return error.InvalidLifecycleReceiptQuery,
        .after = after orelse 0,
        .limit = limit orelse 100,
    };
}

fn parseUnsignedQueryValue(comptime T: type, value: []const u8) !T {
    if (value.len == 0) return error.InvalidLifecycleReceiptQuery;
    for (value) |byte| if (byte < '0' or byte > '9') return error.InvalidLifecycleReceiptQuery;
    return std.fmt.parseInt(T, value, 10) catch return error.InvalidLifecycleReceiptQuery;
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
        std.mem.eql(u8, path, admin_api.routes.ha_base_backups_capture) or
        std.mem.eql(u8, path, admin_api.routes.ha_base_backups_activate) or
        std.mem.eql(u8, path, admin_api.routes.ha_seed_lifecycle_receipts) or
        std.mem.eql(u8, path, admin_api.routes.ha_standby_bootstrap) or
        std.mem.eql(u8, path, admin_api.routes.ha_fence) or
        std.mem.eql(u8, path, admin_api.routes.ha_fence_current) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion_assess) or
        std.mem.eql(u8, path, admin_api.routes.ha_promotion_current_fence) or
        std.mem.eql(u8, path, admin_api.routes.ha_rejoin_assess) or
        std.mem.eql(u8, path, admin_api.routes.ha_rejoin_rewind) or
        std.mem.eql(u8, path, admin_api.routes.ha_rejoin_reseed);
}

fn knownRoute(path: []const u8) bool {
    return knownFixedRoute(path) or
        admin_api.routes.replicationSlotNameFromPath(path, "") != null or
        admin_api.routes.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_pause_suffix) != null or
        admin_api.routes.replicationSlotNameFromPath(path, admin_api.routes.ha_replication_slot_resume_suffix) != null;
}

fn isTypedAdminRoute(path: []const u8) bool {
    return std.mem.startsWith(u8, path, admin_api.routes.ha) and knownRoute(path);
}

fn isAdminAuthRequired(path: []const u8) bool {
    return std.mem.eql(u8, path, Routes.command) or isTypedAdminRoute(path);
}

fn generatedRoutePathAlloc(alloc: Allocator, generated_path: []const u8) ![]u8 {
    const slot_param = "{slot_name}";
    if (std.mem.indexOf(u8, generated_path, slot_param)) |idx| {
        return try std.fmt.allocPrint(alloc, "{s}{s}{s}{s}", .{
            admin_api.routes.base,
            generated_path[0..idx],
            "standby-a",
            generated_path[idx + slot_param.len ..],
        });
    }
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ admin_api.routes.base, generated_path });
}

fn generatedRouteUnsupportedMethod(generated_path: []const u8) ?http_common.Method {
    const candidates = [_]http_common.Method{ .GET, .POST, .PUT, .DELETE };
    for (candidates) |candidate| {
        if (!generatedRouteSupportsMethod(generated_path, methodText(candidate))) return candidate;
    }
    return null;
}

fn generatedRouteSupportsMethod(generated_path: []const u8, method: []const u8) bool {
    for (admin_api.openapi.server.routes) |route| {
        if (!std.mem.eql(u8, route.path, generated_path)) continue;
        if (std.mem.eql(u8, route.method, method)) return true;
    }
    return false;
}

fn implementedAdminRouteDocumented(full_path: []const u8, method: []const u8) bool {
    if (!std.mem.startsWith(u8, full_path, admin_api.routes.base)) return false;
    const spec_path = full_path[admin_api.routes.base.len..];
    for (admin_api.openapi.server.routes) |route| {
        if (!std.mem.eql(u8, route.path, spec_path)) continue;
        if (std.mem.eql(u8, route.method, method)) return true;
    }
    return false;
}

fn methodFromText(method: []const u8) !http_common.Method {
    if (std.mem.eql(u8, method, "GET")) return .GET;
    if (std.mem.eql(u8, method, "POST")) return .POST;
    if (std.mem.eql(u8, method, "PUT")) return .PUT;
    if (std.mem.eql(u8, method, "DELETE")) return .DELETE;
    return error.UnsupportedGeneratedMethod;
}

fn methodText(method: http_common.Method) []const u8 {
    return switch (method) {
        .GET => "GET",
        .POST => "POST",
        .PUT => "PUT",
        .DELETE => "DELETE",
    };
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
    const selection = if (policy.selection) |raw| try parseStandbySelectionQuery(raw) else .any;
    const standby_names = policy.standby_names orelse &.{};
    if (selection == .all and (policy.required != null or standby_names.len == 0)) return error.InvalidAdminRequest;
    const required = if (selection == .all) standby_names.len else if (policy.required) |value| blk: {
        const parsed = try positiveUint64FromJson(value);
        if (parsed > std.math.maxInt(usize)) return error.InvalidAdminRequest;
        break :blk @as(usize, @intCast(parsed));
    } else 1;
    for (standby_names) |name| {
        if (!validation.isIdentifier(name)) return error.InvalidAdminRequest;
    }

    return .{
        .mode = try parseDurabilityModeQuery(policy.mode),
        .selection = selection,
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
        .shard_id = if (request.shard_id) |value| try uint64FromJson(value) else null,
        .table_id = if (request.table_id) |value| try uint64FromJson(value) else null,
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
        .force = request.force,
        .reason = request.reason orelse "",
    };
}

const QuerySyncPolicy = struct {
    policy: ?primary_mod.SyncPolicy = null,
    owned_standby_names: []const []const u8 = &.{},

    fn deinit(self: *QuerySyncPolicy, alloc: Allocator) void {
        for (self.owned_standby_names) |name| alloc.free(name);
        alloc.free(self.owned_standby_names);
        self.* = undefined;
    }
};

fn buildSyncPolicyFromQuery(alloc: Allocator, query: []const u8) !QuerySyncPolicy {
    var mode: ?primary_mod.DurabilityMode = null;
    var selection: primary_mod.StandbySelection = .any;
    var required: usize = 1;
    var required_set = false;
    var failure_policy: primary_mod.FailurePolicy = .block;
    var names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        freeQueryNames(alloc, names.items);
        names.deinit(alloc);
    }

    if (queryValue(query, "sync_mode")) |raw| mode = try parseDurabilityModeQuery(raw);
    if (queryValue(query, "sync_selection")) |raw| selection = try parseStandbySelectionQuery(raw);
    if (queryValue(query, "sync_required")) |raw| {
        const parsed = try uint64Text(raw);
        if (parsed == 0 or parsed > std.math.maxInt(usize)) return error.InvalidAdminRequest;
        required = @intCast(parsed);
        required_set = true;
    }
    if (queryValue(query, "sync_failure")) |raw| failure_policy = try parseFailurePolicyQuery(raw);

    var iter = std.mem.splitScalar(u8, query, '&');
    while (iter.next()) |part| {
        const key, const value = splitQueryPart(part);
        if (std.mem.eql(u8, key, "sync_standby")) {
            const decoded = try percentDecodeQueryValueAlloc(alloc, value);
            if (!validation.isIdentifier(decoded)) {
                alloc.free(decoded);
                return error.InvalidAdminRequest;
            }
            names.append(alloc, decoded) catch |err| {
                alloc.free(decoded);
                return err;
            };
        }
    }

    const configured = mode != null or
        selection != .any or
        required_set or
        failure_policy != .block or
        names.items.len > 0;
    if (!configured) return .{};
    if (selection == .all) {
        if (required_set or names.items.len == 0) return error.InvalidAdminRequest;
        required = names.items.len;
    }

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

fn freeQueryNames(alloc: Allocator, names: []const []const u8) void {
    for (names) |name| alloc.free(name);
}

fn percentDecodeQueryValueAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var idx: usize = 0;
    while (idx < encoded.len) {
        const byte = encoded[idx];
        if (byte == '+') {
            try out.append(alloc, ' ');
            idx += 1;
            continue;
        }
        if (byte != '%') {
            try out.append(alloc, byte);
            idx += 1;
            continue;
        }
        if (idx + 2 >= encoded.len) return error.InvalidAdminRequest;
        const hi = hexValue(encoded[idx + 1]) orelse return error.InvalidAdminRequest;
        const lo = hexValue(encoded[idx + 2]) orelse return error.InvalidAdminRequest;
        try out.append(alloc, (hi << 4) | lo);
        idx += 3;
    }

    return try out.toOwnedSlice(alloc);
}

fn hexValue(byte: u8) ?u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => null,
    };
}

fn validSha256Hex(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    }
    return true;
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
    const out = fencing.Receipt{
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
    try fencing.validateReceipt(out);
    return out;
}

fn adminIdentityFromOpenApi(identity: admin_api.HAIdentity) !standby_mod.Identity {
    return .{
        .cluster_id = try positiveUint64FromJson(identity.cluster_id),
        .shard_id = try uint64FromJson(identity.shard_id),
        .table_id = try uint64FromJson(identity.table_id),
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
        error.FencingRequired,
        error.PromotionRequiresForce,
        error.PromotionNotAllowed,
        error.BaseBackupSlotInUse,
        error.BackupSlotNotRetained,
        error.SlotAlreadyExists,
        error.SlotInactive,
        error.SlotRequiresReseed,
        error.WalNoLongerRetained,
        error.RejoinAssessmentStale,
        error.RejoinForkIdentityMismatch,
        error.RejoinRewindNotAllowed,
        error.RejoinReseedNotAllowed,
        error.FormerPrimaryBeforeFork,
        error.MissingReceivedRecord,
        error.RecordAlreadyReceived,
        error.StandbyAlreadyBootstrapped,
        error.SyncPolicyUnsatisfied,
        error.NonMonotonicFenceGeneration,
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
        error.StandbyAheadOfPrimary,
        error.TargetAheadOfPrimary,
        error.InvalidSyncPolicy,
        error.WrongCluster,
        error.WrongShard,
        error.WrongTable,
        error.WrongTimeline,
        error.WrongEpoch,
        error.RejoinReceiptBindingMismatch,
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
    const cwd = try std.process.currentPathAlloc(std.testing.io, alloc);
    defer alloc.free(cwd);
    const rel = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-http-admin-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(rel);
    return try std.fs.path.join(alloc, &.{ cwd, rel });
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

fn baseRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

const ApplyCounter = struct {
    count: usize = 0,

    fn apply(ctx: *anyopaque, _: replication_record.RecordView) !void {
        const self: *ApplyCounter = @ptrCast(@alignCast(ctx));
        self.count += 1;
    }
};

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

test "storage.ha http admin executes typed former primary log rewind when configured" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-rewind");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var former_log = try replication_log.ReplicationLog.open(paths.primary_log.ptr, .{});
    defer former_log.close();
    _ = try former_log.append(alloc, baseRecord(identity, 1, "one"));
    _ = try former_log.append(alloc, baseRecord(identity, 2, "two"));

    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = Server.init(alloc, .{
        .primary_node_id = "primary-a",
        .fence_store = &fence_store,
        .former_primary_log = &former_log,
    });
    defer server.deinit();

    const body =
        "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":1,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}";
    var rewind = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_rewind,
        .content_type = "application/json",
        .body = body,
    });
    defer rewind.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), rewind.status);
    try std.testing.expectEqualStrings("application/json", rewind.content_type.?);
    try expectContains(rewind.body, "\"action_kind\":\"rejoin_rewind\"");
    try expectContains(rewind.body, "\"action_id\":\"rejoin_rewind:primary-a\"");
    try expectContains(rewind.body, "\"state\":\"applied\"");
    try expectContains(rewind.body, "\"assessment\"");
    try expectContains(rewind.body, "\"rewind\"");
    try expectContains(rewind.body, "\"node_id\":\"primary-a\"");
    try expectContains(rewind.body, "\"discarded_lsn_count\":0");
    try expectContains(rewind.body, "\"current_last_lsn\":3");
    try expectContains(rewind.body, "\"next_lsn\":4");
    try std.testing.expectEqual(@as(u64, 3), former_log.lastLsn());
    const current_receipt = (try fence_store.current(alloc)) orelse return error.TestExpectedEqual;
    defer fencing.freeReceipt(alloc, current_receipt);
    try std.testing.expectEqualStrings("primary-a", current_receipt.old_primary_id);
    try std.testing.expectEqualStrings("standby-a", current_receipt.promoted_node_id);

    var stale = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_rewind,
        .content_type = "application/json",
        .body = body,
    });
    defer stale.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), stale.status);
    try std.testing.expectEqual(@as(u64, 3), former_log.lastLsn());
}

test "storage.ha http admin rejects typed former primary rewind on node without local log" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_rewind,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":1,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), response.status);
    try expectContains(response.body, "FormerPrimaryLogUnavailable");
}

test "storage.ha http admin does not persist rejoin assess receipts" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-assess-readonly");
    defer paths.deinit(alloc);

    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{
        .primary_node_id = "primary-a",
        .fence_store = &fence_store,
    });
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":1,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try expectContains(response.body, "\"action_kind\":\"rejoin_assess\"");
    try expectContains(response.body, "\"action\":\"rewind\"");
    try std.testing.expect((try fence_store.current(alloc)) == null);
}

test "storage.ha http admin assesses the local former primary durable tail" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-assess-local-tail");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var former_log = try replication_log.ReplicationLog.open(paths.primary_log.ptr, .{});
    defer former_log.close();
    _ = try former_log.append(alloc, baseRecord(identity, 1, "one"));
    _ = try former_log.append(alloc, baseRecord(identity, 2, "two"));
    _ = try former_log.append(alloc, baseRecord(identity, 3, "divergent-local-write"));

    var server = Server.init(alloc, .{
        .primary_node_id = "primary-a",
        .former_primary_log = &former_log,
    });
    defer server.deinit();

    // The controller's last_lsn=2 was captured at promotion time and is stale.
    // The old node must assess its own durable LSN=3 or it can misclassify an
    // applied divergent write as safe to rewind.
    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":1,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), response.status);
    try expectContains(response.body, "\"action\":\"reseed\"");
    try expectContains(response.body, "\"former_last_lsn\":3");
}

test "storage.ha http admin rejects unbound rejoin receipts before persisting" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-receipt-binding");
    defer paths.deinit(alloc);

    var former_log = try replication_log.ReplicationLog.open(paths.primary_log.ptr, .{});
    defer former_log.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{
        .primary_node_id = "primary-a",
        .fence_store = &fence_store,
        .former_primary_log = &former_log,
    });
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_rewind,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":3,\"retained_from_lsn\":1,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-b\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), response.status);
    try expectContains(response.body, "invalid HA rejoin receipt binding");
    try std.testing.expect((try fence_store.current(alloc)) == null);
}

test "storage.ha http admin marks former primary slot for typed reseed" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "rejoin-reseed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("primary-a", 0);

    var server = Server.init(alloc, .{ .primary = &primary, .primary_node_id = "primary-a" });
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_reseed,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":3,\"retained_from_lsn\":3,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqualStrings("application/json", response.content_type.?);
    try expectContains(response.body, "\"action_kind\":\"rejoin_reseed\"");
    try expectContains(response.body, "\"action_id\":\"rejoin_reseed:primary-a\"");
    try expectContains(response.body, "\"state\":\"applied\"");
    try expectContains(response.body, "\"assessment\"");
    try expectContains(response.body, "\"action\":\"reseed\"");
    try expectContains(response.body, "\"reseed\"");
    try expectContains(response.body, "\"node_id\":\"primary-a\"");
    try expectContains(response.body, "\"slot_name\":\"primary-a\"");
    try expectContains(response.body, "\"base_backup_required\":true");

    const slot = primary.slot("primary-a") orelse return error.TestExpectedEqual;
    try std.testing.expect(slot.reseed_required);
}

test "storage.ha http admin rejects typed former primary reseed on node without primary context" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_reseed,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":3,\"retained_from_lsn\":3,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"generation\":1,\"forced\":false,\"token\":\"token\",\"reason\":\"http-admin-test\"}}",
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), response.status);
    try expectContains(response.body, "PrimaryUnavailable");
}

test "storage.ha http admin serves health and command endpoint" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "command");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const MetadataProgress = struct {
        lsn: u64,

        fn load(ctx: *anyopaque) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.lsn;
        }
    };

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var metadata_progress = MetadataProgress{ .lsn = 0 };

    var server = Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &fence_store,
        .metadata_applied_lsn_ctx = &metadata_progress,
        .metadata_applied_lsn_fn = MetadataProgress.load,
    });
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
    try std.testing.expectEqualStrings("ok", health.body);

    var ready = try server.handle(.{ .method = .GET, .uri = Routes.ready });
    defer ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ready.status);
    try std.testing.expectEqualStrings("ready", ready.body);

    var identify = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"identify\"]}",
    });
    defer identify.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), identify.status);
    try std.testing.expectEqualStrings("application/json", identify.content_type.?);
    try expectContains(identify.body, "\"identify_system\"");

    var rejected_command = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"slot\",\"create\",\"standby-a\",\"--initial-lsn\",\"0\"]}",
    });
    defer rejected_command.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), rejected_command.status);
    try expectContains(rejected_command.body, "TypedAdminAPIRequired");

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

    var typed_create_a = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-a\",\"initial_lsn\":0}",
    });
    defer typed_create_a.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_create_a.status);

    var typed_create = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-b\",\"initial_lsn\":0}",
    });
    defer typed_create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_create.status);
    try std.testing.expectEqualStrings("application/json", typed_create.content_type.?);
    try expectContains(typed_create.body, "\"action_kind\":\"replication_slot_create\"");
    try expectContains(typed_create.body, "\"action_id\":\"replication_slot_create:standby-b\"");
    try expectContains(typed_create.body, "\"state\":\"applied\"");
    try expectContains(typed_create.body, "\"node_id\":\"primary-a\"");
    try expectContains(typed_create.body, "\"slot_action\":\"create\"");
    try expectContains(typed_create.body, "\"slot_name\":\"standby-b\"");

    var typed_slots = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_replication_slots,
    });
    defer typed_slots.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_slots.status);
    try std.testing.expectEqualStrings("application/json", typed_slots.content_type.?);
    try expectContains(typed_slots.body, "\"slots\"");
    try expectContains(typed_slots.body, "\"slot_name\":\"standby-a\"");
    try expectContains(typed_slots.body, "\"slot_name\":\"standby-b\"");
    try std.testing.expect(std.mem.indexOf(u8, typed_slots.body, "\"snapshot\"") == null);

    const typed_pause_uri = try admin_api.routes.replicationSlotPausePathAlloc(alloc, "standby-b");
    defer alloc.free(typed_pause_uri);
    var typed_pause = try server.handle(.{
        .method = .PUT,
        .uri = typed_pause_uri,
    });
    defer typed_pause.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_pause.status);
    try std.testing.expectEqualStrings("application/json", typed_pause.content_type.?);
    try expectContains(typed_pause.body, "\"action_kind\":\"replication_slot_pause\"");
    try expectContains(typed_pause.body, "\"node_id\":\"primary-a\"");
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
    try expectContains(typed_resume.body, "\"action_kind\":\"replication_slot_resume\"");
    try expectContains(typed_resume.body, "\"node_id\":\"primary-a\"");
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
    try expectContains(typed_drop.body, "\"action_kind\":\"replication_slot_drop\"");
    try expectContains(typed_drop.body, "\"node_id\":\"primary-a\"");
    try expectContains(typed_drop.body, "\"slot_action\":\"drop\"");
    try expectContains(typed_drop.body, "\"slot_name\":\"standby-b\"");
    try expectContains(typed_drop.body, "\"dropped\":true");

    var invalid_typed_slot_path = try server.handle(.{
        .method = .DELETE,
        .uri = admin_api.routes.ha_replication_slot_prefix ++ "standby%XX",
    });
    defer invalid_typed_slot_path.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_typed_slot_path.status);
    try expectContains(invalid_typed_slot_path.body, "invalid HA replication slot path");

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
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-a\",\"initial_lsn\":0}",
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

    var typed_read_wait_metadata = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_read_check,
        .content_type = "application/json",
        .body = "{\"consistency\":\"at_least_lsn\",\"required_lsn\":1}",
    });
    defer typed_read_wait_metadata.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_read_wait_metadata.status);
    try expectContains(typed_read_wait_metadata.body, "\"action\":\"wait_for_metadata\"");
    try expectContains(typed_read_wait_metadata.body, "\"metadata_applied_lsn\":0");
    try expectContains(typed_read_wait_metadata.body, "\"metadata_missing_lsn_count\":1");

    metadata_progress.lsn = 1;
    var typed_read_ready = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_read_check,
        .content_type = "application/json",
        .body = "{\"consistency\":\"at_least_lsn\",\"required_lsn\":1}",
    });
    defer typed_read_ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_read_ready.status);
    try expectContains(typed_read_ready.body, "\"action\":\"serve_standby\"");
    try expectContains(typed_read_ready.body, "\"metadata_applied_lsn\":1");

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
        .uri = admin_api.routes.ha_base_backups,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-a\",\"manifest_id\":\"\"}",
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

    var invalid_sync_policy = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_commit_check,
        .content_type = "application/json",
        .body = "{\"target_lsn\":1,\"sync_policy\":{\"mode\":\"remote_write\",\"selection\":\"any\",\"required\":2,\"standby_names\":[\"standby-a\"]}}",
    });
    defer invalid_sync_policy.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_sync_policy.status);
    try expectContains(invalid_sync_policy.body, "InvalidSyncPolicy");

    var fail_closed_append = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_commit_append,
        .content_type = "application/json",
        .body = "{\"payload\":\"two\",\"sync_policy\":{\"mode\":\"remote_write\",\"standby_names\":[\"standby-a\"],\"failure_policy\":\"fail_closed\"}}",
    });
    defer fail_closed_append.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), fail_closed_append.status);
    try expectContains(fail_closed_append.body, "SyncPolicyUnsatisfied");
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var typed_commit_append = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_commit_append,
        .content_type = "application/json",
        .body = "{\"payload\":\"two\",\"sync_policy\":{\"mode\":\"async\"}}",
    });
    defer typed_commit_append.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_commit_append.status);
    try expectContains(typed_commit_append.body, "\"lsn\":2");
    var appended_entry = (try primary.log.entryAt(alloc, 2)) orelse return error.TestExpectedEqual;
    defer appended_entry.deinit(alloc);
    try std.testing.expectEqual(identity.shard_id, appended_entry.record.shard_id);
    try std.testing.expectEqual(identity.table_id, appended_entry.record.table_id);

    var final_boundary_catchup = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"stream\",\"once\",\"--slot\",\"standby-a\"]}",
    });
    defer final_boundary_catchup.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), final_boundary_catchup.status);
    try expectContains(final_boundary_catchup.body, "applied_lsn=2\n");

    var unfenced_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_current_fence,
        .content_type = "application/json",
    });
    defer unfenced_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), unfenced_promote.status);
    try expectContains(unfenced_promote.body, "FenceReceiptMissing");
    try std.testing.expectEqual(@as(u64, 1), standby.identity.timeline_id);

    var typed_fence = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_fence,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1,\"force\":false,\"reason\":\"http-admin-test\"}",
    });
    defer typed_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_fence.status);
    try std.testing.expectEqualStrings("application/json", typed_fence.content_type.?);
    try expectContains(typed_fence.body, "\"schema_version\":1");
    try expectContains(typed_fence.body, "\"action_kind\":\"fence_acquire\"");
    try expectContains(typed_fence.body, "\"action_id\":\"fence_acquire:standby-a\"");
    try expectContains(typed_fence.body, "\"node_id\":\"primary-a\"");
    try expectContains(typed_fence.body, "\"receipt\"");
    try expectContains(typed_fence.body, "\"promoted_node_id\":\"standby-a\"");

    var typed_fence_doc = try std.json.parseFromSlice(admin_api.HAFenceResponse, alloc, typed_fence.body, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    defer typed_fence_doc.deinit();
    const typed_fence_receipt_json = try std.json.Stringify.valueAlloc(alloc, typed_fence_doc.value.receipt, .{});
    defer alloc.free(typed_fence_receipt_json);

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
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":1,\"retained_from_lsn\":0,\"allow_rewind_after_forced_promotion\":false}",
    });
    defer typed_rejoin_unfenced.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rejoin_unfenced.status);
    try std.testing.expectEqualStrings("application/json", typed_rejoin_unfenced.content_type.?);
    try expectContains(typed_rejoin_unfenced.body, "\"schema_version\":1");
    try expectContains(typed_rejoin_unfenced.body, "\"action_kind\":\"rejoin_assess\"");
    try expectContains(typed_rejoin_unfenced.body, "\"state\":\"assessed\"");
    try expectContains(typed_rejoin_unfenced.body, "\"assessment\"");
    try expectContains(typed_rejoin_unfenced.body, "\"action\":\"reject_unfenced\"");
    try expectContains(typed_rejoin_unfenced.body, "\"reason\":\"no_fence\"");

    const typed_rejoin_fenced_body = try std.fmt.allocPrint(
        alloc,
        "{{\"node_id\":\"primary-a\",\"identity\":{{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1}},\"last_lsn\":2,\"retained_from_lsn\":3,\"allow_rewind_after_forced_promotion\":false,\"receipt\":{s}}}",
        .{typed_fence_receipt_json},
    );
    defer alloc.free(typed_rejoin_fenced_body);

    var typed_rejoin_fenced = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = typed_rejoin_fenced_body,
    });
    defer typed_rejoin_fenced.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rejoin_fenced.status);
    try std.testing.expectEqualStrings("application/json", typed_rejoin_fenced.content_type.?);
    try expectContains(typed_rejoin_fenced.body, "\"assessment\"");
    try expectContains(typed_rejoin_fenced.body, "\"action\":\"reseed\"");
    try expectContains(typed_rejoin_fenced.body, "\"target_timeline_id\":2");

    var typed_rejoin_rewind = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_rewind,
        .content_type = "application/json",
        .body = typed_rejoin_fenced_body,
    });
    defer typed_rejoin_rewind.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), typed_rejoin_rewind.status);
    try expectContains(typed_rejoin_rewind.body, "does not allow rewind");

    var typed_rejoin_reseed_mismatch = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_reseed,
        .content_type = "application/json",
        .body = typed_rejoin_fenced_body,
    });
    defer typed_rejoin_reseed_mismatch.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rejoin_reseed_mismatch.status);
    try expectContains(typed_rejoin_reseed_mismatch.body, "\"action_kind\":\"rejoin_reseed\"");
    try expectContains(typed_rejoin_reseed_mismatch.body, "\"state\":\"applied\"");

    var typed_promote_assess = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_assess,
        .content_type = "application/json",
        .body = "{\"required_lsn\":1,\"fencing_confirmed\":false,\"force\":false,\"use_current_fence\":true}",
    });
    defer typed_promote_assess.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote_assess.status);
    try std.testing.expectEqualStrings("application/json", typed_promote_assess.content_type.?);
    try expectContains(typed_promote_assess.body, "\"schema_version\":1");
    try expectContains(typed_promote_assess.body, "\"action_kind\":\"promotion_assess\"");
    try expectContains(typed_promote_assess.body, "\"action_id\":\"promotion_assess:standby-a\"");
    try expectContains(typed_promote_assess.body, "\"state\":\"assessed\"");
    try expectContains(typed_promote_assess.body, "\"node_id\":\"standby-a\"");
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
    try expectContains(typed_promote.body, "\"action_kind\":\"promotion\"");
    try expectContains(typed_promote.body, "\"action_id\":\"promotion:standby-a\"");
    try expectContains(typed_promote.body, "\"promotion\"");
    try expectContains(typed_promote.body, "\"node_id\":\"standby-a\"");
    try expectContains(typed_promote.body, "\"fence_generation\":1");
    try expectContains(typed_promote.body, "\"new_identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2");
}

test "storage.ha http admin reports unsafe promotion as conflict" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "unsafe-promote-conflict");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));

    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{ .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();

    var typed_fence = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_fence,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":2,\"observed_lsn\":2,\"force\":false,\"reason\":\"unsafe-promotion-test\"}",
    });
    defer typed_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_fence.status);

    var unsafe_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_current_fence,
        .content_type = "application/json",
    });
    defer unsafe_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), unsafe_promote.status);
    try expectContains(unsafe_promote.body, "PromotionNotAllowed");
    try std.testing.expectEqual(@as(u64, 1), standby.identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
}

test "storage.ha http admin rejects invalid rejoin fence receipt" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_rejoin_assess,
        .content_type = "application/json",
        .body = "{\"node_id\":\"primary-a\",\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"last_lsn\":2,\"retained_from_lsn\":0,\"receipt\":{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":2,\"epoch\":2},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"parent_timeline_id\":1,\"parent_epoch\":1,\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1,\"generation\":1,\"forced\":false,\"token\":\"\",\"reason\":\"invalid\"}}",
    });
    defer response.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), response.status);
    try expectContains(response.body, "invalid HA rejoin assessment request");
}

test "storage.ha http admin rejects invalid rejoin node identifiers" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    const invalid_node_ids = [_][]const u8{
        "",
        " primary-a",
        "primary-a ",
        "primary/a",
        "primary a",
    };

    for (invalid_node_ids) |node_id| {
        const body = try std.fmt.allocPrint(
            alloc,
            "{{\"node_id\":\"{s}\",\"identity\":{{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1}},\"last_lsn\":2,\"retained_from_lsn\":0}}",
            .{node_id},
        );
        defer alloc.free(body);

        var response = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_rejoin_assess,
            .content_type = "application/json",
            .body = body,
        });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 400), response.status);
        try expectContains(response.body, "invalid HA rejoin assessment request");
    }
}

test "storage.ha http admin enforces optional bearer token on typed admin routes" {
    const alloc = std.testing.allocator;
    var server = Server.initWithOptions(alloc, .{}, .{ .bearer_token = "secret-token" });
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);

    var command_missing = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"identify\"]}",
    });
    defer command_missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 401), command_missing.status);
    try expectContains(command_missing.body, "unauthorized");

    var missing = try server.handle(.{ .method = .GET, .uri = admin_api.routes.ha_primary_status });
    defer missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 401), missing.status);
    try expectContains(missing.body, "unauthorized");

    var wrong = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_primary_status,
        .authorization = "Bearer wrong-token",
    });
    defer wrong.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 401), wrong.status);

    var command_authorized = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .authorization = "Bearer secret-token",
        .body = "{\"argv\":[\"identify\"]}",
    });
    defer command_authorized.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), command_authorized.status);
    try expectContains(command_authorized.body, "PrimaryUnavailable");

    var authorized = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_primary_status,
        .authorization = "Bearer secret-token",
    });
    defer authorized.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), authorized.status);
    try expectContains(authorized.body, "PrimaryUnavailable");
}

test "storage.ha http admin bearer authorization requires exact token" {
    try std.testing.expect(bearerAuthorizationMatches("secret-token", "Bearer secret-token"));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "bearer secret-token"));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "Token secret-token"));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "Bearer secret-xoken"));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "Bearer secret-token "));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "Bearer secret"));
    try std.testing.expect(!bearerAuthorizationMatches("secret-token", "Bearer secret-token-extra"));
}

test "storage.ha http admin empty configured bearer token fails closed" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "", " \t\r\n " }) |configured_token| {
        var server = Server.initWithOptions(alloc, .{}, .{ .bearer_token = configured_token });
        defer server.deinit();

        var health = try server.handle(.{ .method = .GET, .uri = Routes.health });
        defer health.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), health.status);

        var admin = try server.handle(.{ .method = .GET, .uri = admin_api.routes.ha_primary_status });
        defer admin.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 401), admin.status);

        var command = try server.handle(.{
            .method = .POST,
            .uri = Routes.command,
            .content_type = "application/json",
            .body = "{\"argv\":[\"identify\"]}",
        });
        defer command.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 401), command.status);
    }
}

test "storage.ha http admin trims configured bearer token before comparing" {
    const alloc = std.testing.allocator;
    var server = Server.initWithOptions(alloc, .{}, .{ .bearer_token = " secret-token\n" });
    defer server.deinit();

    var authorized = try server.handle(.{
        .method = .GET,
        .uri = admin_api.routes.ha_primary_status,
        .authorization = "Bearer secret-token",
    });
    defer authorized.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), authorized.status);
    try expectContains(authorized.body, "PrimaryUnavailable");
}

test "storage.ha http admin rejects invalid fence request identity and bounds" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-fence-bounds");
    defer paths.deinit(alloc);

    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{ .fence_store = &fence_store });
    defer server.deinit();

    const invalid_requests = [_][]const u8{
        "{\"identity\":{\"cluster_id\":0,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1}",
        "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":0,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1}",
        "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":0},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1}",
        "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":0,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1}",
        "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":0,\"required_lsn\":1,\"observed_lsn\":1}",
        "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":0,\"observed_lsn\":1}",
    };

    for (invalid_requests) |body| {
        var response = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_fence,
            .content_type = "application/json",
            .body = body,
        });
        defer response.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 400), response.status);
        try expectContains(response.body, "invalid HA fence request");
    }
}

test "storage.ha http admin accepts whole instance identity" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "whole-instance-identity");
    defer paths.deinit(alloc);
    const identity = standby_mod.Identity{
        .cluster_id = 100,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{ .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();

    var typed_fence = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_fence,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":0,\"table_id\":0,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":0,\"force\":true,\"reason\":\"whole-instance\"}",
    });
    defer typed_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_fence.status);
    try expectContains(typed_fence.body, "\"action_kind\":\"fence_acquire\"");
    try expectContains(typed_fence.body, "\"identity\":{\"cluster_id\":100,\"shard_id\":0,\"table_id\":0");
    try expectContains(typed_fence.body, "\"forced\":true");

    var typed_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion_current_fence,
        .content_type = "application/json",
    });
    defer typed_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote.status);
    try expectContains(typed_promote.body, "\"action_kind\":\"promotion\"");
    try expectContains(typed_promote.body, "\"new_identity\":{\"cluster_id\":100,\"shard_id\":0,\"table_id\":0,\"timeline_id\":2");
    try expectContains(typed_promote.body, "\"node_id\":\"standby-a\"");
    try expectContains(typed_promote.body, "\"forced\":true");
    try std.testing.expectEqual(@as(u64, 2), standby.identity.timeline_id);
}

test "storage.ha http admin promotes from operation-specific fence request body" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "direct-promote-body");
    defer paths.deinit(alloc);
    const identity = standby_mod.Identity{
        .cluster_id = 100,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var server = Server.init(alloc, .{ .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();

    var typed_promote = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_promotion,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":0,\"table_id\":0,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":0,\"force\":true,\"reason\":\"direct-promote\"}",
    });
    defer typed_promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_promote.status);
    try expectContains(typed_promote.body, "\"action_kind\":\"promotion\"");
    try expectContains(typed_promote.body, "\"action_id\":\"promotion:standby-a\"");
    try expectContains(typed_promote.body, "\"node_id\":\"standby-a\"");
    try expectContains(typed_promote.body, "\"new_identity\":{\"cluster_id\":100,\"shard_id\":0,\"table_id\":0,\"timeline_id\":2");
    try expectContains(typed_promote.body, "\"forced\":true");
    try expectContains(typed_promote.body, "\"data_loss_possible\":true");
    try std.testing.expectEqual(@as(u64, 2), standby.identity.timeline_id);
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

    var server = Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
    });
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
    try expectContains(typed_begin.body, "\"action_kind\":\"base_backup_begin\"");
    try expectContains(typed_begin.body, "\"action_id\":\"base_backup_begin:base-http\"");
    try expectContains(typed_begin.body, "\"slot_name\":\"standby-seed\"");
    try expectContains(typed_begin.body, "\"manifest_id\":\"base-http\"");
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
    try expectContains(typed_finish.body, "\"action_kind\":\"base_backup_finish\"");
    try expectContains(typed_finish.body, "\"action_id\":\"base_backup_finish:base-http\"");
    try expectContains(typed_finish.body, "\"manifest_id\":\"base-http\"");
    try expectContains(typed_finish.body, "\"end_record_lsn\":3");

    const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const activate_body = try std.fmt.allocPrint(
        alloc,
        "{{\"slot_name\":\"standby-seed\",\"generation\":\"seed-standby-seed-1\",\"manifest_id\":\"base-http\",\"timeline_id\":1,\"checkpoint_lsn\":2,\"seed_receipt_sha256\":\"{s}\",\"manifest_sha256\":\"{s}\",\"aggregate_sha256\":\"{s}\"}}",
        .{ digest, digest, digest },
    );
    defer alloc.free(activate_body);
    var typed_activate = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_base_backups_activate,
        .content_type = "application/json",
        .body = activate_body,
    });
    defer typed_activate.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_activate.status);
    try expectContains(typed_activate.body, "\"action_kind\":\"seeded_slot_activate\"");
    try expectContains(typed_activate.body, "\"action_id\":\"seeded_slot_activate:seed-standby-seed-1\"");
    try expectContains(typed_activate.body, "\"target\":\"seed-standby-seed-1\"");
    try expectContains(typed_activate.body, "\"state\":\"applied\"");
    try expectContains(typed_activate.body, "\"checkpoint_lsn\":2");
    const activated = primary.slot("standby-seed") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(slot_store.SlotLifecycle.streaming, activated.lifecycle);
    try std.testing.expect(activated.active);

    var typed_activate_retry = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_base_backups_activate,
        .content_type = "application/json",
        .body = activate_body,
    });
    defer typed_activate_retry.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_activate_retry.status);
    try expectContains(typed_activate_retry.body, "\"state\":\"already_applied\"");

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
    try expectContains(typed_bootstrap.body, "\"action_kind\":\"standby_bootstrap\"");
    try expectContains(typed_bootstrap.body, "\"action_id\":\"standby_bootstrap:base-http\"");
    try expectContains(typed_bootstrap.body, "\"manifest_id\":\"base-http\"");
    try expectContains(typed_bootstrap.body, "\"checkpoint_lsn\":2");
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());
}

test "storage.ha http admin validates typed seed manifest paths before file access" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "seed-path-validation");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
    });
    defer server.deinit();

    const invalid_manifest_paths = [_]struct {
        path: []const u8,
        error_name: []const u8,
    }{
        .{ .path = "", .error_name = "ManifestPathMissing" },
        .{ .path = " /tmp/base.afha", .error_name = "ManifestPathInvalid" },
        .{ .path = "/tmp/base.afha ", .error_name = "ManifestPathInvalid" },
        .{ .path = "relative/base.afha", .error_name = "ManifestPathInvalid" },
        .{ .path = "/tmp/../base.afha", .error_name = "ManifestPathInvalid" },
        .{ .path = "/tmp//base.afha", .error_name = "ManifestPathInvalid" },
    };

    for (invalid_manifest_paths) |case| {
        const finish_body = try std.fmt.allocPrint(alloc, "{{\"manifest_path\":\"{s}\"}}", .{case.path});
        defer alloc.free(finish_body);
        var finish = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_base_backups_finish,
            .content_type = "application/json",
            .body = finish_body,
        });
        defer finish.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 400), finish.status);
        try expectContains(finish.body, case.error_name);

        const bootstrap_body = try std.fmt.allocPrint(
            alloc,
            "{{\"manifest_path\":\"{s}\",\"content_root\":\"/tmp/base\"}}",
            .{case.path},
        );
        defer alloc.free(bootstrap_body);
        var bootstrap = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_standby_bootstrap,
            .content_type = "application/json",
            .body = bootstrap_body,
        });
        defer bootstrap.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 400), bootstrap.status);
        try expectContains(bootstrap.body, case.error_name);
    }

    const valid_missing_manifest = "/tmp/antfly-ha-valid-missing.afha";
    const invalid_content_roots = [_]struct {
        root: []const u8,
        error_name: []const u8,
    }{
        .{ .root = "", .error_name = "ContentRootMissing" },
        .{ .root = " /tmp/base", .error_name = "ContentRootInvalid" },
        .{ .root = "/tmp/base ", .error_name = "ContentRootInvalid" },
        .{ .root = "relative/base", .error_name = "ContentRootInvalid" },
        .{ .root = "/tmp/../base", .error_name = "ContentRootInvalid" },
        .{ .root = "/tmp//base", .error_name = "ContentRootInvalid" },
    };

    for (invalid_content_roots) |case| {
        const bootstrap_body = try std.fmt.allocPrint(
            alloc,
            "{{\"manifest_path\":\"{s}\",\"content_root\":\"{s}\"}}",
            .{ valid_missing_manifest, case.root },
        );
        defer alloc.free(bootstrap_body);
        var bootstrap = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_standby_bootstrap,
            .content_type = "application/json",
            .body = bootstrap_body,
        });
        defer bootstrap.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 400), bootstrap.status);
        try expectContains(bootstrap.body, case.error_name);
    }
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

    var get_to_post_route = try server.handle(.{ .method = .GET, .uri = admin_api.routes.ha_fence });
    defer get_to_post_route.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), get_to_post_route.status);

    var post_to_get_route = try server.handle(.{ .method = .POST, .uri = admin_api.routes.ha_fence_current });
    defer post_to_get_route.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), post_to_get_route.status);

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

test "storage.ha http admin requires node id before typed action receipts" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "missing-node-id");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();

    var server = Server.init(alloc, .{ .primary = &primary });
    defer server.deinit();

    var response = try server.handle(.{
        .method = .POST,
        .uri = admin_api.routes.ha_replication_slots,
        .content_type = "application/json",
        .body = "{\"slot_name\":\"standby-a\",\"initial_lsn\":0}",
    });
    defer response.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 409), response.status);
    try expectContains(response.body, "PrimaryNodeIDUnavailable");
    try std.testing.expect(primary.slot("standby-a") == null);
}

test "storage.ha http admin rejects invalid primary node id before typed action receipts" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-primary-node-id");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();

    const invalid_ids = [_][]const u8{
        "",
        " primary-a",
        "primary-a ",
        "primary/a",
        "primary a",
    };

    for (invalid_ids) |node_id| {
        var server = Server.init(alloc, .{
            .primary = &primary,
            .primary_node_id = node_id,
        });
        defer server.deinit();

        var response = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_replication_slots,
            .content_type = "application/json",
            .body = "{\"slot_name\":\"standby-a\",\"initial_lsn\":0}",
        });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 409), response.status);
        try expectContains(response.body, "PrimaryNodeIDUnavailable");
        try std.testing.expect(primary.slot("standby-a") == null);
    }
}

test "storage.ha http admin rejects invalid fenced primary node id in typed write check" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-write-check-primary-node-id");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    const invalid_ids = [_][]const u8{
        "",
        " primary-a",
        "primary-a ",
        "primary/a",
        "primary a",
    };

    for (invalid_ids) |node_id| {
        var server = Server.init(alloc, .{
            .primary = &primary,
            .primary_node_id = node_id,
            .fence_store = &fence_store,
        });
        defer server.deinit();

        var response = try server.handle(.{
            .method = .POST,
            .uri = admin_api.routes.ha_write_check,
            .content_type = "application/json",
            .body = "{\"role\":\"primary\"}",
        });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 409), response.status);
        try expectContains(response.body, "PrimaryNodeIDUnavailable");
    }
}

test "storage.ha http admin rejects invalid standby node id in typed status" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "invalid-standby-node-id");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    const invalid_ids = [_][]const u8{
        "",
        " standby-a",
        "standby-a ",
        "standby/a",
        "standby a",
    };

    for (invalid_ids) |node_id| {
        var server = Server.init(alloc, .{
            .standby = &standby,
            .standby_node_id = node_id,
        });
        defer server.deinit();

        var response = try server.handle(.{
            .method = .GET,
            .uri = admin_api.routes.ha_standby_status,
        });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 409), response.status);
        try expectContains(response.body, "StandbyNodeIDUnavailable");
    }
}

test "storage.ha http admin returns method errors for generated admin routes" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    for (admin_api.openapi.server.routes) |route| {
        if (!std.mem.startsWith(u8, route.path, "/ha/")) continue;

        const path = try generatedRoutePathAlloc(alloc, route.path);
        defer alloc.free(path);

        const method = generatedRouteUnsupportedMethod(route.path) orelse return error.TestExpectedUnsupportedMethod;
        var response = try server.handle(.{ .method = method, .uri = path });
        defer response.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 405), response.status);
    }
}

test "storage.ha http admin handles every generated admin route method" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    for (admin_api.openapi.server.routes) |route| {
        if (!std.mem.startsWith(u8, route.path, "/ha/")) continue;

        const path = try generatedRoutePathAlloc(alloc, route.path);
        defer alloc.free(path);

        const method = try methodFromText(route.method);
        var response = try server.handle(.{ .method = method, .uri = path });
        defer response.deinit(alloc);

        if (response.status == 404 or response.status == 405) {
            std.debug.print(
                "generated admin OpenAPI HA route {s} {s} ({s}) was not dispatched by storage HA admin server\n",
                .{ route.method, route.path, route.operation_id },
            );
            return error.TestExpectedGeneratedRouteDispatched;
        }
    }
}

test "storage.ha http admin implemented admin routes are documented" {
    const alloc = std.testing.allocator;
    const ImplementedRoute = struct {
        method: []const u8,
        path: []const u8,
    };
    const static_routes = [_]ImplementedRoute{
        .{ .method = "GET", .path = admin_api.routes.ha_primary_status },
        .{ .method = "GET", .path = admin_api.routes.ha_standby_status },
        .{ .method = "GET", .path = admin_api.routes.ha_replication_slots },
        .{ .method = "GET", .path = admin_api.routes.ha_fence_current },
        .{ .method = "POST", .path = admin_api.routes.ha_replication_slots },
        .{ .method = "POST", .path = admin_api.routes.ha_commit_check },
        .{ .method = "POST", .path = admin_api.routes.ha_commit_append },
        .{ .method = "POST", .path = admin_api.routes.ha_read_check },
        .{ .method = "POST", .path = admin_api.routes.ha_write_check },
        .{ .method = "POST", .path = admin_api.routes.ha_owner_job_check },
        .{ .method = "POST", .path = admin_api.routes.ha_base_backups },
        .{ .method = "POST", .path = admin_api.routes.ha_base_backups_finish },
        .{ .method = "POST", .path = admin_api.routes.ha_standby_bootstrap },
        .{ .method = "POST", .path = admin_api.routes.ha_fence },
        .{ .method = "POST", .path = admin_api.routes.ha_promotion_assess },
        .{ .method = "POST", .path = admin_api.routes.ha_promotion_current_fence },
        .{ .method = "POST", .path = admin_api.routes.ha_promotion },
        .{ .method = "POST", .path = admin_api.routes.ha_rejoin_assess },
        .{ .method = "POST", .path = admin_api.routes.ha_rejoin_rewind },
        .{ .method = "POST", .path = admin_api.routes.ha_rejoin_reseed },
    };

    for (static_routes) |route| {
        try std.testing.expect(knownRoute(route.path));
        if (!implementedAdminRouteDocumented(route.path, route.method)) {
            std.debug.print(
                "implemented HA admin route {s} {s} is missing from generated admin OpenAPI routes\n",
                .{ route.method, route.path },
            );
            return error.TestExpectedImplementedAdminRouteDocumented;
        }
    }

    const slot_path = try admin_api.routes.replicationSlotPathAlloc(alloc, "standby-a");
    defer alloc.free(slot_path);
    const pause_path = try admin_api.routes.replicationSlotPausePathAlloc(alloc, "standby-a");
    defer alloc.free(pause_path);
    const resume_path = try admin_api.routes.replicationSlotResumePathAlloc(alloc, "standby-a");
    defer alloc.free(resume_path);

    const dynamic_routes = [_]ImplementedRoute{
        .{ .method = "DELETE", .path = slot_path },
        .{ .method = "PUT", .path = pause_path },
        .{ .method = "PUT", .path = resume_path },
    };
    const documented_dynamic_routes = [_]ImplementedRoute{
        .{ .method = "DELETE", .path = admin_api.routes.ha_replication_slot_prefix ++ "{slot_name}" },
        .{ .method = "PUT", .path = admin_api.routes.ha_replication_slot_prefix ++ "{slot_name}" ++ admin_api.routes.ha_replication_slot_pause_suffix },
        .{ .method = "PUT", .path = admin_api.routes.ha_replication_slot_prefix ++ "{slot_name}" ++ admin_api.routes.ha_replication_slot_resume_suffix },
    };

    for (dynamic_routes, documented_dynamic_routes) |route, documented| {
        try std.testing.expect(knownRoute(route.path));
        if (!implementedAdminRouteDocumented(documented.path, documented.method)) {
            std.debug.print(
                "implemented HA admin dynamic route {s} {s} is missing from generated admin OpenAPI routes\n",
                .{ route.method, route.path },
            );
            return error.TestExpectedImplementedAdminRouteDocumented;
        }
    }
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

test "storage.ha http admin decodes sync policy query values" {
    const alloc = std.testing.allocator;
    var sync = try buildSyncPolicyFromQuery(
        alloc,
        "sync_mode=remote-apply&sync_selection=first&sync_required=1&sync_failure=fail-closed&sync_standby=standby-a&sync_standby=standby.b%3Az",
    );
    defer sync.deinit(alloc);

    const policy = sync.policy orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_apply, policy.mode);
    try std.testing.expectEqual(primary_mod.StandbySelection.first, policy.selection);
    try std.testing.expectEqual(primary_mod.FailurePolicy.fail_closed, policy.failure_policy);
    try std.testing.expectEqual(@as(usize, 2), policy.standby_names.len);
    try std.testing.expectEqualStrings("standby-a", policy.standby_names[0]);
    try std.testing.expectEqualStrings("standby.b:z", policy.standby_names[1]);

    var all_sync = try buildSyncPolicyFromQuery(
        alloc,
        "sync_mode=remote-apply&sync_selection=all&sync_standby=standby-a&sync_standby=standby-b",
    );
    defer all_sync.deinit(alloc);
    const all_policy = all_sync.policy orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(primary_mod.StandbySelection.all, all_policy.selection);
    try std.testing.expectEqual(@as(usize, 2), all_policy.required);

    try std.testing.expectError(
        error.InvalidAdminRequest,
        buildSyncPolicyFromQuery(alloc, "sync_mode=remote-write&sync_standby=standby%XX"),
    );
    try std.testing.expectError(
        error.InvalidAdminRequest,
        buildSyncPolicyFromQuery(alloc, "sync_mode=remote-write&sync_standby=standby%20bad"),
    );
    try std.testing.expectError(
        error.InvalidAdminRequest,
        buildSyncPolicyFromQuery(alloc, "sync_mode=remote-apply&sync_selection=all&sync_required=1&sync_standby=standby-a"),
    );
    try std.testing.expectError(
        error.InvalidAdminRequest,
        buildSyncPolicyFromQuery(alloc, "sync_mode=remote-apply&sync_selection=all"),
    );
}

test "storage.ha http admin decodes OpenAPI ALL sync policy" {
    const all_policy = try syncPolicyFromOpenApi(.{
        .mode = "remote_apply",
        .selection = "all",
        .standby_names = &.{ "standby-a", "standby-b" },
    });
    try std.testing.expectEqual(primary_mod.StandbySelection.all, all_policy.selection);
    try std.testing.expectEqual(@as(usize, 2), all_policy.required);

    try std.testing.expectError(error.InvalidAdminRequest, syncPolicyFromOpenApi(.{
        .mode = "remote_apply",
        .selection = "all",
        .required = 1,
        .standby_names = &.{"standby-a"},
    }));
    try std.testing.expectError(error.InvalidAdminRequest, syncPolicyFromOpenApi(.{
        .mode = "remote_apply",
        .selection = "all",
        .standby_names = &.{},
    }));
    try std.testing.expectError(error.InvalidAdminRequest, syncPolicyFromOpenApi(.{
        .mode = "remote_apply",
        .selection = "any",
        .standby_names = &.{"standby bad"},
    }));
}

test "storage.ha http admin preserves omitted commit append shard and table defaults" {
    const implicit = try appendOptionsFromOpenApi(.{
        .payload = "implicit",
        .sync_policy = .{ .mode = "async" },
    });
    try std.testing.expect(implicit.shard_id == null);
    try std.testing.expect(implicit.table_id == null);

    const explicit = try appendOptionsFromOpenApi(.{
        .payload = "explicit",
        .shard_id = 0,
        .table_id = 20,
        .sync_policy = .{ .mode = "async" },
    });
    try std.testing.expectEqual(@as(?u64, 0), explicit.shard_id);
    try std.testing.expectEqual(@as(?u64, 20), explicit.table_id);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
