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

//! Client for the HA HTTP admin adapter.

const std = @import("std");
const Allocator = std.mem.Allocator;
const admin_api = @import("../../admin/mod.zig");
const http_common = @import("../../common/http/http_common.zig");
const routes = @import("../../raft/transport/routes.zig");
const backup_manifest = @import("backup_manifest.zig");
const fencing = @import("fencing.zig");
const http_admin = @import("http_admin.zig");
const primary_mod = @import("primary.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");

var test_path_counter: u64 = 0;

pub const RenderedOutput = struct {
    content_type: []u8,
    body: []u8,

    pub fn deinit(self: *RenderedOutput, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn ParsedOutput(comptime T: type) type {
    return struct {
        body: []u8,
        parsed: std.json.Parsed(T),

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            self.parsed.deinit();
            alloc.free(self.body);
            self.* = undefined;
        }
    };
}

pub const PrimaryStatusOptions = struct {
    max_lag_lsn: ?u64 = null,
    max_retained_bytes: ?u64 = null,
    max_retained_age_ns: ?u64 = null,
    sync_policy: ?primary_mod.SyncPolicy = null,
};

pub const AuthOptions = struct {
    bearer_token: ?[]const u8 = null,
};

pub const Client = struct {
    alloc: Allocator,
    executor: http_common.RequestExecutor,
    auth: AuthOptions = .{},

    pub fn init(alloc: Allocator, executor: http_common.RequestExecutor) Client {
        return initWithOptions(alloc, executor, .{});
    }

    pub fn initWithOptions(alloc: Allocator, executor: http_common.RequestExecutor, auth: AuthOptions) Client {
        return .{
            .alloc = alloc,
            .executor = executor,
            .auth = auth,
        };
    }

    pub fn checkHealth(self: *Client, base_uri: []const u8) !void {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.health);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{ .method = .GET, .uri = uri });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status);
    }

    pub fn checkReady(self: *Client, base_uri: []const u8) !void {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.ready);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{ .method = .GET, .uri = uri });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status);
    }

    pub fn executeCommand(self: *Client, base_uri: []const u8, argv: []const []const u8) !RenderedOutput {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.command);
        defer self.alloc.free(uri);

        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            struct { argv: []const []const u8 }{ .argv = argv },
            .{},
        );
        defer self.alloc.free(body);

        var resp = try self.executeWithRetry(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        errdefer resp.deinit(self.alloc);
        try mapStatus(resp.status);

        for (resp.headers) |*header| header.deinit(self.alloc);
        if (resp.headers.len > 0) self.alloc.free(resp.headers);

        return .{
            .content_type = resp.content_type orelse try self.alloc.dupe(u8, "application/octet-stream"),
            .body = resp.body,
        };
    }

    pub fn getPrimaryStatus(
        self: *Client,
        base_uri: []const u8,
        options: PrimaryStatusOptions,
    ) !ParsedOutput(admin_api.HAPrimaryStatusResponse) {
        var uri = try join(self.alloc, base_uri, admin_api.routes.ha_primary_status);
        defer self.alloc.free(uri);
        if (options.max_lag_lsn) |max_lag_lsn| {
            uri = try appendQueryU64(self.alloc, uri, "max_lag_lsn", max_lag_lsn);
        }
        if (options.max_retained_bytes) |max_retained_bytes| {
            uri = try appendQueryU64(self.alloc, uri, "max_retained_bytes", max_retained_bytes);
        }
        if (options.max_retained_age_ns) |max_retained_age_ns| {
            uri = try appendQueryU64(self.alloc, uri, "max_retained_age_ns", max_retained_age_ns);
        }
        if (options.sync_policy) |sync_policy| {
            uri = try appendQuerySyncPolicy(self.alloc, uri, sync_policy);
        }

        return try self.executeJson(admin_api.HAPrimaryStatusResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn getStandbyStatus(
        self: *Client,
        base_uri: []const u8,
        upstream_lsn: ?u64,
    ) !ParsedOutput(admin_api.HAStandbyStatusResponse) {
        var uri = try join(self.alloc, base_uri, admin_api.routes.ha_standby_status);
        defer self.alloc.free(uri);
        if (upstream_lsn) |lsn| {
            uri = try appendQueryU64(self.alloc, uri, "upstream_lsn", lsn);
        }

        return try self.executeJson(admin_api.HAStandbyStatusResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn checkCommit(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.CommitCheckRequest,
    ) !ParsedOutput(admin_api.HACommitCheckResponse) {
        var result = try self.postJson(
            admin_api.HACommitCheckResponse,
            base_uri,
            admin_api.routes.ha_commit_check,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateCommitCheckResponse(result.parsed.value);
        return result;
    }

    pub fn appendCommit(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.CommitAppendRequest,
    ) !ParsedOutput(admin_api.HACommitAppendResponse) {
        var result = try self.postJson(
            admin_api.HACommitAppendResponse,
            base_uri,
            admin_api.routes.ha_commit_append,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateCommitAppendResponse(result.parsed.value);
        return result;
    }

    pub fn checkRead(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.ReadCheckRequest,
    ) !ParsedOutput(admin_api.HAReadCheckResponse) {
        var result = try self.postJson(
            admin_api.HAReadCheckResponse,
            base_uri,
            admin_api.routes.ha_read_check,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateReadCheckResponse(result.parsed.value);
        return result;
    }

    pub fn checkWrite(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.WriteCheckRequest,
    ) !ParsedOutput(admin_api.HAWriteCheckResponse) {
        var result = try self.postJson(
            admin_api.HAWriteCheckResponse,
            base_uri,
            admin_api.routes.ha_write_check,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateWriteCheckResponse(result.parsed.value);
        return result;
    }

    pub fn checkOwnerJob(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.OwnerJobCheckRequest,
    ) !ParsedOutput(admin_api.HAOwnerJobCheckResponse) {
        var result = try self.postJson(
            admin_api.HAOwnerJobCheckResponse,
            base_uri,
            admin_api.routes.ha_owner_job_check,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateOwnerJobCheckResponse(result.parsed.value);
        return result;
    }

    pub fn listReplicationSlots(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.HAReplicationSlotListResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_replication_slots);
        defer self.alloc.free(uri);
        var result = try self.executeJson(admin_api.HAReplicationSlotListResponse, .{
            .method = .GET,
            .uri = uri,
        });
        errdefer result.deinit(self.alloc);
        try validateReplicationSlotListResponse(result.parsed.value);
        return result;
    }

    pub fn createReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        initial_lsn: ?u64,
    ) !ParsedOutput(admin_api.HAReplicationSlotActionResponse) {
        try validateClientSlotName(slot_name);
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_replication_slots);
        defer self.alloc.free(uri);
        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            admin_api.ReplicationSlotCreateRequest{
                .slot_name = slot_name,
                .initial_lsn = if (initial_lsn) |lsn| try i64FromU64(lsn) else null,
            },
            .{},
        );
        defer self.alloc.free(body);

        var result = try self.executeJson(admin_api.HAReplicationSlotActionResponse, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        errdefer result.deinit(self.alloc);
        try validateReplicationSlotActionResponse(result.parsed.value, "create", slot_name);
        return result;
    }

    pub fn pauseReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .pause);
    }

    pub fn resumeReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .@"resume");
    }

    pub fn dropReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .drop);
    }

    pub fn beginBaseBackup(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.BaseBackupStartRequest,
    ) !ParsedOutput(admin_api.HABaseBackupBeginResponse) {
        try validateClientSlotName(request.slot_name);
        var result = try self.postJson(
            admin_api.HABaseBackupBeginResponse,
            base_uri,
            admin_api.routes.ha_base_backups,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateBaseBackupBeginResponse(result.parsed.value, request);
        return result;
    }

    pub fn finishBaseBackup(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.BaseBackupManifestPathRequest,
    ) !ParsedOutput(admin_api.HABaseBackupFinishResponse) {
        var result = try self.postJson(
            admin_api.HABaseBackupFinishResponse,
            base_uri,
            admin_api.routes.ha_base_backups_finish,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateBaseBackupFinishResponse(result.parsed.value);
        return result;
    }

    pub fn bootstrapStandby(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.StandbyBootstrapRequest,
    ) !ParsedOutput(admin_api.HAStandbyBootstrapResponse) {
        var result = try self.postJson(
            admin_api.HAStandbyBootstrapResponse,
            base_uri,
            admin_api.routes.ha_standby_bootstrap,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateStandbyBootstrapResponse(result.parsed.value);
        return result;
    }

    pub fn acquireFence(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.FenceAcquireRequest,
    ) !ParsedOutput(admin_api.HAFenceResponse) {
        var result = try self.postJson(
            admin_api.HAFenceResponse,
            base_uri,
            admin_api.routes.ha_fence,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateFenceResponse(result.parsed.value, request);
        return result;
    }

    pub fn currentFence(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.HACurrentFenceResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_fence_current);
        defer self.alloc.free(uri);
        var result = try self.executeJson(admin_api.HACurrentFenceResponse, .{
            .method = .GET,
            .uri = uri,
        });
        errdefer result.deinit(self.alloc);
        try validateCurrentFenceResponse(result.parsed.value);
        return result;
    }

    pub fn assessPromotion(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.PromotionAssessRequest,
    ) !ParsedOutput(admin_api.HAPromotionAssessResponse) {
        var result = try self.postJson(
            admin_api.HAPromotionAssessResponse,
            base_uri,
            admin_api.routes.ha_promotion_assess,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validatePromotionAssessResponse(result.parsed.value, request);
        return result;
    }

    pub fn promoteWithCurrentFence(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.HAPromotionResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_promotion_current_fence);
        defer self.alloc.free(uri);
        var result = try self.executeJson(admin_api.HAPromotionResponse, .{
            .method = .POST,
            .uri = uri,
        });
        errdefer result.deinit(self.alloc);
        try validatePromotionResponse(result.parsed.value, null);
        return result;
    }

    pub fn promote(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.FenceAcquireRequest,
    ) !ParsedOutput(admin_api.HAPromotionResponse) {
        var result = try self.postJson(
            admin_api.HAPromotionResponse,
            base_uri,
            admin_api.routes.ha_promotion,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validatePromotionResponse(result.parsed.value, request);
        return result;
    }

    pub fn assessRejoin(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.RejoinAssessRequest,
    ) !ParsedOutput(admin_api.HARejoinAssessResponse) {
        var result = try self.postJson(
            admin_api.HARejoinAssessResponse,
            base_uri,
            admin_api.routes.ha_rejoin_assess,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateRejoinResponse(result.parsed.value, request, .assess);
        return result;
    }

    pub fn rewindRejoin(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.RejoinAssessRequest,
    ) !ParsedOutput(admin_api.HARejoinAssessResponse) {
        var result = try self.postJson(
            admin_api.HARejoinAssessResponse,
            base_uri,
            admin_api.routes.ha_rejoin_rewind,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateRejoinResponse(result.parsed.value, request, .rewind);
        return result;
    }

    pub fn reseedRejoin(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.RejoinAssessRequest,
    ) !ParsedOutput(admin_api.HARejoinAssessResponse) {
        var result = try self.postJson(
            admin_api.HARejoinAssessResponse,
            base_uri,
            admin_api.routes.ha_rejoin_reseed,
            request,
        );
        errdefer result.deinit(self.alloc);
        try validateRejoinResponse(result.parsed.value, request, .reseed);
        return result;
    }

    fn replicationSlotLifecycle(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        action: enum { pause, @"resume", drop },
    ) !ParsedOutput(admin_api.HAReplicationSlotActionResponse) {
        try validateClientSlotName(slot_name);
        const path = switch (action) {
            .pause => try admin_api.routes.replicationSlotPausePathAlloc(self.alloc, slot_name),
            .@"resume" => try admin_api.routes.replicationSlotResumePathAlloc(self.alloc, slot_name),
            .drop => try admin_api.routes.replicationSlotPathAlloc(self.alloc, slot_name),
        };
        defer self.alloc.free(path);
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var result = try self.executeJson(admin_api.HAReplicationSlotActionResponse, .{
            .method = switch (action) {
                .drop => .DELETE,
                .pause, .@"resume" => .PUT,
            },
            .uri = uri,
        });
        errdefer result.deinit(self.alloc);
        try validateReplicationSlotActionResponse(result.parsed.value, switch (action) {
            .pause => "pause",
            .@"resume" => "resume",
            .drop => "drop",
        }, slot_name);
        return result;
    }

    fn executeJson(
        self: *Client,
        comptime T: type,
        req: http_common.HttpRequest,
    ) !ParsedOutput(T) {
        var resp = try self.executeWithRetry(req);
        errdefer resp.deinit(self.alloc);
        try mapStatus(resp.status);

        if (resp.content_type) |content_type| self.alloc.free(content_type);
        resp.content_type = null;
        for (resp.headers) |*header| header.deinit(self.alloc);
        if (resp.headers.len > 0) self.alloc.free(resp.headers);
        resp.headers = &.{};

        const body = resp.body;
        resp.body = &.{};
        errdefer self.alloc.free(body);
        const parsed = try std.json.parseFromSlice(
            T,
            self.alloc,
            body,
            .{ .ignore_unknown_fields = true },
        );
        return .{
            .body = body,
            .parsed = parsed,
        };
    }

    fn postJson(
        self: *Client,
        comptime T: type,
        base_uri: []const u8,
        path: []const u8,
        value: anytype,
    ) !ParsedOutput(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);
        const body = try std.json.Stringify.valueAlloc(self.alloc, value, .{});
        defer self.alloc.free(body);
        return try self.executeJson(T, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
    }

    fn executeWithRetry(self: *Client, raw_req: http_common.HttpRequest) !http_common.HttpResponse {
        var req = raw_req;
        var authorization: ?[]u8 = null;
        defer if (authorization) |value| self.alloc.free(value);
        if (self.auth.bearer_token) |raw_token| {
            const token = std.mem.trim(u8, raw_token, " \t\r\n");
            if (token.len > 0) {
                authorization = try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token});
                req.authorization = authorization.?;
            }
        }

        var attempt: usize = 0;
        while (true) {
            return self.executor.execute(self.alloc, req) catch |err| switch (err) {
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.ConnectionRefused,
                error.BrokenPipe,
                error.EndOfStream,
                => {
                    if (attempt >= 1) return err;
                    attempt += 1;
                    continue;
                },
                else => return err,
            };
        }
    }

    fn mapStatus(status: u16) !void {
        if (status >= 200 and status < 300) return;
        if (status == 400) return error.InvalidHaCommand;
        if (status == 401) return error.HaAdminUnauthorized;
        if (status == 404) return error.HaEndpointNotFound;
        if (status == 405) return error.UnsupportedOperation;
        if (status == 409) return error.HaCommandConflict;
        if (status == 503) return error.HaEndpointNotReady;
        return error.UnexpectedHttpStatus;
    }
};

fn join(alloc: Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    if (!validation.isHTTPURLWithHostNoHiddenWhitespace(base_uri)) return error.InvalidHAAdminURL;
    return try routes.Routes.join(alloc, base_uri, path);
}

fn validateClientSlotName(slot_name: []const u8) !void {
    if (!validation.isIdentifier(slot_name)) return error.InvalidSlotName;
}

fn appendQueryU64(alloc: Allocator, old_uri: []u8, key: []const u8, value: u64) ![]u8 {
    const separator: []const u8 = if (std.mem.indexOfScalar(u8, old_uri, '?') == null) "?" else "&";
    const next = try std.fmt.allocPrint(alloc, "{s}{s}{s}={d}", .{ old_uri, separator, key, value });
    alloc.free(old_uri);
    return next;
}

fn appendQueryString(alloc: Allocator, old_uri: []u8, key: []const u8, value: []const u8) ![]u8 {
    const separator: []const u8 = if (std.mem.indexOfScalar(u8, old_uri, '?') == null) "?" else "&";
    const encoded = try percentEncodeQueryValueAlloc(alloc, value);
    defer alloc.free(encoded);
    const next = try std.fmt.allocPrint(alloc, "{s}{s}{s}={s}", .{ old_uri, separator, key, encoded });
    alloc.free(old_uri);
    return next;
}

fn appendQuerySyncPolicy(alloc: Allocator, old_uri: []u8, policy: primary_mod.SyncPolicy) ![]u8 {
    var uri = old_uri;
    errdefer alloc.free(uri);

    uri = try appendQueryString(alloc, uri, "sync_mode", durabilityModeQueryName(policy.mode));
    uri = try appendQueryString(alloc, uri, "sync_selection", @tagName(policy.selection));
    if (policy.selection != .all) {
        uri = try appendQueryU64(alloc, uri, "sync_required", policy.required);
    }
    uri = try appendQueryString(alloc, uri, "sync_failure", failurePolicyQueryName(policy.failure_policy));
    for (policy.standby_names) |name| {
        uri = try appendQueryString(alloc, uri, "sync_standby", name);
    }
    return uri;
}

fn durabilityModeQueryName(mode: primary_mod.DurabilityMode) []const u8 {
    return switch (mode) {
        .async => "async",
        .remote_write => "remote-write",
        .remote_apply => "remote-apply",
    };
}

fn failurePolicyQueryName(policy: primary_mod.FailurePolicy) []const u8 {
    return switch (policy) {
        .block => "block",
        .fail_closed => "fail-closed",
        .degrade_to_async => "degrade-to-async",
    };
}

fn percentEncodeQueryValueAlloc(alloc: Allocator, raw: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (raw) |byte| {
        if (isQueryValueUnreserved(byte)) {
            try out.append(alloc, byte);
        } else {
            var buf: [3]u8 = undefined;
            const encoded = try std.fmt.bufPrint(&buf, "%{X:0>2}", .{byte});
            try out.appendSlice(alloc, encoded);
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn isQueryValueUnreserved(byte: u8) bool {
    return (byte >= 'A' and byte <= 'Z') or
        (byte >= 'a' and byte <= 'z') or
        (byte >= '0' and byte <= '9') or
        byte == '-' or byte == '.' or byte == '_' or byte == '~';
}

fn i64FromU64(value: u64) !i64 {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidHaCommand;
    return @intCast(value);
}

fn validateSchemaVersion(version: i64, err: anyerror) !void {
    if (version <= 0) return err;
}

fn validateActionReceipt(
    receipt: admin_api.HAActionReceipt,
    expected_kind: []const u8,
    expected_state: []const u8,
    expected_target: ?[]const u8,
    err: anyerror,
) !void {
    if (receipt.action_id.len == 0) return err;
    if (!std.mem.eql(u8, receipt.action_kind, expected_kind)) return err;
    if (!receiptStateMatches(receipt.state, expected_state)) return err;
    if (receipt.target.len == 0 or !validation.isIdentifier(receipt.node_id)) return err;
    if (expected_target) |target| {
        if (!std.mem.eql(u8, receipt.target, target)) return err;
        if (!std.mem.startsWith(u8, receipt.action_id, expected_kind)) return err;
        if (receipt.action_id.len != expected_kind.len + 1 + target.len) return err;
        if (receipt.action_id[expected_kind.len] != ':') return err;
        if (!std.mem.eql(u8, receipt.action_id[expected_kind.len + 1 ..], target)) return err;
    }
}

fn receiptStateMatches(actual: []const u8, expected: []const u8) bool {
    return std.mem.eql(u8, actual, expected) or
        (std.mem.eql(u8, expected, "applied") and std.mem.eql(u8, actual, "already_applied"));
}

fn validateIdentity(identity: admin_api.HAIdentity, err: anyerror) !void {
    if (identity.cluster_id <= 0 or identity.timeline_id <= 0 or identity.epoch <= 0) return err;
    if (identity.shard_id < 0 or identity.table_id < 0) return err;
}

fn validateReplicationSlot(slot: admin_api.HAReplicationSlot) !void {
    if (!validation.isIdentifier(slot.slot_name)) return error.AdminReplicationSlotResponseMismatch;
    if (slot.timeline_id <= 0) return error.AdminReplicationSlotResponseMismatch;
    if (slot.restart_lsn < 0 or slot.received_lsn < 0 or slot.applied_lsn < 0 or slot.safe_read_lsn < 0 or slot.current_lsn < 0) {
        return error.AdminReplicationSlotResponseMismatch;
    }
}

fn validateReplicationSlotActionResponse(
    response: admin_api.HAReplicationSlotActionResponse,
    expected_action: []const u8,
    expected_slot: []const u8,
) !void {
    try validateSchemaVersion(response.schema_version, error.AdminReplicationSlotResponseMismatch);
    if (!std.mem.eql(u8, response.slot_action, expected_action)) return error.AdminReplicationSlotResponseMismatch;
    try validateReplicationSlot(response.slot);
    if (!std.mem.eql(u8, response.slot.slot_name, expected_slot)) return error.AdminReplicationSlotResponseMismatch;
    const expected_kind = if (std.mem.eql(u8, expected_action, "create"))
        "replication_slot_create"
    else if (std.mem.eql(u8, expected_action, "pause"))
        "replication_slot_pause"
    else if (std.mem.eql(u8, expected_action, "resume"))
        "replication_slot_resume"
    else if (std.mem.eql(u8, expected_action, "drop"))
        "replication_slot_drop"
    else
        return error.AdminReplicationSlotResponseMismatch;
    try validateActionReceipt(response.action, expected_kind, "applied", expected_slot, error.AdminReplicationSlotResponseMismatch);
}

fn validateReplicationSlotListResponse(response: admin_api.HAReplicationSlotListResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminReplicationSlotResponseMismatch);
    for (response.slots) |slot| try validateReplicationSlot(slot);
}

fn validateBaseBackupBeginResponse(response: admin_api.HABaseBackupBeginResponse, request: admin_api.BaseBackupStartRequest) !void {
    try validateSchemaVersion(response.schema_version, error.AdminSeedResponseMismatch);
    try validateActionReceipt(response.action, "base_backup_begin", "applied", response.manifest_id, error.AdminSeedResponseMismatch);
    if (!std.mem.eql(u8, response.slot_name, request.slot_name)) return error.AdminSeedResponseMismatch;
    if (!std.mem.eql(u8, response.manifest_id, request.manifest_id)) return error.AdminSeedResponseMismatch;
    if (response.backup_lsn <= 0 or response.start_record_lsn <= 0) return error.AdminSeedResponseMismatch;
}

fn validateBaseBackupFinishResponse(response: admin_api.HABaseBackupFinishResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminSeedResponseMismatch);
    try validateActionReceipt(response.action, "base_backup_finish", "applied", response.manifest_id, error.AdminSeedResponseMismatch);
    if (response.manifest_id.len == 0) return error.AdminSeedResponseMismatch;
    if (response.backup_lsn <= 0 or response.end_record_lsn <= 0) return error.AdminSeedResponseMismatch;
}

fn validateStandbyBootstrapResponse(response: admin_api.HAStandbyBootstrapResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminSeedResponseMismatch);
    try validateActionReceipt(response.action, "standby_bootstrap", "applied", response.manifest_id, error.AdminSeedResponseMismatch);
    if (response.manifest_id.len == 0) return error.AdminSeedResponseMismatch;
    if (response.backup_lsn <= 0 or response.checkpoint_lsn <= 0) return error.AdminSeedResponseMismatch;
}

fn validateFenceReceipt(receipt: admin_api.HAFenceReceipt) !void {
    try validateIdentity(receipt.identity, error.AdminFenceResponseMismatch);
    if (!validation.isIdentifier(receipt.old_primary_id) or !validation.isIdentifier(receipt.promoted_node_id) or receipt.token.len == 0) {
        return error.AdminFenceResponseMismatch;
    }
    if (std.mem.eql(u8, receipt.old_primary_id, receipt.promoted_node_id)) return error.AdminFenceResponseMismatch;
    if (receipt.parent_timeline_id <= 0 or receipt.parent_epoch <= 0 or receipt.new_timeline_id <= 0 or receipt.new_epoch <= 0) {
        return error.AdminFenceResponseMismatch;
    }
    if (receipt.required_lsn <= 0 or receipt.observed_lsn < 0 or receipt.generation <= 0) return error.AdminFenceResponseMismatch;
    if (receipt.identity.timeline_id != receipt.new_timeline_id or receipt.identity.epoch != receipt.new_epoch) {
        return error.AdminFenceResponseMismatch;
    }
    if (receipt.new_timeline_id <= receipt.parent_timeline_id or receipt.new_epoch <= receipt.parent_epoch) {
        return error.AdminFenceResponseMismatch;
    }
    if (!receipt.forced and receipt.observed_lsn < receipt.required_lsn) return error.AdminFenceResponseMismatch;
}

fn validateFenceResponse(response: admin_api.HAFenceResponse, request: admin_api.FenceAcquireRequest) !void {
    try validateSchemaVersion(response.schema_version, error.AdminFenceResponseMismatch);
    try validateActionReceipt(response.action, "fence_acquire", "applied", request.promoted_node_id, error.AdminFenceResponseMismatch);
    try validateFenceReceipt(response.receipt);
    if (response.receipt.identity.cluster_id != request.identity.cluster_id or
        response.receipt.identity.shard_id != request.identity.shard_id or
        response.receipt.identity.table_id != request.identity.table_id or
        response.receipt.identity.timeline_id != request.new_timeline_id or
        response.receipt.identity.epoch != request.new_epoch or
        response.receipt.parent_timeline_id != request.identity.timeline_id or
        response.receipt.parent_epoch != request.identity.epoch or
        response.receipt.generation != request.generation)
    {
        return error.AdminFenceResponseMismatch;
    }
    if (!std.mem.eql(u8, response.receipt.promoted_node_id, request.promoted_node_id)) return error.AdminFenceResponseMismatch;
    if (!std.mem.eql(u8, response.receipt.old_primary_id, request.old_primary_id)) return error.AdminFenceResponseMismatch;
    if (response.receipt.new_timeline_id != request.new_timeline_id or response.receipt.new_epoch != request.new_epoch) return error.AdminFenceResponseMismatch;
    if (response.receipt.required_lsn < request.required_lsn or
        response.receipt.observed_lsn < request.observed_lsn or
        response.receipt.forced != request.force or
        (!response.receipt.forced and response.receipt.observed_lsn < response.receipt.required_lsn))
    {
        return error.AdminFenceResponseMismatch;
    }
}

fn validateCurrentFenceResponse(response: admin_api.HACurrentFenceResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminFenceResponseMismatch);
    if (response.held) {
        const receipt = response.receipt orelse return error.AdminFenceResponseMismatch;
        try validateFenceReceipt(receipt);
    } else if (response.receipt != null) {
        return error.AdminFenceResponseMismatch;
    }
}

fn validateCommitGate(gate: admin_api.HACommitGate) !void {
    if (!isCommitGateAction(gate.action)) return error.AdminGateResponseMismatch;
    if (gate.target_lsn < 0) return error.AdminGateResponseMismatch;
    try validateDurabilityDecision(gate.durability);
}

fn validateDurabilityDecision(decision: admin_api.HADurabilityDecision) !void {
    if (!isDurabilityStatus(decision.status) or !isDurabilityMode(decision.mode) or !isDurabilitySelection(decision.selection)) {
        return error.AdminGateResponseMismatch;
    }
    if (decision.target_lsn < 0 or decision.progress_lsn < 0 or decision.missing_lsn_count < 0 or
        decision.satisfied_count < 0 or decision.required_count < 0 or decision.candidate_count < 0)
    {
        return error.AdminGateResponseMismatch;
    }
}

fn validateCommitCheckResponse(response: admin_api.HACommitCheckResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminGateResponseMismatch);
    try validateCommitGate(response.gate);
}

fn validateCommitAppendResponse(response: admin_api.HACommitAppendResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminGateResponseMismatch);
    if (response.lsn < 0) return error.AdminGateResponseMismatch;
    try validateCommitGate(response.gate);
}

fn validateReadCheckResponse(response: admin_api.HAReadCheckResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminGateResponseMismatch);
    const decision = response.decision;
    if (!isReadDecisionAction(decision.action) or !isReadConsistency(decision.consistency)) return error.AdminGateResponseMismatch;
    if (decision.received_lsn < 0 or decision.applied_lsn < 0 or decision.safe_read_lsn < 0 or decision.missing_lsn_count < 0 or decision.metadata_missing_lsn_count < 0) {
        return error.AdminGateResponseMismatch;
    }
}

fn validateWriteCheckResponse(response: admin_api.HAWriteCheckResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminGateResponseMismatch);
    const decision = response.decision;
    if (!isWriteRole(decision.role) or !isWriteAction(decision.action)) return error.AdminGateResponseMismatch;
    try validateIdentity(decision.identity, error.AdminGateResponseMismatch);
    if (decision.durable_lsn < 0 or decision.next_lsn < 0) return error.AdminGateResponseMismatch;
    if (decision.promotion_handoff) |handoff| try validatePromotionHandoff(handoff);
}

fn validateOwnerJobCheckResponse(response: admin_api.HAOwnerJobCheckResponse) !void {
    try validateSchemaVersion(response.schema_version, error.AdminGateResponseMismatch);
    const decision = response.decision;
    if (!isOwnerJobKind(decision.kind) or !isOwnerJobRole(decision.role) or !isOwnerJobAction(decision.action)) {
        return error.AdminGateResponseMismatch;
    }
    try validateIdentity(decision.identity, error.AdminGateResponseMismatch);
    if (decision.durable_lsn < 0 or decision.next_lsn < 0) return error.AdminGateResponseMismatch;
    if (decision.promotion_handoff) |handoff| try validatePromotionHandoff(handoff);
}

fn validatePromotionAssessResponse(response: admin_api.HAPromotionAssessResponse, request: admin_api.PromotionAssessRequest) !void {
    try validateSchemaVersion(response.schema_version, error.AdminPromotionResponseMismatch);
    try validateActionReceipt(response.action, "promotion_assess", "assessed", null, error.AdminPromotionResponseMismatch);
    if (!validation.isIdentifier(response.action.target) or !std.mem.eql(u8, response.action.target, response.action.node_id)) {
        return error.AdminPromotionResponseMismatch;
    }
    if (response.assessment.required_lsn < 0 or response.assessment.received_lsn < 0 or response.assessment.applied_lsn < 0) {
        return error.AdminPromotionResponseMismatch;
    }
    if (request.required_lsn) |required_lsn| {
        if (response.assessment.required_lsn != required_lsn) return error.AdminPromotionResponseMismatch;
    }
    if (response.assessment.force != request.force) return error.AdminPromotionResponseMismatch;
    const expected_fencing_confirmed = request.fencing_confirmed or request.use_current_fence;
    if (response.assessment.fencing_confirmed != expected_fencing_confirmed) return error.AdminPromotionResponseMismatch;
}

fn validatePromotionHandoff(handoff: admin_api.HAPromotionHandoff) !void {
    try validateIdentity(handoff.identity, error.AdminGateResponseMismatch);
    if (handoff.switch_lsn < 0 or handoff.next_lsn < 0) return error.AdminGateResponseMismatch;
}

fn isCommitGateAction(value: []const u8) bool {
    return std.mem.eql(u8, value, "acknowledge") or
        std.mem.eql(u8, value, "wait_for_standby") or
        std.mem.eql(u8, value, "reject") or
        std.mem.eql(u8, value, "acknowledge_degraded");
}

fn isDurabilityStatus(value: []const u8) bool {
    return std.mem.eql(u8, value, "satisfied") or
        std.mem.eql(u8, value, "would_block") or
        std.mem.eql(u8, value, "fail_closed") or
        std.mem.eql(u8, value, "degraded_to_async");
}

fn isDurabilityMode(value: []const u8) bool {
    return std.mem.eql(u8, value, "async") or
        std.mem.eql(u8, value, "remote_write") or
        std.mem.eql(u8, value, "remote_apply");
}

fn isDurabilitySelection(value: []const u8) bool {
    return std.mem.eql(u8, value, "any") or
        std.mem.eql(u8, value, "first") or
        std.mem.eql(u8, value, "all");
}

fn isReadDecisionAction(value: []const u8) bool {
    return std.mem.eql(u8, value, "serve_standby") or
        std.mem.eql(u8, value, "wait_for_apply") or
        std.mem.eql(u8, value, "wait_for_metadata") or
        std.mem.eql(u8, value, "route_to_primary");
}

fn isReadConsistency(value: []const u8) bool {
    return std.mem.eql(u8, value, "stale_ok") or
        std.mem.eql(u8, value, "at_least_lsn") or
        std.mem.eql(u8, value, "primary");
}

fn isWriteRole(value: []const u8) bool {
    return std.mem.eql(u8, value, "primary") or
        std.mem.eql(u8, value, "standby") or
        std.mem.eql(u8, value, "promoted_standby") or
        std.mem.eql(u8, value, "fenced_primary");
}

fn isWriteAction(value: []const u8) bool {
    return std.mem.eql(u8, value, "allow_write") or
        std.mem.eql(u8, value, "reject_read_only_standby") or
        std.mem.eql(u8, value, "open_promoted_primary") or
        std.mem.eql(u8, value, "reject_fenced_primary");
}

fn isOwnerJobKind(value: []const u8) bool {
    return std.mem.eql(u8, value, "compaction_publish") or
        std.mem.eql(u8, value, "derived_effect_writer") or
        std.mem.eql(u8, value, "enrichment_writer") or
        std.mem.eql(u8, value, "retention_advance");
}

fn isOwnerJobRole(value: []const u8) bool {
    return std.mem.eql(u8, value, "primary") or
        std.mem.eql(u8, value, "standby") or
        std.mem.eql(u8, value, "promoted_standby");
}

fn isOwnerJobAction(value: []const u8) bool {
    return std.mem.eql(u8, value, "run") or
        std.mem.eql(u8, value, "disable_on_standby") or
        std.mem.eql(u8, value, "open_promoted_primary");
}

fn validatePromotionResponse(
    response: admin_api.HAPromotionResponse,
    request: ?admin_api.FenceAcquireRequest,
) !void {
    if (response.schema_version <= 0) return error.AdminPromotionResponseMismatch;
    if (!validation.isIdentifier(response.promotion.node_id)) return error.AdminPromotionResponseMismatch;
    try validateActionReceipt(response.action, "promotion", "applied", response.promotion.node_id, error.AdminPromotionResponseMismatch);
    if (!std.mem.eql(u8, response.action.target, response.promotion.node_id)) return error.AdminPromotionResponseMismatch;
    if (!std.mem.eql(u8, response.action.node_id, response.promotion.node_id)) return error.AdminPromotionResponseMismatch;

    const promotion = response.promotion;
    const assessment = response.assessment;
    if (assessment.required_lsn <= 0 or assessment.received_lsn < 0 or assessment.applied_lsn < 0) return error.AdminPromotionResponseMismatch;
    if (promotion.switch_lsn <= 0 or promotion.switch_lsn != assessment.received_lsn + 1) return error.AdminPromotionResponseMismatch;
    if (assessment.has_required_lsn != (assessment.received_lsn >= assessment.required_lsn)) return error.AdminPromotionResponseMismatch;
    if (assessment.caught_up_to_received != (assessment.applied_lsn >= assessment.received_lsn)) return error.AdminPromotionResponseMismatch;
    const data_loss_possible = !assessment.has_required_lsn or !assessment.caught_up_to_received or assessment.applied_lsn < assessment.required_lsn;
    if (assessment.data_loss_possible != data_loss_possible) return error.AdminPromotionResponseMismatch;
    if (assessment.safe != (assessment.fencing_confirmed and !assessment.data_loss_possible)) return error.AdminPromotionResponseMismatch;
    if (assessment.requires_fencing != (!assessment.fencing_confirmed and !assessment.force)) return error.AdminPromotionResponseMismatch;
    if (assessment.requires_force != (assessment.data_loss_possible and !assessment.force)) return error.AdminPromotionResponseMismatch;
    if (assessment.can_promote != (!assessment.requires_fencing and (!assessment.requires_force or assessment.force))) return error.AdminPromotionResponseMismatch;
    if (!std.mem.eql(u8, assessment.mode, expectedPromotionMode(assessment))) return error.AdminPromotionResponseMismatch;
    if (!response.assessment.fencing_confirmed) return error.AdminPromotionResponseMismatch;
    if (!response.assessment.can_promote) return error.AdminPromotionResponseMismatch;
    if (response.forced != promotion.forced) return error.AdminPromotionResponseMismatch;
    if (response.assessment.force != response.forced) return error.AdminPromotionResponseMismatch;
    if (promotion.data_loss_possible != response.assessment.data_loss_possible) return error.AdminPromotionResponseMismatch;
    if (promotion.old_identity.cluster_id != promotion.new_identity.cluster_id) return error.AdminPromotionResponseMismatch;
    if (promotion.old_identity.shard_id != promotion.new_identity.shard_id) return error.AdminPromotionResponseMismatch;
    if (promotion.old_identity.table_id != promotion.new_identity.table_id) return error.AdminPromotionResponseMismatch;
    if (promotion.new_identity.timeline_id <= promotion.old_identity.timeline_id) return error.AdminPromotionResponseMismatch;
    if (promotion.new_identity.epoch <= promotion.old_identity.epoch) return error.AdminPromotionResponseMismatch;
    if (response.fence_generation <= 0 or response.fence_token.len == 0) return error.AdminPromotionResponseMismatch;

    if (request) |expected| {
        if (!std.mem.eql(u8, promotion.node_id, expected.promoted_node_id)) return error.AdminPromotionResponseMismatch;
        if (!std.mem.eql(u8, response.action.target, expected.promoted_node_id)) return error.AdminPromotionResponseMismatch;
        if (promotion.old_identity.cluster_id != expected.identity.cluster_id) return error.AdminPromotionResponseMismatch;
        if (promotion.old_identity.shard_id != expected.identity.shard_id) return error.AdminPromotionResponseMismatch;
        if (promotion.old_identity.table_id != expected.identity.table_id) return error.AdminPromotionResponseMismatch;
        if (promotion.old_identity.timeline_id != expected.identity.timeline_id) return error.AdminPromotionResponseMismatch;
        if (promotion.old_identity.epoch != expected.identity.epoch) return error.AdminPromotionResponseMismatch;
        if (promotion.new_identity.timeline_id != expected.new_timeline_id) return error.AdminPromotionResponseMismatch;
        if (promotion.new_identity.epoch != expected.new_epoch) return error.AdminPromotionResponseMismatch;
        if (response.assessment.required_lsn != expected.required_lsn) return error.AdminPromotionResponseMismatch;
        if (response.assessment.received_lsn < expected.observed_lsn) return error.AdminPromotionResponseMismatch;
        if (response.forced != expected.force) return error.AdminPromotionResponseMismatch;
    }
}

fn expectedPromotionMode(assessment: admin_api.HAPromotionAssessment) []const u8 {
    if (!assessment.can_promote) return "blocked";
    if (assessment.data_loss_possible) return "lossy";
    if (assessment.force) return "forced";
    return "safe";
}

const RejoinResponseKind = enum {
    assess,
    rewind,
    reseed,
};

fn validateRejoinResponse(
    response: admin_api.HARejoinAssessResponse,
    request: admin_api.RejoinAssessRequest,
    expected: RejoinResponseKind,
) !void {
    if (response.schema_version <= 0) return error.AdminRejoinResponseMismatch;

    const expected_action_kind = switch (expected) {
        .assess => "rejoin_assess",
        .rewind => "rejoin_rewind",
        .reseed => "rejoin_reseed",
    };
    const expected_state = switch (expected) {
        .assess => "assessed",
        .rewind, .reseed => "applied",
    };
    try validateActionReceipt(response.action, expected_action_kind, expected_state, request.node_id, error.AdminRejoinResponseMismatch);
    switch (expected) {
        .assess, .rewind => {
            if (!std.mem.eql(u8, response.action.node_id, request.node_id)) {
                return error.AdminRejoinResponseMismatch;
            }
        },
        .reseed => {},
    }

    const assessment = response.assessment;
    if (!validation.isIdentifier(assessment.former_node_id)) return error.AdminRejoinResponseMismatch;
    if (!std.mem.eql(u8, assessment.former_node_id, request.node_id)) {
        return error.AdminRejoinResponseMismatch;
    }
    if (assessment.target_timeline_id <= 0 or assessment.target_epoch <= 0 or
        assessment.parent_cluster_id <= 0 or assessment.parent_shard_id < 0 or assessment.parent_table_id < 0 or
        assessment.parent_timeline_id <= 0 or assessment.parent_epoch <= 0 or
        assessment.fork_lsn < 0 or assessment.former_last_lsn < 0 or assessment.retained_from_lsn < 0)
    {
        return error.AdminRejoinResponseMismatch;
    }
    // last_lsn in the request is a controller observation, not authority. A
    // former primary with local log access replaces it with its durable tail so
    // writes racing the promotion boundary cannot be hidden by stale status.
    if (assessment.retained_from_lsn != request.retained_from_lsn) return error.AdminRejoinResponseMismatch;

    switch (expected) {
        .assess => {},
        .rewind => if (!std.mem.eql(u8, assessment.action, "rewind")) return error.AdminRejoinResponseMismatch,
        .reseed => if (!std.mem.eql(u8, assessment.action, "reseed")) return error.AdminRejoinResponseMismatch,
    }

    if (request.receipt) |receipt| {
        if (assessment.target_timeline_id != receipt.new_timeline_id) return error.AdminRejoinResponseMismatch;
        if (assessment.target_epoch != receipt.new_epoch) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_cluster_id != receipt.identity.cluster_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_shard_id != receipt.identity.shard_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_table_id != receipt.identity.table_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_timeline_id != receipt.parent_timeline_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_epoch != receipt.parent_epoch) return error.AdminRejoinResponseMismatch;
        if (assessment.fork_lsn != receipt.observed_lsn) return error.AdminRejoinResponseMismatch;
    } else {
        if (!std.mem.eql(u8, assessment.action, "reject_unfenced")) return error.AdminRejoinResponseMismatch;
        if (!std.mem.eql(u8, assessment.reason, "no_fence")) return error.AdminRejoinResponseMismatch;
        if (assessment.target_timeline_id != request.identity.timeline_id) return error.AdminRejoinResponseMismatch;
        if (assessment.target_epoch != request.identity.epoch) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_cluster_id != request.identity.cluster_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_shard_id != request.identity.shard_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_table_id != request.identity.table_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_timeline_id != request.identity.timeline_id) return error.AdminRejoinResponseMismatch;
        if (assessment.parent_epoch != request.identity.epoch) return error.AdminRejoinResponseMismatch;
        if (assessment.fork_lsn != assessment.former_last_lsn or assessment.data_loss_discarded) {
            return error.AdminRejoinResponseMismatch;
        }
    }

    switch (expected) {
        .assess => {
            if (response.rewind != null or response.reseed != null) return error.AdminRejoinResponseMismatch;
        },
        .rewind => {
            const rewind = response.rewind orelse return error.AdminRejoinResponseMismatch;
            if (response.reseed != null) return error.AdminRejoinResponseMismatch;
            if (!validation.isIdentifier(rewind.node_id)) return error.AdminRejoinResponseMismatch;
            if (!std.mem.eql(u8, rewind.node_id, request.node_id)) return error.AdminRejoinResponseMismatch;
            if (rewind.fork_lsn != assessment.fork_lsn) return error.AdminRejoinResponseMismatch;
            if (rewind.previous_last_lsn != assessment.former_last_lsn) return error.AdminRejoinResponseMismatch;
            if (rewind.current_last_lsn != assessment.fork_lsn) return error.AdminRejoinResponseMismatch;
            if (rewind.target_timeline_id != assessment.target_timeline_id) return error.AdminRejoinResponseMismatch;
            if (rewind.target_epoch != assessment.target_epoch) return error.AdminRejoinResponseMismatch;
        },
        .reseed => {
            const reseed = response.reseed orelse return error.AdminRejoinResponseMismatch;
            if (response.rewind != null) return error.AdminRejoinResponseMismatch;
            if (!validation.isIdentifier(reseed.node_id) or !validation.isIdentifier(reseed.slot_name)) return error.AdminRejoinResponseMismatch;
            if (!std.mem.eql(u8, reseed.node_id, request.node_id)) return error.AdminRejoinResponseMismatch;
            if (!std.mem.eql(u8, reseed.slot_name, request.node_id)) return error.AdminRejoinResponseMismatch;
            if (reseed.target_timeline_id != assessment.target_timeline_id) return error.AdminRejoinResponseMismatch;
            if (reseed.target_epoch != assessment.target_epoch) return error.AdminRejoinResponseMismatch;
            if (reseed.fork_lsn != assessment.fork_lsn) return error.AdminRejoinResponseMismatch;
            if (reseed.former_last_lsn != assessment.former_last_lsn) return error.AdminRejoinResponseMismatch;
            if (!reseed.reseed_required or !reseed.base_backup_required) return error.AdminRejoinResponseMismatch;
        },
    }
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    former_primary_log: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,
    backup_root: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.former_primary_log);
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
    const former_primary_log = try allocPrintPath(alloc, name, "former-primary-log", nonce);
    defer alloc.free(former_primary_log);
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
    std.Io.Dir.cwd().deleteTree(io_impl.io(), former_primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .former_primary_log = try alloc.dupeZ(u8, former_primary_log),
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
        ".zig-cache/tmp/ha-http-client-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

fn testAdminIdentity() admin_api.HAIdentity {
    const identity = testIdentity();
    return .{
        .cluster_id = @intCast(identity.cluster_id),
        .shard_id = @intCast(identity.shard_id),
        .table_id = @intCast(identity.table_id),
        .timeline_id = @intCast(identity.timeline_id),
        .epoch = @intCast(identity.epoch),
    };
}

fn testFenceReceipt() admin_api.HAFenceReceipt {
    return .{
        .identity = .{
            .cluster_id = 100,
            .shard_id = 10,
            .table_id = 20,
            .timeline_id = 2,
            .epoch = 2,
        },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .parent_timeline_id = 1,
        .parent_epoch = 1,
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 1,
        .observed_lsn = 1,
        .generation = 1,
        .forced = false,
        .token = "token",
        .reason = "http-client-test",
    };
}

fn testFenceResponse() admin_api.HAFenceResponse {
    return .{
        .schema_version = 1,
        .action = .{
            .action_id = "fence_acquire:standby-a",
            .action_kind = "fence_acquire",
            .target = "standby-a",
            .state = "applied",
            // The endpoint actor is the former primary; the promoted standby
            // remains the action target.
            .node_id = "primary-a",
        },
        .receipt = testFenceReceipt(),
    };
}

fn seedFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

const StaticJsonExecutor = struct {
    body: []const u8,

    fn executor(self: *@This()) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .status = 200,
            .content_type = try alloc.dupe(u8, "application/json"),
            .body = try alloc.dupe(u8, self.body),
        };
    }
};

fn testRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
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

fn noOpApply(_: *anyopaque, _: replication_record.RecordView) anyerror!void {}

fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = bytes,
    });
}

test "storage.ha http client accepts zero LSN promotion assessment" {
    const alloc = std.testing.allocator;
    var executor = StaticJsonExecutor{
        .body =
        \\{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":0,"received_lsn":0,"applied_lsn":0,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}
        ,
    };
    var client = Client.init(alloc, executor.executor());

    var assessment = try client.assessPromotion("http://ha-admin.test", .{
        .required_lsn = 0,
        .fencing_confirmed = true,
        .force = false,
        .use_current_fence = false,
    });
    defer assessment.deinit(alloc);

    try std.testing.expectEqual(@as(i64, 0), assessment.parsed.value.assessment.required_lsn);
    try std.testing.expect(assessment.parsed.value.assessment.has_required_lsn);
    try std.testing.expect(assessment.parsed.value.assessment.safe);
}

test "storage.ha http client round trips admin commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "round-trip");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &fence_store,
    });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    try client.checkHealth("http://ha-admin.test");
    try client.checkReady("http://ha-admin.test");

    var typed_primary_status = try client.getPrimaryStatus("http://ha-admin.test", .{ .max_lag_lsn = 1 });
    defer typed_primary_status.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), typed_primary_status.parsed.value.schema_version);
    try std.testing.expectEqualStrings("primary", typed_primary_status.parsed.value.snapshot.role);
    try std.testing.expectEqual(@as(i64, 0), typed_primary_status.parsed.value.snapshot.current_lsn);

    var typed_created = try client.createReplicationSlot("http://ha-admin.test", "standby-typed", 0);
    defer typed_created.deinit(alloc);
    try std.testing.expectEqualStrings("create", typed_created.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby-typed", typed_created.parsed.value.slot.slot_name);
    try std.testing.expect(typed_created.parsed.value.slot.active);

    var typed_slots = try client.listReplicationSlots("http://ha-admin.test");
    defer typed_slots.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), typed_slots.parsed.value.schema_version);
    try std.testing.expectEqual(@as(usize, 1), typed_slots.parsed.value.slots.len);
    try std.testing.expectEqualStrings("standby-typed", typed_slots.parsed.value.slots[0].slot_name);

    var typed_paused = try client.pauseReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_paused.deinit(alloc);
    try std.testing.expectEqualStrings("pause", typed_paused.parsed.value.slot_action);
    try std.testing.expect(!typed_paused.parsed.value.slot.active);

    var typed_resumed = try client.resumeReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_resumed.deinit(alloc);
    try std.testing.expectEqualStrings("resume", typed_resumed.parsed.value.slot_action);
    try std.testing.expect(typed_resumed.parsed.value.slot.active);

    var typed_dropped = try client.dropReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_dropped.deinit(alloc);
    try std.testing.expectEqualStrings("drop", typed_dropped.parsed.value.slot_action);
    try std.testing.expectEqual(@as(?bool, true), typed_dropped.parsed.value.slot.dropped);

    var encoded_created = try client.createReplicationSlot("http://ha-admin.test", "standby:a.b", 0);
    defer encoded_created.deinit(alloc);
    try std.testing.expectEqualStrings("create", encoded_created.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby:a.b", encoded_created.parsed.value.slot.slot_name);

    var encoded_paused = try client.pauseReplicationSlot("http://ha-admin.test", "standby:a.b");
    defer encoded_paused.deinit(alloc);
    try std.testing.expectEqualStrings("pause", encoded_paused.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby:a.b", encoded_paused.parsed.value.slot.slot_name);
    try std.testing.expect(!encoded_paused.parsed.value.slot.active);

    var encoded_resumed = try client.resumeReplicationSlot("http://ha-admin.test", "standby:a.b");
    defer encoded_resumed.deinit(alloc);
    try std.testing.expectEqualStrings("resume", encoded_resumed.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby:a.b", encoded_resumed.parsed.value.slot.slot_name);
    try std.testing.expect(encoded_resumed.parsed.value.slot.active);

    var encoded_dropped = try client.dropReplicationSlot("http://ha-admin.test", "standby:a.b");
    defer encoded_dropped.deinit(alloc);
    try std.testing.expectEqualStrings("drop", encoded_dropped.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby:a.b", encoded_dropped.parsed.value.slot.slot_name);
    try std.testing.expectEqual(@as(?bool, true), encoded_dropped.parsed.value.slot.dropped);

    var stream_slot = try client.createReplicationSlot("http://ha-admin.test/", "standby-a", 0);
    defer stream_slot.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", stream_slot.parsed.value.slot.slot_name);

    var appended = try client.appendCommit("http://ha-admin.test", .{
        .payload = "one",
        .shard_id = @intCast(identity.shard_id),
        .table_id = @intCast(identity.table_id),
        .sync_policy = .{ .mode = "async" },
    });
    defer appended.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), appended.parsed.value.lsn);
    try std.testing.expectEqualStrings("acknowledge", appended.parsed.value.gate.action);

    var streamed = try client.executeCommand("http://ha-admin.test", &.{ "--table", "stream", "once", "--slot", "standby-a" });
    defer streamed.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", streamed.content_type);
    try expectContains(streamed.body, "result=stream_once\n");
    try expectContains(streamed.body, "applied_lsn=1\n");

    var typed_standby_status = try client.getStandbyStatus("http://ha-admin.test", 2);
    defer typed_standby_status.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), typed_standby_status.parsed.value.schema_version);
    try std.testing.expectEqualStrings("standby", typed_standby_status.parsed.value.snapshot.role);
    try std.testing.expectEqual(@as(i64, 1), typed_standby_status.parsed.value.snapshot.applied_lsn);
    try std.testing.expectEqual(@as(?i64, 2), typed_standby_status.parsed.value.snapshot.upstream_lsn);
    try std.testing.expectEqual(@as(?i64, 1), typed_standby_status.parsed.value.snapshot.write_lag_lsn);

    try std.testing.expectError(error.HaCommandConflict, client.executeCommand("http://ha-admin.test", &.{
        "operator",
        "plan",
        "--standby",
        "standby-a",
        "--standby-route-selector",
        "--sync-mode",
        "remote-apply",
        "--sync-standby",
        "standby-a",
        "--auto-failover",
        "--fencing-authority",
        "kubernetes-lease",
        "--current-primary-id",
        "primary-a",
        "--primary-admin-unavailable",
        "--fence-authority",
        "kubernetes-lease",
        "--fence-ready",
        "--fence-holder",
        "standby-a",
        "--fence-generation",
        "4",
        "--fence-reason",
        "LeaseAcquired",
    }));

    const fence_request = admin_api.FenceAcquireRequest{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .force = false,
        .reason = "http-client-test",
    };
    var fenced = try client.acquireFence("http://ha-admin.test", fence_request);
    defer fenced.deinit(alloc);
    try std.testing.expectEqualStrings("fence_acquire", fenced.parsed.value.action.action_kind);
    try std.testing.expectEqualStrings("standby-a", fenced.parsed.value.receipt.promoted_node_id);

    var current_fence = try client.currentFence("http://ha-admin.test");
    defer current_fence.deinit(alloc);
    try std.testing.expect(current_fence.parsed.value.receipt != null);

    var promoted = try client.promoteWithCurrentFence("http://ha-admin.test");
    defer promoted.deinit(alloc);
    try std.testing.expectEqualStrings("promotion", promoted.parsed.value.action.action_kind);
    try std.testing.expectEqual(@as(i64, 2), promoted.parsed.value.promotion.new_identity.timeline_id);

    try std.testing.expectError(error.HaCommandConflict, client.executeCommand("http://ha-admin.test", &.{
        "--table",
        "operator",
        "plan",
        "--former-primary-id",
        "primary-a",
        "--former-cluster-id",
        "100",
        "--former-shard-id",
        "10",
        "--former-table-id",
        "20",
        "--former-timeline-id",
        "1",
        "--former-epoch",
        "1",
        "--former-last-lsn",
        "1",
        "--retained-from-lsn",
        "1",
        "--receipt-old-primary-id",
        "primary-a",
        "--receipt-promoted-node-id",
        "standby-a",
        "--receipt-parent-timeline-id",
        "1",
        "--receipt-parent-epoch",
        "1",
        "--receipt-new-timeline-id",
        "2",
        "--receipt-new-epoch",
        "2",
        "--receipt-required-lsn",
        "1",
        "--receipt-observed-lsn",
        "1",
        "--receipt-generation",
        "1",
        "--receipt-token",
        "token",
        "--receipt-reason",
        "http-client-test",
    }));
}

test "storage.ha http client round trips typed commit operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-commit");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();

    var server = http_admin.Server.init(alloc, .{ .primary = &primary, .primary_node_id = "primary-a" });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    var slot = try client.createReplicationSlot("http://ha-admin.test", "standby-a", 0);
    defer slot.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", slot.parsed.value.slot.slot_name);

    var appended = try client.appendCommit("http://ha-admin.test", .{
        .payload = "one",
        .shard_id = @intCast(identity.shard_id),
        .table_id = @intCast(identity.table_id),
        .sync_policy = .{ .mode = "async" },
    });
    defer appended.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), appended.parsed.value.lsn);
    try std.testing.expectEqualStrings("acknowledge", appended.parsed.value.gate.action);
    try std.testing.expectEqualStrings("async", appended.parsed.value.gate.durability.mode);

    const standby_names = [_][]const u8{"standby-a"};
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 0);
    var checked = try client.checkCommit("http://ha-admin.test", .{
        .target_lsn = 1,
        .sync_policy = .{
            .mode = "remote_write",
            .standby_names = &standby_names,
        },
    });
    defer checked.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), checked.parsed.value.gate.target_lsn);
    try std.testing.expectEqualStrings("acknowledge", checked.parsed.value.gate.action);
    try std.testing.expectEqualStrings("remote_write", checked.parsed.value.gate.durability.mode);
    try std.testing.expectEqual(@as(i64, 1), checked.parsed.value.gate.durability.progress_lsn);

    var degraded = try client.appendCommit("http://ha-admin.test", .{
        .payload = "two",
        .kind = "metadata_mutation",
        .payload_codec = "json",
        .shard_id = 10,
        .table_id = 20,
        .sync_policy = .{
            .mode = "remote_apply",
            .standby_names = &standby_names,
            .failure_policy = "degrade_to_async",
        },
    });
    defer degraded.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 2), degraded.parsed.value.lsn);
    try std.testing.expectEqualStrings("acknowledge_degraded", degraded.parsed.value.gate.action);
    try std.testing.expectEqualStrings("degraded_to_async", degraded.parsed.value.gate.durability.status);
}

test "storage.ha http client round trips typed gate operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-gates");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
    });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    var primary_write = try client.checkWrite("http://ha-admin.test", .{
        .role = "primary",
        .expected_identity = testAdminIdentity(),
    });
    defer primary_write.deinit(alloc);
    try std.testing.expectEqualStrings("allow_write", primary_write.parsed.value.decision.action);
    try std.testing.expectEqual(@as(i64, 1), primary_write.parsed.value.decision.next_lsn);

    var primary_owner_job = try client.checkOwnerJob("http://ha-admin.test", .{
        .role = "primary",
        .kind = "retention_advance",
        .expected_identity = testAdminIdentity(),
    });
    defer primary_owner_job.deinit(alloc);
    try std.testing.expectEqualStrings("run", primary_owner_job.parsed.value.decision.action);

    var slot = try client.createReplicationSlot("http://ha-admin.test", "standby-a", 0);
    defer slot.deinit(alloc);
    var appended = try client.appendCommit("http://ha-admin.test", .{
        .payload = "one",
        .shard_id = @intCast(identity.shard_id),
        .table_id = @intCast(identity.table_id),
        .sync_policy = .{ .mode = "async" },
    });
    defer appended.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), appended.parsed.value.lsn);

    var apply_ctx: u8 = 0;
    _ = try standby.receive(testRecord(identity, 1, "one"));
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_ctx, noOpApply));

    var ready_read = try client.checkRead("http://ha-admin.test", .{
        .consistency = "at_least_lsn",
        .required_lsn = 1,
    });
    defer ready_read.deinit(alloc);
    try std.testing.expectEqualStrings("serve_standby", ready_read.parsed.value.decision.action);
    try std.testing.expectEqual(@as(?i64, 1), ready_read.parsed.value.decision.serve_lsn);

    var waiting_read = try client.checkRead("http://ha-admin.test", .{
        .consistency = "at_least_lsn",
        .required_lsn = 2,
    });
    defer waiting_read.deinit(alloc);
    try std.testing.expectEqualStrings("wait_for_apply", waiting_read.parsed.value.decision.action);
    try std.testing.expectEqual(@as(i64, 1), waiting_read.parsed.value.decision.missing_lsn_count);

    var standby_write = try client.checkWrite("http://ha-admin.test", .{
        .role = "standby",
        .expected_identity = testAdminIdentity(),
    });
    defer standby_write.deinit(alloc);
    try std.testing.expectEqualStrings("reject_read_only_standby", standby_write.parsed.value.decision.action);
    try std.testing.expectEqualStrings("standby", standby_write.parsed.value.decision.role);

    var standby_owner_job = try client.checkOwnerJob("http://ha-admin.test", .{
        .role = "standby",
        .kind = "compaction_publish",
        .expected_identity = testAdminIdentity(),
    });
    defer standby_owner_job.deinit(alloc);
    try std.testing.expectEqualStrings("disable_on_standby", standby_owner_job.parsed.value.decision.action);
}

test "storage.ha http client round trips typed seed operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-seed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
    });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    var begin = try client.beginBaseBackup("http://ha-admin.test", .{
        .slot_name = "standby-seed",
        .manifest_id = "base-http-client",
    });
    defer begin.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.schema_version);
    try std.testing.expectEqualStrings("standby-seed", begin.parsed.value.slot_name);
    try std.testing.expectEqualStrings("base-http-client", begin.parsed.value.manifest_id);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.backup_lsn);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.start_record_lsn);

    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));

    const files = seedFiles();
    const encoded_manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = "base-http-client",
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

    var finish = try client.finishBaseBackup("http://ha-admin.test", .{
        .manifest_path = manifest_path,
    });
    defer finish.deinit(alloc);
    try std.testing.expectEqualStrings("base-http-client", finish.parsed.value.manifest_id);
    try std.testing.expectEqual(@as(i64, 1), finish.parsed.value.backup_lsn);
    try std.testing.expectEqual(@as(i64, 3), finish.parsed.value.end_record_lsn);

    var bootstrap = try client.bootstrapStandby("http://ha-admin.test", .{
        .manifest_path = manifest_path,
        .content_root = paths.backup_root,
    });
    defer bootstrap.deinit(alloc);
    try std.testing.expectEqualStrings("base-http-client", bootstrap.parsed.value.manifest_id);
    try std.testing.expectEqual(@as(i64, 2), bootstrap.parsed.value.checkpoint_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());
}

test "storage.ha http client round trips typed safety operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-safety");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();
    var former_log = try replication_log.ReplicationLog.open(paths.former_primary_log.ptr, .{});
    defer former_log.close();
    _ = try former_log.append(alloc, .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "one",
    });
    _ = try former_log.append(alloc, .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = 2,
        .previous_lsn = 1,
        .payload = "diverged",
    });

    var server = http_admin.Server.init(alloc, .{
        .primary = &primary,
        .primary_node_id = "primary-a",
        .standby = &standby,
        .standby_node_id = "standby-a",
        .fence_store = &fence_store,
        .former_primary_log = &former_log,
    });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    var slot = try client.createReplicationSlot("http://ha-admin.test", "standby-a", 0);
    defer slot.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));

    var streamed = try client.executeCommand("http://ha-admin.test", &.{ "--table", "stream", "once", "--slot", "standby-a" });
    defer streamed.deinit(alloc);
    try expectContains(streamed.body, "applied_lsn=1\n");

    const fence_request = admin_api.FenceAcquireRequest{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .force = false,
        .reason = "typed-client-test",
    };
    var fence = try client.acquireFence("http://ha-admin.test", fence_request);
    defer fence.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), fence.parsed.value.schema_version);
    try std.testing.expectEqualStrings("standby-a", fence.parsed.value.receipt.promoted_node_id);
    try std.testing.expectEqual(@as(i64, 2), fence.parsed.value.receipt.new_timeline_id);

    var current = try client.currentFence("http://ha-admin.test");
    defer current.deinit(alloc);
    try std.testing.expect(current.parsed.value.held);
    try std.testing.expectEqualStrings("primary-a", current.parsed.value.receipt.?.old_primary_id);

    var assessment = try client.assessPromotion("http://ha-admin.test", .{
        .required_lsn = 1,
        .fencing_confirmed = false,
        .force = false,
        .use_current_fence = true,
    });
    defer assessment.deinit(alloc);
    try std.testing.expectEqualStrings("promotion_assess", assessment.parsed.value.action.action_kind);
    try std.testing.expectEqualStrings("promotion_assess:standby-a", assessment.parsed.value.action.action_id);
    try std.testing.expectEqualStrings("assessed", assessment.parsed.value.action.state);
    try std.testing.expectEqualStrings("standby-a", assessment.parsed.value.action.node_id);
    try std.testing.expect(assessment.parsed.value.assessment.can_promote);
    try std.testing.expect(assessment.parsed.value.assessment.fencing_confirmed);

    var promoted = try client.promoteWithCurrentFence("http://ha-admin.test");
    defer promoted.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 2), promoted.parsed.value.promotion.new_identity.timeline_id);
    try std.testing.expectEqual(@as(i64, 1), promoted.parsed.value.fence_generation);

    var rejoin = try client.assessRejoin("http://ha-admin.test", .{
        .node_id = "primary-a",
        .identity = testAdminIdentity(),
        .last_lsn = 2,
        .retained_from_lsn = 0,
        .allow_rewind_after_forced_promotion = false,
        .receipt = fence.parsed.value.receipt,
    });
    defer rejoin.deinit(alloc);
    try std.testing.expectEqualStrings("reseed", rejoin.parsed.value.assessment.action);
    try std.testing.expectEqual(@as(i64, 2), rejoin.parsed.value.assessment.target_timeline_id);
    try std.testing.expectEqual(@as(i64, 100), rejoin.parsed.value.assessment.parent_cluster_id);
    try std.testing.expectEqual(@as(i64, 10), rejoin.parsed.value.assessment.parent_shard_id);
    try std.testing.expectEqual(@as(i64, 20), rejoin.parsed.value.assessment.parent_table_id);
    try std.testing.expectEqual(@as(i64, 1), rejoin.parsed.value.assessment.parent_timeline_id);
    try std.testing.expectEqual(@as(i64, 1), rejoin.parsed.value.assessment.parent_epoch);

    try std.testing.expectError(error.HaCommandConflict, client.rewindRejoin("http://ha-admin.test", .{
        .node_id = "primary-a",
        .identity = testAdminIdentity(),
        .last_lsn = 2,
        .retained_from_lsn = 0,
        .allow_rewind_after_forced_promotion = false,
        .receipt = fence.parsed.value.receipt,
    }));

    var reseed = try client.reseedRejoin("http://ha-admin.test", .{
        .node_id = "primary-a",
        .identity = testAdminIdentity(),
        .last_lsn = 2,
        .retained_from_lsn = 0,
        .allow_rewind_after_forced_promotion = false,
        .receipt = fence.parsed.value.receipt,
    });
    defer reseed.deinit(alloc);
    try std.testing.expectEqualStrings("reseed", reseed.parsed.value.assessment.action);
    try std.testing.expectEqual(@as(i64, 1), reseed.parsed.value.assessment.parent_timeline_id);
    try std.testing.expect(reseed.parsed.value.reseed != null);
    try std.testing.expect(reseed.parsed.value.reseed.?.reseed_required);
    try std.testing.expect(reseed.parsed.value.reseed.?.base_backup_required);
}

test "storage.ha http client accepts authoritative former primary tail in rejoin assessment" {
    const alloc = std.testing.allocator;
    var executor = StaticJsonExecutor{
        .body =
        \\{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"reject_unfenced","reason":"no_fence","former_node_id":"primary-a","target_timeline_id":1,"target_epoch":1,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":1,"parent_epoch":1,"required_lsn":0,"fork_lsn":4,"former_last_lsn":4,"retained_from_lsn":8,"forced":false,"data_loss_discarded":false}}
        ,
    };
    var client = Client.init(alloc, executor.executor());

    // The controller observation may be stale in either direction. The local
    // former-primary endpoint reports its durable tail as the authority.
    var response = try client.assessRejoin("http://ha-admin.test", .{
        .node_id = "primary-a",
        .identity = testAdminIdentity(),
        .last_lsn = 12,
        .retained_from_lsn = 8,
        .allow_rewind_after_forced_promotion = false,
        .receipt = null,
    });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 4), response.parsed.value.assessment.former_last_lsn);
    try std.testing.expectEqual(response.parsed.value.assessment.former_last_lsn, response.parsed.value.assessment.fork_lsn);
}

test "storage.ha http client rejects mismatched rejoin admin responses" {
    const alloc = std.testing.allocator;
    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":2,"target_epoch":2,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":99,"parent_epoch":1,"fork_lsn":1,"former_last_lsn":2,"retained_from_lsn":0,"data_loss_discarded":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());

        try std.testing.expectError(error.AdminRejoinResponseMismatch, client.assessRejoin("http://ha-admin.test", .{
            .node_id = "primary-a",
            .identity = testAdminIdentity(),
            .last_lsn = 2,
            .retained_from_lsn = 0,
            .allow_rewind_after_forced_promotion = false,
            .receipt = testFenceReceipt(),
        }));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"assessed","node_id":"primary-b"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":2,"target_epoch":2,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":1,"parent_epoch":1,"fork_lsn":1,"former_last_lsn":2,"retained_from_lsn":0,"data_loss_discarded":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());

        try std.testing.expectError(error.AdminRejoinResponseMismatch, client.assessRejoin("http://ha-admin.test", .{
            .node_id = "primary-a",
            .identity = testAdminIdentity(),
            .last_lsn = 2,
            .retained_from_lsn = 0,
            .allow_rewind_after_forced_promotion = false,
            .receipt = testFenceReceipt(),
        }));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"applied","node_id":"primary-b"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":2,"target_epoch":2,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":1,"parent_epoch":1,"fork_lsn":1,"former_last_lsn":2,"retained_from_lsn":0,"data_loss_discarded":true},"rewind":{"node_id":"primary-a","fork_lsn":1,"previous_last_lsn":2,"current_last_lsn":1,"next_lsn":2,"discarded_lsn_count":1,"target_timeline_id":2,"target_epoch":2,"data_loss_discarded":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());

        try std.testing.expectError(error.AdminRejoinResponseMismatch, client.rewindRejoin("http://ha-admin.test", .{
            .node_id = "primary-a",
            .identity = testAdminIdentity(),
            .last_lsn = 2,
            .retained_from_lsn = 0,
            .allow_rewind_after_forced_promotion = false,
            .receipt = testFenceReceipt(),
        }));
    }
}

test "storage.ha http client rejects mismatched promotion admin responses" {
    const alloc = std.testing.allocator;
    var executor = StaticJsonExecutor{
        .body =
        \\{"schema_version":1,"action":{"action_id":"promotion:standby-b","action_kind":"promotion","target":"standby-b","state":"applied","node_id":"standby-b"},"assessment":{"required_lsn":1,"received_lsn":1,"applied_lsn":1,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-b","switch_lsn":2,"old_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":1,"epoch":1},"new_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":2,"epoch":2},"forced":false,"data_loss_possible":false},"fence_generation":1,"fence_token":"token","forced":false}
        ,
    };
    var client = Client.init(alloc, executor.executor());

    try std.testing.expectError(error.AdminPromotionResponseMismatch, client.promote("http://ha-admin.test", .{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .force = false,
        .reason = "http-client-test",
    }));
}

test "storage.ha http client accepts empty fence receipt reason" {
    const alloc = std.testing.allocator;
    var executor = StaticJsonExecutor{
        .body =
        \\{"schema_version":1,"action":{"action_id":"fence_acquire:standby-a","action_kind":"fence_acquire","target":"standby-a","state":"applied","node_id":"standby-a"},"receipt":{"identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":2,"epoch":2},"old_primary_id":"primary-a","promoted_node_id":"standby-a","parent_timeline_id":1,"parent_epoch":1,"new_timeline_id":2,"new_epoch":2,"required_lsn":1,"observed_lsn":1,"generation":1,"forced":false,"token":"token","reason":""}}
        ,
    };
    var client = Client.init(alloc, executor.executor());

    var response = try client.acquireFence("http://ha-admin.test", .{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .force = false,
        .reason = "",
    });
    defer response.deinit(alloc);

    try std.testing.expectEqualStrings("", response.parsed.value.receipt.reason);
}

test "storage.ha http client validates live upgraded fence boundary and topology binding" {
    const request = admin_api.FenceAcquireRequest{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .generation = 1,
        .required_lsn = 1,
        .observed_lsn = 1,
        .force = false,
        .reason = "operator-observation",
    };

    // A former primary freezes its live durable tail under the append mutex.
    // The returned boundary can therefore be stronger than the stale
    // control-plane observation carried by the request.
    var upgraded = testFenceResponse();
    upgraded.receipt.required_lsn = 4;
    upgraded.receipt.observed_lsn = 4;
    try validateFenceResponse(upgraded, request);

    var downgraded = testFenceResponse();
    downgraded.receipt.required_lsn = 1;
    downgraded.receipt.observed_lsn = 0;
    downgraded.receipt.forced = true;
    try std.testing.expectError(error.AdminFenceResponseMismatch, validateFenceResponse(downgraded, request));

    var cross_cluster = upgraded;
    cross_cluster.receipt.identity.cluster_id += 1;
    try std.testing.expectError(error.AdminFenceResponseMismatch, validateFenceResponse(cross_cluster, request));

    var wrong_parent = upgraded;
    wrong_parent.receipt.parent_epoch += 1;
    try std.testing.expectError(error.AdminFenceResponseMismatch, validateFenceResponse(wrong_parent, request));

    var unrequested_force = upgraded;
    unrequested_force.receipt.forced = true;
    try std.testing.expectError(error.AdminFenceResponseMismatch, validateFenceResponse(unrequested_force, request));

    var unsafe_unforced = upgraded;
    unsafe_unforced.receipt.required_lsn = 4;
    unsafe_unforced.receipt.observed_lsn = 3;
    try std.testing.expectError(error.AdminFenceResponseMismatch, validateFenceResponse(unsafe_unforced, request));
}

test "storage.ha http client accepts already applied idempotent admin receipts" {
    const alloc = std.testing.allocator;

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"already_applied","node_id":"primary-a"},"slot_action":"create","slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"current_lsn":0}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        var response = try client.createReplicationSlot("http://ha-admin.test", "standby-a", 0);
        defer response.deinit(alloc);
        try std.testing.expectEqualStrings("already_applied", response.parsed.value.action.state);
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"promotion:standby-a","action_kind":"promotion","target":"standby-a","state":"already_applied","node_id":"standby-a"},"assessment":{"required_lsn":1,"received_lsn":1,"applied_lsn":1,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true},"promotion":{"node_id":"standby-a","switch_lsn":2,"old_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":1,"epoch":1},"new_identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":2,"epoch":2},"forced":false,"data_loss_possible":false},"fence_generation":1,"fence_token":"token","forced":false}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        var response = try client.promote("http://ha-admin.test", .{
            .identity = testAdminIdentity(),
            .old_primary_id = "primary-a",
            .promoted_node_id = "standby-a",
            .new_timeline_id = 2,
            .new_epoch = 2,
            .generation = 1,
            .required_lsn = 1,
            .observed_lsn = 1,
            .force = false,
            .reason = "http-client-test",
        });
        defer response.deinit(alloc);
        try std.testing.expectEqualStrings("already_applied", response.parsed.value.action.state);
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"rejoin_rewind:primary-a","action_kind":"rejoin_rewind","target":"primary-a","state":"already_applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":2,"target_epoch":2,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":1,"parent_epoch":1,"fork_lsn":1,"former_last_lsn":2,"retained_from_lsn":0,"data_loss_discarded":true},"rewind":{"node_id":"primary-a","fork_lsn":1,"previous_last_lsn":2,"current_last_lsn":1,"next_lsn":2,"discarded_lsn_count":1,"target_timeline_id":2,"target_epoch":2,"data_loss_discarded":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        var response = try client.rewindRejoin("http://ha-admin.test", .{
            .node_id = "primary-a",
            .identity = testAdminIdentity(),
            .last_lsn = 2,
            .retained_from_lsn = 0,
            .allow_rewind_after_forced_promotion = false,
            .receipt = testFenceReceipt(),
        });
        defer response.deinit(alloc);
        try std.testing.expectEqualStrings("already_applied", response.parsed.value.action.state);
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"rejoin_assess:primary-a","action_kind":"rejoin_assess","target":"primary-a","state":"already_applied","node_id":"primary-a"},"assessment":{"action":"rewind","reason":"parent_timeline_retained","former_node_id":"primary-a","target_timeline_id":2,"target_epoch":2,"parent_cluster_id":100,"parent_shard_id":10,"parent_table_id":20,"parent_timeline_id":1,"parent_epoch":1,"fork_lsn":1,"former_last_lsn":2,"retained_from_lsn":0,"data_loss_discarded":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminRejoinResponseMismatch, client.assessRejoin("http://ha-admin.test", .{
            .node_id = "primary-a",
            .identity = testAdminIdentity(),
            .last_lsn = 2,
            .retained_from_lsn = 0,
            .allow_rewind_after_forced_promotion = false,
            .receipt = testFenceReceipt(),
        }));
    }
}

test "storage.ha http client rejects invalid local admin inputs" {
    const alloc = std.testing.allocator;
    var executor = StaticJsonExecutor{ .body = "{}" };
    var client = Client.init(alloc, executor.executor());

    try std.testing.expectError(error.InvalidHAAdminURL, client.listReplicationSlots(" http://ha-admin.test"));
    try std.testing.expectError(error.InvalidHAAdminURL, client.listReplicationSlots("ftp://ha-admin.test"));
    try std.testing.expectError(error.InvalidHAAdminURL, client.listReplicationSlots("http://ha admin.test"));
    try std.testing.expectError(error.InvalidSlotName, client.createReplicationSlot("http://ha-admin.test", "standby a", 0));
    try std.testing.expectError(error.InvalidSlotName, client.pauseReplicationSlot("http://ha-admin.test", " standby-a"));
    try std.testing.expectError(error.InvalidSlotName, client.beginBaseBackup("http://ha-admin.test", .{
        .slot_name = "standby/a",
        .manifest_id = "base-a",
    }));
}

test "storage.ha http client rejects invalid typed admin responses" {
    const alloc = std.testing.allocator;

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"applied","node_id":"primary-a"},"slot_action":"create","slot":{"slot_name":"standby-b","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"current_lsn":0}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(
            error.AdminReplicationSlotResponseMismatch,
            client.createReplicationSlot("http://ha-admin.test", "standby-a", 0),
        );
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"slots":[{"slot_name":"standby-a","timeline_id":0,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"current_lsn":0}]}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(
            error.AdminReplicationSlotResponseMismatch,
            client.listReplicationSlots("http://ha-admin.test"),
        );
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"slots":[{"slot_name":"standby a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"current_lsn":0}]}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(
            error.AdminReplicationSlotResponseMismatch,
            client.listReplicationSlots("http://ha-admin.test"),
        );
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"replication_slot_create:standby-a","action_kind":"replication_slot_create","target":"standby-a","state":"applied","node_id":"primary a"},"slot_action":"create","slot":{"slot_name":"standby-a","timeline_id":1,"restart_lsn":0,"received_lsn":0,"applied_lsn":0,"safe_read_lsn":0,"active":true,"reseed_required":false,"current_lsn":0}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(
            error.AdminReplicationSlotResponseMismatch,
            client.createReplicationSlot("http://ha-admin.test", "standby-a", 0),
        );
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"base_backup_begin:base-a","action_kind":"base_backup_begin","target":"base-a","state":"applied","node_id":"primary-a"},"slot_name":"standby-a","manifest_id":"base-a","backup_lsn":0,"start_record_lsn":1}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminSeedResponseMismatch, client.beginBaseBackup("http://ha-admin.test", .{
            .slot_name = "standby-a",
            .manifest_id = "base-a",
        }));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"held":true}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminFenceResponseMismatch, client.currentFence("http://ha-admin.test"));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"held":true,"receipt":{"identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":2,"epoch":2},"old_primary_id":"primary a","promoted_node_id":"standby-a","parent_timeline_id":1,"parent_epoch":1,"new_timeline_id":2,"new_epoch":2,"required_lsn":1,"observed_lsn":1,"generation":1,"forced":false,"token":"token","reason":""}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminFenceResponseMismatch, client.currentFence("http://ha-admin.test"));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"action":{"action_id":"promotion_assess:standby-a","action_kind":"promotion_assess","target":"standby-a ","state":"assessed","node_id":"standby-a"},"assessment":{"required_lsn":0,"received_lsn":0,"applied_lsn":0,"has_required_lsn":true,"caught_up_to_received":true,"fencing_confirmed":true,"force":false,"mode":"safe","data_loss_possible":false,"safe":true,"requires_fencing":false,"requires_force":false,"can_promote":true}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminPromotionResponseMismatch, client.assessPromotion("http://ha-admin.test", .{
            .required_lsn = 0,
            .fencing_confirmed = true,
            .force = false,
            .use_current_fence = false,
        }));
    }

    {
        var executor = StaticJsonExecutor{
            .body =
            \\{"schema_version":1,"decision":{"role":"primary","action":"mutate","identity":{"cluster_id":100,"shard_id":10,"table_id":20,"timeline_id":1,"epoch":1},"durable_lsn":1,"next_lsn":2}}
            ,
        };
        var client = Client.init(alloc, executor.executor());
        try std.testing.expectError(error.AdminGateResponseMismatch, client.checkWrite("http://ha-admin.test", .{
            .role = "primary",
            .expected_identity = testAdminIdentity(),
        }));
    }
}

test "storage.ha http client maps admin errors" {
    const alloc = std.testing.allocator;
    var server = http_admin.Server.init(alloc, .{});
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    try std.testing.expectError(error.HaEndpointNotReady, client.checkReady("http://ha-admin.test"));
    try std.testing.expectError(error.HaCommandConflict, client.executeCommand("http://ha-admin.test", &.{"identify"}));
    try std.testing.expectError(error.InvalidHaCommand, client.executeCommand("http://ha-admin.test", &.{"unknown"}));

    var auth_server = http_admin.Server.initWithOptions(alloc, .{}, .{ .bearer_token = "secret-token" });
    defer auth_server.deinit();
    var unauthenticated_client = Client.init(alloc, auth_server.executor());
    try std.testing.expectError(error.HaAdminUnauthorized, unauthenticated_client.getPrimaryStatus("http://ha-admin.test", .{}));
    try std.testing.expectError(error.HaAdminUnauthorized, unauthenticated_client.executeCommand("http://ha-admin.test", &.{"identify"}));

    var authenticated_client = Client.initWithOptions(alloc, auth_server.executor(), .{ .bearer_token = "secret-token" });
    try std.testing.expectError(error.HaCommandConflict, authenticated_client.getPrimaryStatus("http://ha-admin.test", .{}));
    try std.testing.expectError(error.HaCommandConflict, authenticated_client.executeCommand("http://ha-admin.test", &.{"identify"}));
}

test "storage.ha http client renders primary status sync query with OpenAPI enum spelling" {
    const alloc = std.testing.allocator;
    const standby_names = [_][]const u8{ "standby-a", "standby.b:z" };
    var uri = try std.fmt.allocPrint(alloc, "http://ha-admin.test{s}", .{admin_api.routes.ha_primary_status});
    uri = try appendQuerySyncPolicy(alloc, uri, .{
        .mode = .remote_write,
        .selection = .first,
        .required = 1,
        .standby_names = &standby_names,
        .failure_policy = .fail_closed,
    });
    defer alloc.free(uri);

    try expectContains(uri, "sync_mode=remote-write");
    try expectContains(uri, "sync_selection=first");
    try expectContains(uri, "sync_required=1");
    try expectContains(uri, "sync_failure=fail-closed");
    try expectContains(uri, "sync_standby=standby-a");
    try expectContains(uri, "sync_standby=standby.b%3Az");

    const all_names = [_][]const u8{ "standby-a", "standby-b" };
    var all_uri = try std.fmt.allocPrint(alloc, "http://ha-admin.test{s}", .{admin_api.routes.ha_primary_status});
    all_uri = try appendQuerySyncPolicy(alloc, all_uri, .{
        .mode = .remote_apply,
        .selection = .all,
        .required = all_names.len,
        .standby_names = &all_names,
        .failure_policy = .block,
    });
    defer alloc.free(all_uri);

    try expectContains(all_uri, "sync_mode=remote-apply");
    try expectContains(all_uri, "sync_selection=all");
    try expectContains(all_uri, "sync_standby=standby-a");
    try expectContains(all_uri, "sync_standby=standby-b");
    try std.testing.expect(std.mem.indexOf(u8, all_uri, "sync_required") == null);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
