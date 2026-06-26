// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const binder = @import("binder.zig");
const catalog_resources = @import("../api/catalog_resources.zig");
const db_mod = @import("../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const fingerprint = @import("fingerprint.zig");
const lower_expr = @import("lower_expr.zig");
const mem_backend = @import("../storage/mem_backend.zig");
const parser_context = @import("parser_context.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const schema_json = @import("schema_json.zig");
const schema_mutation = @import("schema_mutation.zig");
const tokenized = @import("tokenized.zig");
const transactions_mod = @import("../storage/transactions.zig");
const usermgr = @import("../usermgr/mod.zig");

pub const PreparedTransactionRecoveryOperation = enum {
    register_prepared,
    resolve_commit,
    resolve_rollback,
};

pub const PreparedTransactionRecoveryIntent = struct {
    operation: PreparedTransactionRecoveryOperation,
    gid: []const u8,
    requires_coordinator_recovery: bool = true,
    audit_action: ddl_plan.PreparedTransactionAction,
};

pub const PreparedTransactionCoordinatorResult = struct {
    operation: PreparedTransactionRecoveryOperation,
    gid: []const u8,
    txn_id: transactions_mod.TxnId,
    status: transactions_mod.TxnStatus,
    audit_action: ddl_plan.PreparedTransactionAction,
    coordinator_recovery_log: bool = true,
};

pub fn preparedTransactionRecoveryIntentFromPlan(plan: ddl_plan.PreparedTransactionPlan) PreparedTransactionRecoveryIntent {
    return .{
        .operation = switch (plan.action) {
            .prepare => .register_prepared,
            .commit => .resolve_commit,
            .rollback => .resolve_rollback,
        },
        .gid = plan.gid,
        .audit_action = plan.action,
    };
}

pub fn preparedTransactionRecoveryFingerprintAlloc(alloc: std.mem.Allocator, intent: PreparedTransactionRecoveryIntent) ![]const u8 {
    return try std.fmt.allocPrint(
        alloc,
        "prepared_txn_recovery:op={s}:gid={s}:audit={s}:requires_coordinator={}",
        .{ @tagName(intent.operation), intent.gid, @tagName(intent.audit_action), intent.requires_coordinator_recovery },
    );
}

pub fn preparedTransactionTxnIdFromGid(gid: []const u8) transactions_mod.TxnId {
    const Sha256 = std.crypto.hash.sha2.Sha256;
    var hasher = Sha256.init(.{});
    hasher.update("antfly.sql.prepared_transaction.gid.v1\x00");
    hasher.update(gid);
    var digest: [Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    var txn_id: transactions_mod.TxnId = undefined;
    @memcpy(&txn_id, digest[0..txn_id.len]);
    return txn_id;
}

pub fn executePreparedTransactionRecoveryPlan(
    alloc: std.mem.Allocator,
    store: anytype,
    plan: ddl_plan.PreparedTransactionPlan,
    timestamp: u64,
) !PreparedTransactionCoordinatorResult {
    return try executePreparedTransactionRecoveryIntent(
        alloc,
        store,
        preparedTransactionRecoveryIntentFromPlan(plan),
        timestamp,
    );
}

pub fn executePreparedTransactionRecoveryIntent(
    alloc: std.mem.Allocator,
    store: anytype,
    intent: PreparedTransactionRecoveryIntent,
    timestamp: u64,
) !PreparedTransactionCoordinatorResult {
    const txn_id = preparedTransactionTxnIdFromGid(intent.gid);
    var manager = try transactions_mod.TxnManager.init(alloc, store);
    defer manager.deinit();

    switch (intent.operation) {
        .register_prepared => {
            if (manager.getTransactionStatus(txn_id)) |_| {
                return error.PreparedTransactionAlreadyExists;
            } else |err| switch (err) {
                error.TxnNotFound => {},
                else => return err,
            }
            try manager.initTransaction(txn_id, timestamp);
            return .{
                .operation = intent.operation,
                .gid = intent.gid,
                .txn_id = txn_id,
                .status = .pending,
                .audit_action = intent.audit_action,
            };
        },
        .resolve_commit, .resolve_rollback => {
            const status: transactions_mod.TxnStatus = switch (intent.operation) {
                .resolve_commit => .committed,
                .resolve_rollback => .aborted,
                .register_prepared => unreachable,
            };
            manager.resolveIntents(txn_id, status, timestamp) catch |err| switch (err) {
                error.TxnNotFound => return error.PreparedTransactionNotFound,
                error.DecisionConflict => return error.PreparedTransactionDecisionConflict,
                else => return err,
            };
            return .{
                .operation = intent.operation,
                .gid = intent.gid,
                .txn_id = txn_id,
                .status = status,
                .audit_action = intent.audit_action,
            };
        },
    }
}

pub const OwnedSqlCatalogSession = struct {
    current_database_name: []u8,
    search_path: []const []const u8,
    transaction_local_search_path_base: ?[]const []const u8 = null,
    settings: []const catalog_resources.SqlSessionSetting = &.{},
    transaction_local_settings_base: ?[]const catalog_resources.SqlSessionSetting = null,
    transaction_local_search_path: bool = false,
    transaction_local_settings: bool = false,
    in_sql_transaction: bool = false,
    sql_transaction_failed: bool = false,
    request_read_only: bool = false,
    request_database_name_base: ?[]u8 = null,
    request_search_path_base: ?[]const []const u8 = null,
    notification_session_id: u64 = 0,

    pub fn fromSessionAlloc(alloc: std.mem.Allocator, source_session: catalog_resources.SqlCatalogSession) !OwnedSqlCatalogSession {
        const current_database_name = try alloc.dupe(u8, source_session.currentDatabase());
        errdefer alloc.free(current_database_name);
        const default_search_path: []const []const u8 = &.{catalog_resources.default_namespace_name};
        const source_path = if (source_session.search_path.len == 0) default_search_path else source_session.search_path;
        const search_path = try alloc.alloc([]const u8, source_path.len);
        var initialized: usize = 0;
        errdefer {
            for (search_path[0..initialized]) |name| alloc.free(@constCast(name));
            alloc.free(search_path);
        }
        for (source_path, 0..) |name, i| {
            search_path[i] = try alloc.dupe(u8, name);
            initialized += 1;
        }
        var settings = try alloc.alloc(catalog_resources.SqlSessionSetting, source_session.settings.len);
        var initialized_settings: usize = 0;
        errdefer {
            for (settings[0..initialized_settings]) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (settings.len > 0) alloc.free(settings);
        }
        for (source_session.settings, 0..) |setting, i| {
            settings[i] = .{
                .name = try alloc.dupe(u8, setting.name),
                .value = try alloc.dupe(u8, setting.value),
            };
            initialized_settings += 1;
        }
        return .{
            .current_database_name = current_database_name,
            .search_path = search_path,
            .transaction_local_search_path_base = null,
            .settings = settings,
            .transaction_local_settings_base = null,
            .transaction_local_search_path = false,
            .transaction_local_settings = false,
            .in_sql_transaction = false,
            .sql_transaction_failed = false,
            .request_read_only = false,
            .request_database_name_base = null,
            .request_search_path_base = null,
            .notification_session_id = 0,
        };
    }

    pub fn session(self: OwnedSqlCatalogSession) catalog_resources.SqlCatalogSession {
        return .{
            .current_database_name = self.current_database_name,
            .search_path = self.search_path,
            .settings = self.settings,
        };
    }

    pub fn cloneAlloc(self: OwnedSqlCatalogSession, alloc: std.mem.Allocator) !OwnedSqlCatalogSession {
        var cloned = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, self.session());
        errdefer cloned.deinit(alloc);
        if (self.transaction_local_search_path_base) |base| {
            cloned.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
            cloned.transaction_local_search_path = self.transaction_local_search_path;
        }
        if (self.transaction_local_settings_base) |base| {
            cloned.transaction_local_settings_base = try cloneSessionSettings(alloc, base);
            cloned.transaction_local_settings = self.transaction_local_settings;
        }
        cloned.in_sql_transaction = self.in_sql_transaction;
        cloned.sql_transaction_failed = self.sql_transaction_failed;
        cloned.request_read_only = self.request_read_only;
        if (self.request_database_name_base) |base| {
            cloned.request_database_name_base = try alloc.dupe(u8, base);
        }
        if (self.request_search_path_base) |base| {
            cloned.request_search_path_base = try cloneStringSlice(alloc, base);
        }
        cloned.notification_session_id = self.notification_session_id;
        return cloned;
    }

    pub fn cloneSearchPathAlloc(self: OwnedSqlCatalogSession, alloc: std.mem.Allocator) ![]const []const u8 {
        return try cloneStringSlice(alloc, self.search_path);
    }

    fn cloneRequestDatabaseBaseTo(self: OwnedSqlCatalogSession, alloc: std.mem.Allocator, target: *OwnedSqlCatalogSession) !void {
        if (self.request_database_name_base) |base| {
            target.request_database_name_base = try alloc.dupe(u8, base);
        }
    }

    fn cloneRequestSearchPathBaseTo(self: OwnedSqlCatalogSession, alloc: std.mem.Allocator, target: *OwnedSqlCatalogSession) !void {
        if (self.request_search_path_base) |base| {
            target.request_search_path_base = try cloneStringSlice(alloc, base);
        }
    }

    fn cloneRequestOverridesTo(self: OwnedSqlCatalogSession, alloc: std.mem.Allocator, target: *OwnedSqlCatalogSession) !void {
        target.request_read_only = self.request_read_only;
        try self.cloneRequestDatabaseBaseTo(alloc, target);
        try self.cloneRequestSearchPathBaseTo(alloc, target);
    }

    pub fn restoreRequestOverridesForPersistence(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.request_database_name_base) |base| {
            alloc.free(self.current_database_name);
            self.current_database_name = base;
            self.request_database_name_base = null;
        }
        if (self.request_search_path_base) |base| {
            for (self.search_path) |name| alloc.free(@constCast(name));
            if (self.search_path.len > 0) alloc.free(self.search_path);
            self.search_path = base;
            self.request_search_path_base = null;
        }
        self.request_read_only = false;
    }

    pub fn clearTransactionLocalState(self: *@This(), alloc: std.mem.Allocator) !void {
        if (self.transaction_local_search_path_base) |base| {
            for (self.search_path) |name| alloc.free(@constCast(name));
            if (self.search_path.len > 0) alloc.free(self.search_path);
            self.search_path = base;
            self.transaction_local_search_path_base = null;
        }
        self.transaction_local_search_path = false;
        if (self.transaction_local_settings_base) |base| {
            for (self.settings) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (self.settings.len > 0) alloc.free(self.settings);
            self.settings = base;
            self.transaction_local_settings_base = null;
        }
        self.transaction_local_settings = false;
        self.in_sql_transaction = false;
        self.sql_transaction_failed = false;
    }

    pub fn setTransactionLocalSettingAlloc(self: *@This(), alloc: std.mem.Allocator, name: []const u8, value: []const u8) !void {
        if (self.transaction_local_settings_base == null) {
            self.transaction_local_settings_base = try cloneSessionSettings(alloc, self.settings);
        }
        const settings = try replaceSessionSettingAlloc(alloc, self.settings, name, value);
        freeSessionSettings(alloc, self.settings);
        self.settings = settings;
        self.transaction_local_settings = true;
        self.in_sql_transaction = true;
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.current_database_name);
        for (self.search_path) |name| alloc.free(@constCast(name));
        if (self.search_path.len > 0) alloc.free(self.search_path);
        if (self.transaction_local_search_path_base) |base| {
            for (base) |name| alloc.free(@constCast(name));
            if (base.len > 0) alloc.free(base);
        }
        for (self.settings) |setting| {
            alloc.free(@constCast(setting.name));
            alloc.free(@constCast(setting.value));
        }
        if (self.settings.len > 0) alloc.free(self.settings);
        if (self.transaction_local_settings_base) |base| {
            for (base) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (base.len > 0) alloc.free(base);
        }
        if (self.request_database_name_base) |base| alloc.free(base);
        if (self.request_search_path_base) |base| freeStringSlice(alloc, base);
        self.* = undefined;
    }
};

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(@constCast(value));
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(values);
}

fn cloneSessionSettings(
    alloc: std.mem.Allocator,
    values: []const catalog_resources.SqlSessionSetting,
) ![]const catalog_resources.SqlSessionSetting {
    const out = try alloc.alloc(catalog_resources.SqlSessionSetting, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |setting| {
            alloc.free(@constCast(setting.name));
            alloc.free(@constCast(setting.value));
        }
        alloc.free(out);
    }
    for (values, 0..) |setting, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, setting.name),
            .value = try alloc.dupe(u8, setting.value),
        };
        initialized += 1;
    }
    return out;
}

fn freeSessionSettings(alloc: std.mem.Allocator, values: []const catalog_resources.SqlSessionSetting) void {
    for (values) |setting| {
        alloc.free(@constCast(setting.name));
        alloc.free(@constCast(setting.value));
    }
    if (values.len > 0) alloc.free(values);
}

pub fn parseSqlStatementTimeoutNs(value: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidRoleSetting;

    var digit_count: usize = 0;
    while (digit_count < trimmed.len and std.ascii.isDigit(trimmed[digit_count])) : (digit_count += 1) {}
    if (digit_count == 0) return error.InvalidRoleSetting;
    const amount = std.fmt.parseUnsigned(u64, trimmed[0..digit_count], 10) catch return error.InvalidRoleSetting;
    const unit = trimmed[digit_count..];

    const multiplier: u64 = if (unit.len == 0 or std.mem.eql(u8, unit, "ms"))
        std.time.ns_per_ms
    else if (std.mem.eql(u8, unit, "us"))
        std.time.ns_per_us
    else if (std.mem.eql(u8, unit, "s"))
        std.time.ns_per_s
    else if (std.mem.eql(u8, unit, "min"))
        60 * std.time.ns_per_s
    else if (std.mem.eql(u8, unit, "h"))
        60 * 60 * std.time.ns_per_s
    else
        return error.InvalidRoleSetting;
    return std.math.mul(u64, amount, multiplier) catch return error.InvalidRoleSetting;
}

pub fn sqlStatementTimeoutNsFromSession(session: catalog_resources.SqlCatalogSession) !?u64 {
    const raw = session.settingValue("statement_timeout") orelse return null;
    const timeout_ns = try parseSqlStatementTimeoutNs(raw);
    if (timeout_ns == 0) return null;
    return timeout_ns;
}

pub fn sqlSyncLevelFromSession(session: catalog_resources.SqlCatalogSession) !db_mod.types.SyncLevel {
    const raw = session.settingValue("antfly.sync_level") orelse return .write;
    return db_mod.types.parsePublicSyncLevelText(raw) orelse error.InvalidRoleSetting;
}

pub fn parseSqlBoolSetting(value: []const u8) !bool {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (std.ascii.eqlIgnoreCase(trimmed, "on") or
        std.ascii.eqlIgnoreCase(trimmed, "true") or
        std.ascii.eqlIgnoreCase(trimmed, "yes") or
        std.mem.eql(u8, trimmed, "1"))
    {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(trimmed, "off") or
        std.ascii.eqlIgnoreCase(trimmed, "false") or
        std.ascii.eqlIgnoreCase(trimmed, "no") or
        std.mem.eql(u8, trimmed, "0"))
    {
        return false;
    }
    return error.InvalidRoleSetting;
}

pub fn sqlDefaultTransactionReadOnlyFromSession(session: catalog_resources.SqlCatalogSession) !bool {
    const raw = session.settingValue("default_transaction_read_only") orelse return false;
    return try parseSqlBoolSetting(raw);
}

pub fn sqlTransactionReadOnlyFromSession(session: catalog_resources.SqlCatalogSession) !?bool {
    const raw = session.settingValue("transaction_read_only") orelse return null;
    return try parseSqlBoolSetting(raw);
}

pub fn sqlEffectiveTransactionReadOnlyFromSession(session: catalog_resources.SqlCatalogSession) !bool {
    if (try sqlTransactionReadOnlyFromSession(session)) |transaction_read_only| return transaction_read_only;
    return try sqlDefaultTransactionReadOnlyFromSession(session);
}

pub fn sqlStatementTimeoutExpired(timeout_ns: ?u64, start_ns: u64, now_ns: u64) bool {
    const limit = timeout_ns orelse return false;
    return now_ns -| start_ns >= limit;
}

pub fn enforceSqlStatementTimeoutAt(session: catalog_resources.SqlCatalogSession, start_ns: u64, now_ns: u64) !void {
    if (sqlStatementTimeoutExpired(try sqlStatementTimeoutNsFromSession(session), start_ns, now_ns)) return error.StatementTimeout;
}

pub fn validateSqlRuntimeSettingValue(name: []const u8, value: []const u8) !void {
    if (std.ascii.eqlIgnoreCase(name, "search_path")) {
        var iter = std.mem.splitScalar(u8, value, ',');
        var count: usize = 0;
        while (iter.next()) |part| : (count += 1) {
            const trimmed = std.mem.trim(u8, part, " \t\r\n");
            if (trimmed.len == 0) return error.InvalidRoleSetting;
            const normalized = if (trimmed.len >= 2 and trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"')
                trimmed[1 .. trimmed.len - 1]
            else
                trimmed;
            if (normalized.len == 0) return error.InvalidRoleSetting;
            for (normalized) |ch| {
                if (std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '$') continue;
                return error.InvalidRoleSetting;
            }
        }
        if (count == 0) return error.InvalidRoleSetting;
        return;
    }
    if (std.ascii.eqlIgnoreCase(name, "statement_timeout")) {
        _ = try parseSqlStatementTimeoutNs(value);
        return;
    }
    if (std.ascii.eqlIgnoreCase(name, "default_transaction_read_only") or
        std.ascii.eqlIgnoreCase(name, "transaction_read_only"))
    {
        _ = try parseSqlBoolSetting(value);
        return;
    }
    if (std.ascii.eqlIgnoreCase(name, "timezone")) {
        if (value.len == 0) return error.InvalidRoleSetting;
        for (value) |ch| {
            if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '/' or ch == '+' or ch == '-' or ch == ':' or ch == '.')) {
                return error.InvalidRoleSetting;
            }
        }
        return;
    }
    return error.UnsupportedRoleSetting;
}

pub fn validateSqlAntflySettingValue(name: []const u8, value: []const u8) !void {
    if (std.ascii.eqlIgnoreCase(name, "antfly.sync_level")) {
        _ = db_mod.types.parsePublicSyncLevelText(value) orelse return error.InvalidRoleSetting;
        return;
    }
    return error.UnsupportedRoleSetting;
}

pub fn validateSqlDatabaseSettingValue(name: []const u8, value: []const u8) !void {
    if (std.mem.startsWith(u8, name, "app.")) {
        usermgr.validateRoleSettingName(name) catch return error.UnsupportedRoleSetting;
        if (value.len == 0) return error.InvalidRoleSetting;
        return;
    }
    return try validateSqlRuntimeSettingValue(name, value);
}

fn replaceSessionSettingAlloc(
    alloc: std.mem.Allocator,
    settings: []const catalog_resources.SqlSessionSetting,
    name: []const u8,
    value: []const u8,
) ![]const catalog_resources.SqlSessionSetting {
    var found = false;
    for (settings) |setting| {
        if (std.ascii.eqlIgnoreCase(setting.name, name)) {
            found = true;
            break;
        }
    }
    const out_len = settings.len + @as(usize, @intFromBool(!found));
    const out = try alloc.alloc(catalog_resources.SqlSessionSetting, out_len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |setting| {
            alloc.free(@constCast(setting.name));
            alloc.free(@constCast(setting.value));
        }
        alloc.free(out);
    }
    var wrote_new = false;
    for (settings) |setting| {
        if (std.ascii.eqlIgnoreCase(setting.name, name)) {
            out[initialized] = .{
                .name = try alloc.dupe(u8, name),
                .value = try alloc.dupe(u8, value),
            };
            wrote_new = true;
        } else {
            out[initialized] = .{
                .name = try alloc.dupe(u8, setting.name),
                .value = try alloc.dupe(u8, setting.value),
            };
        }
        initialized += 1;
    }
    if (!wrote_new) {
        out[initialized] = .{
            .name = try alloc.dupe(u8, name),
            .value = try alloc.dupe(u8, value),
        };
        initialized += 1;
    }
    return out;
}

fn removeSessionSettingAlloc(
    alloc: std.mem.Allocator,
    settings: []const catalog_resources.SqlSessionSetting,
    name: []const u8,
) ![]const catalog_resources.SqlSessionSetting {
    var kept: usize = 0;
    for (settings) |setting| {
        if (!std.ascii.eqlIgnoreCase(setting.name, name)) kept += 1;
    }
    const out = try alloc.alloc(catalog_resources.SqlSessionSetting, kept);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |setting| {
            alloc.free(@constCast(setting.name));
            alloc.free(@constCast(setting.value));
        }
        alloc.free(out);
    }
    for (settings) |setting| {
        if (std.ascii.eqlIgnoreCase(setting.name, name)) continue;
        out[initialized] = .{
            .name = try alloc.dupe(u8, setting.name),
            .value = try alloc.dupe(u8, setting.value),
        };
        initialized += 1;
    }
    return out;
}

pub fn applySessionCatalogPlanAlloc(
    alloc: std.mem.Allocator,
    session: catalog_resources.SqlCatalogSession,
    plan: ddl_plan.SessionCatalogPlan,
) !OwnedSqlCatalogSession {
    var owned = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session);
    defer owned.deinit(alloc);
    return try applyOwnedSessionCatalogPlanAlloc(alloc, owned, plan);
}

pub fn applyOwnedSessionCatalogPlanAlloc(
    alloc: std.mem.Allocator,
    session: OwnedSqlCatalogSession,
    plan: ddl_plan.SessionCatalogPlan,
) !OwnedSqlCatalogSession {
    switch (plan) {
        .set_search_path => |set| {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            updated.request_read_only = session.request_read_only;
            updated.in_sql_transaction = session.in_sql_transaction or set.local;
            updated.sql_transaction_failed = session.sql_transaction_failed;
            try session.cloneRequestDatabaseBaseTo(alloc, &updated);
            if (set.local) {
                if (session.transaction_local_search_path_base) |base| {
                    updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                } else if (session.request_search_path_base) |base| {
                    updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                } else {
                    updated.transaction_local_search_path_base = try cloneStringSlice(alloc, session.search_path);
                }
            }
            for (updated.search_path) |name| alloc.free(@constCast(name));
            if (updated.search_path.len > 0) alloc.free(updated.search_path);
            const search_path = try alloc.alloc([]const u8, set.namespaces.len);
            var initialized: usize = 0;
            errdefer {
                for (search_path[0..initialized]) |name| alloc.free(@constCast(name));
                alloc.free(search_path);
            }
            for (set.namespaces, 0..) |name, i| {
                search_path[i] = try alloc.dupe(u8, name);
                initialized += 1;
            }
            updated.search_path = search_path;
            updated.transaction_local_search_path = set.local;
            if (!set.local) {
                if (updated.transaction_local_search_path_base) |base| freeStringSlice(alloc, base);
                updated.transaction_local_search_path_base = null;
            }
            return updated;
        },
        .set_setting => |set| {
            switch (set.kind) {
                .app => if (set.value.len == 0) return error.InvalidRoleSetting,
                .antfly => try validateSqlAntflySettingValue(set.name, set.value),
                .runtime => try validateSqlRuntimeSettingValue(set.name, set.value),
            }
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            try session.cloneRequestOverridesTo(alloc, &updated);
            updated.in_sql_transaction = session.in_sql_transaction or set.local;
            updated.sql_transaction_failed = session.sql_transaction_failed;
            if (session.transaction_local_search_path_base) |base| {
                updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                updated.transaction_local_search_path = session.transaction_local_search_path;
            }
            if (set.local) {
                if (session.transaction_local_settings_base) |base| {
                    updated.transaction_local_settings_base = try cloneSessionSettings(alloc, base);
                } else {
                    updated.transaction_local_settings_base = try cloneSessionSettings(alloc, session.settings);
                }
            }
            const settings = try replaceSessionSettingAlloc(alloc, updated.settings, set.name, set.value);
            for (updated.settings) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (updated.settings.len > 0) alloc.free(updated.settings);
            updated.settings = settings;
            updated.transaction_local_settings = set.local;
            if (!set.local) {
                if (updated.transaction_local_settings_base) |base| freeSessionSettings(alloc, base);
                updated.transaction_local_settings_base = null;
            }
            return updated;
        },
        .reset_setting => |reset| {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            try session.cloneRequestOverridesTo(alloc, &updated);
            updated.in_sql_transaction = session.in_sql_transaction;
            updated.sql_transaction_failed = session.sql_transaction_failed;
            if (session.transaction_local_search_path_base) |base| {
                updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                updated.transaction_local_search_path = session.transaction_local_search_path;
            }
            if (session.transaction_local_settings_base) |base| {
                updated.transaction_local_settings_base = try cloneSessionSettings(alloc, base);
                updated.transaction_local_settings = session.transaction_local_settings;
            }
            const settings = try removeSessionSettingAlloc(alloc, updated.settings, reset.name);
            for (updated.settings) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (updated.settings.len > 0) alloc.free(updated.settings);
            updated.settings = settings;
            return updated;
        },
        .reset_search_path => {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, .{
                .current_database_name = session.session().currentDatabase(),
                .search_path = &.{catalog_resources.default_namespace_name},
                .settings = session.settings,
            });
            errdefer updated.deinit(alloc);
            updated.request_read_only = session.request_read_only;
            updated.in_sql_transaction = session.in_sql_transaction;
            updated.sql_transaction_failed = session.sql_transaction_failed;
            try session.cloneRequestDatabaseBaseTo(alloc, &updated);
            if (session.transaction_local_settings_base) |base| {
                updated.transaction_local_settings_base = try cloneSessionSettings(alloc, base);
                updated.transaction_local_settings = session.transaction_local_settings;
            }
            return updated;
        },
        .discard_all => {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, .{
                .current_database_name = session.session().currentDatabase(),
                .search_path = &.{catalog_resources.default_namespace_name},
            });
            errdefer updated.deinit(alloc);
            updated.request_read_only = session.request_read_only;
            updated.in_sql_transaction = false;
            updated.sql_transaction_failed = false;
            try session.cloneRequestDatabaseBaseTo(alloc, &updated);
            return updated;
        },
        .show_search_path => {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            try session.cloneRequestOverridesTo(alloc, &updated);
            updated.in_sql_transaction = session.in_sql_transaction;
            updated.sql_transaction_failed = session.sql_transaction_failed;
            if (session.transaction_local_search_path_base) |base| {
                updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                updated.transaction_local_search_path = session.transaction_local_search_path;
            }
            if (session.transaction_local_settings_base) |base| {
                updated.transaction_local_settings_base = try cloneSessionSettings(alloc, base);
                updated.transaction_local_settings = session.transaction_local_settings;
            }
            return updated;
        },
    }
}

pub fn applyTableDdlPlanToRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: binder.TableDdlLogicalPlan,
) !runtime_schema.TableSchema {
    return switch (plan) {
        .moved => error.UnsupportedSqlShape,
        .create_table => |create_table| applyCreateTablePlanAlloc(alloc, current, create_table),
        .table_clone => |table_clone| blk: {
            var create_table = try ddl_plan.createTablePlanFromTableCloneSourceAlloc(alloc, current, table_clone);
            defer create_table.deinit(alloc);
            break :blk try ddl_plan.runtimeSchemaFromCreateTablePlanAlloc(alloc, create_table);
        },
        .view_catalog => error.UnsupportedSqlShape,
        .materialized_view_catalog => error.UnsupportedSqlShape,
        .relation_lifetime => error.UnsupportedSqlShape,
        .table_partition_catalog => error.UnsupportedSqlShape,
        .create_index => |create_index| applyCreateIndexPlanAlloc(alloc, current, create_index),
        .drop_index => |drop_index| applyDropIndexPlanAlloc(alloc, current, drop_index),
        .drop_table => |drop_table| applyDropTablePlanAlloc(alloc, current, drop_table),
        .alter_table => |alter_table| applyAlterTablePlanAlloc(alloc, current, alter_table),
        .create_update_policy => |update_policy| applyCreateUpdatePolicyPlanAlloc(alloc, current, update_policy),
    };
}

pub fn applyLogicalDdlPlanToRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: binder.LogicalSqlPlan,
) !runtime_schema.TableSchema {
    return switch (plan) {
        .table_ddl => |table_plan| try applyTableDdlPlanToRuntimeSchemaAlloc(alloc, current, table_plan),
        .catalog_ddl => |catalog_plan| switch (catalog_plan) {
            .comment_metadata => |comment| applyCommentMetadataPlanAlloc(alloc, current, comment),
            else => error.UnsupportedSqlShape,
        },
        .other_ddl, .session => if (current.storage_mode == .relational)
            ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current)
        else
            ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, current),
        else => error.UnsupportedSqlShape,
    };
}

pub fn applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.LoweredDdlPlan,
) !runtime_schema.TableSchema {
    return switch (plan) {
        .adapter_noop, .session_catalog => if (current.storage_mode == .relational)
            ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current)
        else
            ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, current),
        .create_table => |create_table| applyCreateTablePlanAlloc(alloc, current, create_table),
        .table_clone => |table_clone| blk: {
            var create_table = try ddl_plan.createTablePlanFromTableCloneSourceAlloc(alloc, current, table_clone);
            defer create_table.deinit(alloc);
            break :blk try ddl_plan.runtimeSchemaFromCreateTablePlanAlloc(alloc, create_table);
        },
        .view_catalog => error.UnsupportedSqlShape,
        .materialized_view_catalog => error.UnsupportedSqlShape,
        .relation_lifetime => error.UnsupportedSqlShape,
        .enum_type_catalog => error.UnsupportedSqlShape,
        .domain_catalog => error.UnsupportedSqlShape,
        .sequence_catalog => error.UnsupportedSqlShape,
        .identity_allocator_catalog => error.UnsupportedSqlShape,
        .schema_namespace_catalog => error.UnsupportedSqlShape,
        .extension_catalog => error.UnsupportedSqlShape,
        .function_catalog => error.UnsupportedSqlShape,
        .trigger_catalog => error.UnsupportedSqlShape,
        .procedure_call => error.UnsupportedSqlShape,
        .authorization_catalog => error.UnsupportedSqlShape,
        .bulk_io => error.UnsupportedSqlShape,
        .table_partition_catalog => error.UnsupportedSqlShape,
        .row_security_catalog => error.UnsupportedSqlShape,
        .database_catalog => error.UnsupportedSqlShape,
        .tablespace_catalog => error.UnsupportedSqlShape,
        .notification_channel => error.UnsupportedSqlShape,
        .logical_replication => error.UnsupportedSqlShape,
        .type_system_catalog => error.UnsupportedSqlShape,
        .maintenance_job => error.UnsupportedSqlShape,
        .prepared_statement => error.UnsupportedSqlShape,
        .prepared_transaction => error.UnsupportedSqlShape,
        .cursor_portal => error.UnsupportedSqlShape,
        .savepoint_transaction => error.UnsupportedSqlShape,
        .comment_metadata => |comment| applyCommentMetadataPlanAlloc(alloc, current, comment),
        .transaction_control => error.UnsupportedSqlShape,
        .create_index => |create_index| applyCreateIndexPlanAlloc(alloc, current, create_index),
        .drop_index => |drop_index| applyDropIndexPlanAlloc(alloc, current, drop_index),
        .drop_table => |drop_table| applyDropTablePlanAlloc(alloc, current, drop_table),
        .alter_table => |alter_table| applyAlterTablePlanAlloc(alloc, current, alter_table),
        .create_update_policy => |update_policy| applyCreateUpdatePolicyPlanAlloc(alloc, current, update_policy),
    };
}

const AppliedDdlRewriteExpressionSource = struct {
    target_column: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
};

const AppliedDdlRowRewritePlanSource = struct {
    renames: []const ddl_plan.AppliedDdlRowRewriteRename = &.{},
    drops: []const []const u8 = &.{},

    fn empty(self: @This()) bool {
        return self.renames.len == 0 and self.drops.len == 0;
    }
};

fn valueRowExpressionAlloc(
    alloc: std.mem.Allocator,
    value_json: []const u8,
) !db_mod.types.RelationalRowsExpression {
    const field = try alloc.dupe(u8, "");
    errdefer alloc.free(field);
    const owned_value_json = try alloc.dupe(u8, value_json);
    errdefer alloc.free(owned_value_json);
    const json_path = try alloc.dupe(u8, "");
    errdefer alloc.free(json_path);
    return .{
        .kind = .value,
        .field = field,
        .value_json = owned_value_json,
        .json_path = json_path,
    };
}

fn fieldRowExpressionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
) !db_mod.types.RelationalRowsExpression {
    const owned_field = try alloc.dupe(u8, field);
    errdefer alloc.free(owned_field);
    const value_json = try alloc.dupe(u8, "");
    errdefer alloc.free(value_json);
    const json_path = try alloc.dupe(u8, "");
    errdefer alloc.free(json_path);
    return .{
        .kind = .field,
        .field = owned_field,
        .value_json = value_json,
        .json_path = json_path,
    };
}

fn unaryGeneratedRowExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    field: []const u8,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    errdefer alloc.free(operands);
    operands[0] = try fieldRowExpressionAlloc(alloc, field);
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, operands[0]);
    const owned_field = try alloc.dupe(u8, "");
    errdefer alloc.free(owned_field);
    const value_json = try alloc.dupe(u8, "");
    errdefer alloc.free(value_json);
    const json_path = try alloc.dupe(u8, "");
    errdefer alloc.free(json_path);
    return .{
        .kind = kind,
        .field = owned_field,
        .value_json = value_json,
        .json_path = json_path,
        .operands = operands,
    };
}

fn concatGeneratedRowExpressionAlloc(
    alloc: std.mem.Allocator,
    generated: runtime_schema.RelationalGeneratedValue,
) !db_mod.types.RelationalRowsExpression {
    if (generated.fields.len == 0) return error.UnsupportedSqlShape;
    const operand_count = switch (generated.op) {
        .concat => generated.fields.len + if (generated.separator.len != 0 and generated.fields.len > 1) generated.fields.len - 1 else 0,
        .concat_ws => generated.fields.len + 1,
        else => return error.UnsupportedSqlShape,
    };
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, operand_count);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
        alloc.free(operands);
    }

    if (generated.op == .concat_ws) {
        const separator_json = try std.json.Stringify.valueAlloc(alloc, generated.separator, .{});
        defer alloc.free(separator_json);
        operands[initialized] = try valueRowExpressionAlloc(alloc, separator_json);
        initialized += 1;
    }

    for (generated.fields, 0..) |field, i| {
        if (generated.op == .concat and i != 0 and generated.separator.len != 0) {
            const separator_json = try std.json.Stringify.valueAlloc(alloc, generated.separator, .{});
            defer alloc.free(separator_json);
            operands[initialized] = try valueRowExpressionAlloc(alloc, separator_json);
            initialized += 1;
        }
        operands[initialized] = try fieldRowExpressionAlloc(alloc, field);
        initialized += 1;
    }

    return .{
        .kind = switch (generated.op) {
            .concat => .concat,
            .concat_ws => .concat_ws,
            else => return error.UnsupportedSqlShape,
        },
        .operands = operands,
    };
}

fn generatedBackfillRowExpressionAlloc(
    alloc: std.mem.Allocator,
    generated: runtime_schema.RelationalGeneratedValue,
) !db_mod.types.RelationalRowsExpression {
    return switch (generated.op) {
        .lower => try unaryGeneratedRowExpressionAlloc(alloc, .lower, generated.field orelse return error.UnsupportedSqlShape),
        .upper => try unaryGeneratedRowExpressionAlloc(alloc, .upper, generated.field orelse return error.UnsupportedSqlShape),
        .md5 => try unaryGeneratedRowExpressionAlloc(alloc, .md5, generated.field orelse return error.UnsupportedSqlShape),
        .concat, .concat_ws => try concatGeneratedRowExpressionAlloc(alloc, generated),
        .expression => try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, generated.expression orelse return error.UnsupportedSqlShape),
    };
}

fn addColumnRewriteExpressionSourceAlloc(
    alloc: std.mem.Allocator,
    add_column: ddl_plan.AddColumnOperation,
) !?AppliedDdlRewriteExpressionSource {
    if (add_column.column.generated) |generated| {
        const expression = generatedBackfillRowExpressionAlloc(alloc, generated) catch |err| switch (err) {
            error.UnsupportedSqlShape => return null,
            else => return err,
        };
        return .{
            .target_column = add_column.column.name,
            .expression = expression,
        };
    }
    if (add_column.column.default_value) |default_value| {
        if (default_value.kind != .literal) return null;
        return .{
            .target_column = add_column.column.name,
            .expression = try valueRowExpressionAlloc(alloc, default_value.value_json),
        };
    }
    return null;
}

fn freeAppliedDdlRewriteExpressionSource(
    alloc: std.mem.Allocator,
    source: AppliedDdlRewriteExpressionSource,
) void {
    runtime_schema.freeRelationalRowsExpression(alloc, source.expression);
}

fn freeAppliedDdlRowRewritePlanSource(
    alloc: std.mem.Allocator,
    source: AppliedDdlRowRewritePlanSource,
) void {
    for (source.renames) |rename| {
        alloc.free(rename.old_path);
        alloc.free(rename.new_path);
    }
    alloc.free(source.renames);
    for (source.drops) |drop| alloc.free(drop);
    alloc.free(source.drops);
}

fn alterTableRewriteExpressionSourceAlloc(alloc: std.mem.Allocator, plan: ddl_plan.AlterTablePlan) !?AppliedDdlRewriteExpressionSource {
    var out: ?AppliedDdlRewriteExpressionSource = null;
    errdefer if (out) |source| freeAppliedDdlRewriteExpressionSource(alloc, source);
    for (plan.operations) |operation| switch (operation) {
        .alter_column_type => |alter_type| {
            const rewrite = alter_type.rewrite_expression orelse continue;
            if (out != null) return error.UnsupportedSqlShape;
            out = .{
                .target_column = alter_type.column_name,
                .expression = try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, rewrite.expression),
            };
        },
        .add_column => |add_column| {
            const rewrite = (try addColumnRewriteExpressionSourceAlloc(alloc, add_column)) orelse continue;
            if (out != null) {
                freeAppliedDdlRewriteExpressionSource(alloc, rewrite);
                return error.UnsupportedSqlShape;
            }
            out = rewrite;
        },
        else => {},
    };
    const result = out;
    out = null;
    return result;
}

fn relationalColumnPathExists(columns: []const runtime_schema.RelationalColumn, path: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, path)) return true;
    }
    return false;
}

fn rowRewritePlanSourceRenameOldPath(plan: AppliedDdlRowRewritePlanSource, path: []const u8) bool {
    for (plan.renames) |rename| {
        if (std.mem.eql(u8, rename.old_path, path)) return true;
    }
    return false;
}

fn alterTableRowRewritePlanSourceAlloc(
    alloc: std.mem.Allocator,
    old_schema: runtime_schema.TableSchema,
    new_schema: runtime_schema.TableSchema,
    plan: ddl_plan.AlterTablePlan,
) !AppliedDdlRowRewritePlanSource {
    if (old_schema.storage_mode != .relational or new_schema.storage_mode != .relational) return error.InvalidSqlCatalog;
    var renames = std.ArrayListUnmanaged(ddl_plan.AppliedDdlRowRewriteRename).empty;
    var drops = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (renames.items) |rename| {
            alloc.free(rename.old_path);
            alloc.free(rename.new_path);
        }
        renames.deinit(alloc);
        for (drops.items) |drop| alloc.free(drop);
        drops.deinit(alloc);
    }

    for (plan.operations) |operation| switch (operation) {
        .rename_column => |rename| {
            const old_path = try alloc.dupe(u8, rename.old_name);
            errdefer alloc.free(old_path);
            const new_path = try alloc.dupe(u8, rename.new_name);
            errdefer alloc.free(new_path);
            try renames.append(alloc, .{ .old_path = old_path, .new_path = new_path });
        },
        else => {},
    };

    const partial_plan = AppliedDdlRowRewritePlanSource{ .renames = renames.items, .drops = drops.items };
    for (old_schema.relational_columns) |old_column| {
        if (relationalColumnPathExists(new_schema.relational_columns, old_column.path)) continue;
        if (rowRewritePlanSourceRenameOldPath(partial_plan, old_column.path)) continue;
        try drops.append(alloc, try alloc.dupe(u8, old_column.path));
    }

    const owned_renames = try renames.toOwnedSlice(alloc);
    errdefer {
        for (owned_renames) |rename| {
            alloc.free(rename.old_path);
            alloc.free(rename.new_path);
        }
        alloc.free(owned_renames);
    }
    const owned_drops = try drops.toOwnedSlice(alloc);
    return .{
        .renames = owned_renames,
        .drops = owned_drops,
    };
}

pub fn applyLoweredDdlPlanToSchemaJsonForTestAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    plan: ddl_plan.LoweredDdlPlan,
) !ddl_plan.AppliedDdlSchemaJson {
    switch (plan) {
        .adapter_noop, .session_catalog => return .{ .schema_json = try alloc.dupe(u8, current_schema_json) },
        .create_table => |create_table| {
            try validateTemporalForeignKeysSupported(create_table.foreign_keys);
            if (current_schema_json.len != 0) {
                if (create_table.replace_existing) return try appliedDdlSchemaJsonWithFlagsAlloc(
                    alloc,
                    try schema_json.schemaJsonFromCreateTablePlanAlloc(alloc, create_table),
                    true,
                    true,
                    true,
                    null,
                    .{},
                );
                if (!create_table.if_not_exists) return error.InvalidSqlCatalog;
                return .{ .schema_json = try alloc.dupe(u8, current_schema_json) };
            }
            return .{ .schema_json = try schema_json.schemaJsonFromCreateTablePlanAlloc(alloc, create_table) };
        },
        .table_clone => |table_clone| return try appliedDdlSchemaJsonWithFlagsAlloc(
            alloc,
            try schema_json.schemaJsonFromTableClonePlanAlloc(alloc, current_schema_json, table_clone),
            table_clone.options.indexes,
            table_clone.options.constraints or table_clone.options.checks,
            false,
            null,
            .{},
        ),
        .drop_table => |drop_table| {
            if (current_schema_json.len == 0) {
                if (drop_table.if_exists) return .{ .schema_json = try alloc.dupe(u8, "") };
                return error.InvalidSqlCatalog;
            }
            try schema_json.validateDdlAppliedSchemaJsonAlloc(alloc, current_schema_json);
            return .{ .schema_json = try alloc.dupe(u8, "") };
        },
        .view_catalog => return error.UnsupportedSqlShape,
        .materialized_view_catalog => return error.UnsupportedSqlShape,
        .relation_lifetime => return error.UnsupportedSqlShape,
        .enum_type_catalog => return error.UnsupportedSqlShape,
        .domain_catalog => return error.UnsupportedSqlShape,
        .sequence_catalog => return error.UnsupportedSqlShape,
        .identity_allocator_catalog => return error.UnsupportedSqlShape,
        .schema_namespace_catalog => return error.UnsupportedSqlShape,
        .extension_catalog => return error.UnsupportedSqlShape,
        .function_catalog => return error.UnsupportedSqlShape,
        .trigger_catalog => return error.UnsupportedSqlShape,
        .procedure_call => return error.UnsupportedSqlShape,
        .authorization_catalog => return error.UnsupportedSqlShape,
        .bulk_io => return error.UnsupportedSqlShape,
        .table_partition_catalog => return error.UnsupportedSqlShape,
        .row_security_catalog => return error.UnsupportedSqlShape,
        .database_catalog => return error.UnsupportedSqlShape,
        .tablespace_catalog => return error.UnsupportedSqlShape,
        .notification_channel => return error.UnsupportedSqlShape,
        .logical_replication => return error.UnsupportedSqlShape,
        .type_system_catalog => return error.UnsupportedSqlShape,
        .maintenance_job => return error.UnsupportedSqlShape,
        .prepared_statement => return error.UnsupportedSqlShape,
        .prepared_transaction => return error.UnsupportedSqlShape,
        .cursor_portal => return error.UnsupportedSqlShape,
        .savepoint_transaction => return error.UnsupportedSqlShape,
        .transaction_control => return error.UnsupportedSqlShape,
        .create_index, .drop_index, .alter_table, .create_update_policy, .comment_metadata => {},
    }

    if (current_schema_json.len == 0) {
        return switch (plan) {
            .adapter_noop, .session_catalog => unreachable,
            .drop_index => |drop_index| if (drop_index.if_exists) .{ .schema_json = try alloc.dupe(u8, current_schema_json) } else error.InvalidSqlCatalog,
            .alter_table => |alter_table| if (alter_table.if_exists) .{ .schema_json = try alloc.dupe(u8, current_schema_json) } else error.InvalidSqlCatalog,
            .create_table => unreachable,
            .table_clone => unreachable,
            .view_catalog => unreachable,
            .materialized_view_catalog => unreachable,
            .relation_lifetime => unreachable,
            .enum_type_catalog => unreachable,
            .domain_catalog => unreachable,
            .sequence_catalog => unreachable,
            .identity_allocator_catalog => unreachable,
            .schema_namespace_catalog => unreachable,
            .extension_catalog => unreachable,
            .function_catalog => unreachable,
            .trigger_catalog => unreachable,
            .procedure_call => unreachable,
            .authorization_catalog => unreachable,
            .bulk_io => unreachable,
            .table_partition_catalog => unreachable,
            .row_security_catalog => unreachable,
            .database_catalog => unreachable,
            .tablespace_catalog => unreachable,
            .notification_channel => unreachable,
            .logical_replication => unreachable,
            .type_system_catalog => unreachable,
            .maintenance_job => unreachable,
            .prepared_statement => unreachable,
            .prepared_transaction => unreachable,
            .cursor_portal => unreachable,
            .savepoint_transaction => unreachable,
            .comment_metadata => error.InvalidSqlCatalog,
            .transaction_control => unreachable,
            .drop_table => unreachable,
            .create_index, .create_update_policy => error.InvalidSqlCatalog,
        };
    }
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena, current_schema_json, .{});
    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSqlCatalog,
    };

    var result: ddl_plan.AppliedDdlSchemaJson = .{ .schema_json = &.{} };
    switch (plan) {
        .adapter_noop, .session_catalog => unreachable,
        .create_table => unreachable,
        .table_clone => unreachable,
        .view_catalog => unreachable,
        .materialized_view_catalog => unreachable,
        .relation_lifetime => unreachable,
        .enum_type_catalog => unreachable,
        .domain_catalog => unreachable,
        .sequence_catalog => unreachable,
        .identity_allocator_catalog => unreachable,
        .schema_namespace_catalog => unreachable,
        .extension_catalog => unreachable,
        .function_catalog => unreachable,
        .trigger_catalog => unreachable,
        .procedure_call => unreachable,
        .authorization_catalog => unreachable,
        .bulk_io => unreachable,
        .table_partition_catalog => unreachable,
        .row_security_catalog => unreachable,
        .database_catalog => unreachable,
        .tablespace_catalog => unreachable,
        .notification_channel => unreachable,
        .logical_replication => unreachable,
        .type_system_catalog => unreachable,
        .maintenance_job => unreachable,
        .prepared_statement => unreachable,
        .prepared_transaction => unreachable,
        .cursor_portal => unreachable,
        .savepoint_transaction => unreachable,
        .comment_metadata => |comment| try schema_json.applyCommentMetadataPlanToSchemaJsonValue(arena, root, comment),
        .transaction_control => unreachable,
        .drop_table => unreachable,
        .create_index => |create_index| {
            const changed = try schema_json.applyCreateIndexPlanToSchemaJsonValue(arena, root, create_index);
            result.requires_rebuild = changed;
            result.validation_required = changed and create_index.unique;
        },
        .drop_index => |drop_index| try schema_json.applyDropIndexPlanToSchemaJsonValue(arena, root, drop_index),
        .alter_table => |alter_table| {
            const flags = try alterTablePlanWorkFlagsForSchemaJson(root, alter_table);
            result.requires_rebuild = flags.requires_rebuild;
            result.validation_required = flags.validation_required;
            result.rewrite_required = flags.rewrite_required;
            try schema_json.applyAlterTablePlanToSchemaJsonValue(arena, root, alter_table);
        },
        .create_update_policy => |update_policy| {
            try schema_json.applyCreateUpdatePolicyPlanToSchemaJsonValue(arena, root, update_policy);
        },
    }
    const updated_schema_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(updated_schema_json);
    const rewrite_source = switch (plan) {
        .alter_table => |alter_table| try alterTableRewriteExpressionSourceAlloc(alloc, alter_table),
        else => null,
    };
    defer if (rewrite_source) |source| freeAppliedDdlRewriteExpressionSource(alloc, source);
    const row_rewrite_source = switch (plan) {
        .alter_table => |alter_table| blk: {
            const old_schema = try schema_json.runtimeSchemaFromSchemaJsonAlloc(alloc, current_schema_json);
            defer runtime_schema.freeSchema(alloc, old_schema);
            const new_schema = try schema_json.runtimeSchemaFromSchemaJsonAlloc(alloc, updated_schema_json);
            defer runtime_schema.freeSchema(alloc, new_schema);
            break :blk try alterTableRowRewritePlanSourceAlloc(alloc, old_schema, new_schema, alter_table);
        },
        else => AppliedDdlRowRewritePlanSource{},
    };
    defer freeAppliedDdlRowRewritePlanSource(alloc, row_rewrite_source);
    if (rewrite_source != null and !row_rewrite_source.empty()) return error.UnsupportedSqlShape;
    result = try appliedDdlSchemaJsonWithFlagsAlloc(
        alloc,
        updated_schema_json,
        result.requires_rebuild,
        result.validation_required,
        result.rewrite_required,
        rewrite_source,
        row_rewrite_source,
    );
    errdefer result.deinit(alloc);
    try schema_json.validateDdlAppliedSchemaJsonAlloc(alloc, result.schema_json);
    return result;
}

pub fn applyLogicalDdlPlanToSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    plan: binder.LogicalSqlPlan,
) !ddl_plan.AppliedDdlSchemaJson {
    return switch (plan) {
        .table_ddl => |table_plan| try applyTableDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, table_plan),
        .catalog_ddl => |catalog_plan| switch (catalog_plan) {
            .comment_metadata => |comment| try applyCommentMetadataPlanToSchemaJsonAlloc(alloc, current_schema_json, comment),
            else => error.UnsupportedSqlShape,
        },
        .other_ddl, .session => .{ .schema_json = try alloc.dupe(u8, current_schema_json) },
        else => error.UnsupportedSqlShape,
    };
}

fn applyCommentMetadataPlanToSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    comment: ddl_plan.CommentMetadataPlan,
) !ddl_plan.AppliedDdlSchemaJson {
    if (current_schema_json.len == 0) return error.InvalidSqlCatalog;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena, current_schema_json, .{});
    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSqlCatalog,
    };
    try schema_json.applyCommentMetadataPlanToSchemaJsonValue(arena, root, comment);
    const updated_schema_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(updated_schema_json);
    try schema_json.validateDdlAppliedSchemaJsonAlloc(alloc, updated_schema_json);
    return .{ .schema_json = updated_schema_json };
}

pub fn applyTableDdlPlanToSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    plan: binder.TableDdlLogicalPlan,
) !ddl_plan.AppliedDdlSchemaJson {
    switch (plan) {
        .moved => return error.UnsupportedSqlShape,
        .create_table => |create_table| {
            try validateTemporalForeignKeysSupported(create_table.foreign_keys);
            if (current_schema_json.len != 0) {
                if (create_table.replace_existing) return try appliedDdlSchemaJsonWithFlagsAlloc(
                    alloc,
                    try schema_json.schemaJsonFromCreateTablePlanAlloc(alloc, create_table),
                    true,
                    true,
                    true,
                    null,
                    .{},
                );
                if (!create_table.if_not_exists) return error.InvalidSqlCatalog;
                return .{ .schema_json = try alloc.dupe(u8, current_schema_json) };
            }
            return .{ .schema_json = try schema_json.schemaJsonFromCreateTablePlanAlloc(alloc, create_table) };
        },
        .table_clone => |table_clone| return try appliedDdlSchemaJsonWithFlagsAlloc(
            alloc,
            try schema_json.schemaJsonFromTableClonePlanAlloc(alloc, current_schema_json, table_clone),
            table_clone.options.indexes,
            table_clone.options.constraints or table_clone.options.checks,
            false,
            null,
            .{},
        ),
        .drop_table => |drop_table| {
            if (current_schema_json.len == 0) {
                if (drop_table.if_exists) return .{ .schema_json = try alloc.dupe(u8, "") };
                return error.InvalidSqlCatalog;
            }
            try schema_json.validateDdlAppliedSchemaJsonAlloc(alloc, current_schema_json);
            return .{ .schema_json = try alloc.dupe(u8, "") };
        },
        .view_catalog,
        .materialized_view_catalog,
        .relation_lifetime,
        .table_partition_catalog,
        => return error.UnsupportedSqlShape,
        .create_index, .drop_index, .alter_table, .create_update_policy => {},
    }

    if (current_schema_json.len == 0) {
        return switch (plan) {
            .moved => unreachable,
            .drop_index => |drop_index| if (drop_index.if_exists) .{ .schema_json = try alloc.dupe(u8, current_schema_json) } else error.InvalidSqlCatalog,
            .alter_table => |alter_table| if (alter_table.if_exists) .{ .schema_json = try alloc.dupe(u8, current_schema_json) } else error.InvalidSqlCatalog,
            .create_table => unreachable,
            .table_clone => unreachable,
            .view_catalog => unreachable,
            .materialized_view_catalog => unreachable,
            .relation_lifetime => unreachable,
            .table_partition_catalog => unreachable,
            .drop_table => unreachable,
            .create_index, .create_update_policy => error.InvalidSqlCatalog,
        };
    }

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena, current_schema_json, .{});
    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSqlCatalog,
    };

    var result: ddl_plan.AppliedDdlSchemaJson = .{ .schema_json = &.{} };
    switch (plan) {
        .moved => unreachable,
        .create_table => unreachable,
        .table_clone => unreachable,
        .view_catalog => unreachable,
        .materialized_view_catalog => unreachable,
        .relation_lifetime => unreachable,
        .table_partition_catalog => unreachable,
        .drop_table => unreachable,
        .create_index => |create_index| {
            const changed = try schema_json.applyCreateIndexPlanToSchemaJsonValue(arena, root, create_index);
            result.requires_rebuild = changed;
            result.validation_required = changed and create_index.unique;
        },
        .drop_index => |drop_index| try schema_json.applyDropIndexPlanToSchemaJsonValue(arena, root, drop_index),
        .alter_table => |alter_table| {
            const flags = try alterTablePlanWorkFlagsForSchemaJson(root, alter_table);
            result.requires_rebuild = flags.requires_rebuild;
            result.validation_required = flags.validation_required;
            result.rewrite_required = flags.rewrite_required;
            try schema_json.applyAlterTablePlanToSchemaJsonValue(arena, root, alter_table);
        },
        .create_update_policy => |update_policy| {
            try schema_json.applyCreateUpdatePolicyPlanToSchemaJsonValue(arena, root, update_policy);
        },
    }
    const updated_schema_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(updated_schema_json);
    const rewrite_source = switch (plan) {
        .alter_table => |alter_table| try alterTableRewriteExpressionSourceAlloc(alloc, alter_table),
        else => null,
    };
    defer if (rewrite_source) |source| freeAppliedDdlRewriteExpressionSource(alloc, source);
    const row_rewrite_source = switch (plan) {
        .alter_table => |alter_table| blk: {
            const old_schema = try schema_json.runtimeSchemaFromSchemaJsonAlloc(alloc, current_schema_json);
            defer runtime_schema.freeSchema(alloc, old_schema);
            const new_schema = try schema_json.runtimeSchemaFromSchemaJsonAlloc(alloc, updated_schema_json);
            defer runtime_schema.freeSchema(alloc, new_schema);
            break :blk try alterTableRowRewritePlanSourceAlloc(alloc, old_schema, new_schema, alter_table);
        },
        else => AppliedDdlRowRewritePlanSource{},
    };
    defer freeAppliedDdlRowRewritePlanSource(alloc, row_rewrite_source);
    if (rewrite_source != null and !row_rewrite_source.empty()) return error.UnsupportedSqlShape;
    result = try appliedDdlSchemaJsonWithFlagsAlloc(
        alloc,
        updated_schema_json,
        result.requires_rebuild,
        result.validation_required,
        result.rewrite_required,
        rewrite_source,
        row_rewrite_source,
    );
    errdefer result.deinit(alloc);
    try schema_json.validateDdlAppliedSchemaJsonAlloc(alloc, result.schema_json);
    return result;
}

pub fn appliedDdlTableWorkItemsForFlagsAlloc(
    alloc: std.mem.Allocator,
    requires_rebuild: bool,
    validation_required: bool,
    rewrite_required: bool,
) ![]const ddl_plan.AppliedDdlWorkItem {
    return try appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(alloc, requires_rebuild, validation_required, rewrite_required, null, .{});
}

fn appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(
    alloc: std.mem.Allocator,
    requires_rebuild: bool,
    validation_required: bool,
    rewrite_required: bool,
    rewrite_expression: ?AppliedDdlRewriteExpressionSource,
    row_rewrite_plan: AppliedDdlRowRewritePlanSource,
) ![]const ddl_plan.AppliedDdlWorkItem {
    const count: usize =
        (if (requires_rebuild) @as(usize, 1) else 0) +
        (if (validation_required) @as(usize, 1) else 0) +
        (if (rewrite_required) @as(usize, 1) else 0);
    if (count == 0) return &.{};
    var items = try alloc.alloc(ddl_plan.AppliedDdlWorkItem, count);
    var initialized: usize = 0;
    errdefer {
        for (items[0..initialized]) |item| {
            var mutable = item;
            mutable.deinit(alloc);
        }
        alloc.free(items);
    }
    var i: usize = 0;
    if (requires_rebuild) {
        items[i] = .{
            .action = .rebuild,
            .subject = .table,
            .reason = .derived_artifacts,
        };
        i += 1;
        initialized = i;
    }
    if (validation_required) {
        items[i] = .{
            .action = .validate,
            .subject = .table,
            .reason = .constraints,
        };
        i += 1;
        initialized = i;
    }
    if (rewrite_required) {
        if (rewrite_expression != null and !row_rewrite_plan.empty()) return error.UnsupportedSqlShape;
        const owned_rewrite: ?ddl_plan.AppliedDdlRewriteExpression = if (rewrite_expression) |rewrite| blk: {
            const target_column = try alloc.dupe(u8, rewrite.target_column);
            errdefer alloc.free(target_column);
            const expression = try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, rewrite.expression);
            errdefer runtime_schema.freeRelationalRowsExpression(alloc, expression);
            break :blk .{
                .target_column = target_column,
                .expression = expression,
            };
        } else null;
        var owned_row_plan = ddl_plan.AppliedDdlRowRewritePlan{};
        if (!row_rewrite_plan.empty()) {
            const renames = try alloc.alloc(ddl_plan.AppliedDdlRowRewriteRename, row_rewrite_plan.renames.len);
            var rename_count: usize = 0;
            errdefer {
                for (renames[0..rename_count]) |rename| {
                    alloc.free(rename.old_path);
                    alloc.free(rename.new_path);
                }
                alloc.free(renames);
            }
            for (row_rewrite_plan.renames, 0..) |rename, rename_i| {
                const old_path = try alloc.dupe(u8, rename.old_path);
                errdefer alloc.free(old_path);
                const new_path = try alloc.dupe(u8, rename.new_path);
                errdefer alloc.free(new_path);
                renames[rename_i] = .{ .old_path = old_path, .new_path = new_path };
                rename_count += 1;
            }
            const drops = try alloc.alloc([]const u8, row_rewrite_plan.drops.len);
            var drop_count: usize = 0;
            errdefer {
                for (drops[0..drop_count]) |drop| alloc.free(drop);
                alloc.free(drops);
            }
            for (row_rewrite_plan.drops, 0..) |drop, drop_i| {
                drops[drop_i] = try alloc.dupe(u8, drop);
                drop_count += 1;
            }
            owned_row_plan = .{ .renames = renames, .drops = drops };
        }
        items[i] = .{
            .action = .rewrite,
            .subject = .table,
            .reason = .row_images,
            .full_row_rewrite = owned_rewrite == null and owned_row_plan.empty(),
            .rewrite_expression = owned_rewrite,
            .row_rewrite_plan = owned_row_plan,
        };
        i += 1;
        initialized = i;
    }
    return items;
}

fn appliedDdlSchemaJsonWithFlagsAlloc(
    alloc: std.mem.Allocator,
    schema_json_text: []u8,
    requires_rebuild: bool,
    validation_required: bool,
    rewrite_required: bool,
    rewrite_expression: ?AppliedDdlRewriteExpressionSource,
    row_rewrite_plan: AppliedDdlRowRewritePlanSource,
) !ddl_plan.AppliedDdlSchemaJson {
    errdefer alloc.free(schema_json_text);
    const work_items = try appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(alloc, requires_rebuild, validation_required, rewrite_required, rewrite_expression, row_rewrite_plan);
    return .{
        .schema_json = schema_json_text,
        .requires_rebuild = requires_rebuild,
        .validation_required = validation_required,
        .rewrite_required = rewrite_required,
        .work_items = work_items,
    };
}

fn validateTemporalForeignKeysSupported(foreign_keys: []const runtime_schema.ForeignKey) !void {
    for (foreign_keys) |foreign_key| {
        if (foreign_key.child_period != null or foreign_key.parent_period != null) return error.UnsupportedSqlShape;
    }
}

const DdlWorkFlags = struct {
    requires_rebuild: bool = false,
    validation_required: bool = false,
    rewrite_required: bool = false,
};

fn alterTablePlanWorkFlags(plan: ddl_plan.AlterTablePlan) DdlWorkFlags {
    var flags: DdlWorkFlags = .{};
    for (plan.operations) |operation| {
        switch (operation) {
            .alter_column_default => {},
            .drop_constraint => |drop_constraint| {
                if (binder.defaultPrimaryKeyNameEquals(plan.table_name, drop_constraint.name)) {
                    flags.requires_rebuild = true;
                    flags.rewrite_required = true;
                }
            },
            .rename_constraint => {},
            .drop_update_policy => {},
            .alter_column_nullability => |nullability| {
                if (!nullability.nullable) flags.validation_required = true;
            },
            .drop_column, .rename_column, .alter_column_type => {
                flags.requires_rebuild = true;
                flags.validation_required = true;
                flags.rewrite_required = true;
            },
            .add_column => |add_column| {
                flags.requires_rebuild = true;
                flags.validation_required = !add_column.column.nullable or
                    add_column.unique_constraints.len != 0 or
                    add_column.foreign_keys.len != 0 or
                    add_column.checks.len != 0;
                flags.rewrite_required = add_column.column.generated != null or
                    add_column.column.default_value != null or
                    !add_column.column.nullable;
            },
            else => {
                flags.requires_rebuild = true;
                flags.validation_required = true;
            },
        }
    }
    return flags;
}

fn alterTablePlanWorkFlagsForSchemaJson(root: *std.json.ObjectMap, plan: ddl_plan.AlterTablePlan) !DdlWorkFlags {
    var flags = alterTablePlanWorkFlags(plan);
    for (plan.operations) |operation| {
        switch (operation) {
            .drop_constraint => |drop_constraint| {
                if (try schema_json.schemaJsonPrimaryKeyNameEquals(root, plan.table_name, drop_constraint.name)) {
                    flags.requires_rebuild = true;
                    flags.rewrite_required = true;
                }
            },
            else => {},
        }
    }
    return flags;
}

fn applyCreateTablePlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.CreateTablePlan,
) !runtime_schema.TableSchema {
    try validateTemporalForeignKeysSupported(plan.foreign_keys);
    if (binder.tableSchemaCatalogExists(current)) {
        if (plan.replace_existing) return try ddl_plan.runtimeSchemaFromCreateTablePlanAlloc(alloc, plan);
        if (plan.if_not_exists and current.storage_mode == .relational) return try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
        return error.InvalidSqlCatalog;
    }
    return try ddl_plan.runtimeSchemaFromCreateTablePlanAlloc(alloc, plan);
}

fn applyCreateIndexPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.CreateIndexPlan,
) !runtime_schema.TableSchema {
    var schema = try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);

    if (binder.relationalIndexNameExists(schema, plan.index_name)) {
        if (plan.if_not_exists) return schema;
        return error.InvalidSqlCatalog;
    }

    if (plan.derived_index_config_json != null) return error.UnsupportedSqlShape;

    if (plan.method == .gin) {
        if (plan.unique or plan.columns.len != 1 or plan.expressions.len != 0 or plan.generated_expression != null) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForDdl(schema.relational_columns, plan.columns[0]) orelse return error.InvalidSqlCatalog;
        switch (column.field_type) {
            .json => if (plan.opclass == .array_ops) return error.InvalidSqlCatalog,
            .array => if (plan.opclass == .jsonb_path_ops) return error.InvalidSqlCatalog,
            else => return error.InvalidSqlCatalog,
        }
        try lower_expr.validateCreateIndexIncludeColumns(schema.relational_columns, plan.columns, plan.include_columns);
    }

    if (plan.unique) {
        try lower_expr.validateCreateIndexIncludeColumns(schema.relational_columns, plan.columns, plan.include_columns);
        const constraint: runtime_schema.UniqueConstraint = .{
            .name = plan.index_name,
            .columns = plan.columns,
            .expressions = plan.expressions,
            .include_columns = plan.include_columns,
            .without_overlaps_period = plan.without_overlaps_period,
            .nulls_not_distinct = plan.nulls_not_distinct,
            .where = plan.where,
            .where_expressions = plan.where_expressions,
            .validation_state = .unvalidated,
        };
        try lower_expr.validateUniqueConstraintForColumns(schema.relational_columns, schema.periods, constraint);
        try ddl_plan.appendUniqueConstraintAlloc(alloc, &schema, constraint);
        return schema;
    }

    const index_generation = ddl_plan.stableSecondaryIndexGeneration(plan);
    if (plan.generated_expression) |generated_expression| {
        if (plan.columns.len != 0 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
        if (binder.relationalColumnIndex(schema.relational_columns, plan.index_name) != null) return error.InvalidSqlCatalog;
        try lower_expr.validateCreateIndexIncludeColumns(schema.relational_columns, &.{}, plan.include_columns);
        const column: runtime_schema.RelationalColumn = .{
            .name = plan.index_name,
            .path = plan.index_name,
            .field_type = .keyword,
            .nullable = true,
            .indexed = true,
            .index_lifecycle = .building,
            .index_generation = index_generation,
            .index_name = plan.index_name,
            .index_include_columns = plan.include_columns,
            .generated = generated_expression,
            .index_where = plan.where,
            .index_where_expressions = plan.where_expressions,
        };
        try lower_expr.validateGeneratedColumnForColumns(schema.relational_columns, column);
        try lower_expr.validateUniquePredicatesForColumns(schema.relational_columns, plan.where);
        try lower_expr.validateUniquePredicateExpressionsForColumns(schema.relational_columns, plan.where_expressions);
        try ddl_plan.appendRelationalColumnAlloc(alloc, &schema, column);
        return schema;
    }

    if (plan.columns.len == 0 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
    try lower_expr.validateCreateIndexIncludeColumns(schema.relational_columns, plan.columns, plan.include_columns);
    try lower_expr.validateUniquePredicatesForColumns(schema.relational_columns, plan.where);
    try lower_expr.validateUniquePredicateExpressionsForColumns(schema.relational_columns, plan.where_expressions);
    try ddl_plan.markColumnsIndexedAlloc(alloc, &schema, plan.index_name, plan.columns, plan.include_columns, plan.where, plan.where_expressions, index_generation);
    return schema;
}

fn applyCommentMetadataPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.CommentMetadataPlan,
) !runtime_schema.TableSchema {
    if (!binder.tableSchemaCatalogExists(current)) return error.InvalidSqlCatalog;
    try binder.validateCommentMetadataPlanForRuntimeSchemaAlloc(alloc, current, plan);
    return try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
}

fn applyDropIndexPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.DropIndexPlan,
) !runtime_schema.TableSchema {
    if (!binder.tableSchemaCatalogExists(current)) {
        if (plan.if_exists) return try ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, current);
        return error.InvalidSqlCatalog;
    }
    var schema = try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);
    try ddl_plan.dropIndexFromRuntimeSchemaAlloc(alloc, &schema, plan);
    return schema;
}

fn applyDropTablePlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.DropTablePlan,
) !runtime_schema.TableSchema {
    if (!binder.tableSchemaCatalogExists(current)) {
        if (plan.if_exists) return try ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, .{});
        return error.InvalidSqlCatalog;
    }
    return try ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, .{});
}

fn applyAlterTablePlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.AlterTablePlan,
) !runtime_schema.TableSchema {
    if (!binder.tableSchemaCatalogExists(current)) {
        if (plan.if_exists) return try ddl_plan.cloneEmptyRuntimeSchemaAlloc(alloc, current);
        return error.InvalidSqlCatalog;
    }
    var schema = try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);

    for (plan.operations) |operation| {
        switch (operation) {
            .add_column => |add_column| try addRelationalColumnOperationAlloc(alloc, &schema, add_column),
            .add_period => |period| try addRelationalPeriodAlloc(alloc, &schema, period),
            .add_primary_key => |primary_key| try addRelationalPrimaryKeyAlloc(alloc, &schema, primary_key),
            .rename_column => |rename_column| try ddl_plan.renameRelationalColumnAlloc(alloc, &schema, rename_column),
            .rename_constraint => |rename_constraint| try ddl_plan.renameRelationalConstraintAlloc(alloc, &schema, plan.table_name, rename_constraint),
            .drop_column => |drop_column| try ddl_plan.dropRelationalColumnAlloc(alloc, &schema, drop_column),
            .drop_constraint => |drop_constraint| try ddl_plan.dropRelationalConstraintAlloc(alloc, &schema, plan.table_name, drop_constraint),
            .drop_update_policy => |drop_update_policy| try dropUpdatePolicyFromRuntimeSchemaAlloc(alloc, &schema, drop_update_policy),
            .alter_column_default => |alter_column_default| try ddl_plan.alterRelationalColumnDefaultAlloc(alloc, &schema, alter_column_default),
            .alter_column_nullability => |alter_column_nullability| try ddl_plan.alterRelationalColumnNullability(&schema, alter_column_nullability),
            .alter_column_type => |alter_column_type| try schema_mutation.alterRelationalColumnTypeAlloc(alloc, &schema, alter_column_type),
            .add_unique_constraint => |constraint| {
                try lower_expr.validateUniqueConstraintForColumns(schema.relational_columns, schema.periods, constraint);
                try ddl_plan.appendUniqueConstraintAlloc(alloc, &schema, constraint);
            },
            .add_foreign_key => |foreign_key| {
                try validateTemporalForeignKeysSupported(&.{foreign_key});
                try binder.validateForeignKeyForColumns(schema.relational_columns, schema.periods, foreign_key);
                try ddl_plan.appendForeignKeyAlloc(alloc, &schema, foreign_key);
            },
            .add_check => |check| {
                try lower_expr.validateCheckForColumns(schema.relational_columns, check);
                try ddl_plan.appendRelationalCheckAlloc(alloc, &schema, check);
            },
            .validate_constraint => |constraint_name| try ddl_plan.validateConstraintByName(&schema, plan.table_name, constraint_name),
        }
    }
    return schema;
}

fn applyCreateUpdatePolicyPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: ddl_plan.CreateUpdatePolicyPlan,
) !runtime_schema.TableSchema {
    var schema = try ddl_plan.cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);
    try ddl_plan.setColumnOnUpdatePolicyAlloc(alloc, &schema, plan.column_name, plan.on_update_value);
    return schema;
}

fn dropUpdatePolicyFromRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: ddl_plan.DropUpdatePolicyOperation,
) anyerror!void {
    _ = operation.trigger_name;
    var policy_count: usize = 0;
    var policy_index: usize = 0;
    for (schema.relational_columns, 0..) |column, i| {
        if (column.on_update_value == null) continue;
        policy_count += 1;
        policy_index = i;
    }
    if (policy_count == 0) {
        if (operation.if_exists) return;
        return error.InvalidSqlCatalog;
    }
    if (policy_count > 1) return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[policy_index].on_update_value) |value| alloc.free(value.value_json);
    columns[policy_index].on_update_value = null;
}

fn addRelationalColumnOperationAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: ddl_plan.AddColumnOperation,
) anyerror!void {
    if (binder.relationalColumnIndex(schema.relational_columns, operation.column.name) != null) {
        if (operation.if_not_exists) return;
        return error.InvalidSqlCatalog;
    }
    try lower_expr.validateGeneratedColumnForColumns(schema.relational_columns, operation.column);
    try lower_expr.validateUniquePredicatesForColumns(schema.relational_columns, operation.column.index_where);
    try ddl_plan.appendRelationalColumnAlloc(alloc, schema, operation.column);
    for (operation.unique_constraints) |constraint| {
        try lower_expr.validateUniqueConstraintForColumns(schema.relational_columns, schema.periods, constraint);
        try ddl_plan.appendUniqueConstraintAlloc(alloc, schema, constraint);
    }
    for (operation.foreign_keys) |foreign_key| {
        try validateTemporalForeignKeysSupported(&.{foreign_key});
        try binder.validateForeignKeyForColumns(schema.relational_columns, schema.periods, foreign_key);
        try ddl_plan.appendForeignKeyAlloc(alloc, schema, foreign_key);
    }
    for (operation.checks) |check| {
        try lower_expr.validateCheckForColumns(schema.relational_columns, check);
        try ddl_plan.appendRelationalCheckAlloc(alloc, schema, check);
    }
}

fn addRelationalPeriodAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    period: runtime_schema.RelationalPeriod,
) !void {
    if (binder.relationalPeriodNameExists(schema.periods, period.name)) return error.InvalidSqlCatalog;
    const len = schema.periods.len;
    const out = try alloc.alloc(runtime_schema.RelationalPeriod, len + 1);
    var appended = false;
    errdefer {
        if (appended) ddl_plan.freeDdlPeriod(alloc, out[len]);
        alloc.free(out);
    }
    @memcpy(out[0..len], schema.periods);
    out[len] = try ddl_plan.cloneDdlPeriod(alloc, period);
    appended = true;
    try binder.validateRelationalPeriodCatalog(schema.relational_columns, out);
    if (len > 0) alloc.free(schema.periods);
    schema.periods = out;
}

fn addRelationalPrimaryKeyAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    primary_key: runtime_schema.PrimaryKey,
) !void {
    if (schema.primary_key != null) return error.InvalidSqlCatalog;
    try lower_expr.validatePrimaryKeyColumns(schema.relational_columns, primary_key);
    try binder.validatePrimaryKeyTemporalCatalog(schema.periods, primary_key);
    schema.primary_key = try ddl_plan.cloneDdlPrimaryKey(alloc, primary_key);
}

fn lowerDdlPlanForCatalogApplyTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
) !ddl_plan.LoweredDdlPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    const tokens = parsed_sql.items();

    var state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
    };
    return try ddl_plan.parseDdlPlanAlloc(alloc, tokens, &state.pos, .{
        .schema = state.schema,
        .field_expression_qualifiers = state.field_expression_qualifiers,
        .returning_expression_qualifiers = state.returning_expression_qualifiers,
        .defer_row_expression_field_validation = state.defer_row_expression_field_validation,
        .column_definition_options = parser_context.ParserState.ContextAccessors.ddlColumnDefinitionOptions(&state),
        .domain_options = parser_context.ParserState.ContextAccessors.ddlDomainOptions(&state),
        .create_index_options = parser_context.ParserState.ContextAccessors.createIndexOptions(&state),
        .row_security_policy_options = parser_context.ParserState.ContextAccessors.rowSecurityPolicyOptions(&state),
    });
}

fn expectAppliedDdlWorkActions(applied: ddl_plan.AppliedDdlSchemaJson, expected: []const ddl_plan.AppliedDdlWorkAction) !void {
    try std.testing.expectEqual(expected.len, applied.work_items.len);
    for (expected, 0..) |action, i| {
        try std.testing.expectEqual(action, applied.work_items[i].action);
        try std.testing.expectEqual(ddl_plan.AppliedDdlWorkSubject.table, applied.work_items[i].subject);
        switch (action) {
            .rebuild => try std.testing.expectEqual(ddl_plan.AppliedDdlWorkReason.derived_artifacts, applied.work_items[i].reason),
            .validate => try std.testing.expectEqual(ddl_plan.AppliedDdlWorkReason.constraints, applied.work_items[i].reason),
            .rewrite => try std.testing.expectEqual(ddl_plan.AppliedDdlWorkReason.row_images, applied.work_items[i].reason),
        }
    }
}

test "catalog apply applies SQL session catalog plans" {
    const alloc = std.testing.allocator;

    var set_tenant_search_path = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET search_path TO tenant_schema, public;");
    defer set_tenant_search_path.deinit(alloc);
    const set_tenant_search_path_plan = switch (set_tenant_search_path) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var tenant_session = try applySessionCatalogPlanAlloc(alloc, catalog_resources.SqlCatalogSession.default(), set_tenant_search_path_plan);
    defer tenant_session.deinit(alloc);
    try std.testing.expectEqualStrings(catalog_resources.default_database_name, tenant_session.session().currentDatabase());
    try std.testing.expectEqualStrings("tenant_schema", tenant_session.session().primarySearchPathNamespace());

    var set_local_public_search_path = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET LOCAL search_path TO public;");
    defer set_local_public_search_path.deinit(alloc);
    const set_local_public_search_path_plan = switch (set_local_public_search_path) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var local_public_session = try applySessionCatalogPlanAlloc(alloc, tenant_session.session(), set_local_public_search_path_plan);
    defer local_public_session.deinit(alloc);
    try std.testing.expect(local_public_session.transaction_local_search_path);
    try std.testing.expectEqualStrings("public", local_public_session.session().primarySearchPathNamespace());

    var set_local_tenant_search_path = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET LOCAL search_path TO tenant_schema, public;");
    defer set_local_tenant_search_path.deinit(alloc);
    const set_local_tenant_search_path_plan = switch (set_local_tenant_search_path) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var local_tenant_session = try applySessionCatalogPlanAlloc(alloc, tenant_session.session(), set_local_tenant_search_path_plan);
    defer local_tenant_session.deinit(alloc);
    try std.testing.expect(local_tenant_session.transaction_local_search_path);
    try std.testing.expectEqualStrings("tenant_schema", local_tenant_session.session().primarySearchPathNamespace());

    var empty_path_session = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, .{
        .current_database_name = "tenant_ops",
        .search_path = &.{},
    });
    defer empty_path_session.deinit(alloc);
    try std.testing.expectEqualStrings("tenant_ops", empty_path_session.session().currentDatabase());
    try std.testing.expectEqual(@as(usize, 1), empty_path_session.search_path.len);
    try std.testing.expectEqualStrings(catalog_resources.default_namespace_name, empty_path_session.session().primarySearchPathNamespace());

    var set_app_setting = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET app.tenant_id = 'tenant-a';");
    defer set_app_setting.deinit(alloc);
    const set_app_setting_plan = switch (set_app_setting) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var app_setting_session = try applySessionCatalogPlanAlloc(alloc, tenant_session.session(), set_app_setting_plan);
    defer app_setting_session.deinit(alloc);
    try std.testing.expectEqualStrings("tenant-a", app_setting_session.session().settingValue("app.tenant_id") orelse return error.TestUnexpectedResult);

    var set_local_app_setting = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET LOCAL app.tenant_id = 'tenant-b';");
    defer set_local_app_setting.deinit(alloc);
    const set_local_app_setting_plan = switch (set_local_app_setting) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var local_app_setting_session = try applyOwnedSessionCatalogPlanAlloc(alloc, app_setting_session, set_local_app_setting_plan);
    defer local_app_setting_session.deinit(alloc);
    try std.testing.expect(local_app_setting_session.transaction_local_settings);
    try std.testing.expectEqualStrings("tenant-b", local_app_setting_session.session().settingValue("app.tenant_id") orelse return error.TestUnexpectedResult);
    try local_app_setting_session.clearTransactionLocalState(alloc);
    try std.testing.expect(!local_app_setting_session.transaction_local_settings);
    try std.testing.expect(local_app_setting_session.transaction_local_settings_base == null);
    try std.testing.expectEqualStrings("tenant-a", local_app_setting_session.session().settingValue("app.tenant_id") orelse return error.TestUnexpectedResult);

    var set_sync_level = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET antfly.sync_level = 'full_index';");
    defer set_sync_level.deinit(alloc);
    const set_sync_level_plan = switch (set_sync_level) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var sync_level_session = try applySessionCatalogPlanAlloc(alloc, tenant_session.session(), set_sync_level_plan);
    defer sync_level_session.deinit(alloc);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, try sqlSyncLevelFromSession(sync_level_session.session()));

    var set_local_sync_level = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET LOCAL antfly.sync_level = 'propose';");
    defer set_local_sync_level.deinit(alloc);
    const set_local_sync_level_plan = switch (set_local_sync_level) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var local_sync_level_session = try applyOwnedSessionCatalogPlanAlloc(alloc, sync_level_session, set_local_sync_level_plan);
    defer local_sync_level_session.deinit(alloc);
    try std.testing.expect(local_sync_level_session.transaction_local_settings);
    try std.testing.expectEqual(db_mod.types.SyncLevel.propose, try sqlSyncLevelFromSession(local_sync_level_session.session()));
    try local_sync_level_session.clearTransactionLocalState(alloc);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, try sqlSyncLevelFromSession(local_sync_level_session.session()));
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, try sqlSyncLevelFromSession(catalog_resources.SqlCatalogSession.default()));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET antfly.sync_level = 'eventual';"));

    var set_runtime_setting = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET statement_timeout = '1ms';");
    defer set_runtime_setting.deinit(alloc);
    const set_runtime_setting_plan = switch (set_runtime_setting) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var runtime_setting_session = try applySessionCatalogPlanAlloc(alloc, app_setting_session.session(), set_runtime_setting_plan);
    defer runtime_setting_session.deinit(alloc);
    try std.testing.expectEqualStrings("1ms", runtime_setting_session.session().settingValue("statement_timeout") orelse return error.TestUnexpectedResult);
    try std.testing.expectEqualStrings("tenant-a", runtime_setting_session.session().settingValue("app.tenant_id") orelse return error.TestUnexpectedResult);
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), try parseSqlStatementTimeoutNs("1"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_ms), try parseSqlStatementTimeoutNs("1ms"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_us), try parseSqlStatementTimeoutNs("1us"));
    try std.testing.expectEqual(@as(u64, std.time.ns_per_s), try parseSqlStatementTimeoutNs("1s"));
    try std.testing.expectEqual(@as(u64, 60 * std.time.ns_per_s), try parseSqlStatementTimeoutNs("1min"));
    try std.testing.expectEqual(@as(u64, 60 * 60 * std.time.ns_per_s), try parseSqlStatementTimeoutNs("1h"));
    try std.testing.expectError(error.InvalidRoleSetting, parseSqlStatementTimeoutNs(""));
    try std.testing.expectError(error.InvalidRoleSetting, parseSqlStatementTimeoutNs("five"));
    try std.testing.expectError(error.InvalidRoleSetting, parseSqlStatementTimeoutNs("5fortnights"));
    try std.testing.expectEqual(@as(?u64, std.time.ns_per_ms), try sqlStatementTimeoutNsFromSession(runtime_setting_session.session()));
    try std.testing.expect(sqlStatementTimeoutExpired(try sqlStatementTimeoutNsFromSession(runtime_setting_session.session()), 1_000, 1_000 + std.time.ns_per_ms));
    try std.testing.expect(!sqlStatementTimeoutExpired(try sqlStatementTimeoutNsFromSession(runtime_setting_session.session()), 1_000, 1_000 + std.time.ns_per_ms - 1));
    try enforceSqlStatementTimeoutAt(runtime_setting_session.session(), 1_000, 1_000 + std.time.ns_per_ms - 1);
    try std.testing.expectError(error.StatementTimeout, enforceSqlStatementTimeoutAt(runtime_setting_session.session(), 1_000, 1_000 + std.time.ns_per_ms));

    var disable_timeout = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET statement_timeout = '0';");
    defer disable_timeout.deinit(alloc);
    const disable_timeout_plan = switch (disable_timeout) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var disabled_timeout_session = try applySessionCatalogPlanAlloc(alloc, runtime_setting_session.session(), disable_timeout_plan);
    defer disabled_timeout_session.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, null), try sqlStatementTimeoutNsFromSession(disabled_timeout_session.session()));
    try std.testing.expect(!sqlStatementTimeoutExpired(try sqlStatementTimeoutNsFromSession(disabled_timeout_session.session()), 1_000, std.math.maxInt(u64)));

    var invalid_timeout = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET statement_timeout = 'five';");
    defer invalid_timeout.deinit(alloc);
    const invalid_timeout_plan = switch (invalid_timeout) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectError(error.InvalidRoleSetting, applySessionCatalogPlanAlloc(alloc, runtime_setting_session.session(), invalid_timeout_plan));

    var reset_app_setting = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "RESET app.tenant_id;");
    defer reset_app_setting.deinit(alloc);
    const reset_app_setting_plan = switch (reset_app_setting) {
        .session_catalog => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    var reset_app_setting_session = try applySessionCatalogPlanAlloc(alloc, runtime_setting_session.session(), reset_app_setting_plan);
    defer reset_app_setting_session.deinit(alloc);
    try std.testing.expect(reset_app_setting_session.session().settingValue("app.tenant_id") == null);
    try std.testing.expectEqualStrings("1ms", reset_app_setting_session.session().settingValue("statement_timeout") orelse return error.TestUnexpectedResult);
}

test "catalog apply applies adapter noops and comment ddl to public schema json" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\CREATE TABLE users (
        \\  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  email text COLLATE "C",
        \\  attrs jsonb,
        \\  tags text[],
        \\  updated_at_ns bigint DEFAULT 0,
        \\  current_day_ns date DEFAULT CURRENT_DATE,
        \\  CONSTRAINT users_tenant_email_key UNIQUE (tenant_id, email),
        \\  CONSTRAINT users_updated_check CHECK (updated_at_ns >= 0)
        \\);
        ,
    );
    defer create.deinit(alloc);

    var applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", create);
    defer applied.deinit(alloc);

    var create_public_schema = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE SCHEMA IF NOT EXISTS public;");
    defer create_public_schema.deinit(alloc);
    const create_public_schema_noop = switch (create_public_schema) {
        .adapter_noop => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(ddl_plan.AdapterNoopDdlReason.schema_namespace, create_public_schema_noop.reason);
    const create_public_schema_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, create_public_schema);
    defer alloc.free(create_public_schema_fingerprint);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=schema_namespace", create_public_schema_fingerprint);
    var public_schema_applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, create_public_schema);
    defer public_schema_applied.deinit(alloc);
    try std.testing.expectEqualStrings(applied.schema_json, public_schema_applied.schema_json);

    var create_extension = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE EXTENSION IF NOT EXISTS pgcrypto;");
    defer create_extension.deinit(alloc);
    const create_extension_noop = switch (create_extension) {
        .adapter_noop => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(ddl_plan.AdapterNoopDdlReason.extension, create_extension_noop.reason);
    const create_extension_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, create_extension);
    defer alloc.free(create_extension_fingerprint);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=extension", create_extension_fingerprint);
    var extension_applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, create_extension);
    defer extension_applied.deinit(alloc);
    try std.testing.expectEqualStrings(applied.schema_json, extension_applied.schema_json);

    var create_public_extension = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;");
    defer create_public_extension.deinit(alloc);
    const create_public_extension_noop = switch (create_public_extension) {
        .adapter_noop => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(ddl_plan.AdapterNoopDdlReason.extension, create_public_extension_noop.reason);
    const create_public_extension_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, create_public_extension);
    defer alloc.free(create_public_extension_fingerprint);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=extension", create_public_extension_fingerprint);

    var table_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON TABLE users IS 'metered usage rows';");
    defer table_comment.deinit(alloc);
    const table_comment_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, table_comment);
    defer alloc.free(table_comment_fingerprint);
    try std.testing.expectEqualStrings("ddl:comment:kind=table:object=users:comment=true", table_comment_fingerprint);
    var table_commented = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, table_comment);
    defer table_commented.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, table_commented.schema_json, "\"comments\":{\"table\":\"metered usage rows\"") != null);

    var column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN users.email IS 'contact address';");
    defer column_comment.deinit(alloc);
    const column_comment_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, column_comment);
    defer alloc.free(column_comment_fingerprint);
    try std.testing.expectEqualStrings("ddl:comment:kind=column:object=users.email:comment=true", column_comment_fingerprint);
    var column_commented = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, table_commented.schema_json, column_comment);
    defer column_commented.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, column_commented.schema_json, "\"columns\":{\"email\":\"contact address\"}") != null);

    var comment_index_create = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE INDEX users_email_comment_idx ON users (email);");
    defer comment_index_create.deinit(alloc);
    var comment_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, column_commented.schema_json, comment_index_create);
    defer comment_indexed.deinit(alloc);

    var index_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON INDEX users_email_comment_idx IS 'email lookup';");
    defer index_comment.deinit(alloc);
    const index_comment_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, index_comment);
    defer alloc.free(index_comment_fingerprint);
    try std.testing.expectEqualStrings("ddl:comment:kind=index:object=users_email_comment_idx:comment=true", index_comment_fingerprint);
    var index_commented = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, comment_indexed.schema_json, index_comment);
    defer index_commented.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, index_commented.schema_json, "\"indexes\":{\"users_email_comment_idx\":\"email lookup\"}") != null);

    var constraint_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON CONSTRAINT users_updated_check ON users IS 'valid status';");
    defer constraint_comment.deinit(alloc);
    const constraint_comment_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, constraint_comment);
    defer alloc.free(constraint_comment_fingerprint);
    try std.testing.expectEqualStrings("ddl:comment:kind=constraint:object=users_updated_check:table=users:comment=true", constraint_comment_fingerprint);
    var constraint_commented = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, index_commented.schema_json, constraint_comment);
    defer constraint_commented.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, constraint_commented.schema_json, "\"constraints\":{\"users_updated_check\":\"valid status\"}") != null);

    var clear_table_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON TABLE users IS NULL;");
    defer clear_table_comment.deinit(alloc);
    const clear_table_comment_fingerprint = try fingerprint.ddlFingerprintAlloc(alloc, clear_table_comment);
    defer alloc.free(clear_table_comment_fingerprint);
    try std.testing.expectEqualStrings("ddl:comment:kind=table:object=users:comment=false", clear_table_comment_fingerprint);
    var table_comment_cleared = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, constraint_commented.schema_json, clear_table_comment);
    defer table_comment_cleared.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, table_comment_cleared.schema_json, "\"table\":\"metered usage rows\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, table_comment_cleared.schema_json, "\"columns\":{\"email\":\"contact address\"}") != null);

    var rename_commented_column = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME COLUMN email TO contact_email;");
    defer rename_commented_column.deinit(alloc);
    var renamed_column_comment = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, table_comment_cleared.schema_json, rename_commented_column);
    defer renamed_column_comment.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, renamed_column_comment.schema_json, "\"columns\":{\"contact_email\":\"contact address\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, renamed_column_comment.schema_json, "\"columns\":{\"email\":\"contact address\"}") == null);

    var rename_commented_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME CONSTRAINT users_updated_check TO users_updated_at_check;");
    defer rename_commented_constraint.deinit(alloc);
    var renamed_constraint_comment = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, renamed_column_comment.schema_json, rename_commented_constraint);
    defer renamed_constraint_comment.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, renamed_constraint_comment.schema_json, "\"constraints\":{\"users_updated_at_check\":\"valid status\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, renamed_constraint_comment.schema_json, "\"constraints\":{\"users_updated_check\":\"valid status\"}") == null);

    var drop_commented_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_email_comment_idx;");
    defer drop_commented_index.deinit(alloc);
    var dropped_index_comment = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, renamed_constraint_comment.schema_json, drop_commented_index);
    defer dropped_index_comment.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, dropped_index_comment.schema_json, "\"users_email_comment_idx\":\"email lookup\"") == null);
}

test "catalog apply creates clones and replaces public schema json" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\CREATE TABLE users (
        \\  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  email text COLLATE "C",
        \\  attrs jsonb,
        \\  tags text[],
        \\  updated_at_ns bigint DEFAULT 0,
        \\  current_day_ns date DEFAULT CURRENT_DATE,
        \\  CONSTRAINT users_tenant_email_key UNIQUE (tenant_id, email),
        \\  CONSTRAINT users_updated_check CHECK (updated_at_ns >= 0)
        \\);
        ,
    );
    defer create.deinit(alloc);

    var applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", create);
    defer applied.deinit(alloc);
    try std.testing.expect(!applied.requires_rebuild);
    try std.testing.expect(!applied.validation_required);
    try std.testing.expect(!applied.rewrite_required);
    var applied_parsed = try schema_api.parseValidatedTableSchema(alloc, applied.schema_json);
    defer applied_parsed.deinit(alloc);
    const applied_runtime = try schema_api.deriveRuntimeTableSchema(alloc, applied_parsed);
    defer runtime_schema.freeSchema(alloc, applied_runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, applied_runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 7), applied_runtime.relational_columns.len);
    try std.testing.expect(applied_runtime.primary_key != null);
    try std.testing.expectEqualStrings("id", applied_runtime.primary_key.?.columns[0]);
    const applied_email = binder.relationalColumnForField(applied_runtime, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("C", applied_email.collation.?);
    const current_day = binder.relationalColumnForField(applied_runtime, "current_day_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, current_day.field_type);
    try std.testing.expect(current_day.default_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.current_date_ns, current_day.default_value.?.kind);
    try std.testing.expect(std.mem.indexOf(u8, applied.schema_json, "\"current_day_ns\":{\"type\":\"datetime\",\"x-antfly-default\":{\"op\":\"current_date_ns\"}}") != null);
    try std.testing.expectEqual(@as(usize, 1), applied_runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_email_key", applied_runtime.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), applied_runtime.checks.len);

    var begin_protocol = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "BEGIN;");
    defer begin_protocol.deinit(alloc);
    var begin_protocol_applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, begin_protocol);
    defer begin_protocol_applied.deinit(alloc);
    try std.testing.expectEqualStrings(applied.schema_json, begin_protocol_applied.schema_json);

    var set_search_path = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "SET search_path TO public;");
    defer set_search_path.deinit(alloc);
    var set_search_path_applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, set_search_path);
    defer set_search_path_applied.deinit(alloc);
    try std.testing.expectEqualStrings(applied.schema_json, set_search_path_applied.schema_json);

    var duplicate_create = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE users (id uuid PRIMARY KEY);");
    defer duplicate_create.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, duplicate_create));

    var duplicate_create_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY);");
    defer duplicate_create_if_not_exists.deinit(alloc);
    var unchanged = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, duplicate_create_if_not_exists);
    defer unchanged.deinit(alloc);
    try std.testing.expectEqualStrings(applied.schema_json, unchanged.schema_json);
    try std.testing.expect(!unchanged.requires_rebuild);
    try std.testing.expect(!unchanged.validation_required);
    try std.testing.expect(!unchanged.rewrite_required);

    var table_clone = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE IF NOT EXISTS users_copy (LIKE users INCLUDING ALL EXCLUDING COMMENTS);");
    defer table_clone.deinit(alloc);
    var cloned = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, table_clone);
    defer cloned.deinit(alloc);
    try std.testing.expect(cloned.requires_rebuild);
    try std.testing.expect(cloned.validation_required);
    try std.testing.expect(!cloned.rewrite_required);
    var cloned_parsed = try schema_api.parseValidatedTableSchema(alloc, cloned.schema_json);
    defer cloned_parsed.deinit(alloc);
    const cloned_runtime = try schema_api.deriveRuntimeTableSchema(alloc, cloned_parsed);
    defer runtime_schema.freeSchema(alloc, cloned_runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, cloned_runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 7), cloned_runtime.relational_columns.len);
    try std.testing.expect(cloned_runtime.primary_key != null);
    try std.testing.expectEqualStrings("id", cloned_runtime.primary_key.?.columns[0]);
    try std.testing.expectEqual(@as(usize, 1), cloned_runtime.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), cloned_runtime.checks.len);
    const cloned_email = binder.relationalColumnForField(cloned_runtime, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("C", cloned_email.collation.?);
    const cloned_current_day = binder.relationalColumnForField(cloned_runtime, "current_day_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cloned_current_day.default_value != null);

    var table_clone_without_constraints = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE users_copy (LIKE users);");
    defer table_clone_without_constraints.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, table_clone_without_constraints));

    var replace = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE OR REPLACE TABLE users (id uuid PRIMARY KEY);");
    defer replace.deinit(alloc);
    var replaced = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, replace);
    defer replaced.deinit(alloc);
    try std.testing.expect(replaced.requires_rebuild);
    try std.testing.expect(replaced.validation_required);
    try std.testing.expect(replaced.rewrite_required);
    try expectAppliedDdlWorkActions(replaced, &.{ .rebuild, .validate, .rewrite });
    var replaced_parsed = try schema_api.parseValidatedTableSchema(alloc, replaced.schema_json);
    defer replaced_parsed.deinit(alloc);
    const replaced_runtime = try schema_api.deriveRuntimeTableSchema(alloc, replaced_parsed);
    defer runtime_schema.freeSchema(alloc, replaced_runtime);
    try std.testing.expectEqual(@as(usize, 1), replaced_runtime.relational_columns.len);
    try std.testing.expectEqualStrings("id", replaced_runtime.primary_key.?.columns[0]);

    var replace_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE OR REPLACE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY, status text);");
    defer replace_if_not_exists.deinit(alloc);
    var replaced_if_not_exists = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, applied.schema_json, replace_if_not_exists);
    defer replaced_if_not_exists.deinit(alloc);
    try std.testing.expect(replaced_if_not_exists.requires_rebuild);
    try std.testing.expect(replaced_if_not_exists.validation_required);
    try std.testing.expect(replaced_if_not_exists.rewrite_required);
    try expectAppliedDdlWorkActions(replaced_if_not_exists, &.{ .rebuild, .validate, .rewrite });
    var replaced_if_not_exists_parsed = try schema_api.parseValidatedTableSchema(alloc, replaced_if_not_exists.schema_json);
    defer replaced_if_not_exists_parsed.deinit(alloc);
    const replaced_if_not_exists_runtime = try schema_api.deriveRuntimeTableSchema(alloc, replaced_if_not_exists_parsed);
    defer runtime_schema.freeSchema(alloc, replaced_if_not_exists_runtime);
    try std.testing.expectEqual(@as(usize, 2), replaced_if_not_exists_runtime.relational_columns.len);
    try std.testing.expect(binder.relationalColumnForField(replaced_if_not_exists_runtime, "status", null) != null);
}

test "catalog apply applies incremental ddl plans to public schema json" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE users (id uuid PRIMARY KEY, tenant_id text NOT NULL, account_id text, email text, status text, deleted_at timestamptz, updated_at_ns bigint);",
    );
    defer create.deinit(alloc);
    var created = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", create);
    defer created.deinit(alloc);

    var multi_column_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_account_email_idx ON users (account_id, email);",
    );
    defer multi_column_index.deinit(alloc);
    var multi_column_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, created.schema_json, multi_column_index);
    defer multi_column_indexed.deinit(alloc);
    try std.testing.expect(multi_column_indexed.requires_rebuild);
    try std.testing.expect(!multi_column_indexed.validation_required);
    var parsed_multi_column_indexed = try schema_api.parseValidatedTableSchema(alloc, multi_column_indexed.schema_json);
    defer parsed_multi_column_indexed.deinit(alloc);
    const multi_column_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_multi_column_indexed);
    defer runtime_schema.freeSchema(alloc, multi_column_runtime);
    const multi_account = binder.relationalColumnForField(multi_column_runtime, "account_id", null) orelse return error.TestUnexpectedResult;
    const multi_email = binder.relationalColumnForField(multi_column_runtime, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(multi_account.index_name != null);
    try std.testing.expect(multi_email.index_name != null);
    try std.testing.expectEqualStrings("users_account_email_idx", multi_account.index_name.?);
    try std.testing.expectEqualStrings("users_account_email_idx", multi_email.index_name.?);
    try std.testing.expectEqual(multi_account.index_generation, multi_email.index_generation);

    var drop_multi_column_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_account_email_idx;");
    defer drop_multi_column_index.deinit(alloc);
    var multi_column_dropped = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, multi_column_indexed.schema_json, drop_multi_column_index);
    defer multi_column_dropped.deinit(alloc);
    try std.testing.expect(!multi_column_dropped.requires_rebuild);
    try std.testing.expect(!multi_column_dropped.validation_required);
    var parsed_multi_column_dropped = try schema_api.parseValidatedTableSchema(alloc, multi_column_dropped.schema_json);
    defer parsed_multi_column_dropped.deinit(alloc);
    const multi_column_dropped_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_multi_column_dropped);
    defer runtime_schema.freeSchema(alloc, multi_column_dropped_runtime);
    const dropped_multi_account = binder.relationalColumnForField(multi_column_dropped_runtime, "account_id", null) orelse return error.TestUnexpectedResult;
    const dropped_multi_email = binder.relationalColumnForField(multi_column_dropped_runtime, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(dropped_multi_account.index_name == null);
    try std.testing.expect(dropped_multi_email.index_name == null);

    var status_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_idx ON users (status);",
    );
    defer status_index.deinit(alloc);
    var status_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, created.schema_json, status_index);
    defer status_indexed.deinit(alloc);
    try std.testing.expect(status_indexed.requires_rebuild);
    try std.testing.expect(!status_indexed.validation_required);
    try expectAppliedDdlWorkActions(status_indexed, &.{.rebuild});

    var status_index_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX IF NOT EXISTS users_status_idx ON users (status);",
    );
    defer status_index_if_not_exists.deinit(alloc);
    var status_index_noop = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, status_indexed.schema_json, status_index_if_not_exists);
    defer status_index_noop.deinit(alloc);
    try std.testing.expect(!status_index_noop.requires_rebuild);
    try std.testing.expect(!status_index_noop.validation_required);

    var upper_expression_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_upper_email_idx ON users (upper(email));",
    );
    defer upper_expression_index.deinit(alloc);
    var upper_expression_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, status_indexed.schema_json, upper_expression_index);
    defer upper_expression_indexed.deinit(alloc);
    try std.testing.expect(upper_expression_indexed.requires_rebuild);
    try std.testing.expect(!upper_expression_indexed.validation_required);
    var parsed_upper_expression_indexed = try schema_api.parseValidatedTableSchema(alloc, upper_expression_indexed.schema_json);
    defer parsed_upper_expression_indexed.deinit(alloc);
    const upper_expression_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_upper_expression_indexed);
    defer runtime_schema.freeSchema(alloc, upper_expression_runtime);
    const upper_expression = binder.relationalColumnForField(upper_expression_runtime, "users_upper_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(upper_expression.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, upper_expression.generated.?.op);
    try std.testing.expectEqualStrings("email", upper_expression.generated.?.field.?);
    try std.testing.expect(upper_expression.index_name != null);
    try std.testing.expectEqualStrings("users_upper_email_idx", upper_expression.index_name.?);

    var concat_expression_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_tenant_status_idx ON users (concat(tenant_id, ':', status));",
    );
    defer concat_expression_index.deinit(alloc);
    var concat_expression_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, upper_expression_indexed.schema_json, concat_expression_index);
    defer concat_expression_indexed.deinit(alloc);
    try std.testing.expect(concat_expression_indexed.requires_rebuild);
    try std.testing.expect(!concat_expression_indexed.validation_required);
    var parsed_concat_expression_indexed = try schema_api.parseValidatedTableSchema(alloc, concat_expression_indexed.schema_json);
    defer parsed_concat_expression_indexed.deinit(alloc);
    const concat_expression_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_concat_expression_indexed);
    defer runtime_schema.freeSchema(alloc, concat_expression_runtime);
    const concat_expression = binder.relationalColumnForField(concat_expression_runtime, "users_tenant_status_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(concat_expression.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, concat_expression.generated.?.op);
    try std.testing.expectEqual(@as(usize, 2), concat_expression.generated.?.fields.len);
    try std.testing.expectEqualStrings("tenant_id", concat_expression.generated.?.fields[0]);
    try std.testing.expectEqualStrings("status", concat_expression.generated.?.fields[1]);
    try std.testing.expectEqualStrings(":", concat_expression.generated.?.separator);
    try std.testing.expect(concat_expression.index_name != null);
    try std.testing.expectEqualStrings("users_tenant_status_idx", concat_expression.index_name.?);

    var md5_expression_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_md5_email_idx ON users (md5(email));",
    );
    defer md5_expression_index.deinit(alloc);
    var md5_expression_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, concat_expression_indexed.schema_json, md5_expression_index);
    defer md5_expression_indexed.deinit(alloc);
    try std.testing.expect(md5_expression_indexed.requires_rebuild);
    try std.testing.expect(!md5_expression_indexed.validation_required);
    var parsed_md5_expression_indexed = try schema_api.parseValidatedTableSchema(alloc, md5_expression_indexed.schema_json);
    defer parsed_md5_expression_indexed.deinit(alloc);
    const md5_expression_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_md5_expression_indexed);
    defer runtime_schema.freeSchema(alloc, md5_expression_runtime);
    const md5_expression = binder.relationalColumnForField(md5_expression_runtime, "users_md5_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(md5_expression.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, md5_expression.generated.?.op);
    try std.testing.expectEqualStrings("email", md5_expression.generated.?.field.?);
    try std.testing.expect(md5_expression.index_name != null);
    try std.testing.expectEqualStrings("users_md5_email_idx", md5_expression.index_name.?);

    var rich_expression_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_replace_idx ON users (replace(status, 'old', 'new'));",
    );
    defer rich_expression_index.deinit(alloc);
    var rich_expression_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, md5_expression_indexed.schema_json, rich_expression_index);
    defer rich_expression_indexed.deinit(alloc);
    try std.testing.expect(rich_expression_indexed.requires_rebuild);
    try std.testing.expect(!rich_expression_indexed.validation_required);
    var parsed_rich_expression_indexed = try schema_api.parseValidatedTableSchema(alloc, rich_expression_indexed.schema_json);
    defer parsed_rich_expression_indexed.deinit(alloc);
    const rich_expression_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_rich_expression_indexed);
    defer runtime_schema.freeSchema(alloc, rich_expression_runtime);
    const rich_expression = binder.relationalColumnForField(rich_expression_runtime, "users_status_replace_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(rich_expression.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, rich_expression.generated.?.op);
    const rich_expression_ast = rich_expression.generated.?.expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.replace, rich_expression_ast.kind);
    try std.testing.expectEqual(@as(usize, 3), rich_expression_ast.operands.len);
    try std.testing.expect(rich_expression.index_name != null);
    try std.testing.expectEqualStrings("users_status_replace_idx", rich_expression.index_name.?);

    var index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_key ON users (tenant_id, lower(email)) WHERE deleted_at IS NULL;",
    );
    defer index.deinit(alloc);
    var indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, status_indexed.schema_json, index);
    defer indexed.deinit(alloc);
    try std.testing.expect(indexed.requires_rebuild);
    try std.testing.expect(indexed.validation_required);
    try expectAppliedDdlWorkActions(indexed, &.{ .rebuild, .validate });

    var unique_covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_email_cover_key ON users (email) INCLUDE (tenant_id, status);",
    );
    defer unique_covering_index.deinit(alloc);
    var unique_covering_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, indexed.schema_json, unique_covering_index);
    defer unique_covering_indexed.deinit(alloc);
    try std.testing.expect(unique_covering_indexed.requires_rebuild);
    try std.testing.expect(unique_covering_indexed.validation_required);
    var parsed_unique_covering_indexed = try schema_api.parseValidatedTableSchema(alloc, unique_covering_indexed.schema_json);
    defer parsed_unique_covering_indexed.deinit(alloc);
    const unique_covering_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_unique_covering_indexed);
    defer runtime_schema.freeSchema(alloc, unique_covering_runtime);
    try std.testing.expectEqual(@as(usize, 2), unique_covering_runtime.unique_constraints.len);
    const unique_covering = unique_covering_runtime.unique_constraints[1];
    try std.testing.expectEqualStrings("users_email_cover_key", unique_covering.name);
    try std.testing.expectEqual(@as(usize, 1), unique_covering.columns.len);
    try std.testing.expectEqualStrings("email", unique_covering.columns[0]);
    try std.testing.expectEqual(@as(usize, 2), unique_covering.include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", unique_covering.include_columns[0]);
    try std.testing.expectEqualStrings("status", unique_covering.include_columns[1]);

    var expression_where_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_active_expr_key ON users (tenant_id, lower(email)) WHERE lower(status) = 'active';",
    );
    defer expression_where_unique_index.deinit(alloc);
    var expression_where_unique_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, indexed.schema_json, expression_where_unique_index);
    defer expression_where_unique_indexed.deinit(alloc);
    try std.testing.expect(expression_where_unique_indexed.requires_rebuild);
    try std.testing.expect(expression_where_unique_indexed.validation_required);
    var parsed_expression_where_unique_indexed = try schema_api.parseValidatedTableSchema(alloc, expression_where_unique_indexed.schema_json);
    defer parsed_expression_where_unique_indexed.deinit(alloc);
    const expression_where_unique_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_expression_where_unique_indexed);
    defer runtime_schema.freeSchema(alloc, expression_where_unique_runtime);
    try std.testing.expectEqual(@as(usize, 2), expression_where_unique_runtime.unique_constraints.len);
    const expression_where_unique = expression_where_unique_runtime.unique_constraints[1];
    try std.testing.expectEqualStrings("users_tenant_lower_email_active_expr_key", expression_where_unique.name);
    try std.testing.expectEqual(@as(usize, 1), expression_where_unique.expressions.len);
    try std.testing.expectEqual(@as(usize, 0), expression_where_unique.where.len);
    try std.testing.expectEqual(@as(usize, 1), expression_where_unique.where_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, expression_where_unique.where_expressions[0].lhs.kind);
    try std.testing.expectEqualStrings("status", expression_where_unique.where_expressions[0].lhs.operands[0].field);
    try std.testing.expectEqualStrings("\"active\"", expression_where_unique.where_expressions[0].rhs[0].value_json);

    var temporal_create = try lowerDdlPlanForCatalogApplyTestAlloc(alloc,
        \\CREATE TABLE prices (
        \\  id uuid PRIMARY KEY,
        \\  sku text NOT NULL,
        \\  valid_from numeric NOT NULL,
        \\  valid_to numeric NOT NULL,
        \\  price numeric,
        \\  PERIOD FOR valid_time (valid_from, valid_to)
        \\);
    );
    defer temporal_create.deinit(alloc);
    var temporal_created = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", temporal_create);
    defer temporal_created.deinit(alloc);

    var temporal_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX prices_sku_valid_time_key ON prices (sku, valid_time WITHOUT OVERLAPS);",
    );
    defer temporal_unique_index.deinit(alloc);
    var temporal_unique_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, temporal_created.schema_json, temporal_unique_index);
    defer temporal_unique_indexed.deinit(alloc);
    try std.testing.expect(temporal_unique_indexed.requires_rebuild);
    try std.testing.expect(temporal_unique_indexed.validation_required);
    var parsed_temporal_unique_indexed = try schema_api.parseValidatedTableSchema(alloc, temporal_unique_indexed.schema_json);
    defer parsed_temporal_unique_indexed.deinit(alloc);
    const temporal_unique_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_temporal_unique_indexed);
    defer runtime_schema.freeSchema(alloc, temporal_unique_runtime);
    try std.testing.expectEqual(@as(usize, 1), temporal_unique_runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("prices_sku_valid_time_key", temporal_unique_runtime.unique_constraints[0].name);
    try std.testing.expectEqualStrings("valid_time", temporal_unique_runtime.unique_constraints[0].without_overlaps_period.?);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, temporal_unique_runtime.unique_constraints[0].validation_state);

    var md5_unique_expression_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_md5_email_key ON users (md5(email));",
    );
    defer md5_unique_expression_index.deinit(alloc);
    var md5_unique_expression_indexed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, indexed.schema_json, md5_unique_expression_index);
    defer md5_unique_expression_indexed.deinit(alloc);
    try std.testing.expect(md5_unique_expression_indexed.requires_rebuild);
    try std.testing.expect(md5_unique_expression_indexed.validation_required);
    try expectAppliedDdlWorkActions(md5_unique_expression_indexed, &.{ .rebuild, .validate });
    var parsed_md5_unique_expression_indexed = try schema_api.parseValidatedTableSchema(alloc, md5_unique_expression_indexed.schema_json);
    defer parsed_md5_unique_expression_indexed.deinit(alloc);
    const md5_unique_expression_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_md5_unique_expression_indexed);
    defer runtime_schema.freeSchema(alloc, md5_unique_expression_runtime);
    try std.testing.expectEqual(@as(usize, 2), md5_unique_expression_runtime.unique_constraints.len);
    const md5_unique_expression = md5_unique_expression_runtime.unique_constraints[1];
    try std.testing.expectEqualStrings("users_md5_email_key", md5_unique_expression.name);
    try std.testing.expectEqual(@as(usize, 1), md5_unique_expression.expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.md5, md5_unique_expression.expressions[0].op);
    try std.testing.expectEqualStrings("email", md5_unique_expression.expressions[0].field);

    var alter = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE users
        \\  ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED,
        \\  ADD CONSTRAINT users_account_fkey FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE RESTRICT,
        \\  ADD CONSTRAINT users_status_check CHECK (status != 'deleted');
        ,
    );
    defer alter.deinit(alloc);
    var altered = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, indexed.schema_json, alter);
    defer altered.deinit(alloc);
    try std.testing.expect(altered.requires_rebuild);
    try std.testing.expect(altered.validation_required);

    var create_no_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE usage_records (tenant_id text NOT NULL, id uuid NOT NULL, valid_from numeric NOT NULL, valid_to numeric NOT NULL, status text);",
    );
    defer create_no_pk.deinit(alloc);
    var no_pk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", create_no_pk);
    defer no_pk.deinit(alloc);
    var parsed_no_pk = try schema_api.parseValidatedTableSchema(alloc, no_pk.schema_json);
    defer parsed_no_pk.deinit(alloc);
    const no_pk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_no_pk);
    defer runtime_schema.freeSchema(alloc, no_pk_runtime);
    try std.testing.expect(no_pk_runtime.primary_key == null);
    try std.testing.expectEqual(@as(usize, 5), no_pk_runtime.relational_columns.len);

    var add_primary_key = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD PERIOD FOR valid_time (valid_from, valid_to),
        \\  ADD CONSTRAINT usage_records_pk PRIMARY KEY (tenant_id, id, valid_time WITHOUT OVERLAPS) INCLUDE (status);
        ,
    );
    defer add_primary_key.deinit(alloc);
    var primary_keyed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, no_pk.schema_json, add_primary_key);
    defer primary_keyed.deinit(alloc);
    try std.testing.expect(primary_keyed.requires_rebuild);
    try std.testing.expect(primary_keyed.validation_required);
    try std.testing.expect(!primary_keyed.rewrite_required);
    var parsed_primary_keyed = try schema_api.parseValidatedTableSchema(alloc, primary_keyed.schema_json);
    defer parsed_primary_keyed.deinit(alloc);
    const primary_keyed_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_primary_keyed);
    defer runtime_schema.freeSchema(alloc, primary_keyed_runtime);
    try std.testing.expect(primary_keyed_runtime.primary_key != null);
    try std.testing.expectEqualStrings("usage_records_pk", primary_keyed_runtime.primary_key.?.name.?);
    try std.testing.expectEqualStrings("tenant_id", primary_keyed_runtime.primary_key.?.columns[0]);
    try std.testing.expectEqualStrings("id", primary_keyed_runtime.primary_key.?.columns[1]);
    try std.testing.expectEqual(@as(usize, 1), primary_keyed_runtime.primary_key.?.include_columns.len);
    try std.testing.expectEqualStrings("status", primary_keyed_runtime.primary_key.?.include_columns[0]);
    try std.testing.expectEqualStrings("valid_time", primary_keyed_runtime.primary_key.?.without_overlaps_period.?);
    try std.testing.expectEqual(@as(usize, 1), primary_keyed_runtime.periods.len);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, primary_keyed.schema_json, add_primary_key));

    var add_temporal = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE users
        \\  ADD COLUMN valid_from numeric NOT NULL,
        \\  ADD COLUMN valid_to numeric NOT NULL,
        \\  ADD PERIOD FOR valid_time (valid_from, valid_to),
        \\  ADD CONSTRAINT users_tenant_valid_key UNIQUE (tenant_id, valid_time WITHOUT OVERLAPS);
        ,
    );
    defer add_temporal.deinit(alloc);
    var temporal = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, altered.schema_json, add_temporal);
    defer temporal.deinit(alloc);
    try std.testing.expect(temporal.requires_rebuild);
    try std.testing.expect(temporal.validation_required);
    try std.testing.expect(temporal.rewrite_required);
    try expectAppliedDdlWorkActions(temporal, &.{ .rebuild, .validate, .rewrite });
    var parsed_temporal = try schema_api.parseValidatedTableSchema(alloc, temporal.schema_json);
    defer parsed_temporal.deinit(alloc);
    const temporal_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_temporal);
    defer runtime_schema.freeSchema(alloc, temporal_runtime);
    try std.testing.expectEqual(@as(usize, 1), temporal_runtime.periods.len);
    try std.testing.expectEqualStrings("valid_time", temporal_runtime.periods[0].name);
    try std.testing.expectEqual(@as(usize, 2), temporal_runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_valid_key", temporal_runtime.unique_constraints[1].name);
    try std.testing.expectEqualStrings("valid_time", temporal_runtime.unique_constraints[1].without_overlaps_period.?);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.enforced, temporal_runtime.unique_constraints[1].validation_state);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, temporal.schema_json, add_temporal));

    var not_valid = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE users ADD CONSTRAINT users_status_known_check CHECK (status != 'unknown') NOT VALID;",
    );
    defer not_valid.deinit(alloc);
    var with_unvalidated_check = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, altered.schema_json, not_valid);
    defer with_unvalidated_check.deinit(alloc);
    try std.testing.expect(with_unvalidated_check.validation_required);

    var validate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY users VALIDATE CONSTRAINT users_status_known_check;",
    );
    defer validate.deinit(alloc);
    var validated = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, with_unvalidated_check.schema_json, validate);
    defer validated.deinit(alloc);
    try std.testing.expect(validated.validation_required);

    var validate_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY users VALIDATE CONSTRAINT users_pkey;",
    );
    defer validate_default_pk.deinit(alloc);
    var validated_default_pk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, validated.schema_json, validate_default_pk);
    defer validated_default_pk.deinit(alloc);
    var parsed_validated_default_pk = try schema_api.parseValidatedTableSchema(alloc, validated_default_pk.schema_json);
    defer parsed_validated_default_pk.deinit(alloc);
    const validated_default_pk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_validated_default_pk);
    defer runtime_schema.freeSchema(alloc, validated_default_pk_runtime);
    try std.testing.expect(validated_default_pk_runtime.primary_key != null);
    try std.testing.expect(validated_default_pk_runtime.primary_key.?.name == null);

    var rename_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE users RENAME CONSTRAINT users_pkey TO users_id_pk;",
    );
    defer rename_default_pk.deinit(alloc);
    var renamed_default_pk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, validated.schema_json, rename_default_pk);
    defer renamed_default_pk.deinit(alloc);
    var parsed_renamed_default_pk = try schema_api.parseValidatedTableSchema(alloc, renamed_default_pk.schema_json);
    defer parsed_renamed_default_pk.deinit(alloc);
    const renamed_default_pk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_renamed_default_pk);
    defer runtime_schema.freeSchema(alloc, renamed_default_pk_runtime);
    try std.testing.expectEqualStrings("users_id_pk", renamed_default_pk_runtime.primary_key.?.name.?);

    var rename_default_pk_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE users RENAME CONSTRAINT users_pkey TO users_tenant_lower_email_key;",
    );
    defer rename_default_pk_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, validated.schema_json, rename_default_pk_duplicate));

    var drop_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE users DROP CONSTRAINT users_pkey;",
    );
    defer drop_default_pk.deinit(alloc);
    var without_default_pk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, validated.schema_json, drop_default_pk);
    defer without_default_pk.deinit(alloc);
    try std.testing.expect(without_default_pk.requires_rebuild);
    try std.testing.expect(!without_default_pk.validation_required);
    try std.testing.expect(without_default_pk.rewrite_required);
    var parsed_without_default_pk = try schema_api.parseValidatedTableSchema(alloc, without_default_pk.schema_json);
    defer parsed_without_default_pk.deinit(alloc);
    const without_default_pk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_without_default_pk);
    defer runtime_schema.freeSchema(alloc, without_default_pk_runtime);
    try std.testing.expect(without_default_pk_runtime.primary_key == null);

    var drop_named_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE users DROP CONSTRAINT users_id_pk;",
    );
    defer drop_named_pk.deinit(alloc);
    var without_named_pk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, renamed_default_pk.schema_json, drop_named_pk);
    defer without_named_pk.deinit(alloc);
    try std.testing.expect(without_named_pk.requires_rebuild);
    try std.testing.expect(!without_named_pk.validation_required);
    try std.testing.expect(without_named_pk.rewrite_required);
    var parsed_without_named_pk = try schema_api.parseValidatedTableSchema(alloc, without_named_pk.schema_json);
    defer parsed_without_named_pk.deinit(alloc);
    const without_named_pk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_without_named_pk);
    defer runtime_schema.freeSchema(alloc, without_named_pk_runtime);
    try std.testing.expect(without_named_pk_runtime.primary_key == null);

    var trigger = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TRIGGER users_updated_at BEFORE UPDATE ON users EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(alloc);
    var updated = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, validated.schema_json, trigger);
    defer updated.deinit(alloc);
    try std.testing.expect(!updated.requires_rebuild);
    try std.testing.expect(!updated.validation_required);

    var parsed = try schema_api.parseValidatedTableSchema(alloc, updated.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(@as(usize, 8), runtime.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), runtime.unique_constraints.len);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, runtime.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), runtime.foreign_keys.len);
    try std.testing.expectEqualStrings("users_account_fkey", runtime.foreign_keys[0].name);
    try std.testing.expectEqual(runtime_schema.ForeignKeyValidationState.unvalidated, runtime.foreign_keys[0].validation_state);
    try std.testing.expectEqual(@as(usize, 2), runtime.checks.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, runtime.checks[0].validation_state);
    try std.testing.expectEqualStrings("users_status_known_check", runtime.checks[1].name);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.enforced, runtime.checks[1].validation_state);
    const status = binder.relationalColumnForField(runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, status.index_lifecycle);
    try std.testing.expect(status.index_generation != 0);
    try std.testing.expect(status.index_name != null);
    try std.testing.expectEqualStrings("users_status_idx", status.index_name.?);
    const generated = binder.relationalColumnForField(runtime, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    const updated_at = binder.relationalColumnForField(runtime, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(updated_at.on_update_value != null);

    var drop_trigger = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TRIGGER users_updated_at ON users;");
    defer drop_trigger.deinit(alloc);
    var update_policy_dropped = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_trigger);
    defer update_policy_dropped.deinit(alloc);
    try std.testing.expect(!update_policy_dropped.requires_rebuild);
    try std.testing.expect(!update_policy_dropped.validation_required);
    var parsed_update_policy_dropped = try schema_api.parseValidatedTableSchema(alloc, update_policy_dropped.schema_json);
    defer parsed_update_policy_dropped.deinit(alloc);
    const update_policy_dropped_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_update_policy_dropped);
    defer runtime_schema.freeSchema(alloc, update_policy_dropped_runtime);
    const update_policy_dropped_column = binder.relationalColumnForField(update_policy_dropped_runtime, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(update_policy_dropped_column.on_update_value == null);

    var drop_status_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_status_idx;");
    defer drop_status_index.deinit(alloc);
    var status_index_dropped = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_status_index);
    defer status_index_dropped.deinit(alloc);
    try std.testing.expect(!status_index_dropped.requires_rebuild);
    try std.testing.expect(!status_index_dropped.validation_required);
    var parsed_status_index_dropped = try schema_api.parseValidatedTableSchema(alloc, status_index_dropped.schema_json);
    defer parsed_status_index_dropped.deinit(alloc);
    const status_index_dropped_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_status_index_dropped);
    defer runtime_schema.freeSchema(alloc, status_index_dropped_runtime);
    const dropped_json_status = binder.relationalColumnForField(status_index_dropped_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!dropped_json_status.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.ready, dropped_json_status.index_lifecycle);
    try std.testing.expectEqual(@as(u64, 0), dropped_json_status.index_generation);
    try std.testing.expect(dropped_json_status.index_name == null);

    var drop_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_tenant_lower_email_key;");
    defer drop_unique_index.deinit(alloc);
    var unique_index_dropped = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_unique_index);
    defer unique_index_dropped.deinit(alloc);
    var parsed_unique_index_dropped = try schema_api.parseValidatedTableSchema(alloc, unique_index_dropped.schema_json);
    defer parsed_unique_index_dropped.deinit(alloc);
    const unique_index_dropped_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_unique_index_dropped);
    defer runtime_schema.freeSchema(alloc, unique_index_dropped_runtime);
    try std.testing.expectEqual(@as(usize, 0), unique_index_dropped_runtime.unique_constraints.len);

    var set_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'pending';");
    defer set_default.deinit(alloc);
    var defaulted = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, set_default);
    defer defaulted.deinit(alloc);
    try std.testing.expect(!defaulted.requires_rebuild);
    try std.testing.expect(!defaulted.validation_required);
    var parsed_defaulted = try schema_api.parseValidatedTableSchema(alloc, defaulted.schema_json);
    defer parsed_defaulted.deinit(alloc);
    const defaulted_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_defaulted);
    defer runtime_schema.freeSchema(alloc, defaulted_runtime);
    const defaulted_status = binder.relationalColumnForField(defaulted_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(defaulted_status.default_value != null);
    try std.testing.expectEqualStrings("\"pending\"", defaulted_status.default_value.?.value_json);

    var set_casted_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN updated_at_ns SET DEFAULT '7'::numeric;");
    defer set_casted_numeric_default.deinit(alloc);
    var casted_numeric_defaulted = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, set_casted_numeric_default);
    defer casted_numeric_defaulted.deinit(alloc);
    var parsed_casted_numeric_defaulted = try schema_api.parseValidatedTableSchema(alloc, casted_numeric_defaulted.schema_json);
    defer parsed_casted_numeric_defaulted.deinit(alloc);
    const casted_numeric_defaulted_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_casted_numeric_defaulted);
    defer runtime_schema.freeSchema(alloc, casted_numeric_defaulted_runtime);
    const casted_numeric_updated_at = binder.relationalColumnForField(casted_numeric_defaulted_runtime, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(casted_numeric_updated_at.default_value != null);
    try std.testing.expectEqualStrings("7", casted_numeric_updated_at.default_value.?.value_json);

    var set_text_cast_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN updated_at_ns SET DEFAULT '7'::text;");
    defer set_text_cast_numeric_default.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, set_text_cast_numeric_default));

    var drop_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status DROP DEFAULT;");
    defer drop_default.deinit(alloc);
    var undefaulted = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, defaulted.schema_json, drop_default);
    defer undefaulted.deinit(alloc);
    try std.testing.expect(!undefaulted.requires_rebuild);
    try std.testing.expect(!undefaulted.validation_required);
    var parsed_undefaulted = try schema_api.parseValidatedTableSchema(alloc, undefaulted.schema_json);
    defer parsed_undefaulted.deinit(alloc);
    const undefaulted_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_undefaulted);
    defer runtime_schema.freeSchema(alloc, undefaulted_runtime);
    const undefaulted_status = binder.relationalColumnForField(undefaulted_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(undefaulted_status.default_value == null);

    var set_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status SET NOT NULL;");
    defer set_not_null.deinit(alloc);
    var required_status_schema = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, set_not_null);
    defer required_status_schema.deinit(alloc);
    try std.testing.expect(!required_status_schema.requires_rebuild);
    try std.testing.expect(required_status_schema.validation_required);
    var parsed_required_status = try schema_api.parseValidatedTableSchema(alloc, required_status_schema.schema_json);
    defer parsed_required_status.deinit(alloc);
    const required_status_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_required_status);
    defer runtime_schema.freeSchema(alloc, required_status_runtime);
    const required_json_status = binder.relationalColumnForField(required_status_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!required_json_status.nullable);

    var drop_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status DROP NOT NULL;");
    defer drop_not_null.deinit(alloc);
    var nullable_status_schema = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, required_status_schema.schema_json, drop_not_null);
    defer nullable_status_schema.deinit(alloc);
    try std.testing.expect(!nullable_status_schema.requires_rebuild);
    try std.testing.expect(!nullable_status_schema.validation_required);
    var parsed_nullable_status = try schema_api.parseValidatedTableSchema(alloc, nullable_status_schema.schema_json);
    defer parsed_nullable_status.deinit(alloc);
    const nullable_status_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_nullable_status);
    defer runtime_schema.freeSchema(alloc, nullable_status_runtime);
    const nullable_json_status = binder.relationalColumnForField(nullable_status_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(nullable_json_status.nullable);

    var drop_pk_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN id DROP NOT NULL;");
    defer drop_pk_not_null.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_pk_not_null));

    var alter_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN updated_at_ns TYPE timestamptz USING (updated_at_ns)::timestamptz;");
    defer alter_type.deinit(alloc);
    var typed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, alter_type);
    defer typed.deinit(alloc);
    try std.testing.expect(typed.requires_rebuild);
    try std.testing.expect(typed.validation_required);
    try std.testing.expect(typed.rewrite_required);
    var parsed_typed = try schema_api.parseValidatedTableSchema(alloc, typed.schema_json);
    defer parsed_typed.deinit(alloc);
    const typed_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_typed);
    defer runtime_schema.freeSchema(alloc, typed_runtime);
    const typed_updated_at = binder.relationalColumnForField(typed_runtime, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, typed_updated_at.field_type);
    try std.testing.expect(typed_updated_at.on_update_value != null);

    var alter_status_collated = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status TYPE text COLLATE \"C\";");
    defer alter_status_collated.deinit(alloc);
    var collated = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, alter_status_collated);
    defer collated.deinit(alloc);
    try std.testing.expect(collated.requires_rebuild);
    try std.testing.expect(collated.validation_required);
    try std.testing.expect(collated.rewrite_required);
    var parsed_collated = try schema_api.parseValidatedTableSchema(alloc, collated.schema_json);
    defer parsed_collated.deinit(alloc);
    const collated_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_collated);
    defer runtime_schema.freeSchema(alloc, collated_runtime);
    const collated_status = binder.relationalColumnForField(collated_runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, collated_status.field_type);
    try std.testing.expectEqualStrings("C", collated_status.collation.?);

    var alter_collated_status_to_numeric = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN status TYPE numeric;");
    defer alter_collated_status_to_numeric.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, collated.schema_json, alter_collated_status_to_numeric));

    var alter_generated_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ALTER COLUMN tenant_status_key TYPE text;");
    defer alter_generated_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, alter_generated_type));

    var rename_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME COLUMN status TO state;");
    defer rename_status.deinit(alloc);
    var renamed = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_status);
    defer renamed.deinit(alloc);
    try std.testing.expect(renamed.requires_rebuild);
    try std.testing.expect(renamed.validation_required);
    try std.testing.expect(renamed.rewrite_required);
    var parsed_renamed = try schema_api.parseValidatedTableSchema(alloc, renamed.schema_json);
    defer parsed_renamed.deinit(alloc);
    const renamed_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_renamed);
    defer runtime_schema.freeSchema(alloc, renamed_runtime);
    try std.testing.expect(binder.relationalColumnForField(renamed_runtime, "status", null) == null);
    const state = binder.relationalColumnForField(renamed_runtime, "state", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("state", state.path);
    const renamed_generated = binder.relationalColumnForField(renamed_runtime, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("state", renamed_generated.generated.?.fields[1]);
    try std.testing.expectEqualStrings("state", renamed_runtime.checks[0].field);

    var rename_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME COLUMN status TO tenant_id;");
    defer rename_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_duplicate));

    var rename_unique_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME CONSTRAINT users_tenant_lower_email_key TO users_tenant_email_ci_key;");
    defer rename_unique_constraint.deinit(alloc);
    var renamed_unique = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_unique_constraint);
    defer renamed_unique.deinit(alloc);
    try std.testing.expect(!renamed_unique.requires_rebuild);
    try std.testing.expect(!renamed_unique.validation_required);
    var parsed_renamed_unique = try schema_api.parseValidatedTableSchema(alloc, renamed_unique.schema_json);
    defer parsed_renamed_unique.deinit(alloc);
    const renamed_unique_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_renamed_unique);
    defer runtime_schema.freeSchema(alloc, renamed_unique_runtime);
    try std.testing.expectEqualStrings("users_tenant_email_ci_key", renamed_unique_runtime.unique_constraints[0].name);

    var rename_fk_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME CONSTRAINT users_account_fkey TO users_account_fk;");
    defer rename_fk_constraint.deinit(alloc);
    var renamed_fk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_fk_constraint);
    defer renamed_fk.deinit(alloc);
    var parsed_renamed_fk = try schema_api.parseValidatedTableSchema(alloc, renamed_fk.schema_json);
    defer parsed_renamed_fk.deinit(alloc);
    const renamed_fk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_renamed_fk);
    defer runtime_schema.freeSchema(alloc, renamed_fk_runtime);
    try std.testing.expectEqualStrings("users_account_fk", renamed_fk_runtime.foreign_keys[0].name);

    var rename_check_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME CONSTRAINT users_status_check TO users_state_check;");
    defer rename_check_constraint.deinit(alloc);
    var renamed_check = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_check_constraint);
    defer renamed_check.deinit(alloc);
    var parsed_renamed_check = try schema_api.parseValidatedTableSchema(alloc, renamed_check.schema_json);
    defer parsed_renamed_check.deinit(alloc);
    const renamed_check_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_renamed_check);
    defer runtime_schema.freeSchema(alloc, renamed_check_runtime);
    try std.testing.expectEqualStrings("users_state_check", renamed_check_runtime.checks[0].name);

    var rename_constraint_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users RENAME CONSTRAINT users_status_check TO users_tenant_lower_email_key;");
    defer rename_constraint_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, rename_constraint_duplicate));

    var drop_check = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP CONSTRAINT users_status_known_check;");
    defer drop_check.deinit(alloc);
    var without_check = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_check);
    defer without_check.deinit(alloc);
    var parsed_without_check = try schema_api.parseValidatedTableSchema(alloc, without_check.schema_json);
    defer parsed_without_check.deinit(alloc);
    const without_check_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_without_check);
    defer runtime_schema.freeSchema(alloc, without_check_runtime);
    try std.testing.expectEqual(@as(usize, 1), without_check_runtime.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_check_runtime.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), without_check_runtime.checks.len);

    var drop_unique = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP CONSTRAINT users_tenant_lower_email_key;");
    defer drop_unique.deinit(alloc);
    var without_unique = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_unique);
    defer without_unique.deinit(alloc);
    var parsed_without_unique = try schema_api.parseValidatedTableSchema(alloc, without_unique.schema_json);
    defer parsed_without_unique.deinit(alloc);
    const without_unique_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_without_unique);
    defer runtime_schema.freeSchema(alloc, without_unique_runtime);
    try std.testing.expectEqual(@as(usize, 0), without_unique_runtime.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_unique_runtime.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 2), without_unique_runtime.checks.len);

    var drop_fk = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP CONSTRAINT IF EXISTS users_account_fkey;");
    defer drop_fk.deinit(alloc);
    var without_fk = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_fk);
    defer without_fk.deinit(alloc);
    var parsed_without_fk = try schema_api.parseValidatedTableSchema(alloc, without_fk.schema_json);
    defer parsed_without_fk.deinit(alloc);
    const without_fk_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_without_fk);
    defer runtime_schema.freeSchema(alloc, without_fk_runtime);
    try std.testing.expectEqual(@as(usize, 1), without_fk_runtime.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 0), without_fk_runtime.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 2), without_fk_runtime.checks.len);

    var drop_missing_constraint_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP CONSTRAINT IF EXISTS missing_constraint;");
    defer drop_missing_constraint_if_exists.deinit(alloc);
    var unchanged_constraints = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_missing_constraint_if_exists);
    defer unchanged_constraints.deinit(alloc);
    var parsed_unchanged_constraints = try schema_api.parseValidatedTableSchema(alloc, unchanged_constraints.schema_json);
    defer parsed_unchanged_constraints.deinit(alloc);
    const unchanged_constraints_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_unchanged_constraints);
    defer runtime_schema.freeSchema(alloc, unchanged_constraints_runtime);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints_runtime.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints_runtime.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 2), unchanged_constraints_runtime.checks.len);

    var drop_missing_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP CONSTRAINT missing_constraint;");
    defer drop_missing_constraint.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_missing_constraint));

    var drop_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP COLUMN status;");
    defer drop_status.deinit(alloc);
    var dropped = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_status);
    defer dropped.deinit(alloc);
    try std.testing.expect(dropped.requires_rebuild);
    try std.testing.expect(dropped.validation_required);
    try std.testing.expect(dropped.rewrite_required);
    var parsed_dropped = try schema_api.parseValidatedTableSchema(alloc, dropped.schema_json);
    defer parsed_dropped.deinit(alloc);
    const dropped_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_dropped);
    defer runtime_schema.freeSchema(alloc, dropped_runtime);
    try std.testing.expect(binder.relationalColumnForField(dropped_runtime, "status", null) == null);
    try std.testing.expect(binder.relationalColumnForField(dropped_runtime, "tenant_status_key", null) == null);
    try std.testing.expectEqual(@as(usize, 1), dropped_runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_lower_email_key", dropped_runtime.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), dropped_runtime.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_runtime.checks.len);

    var drop_missing_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP COLUMN IF EXISTS missing_column;");
    defer drop_missing_if_exists.deinit(alloc);
    var unchanged = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_missing_if_exists);
    defer unchanged.deinit(alloc);
    var parsed_unchanged = try schema_api.parseValidatedTableSchema(alloc, unchanged.schema_json);
    defer parsed_unchanged.deinit(alloc);
    const unchanged_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_unchanged);
    defer runtime_schema.freeSchema(alloc, unchanged_runtime);
    try std.testing.expectEqual(@as(usize, 8), unchanged_runtime.relational_columns.len);

    var drop_status_restrict = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP COLUMN status RESTRICT;");
    defer drop_status_restrict.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_status_restrict));

    var drop_primary_key = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users DROP COLUMN id;");
    defer drop_primary_key.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, drop_primary_key));

    var duplicate_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE users ADD COLUMN IF NOT EXISTS status text REFERENCES accounts(id);");
    defer duplicate_if_not_exists.deinit(alloc);
    var unchanged_existing = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, updated.schema_json, duplicate_if_not_exists);
    defer unchanged_existing.deinit(alloc);
    var parsed_unchanged_existing = try schema_api.parseValidatedTableSchema(alloc, unchanged_existing.schema_json);
    defer parsed_unchanged_existing.deinit(alloc);
    const unchanged_existing_runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_unchanged_existing);
    defer runtime_schema.freeSchema(alloc, unchanged_existing_runtime);
    try std.testing.expectEqual(@as(usize, 8), unchanged_existing_runtime.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_existing_runtime.foreign_keys.len);

    var missing_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE IF EXISTS missing_users ADD COLUMN status text;");
    defer missing_table_if_exists.deinit(alloc);
    var missing_noop = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", missing_table_if_exists);
    defer missing_noop.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), missing_noop.schema_json.len);
    try std.testing.expect(!missing_noop.requires_rebuild);
    try std.testing.expect(!missing_noop.validation_required);

    var missing_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE missing_users ADD COLUMN status text;");
    defer missing_table.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", missing_table));

    var missing_drop_index_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX IF EXISTS missing_users_status_idx;");
    defer missing_drop_index_if_exists.deinit(alloc);
    var missing_drop_noop = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", missing_drop_index_if_exists);
    defer missing_drop_noop.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), missing_drop_noop.schema_json.len);

    var drop_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE users;");
    defer drop_table.deinit(alloc);
    var dropped_table = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, created.schema_json, drop_table);
    defer dropped_table.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dropped_table.schema_json.len);
    try std.testing.expect(!dropped_table.requires_rebuild);
    try std.testing.expect(!dropped_table.validation_required);
    try std.testing.expect(!dropped_table.rewrite_required);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", drop_table));

    var drop_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE IF EXISTS users;");
    defer drop_table_if_exists.deinit(alloc);
    var missing_drop_table_noop = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, "", drop_table_if_exists);
    defer missing_drop_table_noop.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), missing_drop_table_noop.schema_json.len);
}

test "catalog apply executes prepared transaction recovery intents" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime_store = try backend.runtimeStore(alloc, .{ .name = "sql-prepared-txn" });
    defer runtime_store.deinit();

    var prepare = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "PREPARE TRANSACTION 'usage_batch';");
    defer prepare.deinit(alloc);
    const prepare_plan = switch (prepare) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    const expected_txn_id = preparedTransactionTxnIdFromGid("usage_batch");
    const prepared = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, prepare_plan, 1_000);
    try std.testing.expectEqual(PreparedTransactionRecoveryOperation.register_prepared, prepared.operation);
    try std.testing.expectEqualStrings("usage_batch", prepared.gid);
    try std.testing.expectEqualSlices(u8, &expected_txn_id, &prepared.txn_id);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, prepared.status);
    try std.testing.expect(prepared.coordinator_recovery_log);
    const prepared_fingerprint = try preparedTransactionRecoveryFingerprintAlloc(alloc, preparedTransactionRecoveryIntentFromPlan(prepare_plan));
    defer alloc.free(prepared_fingerprint);
    try std.testing.expectEqualStrings("prepared_txn_recovery:op=register_prepared:gid=usage_batch:audit=prepare:requires_coordinator=true", prepared_fingerprint);

    var manager = try transactions_mod.TxnManager.init(alloc, &runtime_store);
    defer manager.deinit();
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try manager.getTransactionStatus(expected_txn_id));
    try std.testing.expectError(error.PreparedTransactionAlreadyExists, executePreparedTransactionRecoveryPlan(alloc, &runtime_store, prepare_plan, 1_001));

    var commit = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMIT PREPARED 'usage_batch';");
    defer commit.deinit(alloc);
    const commit_plan = switch (commit) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    const committed = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, commit_plan, 2_000);
    try std.testing.expectEqual(PreparedTransactionRecoveryOperation.resolve_commit, committed.operation);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, committed.status);
    const committed_fingerprint = try preparedTransactionRecoveryFingerprintAlloc(alloc, preparedTransactionRecoveryIntentFromPlan(commit_plan));
    defer alloc.free(committed_fingerprint);
    try std.testing.expectEqualStrings("prepared_txn_recovery:op=resolve_commit:gid=usage_batch:audit=commit:requires_coordinator=true", committed_fingerprint);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try manager.getTransactionStatus(expected_txn_id));
    const committed_retry = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, commit_plan, 2_500);
    try std.testing.expectEqual(PreparedTransactionRecoveryOperation.resolve_commit, committed_retry.operation);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, committed_retry.status);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try manager.getTransactionStatus(expected_txn_id));

    var rollback_committed = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ROLLBACK PREPARED 'usage_batch';");
    defer rollback_committed.deinit(alloc);
    const rollback_committed_plan = switch (rollback_committed) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectError(error.PreparedTransactionDecisionConflict, executePreparedTransactionRecoveryPlan(alloc, &runtime_store, rollback_committed_plan, 3_000));

    var rollback = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "PREPARE TRANSACTION 'usage_abort';");
    defer rollback.deinit(alloc);
    const rollback_prepare_plan = switch (rollback) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    _ = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, rollback_prepare_plan, 4_000);

    var rollback_prepared = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ROLLBACK PREPARED 'usage_abort';");
    defer rollback_prepared.deinit(alloc);
    const rollback_prepared_plan = switch (rollback_prepared) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    const aborted = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, rollback_prepared_plan, 5_000);
    try std.testing.expectEqual(PreparedTransactionRecoveryOperation.resolve_rollback, aborted.operation);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, aborted.status);
    const aborted_retry = try executePreparedTransactionRecoveryPlan(alloc, &runtime_store, rollback_prepared_plan, 5_500);
    try std.testing.expectEqual(PreparedTransactionRecoveryOperation.resolve_rollback, aborted_retry.operation);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, aborted_retry.status);

    var missing_commit = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMIT PREPARED 'missing_gid';");
    defer missing_commit.deinit(alloc);
    const missing_commit_plan = switch (missing_commit) {
        .prepared_transaction => |plan| plan,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectError(error.PreparedTransactionNotFound, executePreparedTransactionRecoveryPlan(alloc, &runtime_store, missing_commit_plan, 6_000));
}

test "catalog apply applies create table ddl plan to owned runtime schema" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\CREATE TABLE usage_records (
        \\  id uuid PRIMARY KEY,
        \\  tenant_id text COLLATE "C" NOT NULL,
        \\  status text DEFAULT 'open',
        \\  updated_at_ns bigint DEFAULT 0::numeric,
        \\  CONSTRAINT usage_records_tenant_key UNIQUE (tenant_id),
        \\  CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id),
        \\  CONSTRAINT usage_records_updated_check CHECK (updated_at_ns >= 0::numeric)
        \\);
        ,
    );
    defer lowered.deinit(alloc);

    const schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, lowered);
    defer runtime_schema.freeSchema(alloc, schema);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, lowered));

    var idempotent = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE IF NOT EXISTS usage_records (id uuid PRIMARY KEY);",
    );
    defer idempotent.deinit(alloc);
    const unchanged = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, idempotent);
    defer runtime_schema.freeSchema(alloc, unchanged);
    try std.testing.expectEqual(@as(usize, schema.relational_columns.len), unchanged.relational_columns.len);
    try std.testing.expectEqualStrings("id", unchanged.primary_key.?.columns[0]);

    const tenant_id = binder.relationalColumnForField(schema, "tenant_id", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("C", tenant_id.collation.?);

    var table_clone = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE usage_records_copy (LIKE usage_records INCLUDING ALL);");
    defer table_clone.deinit(alloc);
    const cloned = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, table_clone);
    defer runtime_schema.freeSchema(alloc, cloned);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, cloned.storage_mode);
    try std.testing.expectEqual(@as(usize, schema.relational_columns.len), cloned.relational_columns.len);
    try std.testing.expect(cloned.primary_key != null);
    try std.testing.expectEqualStrings("id", cloned.primary_key.?.columns[0]);
    const cloned_tenant_id = binder.relationalColumnForField(cloned, "tenant_id", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("C", cloned_tenant_id.collation.?);
    const cloned_status = binder.relationalColumnForField(cloned, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(cloned_status.default_value != null);
    try std.testing.expectEqualStrings("\"open\"", cloned_status.default_value.?.value_json);
    try std.testing.expectEqual(@as(usize, 1), cloned.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_key", cloned.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), cloned.foreign_keys.len);
    try std.testing.expectEqualStrings("usage_records_tenant_fkey", cloned.foreign_keys[0].name);
    try std.testing.expectEqual(@as(usize, 1), cloned.checks.len);
    try std.testing.expectEqualStrings("usage_records_updated_check", cloned.checks[0].name);

    var table_clone_without_constraints = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE usage_records_copy (LIKE usage_records);");
    defer table_clone_without_constraints.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, table_clone_without_constraints));
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, table_clone));

    try std.testing.expectEqual(runtime_schema.StorageMode.relational, schema.storage_mode);
    try std.testing.expect(schema.enforce_types);
    try std.testing.expectEqual(@as(usize, 4), schema.relational_columns.len);
    const updated_at = binder.relationalColumnForField(schema, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(updated_at.default_value != null);
    try std.testing.expectEqualStrings("0", updated_at.default_value.?.value_json);
    try std.testing.expect(schema.primary_key != null);
    try std.testing.expectEqualStrings("id", schema.primary_key.?.columns[0]);
    try std.testing.expectEqual(@as(usize, 1), schema.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_key", schema.unique_constraints[0].name);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.enforced, schema.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), schema.foreign_keys.len);
    try std.testing.expectEqualStrings("usage_records_tenant_fkey", schema.foreign_keys[0].name);
    try std.testing.expectEqual(@as(usize, 1), schema.checks.len);
    try std.testing.expectEqualStrings("usage_records_updated_check", schema.checks[0].name);
    try std.testing.expectEqualStrings("0", schema.checks[0].value_json.?);
}

test "catalog apply applies additive alter table ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    const no_pk_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .nullable = false },
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "valid_from", .path = "valid_from", .field_type = .numeric, .nullable = false },
        .{ .name = "valid_to", .path = "valid_to", .field_type = .numeric, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = true },
    };
    const no_pk_schema: runtime_schema.TableSchema = .{
        .version = 1,
        .default_type = try alloc.dupe(u8, "row"),
        .ttl_field = try alloc.dupe(u8, "_timestamp"),
        .enforce_types = true,
        .storage_mode = .relational,
        .relational_columns = try ddl_plan.cloneDdlRelationalColumns(alloc, &no_pk_columns),
    };
    defer runtime_schema.freeSchema(alloc, no_pk_schema);
    var add_primary_key = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD PERIOD FOR valid_time (valid_from, valid_to),
        \\  ADD CONSTRAINT usage_records_pk PRIMARY KEY (tenant_id, id, valid_time WITHOUT OVERLAPS) INCLUDE (status);
        ,
    );
    defer add_primary_key.deinit(alloc);
    const primary_keyed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, no_pk_schema, add_primary_key);
    defer runtime_schema.freeSchema(alloc, primary_keyed);
    try std.testing.expect(primary_keyed.primary_key != null);
    try std.testing.expectEqualStrings("usage_records_pk", primary_keyed.primary_key.?.name.?);
    try std.testing.expectEqualStrings("tenant_id", primary_keyed.primary_key.?.columns[0]);
    try std.testing.expectEqualStrings("id", primary_keyed.primary_key.?.columns[1]);
    try std.testing.expectEqual(@as(usize, 1), primary_keyed.primary_key.?.include_columns.len);
    try std.testing.expectEqualStrings("status", primary_keyed.primary_key.?.include_columns[0]);
    try std.testing.expectEqualStrings("valid_time", primary_keyed.primary_key.?.without_overlaps_period.?);
    try std.testing.expectEqual(@as(usize, 1), primary_keyed.periods.len);
    try std.testing.expectEqualStrings("valid_time", primary_keyed.periods[0].name);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, primary_keyed, add_primary_key));

    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, tenant_id text NOT NULL, status text);",
    );
    defer create.deinit(alloc);
    const schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var alter = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED,
        \\  ADD COLUMN status_upper_key text GENERATED ALWAYS AS (upper(status)) STORED,
        \\  ADD CONSTRAINT usage_records_tenant_status_key UNIQUE (tenant_id, status),
        \\  ADD CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id),
        \\  ADD CONSTRAINT usage_records_status_check CHECK (status != 'deleted');
        ,
    );
    defer alter.deinit(alloc);

    const updated = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, alter);
    defer runtime_schema.freeSchema(alloc, updated);

    try std.testing.expectEqual(@as(usize, 5), updated.relational_columns.len);
    const generated = binder.relationalColumnForField(updated, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, generated.generated.?.op);
    const upper_generated = binder.relationalColumnForField(updated, "status_upper_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(upper_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, upper_generated.generated.?.op);
    try std.testing.expectEqualStrings("status", upper_generated.generated.?.field.?);
    try std.testing.expectEqual(@as(usize, 1), updated.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_status_key", updated.unique_constraints[0].name);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.enforced, updated.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), updated.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), updated.checks.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, updated.checks[0].validation_state);

    var add_temporal = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD COLUMN valid_from numeric NOT NULL,
        \\  ADD COLUMN valid_to numeric NOT NULL,
        \\  ADD PERIOD FOR valid_time (valid_from, valid_to),
        \\  ADD CONSTRAINT usage_records_tenant_valid_key UNIQUE (tenant_id, valid_time WITHOUT OVERLAPS);
        ,
    );
    defer add_temporal.deinit(alloc);
    const temporal = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, add_temporal);
    defer runtime_schema.freeSchema(alloc, temporal);
    try std.testing.expectEqual(@as(usize, 7), temporal.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), temporal.periods.len);
    try std.testing.expectEqualStrings("valid_time", temporal.periods[0].name);
    try std.testing.expectEqualStrings("valid_from", temporal.periods[0].start_column);
    try std.testing.expectEqualStrings("valid_to", temporal.periods[0].end_column);
    try std.testing.expectEqual(@as(usize, 2), temporal.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_valid_key", temporal.unique_constraints[1].name);
    try std.testing.expectEqualStrings("valid_time", temporal.unique_constraints[1].without_overlaps_period.?);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.enforced, temporal.unique_constraints[1].validation_state);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, temporal, add_temporal));

    var not_valid = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_not_deleted CHECK (status != 'deleted') NOT VALID;",
    );
    defer not_valid.deinit(alloc);
    const with_unvalidated_check = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, not_valid);
    defer runtime_schema.freeSchema(alloc, with_unvalidated_check);
    try std.testing.expectEqual(@as(usize, 2), with_unvalidated_check.checks.len);
    try std.testing.expectEqualStrings("usage_records_status_not_deleted", with_unvalidated_check.checks[1].name);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, with_unvalidated_check.checks[1].validation_state);

    var validate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_status_not_deleted;",
    );
    defer validate.deinit(alloc);
    const validated = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, with_unvalidated_check, validate);
    defer runtime_schema.freeSchema(alloc, validated);
    try std.testing.expectEqual(@as(usize, 2), validated.checks.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.enforced, validated.checks[1].validation_state);

    var validate_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_pkey;",
    );
    defer validate_default_pk.deinit(alloc);
    const validated_default_pk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, validate_default_pk);
    defer runtime_schema.freeSchema(alloc, validated_default_pk);
    try std.testing.expect(validated_default_pk.primary_key != null);
    try std.testing.expect(validated_default_pk.primary_key.?.name == null);

    var rename_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_pkey TO usage_records_id_pk;",
    );
    defer rename_default_pk.deinit(alloc);
    const renamed_default_pk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_default_pk);
    defer runtime_schema.freeSchema(alloc, renamed_default_pk);
    try std.testing.expectEqualStrings("usage_records_id_pk", renamed_default_pk.primary_key.?.name.?);

    var rename_default_pk_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_pkey TO usage_records_tenant_status_key;",
    );
    defer rename_default_pk_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_default_pk_duplicate));

    var drop_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records DROP CONSTRAINT usage_records_pkey;",
    );
    defer drop_default_pk.deinit(alloc);
    const without_default_pk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_default_pk);
    defer runtime_schema.freeSchema(alloc, without_default_pk);
    try std.testing.expect(without_default_pk.primary_key == null);

    var drop_named_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records DROP CONSTRAINT usage_records_id_pk;",
    );
    defer drop_named_pk.deinit(alloc);
    const without_named_pk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, renamed_default_pk, drop_named_pk);
    defer runtime_schema.freeSchema(alloc, without_named_pk);
    try std.testing.expect(without_named_pk.primary_key == null);

    var drop_check = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT usage_records_status_check;");
    defer drop_check.deinit(alloc);
    const without_check = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_check);
    defer runtime_schema.freeSchema(alloc, without_check);
    try std.testing.expectEqual(@as(usize, 1), without_check.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_check.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), without_check.checks.len);

    var drop_unique = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT usage_records_tenant_status_key;");
    defer drop_unique.deinit(alloc);
    const without_unique = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_unique);
    defer runtime_schema.freeSchema(alloc, without_unique);
    try std.testing.expectEqual(@as(usize, 0), without_unique.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_unique.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), without_unique.checks.len);

    var drop_fk = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT IF EXISTS usage_records_tenant_fkey;");
    defer drop_fk.deinit(alloc);
    const without_fk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_fk);
    defer runtime_schema.freeSchema(alloc, without_fk);
    try std.testing.expectEqual(@as(usize, 1), without_fk.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 0), without_fk.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), without_fk.checks.len);

    var drop_missing_constraint_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT IF EXISTS missing_constraint;");
    defer drop_missing_constraint_if_exists.deinit(alloc);
    const unchanged_constraints = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_missing_constraint_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged_constraints);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.checks.len);

    var drop_missing_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT missing_constraint;");
    defer drop_missing_constraint.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_missing_constraint));

    var set_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET DEFAULT 'pending';");
    defer set_default.deinit(alloc);
    const with_default = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, set_default);
    defer runtime_schema.freeSchema(alloc, with_default);
    const status_defaulted = binder.relationalColumnForField(with_default, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status_defaulted.default_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, status_defaulted.default_value.?.kind);
    try std.testing.expectEqualStrings("\"pending\"", status_defaulted.default_value.?.value_json);

    var set_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET DEFAULT 5;");
    defer set_numeric_default.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, set_numeric_default));

    var add_amount = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN amount numeric;");
    defer add_amount.deinit(alloc);
    const with_amount = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, add_amount);
    defer runtime_schema.freeSchema(alloc, with_amount);

    var set_casted_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN amount SET DEFAULT '7'::numeric;");
    defer set_casted_numeric_default.deinit(alloc);
    const with_casted_numeric_default = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, with_amount, set_casted_numeric_default);
    defer runtime_schema.freeSchema(alloc, with_casted_numeric_default);
    const amount_defaulted = binder.relationalColumnForField(with_casted_numeric_default, "amount", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(amount_defaulted.default_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, amount_defaulted.default_value.?.kind);
    try std.testing.expectEqualStrings("7", amount_defaulted.default_value.?.value_json);

    var set_text_cast_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN amount SET DEFAULT '7'::text;");
    defer set_text_cast_numeric_default.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, with_amount, set_text_cast_numeric_default));

    var drop_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status DROP DEFAULT;");
    defer drop_default.deinit(alloc);
    const without_default = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, with_default, drop_default);
    defer runtime_schema.freeSchema(alloc, without_default);
    const status_without_default = binder.relationalColumnForField(without_default, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status_without_default.default_value == null);

    var set_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET NOT NULL;");
    defer set_not_null.deinit(alloc);
    const status_required = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, set_not_null);
    defer runtime_schema.freeSchema(alloc, status_required);
    const required_status = binder.relationalColumnForField(status_required, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!required_status.nullable);

    var drop_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status DROP NOT NULL;");
    defer drop_not_null.deinit(alloc);
    const status_nullable = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, status_required, drop_not_null);
    defer runtime_schema.freeSchema(alloc, status_nullable);
    const nullable_status = binder.relationalColumnForField(status_nullable, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(nullable_status.nullable);

    var drop_pk_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN id DROP NOT NULL;");
    defer drop_pk_not_null.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_pk_not_null));

    var alter_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE varchar(64);");
    defer alter_type.deinit(alloc);
    const typed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, alter_type);
    defer runtime_schema.freeSchema(alloc, typed);
    const typed_status = binder.relationalColumnForField(typed, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, typed_status.field_type);
    try std.testing.expect(typed_status.array_item_type == null);
    try std.testing.expect(typed_status.collation == null);

    var alter_type_collated = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE text COLLATE \"C\";");
    defer alter_type_collated.deinit(alloc);
    const typed_collated = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, alter_type_collated);
    defer runtime_schema.freeSchema(alloc, typed_collated);
    const collated_status = binder.relationalColumnForField(typed_collated, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, collated_status.field_type);
    try std.testing.expectEqualStrings("C", collated_status.collation.?);

    var alter_collated_to_numeric = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE numeric;");
    defer alter_collated_to_numeric.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, typed_collated, alter_collated_to_numeric));

    var alter_generated_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN tenant_status_key TYPE text;");
    defer alter_generated_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, alter_generated_type));

    var rename_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME COLUMN status TO state;");
    defer rename_status.deinit(alloc);
    const renamed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_status);
    defer runtime_schema.freeSchema(alloc, renamed);
    try std.testing.expect(binder.relationalColumnForField(renamed, "status", null) == null);
    const state = binder.relationalColumnForField(renamed, "state", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("state", state.path);
    const renamed_generated = binder.relationalColumnForField(renamed, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("state", renamed_generated.generated.?.fields[1]);
    try std.testing.expectEqualStrings("state", renamed.unique_constraints[0].columns[1]);
    try std.testing.expectEqualStrings("state", renamed.checks[0].field);

    var rename_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME COLUMN status TO tenant_id;");
    defer rename_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_duplicate));

    var rename_unique_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_tenant_status_key TO usage_records_tenant_state_key;");
    defer rename_unique_constraint.deinit(alloc);
    const renamed_unique = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_unique_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_unique);
    try std.testing.expectEqualStrings("usage_records_tenant_state_key", renamed_unique.unique_constraints[0].name);

    var rename_fk_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_tenant_fkey TO usage_records_tenant_fk;");
    defer rename_fk_constraint.deinit(alloc);
    const renamed_fk = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_fk_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_fk);
    try std.testing.expectEqualStrings("usage_records_tenant_fk", renamed_fk.foreign_keys[0].name);

    var rename_check_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_status_check TO usage_records_state_check;");
    defer rename_check_constraint.deinit(alloc);
    const renamed_check = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_check_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_check);
    try std.testing.expectEqualStrings("usage_records_state_check", renamed_check.checks[0].name);

    var rename_constraint_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_status_check TO usage_records_tenant_status_key;");
    defer rename_constraint_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, rename_constraint_duplicate));

    var drop_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN status;");
    defer drop_status.deinit(alloc);
    const dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_status);
    defer runtime_schema.freeSchema(alloc, dropped);
    try std.testing.expectEqual(@as(usize, 2), dropped.relational_columns.len);
    try std.testing.expect(binder.relationalColumnForField(dropped, "status", null) == null);
    try std.testing.expect(binder.relationalColumnForField(dropped, "tenant_status_key", null) == null);
    try std.testing.expectEqual(@as(usize, 0), dropped.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), dropped.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), dropped.checks.len);

    var drop_missing_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN IF EXISTS missing_column;");
    defer drop_missing_if_exists.deinit(alloc);
    const unchanged = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_missing_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged);
    try std.testing.expectEqual(@as(usize, 5), unchanged.relational_columns.len);

    var drop_status_restrict = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN status RESTRICT;");
    defer drop_status_restrict.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_status_restrict));

    var drop_primary_key = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN id;");
    defer drop_primary_key.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_primary_key));

    var duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN status text;");
    defer duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, duplicate));

    var duplicate_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN IF NOT EXISTS status text REFERENCES tenants(id);");
    defer duplicate_if_not_exists.deinit(alloc);
    const unchanged_existing = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, duplicate_if_not_exists);
    defer runtime_schema.freeSchema(alloc, unchanged_existing);
    try std.testing.expectEqual(@as(usize, 5), unchanged_existing.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_existing.foreign_keys.len);

    var missing_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE IF EXISTS missing_usage ADD COLUMN status text;");
    defer missing_table_if_exists.deinit(alloc);
    const missing_noop = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, missing_table_if_exists);
    defer runtime_schema.freeSchema(alloc, missing_noop);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, missing_noop.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), missing_noop.relational_columns.len);

    var missing_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE missing_usage ADD COLUMN status text;");
    defer missing_table.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, missing_table));
}

test "catalog apply applies create index ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE users (id uuid PRIMARY KEY, tenant_id text NOT NULL, email text, amount numeric, status text, deleted_at timestamptz, metadata jsonb, tags text[]);",
    );
    defer create.deinit(alloc);
    const schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var partial_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_active_idx ON users (status DESC NULLS LAST) WHERE deleted_at IS NULL;",
    );
    defer partial_index.deinit(alloc);
    const indexed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, partial_index);
    defer runtime_schema.freeSchema(alloc, indexed);
    const status = binder.relationalColumnForField(indexed, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, status.index_lifecycle);
    try std.testing.expect(status.index_generation != 0);
    try std.testing.expect(status.index_name != null);
    try std.testing.expectEqualStrings("users_status_active_idx", status.index_name.?);
    try std.testing.expectEqual(@as(usize, 1), status.index_where.len);
    try std.testing.expectEqualStrings("deleted_at", status.index_where[0].field);

    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, indexed, partial_index));

    var partial_index_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX IF NOT EXISTS users_status_active_idx ON users (status) WHERE deleted_at IS NULL;",
    );
    defer partial_index_if_not_exists.deinit(alloc);
    const indexed_noop = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, indexed, partial_index_if_not_exists);
    defer runtime_schema.freeSchema(alloc, indexed_noop);
    const status_noop = binder.relationalColumnForField(indexed_noop, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(status.index_generation, status_noop.index_generation);
    try std.testing.expect(status_noop.index_name != null);
    try std.testing.expectEqualStrings("users_status_active_idx", status_noop.index_name.?);

    const indexed_again = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, partial_index);
    defer runtime_schema.freeSchema(alloc, indexed_again);
    const status_again = binder.relationalColumnForField(indexed_again, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(status.index_generation, status_again.index_generation);

    var covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_email_cover_idx ON users (email) INCLUDE (tenant_id, amount);",
    );
    defer covering_index.deinit(alloc);
    const covering_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, covering_index);
    defer runtime_schema.freeSchema(alloc, covering_schema);
    const covered_email = binder.relationalColumnForField(covering_schema, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(covered_email.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, covered_email.index_lifecycle);
    try std.testing.expect(covered_email.index_generation != 0);
    try std.testing.expect(covered_email.index_name != null);
    try std.testing.expectEqualStrings("users_email_cover_idx", covered_email.index_name.?);
    try std.testing.expectEqual(@as(usize, 2), covered_email.index_include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", covered_email.index_include_columns[0]);
    try std.testing.expectEqualStrings("amount", covered_email.index_include_columns[1]);
    const covered_tenant = binder.relationalColumnForField(covering_schema, "tenant_id", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), covered_tenant.index_include_columns.len);

    var drop_covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_email_cover_idx;");
    defer drop_covering_index.deinit(alloc);
    const covering_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, covering_schema, drop_covering_index);
    defer runtime_schema.freeSchema(alloc, covering_dropped);
    const dropped_email_cover = binder.relationalColumnForField(covering_dropped, "email", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!dropped_email_cover.indexed);
    try std.testing.expect(dropped_email_cover.index_name == null);
    try std.testing.expectEqual(@as(usize, 0), dropped_email_cover.index_include_columns.len);

    var generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_lower_email_idx ON users (lower(email));",
    );
    defer generated_index.deinit(alloc);
    const generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, indexed, generated_index);
    defer runtime_schema.freeSchema(alloc, generated_schema);
    const generated = binder.relationalColumnForField(generated_schema, "users_lower_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, generated.index_lifecycle);
    try std.testing.expect(generated.index_generation != 0);
    try std.testing.expect(generated.index_name != null);
    try std.testing.expectEqualStrings("users_lower_email_idx", generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, generated.generated.?.op);
    try std.testing.expectEqualStrings("email", generated.generated.?.field.?);

    var generated_covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_lower_email_cover_idx ON users (lower(email)) INCLUDE (tenant_id, amount);",
    );
    defer generated_covering_index.deinit(alloc);
    const generated_covering_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, generated_schema, generated_covering_index);
    defer runtime_schema.freeSchema(alloc, generated_covering_schema);
    const generated_covering = binder.relationalColumnForField(generated_covering_schema, "users_lower_email_cover_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated_covering.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, generated_covering.generated.?.op);
    try std.testing.expectEqual(@as(usize, 2), generated_covering.index_include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", generated_covering.index_include_columns[0]);
    try std.testing.expectEqualStrings("amount", generated_covering.index_include_columns[1]);

    var gin_covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_metadata_gin_cover_idx ON users USING gin (metadata jsonb_path_ops) INCLUDE (tenant_id);",
    );
    defer gin_covering_index.deinit(alloc);
    const gin_covering_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, gin_covering_index);
    defer runtime_schema.freeSchema(alloc, gin_covering_schema);
    const gin_covering = binder.relationalColumnForField(gin_covering_schema, "metadata", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(gin_covering.indexed);
    try std.testing.expectEqualStrings("users_metadata_gin_cover_idx", gin_covering.index_name.?);
    try std.testing.expectEqual(@as(usize, 1), gin_covering.index_include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", gin_covering.index_include_columns[0]);

    var wrapped_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_lower_email_wrapped_idx ON users ((lower(email)));",
    );
    defer wrapped_generated_index.deinit(alloc);
    const wrapped_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, generated_schema, wrapped_generated_index);
    defer runtime_schema.freeSchema(alloc, wrapped_generated_schema);
    const wrapped_generated = binder.relationalColumnForField(wrapped_generated_schema, "users_lower_email_wrapped_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(wrapped_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, wrapped_generated.generated.?.op);
    try std.testing.expectEqualStrings("email", wrapped_generated.generated.?.field.?);

    var upper_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_upper_email_idx ON users (upper(email));",
    );
    defer upper_generated_index.deinit(alloc);
    const upper_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, generated_schema, upper_generated_index);
    defer runtime_schema.freeSchema(alloc, upper_generated_schema);
    const upper_generated = binder.relationalColumnForField(upper_generated_schema, "users_upper_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(upper_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, upper_generated.index_lifecycle);
    try std.testing.expect(upper_generated.index_generation != 0);
    try std.testing.expect(upper_generated.index_name != null);
    try std.testing.expectEqualStrings("users_upper_email_idx", upper_generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, upper_generated.generated.?.op);
    try std.testing.expectEqualStrings("email", upper_generated.generated.?.field.?);

    var md5_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_md5_email_idx ON users (md5(email));",
    );
    defer md5_generated_index.deinit(alloc);
    const md5_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_generated_schema, md5_generated_index);
    defer runtime_schema.freeSchema(alloc, md5_generated_schema);
    const md5_generated = binder.relationalColumnForField(md5_generated_schema, "users_md5_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(md5_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, md5_generated.index_lifecycle);
    try std.testing.expect(md5_generated.index_generation != 0);
    try std.testing.expect(md5_generated.index_name != null);
    try std.testing.expectEqualStrings("users_md5_email_idx", md5_generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, md5_generated.generated.?.op);
    try std.testing.expectEqualStrings("email", md5_generated.generated.?.field.?);

    var concat_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_tenant_status_idx ON users (concat(tenant_id, ':', status));",
    );
    defer concat_generated_index.deinit(alloc);
    const concat_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_generated_schema, concat_generated_index);
    defer runtime_schema.freeSchema(alloc, concat_generated_schema);
    const concat_generated = binder.relationalColumnForField(concat_generated_schema, "users_tenant_status_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(concat_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, concat_generated.index_lifecycle);
    try std.testing.expect(concat_generated.index_generation != 0);
    try std.testing.expect(concat_generated.index_name != null);
    try std.testing.expectEqualStrings("users_tenant_status_idx", concat_generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, concat_generated.generated.?.op);
    try std.testing.expectEqual(@as(usize, 2), concat_generated.generated.?.fields.len);
    try std.testing.expectEqualStrings("tenant_id", concat_generated.generated.?.fields[0]);
    try std.testing.expectEqualStrings("status", concat_generated.generated.?.fields[1]);
    try std.testing.expectEqualStrings(":", concat_generated.generated.?.separator);

    var concat_ws_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_tenant_status_ws_idx ON users (concat_ws(':', tenant_id, status));",
    );
    defer concat_ws_generated_index.deinit(alloc);
    const concat_ws_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, concat_generated_schema, concat_ws_generated_index);
    defer runtime_schema.freeSchema(alloc, concat_ws_generated_schema);
    const concat_ws_generated = binder.relationalColumnForField(concat_ws_generated_schema, "users_tenant_status_ws_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(concat_ws_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, concat_ws_generated.index_lifecycle);
    try std.testing.expect(concat_ws_generated.index_generation != 0);
    try std.testing.expect(concat_ws_generated.index_name != null);
    try std.testing.expectEqualStrings("users_tenant_status_ws_idx", concat_ws_generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat_ws, concat_ws_generated.generated.?.op);
    try std.testing.expectEqual(@as(usize, 2), concat_ws_generated.generated.?.fields.len);
    try std.testing.expectEqualStrings("tenant_id", concat_ws_generated.generated.?.fields[0]);
    try std.testing.expectEqualStrings("status", concat_ws_generated.generated.?.fields[1]);
    try std.testing.expectEqualStrings(":", concat_ws_generated.generated.?.separator);

    var rich_expression_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_replace_idx ON users (replace(status, 'old', 'new'));",
    );
    defer rich_expression_generated_index.deinit(alloc);
    const rich_expression_generated_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, concat_ws_generated_schema, rich_expression_generated_index);
    defer runtime_schema.freeSchema(alloc, rich_expression_generated_schema);
    const rich_expression_generated = binder.relationalColumnForField(rich_expression_generated_schema, "users_status_replace_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(rich_expression_generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, rich_expression_generated.index_lifecycle);
    try std.testing.expect(rich_expression_generated.index_generation != 0);
    try std.testing.expect(rich_expression_generated.index_name != null);
    try std.testing.expectEqualStrings("users_status_replace_idx", rich_expression_generated.index_name.?);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.expression, rich_expression_generated.generated.?.op);
    const rich_expression = rich_expression_generated.generated.?.expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.replace, rich_expression.kind);
    try std.testing.expectEqual(@as(usize, 3), rich_expression.operands.len);

    var unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_key ON users (tenant_id, lower(email)) WHERE deleted_at IS NULL;",
    );
    defer unique_index.deinit(alloc);
    const unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, generated_schema, unique_index);
    defer runtime_schema.freeSchema(alloc, unique_schema);
    try std.testing.expectEqual(@as(usize, 1), unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_lower_email_key", unique_schema.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), unique_schema.unique_constraints[0].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, unique_schema.unique_constraints[0].validation_state);

    var unique_covering_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_email_cover_key ON users (email) INCLUDE (tenant_id, status);",
    );
    defer unique_covering_index.deinit(alloc);
    const unique_covering_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, unique_covering_index);
    defer runtime_schema.freeSchema(alloc, unique_covering_schema);
    try std.testing.expectEqual(@as(usize, 2), unique_covering_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_email_cover_key", unique_covering_schema.unique_constraints[1].name);
    try std.testing.expectEqual(@as(usize, 1), unique_covering_schema.unique_constraints[1].columns.len);
    try std.testing.expectEqualStrings("email", unique_covering_schema.unique_constraints[1].columns[0]);
    try std.testing.expectEqual(@as(usize, 2), unique_covering_schema.unique_constraints[1].include_columns.len);
    try std.testing.expectEqualStrings("tenant_id", unique_covering_schema.unique_constraints[1].include_columns[0]);
    try std.testing.expectEqualStrings("status", unique_covering_schema.unique_constraints[1].include_columns[1]);

    var upper_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_upper_email_key ON users (upper(email));",
    );
    defer upper_unique_index.deinit(alloc);
    const upper_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, upper_unique_index);
    defer runtime_schema.freeSchema(alloc, upper_unique_schema);
    try std.testing.expectEqual(@as(usize, 2), upper_unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_upper_email_key", upper_unique_schema.unique_constraints[1].name);
    try std.testing.expectEqual(@as(usize, 1), upper_unique_schema.unique_constraints[1].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.upper, upper_unique_schema.unique_constraints[1].expressions[0].op);
    try std.testing.expectEqualStrings("email", upper_unique_schema.unique_constraints[1].expressions[0].field);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, upper_unique_schema.unique_constraints[1].validation_state);

    var md5_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_md5_email_key ON users (md5(email));",
    );
    defer md5_unique_index.deinit(alloc);
    const md5_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, md5_unique_index);
    defer runtime_schema.freeSchema(alloc, md5_unique_schema);
    try std.testing.expectEqual(@as(usize, 3), md5_unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_md5_email_key", md5_unique_schema.unique_constraints[2].name);
    try std.testing.expectEqual(@as(usize, 1), md5_unique_schema.unique_constraints[2].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.md5, md5_unique_schema.unique_constraints[2].expressions[0].op);
    try std.testing.expectEqualStrings("email", md5_unique_schema.unique_constraints[2].expressions[0].field);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, md5_unique_schema.unique_constraints[2].validation_state);

    var rich_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_status_replace_key ON users (replace(status, 'old', 'new'));",
    );
    defer rich_unique_index.deinit(alloc);
    const rich_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, md5_unique_schema, rich_unique_index);
    defer runtime_schema.freeSchema(alloc, rich_unique_schema);
    try std.testing.expectEqual(@as(usize, 4), rich_unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_status_replace_key", rich_unique_schema.unique_constraints[3].name);
    try std.testing.expectEqual(@as(usize, 1), rich_unique_schema.unique_constraints[3].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.expression, rich_unique_schema.unique_constraints[3].expressions[0].op);
    const rich_unique_expression = rich_unique_schema.unique_constraints[3].expressions[0].expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.replace, rich_unique_expression.kind);
    try std.testing.expectEqual(@as(usize, 3), rich_unique_expression.operands.len);
    try std.testing.expectEqualStrings("status", rich_unique_expression.operands[0].field);
    try std.testing.expectEqualStrings("\"old\"", rich_unique_expression.operands[1].value_json);
    try std.testing.expectEqualStrings("\"new\"", rich_unique_expression.operands[2].value_json);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, rich_unique_schema.unique_constraints[3].validation_state);

    var temporal_create = try lowerDdlPlanForCatalogApplyTestAlloc(alloc,
        \\CREATE TABLE prices (
        \\  id uuid PRIMARY KEY,
        \\  sku text NOT NULL,
        \\  valid_from numeric NOT NULL,
        \\  valid_to numeric NOT NULL,
        \\  price numeric,
        \\  PERIOD FOR valid_time (valid_from, valid_to)
        \\);
    );
    defer temporal_create.deinit(alloc);
    const temporal_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, temporal_create);
    defer runtime_schema.freeSchema(alloc, temporal_schema);

    var temporal_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX prices_sku_valid_time_key ON prices (sku, valid_time WITHOUT OVERLAPS);",
    );
    defer temporal_unique_index.deinit(alloc);
    const temporal_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, temporal_schema, temporal_unique_index);
    defer runtime_schema.freeSchema(alloc, temporal_unique_schema);
    try std.testing.expectEqual(@as(usize, 1), temporal_unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("prices_sku_valid_time_key", temporal_unique_schema.unique_constraints[0].name);
    try std.testing.expectEqualStrings("sku", temporal_unique_schema.unique_constraints[0].columns[0]);
    try std.testing.expectEqualStrings("valid_time", temporal_unique_schema.unique_constraints[0].without_overlaps_period.?);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, temporal_unique_schema.unique_constraints[0].validation_state);

    var wrapped_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_wrapped_lower_email_key ON users (tenant_id, (lower(email))) WHERE deleted_at IS NULL;",
    );
    defer wrapped_unique_index.deinit(alloc);
    const wrapped_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, md5_unique_schema, wrapped_unique_index);
    defer runtime_schema.freeSchema(alloc, wrapped_unique_schema);
    try std.testing.expectEqual(@as(usize, 4), wrapped_unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_wrapped_lower_email_key", wrapped_unique_schema.unique_constraints[3].name);
    try std.testing.expectEqual(@as(usize, 1), wrapped_unique_schema.unique_constraints[3].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, wrapped_unique_schema.unique_constraints[3].expressions[0].op);
    try std.testing.expectEqualStrings("email", wrapped_unique_schema.unique_constraints[3].expressions[0].field);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, wrapped_unique_schema.unique_constraints[3].validation_state);

    var expression_where_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_active_expr_key ON users (tenant_id, lower(email)) WHERE lower(status) = 'active';",
    );
    defer expression_where_unique_index.deinit(alloc);
    const expression_where_unique_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, wrapped_unique_schema, expression_where_unique_index);
    defer runtime_schema.freeSchema(alloc, expression_where_unique_schema);
    try std.testing.expectEqual(@as(usize, 5), expression_where_unique_schema.unique_constraints.len);
    const expression_where_unique = expression_where_unique_schema.unique_constraints[4];
    try std.testing.expectEqualStrings("users_tenant_lower_email_active_expr_key", expression_where_unique.name);
    try std.testing.expectEqual(@as(usize, 1), expression_where_unique.expressions.len);
    try std.testing.expectEqual(@as(usize, 0), expression_where_unique.where.len);
    try std.testing.expectEqual(@as(usize, 1), expression_where_unique.where_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, expression_where_unique.where_expressions[0].lhs.kind);
    try std.testing.expectEqualStrings("status", expression_where_unique.where_expressions[0].lhs.operands[0].field);
    try std.testing.expectEqualStrings("\"active\"", expression_where_unique.where_expressions[0].rhs[0].value_json);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, expression_where_unique.validation_state);

    var gin_json_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_metadata_gin ON users USING gin (metadata jsonb_path_ops);",
    );
    defer gin_json_index.deinit(alloc);
    const json_indexed_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, gin_json_index);
    defer runtime_schema.freeSchema(alloc, json_indexed_schema);
    const metadata = binder.relationalColumnForField(json_indexed_schema, "metadata", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(metadata.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, metadata.index_lifecycle);
    try std.testing.expect(metadata.index_generation != 0);
    try std.testing.expect(metadata.index_name != null);
    try std.testing.expectEqualStrings("users_metadata_gin", metadata.index_name.?);

    var gin_array_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_tags_gin ON users USING gin (tags array_ops);",
    );
    defer gin_array_index.deinit(alloc);
    const array_indexed_schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, json_indexed_schema, gin_array_index);
    defer runtime_schema.freeSchema(alloc, array_indexed_schema);
    const tags = binder.relationalColumnForField(array_indexed_schema, "tags", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(tags.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, tags.index_lifecycle);
    try std.testing.expect(tags.index_generation != 0);
    try std.testing.expect(tags.index_name != null);
    try std.testing.expectEqualStrings("users_tags_gin", tags.index_name.?);

    var invalid_gin_scalar = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_gin ON users USING gin (status);",
    );
    defer invalid_gin_scalar.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, invalid_gin_scalar));

    var invalid_gin_json_opclass = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_metadata_bad_gin ON users USING gin (metadata array_ops);",
    );
    defer invalid_gin_json_opclass.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, invalid_gin_json_opclass));

    var invalid_gin_array_opclass = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_tags_bad_gin ON users USING gin (tags jsonb_path_ops);",
    );
    defer invalid_gin_array_opclass.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, invalid_gin_array_opclass));

    var drop_ordinary_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_status_active_idx;");
    defer drop_ordinary_index.deinit(alloc);
    const ordinary_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, upper_unique_schema, drop_ordinary_index);
    defer runtime_schema.freeSchema(alloc, ordinary_dropped);
    const dropped_status = binder.relationalColumnForField(ordinary_dropped, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!dropped_status.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.ready, dropped_status.index_lifecycle);
    try std.testing.expectEqual(@as(u64, 0), dropped_status.index_generation);
    try std.testing.expect(dropped_status.index_name == null);
    try std.testing.expectEqual(@as(usize, 0), dropped_status.index_where.len);

    var concurrent_status_index = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE INDEX users_status_concurrent_idx ON users (status);",
    );
    defer concurrent_status_index.deinit(alloc);
    const concurrent_indexed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, ordinary_dropped, concurrent_status_index);
    defer runtime_schema.freeSchema(alloc, concurrent_indexed);
    var drop_concurrent_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX CONCURRENTLY users_status_concurrent_idx;");
    defer drop_concurrent_index.deinit(alloc);
    const concurrent_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, concurrent_indexed, drop_concurrent_index);
    defer runtime_schema.freeSchema(alloc, concurrent_dropped);
    const concurrent_dropped_status = binder.relationalColumnForField(concurrent_dropped, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!concurrent_dropped_status.indexed);
    try std.testing.expect(concurrent_dropped_status.index_name == null);

    var drop_generated_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_lower_email_idx;");
    defer drop_generated_index.deinit(alloc);
    const generated_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, drop_generated_index);
    defer runtime_schema.freeSchema(alloc, generated_dropped);
    try std.testing.expect(binder.relationalColumnForField(generated_dropped, "users_lower_email_idx", null) == null);

    var drop_unique_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_tenant_lower_email_key;");
    defer drop_unique_index.deinit(alloc);
    const unique_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, drop_unique_index);
    defer runtime_schema.freeSchema(alloc, unique_dropped);
    try std.testing.expectEqual(@as(usize, 0), unique_dropped.unique_constraints.len);

    var drop_missing_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX missing_idx;");
    defer drop_missing_index.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, drop_missing_index));

    var drop_missing_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX IF EXISTS missing_idx;");
    defer drop_missing_if_exists.deinit(alloc);
    const unchanged_drop = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, unique_schema, drop_missing_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged_drop);
    try std.testing.expectEqual(@as(usize, unique_schema.relational_columns.len), unchanged_drop.relational_columns.len);
    try std.testing.expectEqual(@as(usize, unique_schema.unique_constraints.len), unchanged_drop.unique_constraints.len);

    const empty_drop_noop = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, drop_missing_if_exists);
    defer runtime_schema.freeSchema(alloc, empty_drop_noop);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, empty_drop_noop.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), empty_drop_noop.relational_columns.len);

    var multi_column_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE INDEX users_tenant_status_idx ON users (tenant_id, status);");
    defer multi_column_index.deinit(alloc);
    const multi_indexed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, multi_column_index);
    defer runtime_schema.freeSchema(alloc, multi_indexed);
    const indexed_tenant = binder.relationalColumnForField(multi_indexed, "tenant_id", null) orelse return error.TestUnexpectedResult;
    const indexed_status = binder.relationalColumnForField(multi_indexed, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(indexed_tenant.indexed);
    try std.testing.expect(indexed_status.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, indexed_tenant.index_lifecycle);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, indexed_status.index_lifecycle);
    try std.testing.expectEqual(indexed_tenant.index_generation, indexed_status.index_generation);
    try std.testing.expect(indexed_tenant.index_generation != 0);
    try std.testing.expect(indexed_tenant.index_name != null);
    try std.testing.expect(indexed_status.index_name != null);
    try std.testing.expectEqualStrings("users_tenant_status_idx", indexed_tenant.index_name.?);
    try std.testing.expectEqualStrings("users_tenant_status_idx", indexed_status.index_name.?);

    var drop_multi_column_index = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP INDEX users_tenant_status_idx;");
    defer drop_multi_column_index.deinit(alloc);
    const multi_dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, multi_indexed, drop_multi_column_index);
    defer runtime_schema.freeSchema(alloc, multi_dropped);
    const dropped_tenant = binder.relationalColumnForField(multi_dropped, "tenant_id", null) orelse return error.TestUnexpectedResult;
    const dropped_multi_status = binder.relationalColumnForField(multi_dropped, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!dropped_tenant.indexed);
    try std.testing.expect(!dropped_multi_status.indexed);
    try std.testing.expect(dropped_tenant.index_name == null);
    try std.testing.expect(dropped_multi_status.index_name == null);
}

test "catalog apply applies updated-at trigger ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, updated_at_ns bigint, CONSTRAINT usage_records_updated_check CHECK (updated_at_ns >= 0));",
    );
    defer create.deinit(alloc);
    const schema = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var table_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON TABLE usage_records IS 'metered usage rows';");
    defer table_comment.deinit(alloc);
    const table_commented = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, table_comment);
    defer runtime_schema.freeSchema(alloc, table_commented);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, table_commented.storage_mode);
    try std.testing.expectEqual(schema.relational_columns.len, table_commented.relational_columns.len);

    var column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.updated_at_ns IS 'update clock';");
    defer column_comment.deinit(alloc);
    const column_commented = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, column_comment);
    defer runtime_schema.freeSchema(alloc, column_commented);
    try std.testing.expect(binder.relationalColumnForField(column_commented, "updated_at_ns", null) != null);

    var clear_column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.updated_at_ns IS NULL;");
    defer clear_column_comment.deinit(alloc);
    const column_cleared = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, clear_column_comment);
    defer runtime_schema.freeSchema(alloc, column_cleared);
    try std.testing.expect(binder.relationalColumnForField(column_cleared, "updated_at_ns", null) != null);

    var missing_column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.missing IS 'missing';");
    defer missing_column_comment.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, missing_column_comment));

    var index_plan = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE INDEX usage_records_updated_idx ON usage_records (updated_at_ns);");
    defer index_plan.deinit(alloc);
    const indexed = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, index_plan);
    defer runtime_schema.freeSchema(alloc, indexed);

    var index_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON INDEX usage_records_updated_idx IS 'updated-at lookup';");
    defer index_comment.deinit(alloc);
    const index_commented = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, indexed, index_comment);
    defer runtime_schema.freeSchema(alloc, index_commented);
    try std.testing.expect(binder.relationalIndexNameExists(index_commented, "usage_records_updated_idx"));

    var constraint_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON CONSTRAINT usage_records_updated_check ON usage_records IS 'valid update clock';");
    defer constraint_comment.deinit(alloc);
    const constraint_commented = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, constraint_comment);
    defer runtime_schema.freeSchema(alloc, constraint_commented);
    try std.testing.expect(binder.relationalConstraintNameExists(constraint_commented, "usage_records", "usage_records_updated_check"));

    var missing_constraint_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON CONSTRAINT missing_check ON usage_records IS 'missing';");
    defer missing_constraint_comment.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, missing_constraint_comment));

    var trigger = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(alloc);
    const updated = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, trigger);
    defer runtime_schema.freeSchema(alloc, updated);

    const column = binder.relationalColumnForField(updated, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(column.on_update_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, column.on_update_value.?.kind);

    var drop_trigger = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "DROP TRIGGER update_timestamp ON usage_records;",
    );
    defer drop_trigger.deinit(alloc);
    const dropped = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, updated, drop_trigger);
    defer runtime_schema.freeSchema(alloc, dropped);
    const dropped_column = binder.relationalColumnForField(dropped, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(dropped_column.on_update_value == null);

    var drop_trigger_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "DROP TRIGGER IF EXISTS update_timestamp ON usage_records;",
    );
    defer drop_trigger_if_exists.deinit(alloc);
    const unchanged = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, dropped, drop_trigger_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged);
    const unchanged_column = binder.relationalColumnForField(unchanged, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(unchanged_column.on_update_value == null);

    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, dropped, drop_trigger));

    var drop_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE usage_records;");
    defer drop_table.deinit(alloc);
    const dropped_table = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, schema, drop_table);
    defer runtime_schema.freeSchema(alloc, dropped_table);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, dropped_table.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), dropped_table.relational_columns.len);
    try std.testing.expect(dropped_table.primary_key == null);
    try std.testing.expectError(error.InvalidSqlCatalog, applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, drop_table));

    var drop_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE IF EXISTS usage_records;");
    defer drop_table_if_exists.deinit(alloc);
    const missing_table_noop = try applyLoweredDdlPlanToRuntimeSchemaForTestAlloc(alloc, .{}, drop_table_if_exists);
    defer runtime_schema.freeSchema(alloc, missing_table_noop);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, missing_table_noop.storage_mode);
}

test "catalog apply emits typed row rewrite plan for column rename" {
    const alloc = std.testing.allocator;
    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    var applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, schema_v1, .{ .alter_table = .{
        .table_name = "events",
        .operations = &.{.{ .rename_column = .{ .old_name = "status", .new_name = "state" } }},
    } });
    defer applied.deinit(alloc);

    try std.testing.expect(applied.rewrite_required);
    try std.testing.expectEqual(@as(usize, 3), applied.work_items.len);
    const rewrite = applied.work_items[2];
    try std.testing.expectEqual(ddl_plan.AppliedDdlWorkAction.rewrite, rewrite.action);
    try std.testing.expect(!rewrite.full_row_rewrite);
    try std.testing.expect(rewrite.rewrite_expression == null);
    try std.testing.expectEqual(@as(usize, 1), rewrite.row_rewrite_plan.renames.len);
    try std.testing.expectEqualStrings("status", rewrite.row_rewrite_plan.renames[0].old_path);
    try std.testing.expectEqualStrings("state", rewrite.row_rewrite_plan.renames[0].new_path);
    try std.testing.expectEqual(@as(usize, 0), rewrite.row_rewrite_plan.drops.len);
}

test "catalog apply emits typed row rewrite plan for column drop" {
    const alloc = std.testing.allocator;
    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"legacy_status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    var applied = try applyLoweredDdlPlanToSchemaJsonForTestAlloc(alloc, schema_v1, .{ .alter_table = .{
        .table_name = "events",
        .operations = &.{.{ .drop_column = .{ .name = "legacy_status" } }},
    } });
    defer applied.deinit(alloc);

    try std.testing.expect(applied.rewrite_required);
    try std.testing.expectEqual(@as(usize, 3), applied.work_items.len);
    const rewrite = applied.work_items[2];
    try std.testing.expectEqual(ddl_plan.AppliedDdlWorkAction.rewrite, rewrite.action);
    try std.testing.expect(!rewrite.full_row_rewrite);
    try std.testing.expect(rewrite.rewrite_expression == null);
    try std.testing.expectEqual(@as(usize, 0), rewrite.row_rewrite_plan.renames.len);
    try std.testing.expectEqual(@as(usize, 1), rewrite.row_rewrite_plan.drops.len);
    try std.testing.expectEqualStrings("legacy_status", rewrite.row_rewrite_plan.drops[0]);
}

test "catalog apply marks generic row rewrite work explicitly full" {
    const alloc = std.testing.allocator;
    const work_items = try appliedDdlTableWorkItemsForFlagsAlloc(alloc, true, true, true);
    defer {
        for (work_items) |*item| @constCast(item).deinit(alloc);
        alloc.free(work_items);
    }

    try std.testing.expectEqual(@as(usize, 3), work_items.len);
    const rewrite = work_items[2];
    try std.testing.expectEqual(ddl_plan.AppliedDdlWorkAction.rewrite, rewrite.action);
    try std.testing.expect(rewrite.full_row_rewrite);
    try std.testing.expect(rewrite.rewrite_expression == null);
    try std.testing.expect(rewrite.row_rewrite_plan.empty());
}
