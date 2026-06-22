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
const catalog_resources = @import("../catalog_resources.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("ddl_plan.zig");
const lower_expr = @import("lower_expr.zig");
const parser_context = @import("parser_context.zig");
const runtime_schema = @import("../../storage/schema.zig");
const schema_json = @import("schema_json.zig");
const schema_mutation = @import("schema_mutation.zig");
const tokenized = @import("tokenized.zig");
const transactions_mod = @import("../../storage/transactions.zig");
const usermgr = @import("../../usermgr/mod.zig");

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
            if (set.local) {
                if (session.transaction_local_search_path_base) |base| {
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
        .reset_search_path, .discard_all => {
            return try OwnedSqlCatalogSession.fromSessionAlloc(alloc, .{
                .current_database_name = session.session().currentDatabase(),
                .search_path = &.{catalog_resources.default_namespace_name},
            });
        },
        .show_search_path => {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
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

pub fn applyDdlPlanToRuntimeSchemaAlloc(
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

pub fn applyDdlPlanToSchemaJsonAlloc(
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

    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, lowered);
    defer runtime_schema.freeSchema(alloc, schema);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, lowered));

    var idempotent = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE IF NOT EXISTS usage_records (id uuid PRIMARY KEY);",
    );
    defer idempotent.deinit(alloc);
    const unchanged = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, idempotent);
    defer runtime_schema.freeSchema(alloc, unchanged);
    try std.testing.expectEqual(@as(usize, schema.relational_columns.len), unchanged.relational_columns.len);
    try std.testing.expectEqualStrings("id", unchanged.primary_key.?.columns[0]);

    const tenant_id = binder.relationalColumnForField(schema, "tenant_id", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("C", tenant_id.collation.?);

    var table_clone = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE TABLE usage_records_copy (LIKE usage_records INCLUDING ALL);");
    defer table_clone.deinit(alloc);
    const cloned = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, table_clone);
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
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, table_clone_without_constraints));
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, table_clone));

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
    const primary_keyed = try applyDdlPlanToRuntimeSchemaAlloc(alloc, no_pk_schema, add_primary_key);
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
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, primary_keyed, add_primary_key));

    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, tenant_id text NOT NULL, status text);",
    );
    defer create.deinit(alloc);
    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, create);
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

    const updated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, alter);
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
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, updated.unique_constraints[0].validation_state);
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
    const temporal = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, add_temporal);
    defer runtime_schema.freeSchema(alloc, temporal);
    try std.testing.expectEqual(@as(usize, 7), temporal.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), temporal.periods.len);
    try std.testing.expectEqualStrings("valid_time", temporal.periods[0].name);
    try std.testing.expectEqualStrings("valid_from", temporal.periods[0].start_column);
    try std.testing.expectEqualStrings("valid_to", temporal.periods[0].end_column);
    try std.testing.expectEqual(@as(usize, 2), temporal.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_valid_key", temporal.unique_constraints[1].name);
    try std.testing.expectEqualStrings("valid_time", temporal.unique_constraints[1].without_overlaps_period.?);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, temporal.unique_constraints[1].validation_state);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, temporal, add_temporal));

    var not_valid = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_not_deleted CHECK (status != 'deleted') NOT VALID;",
    );
    defer not_valid.deinit(alloc);
    const with_unvalidated_check = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, not_valid);
    defer runtime_schema.freeSchema(alloc, with_unvalidated_check);
    try std.testing.expectEqual(@as(usize, 2), with_unvalidated_check.checks.len);
    try std.testing.expectEqualStrings("usage_records_status_not_deleted", with_unvalidated_check.checks[1].name);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, with_unvalidated_check.checks[1].validation_state);

    var validate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_status_not_deleted;",
    );
    defer validate.deinit(alloc);
    const validated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, with_unvalidated_check, validate);
    defer runtime_schema.freeSchema(alloc, validated);
    try std.testing.expectEqual(@as(usize, 2), validated.checks.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.enforced, validated.checks[1].validation_state);

    var validate_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_pkey;",
    );
    defer validate_default_pk.deinit(alloc);
    const validated_default_pk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, validate_default_pk);
    defer runtime_schema.freeSchema(alloc, validated_default_pk);
    try std.testing.expect(validated_default_pk.primary_key != null);
    try std.testing.expect(validated_default_pk.primary_key.?.name == null);

    var rename_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_pkey TO usage_records_id_pk;",
    );
    defer rename_default_pk.deinit(alloc);
    const renamed_default_pk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_default_pk);
    defer runtime_schema.freeSchema(alloc, renamed_default_pk);
    try std.testing.expectEqualStrings("usage_records_id_pk", renamed_default_pk.primary_key.?.name.?);

    var rename_default_pk_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_pkey TO usage_records_tenant_status_key;",
    );
    defer rename_default_pk_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_default_pk_duplicate));

    var drop_default_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records DROP CONSTRAINT usage_records_pkey;",
    );
    defer drop_default_pk.deinit(alloc);
    const without_default_pk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_default_pk);
    defer runtime_schema.freeSchema(alloc, without_default_pk);
    try std.testing.expect(without_default_pk.primary_key == null);

    var drop_named_pk = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "ALTER TABLE usage_records DROP CONSTRAINT usage_records_id_pk;",
    );
    defer drop_named_pk.deinit(alloc);
    const without_named_pk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, renamed_default_pk, drop_named_pk);
    defer runtime_schema.freeSchema(alloc, without_named_pk);
    try std.testing.expect(without_named_pk.primary_key == null);

    var drop_check = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT usage_records_status_check;");
    defer drop_check.deinit(alloc);
    const without_check = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_check);
    defer runtime_schema.freeSchema(alloc, without_check);
    try std.testing.expectEqual(@as(usize, 1), without_check.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_check.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), without_check.checks.len);

    var drop_unique = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT usage_records_tenant_status_key;");
    defer drop_unique.deinit(alloc);
    const without_unique = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_unique);
    defer runtime_schema.freeSchema(alloc, without_unique);
    try std.testing.expectEqual(@as(usize, 0), without_unique.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), without_unique.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), without_unique.checks.len);

    var drop_fk = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT IF EXISTS usage_records_tenant_fkey;");
    defer drop_fk.deinit(alloc);
    const without_fk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_fk);
    defer runtime_schema.freeSchema(alloc, without_fk);
    try std.testing.expectEqual(@as(usize, 1), without_fk.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 0), without_fk.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), without_fk.checks.len);

    var drop_missing_constraint_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT IF EXISTS missing_constraint;");
    defer drop_missing_constraint_if_exists.deinit(alloc);
    const unchanged_constraints = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_missing_constraint_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged_constraints);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_constraints.checks.len);

    var drop_missing_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP CONSTRAINT missing_constraint;");
    defer drop_missing_constraint.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_missing_constraint));

    var set_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET DEFAULT 'pending';");
    defer set_default.deinit(alloc);
    const with_default = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, set_default);
    defer runtime_schema.freeSchema(alloc, with_default);
    const status_defaulted = binder.relationalColumnForField(with_default, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status_defaulted.default_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, status_defaulted.default_value.?.kind);
    try std.testing.expectEqualStrings("\"pending\"", status_defaulted.default_value.?.value_json);

    var set_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET DEFAULT 5;");
    defer set_numeric_default.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, set_numeric_default));

    var add_amount = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN amount numeric;");
    defer add_amount.deinit(alloc);
    const with_amount = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, add_amount);
    defer runtime_schema.freeSchema(alloc, with_amount);

    var set_casted_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN amount SET DEFAULT '7'::numeric;");
    defer set_casted_numeric_default.deinit(alloc);
    const with_casted_numeric_default = try applyDdlPlanToRuntimeSchemaAlloc(alloc, with_amount, set_casted_numeric_default);
    defer runtime_schema.freeSchema(alloc, with_casted_numeric_default);
    const amount_defaulted = binder.relationalColumnForField(with_casted_numeric_default, "amount", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(amount_defaulted.default_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, amount_defaulted.default_value.?.kind);
    try std.testing.expectEqualStrings("7", amount_defaulted.default_value.?.value_json);

    var set_text_cast_numeric_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN amount SET DEFAULT '7'::text;");
    defer set_text_cast_numeric_default.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, with_amount, set_text_cast_numeric_default));

    var drop_default = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status DROP DEFAULT;");
    defer drop_default.deinit(alloc);
    const without_default = try applyDdlPlanToRuntimeSchemaAlloc(alloc, with_default, drop_default);
    defer runtime_schema.freeSchema(alloc, without_default);
    const status_without_default = binder.relationalColumnForField(without_default, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status_without_default.default_value == null);

    var set_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status SET NOT NULL;");
    defer set_not_null.deinit(alloc);
    const status_required = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, set_not_null);
    defer runtime_schema.freeSchema(alloc, status_required);
    const required_status = binder.relationalColumnForField(status_required, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!required_status.nullable);

    var drop_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status DROP NOT NULL;");
    defer drop_not_null.deinit(alloc);
    const status_nullable = try applyDdlPlanToRuntimeSchemaAlloc(alloc, status_required, drop_not_null);
    defer runtime_schema.freeSchema(alloc, status_nullable);
    const nullable_status = binder.relationalColumnForField(status_nullable, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(nullable_status.nullable);

    var drop_pk_not_null = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN id DROP NOT NULL;");
    defer drop_pk_not_null.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_pk_not_null));

    var alter_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE varchar(64);");
    defer alter_type.deinit(alloc);
    const typed = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, alter_type);
    defer runtime_schema.freeSchema(alloc, typed);
    const typed_status = binder.relationalColumnForField(typed, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, typed_status.field_type);
    try std.testing.expect(typed_status.array_item_type == null);
    try std.testing.expect(typed_status.collation == null);

    var alter_type_collated = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE text COLLATE \"C\";");
    defer alter_type_collated.deinit(alloc);
    const typed_collated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, alter_type_collated);
    defer runtime_schema.freeSchema(alloc, typed_collated);
    const collated_status = binder.relationalColumnForField(typed_collated, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, collated_status.field_type);
    try std.testing.expectEqualStrings("C", collated_status.collation.?);

    var alter_collated_to_numeric = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN status TYPE numeric;");
    defer alter_collated_to_numeric.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, typed_collated, alter_collated_to_numeric));

    var alter_generated_type = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ALTER COLUMN tenant_status_key TYPE text;");
    defer alter_generated_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, alter_generated_type));

    var rename_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME COLUMN status TO state;");
    defer rename_status.deinit(alloc);
    const renamed = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_status);
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
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_duplicate));

    var rename_unique_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_tenant_status_key TO usage_records_tenant_state_key;");
    defer rename_unique_constraint.deinit(alloc);
    const renamed_unique = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_unique_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_unique);
    try std.testing.expectEqualStrings("usage_records_tenant_state_key", renamed_unique.unique_constraints[0].name);

    var rename_fk_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_tenant_fkey TO usage_records_tenant_fk;");
    defer rename_fk_constraint.deinit(alloc);
    const renamed_fk = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_fk_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_fk);
    try std.testing.expectEqualStrings("usage_records_tenant_fk", renamed_fk.foreign_keys[0].name);

    var rename_check_constraint = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_status_check TO usage_records_state_check;");
    defer rename_check_constraint.deinit(alloc);
    const renamed_check = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_check_constraint);
    defer runtime_schema.freeSchema(alloc, renamed_check);
    try std.testing.expectEqualStrings("usage_records_state_check", renamed_check.checks[0].name);

    var rename_constraint_duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records RENAME CONSTRAINT usage_records_status_check TO usage_records_tenant_status_key;");
    defer rename_constraint_duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, rename_constraint_duplicate));

    var drop_status = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN status;");
    defer drop_status.deinit(alloc);
    const dropped = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_status);
    defer runtime_schema.freeSchema(alloc, dropped);
    try std.testing.expectEqual(@as(usize, 2), dropped.relational_columns.len);
    try std.testing.expect(binder.relationalColumnForField(dropped, "status", null) == null);
    try std.testing.expect(binder.relationalColumnForField(dropped, "tenant_status_key", null) == null);
    try std.testing.expectEqual(@as(usize, 0), dropped.unique_constraints.len);
    try std.testing.expectEqual(@as(usize, 1), dropped.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 0), dropped.checks.len);

    var drop_missing_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN IF EXISTS missing_column;");
    defer drop_missing_if_exists.deinit(alloc);
    const unchanged = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_missing_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged);
    try std.testing.expectEqual(@as(usize, 5), unchanged.relational_columns.len);

    var drop_status_restrict = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN status RESTRICT;");
    defer drop_status_restrict.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_status_restrict));

    var drop_primary_key = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records DROP COLUMN id;");
    defer drop_primary_key.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_primary_key));

    var duplicate = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN status text;");
    defer duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, duplicate));

    var duplicate_if_not_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN IF NOT EXISTS status text REFERENCES tenants(id);");
    defer duplicate_if_not_exists.deinit(alloc);
    const unchanged_existing = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, duplicate_if_not_exists);
    defer runtime_schema.freeSchema(alloc, unchanged_existing);
    try std.testing.expectEqual(@as(usize, 5), unchanged_existing.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), unchanged_existing.foreign_keys.len);

    var missing_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE IF EXISTS missing_usage ADD COLUMN status text;");
    defer missing_table_if_exists.deinit(alloc);
    const missing_noop = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, missing_table_if_exists);
    defer runtime_schema.freeSchema(alloc, missing_noop);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, missing_noop.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), missing_noop.relational_columns.len);

    var missing_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "ALTER TABLE missing_usage ADD COLUMN status text;");
    defer missing_table.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, missing_table));
}

test "catalog apply applies updated-at trigger ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, updated_at_ns bigint, CONSTRAINT usage_records_updated_check CHECK (updated_at_ns >= 0));",
    );
    defer create.deinit(alloc);
    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var table_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON TABLE usage_records IS 'metered usage rows';");
    defer table_comment.deinit(alloc);
    const table_commented = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, table_comment);
    defer runtime_schema.freeSchema(alloc, table_commented);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, table_commented.storage_mode);
    try std.testing.expectEqual(schema.relational_columns.len, table_commented.relational_columns.len);

    var column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.updated_at_ns IS 'update clock';");
    defer column_comment.deinit(alloc);
    const column_commented = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, column_comment);
    defer runtime_schema.freeSchema(alloc, column_commented);
    try std.testing.expect(binder.relationalColumnForField(column_commented, "updated_at_ns", null) != null);

    var clear_column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.updated_at_ns IS NULL;");
    defer clear_column_comment.deinit(alloc);
    const column_cleared = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, clear_column_comment);
    defer runtime_schema.freeSchema(alloc, column_cleared);
    try std.testing.expect(binder.relationalColumnForField(column_cleared, "updated_at_ns", null) != null);

    var missing_column_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON COLUMN usage_records.missing IS 'missing';");
    defer missing_column_comment.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, missing_column_comment));

    var index_plan = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "CREATE INDEX usage_records_updated_idx ON usage_records (updated_at_ns);");
    defer index_plan.deinit(alloc);
    const indexed = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, index_plan);
    defer runtime_schema.freeSchema(alloc, indexed);

    var index_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON INDEX usage_records_updated_idx IS 'updated-at lookup';");
    defer index_comment.deinit(alloc);
    const index_commented = try applyDdlPlanToRuntimeSchemaAlloc(alloc, indexed, index_comment);
    defer runtime_schema.freeSchema(alloc, index_commented);
    try std.testing.expect(binder.relationalIndexNameExists(index_commented, "usage_records_updated_idx"));

    var constraint_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON CONSTRAINT usage_records_updated_check ON usage_records IS 'valid update clock';");
    defer constraint_comment.deinit(alloc);
    const constraint_commented = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, constraint_comment);
    defer runtime_schema.freeSchema(alloc, constraint_commented);
    try std.testing.expect(binder.relationalConstraintNameExists(constraint_commented, "usage_records", "usage_records_updated_check"));

    var missing_constraint_comment = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "COMMENT ON CONSTRAINT missing_check ON usage_records IS 'missing';");
    defer missing_constraint_comment.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, missing_constraint_comment));

    var trigger = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(alloc);
    const updated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, trigger);
    defer runtime_schema.freeSchema(alloc, updated);

    const column = binder.relationalColumnForField(updated, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(column.on_update_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, column.on_update_value.?.kind);

    var drop_trigger = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "DROP TRIGGER update_timestamp ON usage_records;",
    );
    defer drop_trigger.deinit(alloc);
    const dropped = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, drop_trigger);
    defer runtime_schema.freeSchema(alloc, dropped);
    const dropped_column = binder.relationalColumnForField(dropped, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(dropped_column.on_update_value == null);

    var drop_trigger_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(
        alloc,
        "DROP TRIGGER IF EXISTS update_timestamp ON usage_records;",
    );
    defer drop_trigger_if_exists.deinit(alloc);
    const unchanged = try applyDdlPlanToRuntimeSchemaAlloc(alloc, dropped, drop_trigger_if_exists);
    defer runtime_schema.freeSchema(alloc, unchanged);
    const unchanged_column = binder.relationalColumnForField(unchanged, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(unchanged_column.on_update_value == null);

    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, dropped, drop_trigger));

    var drop_table = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE usage_records;");
    defer drop_table.deinit(alloc);
    const dropped_table = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, drop_table);
    defer runtime_schema.freeSchema(alloc, dropped_table);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, dropped_table.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), dropped_table.relational_columns.len);
    try std.testing.expect(dropped_table.primary_key == null);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, drop_table));

    var drop_table_if_exists = try lowerDdlPlanForCatalogApplyTestAlloc(alloc, "DROP TABLE IF EXISTS usage_records;");
    defer drop_table_if_exists.deinit(alloc);
    const missing_table_noop = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, drop_table_if_exists);
    defer runtime_schema.freeSchema(alloc, missing_table_noop);
    try std.testing.expectEqual(runtime_schema.StorageMode.document, missing_table_noop.storage_mode);
}

test "catalog apply emits typed row rewrite plan for column rename" {
    const alloc = std.testing.allocator;
    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    var applied = try applyDdlPlanToSchemaJsonAlloc(alloc, schema_v1, .{ .alter_table = .{
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
    var applied = try applyDdlPlanToSchemaJsonAlloc(alloc, schema_v1, .{ .alter_table = .{
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
