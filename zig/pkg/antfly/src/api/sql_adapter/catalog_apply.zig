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
const runtime_schema = @import("../../storage/schema.zig");
const schema_json = @import("schema_json.zig");
const schema_mutation = @import("schema_mutation.zig");
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
    transaction_local_search_path: bool = false,
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
            .transaction_local_search_path = false,
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
                .runtime => try validateSqlRuntimeSettingValue(set.name, set.value),
            }
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            if (session.transaction_local_search_path_base) |base| {
                updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                updated.transaction_local_search_path = session.transaction_local_search_path;
            }
            const settings = try replaceSessionSettingAlloc(alloc, updated.settings, set.name, set.value);
            for (updated.settings) |setting| {
                alloc.free(@constCast(setting.name));
                alloc.free(@constCast(setting.value));
            }
            if (updated.settings.len > 0) alloc.free(updated.settings);
            updated.settings = settings;
            return updated;
        },
        .reset_setting => |reset| {
            var updated = try OwnedSqlCatalogSession.fromSessionAlloc(alloc, session.session());
            errdefer updated.deinit(alloc);
            if (session.transaction_local_search_path_base) |base| {
                updated.transaction_local_search_path_base = try cloneStringSlice(alloc, base);
                updated.transaction_local_search_path = session.transaction_local_search_path;
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

fn alterTableRewriteExpressionSource(plan: ddl_plan.AlterTablePlan) ?AppliedDdlRewriteExpressionSource {
    for (plan.operations) |operation| switch (operation) {
        .alter_column_type => |alter_type| {
            const rewrite = alter_type.rewrite_expression orelse continue;
            return .{
                .target_column = alter_type.column_name,
                .expression = rewrite.expression,
            };
        },
        else => {},
    };
    return null;
}

pub fn applyDdlPlanToSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    plan: ddl_plan.LoweredDdlPlan,
) !ddl_plan.AppliedDdlSchemaJson {
    switch (plan) {
        .adapter_noop, .session_catalog => return .{ .schema_json = try alloc.dupe(u8, current_schema_json) },
        .create_table => |create_table| {
            if (current_schema_json.len != 0) {
                if (create_table.replace_existing) return try appliedDdlSchemaJsonWithFlagsAlloc(
                    alloc,
                    try schema_json.schemaJsonFromCreateTablePlanAlloc(alloc, create_table),
                    true,
                    true,
                    true,
                    null,
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
    const rewrite_source = switch (plan) {
        .alter_table => |alter_table| alterTableRewriteExpressionSource(alter_table),
        else => null,
    };
    result = try appliedDdlSchemaJsonWithFlagsAlloc(
        alloc,
        updated_schema_json,
        result.requires_rebuild,
        result.validation_required,
        result.rewrite_required,
        rewrite_source,
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
    return try appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(alloc, requires_rebuild, validation_required, rewrite_required, null);
}

fn appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(
    alloc: std.mem.Allocator,
    requires_rebuild: bool,
    validation_required: bool,
    rewrite_required: bool,
    rewrite_expression: ?AppliedDdlRewriteExpressionSource,
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
        items[i] = .{
            .action = .rewrite,
            .subject = .table,
            .reason = .row_images,
            .rewrite_expression = owned_rewrite,
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
) !ddl_plan.AppliedDdlSchemaJson {
    errdefer alloc.free(schema_json_text);
    const work_items = try appliedDdlTableWorkItemsForFlagsAndRewriteAlloc(alloc, requires_rebuild, validation_required, rewrite_required, rewrite_expression);
    return .{
        .schema_json = schema_json_text,
        .requires_rebuild = requires_rebuild,
        .validation_required = validation_required,
        .rewrite_required = rewrite_required,
        .work_items = work_items,
    };
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
