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

const std = @import("std");
const catalog_apply = @import("catalog_apply.zig");
const catalog_resources = @import("catalog_resources.zig");
const ddl_plan = @import("ddl.zig");
const tokenized = @import("tokenized.zig");

const sql_adapter = struct {
    const OwnedSqlCatalogSession = catalog_apply.OwnedSqlCatalogSession;
    const ParsedSql = tokenized.ParsedSql;
    const SessionCatalogPlan = ddl_plan.SessionCatalogPlan;
    const TransactionControlPlan = ddl_plan.TransactionControlPlan;
    const TransactionModePlan = ddl_plan.TransactionModePlan;
    const parseSqlBoolSetting = catalog_apply.parseSqlBoolSetting;
    const sqlDefaultTransactionReadOnlyFromSession = catalog_apply.sqlDefaultTransactionReadOnlyFromSession;
    const sqlEffectiveTransactionReadOnlyFromSession = catalog_apply.sqlEffectiveTransactionReadOnlyFromSession;
};

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    sessions: std.AutoHashMapUnmanaged(u64, sql_adapter.OwnedSqlCatalogSession) = .empty,

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(self.alloc);
        self.sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn loadAlloc(self: *@This(), session_id: ?u64) !sql_adapter.OwnedSqlCatalogSession {
        if (session_id) |id| {
            if (self.sessions.getPtr(id)) |stored| {
                var session = try stored.cloneAlloc(self.alloc);
                session.request_read_only = false;
                session.notification_session_id = id;
                return session;
            }
        }
        var session = try sql_adapter.OwnedSqlCatalogSession.fromSessionAlloc(self.alloc, catalog_resources.SqlCatalogSession.default());
        if (session_id) |id| session.notification_session_id = id;
        return session;
    }

    pub fn save(self: *@This(), session: sql_adapter.OwnedSqlCatalogSession) !void {
        if (session.notification_session_id == 0) return;
        var stored = try session.cloneAlloc(self.alloc);
        errdefer stored.deinit(self.alloc);
        stored.notification_session_id = session.notification_session_id;
        stored.restoreRequestOverridesForPersistence(self.alloc);
        if (!sessionHasPersistentCatalogState(stored)) {
            if (self.sessions.fetchRemove(session.notification_session_id)) |old| {
                var old_value = old.value;
                old_value.deinit(self.alloc);
            }
            stored.deinit(self.alloc);
            return;
        }
        if (try self.sessions.fetchPut(self.alloc, session.notification_session_id, stored)) |old| {
            var old_value = old.value;
            old_value.deinit(self.alloc);
        }
    }

    pub fn sessionCountForTest(self: *@This()) usize {
        return self.sessions.count();
    }
};

pub fn parsedSqlTransactionBoundaryClearsLocalSession(parsed_sql: *const sql_adapter.ParsedSql) bool {
    const tokens = parsed_sql.items();
    const raw = parsed_sql.statement.raw();
    if (raw.token_start >= raw.token_end or raw.token_start >= tokens.len) return false;
    const first = tokens[raw.token_start];
    const next = raw.token_start + 1;
    if (first.matchesKeywordTag(.commit)) {
        return next >= raw.token_end or !tokens[next].matchesKeywordTag(.prepared);
    }
    if (first.matchesKeywordTag(.rollback)) {
        return next >= raw.token_end or
            (!tokens[next].matchesKeywordTag(.prepared) and
                !tokens[next].matchesKeywordTag(.to) and
                !tokens[next].matchesKeywordTag(.savepoint));
    }
    return false;
}

pub fn parsedSqlTransactionBoundaryStartsSession(parsed_sql: *const sql_adapter.ParsedSql) bool {
    const tokens = parsed_sql.items();
    const raw = parsed_sql.statement.raw();
    if (raw.token_start >= raw.token_end or raw.token_start >= tokens.len) return false;
    const first = tokens[raw.token_start];
    if (first.matchesKeywordTag(.begin)) return true;
    if (first.matchesKeyword("start")) {
        const next = raw.token_start + 1;
        return next < raw.token_end and tokens[next].matchesKeyword("transaction");
    }
    return false;
}

pub fn transactionIsActive(session: *const sql_adapter.OwnedSqlCatalogSession) bool {
    return session.in_sql_transaction or session.transaction_local_search_path or session.transaction_local_settings;
}

pub fn markTransactionFailedIfActive(session: *sql_adapter.OwnedSqlCatalogSession) void {
    if (transactionIsActive(session)) {
        session.in_sql_transaction = true;
        session.sql_transaction_failed = true;
    }
}

pub fn readOnlyActive(session: *const sql_adapter.OwnedSqlCatalogSession) !bool {
    return session.request_read_only or try sql_adapter.sqlEffectiveTransactionReadOnlyFromSession(session.session());
}

pub fn sessionCatalogPlanAllowedInReadOnly(
    session: *sql_adapter.OwnedSqlCatalogSession,
    plan: sql_adapter.SessionCatalogPlan,
) !bool {
    return switch (plan) {
        .set_search_path,
        .reset_search_path,
        .show_search_path,
        => true,
        .discard_all => false,
        .set_setting => |set| blk: {
            if (std.ascii.eqlIgnoreCase(set.name, "transaction_read_only") or
                std.ascii.eqlIgnoreCase(set.name, "default_transaction_read_only"))
            {
                break :blk try sql_adapter.parseSqlBoolSetting(set.value);
            }
            break :blk true;
        },
        .reset_setting => |reset| blk: {
            if (std.ascii.eqlIgnoreCase(reset.name, "default_transaction_read_only")) break :blk false;
            if (std.ascii.eqlIgnoreCase(reset.name, "transaction_read_only")) {
                break :blk try sql_adapter.sqlDefaultTransactionReadOnlyFromSession(session.session());
            }
            break :blk true;
        },
    };
}

pub fn transactionControlPlanAllowedInReadOnly(plan: sql_adapter.TransactionControlPlan) !bool {
    return switch (plan) {
        .transaction_mode => |mode| switch (mode.access_mode orelse .read_only) {
            .read_only => true,
            .read_write => false,
        },
        .constraint_mode,
        .advisory_lock,
        => true,
        .table_lock => false,
    };
}

pub fn applyTransactionModePlanToSession(
    alloc: std.mem.Allocator,
    session: *sql_adapter.OwnedSqlCatalogSession,
    plan: sql_adapter.TransactionModePlan,
) !bool {
    if (plan.starter == .begin or plan.starter == .start_transaction) {
        session.in_sql_transaction = true;
        session.sql_transaction_failed = false;
    }
    const access_mode = plan.access_mode orelse return false;
    const value = switch (access_mode) {
        .read_only => "on",
        .read_write => "off",
    };
    try session.setTransactionLocalSettingAlloc(alloc, "transaction_read_only", value);
    return true;
}

fn sessionHasPersistentCatalogState(session: sql_adapter.OwnedSqlCatalogSession) bool {
    if (session.in_sql_transaction) return true;
    if (session.sql_transaction_failed) return true;
    if (session.settings.len > 0) return true;
    if (session.transaction_local_settings_base != null) return true;
    if (session.transaction_local_search_path_base != null) return true;
    if (!std.ascii.eqlIgnoreCase(session.current_database_name, catalog_resources.default_database_name)) return true;
    if (session.search_path.len != 1 or !std.ascii.eqlIgnoreCase(session.search_path[0], catalog_resources.default_namespace_name)) return true;
    return false;
}

test "sql session helpers classify transaction boundaries and read-only policy" {
    const alloc = std.testing.allocator;

    var commit = try sql_adapter.ParsedSql.initAlloc(alloc, "COMMIT;");
    defer commit.deinit(alloc);
    try std.testing.expect(parsedSqlTransactionBoundaryClearsLocalSession(&commit));

    var commit_prepared = try sql_adapter.ParsedSql.initAlloc(alloc, "COMMIT PREPARED 'usage_batch';");
    defer commit_prepared.deinit(alloc);
    try std.testing.expect(!parsedSqlTransactionBoundaryClearsLocalSession(&commit_prepared));

    var rollback_to = try sql_adapter.ParsedSql.initAlloc(alloc, "ROLLBACK TO SAVEPOINT before_batch;");
    defer rollback_to.deinit(alloc);
    try std.testing.expect(!parsedSqlTransactionBoundaryClearsLocalSession(&rollback_to));

    var begin = try sql_adapter.ParsedSql.initAlloc(alloc, "BEGIN;");
    defer begin.deinit(alloc);
    try std.testing.expect(parsedSqlTransactionBoundaryStartsSession(&begin));

    var session = try sql_adapter.OwnedSqlCatalogSession.fromSessionAlloc(alloc, catalog_resources.SqlCatalogSession.default());
    defer session.deinit(alloc);
    try std.testing.expect(!try readOnlyActive(&session));
    session.request_read_only = true;
    try std.testing.expect(try readOnlyActive(&session));
    session.request_read_only = false;

    try std.testing.expect(try sessionCatalogPlanAllowedInReadOnly(&session, .show_search_path));
    try std.testing.expect(!try sessionCatalogPlanAllowedInReadOnly(&session, .discard_all));
    try std.testing.expect(try sessionCatalogPlanAllowedInReadOnly(&session, .{ .set_setting = .{ .name = "transaction_read_only", .value = "on" } }));
    try std.testing.expect(!try sessionCatalogPlanAllowedInReadOnly(&session, .{ .set_setting = .{ .name = "transaction_read_only", .value = "off" } }));

    try std.testing.expect(try transactionControlPlanAllowedInReadOnly(.{ .transaction_mode = .{ .starter = .begin, .access_mode = .read_only } }));
    try std.testing.expect(!try transactionControlPlanAllowedInReadOnly(.{ .transaction_mode = .{ .starter = .begin, .access_mode = .read_write } }));

    const applied = try applyTransactionModePlanToSession(alloc, &session, .{ .starter = .begin, .access_mode = .read_only });
    try std.testing.expect(applied);
    try std.testing.expect(session.in_sql_transaction);
    try std.testing.expect(try readOnlyActive(&session));

    markTransactionFailedIfActive(&session);
    try std.testing.expect(session.sql_transaction_failed);
}
