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

const catalog_apply = @import("../../sql/catalog_apply.zig");
const catalog_resources = @import("../../sql/catalog_resources.zig");
const ddl_plan = @import("../../sql/ddl.zig");
const tokenized = @import("../../sql/tokenized.zig");

const sql_adapter = struct {
    const OwnedSqlCatalogSession = catalog_apply.OwnedSqlCatalogSession;
    const ParsedSql = tokenized.ParsedSql;
    const TransactionControlPlan = ddl_plan.TransactionControlPlan;
    const TransactionModePlan = ddl_plan.TransactionModePlan;
    const sqlEffectiveTransactionReadOnlyFromSession = catalog_apply.sqlEffectiveTransactionReadOnlyFromSession;
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

test "sql transaction helpers classify boundaries and read-only policy" {
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

    try std.testing.expect(try transactionControlPlanAllowedInReadOnly(.{ .transaction_mode = .{ .starter = .begin, .access_mode = .read_only } }));
    try std.testing.expect(!try transactionControlPlanAllowedInReadOnly(.{ .transaction_mode = .{ .starter = .begin, .access_mode = .read_write } }));

    const applied = try applyTransactionModePlanToSession(alloc, &session, .{ .starter = .begin, .access_mode = .read_only });
    try std.testing.expect(applied);
    try std.testing.expect(session.in_sql_transaction);
    try std.testing.expect(try readOnlyActive(&session));

    markTransactionFailedIfActive(&session);
    try std.testing.expect(session.sql_transaction_failed);
}
