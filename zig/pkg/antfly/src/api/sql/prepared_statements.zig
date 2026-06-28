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
const ddl_plan = @import("../../sql/ddl.zig");
const sql_read_kind = @import("../../sql/statement_kind.zig");
const sql_value = @import("../../sql/value.zig");
const tokenized = @import("../../sql/tokenized.zig");

const sql_adapter = struct {
    const ParsedSql = tokenized.ParsedSql;
    const PreparedStatementPlan = ddl_plan.PreparedStatementPlan;
    const PrepareStatementPlan = ddl_plan.PrepareStatementPlan;
    const ExecutePreparedStatementPlan = ddl_plan.ExecutePreparedStatementPlan;
    const DeallocatePreparedStatementPlan = ddl_plan.DeallocatePreparedStatementPlan;
    const PreparedStatementSubjectKind = ddl_plan.PreparedStatementSubjectKind;
    const PreparedStatementStatementKind = ddl_plan.PreparedStatementStatementKind;
    const SqlReadStatementKind = sql_read_kind.SqlReadStatementKind;
    const SqlValue = sql_value.SqlValue;
    const preparedStatementPlanFromGeneratedAstAlloc = ddl_plan.preparedStatementPlanFromGeneratedAstAlloc;
};

pub const Runtime = struct {
    alloc: std.mem.Allocator,
    sessions: std.AutoHashMapUnmanaged(u64, Session) = .empty,

    const Session = struct {
        statements: std.StringHashMapUnmanaged(Record) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.statements.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                entry.value_ptr.deinit(alloc);
            }
            self.statements.deinit(alloc);
            self.* = undefined;
        }

        fn clear(self: *@This(), alloc: std.mem.Allocator) void {
            var it = self.statements.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                entry.value_ptr.deinit(alloc);
            }
            self.statements.clearRetainingCapacity();
        }
    };

    const Record = struct {
        parameter_count: usize,
        statement_kind: sql_adapter.PreparedStatementSubjectKind,
        statement_family: sql_adapter.PreparedStatementStatementKind,
        subject_parsed_sql: sql_adapter.ParsedSql,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            const owned_source_sql = self.subject_parsed_sql.sql();
            self.subject_parsed_sql.deinit(alloc);
            alloc.free(@constCast(owned_source_sql));
            self.* = undefined;
        }
    };

    pub const ExecutableStatement = struct {
        statement_kind: sql_adapter.PreparedStatementSubjectKind,
        statement_family: sql_adapter.PreparedStatementStatementKind,
        parsed_sql: *const sql_adapter.ParsedSql,
    };

    pub fn init(alloc: std.mem.Allocator) Runtime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *@This()) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(self.alloc);
        self.sessions.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn apply(self: *@This(), plan: sql_adapter.PreparedStatementPlan, session_id: u64) !void {
        if (session_id == 0) return error.InvalidSqlSession;
        switch (plan) {
            .prepare => |prepare_plan| try self.prepare(session_id, prepare_plan),
            .execute => |execute_plan| try self.execute(session_id, execute_plan),
            .deallocate => |deallocate_plan| try self.deallocate(session_id, deallocate_plan),
        }
    }

    pub fn statementKindForExecute(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.ExecutePreparedStatementPlan,
    ) !sql_adapter.PreparedStatementSubjectKind {
        return (try self.executeRecord(session_id, plan)).statement_kind;
    }

    pub fn planAllowedInReadOnly(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.PreparedStatementPlan,
    ) !bool {
        return switch (plan) {
            .prepare => |prepare_plan| prepare_plan.statement_kind == .read,
            .deallocate => true,
            .execute => |execute_plan| (try self.statementKindForExecute(session_id, execute_plan)) == .read,
        };
    }

    pub fn executableForExecute(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.ExecutePreparedStatementPlan,
    ) !ExecutableStatement {
        const record = try self.executeRecord(session_id, plan);
        return .{
            .statement_kind = record.statement_kind,
            .statement_family = record.statement_family,
            .parsed_sql = &record.subject_parsed_sql,
        };
    }

    pub fn statementCountForTest(self: *@This(), session_id: u64) usize {
        const session = self.sessions.getPtr(session_id) orelse return 0;
        return session.statements.count();
    }

    fn prepare(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.PrepareStatementPlan,
    ) !void {
        const result = try self.sessions.getOrPut(self.alloc, session_id);
        if (!result.found_existing) result.value_ptr.* = .{};
        const session = result.value_ptr;
        if (session.statements.contains(plan.statement_name)) return error.PreparedStatementAlreadyExists;
        const key = try self.alloc.dupe(u8, plan.statement_name);
        errdefer self.alloc.free(key);
        const owned_subject_sql = try self.ownedSubjectSqlForPreparePlanAlloc(plan);
        errdefer self.alloc.free(owned_subject_sql);
        var subject_parsed_sql = try self.ownedSubjectParsedSqlForPreparePlanAlloc(plan, owned_subject_sql);
        errdefer subject_parsed_sql.deinit(self.alloc);
        const subject_family = preparedStatementFamilyFromParsedSql(&subject_parsed_sql) orelse return error.UnsupportedSqlShape;
        if (subject_family != plan.statement_family or preparedStatementSubjectKindFromFamily(subject_family) != plan.statement_kind) {
            return error.UnsupportedSqlShape;
        }
        try session.statements.put(self.alloc, key, .{
            .parameter_count = plan.parameter_count,
            .statement_kind = plan.statement_kind,
            .statement_family = plan.statement_family,
            .subject_parsed_sql = subject_parsed_sql,
        });
    }

    fn ownedSubjectSqlForPreparePlanAlloc(self: *@This(), plan: sql_adapter.PrepareStatementPlan) ![]const u8 {
        if (plan.subject_parsed_sql) |parsed| return try self.alloc.dupe(u8, parsed.sql());
        return try self.alloc.dupe(u8, plan.subject_sql orelse return error.UnsupportedSqlShape);
    }

    fn ownedSubjectParsedSqlForPreparePlanAlloc(
        self: *@This(),
        plan: sql_adapter.PrepareStatementPlan,
        owned_subject_sql: []const u8,
    ) !sql_adapter.ParsedSql {
        if (plan.subject_parsed_sql) |parsed| {
            return try sql_adapter.ParsedSql.initFromTokenSliceAlloc(self.alloc, owned_subject_sql, parsed.items());
        }
        return try sql_adapter.ParsedSql.initAlloc(self.alloc, owned_subject_sql);
    }

    fn execute(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.ExecutePreparedStatementPlan,
    ) !void {
        _ = try self.executeRecord(session_id, plan);
    }

    fn executeRecord(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.ExecutePreparedStatementPlan,
    ) !*const Record {
        const session = self.sessions.getPtr(session_id) orelse return error.PreparedStatementNotFound;
        const record = session.statements.getPtr(plan.statement_name) orelse return error.PreparedStatementNotFound;
        if (record.parameter_count != plan.arguments.len or record.parameter_count != plan.argument_count) return error.PreparedStatementArgumentMismatch;
        return record;
    }

    fn deallocate(
        self: *@This(),
        session_id: u64,
        plan: sql_adapter.DeallocatePreparedStatementPlan,
    ) !void {
        const session = self.sessions.getPtr(session_id) orelse {
            if (plan.all) return;
            return error.PreparedStatementNotFound;
        };
        if (plan.all) {
            session.clear(self.alloc);
            return;
        }
        const name = plan.statement_name orelse return error.UnsupportedSqlShape;
        if (session.statements.fetchRemove(name)) |removed| {
            self.alloc.free(removed.key);
            var value = removed.value;
            value.deinit(self.alloc);
            return;
        }
        return error.PreparedStatementNotFound;
    }
};

fn preparedStatementFamilyFromParsedSql(parsed_sql: *const sql_adapter.ParsedSql) ?sql_adapter.PreparedStatementStatementKind {
    return switch (parsed_sql.statement) {
        .read => if (parsed_sql.readStatementKindIncludingGeneratedAst() != null) .read else null,
        .write => switch (parsed_sql.writeStatementKindIncludingGeneratedAst() orelse return null) {
            .insert => .insert,
            .insert_source => .insert_source,
            .update, .update_source, .update_joined_source => .update,
            .delete, .delete_source, .delete_joined_source => .delete,
            .truncate => .truncate,
            .merge => .merge,
        },
        .ddl,
        .explain,
        .transaction,
        .prepared,
        .session,
        => .ddl,
        .unsupported, .unknown => null,
    };
}

fn preparedStatementSubjectKindFromFamily(family: sql_adapter.PreparedStatementStatementKind) sql_adapter.PreparedStatementSubjectKind {
    return switch (family) {
        .read => .read,
        .insert,
        .insert_source,
        .update,
        .delete,
        .truncate,
        .merge,
        => .write,
        .ddl => .ddl,
    };
}

test "sql prepared statement runtime stores session scoped plans" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    const session_a: u64 = 101;
    const session_b: u64 = 202;
    const prepare_read = sql_adapter.PrepareStatementPlan{
        .statement_name = "usage_plan",
        .parameter_count = 1,
        .statement_kind = .read,
        .statement_family = .read,
        .subject_sql = "SELECT id FROM usage_records WHERE status = $1;",
    };

    try runtime.apply(.{ .prepare = prepare_read }, session_a);
    try std.testing.expectEqual(@as(usize, 1), runtime.statementCountForTest(session_a));
    try std.testing.expectError(error.PreparedStatementAlreadyExists, runtime.apply(.{ .prepare = prepare_read }, session_a));

    const args = [_]sql_adapter.SqlValue{.{ .string = "open" }};
    const execute_read = sql_adapter.ExecutePreparedStatementPlan{
        .statement_name = "usage_plan",
        .argument_count = args.len,
        .arguments = &args,
    };
    try std.testing.expect(try runtime.planAllowedInReadOnly(session_a, .{ .execute = execute_read }));
    const executable = try runtime.executableForExecute(session_a, execute_read);
    try std.testing.expectEqual(sql_adapter.PreparedStatementSubjectKind.read, executable.statement_kind);
    try std.testing.expectEqual(sql_adapter.PreparedStatementStatementKind.read, executable.statement_family);
    try std.testing.expectEqual(sql_adapter.SqlReadStatementKind.query, executable.parsed_sql.readStatementKind().?);

    try runtime.apply(.{ .execute = execute_read }, session_a);
    try std.testing.expectError(
        error.PreparedStatementArgumentMismatch,
        runtime.apply(.{ .execute = .{ .statement_name = "usage_plan" } }, session_a),
    );
    try std.testing.expectError(error.PreparedStatementNotFound, runtime.apply(.{ .execute = execute_read }, session_b));

    try runtime.apply(.{ .deallocate = .{ .statement_name = "usage_plan" } }, session_a);
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(session_a));
    try std.testing.expectError(error.PreparedStatementNotFound, runtime.apply(.{ .execute = execute_read }, session_a));

    const generated_prepare_sql = "PREPARE generated_usage_plan(text) AS SELECT id FROM usage_records WHERE status = $1;";
    var generated_prepare_parsed = try sql_adapter.ParsedSql.initAlloc(alloc, generated_prepare_sql);
    defer generated_prepare_parsed.deinit(alloc);
    const generated_raw = generated_prepare_parsed.generated_statement orelse return error.TestUnexpectedResult;
    const generated_ast = switch (generated_raw.ast orelse return error.TestUnexpectedResult) {
        .prepared => |ast| ast,
        else => return error.TestUnexpectedResult,
    };
    var generated_prepare_plan = try sql_adapter.preparedStatementPlanFromGeneratedAstAlloc(alloc, &generated_prepare_parsed, generated_ast);
    defer generated_prepare_plan.deinit(alloc);
    try runtime.apply(generated_prepare_plan, session_a);
    const generated_execute = sql_adapter.ExecutePreparedStatementPlan{
        .statement_name = "generated_usage_plan",
        .argument_count = args.len,
        .arguments = &args,
    };
    const generated_executable = try runtime.executableForExecute(session_a, generated_execute);
    try std.testing.expectEqual(sql_adapter.PreparedStatementStatementKind.read, generated_executable.statement_family);
    try std.testing.expectEqualStrings(generated_prepare_sql, generated_executable.parsed_sql.sql());
    try std.testing.expectEqualStrings("SELECT id FROM usage_records WHERE status = $1", generated_executable.parsed_sql.statementSql());
    try runtime.apply(.{ .deallocate = .{ .statement_name = "generated_usage_plan" } }, session_a);

    try runtime.apply(.{ .prepare = .{
        .statement_name = "read_plan",
        .statement_kind = .read,
        .statement_family = .read,
        .subject_sql = "SELECT id FROM usage_records;",
    } }, session_a);
    try runtime.apply(.{ .prepare = .{
        .statement_name = "write_plan",
        .parameter_count = 1,
        .statement_kind = .write,
        .statement_family = .insert,
        .subject_sql = "INSERT INTO usage_records(id, status) VALUES ($1, 'prepared');",
    } }, session_a);
    const write_args = [_]sql_adapter.SqlValue{.{ .string = "open" }};
    try std.testing.expect(!try runtime.planAllowedInReadOnly(session_a, .{ .execute = .{
        .statement_name = "write_plan",
        .argument_count = write_args.len,
        .arguments = &write_args,
    } }));
    try std.testing.expectEqual(@as(usize, 2), runtime.statementCountForTest(session_a));
    try runtime.apply(.{ .deallocate = .{ .all = true } }, session_a);
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(session_a));
}

test "sql prepared statement runtime rejects mismatched subject metadata" {
    const alloc = std.testing.allocator;
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    try std.testing.expectError(error.UnsupportedSqlShape, runtime.apply(.{ .prepare = .{
        .statement_name = "mismatched_read",
        .statement_kind = .write,
        .statement_family = .insert,
        .subject_sql = "SELECT id FROM usage_records;",
    } }, 303));
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(303));

    try std.testing.expectError(error.UnsupportedSqlShape, runtime.apply(.{ .prepare = .{
        .statement_name = "mismatched_insert_source",
        .statement_kind = .write,
        .statement_family = .insert,
        .subject_sql = "INSERT INTO usage_records(id) SELECT id FROM incoming_usage;",
    } }, 303));
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(303));

    var malformed_generated_read = try sql_adapter.ParsedSql.initAlloc(alloc, "SELECT u.id FROM usage_records AS u WHERE u.status = 'open'");
    defer malformed_generated_read.deinit(alloc);
    if (malformed_generated_read.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .read => |read_ast| read_ast.source_alias_tokens = null,
            else => return error.TestUnexpectedResult,
        };
    }
    try std.testing.expect(malformed_generated_read.readStatementKindIncludingGeneratedAst() == null);
    try std.testing.expect(preparedStatementFamilyFromParsedSql(&malformed_generated_read) == null);
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(303));

    var malformed_generated_write = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "INSERT INTO usage_records (id) SELECT s.id FROM ONLY incoming_usage AS s WHERE s.status = 'open'",
    );
    defer malformed_generated_write.deinit(alloc);
    if (malformed_generated_write.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .dml => |*dml_ast| {
                if (dml_ast.source_read) |*source_read| {
                    source_read.source_alias_tokens = null;
                    source_read.source_alias_name_tokens = null;
                } else return error.TestUnexpectedResult;
            },
            else => return error.TestUnexpectedResult,
        };
    }
    try std.testing.expect(malformed_generated_write.writeStatementKindIncludingGeneratedAst() == null);
    try std.testing.expect(preparedStatementFamilyFromParsedSql(&malformed_generated_write) == null);
    try std.testing.expectEqual(@as(usize, 0), runtime.statementCountForTest(303));
}
