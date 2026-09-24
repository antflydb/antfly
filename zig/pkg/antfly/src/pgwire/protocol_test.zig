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
const protocol = @import("protocol.zig");
const backend = @import("backend.zig");

const Mock = struct {
    describes: usize = 0,
    executions: usize = 0,
    authentications: usize = 0,
    disconnects: usize = 0,
    releases: usize = 0,
    fail_auth: bool = false,
    fail_execute: bool = false,
    json_null_results: bool = false,
    ddl_pending: bool = false,
    ddl_unknown: bool = false,
    unknown_outcome: bool = false,
    mutation_outcome: ?backend.MutationOutcome = null,
    owned_results: bool = false,
    result_releases: usize = 0,
    saw_binding_guard: bool = false,
    seen_parameter: ?i64 = null,
    expected_text_parameter: ?[]const u8 = null,
    saw_text_parameter: bool = false,
    saw_statement_unchanged: bool = false,
    entered: ?*std.Io.Event = null,
    blocked: bool = false,
    stream_rows: usize = 0,
    expected_stream_statement: ?[]const u8 = null,
    expected_execute_statement: ?[]const u8 = null,
    setting_stream_opens: usize = 0,
    stream_offset: usize = 0,
    stream_closes: usize = 0,
    stream_pulls: usize = 0,
    stream_detaches: usize = 0,
    stream_validations: usize = 0,
    revoke_after_commit: bool = false,
    unknown_commit: bool = false,
    cursor_revoked: bool = false,
    stream_fail_at: ?usize = null,
    failed_transactions: usize = 0,
    canceled: std.atomic.Value(bool) = .init(false),
    namespace_log: [16]@import("session_commands.zig").Namespace = undefined,
    namespace_count: usize = 0,
    namespace_checks: usize = 0,
    saw_distinct_owner_namespace: bool = false,
    expected_cursor_namespace: ?[]const u8 = null,
    setting_generation: u64 = 1,
    setting_snapshots: usize = 0,
    observed_setting: ?i64 = null,
    describe_setting_epoch: ?u64 = null,
    observed_setting_epoch: ?u64 = null,

    fn source(self: *Mock) backend.Backend {
        return .{ .context = self, .vtable = &.{ .authenticate = authenticate, .describe = describe, .execute = Mock.execute, .load_settings = loadSettings, .validate_namespace = validateNamespace, .evaluate_parameters = evaluateParameters, .fail_transaction = failTransaction, .open_stream = openStream, .disconnect = disconnect } };
    }
    fn loadSettings(raw: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, request: backend.Request) !@import("../sql/setting_catalog.zig").RawSnapshot {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.setting_snapshots += 1;
        const definitions = try alloc.alloc(@import("../sql/setting_catalog.zig").Definition, 3);
        definitions[0] = .{ .identity = .{ .id = 1, .generation = self.setting_generation }, .name = "app.limit", .kind = .integer, .session_writable = true, .default = .{ .integer = 3 } };
        definitions[1] = .{ .identity = .{ .id = 2, .generation = 1 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "owner" } };
        definitions[2] = .{ .identity = .{ .id = 3, .generation = 1 }, .name = "app.tenant_id", .kind = .string, .session_writable = true, .default = .{ .string = "unassigned" } };
        return .{ .scope = .{ .principal = "tester", .database = request.database orelse "db" }, .epoch = self.setting_generation, .definitions = definitions };
    }
    fn validateNamespace(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, request: backend.Request) !void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        try request.check();
        self.namespace_checks += 1;
        const name = request.namespace orelse "public";
        if (!std.mem.eql(u8, name, "public") and !std.mem.eql(u8, name, "analytics") and !std.mem.eql(u8, name, "tenant")) return error.Forbidden;
    }
    fn failTransaction(raw: *anyopaque, _: backend.Identity, _: backend.Request) anyerror!void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.failed_transactions += 1;
    }
    fn evaluateParameters(_: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, request: backend.Request, expressions: []const []const u8) ![]const std.json.Value {
        try request.check();
        const result = try alloc.alloc(std.json.Value, expressions.len);
        for (expressions, result) |expression, *value| {
            value.* = if (expression.len >= 2 and expression[0] == '\'' and expression[expression.len - 1] == '\'')
                .{ .string = expression[1 .. expression.len - 1] }
            else
                .{ .integer = try std.fmt.parseInt(i64, expression, 10) };
        }
        return result;
    }
    fn openStream(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, request: backend.Request) !?backend.ReadStream {
        const self: *Mock = @ptrCast(@alignCast(raw));
        if (std.mem.indexOf(u8, request.statement, "application_name") != null or std.mem.indexOf(u8, request.statement, "client_encoding") != null) self.setting_stream_opens += 1;
        if (self.stream_rows == 0 or !std.mem.startsWith(u8, std.mem.trimStart(u8, request.statement, " \t\r\n"), "SELECT")) return null;
        if (self.expected_stream_statement) |expected| try std.testing.expectEqualStrings(expected, request.statement);
        try request.check();
        if (request.parameters.len > 0) self.seen_parameter = request.parameters[0].integer;
        self.saw_binding_guard = if (request.binding_guard) |guard| std.mem.eql(u8, guard, "immutable-catalog-binding") else false;
        return .{ .context = self, .columns = &.{.{ .name = "n", .type = .integer }}, .next = nextPage, .close = closeStream, .detach = detachStream, .validate = validateStream };
    }
    fn detachStream(raw: *anyopaque) void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.stream_detaches += 1;
    }
    fn validateStream(raw: *anyopaque, _: std.mem.Allocator, request: backend.Request) !void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        try request.check();
        if (self.expected_cursor_namespace) |expected| if (!std.mem.eql(u8, expected, request.namespace orelse "public")) return error.CatalogGenerationChanged;
        self.stream_validations += 1;
        if (self.cursor_revoked) return error.Forbidden;
    }
    fn nextPage(raw: *anyopaque, alloc: std.mem.Allocator, request: backend.Request, wanted: u32) !backend.StreamPage {
        const self: *Mock = @ptrCast(@alignCast(raw));
        try request.check();
        self.stream_pulls += 1;
        if (self.stream_fail_at) |at| if (self.stream_offset >= at) return error.QueryCanceled;
        const count = @min(wanted, self.stream_rows - self.stream_offset);
        const rows = try alloc.alloc([]const std.json.Value, count);
        for (rows, 0..) |*row, i| row.* = try alloc.dupe(std.json.Value, &.{.{ .integer = @intCast(self.stream_offset + i) }});
        self.stream_offset += count;
        return .{ .exhausted = self.stream_offset == self.stream_rows, .result = .{ .columns = &.{.{ .name = "n", .type = .integer }}, .rows = rows, .command_tag = "SELECT" } };
    }
    fn closeStream(raw: *anyopaque) void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        std.debug.assert(self.releases == 0);
        self.stream_closes += 1;
    }
    fn authenticate(raw: *anyopaque, _: std.mem.Allocator, user: []const u8, password: []const u8) !backend.Identity {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.authentications += 1;
        if (self.fail_auth or !std.mem.eql(u8, user, "tester") or !std.mem.eql(u8, password, "secret")) return error.Unauthorized;
        return .{ .context = self, .release = release };
    }
    fn describe(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, request: backend.Request) !backend.Description {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.describes += 1;
        try request.check();
        if (self.cursor_revoked) return error.Forbidden;
        if (self.ddl_pending or self.ddl_unknown) return .{ .columns = &.{} };
        if (self.json_null_results) return .{ .columns = &.{.{ .name = "j", .type = .json }} };
        return .{ .columns = &.{.{ .name = "n", .type = .integer }}, .parameter_types = if (std.mem.indexOf(u8, request.statement, "$1") != null) (if (std.mem.indexOf(u8, request.statement, "usage_records") != null) &.{.string} else &.{.integer}) else &.{}, .binding_guard = "immutable-catalog-binding", .setting_epoch = self.describe_setting_epoch };
    }
    fn execute(raw: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, request: backend.Request) !backend.Result {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.observed_setting = if (request.setting_overlay.len == 0) null else request.setting_overlay[0].value.integer;
        self.observed_setting_epoch = request.setting_epoch;
        if (self.expected_execute_statement) |expected| if (std.mem.startsWith(u8, request.statement, "SELECT")) try std.testing.expectEqualStrings(expected, request.statement);
        self.executions += 1;
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, request.statement, " \t\r\n;"), "begin")) return .{ .command_tag = "BEGIN", .transaction_status = .in_transaction, .session_id = "0123456789abcdef0123456789abcdef" };
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, request.statement, " \t\r\n;"), "commit")) {
            if (self.unknown_commit) {
                request.diagnostics.?.transaction_status = .idle;
                return error.SqlTransactionOutcomeUnknown;
            }
            if (self.revoke_after_commit) self.cursor_revoked = true;
            return .{ .command_tag = "COMMIT", .transaction_status = .idle };
        }
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, request.statement, " \t\r\n;"), "rollback")) return .{ .command_tag = "ROLLBACK", .transaction_status = .idle };
        if (try @import("session_commands.zig").control(alloc, request.statement)) |command| return .{ .command_tag = switch (command) {
            .savepoint => "SAVEPOINT",
            .rollback_to => "ROLLBACK",
            .release => "RELEASE",
        }, .transaction_status = .in_transaction, .session_id = "0123456789abcdef0123456789abcdef" };
        self.saw_binding_guard = if (request.binding_guard) |guard| std.mem.eql(u8, guard, "immutable-catalog-binding") else false;
        if (self.entered) |event| event.set(request.io);
        while (self.blocked) {
            request.check() catch |err| {
                self.canceled.store(true, .release);
                return err;
            };
            try request.io.sleep(.fromMilliseconds(1), .awake);
        }
        if (self.fail_execute) return error.UniqueConstraintViolation;
        if (self.unknown_outcome) {
            request.diagnostics.?.set("40003", "transaction outcome is unknown; do not replay", "0123456789abcdef0123456789abcdef".*, false);
            return error.SqlMutationOutcomeUnknown;
        }
        try request.check();
        if (self.ddl_unknown) return .{ .command_tag = "DDL PENDING", .ddl_receipt_json = "{\"state\":\"admission_unknown\",\"restore_job_id\":\"job-1\"}" };
        if (self.ddl_pending) return .{ .command_tag = "DDL PENDING", .mutation_outcome = .committed_pending, .ddl_receipt_json = "{\"table_id\":\"17\",\"schema_version\":8,\"state\":\"pending\"}" };
        if (self.json_null_results) return .{
            .columns = &.{.{ .name = "j", .type = .json }},
            .rows = &.{ &.{.null}, &.{.null} },
            .sql_nulls = &.{ &.{false}, &.{true} },
            .command_tag = "SELECT 2",
        };
        self.saw_statement_unchanged = std.mem.eql(u8, request.statement, "SELECT $1");
        if (self.namespace_count < self.namespace_log.len) {
            self.namespace_log[self.namespace_count] = try @import("session_commands.zig").Namespace.init(request.namespace orelse "public");
            self.namespace_count += 1;
        }
        if (request.session_namespace) |owner| if (std.mem.eql(u8, owner, "public") and !std.mem.eql(u8, owner, request.namespace orelse "public")) {
            self.saw_distinct_owner_namespace = true;
        };
        if (request.parameters.len > 0) {
            if (self.expected_text_parameter) |expected| {
                try std.testing.expectEqualStrings(expected, request.parameters[0].string);
                self.saw_text_parameter = true;
            } else self.seen_parameter = request.parameters[0].integer;
        }
        const rows = try alloc.alloc([]const std.json.Value, 2);
        rows[0] = try alloc.dupe(std.json.Value, &.{.{ .integer = self.seen_parameter orelse 9007199254740993 }});
        rows[1] = try alloc.dupe(std.json.Value, &.{.{ .integer = 2 }});
        return .{
            .columns = &.{.{ .name = "n", .type = .integer }},
            .rows = rows,
            .command_tag = "SELECT 2",
            .transaction_status = if (request.session_id != null) .in_transaction else .idle,
            .session_id = request.session_id,
            .mutation_outcome = self.mutation_outcome,
            .transaction_id = if (self.mutation_outcome != null) "0123456789abcdef0123456789abcdef".* else null,
            .owner = if (self.owned_results) .{ .context = self, .release = releaseResult } else null,
        };
    }
    fn releaseResult(raw: *anyopaque) void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.result_releases += 1;
    }
    fn disconnect(raw: *anyopaque, _: backend.Identity, _: ?[]const u8) void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.disconnects += 1;
    }
    fn release(raw: *anyopaque, _: std.mem.Allocator) void {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.releases += 1;
    }
};

fn frame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(payload.len + 4), .big);
    try out.writeAll(payload);
}

fn startup(out: *std.Io.Writer) !void {
    const body = "user\x00tester\x00database\x00db\x00\x00";
    try out.writeInt(u32, @intCast(body.len + 8), .big);
    try out.writeInt(u32, 196608, .big);
    try out.writeAll(body);
    try frame(out, 'p', "secret\x00");
}

fn parse(out: *std.Io.Writer, name: []const u8, statement: []const u8, parameter: bool) !void {
    var bytes = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer bytes.deinit();
    try bytes.writer.writeAll(name);
    try bytes.writer.writeByte(0);
    try bytes.writer.writeAll(statement);
    try bytes.writer.writeByte(0);
    try bytes.writer.writeInt(u16, if (parameter) 1 else 0, .big);
    if (parameter) try bytes.writer.writeInt(u32, 20, .big);
    try frame(out, 'P', bytes.written());
}

fn bind(out: *std.Io.Writer, name: []const u8, statement: []const u8, parameter: ?i64) !void {
    var bytes = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer bytes.deinit();
    try bytes.writer.writeAll(name);
    try bytes.writer.writeByte(0);
    try bytes.writer.writeAll(statement);
    try bytes.writer.writeByte(0);
    try bytes.writer.writeInt(u16, 1, .big);
    try bytes.writer.writeInt(u16, 1, .big);
    try bytes.writer.writeInt(u16, if (parameter != null) 1 else 0, .big);
    if (parameter) |value| {
        try bytes.writer.writeInt(i32, 8, .big);
        try bytes.writer.writeInt(i64, value, .big);
    }
    try bytes.writer.writeInt(u16, 1, .big);
    try bytes.writer.writeInt(u16, 1, .big);
    try frame(out, 'B', bytes.written());
}

fn execute(out: *std.Io.Writer, name: []const u8, count: i32) !void {
    var bytes = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer bytes.deinit();
    try bytes.writer.writeAll(name);
    try bytes.writer.writeByte(0);
    try bytes.writer.writeInt(i32, count, .big);
    try frame(out, 'E', bytes.written());
}

fn run(mock: *Mock, input: []const u8, limits: protocol.Limits) !std.Io.Writer.Allocating {
    var reader = std.Io.Reader.fixed(input);
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    errdefer output.deinit();
    var session = protocol.Session{ .alloc = std.testing.allocator, .io = std.testing.io, .source = mock.source(), .reader = &reader, .writer = &output.writer, .limits = limits };
    defer session.deinit();
    try session.run();
    return output;
}

fn tags(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var cursor = protocol.Cursor{ .bytes = bytes };
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    while (cursor.offset < bytes.len) {
        try out.append(alloc, try cursor.int(u8));
        const len = try cursor.int(u32);
        _ = try cursor.take(len - 4);
    }
    return out.toOwnedSlice(alloc);
}

test "pgwire pull portals stream beyond result cap without replay and release on exhaustion" {
    for ([_]bool{ false, true }) |simple| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        if (simple) {
            try frame(&input.writer, 'Q', "SELECT n FROM t\x00");
        } else {
            try parse(&input.writer, "q", "SELECT n FROM t", false);
            try bind(&input.writer, "p", "q", null);
            try execute(&input.writer, "p", 3);
            try execute(&input.writer, "p", 0);
            try frame(&input.writer, 'S', "");
        }
        try frame(&input.writer, 'X', "");
        var mock: Mock = .{ .stream_rows = 600 };
        var output = try run(&mock, input.written(), .{ .result_rows = 7 });
        defer output.deinit();
        const messages = try tags(std.testing.allocator, output.written());
        defer std.testing.allocator.free(messages);
        try std.testing.expectEqual(@as(usize, 600), std.mem.count(u8, messages, "D"));
        try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "E"));
        try std.testing.expectEqual(@as(usize, if (simple) 0 else 1), std.mem.count(u8, messages, "s"));
        try std.testing.expectEqual(@as(usize, 0), mock.executions);
        try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
        try std.testing.expect(mock.stream_pulls > 1);
        try std.testing.expect(std.mem.indexOf(u8, output.written(), "SELECT 600") != null);
    }
}

test "pgwire forward cursors stream bounded fetches and close with transaction" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "BEGIN\x00");
    try frame(&input.writer, 'Q', "DECLARE rows CURSOR FOR SELECT n FROM t\x00");
    try frame(&input.writer, 'Q', "FETCH FORWARD 2 FROM rows\x00");
    try frame(&input.writer, 'Q', "FETCH ALL FROM rows\x00");
    try frame(&input.writer, 'Q', "COMMIT\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 5 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 5), std.mem.count(u8, messages, "D"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
    try std.testing.expectEqual(@as(usize, 2), mock.stream_pulls);
    try std.testing.expectEqual(@as(usize, 2), mock.executions);
}

test "pgwire original cursor declaration and fetch retain one bounded result" {
    // sql-0048 and sql-0050: exact original SQL, exercised through the
    // PostgreSQL simple-query protocol with transaction-owned cursor state.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "BEGIN\x00");
    try frame(&input.writer, 'Q', "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00");
    try frame(&input.writer, 'Q', "FETCH NEXT FROM usage_cursor\x00");
    try frame(&input.writer, 'Q', "CLOSE usage_cursor\x00");
    try frame(&input.writer, 'Q', "COMMIT\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .expected_execute_statement = "SELECT id FROM usage_records ORDER BY id" };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, messages, "D"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 0), mock.stream_pulls);
    try std.testing.expectEqual(@as(usize, 0), mock.stream_closes);
    try std.testing.expectEqual(@as(usize, 3), mock.executions);
}

test "pgwire original forward fetch forms and cursor close commands" {
    // sql-0051, sql-0052, sql-0053, sql-0054, sql-0061, sql-0062, sql-0063. The exact
    // original commands must consume one cursor position, not rerun SELECT.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "BEGIN\x00",
        "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00",
        "FETCH FORWARD 10 IN usage_cursor\x00",
        "FETCH usage_cursor\x00",
        "FETCH 10 usage_cursor\x00",
        "FETCH FORWARD usage_cursor\x00",
        "FETCH ALL FROM usage_cursor\x00",
        "CLOSE usage_cursor\x00",
        "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00",
        "CLOSE ALL\x00",
        "COMMIT\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 30, .expected_stream_statement = "SELECT id FROM usage_records ORDER BY id" };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected_rows = [_]usize{ 0, 0, 10, 1, 10, 1, 8, 0, 0, 0, 0 };
    var query_index: usize = 0;
    var rows: usize = 0;
    var startup_ready = false;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        _ = try cursor.take(length - 4);
        try std.testing.expect(tag != 'E');
        if (tag == 'D') rows += 1;
        if (tag == 'Z') {
            if (!startup_ready) {
                startup_ready = true;
            } else {
                try std.testing.expect(query_index < expected_rows.len);
                try std.testing.expectEqual(expected_rows[query_index], rows);
                query_index += 1;
                rows = 0;
            }
        }
    }
    try std.testing.expectEqual(expected_rows.len, query_index);
    try std.testing.expectEqual(@as(usize, 30), mock.stream_offset);
    try std.testing.expectEqual(@as(usize, 2), mock.stream_closes);
}

test "pgwire original directional fetch forms preserve scroll position" {
    // sql-0055, sql-0056, sql-0057, sql-0058, sql-0059, sql-0060 use the exact original FETCH statements.
    // A SCROLL declaration is required for backward and absolute motion.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "BEGIN\x00",
        "DECLARE usage_cursor SCROLL CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00",
        "FETCH FORWARD 6 FROM usage_cursor\x00",
        "FETCH BACKWARD 5 FROM usage_cursor\x00",
        "MOVE ABSOLUTE 0 FROM usage_cursor\x00",
        "FETCH FIRST FROM usage_cursor\x00",
        "FETCH LAST FROM usage_cursor\x00",
        "FETCH ABSOLUTE 3 FROM usage_cursor\x00",
        "FETCH RELATIVE 2 FROM usage_cursor\x00",
        "FETCH PRIOR FROM usage_cursor\x00",
        "CLOSE ALL\x00",
        "COMMIT\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 10, .expected_stream_statement = "SELECT id FROM usage_records ORDER BY id" };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "0", "1", "2", "3", "4", "5", "4", "3", "2", "1", "0", "0", "9", "2", "4", "3" };
    var row_index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const cell_length = try payload.int(u32);
            try std.testing.expect(row_index < expected.len);
            try std.testing.expectEqualStrings(expected[row_index], try payload.take(cell_length));
            row_index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, row_index);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
}

test "pgwire materialized cursor enforces quota and reauthorizes held fetch" {
    for ([_]bool{ false, true }) |revoke| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        try frame(&input.writer, 'Q', "BEGIN\x00");
        try frame(&input.writer, 'Q', "DECLARE rows CURSOR WITH HOLD FOR SELECT id FROM usage_records ORDER BY id\x00");
        try frame(&input.writer, 'Q', "COMMIT\x00");
        try frame(&input.writer, 'Q', "FETCH NEXT FROM rows\x00");
        try frame(&input.writer, 'X', "");
        var mock: Mock = .{ .revoke_after_commit = revoke, .expected_execute_statement = "SELECT id FROM usage_records ORDER BY id" };
        var output = try run(&mock, input.written(), .{ .cursor_bytes = if (revoke) 4096 else 16 });
        defer output.deinit();
        const messages = try tags(std.testing.allocator, output.written());
        defer std.testing.allocator.free(messages);
        try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "D"));
        try std.testing.expect(std.mem.count(u8, messages, "E") >= 1);
        if (revoke) {
            try std.testing.expect(std.mem.indexOf(u8, output.written(), "42501") != null);
        } else {
            try std.testing.expect(std.mem.indexOf(u8, output.written(), "54000") != null);
        }
    }
}

test "pgwire failed cursor fetch marks transaction aborted and releases retained stream" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "BEGIN\x00");
    try frame(&input.writer, 'Q', "DECLARE rows CURSOR FOR SELECT n FROM t\x00");
    try frame(&input.writer, 'Q', "FETCH ALL FROM rows\x00");
    try frame(&input.writer, 'Q', "ROLLBACK\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 5, .stream_fail_at = 0 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 1), mock.failed_transactions);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
    try std.testing.expectEqual(@as(usize, 2), mock.executions);
}

test "pgwire scroll hold cursor materializes before commit and retains exact position" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "BEGIN\x00",                       "DECLARE rows SCROLL CURSOR WITH HOLD FOR SELECT n FROM t\x00",
        "FETCH FORWARD 2 FROM rows\x00",   "FETCH PRIOR FROM rows\x00",
        "COMMIT\x00",                      "FETCH LAST FROM rows\x00",
        "FETCH ABSOLUTE -2 FROM rows\x00", "MOVE ABSOLUTE 0 FROM rows\x00",
        "FETCH NEXT FROM rows\x00",        "BEGIN\x00",
        "ROLLBACK\x00",                    "FETCH RELATIVE 1 FROM rows\x00",
        "CLOSE rows\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 5 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 7), std.mem.count(u8, messages, "D"));
    var wire_cursor: protocol.Cursor = .{ .bytes = output.written() };
    var row_index: usize = 0;
    const expected = [_][]const u8{ "0", "1", "0", "4", "3", "0", "1" };
    while (wire_cursor.offset < wire_cursor.bytes.len) {
        const tag = try wire_cursor.int(u8);
        const length = try wire_cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try wire_cursor.take(length - 4) };
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const cell_length = try payload.int(u32);
            try std.testing.expectEqualStrings(expected[row_index], try payload.take(cell_length));
            row_index += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), mock.stream_detaches);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
    try std.testing.expect(mock.stream_validations >= 6);
    try std.testing.expectEqual(@as(usize, 4), mock.executions);
}

test "pgwire rollback to savepoint closes only later cursors and preserves earlier position" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "BEGIN\x00",                                           "DECLARE earlier SCROLL CURSOR FOR SELECT n FROM t\x00",
        "SAVEPOINT \"point\"\x00",                             "FETCH NEXT FROM earlier\x00",
        "DECLARE later SCROLL CURSOR FOR SELECT n FROM t\x00", "ROLLBACK TO SAVEPOINT \"point\"\x00",
        "FETCH NEXT FROM earlier\x00",                         "RELEASE \"point\"\x00",
        "COMMIT\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 5 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, messages, "D"));
    try std.testing.expectEqual(@as(usize, 2), mock.stream_closes);
    try std.testing.expectEqual(@as(usize, 5), mock.executions);
}

test "pgwire held cursor commit spool quota and revoked grants fail closed" {
    for ([_]bool{ false, true }) |revoke| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        for ([_][]const u8{ "BEGIN\x00", "DECLARE rows CURSOR WITH HOLD FOR SELECT n FROM t\x00", "COMMIT\x00", "FETCH ALL FROM rows\x00" }) |statement| try frame(&input.writer, 'Q', statement);
        try frame(&input.writer, 'X', "");
        var mock: Mock = .{ .stream_rows = 5, .revoke_after_commit = revoke };
        var output = try run(&mock, input.written(), .{ .cursor_rows = if (revoke) 5 else 2 });
        defer output.deinit();
        const messages = try tags(std.testing.allocator, output.written());
        defer std.testing.allocator.free(messages);
        try std.testing.expect(std.mem.count(u8, messages, "E") >= 1);
        try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "D"));
        try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
        try std.testing.expectEqual(@as(usize, if (revoke) 2 else 1), mock.executions);
    }
}

test "pgwire statement timeout settings preserve transaction local and savepoint semantics" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET statement_timeout TO '4s'\x00", "BEGIN\x00",                  "SET LOCAL statement_timeout = 100\x00",
        "SHOW statement_timeout\x00",        "SAVEPOINT point\x00",        "SET SESSION statement_timeout = 200\x00",
        "ROLLBACK TO point\x00",             "SHOW statement_timeout\x00", "COMMIT\x00",
        "SHOW statement_timeout\x00",        "BEGIN\x00",                  "SET statement_timeout = 300\x00",
        "ROLLBACK\x00",                      "SHOW statement_timeout\x00", "RESET statement_timeout\x00",
        "SHOW statement_timeout\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try parse(&input.writer, "timeout", "SHOW statement_timeout", false);
    try bind(&input.writer, "timeout_portal", "timeout", null);
    try execute(&input.writer, "timeout_portal", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "100ms", "100ms", "4000ms", "4000ms", "30000ms", "30000ms" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
    try std.testing.expectEqual(@as(usize, 6), mock.executions);
}

test "pgwire original public search path and timeout session commands" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    // sql-0037, sql-0039, sql-0041, sql-0043, sql-0046 are exact
    // original statements. The intervening change makes LOCAL rollback
    // observable instead of merely accepting its syntax.
    for ([_][]const u8{
        "SET search_path TO analytics\x00",
        "SET search_path TO public;\x00",
        "SHOW search_path;\x00",
        "SET search_path TO analytics\x00",
        "BEGIN\x00",
        "SET LOCAL search_path TO public;\x00",
        "SHOW search_path;\x00",
        "ROLLBACK\x00",
        "SHOW search_path;\x00",
        "RESET search_path;\x00",
        "SHOW search_path;\x00",
        "SET statement_timeout = '1ms';\x00",
        "SHOW statement_timeout\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "public", "public", "analytics", "public", "1ms" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
}

test "pgwire application name preserves transaction local and savepoint semantics" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET application_name TO 'base client'\x00",       "BEGIN\x00",
        "SET LOCAL application_name = 'local client'\x00", "SHOW application_name\x00",
        "SAVEPOINT point\x00",                             "SET SESSION application_name = 'new client'\x00",
        "ROLLBACK TO point\x00",                           "SHOW application_name\x00",
        "COMMIT\x00",                                      "SHOW application_name\x00",
        "BEGIN\x00",                                       "SET application_name = 'other client'\x00",
        "ROLLBACK\x00",                                    "SHOW application_name\x00",
        "RESET application_name\x00",                      "SHOW application_name\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try parse(&input.writer, "app", "SHOW application_name", false);
    try bind(&input.writer, "app_portal", "app", null);
    try execute(&input.writer, "app_portal", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "local client", "local client", "base client", "base client", "", "" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
    try std.testing.expectEqual(@as(usize, 6), mock.executions);
    try std.testing.expectEqual(@as(usize, 0), mock.setting_stream_opens);
}

test "pgwire UTF-8 encoding setting stays connection owned in simple and extended paths" {
    // sql-0778: mounted wire proof for the original statement, including
    // simple and extended routing without a SQL storage cursor.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET client_encoding = 'UTF8';\x00", "SET NAMES 'UTF8'\x00",                     "SHOW client_encoding\x00",
        "BEGIN\x00",                         "SET LOCAL client_encoding TO 'UTF-8'\x00", "COMMIT\x00",
        "RESET client_encoding\x00",         "SHOW client_encoding\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try parse(&input.writer, "encoding", "SHOW client_encoding", false);
    try bind(&input.writer, "encoding_portal", "encoding", null);
    try execute(&input.writer, "encoding_portal", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    var rows: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expectEqualStrings("UTF8", try payload.take(n));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), rows);
    try std.testing.expectEqual(@as(usize, 0), mock.setting_stream_opens);
}

test "pgwire RESET ALL restores supported settings with transaction rollback and commit" {
    // sql-0045: exact command with current settings. Original custom app.*
    // catalog semantics still need a separate implementation and parity gate.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET application_name = 'base'\x00",
        "SET statement_timeout = 100\x00",
        "SET search_path = tenant\x00",
        "BEGIN\x00",
        "SET application_name = 'tx'\x00",
        "SAVEPOINT before_reset\x00",
        "RESET ALL;\x00",
        "SHOW application_name\x00",
        "SHOW search_path\x00",
        "ROLLBACK TO before_reset\x00",
        "SHOW application_name\x00",
        "SHOW search_path\x00",
        "RESET ALL;\x00",
        "COMMIT\x00",
        "SHOW application_name\x00",
        "SHOW statement_timeout\x00",
        "SHOW search_path\x00",
        "SET application_name = 'later'\x00",
        "BEGIN\x00",
        "RESET ALL;\x00",
        "ROLLBACK\x00",
        "SHOW application_name\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try parse(&input.writer, "reset", "RESET ALL;", false);
    try bind(&input.writer, "reset_portal", "reset", null);
    try execute(&input.writer, "reset_portal", 0);
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'Q', "SHOW application_name\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "", "public", "tx", "tenant", "", "30000ms", "public", "later", "" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
}

test "pgwire DISCARD ALL releases active portal after extended reply" {
    // sql-0047: current connection-resource lifecycle only. Original custom
    // setting and broader catalog semantics remain unresolved.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "old", "SELECT n FROM t", false);
    try bind(&input.writer, "live", "old", null);
    try execute(&input.writer, "live", 1);
    try parse(&input.writer, "discard", "DISCARD ALL;", false);
    try bind(&input.writer, "command", "discard", null);
    try execute(&input.writer, "command", 0);
    // Reusing the old name proves the DISCARD reply released prepared plans.
    try parse(&input.writer, "old", "SHOW client_encoding", false);
    try bind(&input.writer, "fresh", "old", null);
    try execute(&input.writer, "fresh", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 4 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    var saw_encoding = false;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            if (std.mem.eql(u8, try payload.take(n), "UTF8")) saw_encoding = true;
        }
    }
    try std.testing.expect(saw_encoding);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
}

test "pgwire typed catalog settings honor local savepoint reset and discard overlays" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET app.limit = 5\x00",
        "SHOW app.limit\x00",
        "BEGIN\x00",
        "SET LOCAL app.limit = 7\x00",
        "SHOW app.limit\x00",
        "SAVEPOINT saved\x00",
        "SET app.limit = 9\x00",
        "ROLLBACK TO saved\x00",
        "SHOW app.limit\x00",
        "COMMIT\x00",
        "SHOW app.limit\x00",
        "RESET ALL\x00",
        "SHOW app.limit\x00",
        "SET app.limit = 6\x00",
        "DISCARD ALL\x00",
        "SHOW app.limit\x00",
        "SELECT 1\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "5", "7", "7", "5", "3", "3", "9007199254740993", "2" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
    try std.testing.expect(mock.setting_snapshots >= 9);
    try std.testing.expectEqual(@as(?i64, null), mock.observed_setting);
}

test "pgwire original app setting reset all and discard all commands are connection scoped" {
    // sql-0042, sql-0044, sql-0045, sql-0047: exact public commands use one
    // typed, authorized connection overlay rather than shared catalog writes.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET app.tenant_id = 'tenant-a';\x00",
        "SHOW app.tenant_id;\x00",
        "RESET app.tenant_id;\x00",
        "SHOW app.tenant_id;\x00",
        "SET app.tenant_id = 'tenant-b';\x00",
        "RESET ALL;\x00",
        "SHOW app.tenant_id;\x00",
        "SET app.tenant_id = 'tenant-c';\x00",
        "DISCARD ALL;\x00",
        "SHOW app.tenant_id;\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const expected = [_][]const u8{ "tenant-a", "unassigned", "unassigned", "unassigned" };
    var index: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        try std.testing.expect(tag != 'E');
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expect(index < expected.len);
            try std.testing.expectEqualStrings(expected[index], try payload.take(n));
            index += 1;
        }
    }
    try std.testing.expectEqual(expected.len, index);
    try std.testing.expectEqual(@as(usize, 0), mock.executions);
}

test "pgwire typed catalog setting writes fail closed for policy type and local scope" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET app.tenant = 'other'\x00",
        "SET app.limit = nope\x00",
        "SET app.unknown = 1\x00",
        "SET LOCAL app.limit = 8\x00",
        "SHOW app.limit\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    for ([_][]const u8{ "42501", "22023", "42704", "25P01" }) |state| try std.testing.expect(std.mem.indexOf(u8, output.written(), state) != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "3") != null);
    try std.testing.expectEqual(@as(usize, 0), mock.executions);
}

test "pgwire prepared statement carries setting catalog epoch to execution" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "setting_plan", "SELECT 1", false);
    try bind(&input.writer, "setting_portal", "setting_plan", null);
    try execute(&input.writer, "setting_portal", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .describe_setting_epoch = 17 };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(?u64, 17), mock.observed_setting_epoch);
}

test "pgwire DISCARD ALL inside a transaction leaves prepared plans intact" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "keep", "SHOW client_encoding", false);
    try frame(&input.writer, 'Q', "BEGIN\x00");
    try frame(&input.writer, 'Q', "DISCARD ALL;\x00");
    try frame(&input.writer, 'Q', "ROLLBACK\x00");
    try bind(&input.writer, "kept", "keep", null);
    try execute(&input.writer, "kept", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    var errors: usize = 0;
    var rows: usize = 0;
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    while (cursor.offset < cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var payload: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        if (tag == 'E') errors += 1;
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try payload.int(u16));
            const n = try payload.int(u32);
            try std.testing.expectEqualStrings("UTF8", try payload.take(n));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), errors);
    try std.testing.expectEqual(@as(usize, 1), rows);
}

test "pgwire unknown commit cannot publish held cursor rows" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{ "BEGIN\x00", "DECLARE rows SCROLL CURSOR WITH HOLD FOR SELECT n FROM t\x00", "COMMIT\x00", "FETCH NEXT FROM rows\x00" }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 5, .unknown_commit = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const messages = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, messages, "D"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, messages, "E"));
    try std.testing.expectEqual(@as(usize, 1), mock.stream_detaches);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
}

test "pgwire pull failure and disconnect close snapshots before releasing identity" {
    for ([_]bool{ false, true }) |fail| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        try parse(&input.writer, "q", "SELECT n FROM t", false);
        try bind(&input.writer, "p", "q", null);
        try execute(&input.writer, "p", 2);
        if (fail) try execute(&input.writer, "p", 0);
        try frame(&input.writer, 'X', "");
        var mock: Mock = .{ .stream_rows = 100, .stream_fail_at = if (fail) 2 else null };
        var output = try run(&mock, input.written(), .{});
        defer output.deinit();
        try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
        try std.testing.expectEqual(@as(usize, 0), mock.executions);
        try std.testing.expectEqual(@as(usize, 1), mock.releases);
    }
}

test "pgwire distinguishes JSON null from SQL NULL in text and binary suspended portals" {
    for ([_]bool{ false, true }) |binary| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        if (binary) {
            try parse(&input.writer, "q", "SELECT j FROM t", false);
            try bind(&input.writer, "p", "q", null);
            try execute(&input.writer, "p", 1);
            try execute(&input.writer, "p", 1);
            try frame(&input.writer, 'S', "");
        } else try frame(&input.writer, 'Q', "SELECT j FROM t\x00");
        try frame(&input.writer, 'X', "");
        var mock: Mock = .{ .json_null_results = true };
        var output = try run(&mock, input.written(), .{});
        defer output.deinit();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var row_index: usize = 0;
        while (cursor.offset < cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const length = try cursor.int(u32);
            const payload = try cursor.take(length - 4);
            try std.testing.expect(tag != 'E');
            if (tag != 'D') continue;
            var row: protocol.Cursor = .{ .bytes = payload };
            try std.testing.expectEqual(@as(u16, 1), try row.int(u16));
            const cell_length = try row.int(i32);
            if (row_index == 0) {
                try std.testing.expectEqual(@as(i32, if (binary) 5 else 4), cell_length);
                if (binary) try std.testing.expectEqual(@as(u8, 1), try row.int(u8));
                try std.testing.expectEqualStrings("null", try row.take(4));
            } else try std.testing.expectEqual(@as(i32, -1), cell_length);
            try row.finish();
            row_index += 1;
        }
        try std.testing.expectEqual(@as(usize, 2), row_index);
        try std.testing.expectEqual(@as(usize, 1), mock.executions);
    }
}

test "pgwire preserves unknown transaction receipt and never replays queued execute" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "q", "UPDATE t SET n = 1", false);
    try bind(&input.writer, "p", "q", null);
    try execute(&input.writer, "p", 0);
    try execute(&input.writer, "p", 0);
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'X', "");
    var mock = Mock{ .unknown_outcome = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.executions);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "40003") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "\"transaction_id\":\"0123456789abcdef0123456789abcdef\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "\"retryable\":false") != null);
}

test "pgwire committed outcomes retain native owner until portal cleanup and emit receipt notice" {
    for ([_]backend.MutationOutcome{ .committed, .committed_pending, .committed_repair_required, .committed_graph_metric_materialization_rejected }) |outcome| {
        var input = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer input.deinit();
        try startup(&input.writer);
        try parse(&input.writer, "q", "UPDATE t SET n = 1", false);
        try bind(&input.writer, "p", "q", null);
        try execute(&input.writer, "p", 1);
        try execute(&input.writer, "p", 1);
        try frame(&input.writer, 'C', "Pp\x00");
        try frame(&input.writer, 'S', "");
        try frame(&input.writer, 'X', "");
        var mock = Mock{ .mutation_outcome = outcome, .owned_results = true };
        var output = try run(&mock, input.written(), .{});
        defer output.deinit();
        try std.testing.expectEqual(@as(usize, 1), mock.executions);
        try std.testing.expectEqual(@as(usize, 1), mock.result_releases);
        const observed = try tags(std.testing.allocator, output.written());
        defer std.testing.allocator.free(observed);
        try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'E') == null);
        try std.testing.expect(std.mem.endsWith(u8, observed, "DsDNC3Z"));
        try std.testing.expect(std.mem.indexOf(u8, output.written(), @tagName(outcome)) != null);
    }
}

test "pgwire pending DDL returns error receipt without successful command completion" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "CREATE UNIQUE INDEX i ON t (id)\x00");
    try frame(&input.writer, 'X', "");
    var mock = Mock{ .ddl_pending = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'E') != null);
    try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'C') == null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "55000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "\"schema_version\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "do not replay") != null);
}

test "pgwire unknown DDL admission never claims a committed declaration" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "TRUNCATE TABLE t\x00");
    try frame(&input.writer, 'X', "");
    var mock = Mock{ .ddl_unknown = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'E') != null);
    try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'C') == null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "40003") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "committed") == null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "job-1") != null);
}

test "pgwire unknown DDL admission drains extended execution until Sync without replay" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "ddl", "TRUNCATE TABLE t", false);
    try bind(&input.writer, "ddl_portal", "ddl", null);
    try execute(&input.writer, "ddl_portal", 0);
    try execute(&input.writer, "ddl_portal", 0);
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'X', "");
    var mock = Mock{ .ddl_unknown = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, observed, "E"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, observed, "C"));
    try std.testing.expectEqual(@as(usize, 1), mock.executions);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "40003") != null);
}

test "pgwire scoped search path pins prepared namespaces and restores transactional settings" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "SET search_path = analytics\x00");
    try frame(&input.writer, 'Q', "PREPARE pinned AS SELECT n FROM t\x00");
    try parse(&input.writer, "wire_pinned", "SELECT n FROM t", false);
    try frame(&input.writer, 'Q', "SET search_path = public\x00");
    try frame(&input.writer, 'Q', "EXECUTE pinned\x00");
    try bind(&input.writer, "wire_portal", "wire_pinned", null);
    try execute(&input.writer, "wire_portal", 0);
    for ([_][]const u8{
        "BEGIN\x00",           "SET LOCAL search_path = analytics\x00", "SELECT n FROM t\x00",
        "SAVEPOINT point\x00", "SET search_path = tenant\x00",          "ROLLBACK TO point\x00",
        "SELECT n FROM t\x00", "COMMIT\x00",                            "SELECT n FROM t\x00",
        "BEGIN\x00",           "SET search_path = analytics\x00",       "ROLLBACK\x00",
        "SELECT n FROM t\x00", "SHOW search_path\x00",                  "RESET search_path\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try parse(&input.writer, "show_path", "SHOW search_path", false);
    try bind(&input.writer, "show_path_portal", "show_path", null);
    try execute(&input.writer, "show_path_portal", 0);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expect(std.mem.indexOfScalar(u8, observed, 'E') == null);
    const expected = [_][]const u8{ "analytics", "analytics", "analytics", "analytics", "public", "public" };
    try std.testing.expectEqual(expected.len, mock.namespace_count);
    for (expected, 0..) |value, index| try std.testing.expectEqualStrings(value, mock.namespace_log[index].slice());
    try std.testing.expect(mock.saw_distinct_owner_namespace);
    try std.testing.expectEqual(@as(usize, 6), mock.namespace_checks);
}

test "pgwire multi namespace search path fails closed without changing the lookup scope" {
    // sql-0038, sql-0040: ordered namespace binding is not yet implemented;
    // neither exact public command may silently use only its first entry.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{
        "SET search_path = analytics\x00",
        "SET SESSION search_path TO tenant_schema, public;\x00",
        "SET LOCAL search_path TO tenant_schema, public;\x00",
        "SHOW search_path\x00",
        "SELECT n FROM t\x00",
    }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, observed, "E"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, output.written(), "0A000"));
    try std.testing.expectEqual(@as(usize, 1), mock.namespace_count);
    try std.testing.expectEqualStrings("analytics", mock.namespace_log[0].slice());
    try std.testing.expectEqual(@as(usize, 1), mock.namespace_checks);
}

test "pgwire held cursor keeps declaration namespace after mutable search path changes" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{ "SET search_path = analytics\x00", "BEGIN\x00", "DECLARE rows SCROLL CURSOR WITH HOLD FOR SELECT n FROM t\x00", "COMMIT\x00", "SET search_path = public\x00", "FETCH NEXT FROM rows\x00" }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 2, .expected_cursor_namespace = "analytics" };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, observed, "E"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, observed, "D"));
    try std.testing.expect(mock.stream_validations != 0);
}

test "pgwire denied namespace setting fails native transaction and rollback restores scope" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    for ([_][]const u8{ "BEGIN\x00", "SET LOCAL search_path = forbidden\x00", "SHOW search_path\x00", "ROLLBACK\x00", "SELECT n FROM t\x00" }) |statement| try frame(&input.writer, 'Q', statement);
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    const observed = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, observed, "E"));
    try std.testing.expectEqual(@as(usize, 1), mock.failed_transactions);
    try std.testing.expectEqual(@as(usize, 1), mock.namespace_count);
    try std.testing.expectEqualStrings("public", mock.namespace_log[0].slice());
}

test "pgwire simple query releases native result owner exactly once" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "SELECT 1\x00");
    try frame(&input.writer, 'X', "");
    var mock = Mock{ .owned_results = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.result_releases);
}

test "pgwire SQL execute streams typed parameters without eager result cap" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "PREPARE q(bigint) AS SELECT $1\x00");
    try frame(&input.writer, 'Q', "EXECUTE q(17)\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .stream_rows = 7 };
    var output = try run(&mock, input.written(), .{ .result_rows = 2 });
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 0), mock.executions);
    try std.testing.expectEqual(@as(usize, 7), mock.stream_offset);
    try std.testing.expectEqual(@as(usize, 1), mock.stream_closes);
    try std.testing.expect(mock.saw_binding_guard);
    try std.testing.expectEqual(@as(i64, 17), mock.seen_parameter.?);
}

test "pgwire SQL prepare execute deallocate share connection ownership with wire statements" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "PREPARE Mixed(bigint) AS SELECT $1\x00");
    try frame(&input.writer, 'Q', "PREPARE mixed AS SELECT $1\x00");
    try frame(&input.writer, 'Q', "COMMIT\x00");
    try frame(&input.writer, 'Q', "EXECUTE MIXED(9007199254740993)\x00");
    try frame(&input.writer, 'Q', "DEALLOCATE PREPARE mixed\x00");
    try frame(&input.writer, 'Q', "EXECUTE mixed(1)\x00");
    try parse(&input.writer, "wire", "SELECT $1", true);
    try frame(&input.writer, 'Q', "EXECUTE wire(7)\x00");
    try frame(&input.writer, 'Q', "DEALLOCATE ALL\x00");
    try frame(&input.writer, 'Q', "EXECUTE wire(8)\x00");
    try frame(&input.writer, 'X', "");
    var mock = Mock{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 2), mock.describes);
    try std.testing.expectEqual(@as(usize, 3), mock.executions);
    try std.testing.expect(mock.saw_statement_unchanged);
    try std.testing.expect(mock.saw_binding_guard);
    try std.testing.expectEqual(@as(i64, 7), mock.seen_parameter.?);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "42P05") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "26000") != null);
    try std.testing.expectEqual(@as(usize, 1), mock.disconnects);
}

test "pgwire original prepared read executes text and deallocates connection state" {
    // sql-0001, sql-0034, sql-0035, sql-0036: exact original statements.
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try frame(&input.writer, 'Q', "PREPARE usage_plan(text) AS SELECT id FROM usage_records WHERE status = $1\x00");
    try frame(&input.writer, 'Q', "EXECUTE usage_plan('open')\x00");
    try frame(&input.writer, 'Q', "DEALLOCATE usage_plan\x00");
    try frame(&input.writer, 'Q', "EXECUTE usage_plan('open')\x00");
    try frame(&input.writer, 'Q', "PREPARE usage_plan(text) AS SELECT id FROM usage_records WHERE status = $1\x00");
    try frame(&input.writer, 'Q', "DEALLOCATE ALL\x00");
    try frame(&input.writer, 'Q', "EXECUTE usage_plan('open')\x00");
    try frame(&input.writer, 'X', "");
    var mock: Mock = .{ .expected_text_parameter = "open" };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expect(mock.saw_text_parameter);
    try std.testing.expectEqual(@as(usize, 1), mock.executions);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, output.written(), "26000"));
    try std.testing.expectEqual(@as(usize, 1), mock.disconnects);
}

test "pgwire extended typed bind describes without execution and resumes once" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "q", "SELECT $1", true);
    try frame(&input.writer, 'D', "Sq\x00");
    try bind(&input.writer, "p", "q", 9007199254740993);
    // Portals own their statement/description independently of Close Statement.
    try frame(&input.writer, 'C', "Sq\x00");
    try frame(&input.writer, 'D', "Pp\x00");
    try execute(&input.writer, "p", 1);
    try execute(&input.writer, "p", 1);
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'X', "");
    var mock = Mock{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.describes);
    try std.testing.expectEqual(@as(usize, 1), mock.executions);
    try std.testing.expect(mock.saw_statement_unchanged);
    try std.testing.expect(mock.saw_binding_guard);
    try std.testing.expectEqual(@as(i64, 9007199254740993), mock.seen_parameter.?);
    const observed_tags = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed_tags);
    try std.testing.expect(std.mem.endsWith(u8, observed_tags, "1tT23TDsDCZ"));
    try std.testing.expectEqual(@as(usize, 1), mock.releases);
    try std.testing.expectEqual(@as(usize, 1), mock.disconnects);
}

test "pgwire failed extended pipeline ignores queued execute until sync" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try execute(&input.writer, "missing", 0);
    try frame(&input.writer, 'Q', "SELECT must_not_run\x00");
    try frame(&input.writer, 'H', "");
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'Q', "SELECT 1\x00");
    try frame(&input.writer, 'X', "");
    var mock = Mock{};
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.executions);
    const observed_tags = try tags(std.testing.allocator, output.written());
    defer std.testing.allocator.free(observed_tags);
    try std.testing.expect(std.mem.endsWith(u8, observed_tags, "EZTDDCZ"));
}

test "pgwire authentication fails before resolution and execution" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "q", "SELECT secret", false);
    try frame(&input.writer, 'Q', "SELECT secret\x00");
    var mock = Mock{ .fail_auth = true };
    var output = try run(&mock, input.written(), .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 0), mock.describes);
    try std.testing.expectEqual(@as(usize, 0), mock.executions);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "28P01") != null);
}

test "pgwire prepared bound rejects growth and unnamed replacement reclaims" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "", "SELECT 1", false);
    try parse(&input.writer, "", "SELECT 2", false);
    try parse(&input.writer, "overflow", "SELECT 3", false);
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'X', "");
    var mock = Mock{};
    var output = try run(&mock, input.written(), .{ .prepared_statements = 1 });
    defer output.deinit();
    try std.testing.expectEqual(@as(usize, 2), mock.describes);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "54000") != null);
}

test "pgwire malformed framing is bounded before payload allocation" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try input.writer.writeInt(u32, std.math.maxInt(u32), .big);
    var mock = Mock{};
    try std.testing.expectError(error.ProtocolViolation, run(&mock, input.written(), .{}));
}

fn allocationFailureTranscript(alloc: std.mem.Allocator, input: []const u8) !void {
    var mock = Mock{};
    var reader = std.Io.Reader.fixed(input);
    var buffer: [65536]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    var session = protocol.Session{ .alloc = alloc, .io = std.testing.io, .source = mock.source(), .reader = &reader, .writer = &writer };
    defer session.deinit();
    try session.run();
}

test "pgwire allocation failures release prepared portal and result owners" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    try parse(&input.writer, "q", "SELECT $1", true);
    try bind(&input.writer, "p", "q", 9007199254740993);
    try frame(&input.writer, 'D', "Pp\x00");
    try execute(&input.writer, "p", 1);
    try execute(&input.writer, "p", 1);
    try frame(&input.writer, 'C', "Pp\x00");
    try frame(&input.writer, 'C', "Sq\x00");
    try frame(&input.writer, 'S', "");
    try frame(&input.writer, 'X', "");
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureTranscript, .{input.written()});
}

test "pgwire startup rejects unchecked options before authentication" {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    const body = "user\x00tester\x00options\x00-c search_path=secret\x00\x00";
    try input.writer.writeInt(u32, @intCast(body.len + 8), .big);
    try input.writer.writeInt(u32, 196608, .big);
    try input.writer.writeAll(body);
    var mock = Mock{};
    try std.testing.expectError(error.UnsupportedStartupOption, run(&mock, input.written(), .{}));
    try std.testing.expectEqual(@as(usize, 0), mock.authentications);
}

test "pgwire listener validates protected transport before accepting connections" {
    const server = @import("server.zig");
    var mock = Mock{};
    try std.testing.expectError(error.PgwireRequiresProtectedTransport, server.start(std.testing.allocator, .{ .io = std.testing.io, .backend = mock.source(), .bind_host = "0.0.0.0", .bind_port = 0 }));
}

fn clientStartup(stream: std.Io.net.Stream, reader: *std.Io.Reader) !struct { pid: i32, key: i32 } {
    var input = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer input.deinit();
    try startup(&input.writer);
    var writer = stream.writer(std.testing.io, &.{});
    try writer.interface.writeAll(input.written());
    try writer.interface.flush();
    var key: struct { pid: i32, key: i32 } = .{ .pid = 0, .key = 0 };
    while (true) {
        const tag = try reader.takeByte();
        const len = try reader.takeInt(u32, .big);
        const payload = try reader.readAlloc(std.testing.allocator, len - 4);
        defer std.testing.allocator.free(payload);
        if (tag == 'K') key = .{ .pid = std.mem.readInt(i32, payload[0..4], .big), .key = std.mem.readInt(i32, payload[4..8], .big) };
        if (tag == 'Z') return .{ .pid = key.pid, .key = key.key };
    }
}

fn sendQuery(stream: std.Io.net.Stream) !void {
    var writer = stream.writer(std.testing.io, &.{});
    try frame(&writer.interface, 'Q', "SELECT wait\x00");
    try writer.interface.flush();
}

test "pgwire structured shutdown cancels and joins active backend before releasing identity" {
    const server = @import("server.zig");
    var entered: std.Io.Event = .unset;
    var mock = Mock{ .blocked = true, .entered = &entered };
    var listener = try server.start(std.testing.allocator, .{ .io = std.testing.io, .backend = mock.source(), .bind_port = 0 });
    defer listener.deinit();
    const stream = try listener.address().connect(std.testing.io, .{ .mode = .stream });
    defer stream.close(std.testing.io);
    var buffer: [1024]u8 = undefined;
    var reader = stream.reader(std.testing.io, &buffer);
    _ = try clientStartup(stream, &reader.interface);
    try sendQuery(stream);
    try entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
    listener.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.disconnects);
    try std.testing.expectEqual(@as(usize, 1), mock.releases);
}

fn sendCancel(address: std.Io.net.IpAddress, pid: i32, secret: i32) !void {
    const stream = try address.connect(std.testing.io, .{ .mode = .stream });
    defer stream.close(std.testing.io);
    var writer = stream.writer(std.testing.io, &.{});
    try writer.interface.writeInt(u32, 16, .big);
    try writer.interface.writeInt(u32, 80877102, .big);
    try writer.interface.writeInt(i32, pid, .big);
    try writer.interface.writeInt(i32, secret, .big);
    try writer.interface.flush();
}

test "pgwire separate cancel authenticates secret and works at connection capacity" {
    const server = @import("server.zig");
    var entered: std.Io.Event = .unset;
    var mock = Mock{ .blocked = true, .entered = &entered };
    var listener = try server.start(std.testing.allocator, .{ .io = std.testing.io, .backend = mock.source(), .bind_port = 0, .max_connections = 1 });
    defer listener.deinit();
    const stream = try listener.address().connect(std.testing.io, .{ .mode = .stream });
    defer stream.close(std.testing.io);
    var buffer: [1024]u8 = undefined;
    var reader = stream.reader(std.testing.io, &buffer);
    const key = try clientStartup(stream, &reader.interface);
    try sendQuery(stream);
    try entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
    try sendCancel(listener.address(), key.pid, key.key ^ 1);
    try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    try std.testing.expect(!mock.canceled.load(.acquire));
    try sendCancel(listener.address(), key.pid, key.key);
    try std.testing.expectEqual(@as(u8, 'E'), try reader.interface.takeByte());
    const len = try reader.interface.takeInt(u32, .big);
    const payload = try reader.interface.readAlloc(std.testing.allocator, len - 4);
    defer std.testing.allocator.free(payload);
    try std.testing.expect(std.mem.indexOf(u8, payload, "57014") != null);
    try std.testing.expect(mock.canceled.load(.acquire));
}
