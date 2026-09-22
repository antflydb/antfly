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
    unknown_outcome: bool = false,
    mutation_outcome: ?backend.MutationOutcome = null,
    owned_results: bool = false,
    result_releases: usize = 0,
    saw_binding_guard: bool = false,
    seen_parameter: ?i64 = null,
    saw_statement_unchanged: bool = false,
    entered: ?*std.Io.Event = null,
    blocked: bool = false,
    stream_rows: usize = 0,
    stream_offset: usize = 0,
    stream_closes: usize = 0,
    stream_pulls: usize = 0,
    stream_fail_at: ?usize = null,
    canceled: std.atomic.Value(bool) = .init(false),

    fn source(self: *Mock) backend.Backend {
        return .{ .context = self, .vtable = &.{ .authenticate = authenticate, .describe = describe, .execute = Mock.execute, .evaluate_parameters = evaluateParameters, .open_stream = openStream, .disconnect = disconnect } };
    }
    fn evaluateParameters(_: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, request: backend.Request, expressions: []const []const u8) ![]const std.json.Value {
        try request.check();
        const result = try alloc.alloc(std.json.Value, expressions.len);
        for (expressions, result) |expression, *value| value.* = .{ .integer = try std.fmt.parseInt(i64, expression, 10) };
        return result;
    }
    fn openStream(raw: *anyopaque, _: std.mem.Allocator, _: backend.Identity, request: backend.Request) !?backend.ReadStream {
        const self: *Mock = @ptrCast(@alignCast(raw));
        if (self.stream_rows == 0) return null;
        try request.check();
        if (request.parameters.len > 0) self.seen_parameter = request.parameters[0].integer;
        self.saw_binding_guard = if (request.binding_guard) |guard| std.mem.eql(u8, guard, "immutable-catalog-binding") else false;
        return .{ .context = self, .columns = &.{.{ .name = "n", .type = .integer }}, .next = nextPage, .close = closeStream };
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
        if (self.json_null_results) return .{ .columns = &.{.{ .name = "j", .type = .json }} };
        return .{ .columns = &.{.{ .name = "n", .type = .integer }}, .parameter_types = if (std.mem.indexOf(u8, request.statement, "$1") != null) &.{.integer} else &.{}, .binding_guard = "immutable-catalog-binding" };
    }
    fn execute(raw: *anyopaque, alloc: std.mem.Allocator, _: backend.Identity, request: backend.Request) !backend.Result {
        const self: *Mock = @ptrCast(@alignCast(raw));
        self.executions += 1;
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
        if (self.ddl_pending) return .{ .command_tag = "DDL PENDING", .mutation_outcome = .committed_pending, .ddl_receipt_json = "{\"table_id\":\"17\",\"schema_version\":8,\"state\":\"pending\"}" };
        if (self.json_null_results) return .{
            .columns = &.{.{ .name = "j", .type = .json }},
            .rows = &.{ &.{.null}, &.{.null} },
            .sql_nulls = &.{ &.{false}, &.{true} },
            .command_tag = "SELECT 2",
        };
        self.saw_statement_unchanged = std.mem.eql(u8, request.statement, "SELECT $1");
        if (request.parameters.len > 0) self.seen_parameter = request.parameters[0].integer;
        const rows = try alloc.alloc([]const std.json.Value, 2);
        rows[0] = try alloc.dupe(std.json.Value, &.{.{ .integer = self.seen_parameter orelse 9007199254740993 }});
        rows[1] = try alloc.dupe(std.json.Value, &.{.{ .integer = 2 }});
        return .{
            .columns = &.{.{ .name = "n", .type = .integer }},
            .rows = rows,
            .command_tag = "SELECT 2",
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
