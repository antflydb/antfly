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

//! One-shot SQL uses the same generated public contract as every SDK. There
//! is no local SQL substitution, transaction emulation, or automatic retry.
const std = @import("std");
const client_mod = @import("antfly-client");
const cli = @import("mod.zig");

const Options = struct {
    statement: ?[]const u8 = null,
    parameters: ?[]const u8 = null,
    database: ?[]const u8 = null,
    namespace: ?[]const u8 = null,
    limit: ?i64 = null,
    interactive: bool = false,
};

fn parse(args: *std.process.Args.Iterator) !Options {
    var options: Options = .{};
    while (args.next()) |flag| {
        if (std.mem.eql(u8, flag, "--interactive")) {
            if (options.interactive) return error.DuplicateSqlOption;
            options.interactive = true;
        } else if (std.mem.eql(u8, flag, "--limit")) {
            if (options.limit != null) return error.DuplicateSqlOption;
            const raw = args.next() orelse return error.MissingSqlOptionValue;
            const limit = std.fmt.parseInt(i64, raw, 10) catch return error.InvalidSqlLimit;
            if (limit < 1 or limit > 4096) return error.InvalidSqlLimit;
            options.limit = limit;
        } else {
            const slot: *?[]const u8 = if (std.mem.eql(u8, flag, "--statement")) &options.statement else if (std.mem.eql(u8, flag, "--parameters")) &options.parameters else if (std.mem.eql(u8, flag, "--database")) &options.database else if (std.mem.eql(u8, flag, "--namespace")) &options.namespace else return error.UnknownSqlOption;
            if (slot.* != null) return error.DuplicateSqlOption;
            slot.* = args.next() orelse return error.MissingSqlOptionValue;
        }
    }
    if (options.interactive) {
        if (options.statement != null) return error.AmbiguousSqlMode;
        return options;
    }
    const statement = options.statement orelse return error.SqlStatementRequired;
    if (std.mem.trim(u8, statement, " \t\r\n").len == 0) return error.SqlStatementRequired;
    const parameter_json: []const u8 = options.parameters orelse "";
    if (statement.len > 1 << 20 or parameter_json.len > 1 << 20) return error.SqlRequestTooLarge;
    return options;
}

pub fn run(alloc: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, args: *std.process.Args.Iterator) !void {
    const options = parse(args) catch |err| {
        std.debug.print("SQL arguments: {s}\n", .{@errorName(err)});
        cli.printCommandUsage("sql");
        return error.InvalidArguments;
    };
    if (options.interactive) return interactive(alloc, io, client, options);
    _ = try execute(alloc, io, client, options, null, null);
}

fn execute(alloc: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, options: Options, session_id: ?[]const u8, session_ended: ?*bool) !?[32]u8 {
    const defaults = cli.CatalogFlags.defaultsFromEnv();
    // JSON numbers stay lexical until server-side column binding. Passing an
    // integer through f64 here would corrupt bigint parameters before SQL ran.
    var parameters = std.json.parseFromSlice([]const std.json.Value, alloc, options.parameters orelse "[]", .{ .parse_numbers = false }) catch {
        std.debug.print("SQL --parameters must be a JSON array\n", .{});
        return error.InvalidArguments;
    };
    defer parameters.deinit();
    if (parameters.value.len > 1024) return error.SqlParameterLimitExceeded;
    var response = try client.executeSQL(.{
        .statement = options.statement.?,
        .parameters = parameters.value,
        .database = options.database orelse defaults.database,
        .namespace = options.namespace orelse defaults.namespace,
        .limit = options.limit,
        .session_id = session_id,
    });
    defer response.deinit();
    if (response.status_code < 200 or response.status_code >= 300) {
        // Preserve the SQLSTATE, transaction receipt and retryability exactly;
        // do not flatten an ambiguous commit into a generic request failure.
        std.debug.print("SQL HTTP {d}: {s}\n", .{ response.status_code, response.err_body orelse "no diagnostic body" });
        if (response.err_body) |body| {
            var diagnostic = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.SqlExecutionFailed;
            defer diagnostic.deinit();
            if (diagnostic.value == .object) {
                const value = diagnostic.value.object.get("error") orelse diagnostic.value;
                if (value == .object) {
                    if (session_ended) |ended| if (value.object.get("transaction_status")) |status| {
                        ended.* = status == .string and std.mem.eql(u8, status.string, "idle");
                    };
                    if (value.object.get("code")) |code| {
                        if (code == .string and std.mem.eql(u8, code.string, "40003")) return error.SqlExecutionUncertain;
                    }
                }
            }
        }
        return error.SqlExecutionFailed;
    }
    const data = response.data orelse return error.InvalidApiResponse;
    try cli.writeJson(alloc, io, data.value);
    if (data.value.session_id) |id| {
        if (id.len != 32) return error.InvalidApiResponse;
        return id[0..32].*;
    }
    return null;
}

fn interactive(alloc: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, defaults: Options) !void {
    // Delimiter reads are bounded by this reusable buffer. Statements are never
    // split on semicolons, substituted, or automatically replayed.
    const buffer = try alloc.alloc(u8, 1 << 20);
    defer alloc.free(buffer);
    var reader = std.Io.File.stdin().readerStreaming(io, buffer);
    var session_id: ?[32]u8 = null;
    std.debug.print("SQL: one statement per line; \\q exits and rolls back an active session. No automatic retries.\n", .{});
    while (true) {
        std.debug.print("sql{s}> ", .{if (session_id != null) "[transaction]" else ""});
        const raw = try reader.interface.takeDelimiter('\n') orelse break;
        const line = std.mem.trim(u8, raw, " \t\r");
        if (std.mem.eql(u8, line, "\\q")) break;
        if (line.len == 0) continue;
        var options = defaults;
        options.statement = line;
        var session_ended = false;
        session_id = execute(alloc, io, client, options, if (session_id) |*id| id else null, &session_ended) catch |err| switch (err) {
            error.SqlExecutionFailed, error.InvalidArguments, error.SqlParameterLimitExceeded => {
                if (session_ended) session_id = null;
                continue;
            },
            else => {
                if (session_id) |id| std.debug.print("Session receipt: {s}\n", .{id});
                std.debug.print("Execution response is unconfirmed; reconcile before submitting another mutation. No command was retried.\n", .{});
                return err;
            },
        };
    }
    if (session_id) |id| {
        var rollback = defaults;
        rollback.statement = "ROLLBACK";
        rollback.parameters = null;
        _ = execute(alloc, io, client, rollback, &id, null) catch |err| {
            std.debug.print("Rollback unconfirmed for session {s}; reconcile through the native transaction API.\n", .{id});
            return err;
        };
    }
}

test "SQL CLI parses scoped typed arguments and rejects ambiguous flags" {
    var argv = [_][*:0]const u8{ "--statement", "SELECT id FROM things WHERE id=$1", "--parameters", "[9007199254740993]", "--database", "tenant", "--namespace", "app", "--limit", "12" };
    var args = std.process.Args.Iterator.init(.{ .vector = &argv });
    const options = try parse(&args);
    try std.testing.expectEqualStrings("tenant", options.database.?);
    try std.testing.expectEqual(@as(?i64, 12), options.limit);
    var parameters = try std.json.parseFromSlice([]const std.json.Value, std.testing.allocator, options.parameters.?, .{ .parse_numbers = false });
    defer parameters.deinit();
    try std.testing.expectEqualStrings("9007199254740993", parameters.value[0].number_string);
    var interactive_argv = [_][*:0]const u8{"--interactive"};
    var interactive_args = std.process.Args.Iterator.init(.{ .vector = &interactive_argv });
    try std.testing.expect((try parse(&interactive_args)).interactive);

    const Case = struct { args: []const [*:0]const u8, err: anyerror };
    for ([_]Case{
        .{ .args = &.{"--statement"}, .err = error.MissingSqlOptionValue },
        .{ .args = &.{ "--statement", " " }, .err = error.SqlStatementRequired },
        .{ .args = &.{ "--statement", "SELECT", "--statement", "DELETE" }, .err = error.DuplicateSqlOption },
        .{ .args = &.{ "--statement", "SELECT", "--limit", "4097" }, .err = error.InvalidSqlLimit },
        .{ .args = &.{ "--statement", "SELECT", "--retry" }, .err = error.UnknownSqlOption },
        .{ .args = &.{ "--interactive", "--statement", "SELECT 1" }, .err = error.AmbiguousSqlMode },
    }) |case| {
        var iterator = std.process.Args.Iterator.init(.{ .vector = case.args });
        try std.testing.expectError(case.err, parse(&iterator));
    }
}

test "SQL CLI executes one generated-contract request without SQL substitution" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const httpx = client_mod.httpx;
    const Check = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            var body = try std.json.parseFromSlice(client_mod.types.SQLRequest, std.testing.allocator, info.body, .{ .parse_numbers = false });
            defer body.deinit();
            try std.testing.expectEqualStrings("SELECT id FROM things WHERE id=$1", body.value.statement);
            try std.testing.expectEqualStrings("9007199254740993", body.value.parameters.?[0].number_string);
            try std.testing.expectEqualStrings("tenant", body.value.database.?);
            try std.testing.expectEqualStrings("app", body.value.namespace.?);
        }
    };
    var server = try httpx.TestServer.start(alloc, io, &.{.{
        .method = .POST,
        .path = "/db/v1/sql",
        .respond = .{ .body = "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\"}],\"rows\":[[\"9007199254740993\"]],\"rows_affected\":0,\"command_tag\":\"SELECT\"}" },
        .assert_request = Check.request,
    }});
    defer server.deinit();
    var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
    defer serving.cancel(io) catch {};
    var http = httpx.Client.initWithConfig(alloc, io, .{});
    defer http.deinit();
    var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();
    var argv = [_][*:0]const u8{ "--statement", "SELECT id FROM things WHERE id=$1", "--parameters", "[9007199254740993]", "--database", "tenant", "--namespace", "app" };
    var args = std.process.Args.Iterator.init(.{ .vector = &argv });
    try run(alloc, io, &client, &args);
    try serving.await(io);
    try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
}

test "SQL CLI clears native ended sessions on conflict without replay" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const httpx = client_mod.httpx;
    var server = try httpx.TestServer.start(alloc, io, &.{.{
        .method = .POST,
        .path = "/db/v1/sql",
        .respond = .{ .status = 409, .body = "{\"code\":\"40001\",\"message\":\"conflict\",\"transaction_status\":\"idle\"}" },
    }});
    defer server.deinit();
    var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
    defer serving.cancel(io) catch {};
    var http = httpx.Client.initWithConfig(alloc, io, .{});
    defer http.deinit();
    var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();
    var ended = false;
    try std.testing.expectError(error.SqlExecutionFailed, execute(alloc, io, &client, .{ .statement = "COMMIT" }, "00112233445566778899aabbccddeeff", &ended));
    try serving.await(io);
    try std.testing.expect(ended);
    try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
}
