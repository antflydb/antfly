// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Resource-scoped operator commands. Execution and durable job ownership stay
//! in the existing public repair and index APIs.
const std = @import("std");
const client_mod = @import("antfly-client");
const types = client_mod.types;
const cli = @import("mod.zig");

const commands = @import("../../maintenance_commands.zig");
pub const Resource = commands.Resource;
const Action = commands.Action;

const Options = struct {
    resource: Resource,
    action: Action = .issues,
    table: []const u8 = "",
    index: ?[]const u8 = null,
    metric: ?[]const u8 = null,
    job: ?[]const u8 = null,
    kind: ?types.ArtifactRepairKind = null,
    cursor: ?[]const u8 = null,
    repair_id: ?[]const u8 = null,
    limit: ?i64 = null,
    once: bool = false,

    fn target(self: Options) types.RepairTarget {
        return if (self.resource == .index) .index else .artifact;
    }

    fn repairRequest(self: Options) types.RepairRunRequest {
        return .{
            .target = self.target(),
            .index = self.index,
            .kind = self.kind,
            .cursor = self.cursor,
            .limit = self.limit,
            .force = if (self.action == .rebuild) true else null,
            .control = switch (self.action) {
                .pause => "pause_automatic",
                .@"resume" => "resume_automatic",
                .cancel => "cancel_current_attempt",
                else => null,
            },
            .repair_id = self.repair_id,
        };
    }

    fn jobRequest(self: Options) types.TableRepairJobStartRequest {
        const request = self.repairRequest();
        return .{ .target = request.target, .index = request.index, .kind = request.kind, .cursor = request.cursor, .limit = request.limit, .force = request.force, .advance = true };
    }

    fn controlJobRequest(self: Options) types.TableRepairControlJobStartRequest {
        const request = self.repairRequest();
        return .{ .index = request.index.?, .control = request.control.?, .repair_id = request.repair_id, .cursor = request.cursor, .limit = request.limit, .advance = true };
    }
};

fn take(args: anytype, slot: *?[]const u8) !void {
    if (slot.* != null) return error.DuplicateOption;
    const value = args.next() orelse return error.MissingOptionValue;
    if (value.len == 0 or std.mem.startsWith(u8, value, "-")) return error.MissingOptionValue;
    slot.* = value;
}

fn parse(resource: Resource, args: anytype) !Options {
    var result = Options{ .resource = resource };
    var table: ?[]const u8 = null;
    var kind: ?[]const u8 = null;
    var limit: ?[]const u8 = null;
    var action: ?Action = null;
    var namespace_seen = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "maintenance")) {
            if (namespace_seen) return error.DuplicateOption;
            namespace_seen = true;
        } else if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            try take(args, &table);
        } else if (std.mem.eql(u8, arg, "--index") or std.mem.eql(u8, arg, "-i")) {
            try take(args, &result.index);
        } else if (std.mem.eql(u8, arg, "--metric")) {
            try take(args, &result.metric);
        } else if (std.mem.eql(u8, arg, "--job")) {
            try take(args, &result.job);
        } else if (std.mem.eql(u8, arg, "--kind")) {
            try take(args, &kind);
        } else if (std.mem.eql(u8, arg, "--cursor")) {
            try take(args, &result.cursor);
        } else if (std.mem.eql(u8, arg, "--repair-id")) {
            try take(args, &result.repair_id);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            try take(args, &limit);
        } else if (std.mem.eql(u8, arg, "--once")) {
            if (result.once) return error.DuplicateOption;
            result.once = true;
        } else {
            if (action != null) return error.UnexpectedArgument;
            action = std.meta.stringToEnum(Action, arg) orelse return error.UnknownMaintenanceAction;
        }
    }
    result.table = table orelse return error.TableRequired;
    result.action = action orelse .issues;
    if (kind) |value| result.kind = std.meta.stringToEnum(types.ArtifactRepairKind, value) orelse return error.InvalidArtifactKind;
    if (limit) |value| {
        result.limit = std.fmt.parseInt(i64, value, 10) catch return error.InvalidLimit;
        const maximum: i64 = if (result.action == .issues) 500 else 1000;
        if (result.limit.? < 1 or result.limit.? > maximum) return error.InvalidLimit;
    }
    if (resource == .artifact and (result.metric != null or result.repair_id != null)) return error.UnsupportedMaintenanceAction;
    if (resource == .index and result.kind != null) return error.UnsupportedMaintenanceAction;
    if (result.job != null) {
        if (result.action != .status and result.action != .advance and result.action != .cancel) return error.UnsupportedMaintenanceAction;
        if (result.index != null or result.metric != null or result.kind != null or result.cursor != null or result.limit != null or result.once or result.repair_id != null) return error.UnexpectedArgument;
        return result;
    }
    if (result.metric != null) {
        if (result.index == null) return error.IndexRequired;
        switch (result.action) {
            .refresh, .rebuild, .pause, .@"resume", .delete => {},
            else => return error.UnsupportedMaintenanceAction,
        }
        if (result.cursor != null or result.limit != null or result.once or result.repair_id != null) return error.UnexpectedArgument;
        return result;
    }
    switch (result.action) {
        .issues, .repair => {},
        .rebuild, .pause, .@"resume", .cancel, .status => if (resource != .index) return error.UnsupportedMaintenanceAction,
        else => return error.UnsupportedMaintenanceAction,
    }
    if (resource == .index and result.action != .issues and result.index == null) return error.IndexRequired;
    const control = result.action == .pause or result.action == .@"resume" or result.action == .cancel;
    if (result.once and result.action != .repair and result.action != .rebuild and !control) return error.UnexpectedArgument;
    if (result.repair_id) |raw| _ = std.fmt.parseInt(u128, raw, 10) catch return error.InvalidRepairId;
    if (result.repair_id != null and !control) return error.UnexpectedArgument;
    if ((result.action == .status) and (result.cursor != null or result.limit != null)) return error.UnexpectedArgument;
    return result;
}

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, resource: Resource, args: *std.process.Args.Iterator) !void {
    const options = parse(resource, args) catch |err| cli.fatal("invalid {s} maintenance arguments: {s}; run {s} --help", .{ @tagName(resource), @errorName(err), @tagName(resource) });
    if (options.job) |job| {
        var response = switch (options.action) {
            .status => try client.getTableRepairJob(options.table, job),
            .advance => try client.advanceTableRepairJob(options.table, job),
            .cancel => try client.cancelTableRepairJob(options.table, job),
            else => unreachable,
        };
        defer response.deinit();
        return cli.printResponse(allocator, io, &response);
    }
    if (options.metric) |metric| {
        var response = try client.executeGraphMetricAction(options.table, options.index.?, metric, @tagName(options.action));
        defer response.deinit();
        return cli.printResponse(allocator, io, &response);
    }
    switch (options.action) {
        .issues => {
            var response = try client.listTableRepairIssues(options.table, .{ .target = options.target(), .index = options.index, .kind = options.kind, .cursor = options.cursor, .limit = options.limit });
            defer response.deinit();
            return cli.printResponse(allocator, io, &response);
        },
        .status => {
            var response = try client.getIndex(options.table, options.index.?);
            defer response.deinit();
            return cli.printResponse(allocator, io, &response);
        },
        .repair, .rebuild, .pause, .@"resume", .cancel => if (!options.once) {
            var response = if (options.repairRequest().control != null)
                try client.startTableRepairControlJob(options.table, options.controlJobRequest())
            else
                try client.startTableRepairJob(options.table, options.jobRequest());
            defer response.deinit();
            return cli.printResponse(allocator, io, &response);
        },
        else => {},
    }
    var response = try client.runTableRepair(options.table, options.repairRequest());
    defer response.deinit();
    return cli.printResponse(allocator, io, &response);
}

fn parseText(resource: Resource, text: []const u8) !Options {
    var args = std.mem.tokenizeScalar(u8, text, ' ');
    return parse(resource, &args);
}

test "maintenance routes named index rebuild to a durable forced repair job" {
    const options = try parseText(.index, "--table docs maintenance rebuild --index dense");
    const request = options.jobRequest();
    try std.testing.expectEqual(types.RepairTarget.index, request.target.?);
    try std.testing.expectEqualStrings("dense", request.index.?);
    try std.testing.expect(request.force.? and request.advance.?);
    try std.testing.expect(!options.once);
}

test "maintenance keeps graph actions and artifact repair capabilities distinct" {
    const graph = try parseText(.index, "maintenance refresh --table docs --index graph --metric rank");
    try std.testing.expectEqual(Action.refresh, graph.action);
    try std.testing.expectEqualStrings("rank", graph.metric.?);
    const artifact = try parseText(.artifact, "repair --table docs --cursor next --limit 12 --once");
    try std.testing.expectEqual(types.RepairTarget.artifact, artifact.repairRequest().target.?);
    try std.testing.expectEqualStrings("next", artifact.repairRequest().cursor.?);
    try std.testing.expectEqual(@as(i64, 12), artifact.repairRequest().limit.?);
    try std.testing.expectError(error.UnsupportedMaintenanceAction, parseText(.artifact, "rebuild --table docs"));
    try std.testing.expectError(error.UnsupportedMaintenanceAction, parseText(.index, "repair --table docs --index graph --metric rank"));
    try std.testing.expectError(error.UnexpectedArgument, parseText(.index, "refresh --table docs --index graph --metric rank --once"));
}

test "maintenance rejects ambiguous scopes invalid bounds and ignored options" {
    try std.testing.expectError(error.DuplicateOption, parseText(.index, "issues --table a --table b"));
    try std.testing.expectError(error.MissingOptionValue, parseText(.index, "issues --table --index x"));
    try std.testing.expectError(error.IndexRequired, parseText(.index, "repair --table docs"));
    try std.testing.expectError(error.InvalidLimit, parseText(.artifact, "issues --table docs --limit 501"));
    try std.testing.expectError(error.InvalidLimit, parseText(.artifact, "repair --table docs --limit 0"));
    try std.testing.expectError(error.UnexpectedArgument, parseText(.index, "status --table docs --job j --index x"));
    const continued = try parseText(.index, "pause --table docs --index x --cursor 65: --limit 4 --once");
    try std.testing.expectEqualStrings("65:", continued.repairRequest().cursor.?);
    try std.testing.expectEqual(@as(i64, 4), continued.repairRequest().limit.?);
    const control = try parseText(.index, "pause --table docs --index x --repair-id 17");
    try std.testing.expectEqualStrings("pause_automatic", control.repairRequest().control.?);
    try std.testing.expectEqualStrings("17", control.controlJobRequest().repair_id.?);
    try std.testing.expectEqualStrings("pause_automatic", control.controlJobRequest().control);
}

test "maintenance public routes send scoped API requests through the client" {
    const httpx = @import("httpx");
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Case = struct {
        resource: Resource,
        argv: []const [*:0]const u8,
        path: []const u8,
    };
    const cases = [_]Case{
        .{ .resource = .index, .argv = &.{ "maintenance", "rebuild", "--table", "docs", "--index", "dense" }, .path = "/db/v1/tables/docs/repair/jobs" },
        .{ .resource = .artifact, .argv = &.{ "maintenance", "repair", "--table", "docs", "--once", "--cursor", "next", "--limit", "12" }, .path = "/db/v1/tables/docs/repair/run" },
        .{ .resource = .index, .argv = &.{ "maintenance", "refresh", "--table", "docs", "--index", "graph", "--metric", "rank" }, .path = "/db/v1/tables/docs/indexes/graph/graph-metrics/rank:refresh" },
        .{ .resource = .index, .argv = &.{ "maintenance", "pause", "--table", "docs", "--index", "dense", "--repair-id", "17", "--cursor", "65:", "--limit", "4" }, .path = "/db/v1/tables/docs/repair/control-jobs" },
        .{ .resource = .index, .argv = &.{ "maintenance", "advance", "--table", "docs", "--job", "17" }, .path = "/db/v1/tables/docs/repair/jobs/17/advance" },
    };
    const Task = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            if (std.mem.indexOf(u8, info.body, "pause_automatic") != null) {
                try @import("antfly-json").testing.expectSubsetJsonText(std.testing.allocator,
                    \\{"index":"dense","control":"pause_automatic","repair_id":"17","cursor":"65:","limit":4,"advance":true}
                , info.body);
            } else if (std.mem.endsWith(u8, info.path, "/repair/jobs")) {
                try @import("antfly-json").testing.expectSubsetJsonText(std.testing.allocator,
                    \\{"target":"index","index":"dense","force":true,"advance":true}
                , info.body);
            } else if (std.mem.endsWith(u8, info.path, "/repair/run")) {
                try @import("antfly-json").testing.expectSubsetJsonText(std.testing.allocator,
                    \\{"target":"artifact","cursor":"next","limit":12}
                , info.body);
            }
        }
        fn client(test_io: std.Io, c: *client_mod.AntflyClient, case: Case, success: *bool) std.Io.Cancelable!void {
            var args = std.process.Args.Iterator.init(.{ .vector = case.argv });
            switch (case.resource) {
                .index => @import("index.zig").run(std.testing.allocator, test_io, c, &args) catch return,
                .artifact => @import("artifact.zig").run(std.testing.allocator, test_io, c, &args) catch return,
            }
            success.* = true;
        }
    };
    for (cases) |case| {
        var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = .POST, .path = case.path, .respond = .{ .status = 204, .body = "" }, .assert_request = Task.request }});
        defer server.deinit();
        var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        var success = false;
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        try group.concurrent(io, Task.client, .{ io, &client, case, &success });
        try server.handleOne();
        try group.await(io);
        try std.testing.expect(success);
    }
}

test "maintenance repair job responses retain handles and cursors and reject malformed success" {
    const body =
        \\{
        \\  "job_id": 17,
        \\  "attempt_id": 2,
        \\  "table_name": "docs",
        \\  "phase": "queued",
        \\  "repair_status": "in_progress",
        \\  "target": "index",
        \\  "index": "dense",
        \\  "control": "pause_automatic",
        \\  "repair_id": "91",
        \\  "cursor": "65:",
        \\  "limit": 64,
        \\  "force": false,
        \\  "result": {
        \\    "scanned": 0,
        \\    "groups_scanned": 0,
        \\    "reprocessed": 0,
        \\    "repaired": 0,
        \\    "missing_source_docs": 0,
        \\    "failed": 0,
        \\    "unsupported": 0,
        \\    "unresolved": 0,
        \\    "in_progress": 0,
        \\    "indexes_rebuilt": 0,
        \\    "indexes_degraded_before": 0,
        \\    "indexes_degraded_after": 0,
        \\    "controls_applied": 0,
        \\    "limit": 0,
        \\    "has_more": true,
        \\    "debt_remaining": true,
        \\    "next_cursor": "65:",
        \\    "future_result_field": true
        \\  },
        \\  "cancel_requested": false,
        \\  "next_retry_at_millis": 5000,
        \\  "created_at_millis": 1000,
        \\  "last_updated_at_millis": 2000,
        \\  "expires_at_millis": 9000,
        \\  "future_job_field": true
        \\}
    ;
    const httpx = @import("httpx");
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Case = struct { status: u16, body: []const u8, invalid: bool = false, control: bool = false };
    const Task = struct {
        fn run(c: *client_mod.AntflyClient, case: Case, failure: *?anyerror) std.Io.Cancelable!void {
            check(c, case) catch |err| {
                failure.* = err;
            };
        }
        fn check(c: *client_mod.AntflyClient, case: Case) !void {
            const fetched = if (case.control) c.startTableRepairControlJob("docs", .{ .index = "dense", .control = "pause_automatic" }) else if (case.status == 202) c.startTableRepairJob("docs", .{ .target = .index, .index = "dense" }) else c.getTableRepairJob("docs", "17");
            if (case.invalid) {
                try std.testing.expectError(error.InvalidApiResponse, fetched);
                return;
            }
            var response = try fetched;
            defer response.deinit();
            const data = response.data orelse return error.MissingJobResponse;
            try std.testing.expectEqual(@as(i64, 17), data.value.job_id);
            try std.testing.expectEqualStrings("65:", data.value.cursor.value);
            try std.testing.expectEqual(@as(i64, 5000), data.value.next_retry_at_millis.?);
            try std.testing.expectEqualStrings("91", data.value.repair_id.?);
            try std.testing.expectEqualStrings("65:", data.value.result.next_cursor.value);
        }
    };
    for ([_]Case{
        .{ .status = 200, .body = body },                  .{ .status = 202, .body = body },
        .{ .status = 200, .body = "{", .invalid = true },  .{ .status = 202, .body = "{}", .invalid = true },
        .{ .status = 200, .body = "", .invalid = true },   .{ .status = 200, .body = body, .control = true },
        .{ .status = 202, .body = body, .control = true },
    }) |case| {
        var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = if (case.control or case.status == 202) .POST else .GET, .path = if (case.control) "/db/v1/tables/docs/repair/control-jobs" else if (case.status == 202) "/db/v1/tables/docs/repair/jobs" else "/db/v1/tables/docs/repair/jobs/17", .respond = .{ .status = case.status, .body = case.body } }});
        defer server.deinit();
        var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        var failure: ?anyerror = null;
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        try group.concurrent(io, Task.run, .{ &client, case, &failure });
        try server.handleOne();
        try group.await(io);
        if (failure) |err| return err;
    }
}

test "maintenance help and nested completions share action descriptions" {
    const completion = @import("../../completion.zig");
    inline for (.{ Resource.index, Resource.artifact }) |resource| {
        const usage = commands.usage(resource);
        try std.testing.expect(std.mem.indexOf(u8, usage, "--cursor") != null);
        inline for (commands.actions) |description| {
            if (resource == .index or description.artifact) try std.testing.expect(std.mem.indexOf(u8, usage, description.text(resource)) != null);
        }
    }
    inline for (.{ completion.Shell.bash, completion.Shell.zsh, completion.Shell.fish }) |shell| {
        var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer out.deinit();
        try completion.write(shell, &out.writer);
        try std.testing.expect(std.mem.indexOf(u8, out.written(), "maintenance") != null);
        try std.testing.expect(std.mem.indexOf(u8, out.written(), "refresh") != null);
        try std.testing.expect(std.mem.indexOf(u8, out.written(), "advance") != null);
        try std.testing.expect(std.mem.indexOf(u8, out.written(), "__maintenance-worker") == null);
    }
}
