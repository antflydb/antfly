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
const recovery_replay = @import("index_maintenance_replay.zig");

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
    recovery_file: ?[]const u8 = null,
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
        } else if (std.mem.eql(u8, arg, "--recovery-file")) {
            try take(args, &result.recovery_file);
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
    if (result.action == .replay or result.recovery_file != null) {
        if (resource != .index or result.action != .replay) return error.UnsupportedMaintenanceAction;
        if (result.index == null) return error.IndexRequired;
        if (result.recovery_file == null) return error.RecoveryFileRequired;
        if (kind != null or limit != null or result.job != null or result.metric != null or
            result.cursor != null or result.repair_id != null or result.once) return error.UnexpectedArgument;
        return result;
    }
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
        .retry, .rebuild, .pause, .@"resume", .cancel, .status => if (resource != .index) return error.UnsupportedMaintenanceAction,
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
    if (options.action == .replay) return runReplay(allocator, io, client, options);
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
    if (options.resource == .index and options.index != null and options.action != .status) {
        var response = try client.getIndex(options.table, options.index.?);
        defer response.deinit();
        const observed = response.data orelse return error.InvalidIndexStatus;
        if (observed.value.status == .relational_index_stats)
            return runRelational(allocator, io, client, options, observed.value.status.relational_index_stats.relational_index);
        if (options.action == .retry) return error.UnsupportedMaintenanceAction;
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

const RecoveryLog = struct {
    file: std.Io.File,
    path: []u8,
    alloc: std.mem.Allocator,
    offset: u64 = 0,
    failed: bool = false,

    fn init(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !RecoveryLog {
        if (!@hasDecl(std.Io.File.Permissions, "fromMode")) return error.PrivateRecoveryFileUnsupported;
        var nonce: [16]u8 = undefined;
        try io.randomSecure(&nonce);
        const hex = std.fmt.bytesToHex(nonce, .lower);
        const path = try std.fmt.allocPrint(allocator, ".antfly-index-maintenance-{s}.jsonl", .{hex});
        errdefer allocator.free(path);
        var file = try dir.createFile(io, path, .{ .exclusive = true, .permissions = if (@hasDecl(std.Io.File.Permissions, "fromMode")) .fromMode(0o600) else .default_file });
        errdefer file.close(io);
        try @import("../../common/fs_paths.zig").syncDirectoryHandlePortable(io, dir);
        return .{ .file = file, .path = path, .alloc = allocator };
    }

    fn append(self: *RecoveryLog, io: std.Io, envelope: anytype) !void {
        if (self.failed) return error.RecoveryLogFailed;
        errdefer self.failed = true;
        const encoded = try std.json.Stringify.valueAlloc(self.alloc, envelope, .{});
        defer self.alloc.free(encoded);
        var buffer: [4096]u8 = undefined;
        var writer = self.file.writer(io, &buffer);
        writer.pos = self.offset;
        try writer.interface.writeAll(encoded);
        try writer.interface.writeByte('\n');
        try writer.end();
        try self.file.sync(io);
        self.offset = writer.pos;
    }

    fn deinit(self: *RecoveryLog, io: std.Io) void {
        self.file.close(io);
        self.alloc.free(self.path);
    }
};

test "maintenance recovery artifact preserves synced lines and restrictive permissions" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    var log = try RecoveryLog.init(alloc, io, dir.dir);
    defer log.deinit(io);
    try log.append(io, .{ .request = "first" });
    const first_offset = log.offset;
    try log.append(io, .{ .request = "second" });
    try std.testing.expect(log.offset > first_offset);
    const bytes = try dir.dir.readFileAlloc(io, log.path, alloc, .limited(1024));
    defer alloc.free(bytes);
    try std.testing.expectEqualStrings("{\"request\":\"first\"}\n{\"request\":\"second\"}\n", bytes);
    if (@import("builtin").os.tag != .windows) try std.testing.expectEqual(@as(u32, 0o600), @as(u32, @intCast((try log.file.stat(io)).permissions.toMode() & 0o777)));
    try std.testing.expectError(error.PathAlreadyExists, dir.dir.createFile(io, log.path, .{ .exclusive = true }));
    log.failed = true;
    try std.testing.expectError(error.RecoveryLogFailed, log.append(io, .{ .request = "ignored" }));
}

fn runRelational(allocator: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, options: Options, status: types.RelationalIndexStatus) !void {
    switch (options.action) {
        .retry, .repair, .rebuild => {},
        else => return error.UnsupportedMaintenanceAction,
    }
    if (options.once or options.limit != null or options.cursor != null or options.repair_id != null) return error.UnsupportedMaintenanceAction;
    if (!std.mem.eql(u8, status.index_name, options.index.?)) return error.InvalidIndexStatus;
    var test_directory = if (@import("builtin").is_test) std.testing.tmpDir(.{}) else {};
    defer if (@import("builtin").is_test) test_directory.cleanup();
    var recovery: ?RecoveryLog = null;
    defer if (recovery) |*log| log.deinit(io);
    var proofs: [128]types.IndexMaintenanceOwnerProof = undefined;
    var count: usize = 0;
    var submitted: usize = 0;
    for (status.ranges, 0..) |range, offset| {
        const eligible = if (options.action == .retry) range.state == .failed else range.state != .building;
        if (eligible) {
            proofs[count] = .{ .group_id = range.group_id, .generation = range.generation, .slot = range.slot, .owner = range.owner, .comparison = range.comparison, .progress_digest = range.progress_digest, .maintenance_epoch = range.maintenance_epoch };
            count += 1;
        }
        if (count == proofs.len or (offset + 1 == status.ranges.len and count != 0)) {
            const request = types.IndexMaintenanceRequest{ .table_id = status.table_id, .schema_version = status.schema_version, .owners = proofs[0..count] };
            // Preserve the exact observation before any external mutation. A
            // later conflict/lost acknowledgement may leave partial admission;
            // operators can replay this request instead of inventing new proof.
            if (recovery == null) {
                recovery = try RecoveryLog.init(allocator, io, if (@import("builtin").is_test) test_directory.dir else std.Io.Dir.cwd());
                try cli.writeJson(allocator, io, .{ .recovery_file = recovery.?.path, .recovery_instructions = "Use antfly index maintenance replay --table <table> --index <index> --recovery-file <path> with normal authentication. Admission may be partial; requests are replayed unchanged and this file is retained on success and failure." });
            }
            try recovery.?.append(io, .{ .table = options.table, .index = options.index.?, .action = if (options.action == .retry) "retry" else "repair", .request = request });
            var response = if (options.action == .retry) try client.retryIndex(options.table, options.index.?, request) else try client.repairIndex(options.table, options.index.?, request);
            defer response.deinit();
            const acknowledged = response.data orelse return error.InvalidMaintenanceResponse;
            try recovery_replay.validateAcknowledgement(request, acknowledged.value);
            try cli.printResponse(allocator, io, &response);
            submitted += count;
            count = 0;
        }
    }
    if (submitted == 0) return error.NoEligibleMaintenanceOwners;
}

fn runReplay(allocator: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, options: Options) !void {
    var plan = blk: {
        const bytes = try cli.readFileAlloc(io, allocator, options.recovery_file.?, recovery_replay.max_file_bytes);
        defer allocator.free(bytes);
        break :blk try recovery_replay.Plan.parse(allocator, bytes, options.table, options.index.?);
    };
    defer plan.deinit();
    try replayPlan(allocator, io, client, plan);
}

fn replayPlan(allocator: std.mem.Allocator, io: std.Io, client: *client_mod.AntflyClient, plan: recovery_replay.Plan) !void {
    var acknowledged_owners: usize = 0;
    for (plan.batches) |batch| {
        var response = switch (batch.action) {
            .retry => try client.retryIndex(batch.table, batch.index, batch.request),
            .repair => try client.repairIndex(batch.table, batch.index, batch.request),
        };
        defer response.deinit();
        const acknowledged = response.data orelse return error.InvalidMaintenanceResponse;
        try recovery_replay.validateAcknowledgement(batch.request, acknowledged.value);
        acknowledged_owners += batch.request.owners.len;
    }
    try cli.writeJson(allocator, io, .{ .acknowledged_owners = acknowledged_owners, .replayed_batches = plan.batches.len, .ignored_torn_tail_bytes = plan.ignored_tail_bytes });
}

test "maintenance recovery replay requires an explicit target and rejects mixed actions" {
    _ = recovery_replay;
    const parsed = try parseText(.index, "maintenance replay --table rows --index by_id --recovery-file recovery.jsonl");
    try std.testing.expectEqual(.replay, parsed.action);
    try std.testing.expectEqualStrings("recovery.jsonl", parsed.recovery_file.?);
    try std.testing.expectError(error.IndexRequired, parseText(.index, "maintenance replay --table rows --recovery-file recovery.jsonl"));
    try std.testing.expectError(error.UnexpectedArgument, parseText(.index, "maintenance replay --table rows --index by_id --recovery-file recovery.jsonl --metric degree"));
    try std.testing.expectError(error.UnsupportedMaintenanceAction, parseText(.index, "maintenance repair --table rows --index by_id --recovery-file recovery.jsonl"));
    try std.testing.expectError(error.UnsupportedMaintenanceAction, parseText(.artifact, "maintenance replay --table rows --index by_id --recovery-file recovery.jsonl"));
}

test "maintenance recovery replay sends saved proofs directly and rejects foreign acknowledgements" {
    const httpx = @import("httpx");
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const request: types.IndexMaintenanceRequest = .{ .table_id = "7", .schema_version = 0, .owners = &.{.{ .group_id = "9", .generation = "1", .slot = 0, .owner = "aa" ** 32, .comparison = "bb" ** 32, .progress_digest = "cc" ** 32, .maintenance_epoch = "0" }} };
    const Task = struct {
        fn check(info: httpx.testing_mod.RequestInfo) !void {
            try @import("antfly-json").testing.expectSubsetJsonText(std.testing.allocator,
                \\{"table_id":"7","schema_version":0,"owners":[{"group_id":"9","generation":"1","maintenance_epoch":"0"}]}
            , info.body);
        }
        fn execute(test_io: std.Io, client: *client_mod.AntflyClient, plan: recovery_replay.Plan, result: *?anyerror) std.Io.Cancelable!void {
            replayPlan(std.testing.allocator, test_io, client, plan) catch |err| {
                result.* = err;
            };
        }
    };
    for ([_]bool{ false, true }) |malformed| {
        const line = try std.json.Stringify.valueAlloc(alloc, recovery_replay.Envelope{ .table = "rows", .index = "by_id", .action = .repair, .request = request }, .{});
        defer alloc.free(line);
        const bytes = try std.fmt.allocPrint(alloc, "{s}\n", .{line});
        defer alloc.free(bytes);
        var plan = try recovery_replay.Plan.parse(alloc, bytes, "rows", "by_id");
        defer plan.deinit();
        var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = .POST, .path = "/db/v1/tables/rows/indexes/by_id/repair", .respond = .{ .status = 200, .body = if (malformed) "{\"acknowledged_groups\":[\"10\"]}" else "{\"acknowledged_groups\":[\"9\"]}" }, .assert_request = Task.check }});
        defer server.deinit();
        var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        var result: ?anyerror = null;
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        try group.concurrent(io, Task.execute, .{ io, &client, plan, &result });
        try server.handleOne();
        try group.await(io);
        if (malformed) try std.testing.expectEqual(error.InvalidMaintenanceResponse, result.?) else try std.testing.expect(result == null);
    }
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
        var server = try httpx.TestServer.start(alloc, io, &.{
            .{ .method = .GET, .path = "/db/v1/tables/docs/indexes/dense", .respond = .{ .status = 200, .body = "{\"shard_status\":{},\"config\":{\"name\":\"dense\",\"type\":\"full_text\"},\"status\":{\"index_type\":\"full_text\"}}" } },
            .{ .method = .POST, .path = case.path, .respond = .{ .status = 204, .body = "" }, .assert_request = Task.request },
        });
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
        if (std.mem.eql(u8, case.path, "/db/v1/tables/docs/repair/jobs") or std.mem.eql(u8, case.path, "/db/v1/tables/docs/repair/control-jobs")) try server.handleOne();
        try group.await(io);
        try std.testing.expect(success);
    }
}

test "maintenance relational commands auto detect index type and submit exact proofs" {
    const httpx = @import("httpx");
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const status_json =
        \\{"shard_status":{},"config":{"name":"by_tenant","type":"relational","keys":[{"column":"tenant"}]},"status":{"index_type":"relational","milestones":{"queryable":{"reached":false,"blockers":[]},"complete":{"reached":false,"blockers":[]}},"relational_index":{"table_id":"71","schema_version":3,"index_name":"by_tenant","state":"failed","ranges":[{"group_id":"9","generation":"7","slot":2,"owner":"owner","comparison":"comparison","progress_digest":"progress","maintenance_epoch":"4","state":"failed","rows_scanned":"3"}]}}}
    ;
    const Task = struct {
        fn check(info: httpx.testing_mod.RequestInfo) !void {
            try @import("antfly-json").testing.expectSubsetJsonText(std.testing.allocator,
                \\{"table_id":"71","schema_version":3,"owners":[{"group_id":"9","generation":"7","slot":2,"owner":"owner","comparison":"comparison","progress_digest":"progress","maintenance_epoch":"4"}]}
            , info.body);
        }
        fn execute(test_io: std.Io, client: *client_mod.AntflyClient, verb: [*:0]const u8, result: *?anyerror) std.Io.Cancelable!void {
            var args = std.process.Args.Iterator.init(.{ .vector = &.{ "maintenance", verb, "--table", "docs", "--index", "by_tenant" } });
            run(std.testing.allocator, test_io, client, .index, &args) catch |err| {
                result.* = err;
            };
        }
    };
    for ([_][*:0]const u8{ "retry", "repair", "rebuild", "pause" }) |verb| {
        const is_pause = std.mem.eql(u8, std.mem.span(verb), "pause");
        var server = try httpx.TestServer.start(alloc, io, &.{
            .{ .method = .GET, .path = "/db/v1/tables/docs/indexes/by_tenant", .respond = .{ .status = 200, .body = status_json } },
            .{ .method = .POST, .path = if (std.mem.eql(u8, std.mem.span(verb), "retry")) "/db/v1/tables/docs/indexes/by_tenant/retry" else "/db/v1/tables/docs/indexes/by_tenant/repair", .respond = .{ .status = 200, .body = "{\"acknowledged_groups\":[\"9\"]}" }, .assert_request = Task.check },
        });
        defer server.deinit();
        var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
        defer http.deinit();
        var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        var result: ?anyerror = null;
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        try group.concurrent(io, Task.execute, .{ io, &client, verb, &result });
        try server.handleOne();
        if (!is_pause) try server.handleOne();
        try group.await(io);
        if (is_pause) try std.testing.expectEqual(error.UnsupportedMaintenanceAction, result.?) else try std.testing.expect(result == null);
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
