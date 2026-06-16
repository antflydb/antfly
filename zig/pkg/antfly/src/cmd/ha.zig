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
const antfly = @import("antfly-zig");

const ha = antfly.ha;

const LocalOptions = struct {
    remote_url: ?[]const u8 = null,
    primary_log: ?[]const u8 = null,
    primary_slots: ?[]const u8 = null,
    standby_log: ?[]const u8 = null,
    standby_progress: ?[]const u8 = null,
    fence_wal: ?[]const u8 = null,
    identity: IdentityOptions = .{},

    fn wantsPrimary(self: LocalOptions) bool {
        return self.primary_log != null or self.primary_slots != null;
    }

    fn wantsStandby(self: LocalOptions) bool {
        return self.standby_log != null or self.standby_progress != null;
    }

    fn primaryIdentity(self: LocalOptions) !ha.standby.Identity {
        if (self.primary_log == null) return error.PrimaryLogMissing;
        if (self.primary_slots == null) return error.PrimarySlotsMissing;
        return try self.identity.finish();
    }

    fn standbyIdentity(self: LocalOptions) !ha.standby.Identity {
        if (self.standby_log == null) return error.StandbyLogMissing;
        if (self.standby_progress == null) return error.StandbyProgressMissing;
        return try self.identity.finish();
    }
};

const IdentityOptions = struct {
    cluster_id: ?u64 = null,
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    timeline_id: ?u64 = null,
    epoch: ?u64 = null,

    fn finish(self: IdentityOptions) !ha.standby.Identity {
        return .{
            .cluster_id = self.cluster_id orelse return error.ClusterIdMissing,
            .shard_id = self.shard_id orelse return error.ShardIdMissing,
            .table_id = self.table_id orelse return error.TableIdMissing,
            .timeline_id = self.timeline_id orelse return error.TimelineIdMissing,
            .epoch = self.epoch orelse return error.EpochMissing,
        };
    }
};

const ParsedArgs = struct {
    options: LocalOptions,
    command_args: []const []const u8,

    fn deinit(self: *ParsedArgs, alloc: std.mem.Allocator) void {
        alloc.free(self.command_args);
        self.* = undefined;
    }
};

pub fn run(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const first = args.next() orelse {
        printUsage(argv0);
        return;
    };
    if (std.mem.eql(u8, first, "--help") or std.mem.eql(u8, first, "-h") or std.mem.eql(u8, first, "help")) {
        printUsage(argv0);
        return;
    }

    var all_args = std.ArrayListUnmanaged([]const u8).empty;
    defer all_args.deinit(init.gpa);
    try all_args.append(init.gpa, first);
    while (args.next()) |arg| try all_args.append(init.gpa, arg);

    try runArgv(init.gpa, init.io, all_args.items);
}

pub fn runArgv(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    var parsed = try parseLocalArgs(alloc, argv);
    defer parsed.deinit(alloc);
    if (parsed.command_args.len == 0) return error.HaCommandMissing;

    if (parsed.options.remote_url) |remote_url| {
        if (parsed.options.wantsPrimary() or parsed.options.wantsStandby() or parsed.options.fence_wal != null) {
            return error.HaRemoteCannotUseLocalHandles;
        }
        var executor = antfly.common.http.StdHttpExecutor.init(alloc, .{});
        defer executor.deinit();
        var client = ha.http_client.Client.init(alloc, executor.executor());
        var rendered = try client.executeCommand(remote_url, parsed.command_args);
        defer rendered.deinit(alloc);
        std.Io.File.stdout().writeStreamingAll(io, rendered.body) catch {};
        std.Io.File.stdout().writeStreamingAll(io, "\n") catch {};
        return;
    }

    var plan = try ha.admin_cli.parse(alloc, parsed.command_args);
    defer plan.deinit(alloc);

    var primary: ?ha.primary.Primary = null;
    defer if (primary) |*handle| handle.close();

    var standby: ?ha.standby.Standby = null;
    defer if (standby) |*handle| handle.close();

    var fence_store: ?ha.fencing.Store = null;
    defer if (fence_store) |*handle| handle.close();

    if (parsed.options.wantsPrimary()) {
        const identity = try parsed.options.primaryIdentity();
        const primary_log = try zPath(alloc, parsed.options.primary_log.?);
        defer alloc.free(primary_log);
        const primary_slots = try zPath(alloc, parsed.options.primary_slots.?);
        defer alloc.free(primary_slots);
        primary = try ha.primary.Primary.open(
            alloc,
            primary_log.ptr,
            primary_slots.ptr,
            identity,
            .{},
        );
    }
    if (parsed.options.wantsStandby()) {
        const identity = try parsed.options.standbyIdentity();
        const standby_log = try zPath(alloc, parsed.options.standby_log.?);
        defer alloc.free(standby_log);
        const standby_progress = try zPath(alloc, parsed.options.standby_progress.?);
        defer alloc.free(standby_progress);
        standby = try ha.standby.Standby.open(
            alloc,
            standby_log.ptr,
            standby_progress.ptr,
            identity,
            .{},
        );
    }
    if (parsed.options.fence_wal) |path| {
        const fence_wal = try zPath(alloc, path);
        defer alloc.free(fence_wal);
        fence_store = try ha.fencing.Store.open(alloc, fence_wal.ptr, .{});
    }

    var rendered = try ha.admin_exec.executeAndRenderAlloc(alloc, .{
        .primary = if (primary) |*handle| handle else null,
        .standby = if (standby) |*handle| handle else null,
        .fence_store = if (fence_store) |*handle| handle else null,
    }, plan);
    defer rendered.deinit(alloc);

    std.Io.File.stdout().writeStreamingAll(io, rendered.body) catch {};
    std.Io.File.stdout().writeStreamingAll(io, "\n") catch {};
}

fn zPath(alloc: std.mem.Allocator, path: []const u8) ![:0]u8 {
    return try alloc.dupeZ(u8, path);
}

fn parseLocalArgs(alloc: std.mem.Allocator, argv: []const []const u8) !ParsedArgs {
    var options = LocalOptions{};
    var command_start: usize = 0;

    while (command_start < argv.len) {
        const arg = argv[command_start];
        if (std.mem.eql(u8, arg, "--")) {
            command_start += 1;
            break;
        } else if (std.mem.eql(u8, arg, "--ha-url")) {
            command_start += 1;
            options.remote_url = try value(argv, &command_start, "--ha-url");
        } else if (std.mem.eql(u8, arg, "--primary-log")) {
            command_start += 1;
            options.primary_log = try value(argv, &command_start, "--primary-log");
        } else if (std.mem.eql(u8, arg, "--primary-slots")) {
            command_start += 1;
            options.primary_slots = try value(argv, &command_start, "--primary-slots");
        } else if (std.mem.eql(u8, arg, "--standby-log")) {
            command_start += 1;
            options.standby_log = try value(argv, &command_start, "--standby-log");
        } else if (std.mem.eql(u8, arg, "--standby-progress")) {
            command_start += 1;
            options.standby_progress = try value(argv, &command_start, "--standby-progress");
        } else if (std.mem.eql(u8, arg, "--fence-wal")) {
            command_start += 1;
            options.fence_wal = try value(argv, &command_start, "--fence-wal");
        } else if (std.mem.eql(u8, arg, "--ha-cluster-id")) {
            command_start += 1;
            options.identity.cluster_id = try parseU64(try value(argv, &command_start, "--ha-cluster-id"));
        } else if (std.mem.eql(u8, arg, "--ha-shard-id")) {
            command_start += 1;
            options.identity.shard_id = try parseU64(try value(argv, &command_start, "--ha-shard-id"));
        } else if (std.mem.eql(u8, arg, "--ha-table-id")) {
            command_start += 1;
            options.identity.table_id = try parseU64(try value(argv, &command_start, "--ha-table-id"));
        } else if (std.mem.eql(u8, arg, "--ha-timeline-id")) {
            command_start += 1;
            options.identity.timeline_id = try parseU64(try value(argv, &command_start, "--ha-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--ha-epoch")) {
            command_start += 1;
            options.identity.epoch = try parseU64(try value(argv, &command_start, "--ha-epoch"));
        } else {
            break;
        }
    }

    const command_args = try alloc.dupe([]const u8, argv[command_start..]);
    return .{
        .options = options,
        .command_args = command_args,
    };
}

fn value(argv: []const []const u8, idx: *usize, flag: []const u8) ![]const u8 {
    if (idx.* >= argv.len) {
        if (std.mem.eql(u8, flag, "--primary-log")) return error.PrimaryLogMissing;
        if (std.mem.eql(u8, flag, "--primary-slots")) return error.PrimarySlotsMissing;
        if (std.mem.eql(u8, flag, "--standby-log")) return error.StandbyLogMissing;
        if (std.mem.eql(u8, flag, "--standby-progress")) return error.StandbyProgressMissing;
        if (std.mem.eql(u8, flag, "--fence-wal")) return error.FenceWalMissing;
        return error.FlagValueMissing;
    }
    const out = argv[idx.*];
    idx.* += 1;
    return out;
}

fn parseU64(raw: []const u8) !u64 {
    return try std.fmt.parseInt(u64, raw, 10);
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} ha [local options] -- <ha command>
        \\
        \\local options:
        \\  --ha-url URL
        \\  --primary-log PATH
        \\  --primary-slots PATH
        \\  --standby-log PATH
        \\  --standby-progress PATH
        \\  --fence-wal PATH
        \\  --ha-cluster-id N
        \\  --ha-shard-id N
        \\  --ha-table-id N
        \\  --ha-timeline-id N
        \\  --ha-epoch N
        \\
        \\examples:
        \\  {s} ha --ha-url http://127.0.0.1:8081 -- status primary
        \\  {s} ha --primary-log primary.wal --primary-slots slots.wal --ha-cluster-id 1 --ha-shard-id 1 --ha-table-id 1 --ha-timeline-id 1 --ha-epoch 1 -- slot list
        \\  {s} ha --standby-log standby.wal --standby-progress progress.wal --ha-cluster-id 1 --ha-shard-id 1 --ha-table-id 1 --ha-timeline-id 1 --ha-epoch 1 -- status standby
        \\
    , .{ argv0, argv0, argv0, argv0 });
}

test "ha cmd parses local handles before admin command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--primary-log",    "p.wal",
        "--primary-slots",  "slots.wal",
        "--ha-cluster-id",  "10",
        "--ha-shard-id",    "20",
        "--ha-table-id",    "30",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "--table",
        "slot",             "list",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("p.wal", parsed.options.primary_log.?);
    try std.testing.expectEqual(@as(u64, 10), parsed.options.identity.cluster_id.?);
    try std.testing.expectEqual(@as(usize, 3), parsed.command_args.len);
    try std.testing.expectEqualStrings("--table", parsed.command_args[0]);
    try std.testing.expectEqualStrings("slot", parsed.command_args[1]);
    try std.testing.expectEqualStrings("list", parsed.command_args[2]);

    const identity = try parsed.options.primaryIdentity();
    try std.testing.expectEqual(@as(u64, 30), identity.table_id);
}

test "ha cmd parses remote admin URL before command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--ha-url", "http://127.0.0.1:8081",
        "--",       "--table",
        "status",   "primary",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("http://127.0.0.1:8081", parsed.options.remote_url.?);
    try std.testing.expectEqual(@as(usize, 3), parsed.command_args.len);
    try std.testing.expectEqualStrings("--table", parsed.command_args[0]);
    try std.testing.expectEqualStrings("status", parsed.command_args[1]);
    try std.testing.expectEqualStrings("primary", parsed.command_args[2]);
}

test "ha cmd keeps promotion identity flags in admin command" {
    const alloc = std.testing.allocator;
    var parsed = try parseLocalArgs(alloc, &.{
        "--fence-wal", "fence.wal",
        "promote",     "--cluster-id",
        "10",          "--shard-id",
        "20",          "--table-id",
        "30",
    });
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("fence.wal", parsed.options.fence_wal.?);
    try std.testing.expectEqualStrings("promote", parsed.command_args[0]);
    try std.testing.expectEqualStrings("--cluster-id", parsed.command_args[1]);
}

test "ha cmd compiles" {
    _ = run;
    _ = runFromIterator;
    _ = runArgv;
}
