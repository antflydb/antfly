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

const admin_api = antfly.admin;
const ha = antfly.ha;
const http_common = antfly.common.http.http_common;

var test_path_counter: u64 = 0;

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
        try runRemoteArgv(alloc, io, remote_url, parsed.command_args, executor.executor());
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

fn runRemoteArgv(
    alloc: std.mem.Allocator,
    io: std.Io,
    remote_url: []const u8,
    command_args: []const []const u8,
    executor: http_common.RequestExecutor,
) !void {
    var plan = try ha.admin_cli.parse(alloc, command_args);
    defer plan.deinit(alloc);

    var client = ha.http_client.Client.init(alloc, executor);
    if (try executeTypedRemote(alloc, io, &client, remote_url, plan)) return;

    var rendered = try client.executeCommand(remote_url, command_args);
    defer rendered.deinit(alloc);
    writeRemoteBody(io, rendered.body);
}

fn executeTypedRemote(
    alloc: std.mem.Allocator,
    io: std.Io,
    client: *ha.http_client.Client,
    remote_url: []const u8,
    plan: ha.admin_cli.Plan,
) !bool {
    if (plan.output == .prometheus) return false;

    switch (plan.command) {
        .slot => |command| switch (command.action) {
            .create => {
                var out = try client.createReplicationSlot(
                    remote_url,
                    command.request.slot_name,
                    command.request.initial_lsn,
                );
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .pause => {
                var out = try client.pauseReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .@"resume" => {
                var out = try client.resumeReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .drop => {
                var out = try client.dropReplicationSlot(remote_url, command.request.slot_name);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
        },
        .slot_list => |command| {
            if (command.retention_policy.max_lag_lsn == 0) {
                var out = try client.listReplicationSlots(remote_url);
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            } else {
                var out = try client.getPrimaryStatus(remote_url, .{
                    .max_lag_lsn = command.retention_policy.max_lag_lsn,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            }
            return true;
        },
        .seed => |command| switch (command) {
            .begin => |request| {
                var out = try client.beginBaseBackup(remote_url, .{
                    .slot_name = request.slot_name,
                    .manifest_id = request.manifest_id,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .finish => |request| {
                var out = try client.finishBaseBackup(remote_url, .{
                    .manifest_path = request.manifest_path,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
            .bootstrap => |request| {
                var out = try client.bootstrapStandby(remote_url, .{
                    .manifest_path = request.manifest_path,
                    .content_root = request.content_root,
                });
                defer out.deinit(alloc);
                try writeTypedRemoteBody(alloc, io, plan.output, out.body);
                return true;
            },
        },
        .primary_status => |command| {
            if (command.view != .status) return false;
            var out = try client.getPrimaryStatus(remote_url, .{
                .max_lag_lsn = if (command.retention_policy.max_lag_lsn == 0) null else command.retention_policy.max_lag_lsn,
                .sync_policy = command.sync_policy,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .standby_status => |command| {
            if (command.view != .status) return false;
            var out = try client.getStandbyStatus(remote_url, command.upstream_lsn);
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .commit_check => |command| {
            var out = try client.checkCommit(remote_url, .{
                .target_lsn = try i64FromU64(command.target_lsn),
                .sync_policy = try syncPolicyOpenApi(command.policy),
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .commit_append => |command| {
            var out = try client.appendCommit(remote_url, .{
                .payload = command.append.payload,
                .kind = try recordKindName(command.append.kind),
                .payload_codec = try payloadCodecName(command.append.payload_codec),
                .shard_id = if (command.append.shard_id) |raw| try i64FromU64(raw) else null,
                .table_id = if (command.append.table_id) |raw| try i64FromU64(raw) else null,
                .commit_timestamp_ns = command.append.commit_timestamp_ns,
                .sync_policy = try syncPolicyOpenApi(command.policy),
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .read_check => |request| {
            var out = try client.checkRead(remote_url, .{
                .consistency = @tagName(request.consistency),
                .required_lsn = if (request.required_lsn) |raw| try i64FromU64(raw) else null,
                .required_metadata_lsn = if (request.required_metadata_lsn) |raw| try i64FromU64(raw) else null,
                .metadata_applied_lsn = if (request.metadata_applied_lsn) |raw| try i64FromU64(raw) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .write_check => |command| {
            var out = try client.checkWrite(remote_url, .{
                .role = @tagName(command.role),
                .expected_identity = if (command.request.expected_identity) |identity| try adminIdentity(identity) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .owner_job_check => |command| {
            var out = try client.checkOwnerJob(remote_url, .{
                .role = @tagName(command.role),
                .kind = @tagName(command.request.kind),
                .expected_identity = if (command.request.expected_identity) |identity| try adminIdentity(identity) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .fence_acquire => |request| {
            var out = try client.acquireFence(remote_url, try fenceRequestOpenApi(request));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .fence_current => {
            var out = try client.currentFence(remote_url);
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .promote_assess => |command| {
            var out = try client.assessPromotion(remote_url, .{
                .required_lsn = if (command.check.required_lsn) |raw| try i64FromU64(raw) else null,
                .fencing_confirmed = command.check.fencing_confirmed,
                .force = command.check.force,
                .use_current_fence = command.use_current_fence,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .promote_current_fence => {
            var out = try client.promoteWithCurrentFence(remote_url);
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .promote => |command| {
            var out = try client.promote(remote_url, try fenceRequestOpenApi(command.fence));
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .rejoin_assess => |command| {
            var out = try client.assessRejoin(remote_url, .{
                .node_id = command.former.node_id,
                .identity = try adminIdentity(command.former.identity),
                .last_lsn = try i64FromU64(command.former.last_lsn),
                .retained_from_lsn = try i64FromU64(command.policy.retained_from_lsn),
                .allow_rewind_after_forced_promotion = command.policy.allow_rewind_after_forced_promotion,
                .receipt = if (command.receipt) |receipt| try fenceReceiptOpenApi(receipt) else null,
            });
            defer out.deinit(alloc);
            try writeTypedRemoteBody(alloc, io, plan.output, out.body);
            return true;
        },
        .identify_system,
        .start_replication,
        .stream_once,
        .standby_status_update,
        .operator_plan,
        => return false,
    }
}

fn writeRemoteBody(io: std.Io, body: []const u8) void {
    std.Io.File.stdout().writeStreamingAll(io, body) catch {};
    std.Io.File.stdout().writeStreamingAll(io, "\n") catch {};
}

fn writeTypedRemoteBody(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: ha.admin_cli.OutputFormat,
    body: []const u8,
) !void {
    switch (output) {
        .json => writeRemoteBody(io, body),
        .table => {
            const table = try renderJsonTableAlloc(alloc, body);
            defer alloc.free(table);
            writeRemoteBody(io, table);
        },
        .prometheus => unreachable,
    }
}

fn renderJsonTableAlloc(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendJsonTableValue(alloc, &out, "", parsed.value);
    return try out.toOwnedSlice(alloc);
}

fn appendJsonTableValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    path: []const u8,
    json_value: std.json.Value,
) !void {
    switch (json_value) {
        .object => |object| {
            var iter = object.iterator();
            while (iter.next()) |entry| {
                const next_path = if (path.len == 0)
                    try alloc.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(alloc, "{s}.{s}", .{ path, entry.key_ptr.* });
                defer alloc.free(next_path);
                try appendJsonTableValue(alloc, out, next_path, entry.value_ptr.*);
            }
        },
        .array => |array| {
            for (array.items, 0..) |item, idx| {
                const next_path = try std.fmt.allocPrint(alloc, "{s}[{d}]", .{ path, idx });
                defer alloc.free(next_path);
                try appendJsonTableValue(alloc, out, next_path, item);
            }
            if (array.items.len == 0) try appendJsonTableLine(alloc, out, path, "[]");
        },
        .string => |text| try appendJsonTableLine(alloc, out, path, text),
        .number_string => |text| try appendJsonTableLine(alloc, out, path, text),
        .integer => |number| try appendJsonTableLineFmt(alloc, out, path, "{d}", .{number}),
        .float => |number| try appendJsonTableLineFmt(alloc, out, path, "{d}", .{number}),
        .bool => |flag| try appendJsonTableLine(alloc, out, path, if (flag) "true" else "false"),
        .null => try appendJsonTableLine(alloc, out, path, "null"),
    }
}

fn appendJsonTableLine(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    value_text: []const u8,
) !void {
    try out.appendSlice(alloc, key);
    try out.append(alloc, '=');
    try out.appendSlice(alloc, value_text);
    try out.append(alloc, '\n');
}

fn appendJsonTableLineFmt(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    comptime fmt: []const u8,
    args: anytype,
) !void {
    try out.appendSlice(alloc, key);
    try out.append(alloc, '=');
    const rendered = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
    try out.append(alloc, '\n');
}

fn syncPolicyOpenApi(policy: ha.primary.SyncPolicy) !admin_api.openapi.HASyncPolicy {
    return .{
        .mode = @tagName(policy.mode),
        .selection = @tagName(policy.selection),
        .required = try i64FromU64(policy.required),
        .standby_names = policy.standby_names,
        .failure_policy = @tagName(policy.failure_policy),
    };
}

fn adminIdentity(identity: ha.standby.Identity) !admin_api.openapi.HAIdentity {
    return .{
        .cluster_id = try i64FromU64(identity.cluster_id),
        .shard_id = try i64FromU64(identity.shard_id),
        .table_id = try i64FromU64(identity.table_id),
        .timeline_id = try i64FromU64(identity.timeline_id),
        .epoch = try i64FromU64(identity.epoch),
    };
}

fn fenceRequestOpenApi(request: ha.fencing.FenceRequest) !admin_api.openapi.FenceAcquireRequest {
    return .{
        .identity = try adminIdentity(request.identity),
        .old_primary_id = request.old_primary_id,
        .promoted_node_id = request.promoted_node_id,
        .new_timeline_id = try i64FromU64(request.new_timeline_id),
        .new_epoch = try i64FromU64(request.new_epoch),
        .required_lsn = try i64FromU64(request.required_lsn),
        .observed_lsn = try i64FromU64(request.observed_lsn),
        .force = request.force,
        .reason = request.reason,
    };
}

fn fenceReceiptOpenApi(receipt: ha.fencing.Receipt) !admin_api.openapi.HAFenceReceipt {
    return .{
        .identity = try adminIdentity(receipt.identity),
        .old_primary_id = receipt.old_primary_id,
        .promoted_node_id = receipt.promoted_node_id,
        .parent_timeline_id = try i64FromU64(receipt.parent_timeline_id),
        .parent_epoch = try i64FromU64(receipt.parent_epoch),
        .new_timeline_id = try i64FromU64(receipt.new_timeline_id),
        .new_epoch = try i64FromU64(receipt.new_epoch),
        .required_lsn = try i64FromU64(receipt.required_lsn),
        .observed_lsn = try i64FromU64(receipt.observed_lsn),
        .generation = try i64FromU64(receipt.generation),
        .forced = receipt.forced,
        .token = receipt.token,
        .reason = receipt.reason,
    };
}

fn recordKindName(kind: ha.replication_record.RecordKind) ![]const u8 {
    return switch (kind) {
        .batch_mutation => "batch_mutation",
        .metadata_mutation => "metadata_mutation",
        .derived_effect => "derived_effect",
        .backup_start => "backup_start",
        .backup_end => "backup_end",
        .checkpoint => "checkpoint",
        .manifest => "manifest",
        .truncate => "truncate",
        .timeline_switch => "timeline_switch",
        _ => error.InvalidHaCommand,
    };
}

fn payloadCodecName(codec: ha.replication_record.PayloadCodec) ![]const u8 {
    return switch (codec) {
        .raw => "raw",
        .json => "json",
        .binary => "binary",
        _ => error.InvalidHaCommand,
    };
}

fn i64FromU64(raw: u64) !i64 {
    if (raw > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidHaCommand;
    return @intCast(raw);
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
        \\  {s} ha --primary-log primary.wal --primary-slots slots.wal --ha-cluster-id 1 --ha-shard-id 1 --ha-table-id 1 --ha-timeline-id 1 --ha-epoch 1 -- write check --role primary
        \\  {s} ha --standby-log standby.wal --standby-progress progress.wal --ha-cluster-id 1 --ha-shard-id 1 --ha-table-id 1 --ha-timeline-id 1 --ha-epoch 1 -- owner-job check --role standby --kind derived-effect-writer
        \\
    , .{ argv0, argv0, argv0, argv0, argv0, argv0 });
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

test "ha cmd remote JSON commands prefer typed admin routes" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "remote-typed");
    defer paths.deinit(alloc);

    var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
    defer primary.close();

    var server = ha.http_admin.Server.init(alloc, .{ .primary = &primary });
    defer server.deinit();
    var recorder = RecordingExecutor.init(alloc, server.executor());
    defer recorder.deinit();

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "slot",
        "create",
        "standby-json",
        "--initial-lsn",
        "0",
    }, recorder.executor());

    try std.testing.expectEqual(http_common.Method.POST, recorder.last_method.?);
    try expectContains(recorder.last_uri.?, admin_api.routes.ha_replication_slots);
    try std.testing.expect(std.mem.indexOf(u8, recorder.last_uri.?, ha.http_admin.Routes.command) == null);

    try runRemoteArgv(alloc, std.testing.io, "http://ha-admin.test", &.{
        "--table",
        "slot",
        "create",
        "standby-table",
        "--initial-lsn",
        "0",
    }, recorder.executor());

    try std.testing.expectEqual(http_common.Method.POST, recorder.last_method.?);
    try expectContains(recorder.last_uri.?, admin_api.routes.ha_replication_slots);
    try std.testing.expect(std.mem.indexOf(u8, recorder.last_uri.?, ha.http_admin.Routes.command) == null);
}

test "ha cmd renders typed JSON responses as dotted table fields" {
    const alloc = std.testing.allocator;
    const table = try renderJsonTableAlloc(alloc,
        \\{"schema_version":1,"slot":{"slot_name":"standby-a","active":true,"restart_lsn":4},"empty":[]}
    );
    defer alloc.free(table);

    try expectContains(table, "schema_version=1\n");
    try expectContains(table, "slot.slot_name=standby-a\n");
    try expectContains(table, "slot.active=true\n");
    try expectContains(table, "slot.restart_lsn=4\n");
    try expectContains(table, "empty=[]\n");
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

test "ha cmd streams local primary WAL into durable standby state" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream-command");
    defer paths.deinit(alloc);

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",    paths.primary_log,
        "--primary-slots",  paths.primary_slots,
        "--ha-cluster-id",  "10",
        "--ha-shard-id",    "20",
        "--ha-table-id",    "30",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "--table",
        "slot",             "create",
        "standby-cli",      "--initial-lsn",
        "0",
    });

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",    paths.primary_log,
        "--primary-slots",  paths.primary_slots,
        "--ha-cluster-id",  "10",
        "--ha-shard-id",    "20",
        "--ha-table-id",    "30",
        "--ha-timeline-id", "1",
        "--ha-epoch",       "2",
        "--",               "--table",
        "commit",           "append",
        "--payload",        "one",
        "--sync-mode",      "async",
    });

    try runArgv(alloc, std.testing.io, &.{
        "--primary-log",      paths.primary_log,
        "--primary-slots",    paths.primary_slots,
        "--standby-log",      paths.standby_log,
        "--standby-progress", paths.standby_progress,
        "--ha-cluster-id",    "10",
        "--ha-shard-id",      "20",
        "--ha-table-id",      "30",
        "--ha-timeline-id",   "1",
        "--ha-epoch",         "2",
        "--",                 "--table",
        "stream",             "once",
        "--slot",             "standby-cli",
    });

    {
        var primary = try ha.primary.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, testIdentity(), .{});
        defer primary.close();
        const slot = primary.slot("standby-cli") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.timeline_id);
        try std.testing.expectEqual(@as(u64, 0), slot.restart_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
        try std.testing.expect(slot.active);
    }

    {
        var standby = try ha.standby.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, testIdentity(), .{});
        defer standby.close();
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 1), standby.currentProgress().safe_read_lsn);
    }
}

test "ha cmd compiles" {
    _ = run;
    _ = runFromIterator;
    _ = runArgv;
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
    }
};

fn testPaths(alloc: std.mem.Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
    };
}

fn allocPrintPath(alloc: std.mem.Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-cmd-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() ha.standby.Identity {
    return .{
        .cluster_id = 10,
        .shard_id = 20,
        .table_id = 30,
        .timeline_id = 1,
        .epoch = 2,
    };
}

const RecordingExecutor = struct {
    alloc: std.mem.Allocator,
    inner: http_common.RequestExecutor,
    last_method: ?http_common.Method = null,
    last_uri: ?[]u8 = null,

    fn init(alloc: std.mem.Allocator, inner: http_common.RequestExecutor) RecordingExecutor {
        return .{
            .alloc = alloc,
            .inner = inner,
        };
    }

    fn deinit(self: *RecordingExecutor) void {
        if (self.last_uri) |uri| self.alloc.free(uri);
        self.* = undefined;
    }

    fn executor(self: *RecordingExecutor) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *RecordingExecutor = @ptrCast(@alignCast(ptr));
        if (self.last_uri) |uri| self.alloc.free(uri);
        self.last_uri = try self.alloc.dupe(u8, req.uri);
        self.last_method = req.method;
        return try self.inner.execute(alloc, req);
    }
};

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        std.debug.print("expected to find '{s}' in '{s}'\n", .{ needle, haystack });
        return error.TestExpectedSubstring;
    }
}
