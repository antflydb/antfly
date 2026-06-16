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

//! HA admin CLI command planner.
//!
//! The production CLI, HTTP admin routes, and operator controllers should map
//! user input into this small command contract before calling `admin.zig` or
//! `replication_api.zig`. Keeping parsing and command semantics here prevents
//! each integration layer from inventing a slightly different HA vocabulary.

const std = @import("std");
const Allocator = std.mem.Allocator;
const admin = @import("admin.zig");
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const replication_api = @import("replication_api.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");

pub const OutputFormat = enum {
    json,
    table,
    prometheus,
};

pub const StatusView = enum {
    status,
    metrics,
};

pub const PrimaryStatusCommand = struct {
    retention_policy: slot_store.RetentionPolicy = .{},
    sync_policy: ?primary_mod.SyncPolicy = null,
    view: StatusView = .status,
};

pub const StandbyStatusCommand = struct {
    upstream_lsn: ?u64 = null,
    view: StatusView = .status,
};

pub const SlotCommand = struct {
    action: admin.SlotAction,
    request: admin.SlotRequest,
};

pub const CommitCheckCommand = struct {
    target_lsn: u64,
    policy: primary_mod.SyncPolicy,
};

pub const Command = union(enum) {
    identify_system,
    slot: SlotCommand,
    start_replication: replication_api.StartReplicationRequest,
    standby_status_update: replication_api.StandbyStatusUpdateRequest,
    primary_status: PrimaryStatusCommand,
    standby_status: StandbyStatusCommand,
    commit_check: CommitCheckCommand,
    read_check: read_gate.Request,
    promote: admin.FencedPromotionRequest,
};

pub const Plan = struct {
    output: OutputFormat = .json,
    command: Command,
    owned_standby_names: []const []const u8 = &.{},

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.owned_standby_names);
        self.* = undefined;
    }
};

pub fn parse(alloc: Allocator, argv: []const []const u8) !Plan {
    var cursor = Cursor{ .args = argv };
    var output: OutputFormat = .json;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--output")) {
            _ = cursor.next();
            output = try parseOutputFormat(try cursor.value("--output"));
        } else if (std.mem.eql(u8, arg, "--json")) {
            _ = cursor.next();
            output = .json;
        } else if (std.mem.eql(u8, arg, "--table")) {
            _ = cursor.next();
            output = .table;
        } else if (std.mem.eql(u8, arg, "--prometheus")) {
            _ = cursor.next();
            output = .prometheus;
        } else {
            break;
        }
    }

    const root = cursor.next() orelse return error.HaCommandMissing;
    var plan = Plan{
        .output = output,
        .command = undefined,
    };
    errdefer plan.deinit(alloc);

    if (std.mem.eql(u8, root, "identify")) {
        try cursor.expectEnd();
        plan.command = .identify_system;
        return plan;
    }
    if (std.mem.eql(u8, root, "slot")) {
        plan.command = .{ .slot = try parseSlot(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "stream")) {
        plan.command = .{ .start_replication = try parseStream(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "standby")) {
        plan.command = try parseStandby(&cursor);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "status")) {
        plan.command = try parseStatus(alloc, &cursor, &plan.owned_standby_names);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "commit")) {
        plan.command = .{ .commit_check = try parseCommitCheck(alloc, &cursor, &plan.owned_standby_names) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "read")) {
        plan.command = .{ .read_check = try parseReadCheck(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "promote")) {
        plan.command = .{ .promote = try parsePromote(&cursor) };
        try cursor.expectEnd();
        return plan;
    }

    return error.UnknownHaCommand;
}

fn parseSlot(cursor: *Cursor) !SlotCommand {
    const action_raw = cursor.next() orelse return error.SlotActionMissing;
    const action = if (std.mem.eql(u8, action_raw, "create"))
        admin.SlotAction.create
    else if (std.mem.eql(u8, action_raw, "pause"))
        admin.SlotAction.pause
    else if (std.mem.eql(u8, action_raw, "resume"))
        admin.SlotAction.@"resume"
    else if (std.mem.eql(u8, action_raw, "drop"))
        admin.SlotAction.drop
    else
        return error.UnknownSlotAction;

    var slot_name: ?[]const u8 = null;
    var initial_lsn: ?u64 = null;
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot") or std.mem.eql(u8, arg, "--name")) {
            _ = cursor.next();
            slot_name = try cursor.value(arg);
        } else if (std.mem.eql(u8, arg, "--initial-lsn")) {
            _ = cursor.next();
            initial_lsn = try parseU64(try cursor.value("--initial-lsn"));
        } else if (slot_name == null and !isFlag(arg)) {
            slot_name = cursor.next().?;
        } else {
            break;
        }
    }

    return .{
        .action = action,
        .request = .{
            .slot_name = slot_name orelse return error.SlotNameMissing,
            .initial_lsn = initial_lsn,
        },
    };
}

fn parseStream(cursor: *Cursor) !replication_api.StartReplicationRequest {
    var slot_name: ?[]const u8 = null;
    var from_lsn: ?u64 = null;
    var max_records: usize = 0;
    var max_encoded_bytes: usize = 0;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot")) {
            _ = cursor.next();
            slot_name = try cursor.value("--slot");
        } else if (std.mem.eql(u8, arg, "--from-lsn")) {
            _ = cursor.next();
            from_lsn = try parseU64(try cursor.value("--from-lsn"));
        } else if (std.mem.eql(u8, arg, "--max-records")) {
            _ = cursor.next();
            max_records = try parseUsize(try cursor.value("--max-records"));
        } else if (std.mem.eql(u8, arg, "--max-encoded-bytes")) {
            _ = cursor.next();
            max_encoded_bytes = try parseUsize(try cursor.value("--max-encoded-bytes"));
        } else {
            break;
        }
    }

    return .{
        .slot_name = slot_name orelse return error.SlotNameMissing,
        .from_lsn = from_lsn orelse return error.FromLsnMissing,
        .max_records = max_records,
        .max_encoded_bytes = max_encoded_bytes,
    };
}

fn parseStandby(cursor: *Cursor) !Command {
    const subcommand = cursor.next() orelse return error.StandbySubcommandMissing;
    if (std.mem.eql(u8, subcommand, "ack") or std.mem.eql(u8, subcommand, "status-update")) {
        return .{ .standby_status_update = try parseStandbyStatusUpdate(cursor) };
    }
    return error.UnknownStandbySubcommand;
}

fn parseStandbyStatusUpdate(cursor: *Cursor) !replication_api.StandbyStatusUpdateRequest {
    var slot_name: ?[]const u8 = null;
    var timeline_id: ?u64 = null;
    var received_lsn: ?u64 = null;
    var applied_lsn: ?u64 = null;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot")) {
            _ = cursor.next();
            slot_name = try cursor.value("--slot");
        } else if (std.mem.eql(u8, arg, "--timeline-id")) {
            _ = cursor.next();
            timeline_id = try parseU64(try cursor.value("--timeline-id"));
        } else if (std.mem.eql(u8, arg, "--received-lsn")) {
            _ = cursor.next();
            received_lsn = try parseU64(try cursor.value("--received-lsn"));
        } else if (std.mem.eql(u8, arg, "--applied-lsn")) {
            _ = cursor.next();
            applied_lsn = try parseU64(try cursor.value("--applied-lsn"));
        } else {
            break;
        }
    }

    return .{
        .slot_name = slot_name orelse return error.SlotNameMissing,
        .timeline_id = timeline_id orelse return error.TimelineIdMissing,
        .received_lsn = received_lsn orelse return error.ReceivedLsnMissing,
        .applied_lsn = applied_lsn orelse return error.AppliedLsnMissing,
    };
}

fn parseStatus(alloc: Allocator, cursor: *Cursor, owned_standby_names: *[]const []const u8) !Command {
    const role = cursor.next() orelse return error.StatusRoleMissing;
    if (std.mem.eql(u8, role, "primary")) {
        var command = PrimaryStatusCommand{};
        var sync_builder = SyncPolicyBuilder{};
        defer sync_builder.deinit(alloc);

        while (cursor.peek()) |arg| {
            if (std.mem.eql(u8, arg, "--max-lag-lsn")) {
                _ = cursor.next();
                command.retention_policy.max_lag_lsn = try parseU64(try cursor.value("--max-lag-lsn"));
            } else if (std.mem.eql(u8, arg, "--view")) {
                _ = cursor.next();
                command.view = try parseStatusView(try cursor.value("--view"));
            } else if (try sync_builder.parseFlag(alloc, cursor, arg)) {
                continue;
            } else {
                break;
            }
        }

        command.sync_policy = try sync_builder.finish(alloc, owned_standby_names);
        return .{ .primary_status = command };
    }
    if (std.mem.eql(u8, role, "standby")) {
        var command = StandbyStatusCommand{};
        while (cursor.peek()) |arg| {
            if (std.mem.eql(u8, arg, "--upstream-lsn")) {
                _ = cursor.next();
                command.upstream_lsn = try parseU64(try cursor.value("--upstream-lsn"));
            } else if (std.mem.eql(u8, arg, "--view")) {
                _ = cursor.next();
                command.view = try parseStatusView(try cursor.value("--view"));
            } else {
                break;
            }
        }
        return .{ .standby_status = command };
    }
    return error.UnknownStatusRole;
}

fn parseCommitCheck(alloc: Allocator, cursor: *Cursor, owned_standby_names: *[]const []const u8) !CommitCheckCommand {
    const subcommand = cursor.next() orelse return error.CommitSubcommandMissing;
    if (!std.mem.eql(u8, subcommand, "check")) return error.UnknownCommitSubcommand;

    var target_lsn: ?u64 = null;
    var sync_builder = SyncPolicyBuilder{};
    defer sync_builder.deinit(alloc);
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--target-lsn")) {
            _ = cursor.next();
            target_lsn = try parseU64(try cursor.value("--target-lsn"));
        } else if (try sync_builder.parseFlag(alloc, cursor, arg)) {
            continue;
        } else {
            break;
        }
    }

    const policy = (try sync_builder.finish(alloc, owned_standby_names)) orelse return error.SyncPolicyMissing;
    return .{
        .target_lsn = target_lsn orelse return error.TargetLsnMissing,
        .policy = policy,
    };
}

fn parseReadCheck(cursor: *Cursor) !read_gate.Request {
    const subcommand = cursor.next() orelse return error.ReadSubcommandMissing;
    if (!std.mem.eql(u8, subcommand, "check")) return error.UnknownReadSubcommand;

    var request = read_gate.Request{};
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--consistency")) {
            _ = cursor.next();
            request.consistency = try parseConsistency(try cursor.value("--consistency"));
        } else if (std.mem.eql(u8, arg, "--required-lsn") or std.mem.eql(u8, arg, "--at-least-lsn")) {
            _ = cursor.next();
            request.required_lsn = try parseU64(try cursor.value(arg));
            if (std.mem.eql(u8, arg, "--at-least-lsn")) request.consistency = .at_least_lsn;
        } else {
            break;
        }
    }
    return request;
}

fn parsePromote(cursor: *Cursor) !admin.FencedPromotionRequest {
    var identity = standby_mod.Identity{
        .cluster_id = 0,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 0,
        .epoch = 0,
    };
    var old_primary_id: ?[]const u8 = null;
    var promoted_node_id: ?[]const u8 = null;
    var new_timeline_id: ?u64 = null;
    var new_epoch: ?u64 = null;
    var required_lsn: ?u64 = null;
    var observed_lsn: ?u64 = null;
    var force = false;
    var reason: []const u8 = &.{};

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--cluster-id")) {
            _ = cursor.next();
            identity.cluster_id = try parseU64(try cursor.value("--cluster-id"));
        } else if (std.mem.eql(u8, arg, "--shard-id")) {
            _ = cursor.next();
            identity.shard_id = try parseU64(try cursor.value("--shard-id"));
        } else if (std.mem.eql(u8, arg, "--table-id")) {
            _ = cursor.next();
            identity.table_id = try parseU64(try cursor.value("--table-id"));
        } else if (std.mem.eql(u8, arg, "--timeline-id")) {
            _ = cursor.next();
            identity.timeline_id = try parseU64(try cursor.value("--timeline-id"));
        } else if (std.mem.eql(u8, arg, "--epoch")) {
            _ = cursor.next();
            identity.epoch = try parseU64(try cursor.value("--epoch"));
        } else if (std.mem.eql(u8, arg, "--old-primary-id")) {
            _ = cursor.next();
            old_primary_id = try cursor.value("--old-primary-id");
        } else if (std.mem.eql(u8, arg, "--promoted-node-id")) {
            _ = cursor.next();
            promoted_node_id = try cursor.value("--promoted-node-id");
        } else if (std.mem.eql(u8, arg, "--new-timeline-id")) {
            _ = cursor.next();
            new_timeline_id = try parseU64(try cursor.value("--new-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--new-epoch")) {
            _ = cursor.next();
            new_epoch = try parseU64(try cursor.value("--new-epoch"));
        } else if (std.mem.eql(u8, arg, "--required-lsn")) {
            _ = cursor.next();
            required_lsn = try parseU64(try cursor.value("--required-lsn"));
        } else if (std.mem.eql(u8, arg, "--observed-lsn")) {
            _ = cursor.next();
            observed_lsn = try parseU64(try cursor.value("--observed-lsn"));
        } else if (std.mem.eql(u8, arg, "--force")) {
            _ = cursor.next();
            force = true;
        } else if (std.mem.eql(u8, arg, "--reason")) {
            _ = cursor.next();
            reason = try cursor.value("--reason");
        } else {
            break;
        }
    }

    if (identity.cluster_id == 0) return error.ClusterIdMissing;
    if (identity.shard_id == 0) return error.ShardIdMissing;
    if (identity.table_id == 0) return error.TableIdMissing;
    if (identity.timeline_id == 0) return error.TimelineIdMissing;
    if (identity.epoch == 0) return error.EpochMissing;

    return .{
        .fence = fencing.FenceRequest{
            .identity = identity,
            .old_primary_id = old_primary_id orelse return error.OldPrimaryIdMissing,
            .promoted_node_id = promoted_node_id orelse return error.PromotedNodeIdMissing,
            .new_timeline_id = new_timeline_id orelse return error.NewTimelineIdMissing,
            .new_epoch = new_epoch orelse return error.NewEpochMissing,
            .required_lsn = required_lsn orelse return error.RequiredLsnMissing,
            .observed_lsn = observed_lsn orelse return error.ObservedLsnMissing,
            .force = force,
            .reason = reason,
        },
    };
}

const SyncPolicyBuilder = struct {
    mode: ?primary_mod.DurabilityMode = null,
    selection: primary_mod.StandbySelection = .any,
    required: usize = 1,
    failure_policy: primary_mod.FailurePolicy = .block,
    standby_names: std.ArrayListUnmanaged([]const u8) = .empty,

    fn deinit(self: *SyncPolicyBuilder, alloc: Allocator) void {
        self.standby_names.deinit(alloc);
        self.* = undefined;
    }

    fn parseFlag(self: *SyncPolicyBuilder, alloc: Allocator, cursor: *Cursor, arg: []const u8) !bool {
        if (std.mem.eql(u8, arg, "--sync-mode")) {
            _ = cursor.next();
            self.mode = try parseDurabilityMode(try cursor.value("--sync-mode"));
            return true;
        }
        if (std.mem.eql(u8, arg, "--sync-selection")) {
            _ = cursor.next();
            self.selection = try parseStandbySelection(try cursor.value("--sync-selection"));
            return true;
        }
        if (std.mem.eql(u8, arg, "--sync-required")) {
            _ = cursor.next();
            self.required = try parseUsize(try cursor.value("--sync-required"));
            return true;
        }
        if (std.mem.eql(u8, arg, "--sync-standby")) {
            _ = cursor.next();
            try self.standby_names.append(alloc, try cursor.value("--sync-standby"));
            return true;
        }
        if (std.mem.eql(u8, arg, "--sync-failure")) {
            _ = cursor.next();
            self.failure_policy = try parseFailurePolicy(try cursor.value("--sync-failure"));
            return true;
        }
        return false;
    }

    fn finish(self: *SyncPolicyBuilder, alloc: Allocator, owned_standby_names: *[]const []const u8) !?primary_mod.SyncPolicy {
        const configured = self.mode != null or
            self.standby_names.items.len > 0 or
            self.selection != .any or
            self.required != 1 or
            self.failure_policy != .block;
        if (!configured) return null;
        if (self.required == 0) return error.SyncRequiredMustBePositive;

        const names = try self.standby_names.toOwnedSlice(alloc);
        self.standby_names = .empty;
        owned_standby_names.* = names;
        return primary_mod.SyncPolicy{
            .mode = self.mode orelse .remote_write,
            .selection = self.selection,
            .required = self.required,
            .standby_names = names,
            .failure_policy = self.failure_policy,
        };
    }
};

const Cursor = struct {
    args: []const []const u8,
    index: usize = 0,

    fn peek(self: *const Cursor) ?[]const u8 {
        if (self.index >= self.args.len) return null;
        return self.args[self.index];
    }

    fn next(self: *Cursor) ?[]const u8 {
        const value_or_null = self.peek();
        if (value_or_null != null) self.index += 1;
        return value_or_null;
    }

    fn value(self: *Cursor, flag: []const u8) ![]const u8 {
        const raw = self.next() orelse return error.FlagValueMissing;
        if (isFlag(raw)) {
            self.index -= 1;
            _ = flag;
            return error.FlagValueMissing;
        }
        return raw;
    }

    fn expectEnd(self: *const Cursor) !void {
        if (self.index != self.args.len) return error.UnexpectedHaArgument;
    }
};

fn parseOutputFormat(raw: []const u8) !OutputFormat {
    if (std.mem.eql(u8, raw, "json")) return .json;
    if (std.mem.eql(u8, raw, "table")) return .table;
    if (std.mem.eql(u8, raw, "prometheus")) return .prometheus;
    return error.InvalidOutputFormat;
}

fn parseStatusView(raw: []const u8) !StatusView {
    if (std.mem.eql(u8, raw, "status")) return .status;
    if (std.mem.eql(u8, raw, "metrics")) return .metrics;
    return error.InvalidStatusView;
}

fn parseDurabilityMode(raw: []const u8) !primary_mod.DurabilityMode {
    if (std.mem.eql(u8, raw, "async")) return .async;
    if (std.mem.eql(u8, raw, "remote_write")) return .remote_write;
    if (std.mem.eql(u8, raw, "remote-write")) return .remote_write;
    if (std.mem.eql(u8, raw, "remote_apply")) return .remote_apply;
    if (std.mem.eql(u8, raw, "remote-apply")) return .remote_apply;
    return error.InvalidDurabilityMode;
}

fn parseStandbySelection(raw: []const u8) !primary_mod.StandbySelection {
    if (std.mem.eql(u8, raw, "any")) return .any;
    if (std.mem.eql(u8, raw, "first")) return .first;
    if (std.mem.eql(u8, raw, "all")) return .all;
    return error.InvalidStandbySelection;
}

fn parseFailurePolicy(raw: []const u8) !primary_mod.FailurePolicy {
    if (std.mem.eql(u8, raw, "block")) return .block;
    if (std.mem.eql(u8, raw, "fail_closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "fail-closed")) return .fail_closed;
    if (std.mem.eql(u8, raw, "degrade_to_async")) return .degrade_to_async;
    if (std.mem.eql(u8, raw, "degrade-to-async")) return .degrade_to_async;
    return error.InvalidFailurePolicy;
}

fn parseConsistency(raw: []const u8) !read_gate.Consistency {
    if (std.mem.eql(u8, raw, "stale_ok")) return .stale_ok;
    if (std.mem.eql(u8, raw, "stale-ok")) return .stale_ok;
    if (std.mem.eql(u8, raw, "at_least_lsn")) return .at_least_lsn;
    if (std.mem.eql(u8, raw, "at-least-lsn")) return .at_least_lsn;
    if (std.mem.eql(u8, raw, "primary")) return .primary;
    return error.InvalidReadConsistency;
}

fn parseU64(raw: []const u8) !u64 {
    return std.fmt.parseInt(u64, raw, 10) catch return error.InvalidInteger;
}

fn parseUsize(raw: []const u8) !usize {
    return std.fmt.parseInt(usize, raw, 10) catch return error.InvalidInteger;
}

fn isFlag(raw: []const u8) bool {
    return std.mem.startsWith(u8, raw, "-");
}

test "storage.ha admin cli parses slot lifecycle commands" {
    const alloc = std.testing.allocator;
    var create = try parse(alloc, &.{ "slot", "create", "standby-a", "--initial-lsn", "12" });
    defer create.deinit(alloc);
    try std.testing.expectEqual(OutputFormat.json, create.output);
    try std.testing.expectEqual(admin.SlotAction.create, create.command.slot.action);
    try std.testing.expectEqualStrings("standby-a", create.command.slot.request.slot_name);
    try std.testing.expectEqual(@as(?u64, 12), create.command.slot.request.initial_lsn);

    var pause = try parse(alloc, &.{ "--table", "slot", "pause", "--slot", "standby-a" });
    defer pause.deinit(alloc);
    try std.testing.expectEqual(OutputFormat.table, pause.output);
    try std.testing.expectEqual(admin.SlotAction.pause, pause.command.slot.action);
    try std.testing.expectEqualStrings("standby-a", pause.command.slot.request.slot_name);
}

test "storage.ha admin cli parses primary status with sync policy" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "status",           "primary",
        "--view",           "metrics",
        "--max-lag-lsn",    "50",
        "--sync-mode",      "remote-apply",
        "--sync-selection", "first",
        "--sync-required",  "2",
        "--sync-standby",   "a",
        "--sync-standby",   "b",
        "--sync-failure",   "fail-closed",
    });
    defer plan.deinit(alloc);

    const command = plan.command.primary_status;
    try std.testing.expectEqual(StatusView.metrics, command.view);
    try std.testing.expectEqual(@as(u64, 50), command.retention_policy.max_lag_lsn);
    const policy = command.sync_policy.?;
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_apply, policy.mode);
    try std.testing.expectEqual(primary_mod.StandbySelection.first, policy.selection);
    try std.testing.expectEqual(@as(usize, 2), policy.required);
    try std.testing.expectEqual(primary_mod.FailurePolicy.fail_closed, policy.failure_policy);
    try std.testing.expectEqual(@as(usize, 2), policy.standby_names.len);
    try std.testing.expectEqualStrings("a", policy.standby_names[0]);
    try std.testing.expectEqualStrings("b", policy.standby_names[1]);
}

test "storage.ha admin cli parses stream ack commit and read checks" {
    const alloc = std.testing.allocator;

    var stream = try parse(alloc, &.{ "stream", "--slot", "standby-a", "--from-lsn", "7", "--max-records", "10" });
    defer stream.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", stream.command.start_replication.slot_name);
    try std.testing.expectEqual(@as(u64, 7), stream.command.start_replication.from_lsn);
    try std.testing.expectEqual(@as(usize, 10), stream.command.start_replication.max_records);

    var ack = try parse(alloc, &.{ "standby", "ack", "--slot", "standby-a", "--timeline-id", "2", "--received-lsn", "9", "--applied-lsn", "8" });
    defer ack.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), ack.command.standby_status_update.timeline_id);
    try std.testing.expectEqual(@as(u64, 9), ack.command.standby_status_update.received_lsn);
    try std.testing.expectEqual(@as(u64, 8), ack.command.standby_status_update.applied_lsn);

    var commit = try parse(alloc, &.{ "commit", "check", "--target-lsn", "9", "--sync-mode", "remote-write", "--sync-standby", "standby-a" });
    defer commit.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), commit.command.commit_check.target_lsn);
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_write, commit.command.commit_check.policy.mode);
    try std.testing.expectEqualStrings("standby-a", commit.command.commit_check.policy.standby_names[0]);

    var read = try parse(alloc, &.{ "read", "check", "--at-least-lsn", "9" });
    defer read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Consistency.at_least_lsn, read.command.read_check.consistency);
    try std.testing.expectEqual(@as(?u64, 9), read.command.read_check.required_lsn);
}

test "storage.ha admin cli parses fenced promotion request" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "promote",
        "--cluster-id",
        "1",
        "--shard-id",
        "2",
        "--table-id",
        "3",
        "--timeline-id",
        "4",
        "--epoch",
        "5",
        "--old-primary-id",
        "primary-a",
        "--promoted-node-id",
        "standby-b",
        "--new-timeline-id",
        "6",
        "--new-epoch",
        "7",
        "--required-lsn",
        "100",
        "--observed-lsn",
        "99",
        "--force",
        "--reason",
        "operator-approved",
    });
    defer plan.deinit(alloc);

    const fence = plan.command.promote.fence;
    try std.testing.expectEqual(@as(u64, 1), fence.identity.cluster_id);
    try std.testing.expectEqual(@as(u64, 4), fence.identity.timeline_id);
    try std.testing.expectEqualStrings("primary-a", fence.old_primary_id);
    try std.testing.expectEqualStrings("standby-b", fence.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 6), fence.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 100), fence.required_lsn);
    try std.testing.expectEqual(@as(u64, 99), fence.observed_lsn);
    try std.testing.expect(fence.force);
    try std.testing.expectEqualStrings("operator-approved", fence.reason);
}
