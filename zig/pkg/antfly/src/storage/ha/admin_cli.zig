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
const owner_job_gate = @import("owner_job_gate.zig");
const operator = @import("operator.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const rejoin = @import("rejoin.zig");
const replication_api = @import("replication_api.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");
const status = @import("status.zig");
const validation = @import("validation.zig");
const write_gate = @import("write_gate.zig");

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

pub const SlotListCommand = struct {
    retention_policy: slot_store.RetentionPolicy = .{},
};

pub const SeedManifestPathCommand = struct {
    manifest_path: []const u8,
};

pub const SeedBootstrapCommand = struct {
    manifest_path: []const u8,
    content_root: ?[]const u8 = null,
};

pub const SeedCommand = union(enum) {
    begin: primary_mod.BaseBackupStart,
    finish: SeedManifestPathCommand,
    bootstrap: SeedBootstrapCommand,
};

pub const StreamOnceCommand = struct {
    slot_name: []const u8,
};

pub const CommitCheckCommand = struct {
    target_lsn: u64,
    policy: primary_mod.SyncPolicy,
};

pub const CommitAppendCommand = struct {
    append: primary_mod.AppendOptions,
    policy: primary_mod.SyncPolicy,
};

pub const GateRole = enum {
    primary,
    standby,
};

pub const WriteCheckCommand = struct {
    role: GateRole,
    request: write_gate.Request = .{},
};

pub const OwnerJobCheckCommand = struct {
    role: GateRole,
    request: owner_job_gate.Request,
};

pub const RejoinAssessCommand = struct {
    former: rejoin.FormerPrimaryState,
    receipt: ?fencing.Receipt = null,
    policy: rejoin.RejoinPolicy,
};

pub const OperatorPlanCommand = struct {
    spec: operator.Spec,
    current_primary_id: ?[]const u8 = null,
    primary_admin_unavailable: bool = false,
    fencing: operator.FencingObservation = .{},
    former_primary: ?rejoin.FormerPrimaryState = null,
    promotion_receipt: ?fencing.Receipt = null,
    rejoin_policy: rejoin.RejoinPolicy = .{ .retained_from_lsn = 0 },
};

pub const PromoteAssessCommand = struct {
    check: status.PromotionCheck,
    use_current_fence: bool = false,
};

pub const Command = union(enum) {
    identify_system,
    slot: SlotCommand,
    slot_list: SlotListCommand,
    seed: SeedCommand,
    start_replication: replication_api.StartReplicationRequest,
    stream_once: StreamOnceCommand,
    standby_status_update: replication_api.StandbyStatusUpdateRequest,
    primary_status: PrimaryStatusCommand,
    standby_status: StandbyStatusCommand,
    commit_check: CommitCheckCommand,
    commit_append: CommitAppendCommand,
    read_check: read_gate.Request,
    write_check: WriteCheckCommand,
    owner_job_check: OwnerJobCheckCommand,
    fence_acquire: fencing.FenceRequest,
    fence_current,
    promote_assess: PromoteAssessCommand,
    promote_current_fence,
    promote: admin.FencedPromotionRequest,
    rejoin_assess: RejoinAssessCommand,
    rejoin_rewind: RejoinAssessCommand,
    rejoin_reseed: RejoinAssessCommand,
    operator_plan: OperatorPlanCommand,
};

pub const Plan = struct {
    output: OutputFormat = .json,
    command: Command,
    owned_standby_names: []const []const u8 = &.{},
    owned_operator_standbys: []operator.StandbySpec = &.{},

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        alloc.free(self.owned_standby_names);
        alloc.free(self.owned_operator_standbys);
        self.* = undefined;
    }
};

pub const PlanDocument = struct {
    schema_version: u32 = 1,
    output: OutputFormat,
    command: Command,
};

pub fn planDocument(plan: Plan) PlanDocument {
    return .{
        .output = plan.output,
        .command = plan.command,
    };
}

pub fn renderJsonAlloc(alloc: Allocator, plan: Plan) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, planDocument(plan), .{});
}

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
        plan.command = try parseSlot(&cursor);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "seed")) {
        plan.command = .{ .seed = try parseSeed(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "stream")) {
        plan.command = try parseStreamCommand(&cursor);
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
        plan.command = try parseCommit(alloc, &cursor, &plan.owned_standby_names);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "read")) {
        plan.command = .{ .read_check = try parseReadCheck(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "write")) {
        plan.command = .{ .write_check = try parseWriteCheck(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "owner-job")) {
        plan.command = .{ .owner_job_check = try parseOwnerJobCheck(&cursor) };
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "fence")) {
        plan.command = try parseFence(&cursor);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "promote")) {
        plan.command = try parsePromote(&cursor);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "rejoin")) {
        plan.command = try parseRejoin(&cursor);
        try cursor.expectEnd();
        return plan;
    }
    if (std.mem.eql(u8, root, "operator")) {
        plan.command = .{ .operator_plan = try parseOperator(alloc, &cursor, &plan.owned_operator_standbys, &plan.owned_standby_names) };
        try cursor.expectEnd();
        return plan;
    }

    return error.UnknownHaCommand;
}

fn parseSlot(cursor: *Cursor) !Command {
    const action_raw = cursor.next() orelse return error.SlotActionMissing;
    if (std.mem.eql(u8, action_raw, "list")) {
        var command = SlotListCommand{};
        while (cursor.peek()) |arg| {
            if (std.mem.eql(u8, arg, "--max-lag-lsn")) {
                _ = cursor.next();
                command.retention_policy.max_lag_lsn = try parseU64(try cursor.value("--max-lag-lsn"));
            } else if (std.mem.eql(u8, arg, "--max-retained-bytes")) {
                _ = cursor.next();
                command.retention_policy.max_retained_bytes = try parseU64(try cursor.value("--max-retained-bytes"));
            } else if (std.mem.eql(u8, arg, "--max-retained-age-ns")) {
                _ = cursor.next();
                command.retention_policy.max_retained_age_ns = try parseU64(try cursor.value("--max-retained-age-ns"));
            } else {
                break;
            }
        }
        return .{ .slot_list = command };
    }

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
            slot_name = try validateHASlotName(try cursor.value(arg));
        } else if (std.mem.eql(u8, arg, "--initial-lsn")) {
            _ = cursor.next();
            initial_lsn = try parseU64(try cursor.value("--initial-lsn"));
        } else if (slot_name == null and !isFlag(arg)) {
            slot_name = try validateHASlotName(cursor.next().?);
        } else {
            break;
        }
    }

    return .{
        .slot = .{
            .action = action,
            .request = .{
                .slot_name = slot_name orelse return error.SlotNameMissing,
                .initial_lsn = initial_lsn,
            },
        },
    };
}

fn parseSeed(cursor: *Cursor) !SeedCommand {
    const subcommand = cursor.next() orelse return error.SeedSubcommandMissing;
    if (std.mem.eql(u8, subcommand, "begin")) {
        var slot_name: ?[]const u8 = null;
        var manifest_id: ?[]const u8 = null;
        while (cursor.peek()) |arg| {
            if (std.mem.eql(u8, arg, "--slot")) {
                _ = cursor.next();
                slot_name = try validateHASlotName(try cursor.value("--slot"));
            } else if (std.mem.eql(u8, arg, "--manifest-id")) {
                _ = cursor.next();
                manifest_id = try cursor.value("--manifest-id");
            } else {
                break;
            }
        }
        return .{ .begin = .{
            .slot_name = slot_name orelse return error.SlotNameMissing,
            .manifest_id = manifest_id orelse return error.ManifestIdMissing,
        } };
    }
    if (std.mem.eql(u8, subcommand, "finish") or std.mem.eql(u8, subcommand, "end")) {
        return .{ .finish = .{ .manifest_path = try parseManifestPath(cursor) } };
    }
    if (std.mem.eql(u8, subcommand, "bootstrap")) {
        var manifest_path: ?[]const u8 = null;
        var content_root: ?[]const u8 = null;
        while (cursor.peek()) |arg| {
            if (std.mem.eql(u8, arg, "--manifest")) {
                _ = cursor.next();
                manifest_path = try validateHAPath(try cursor.value("--manifest"), .manifest);
            } else if (std.mem.eql(u8, arg, "--content-root")) {
                _ = cursor.next();
                content_root = try validateHAPath(try cursor.value("--content-root"), .content_root);
            } else {
                break;
            }
        }
        return .{ .bootstrap = .{
            .manifest_path = manifest_path orelse return error.ManifestPathMissing,
            .content_root = content_root,
        } };
    }
    return error.UnknownSeedSubcommand;
}

fn parseManifestPath(cursor: *Cursor) ![]const u8 {
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--manifest")) {
            _ = cursor.next();
            return try validateHAPath(try cursor.value("--manifest"), .manifest);
        }
        break;
    }
    return error.ManifestPathMissing;
}

fn parseStreamCommand(cursor: *Cursor) !Command {
    if (cursor.peek()) |subcommand| {
        if (std.mem.eql(u8, subcommand, "once")) {
            _ = cursor.next();
            return .{ .stream_once = try parseStreamOnce(cursor) };
        }
        if (std.mem.eql(u8, subcommand, "start")) {
            _ = cursor.next();
            return .{ .start_replication = try parseStreamStart(cursor) };
        }
    }
    return .{ .start_replication = try parseStreamStart(cursor) };
}

fn parseStreamOnce(cursor: *Cursor) !StreamOnceCommand {
    var slot_name: ?[]const u8 = null;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot")) {
            _ = cursor.next();
            slot_name = try validateHASlotName(try cursor.value("--slot"));
        } else if (slot_name == null and !isFlag(arg)) {
            slot_name = try validateHASlotName(cursor.next().?);
        } else {
            break;
        }
    }

    return .{ .slot_name = slot_name orelse return error.SlotNameMissing };
}

fn parseStreamStart(cursor: *Cursor) !replication_api.StartReplicationRequest {
    var slot_name: ?[]const u8 = null;
    var from_lsn: ?u64 = null;
    var max_records: usize = 0;
    var max_encoded_bytes: usize = 0;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot")) {
            _ = cursor.next();
            slot_name = try validateHASlotName(try cursor.value("--slot"));
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
    var safe_read_lsn: ?u64 = null;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--slot")) {
            _ = cursor.next();
            slot_name = try validateHASlotName(try cursor.value("--slot"));
        } else if (std.mem.eql(u8, arg, "--timeline-id")) {
            _ = cursor.next();
            timeline_id = try parseU64(try cursor.value("--timeline-id"));
        } else if (std.mem.eql(u8, arg, "--received-lsn")) {
            _ = cursor.next();
            received_lsn = try parseU64(try cursor.value("--received-lsn"));
        } else if (std.mem.eql(u8, arg, "--applied-lsn")) {
            _ = cursor.next();
            applied_lsn = try parseU64(try cursor.value("--applied-lsn"));
        } else if (std.mem.eql(u8, arg, "--safe-read-lsn")) {
            _ = cursor.next();
            safe_read_lsn = try parseU64(try cursor.value("--safe-read-lsn"));
        } else {
            break;
        }
    }

    return .{
        .slot_name = slot_name orelse return error.SlotNameMissing,
        .timeline_id = timeline_id orelse return error.TimelineIdMissing,
        .received_lsn = received_lsn orelse return error.ReceivedLsnMissing,
        .applied_lsn = applied_lsn orelse return error.AppliedLsnMissing,
        .safe_read_lsn = safe_read_lsn,
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
            } else if (std.mem.eql(u8, arg, "--max-retained-bytes")) {
                _ = cursor.next();
                command.retention_policy.max_retained_bytes = try parseU64(try cursor.value("--max-retained-bytes"));
            } else if (std.mem.eql(u8, arg, "--max-retained-age-ns")) {
                _ = cursor.next();
                command.retention_policy.max_retained_age_ns = try parseU64(try cursor.value("--max-retained-age-ns"));
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

fn parseCommit(alloc: Allocator, cursor: *Cursor, owned_standby_names: *[]const []const u8) !Command {
    const subcommand = cursor.next() orelse return error.CommitSubcommandMissing;
    if (std.mem.eql(u8, subcommand, "check")) {
        return .{ .commit_check = try parseCommitCheck(alloc, cursor, owned_standby_names) };
    }
    if (std.mem.eql(u8, subcommand, "append")) {
        return .{ .commit_append = try parseCommitAppend(alloc, cursor, owned_standby_names) };
    }
    return error.UnknownCommitSubcommand;
}

fn parseCommitCheck(alloc: Allocator, cursor: *Cursor, owned_standby_names: *[]const []const u8) !CommitCheckCommand {
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

fn parseCommitAppend(alloc: Allocator, cursor: *Cursor, owned_standby_names: *[]const []const u8) !CommitAppendCommand {
    var append = primary_mod.AppendOptions{};
    var payload_seen = false;
    var sync_builder = SyncPolicyBuilder{};
    defer sync_builder.deinit(alloc);

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--payload")) {
            _ = cursor.next();
            append.payload = try cursor.value("--payload");
            payload_seen = true;
        } else if (std.mem.eql(u8, arg, "--kind")) {
            _ = cursor.next();
            append.kind = try parseRecordKind(try cursor.value("--kind"));
        } else if (std.mem.eql(u8, arg, "--payload-codec")) {
            _ = cursor.next();
            append.payload_codec = try parsePayloadCodec(try cursor.value("--payload-codec"));
        } else if (std.mem.eql(u8, arg, "--shard-id")) {
            _ = cursor.next();
            append.shard_id = try parseU64(try cursor.value("--shard-id"));
        } else if (std.mem.eql(u8, arg, "--table-id")) {
            _ = cursor.next();
            append.table_id = try parseU64(try cursor.value("--table-id"));
        } else if (std.mem.eql(u8, arg, "--commit-timestamp-ns")) {
            _ = cursor.next();
            append.commit_timestamp_ns = try parseI64(try cursor.value("--commit-timestamp-ns"));
        } else if (try sync_builder.parseFlag(alloc, cursor, arg)) {
            continue;
        } else {
            break;
        }
    }

    if (!payload_seen) return error.PayloadMissing;
    const policy = (try sync_builder.finish(alloc, owned_standby_names)) orelse primary_mod.SyncPolicy{ .mode = .async };
    return .{
        .append = append,
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
        } else if (std.mem.eql(u8, arg, "--required-metadata-lsn")) {
            _ = cursor.next();
            request.required_metadata_lsn = try parseU64(try cursor.value("--required-metadata-lsn"));
        } else if (std.mem.eql(u8, arg, "--metadata-applied-lsn")) {
            _ = cursor.next();
            request.metadata_applied_lsn = try parseU64(try cursor.value("--metadata-applied-lsn"));
        } else {
            break;
        }
    }
    return request;
}

fn parseWriteCheck(cursor: *Cursor) !WriteCheckCommand {
    const subcommand = cursor.next() orelse return error.WriteSubcommandMissing;
    if (!std.mem.eql(u8, subcommand, "check")) return error.UnknownWriteSubcommand;

    var role: ?GateRole = null;
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--role")) {
            _ = cursor.next();
            role = try parseGateRole(try cursor.value("--role"));
        } else {
            break;
        }
    }

    return .{
        .role = role orelse return error.RoleMissing,
    };
}

fn parseOwnerJobCheck(cursor: *Cursor) !OwnerJobCheckCommand {
    const subcommand = cursor.next() orelse return error.OwnerJobSubcommandMissing;
    if (!std.mem.eql(u8, subcommand, "check")) return error.UnknownOwnerJobSubcommand;

    var role: ?GateRole = null;
    var kind: ?owner_job_gate.JobKind = null;
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--role")) {
            _ = cursor.next();
            role = try parseGateRole(try cursor.value("--role"));
        } else if (std.mem.eql(u8, arg, "--kind")) {
            _ = cursor.next();
            kind = try parseOwnerJobKind(try cursor.value("--kind"));
        } else {
            break;
        }
    }

    return .{
        .role = role orelse return error.RoleMissing,
        .request = .{ .kind = kind orelse return error.OwnerJobKindMissing },
    };
}

fn parseFence(cursor: *Cursor) !Command {
    const subcommand = cursor.next() orelse return error.FenceSubcommandMissing;
    if (std.mem.eql(u8, subcommand, "acquire")) {
        return .{ .fence_acquire = try parseFenceRequest(cursor) };
    }
    if (std.mem.eql(u8, subcommand, "current") or std.mem.eql(u8, subcommand, "status")) {
        return .fence_current;
    }
    return error.UnknownFenceSubcommand;
}

fn parsePromote(cursor: *Cursor) !Command {
    if (cursor.peek()) |subcommand| {
        if (std.mem.eql(u8, subcommand, "assess")) {
            _ = cursor.next();
            return .{ .promote_assess = try parsePromoteAssess(cursor) };
        }
        if (std.mem.eql(u8, subcommand, "current-fence")) {
            _ = cursor.next();
            return .promote_current_fence;
        }
        if (std.mem.eql(u8, subcommand, "--current-fence") or std.mem.eql(u8, subcommand, "--use-current-fence")) {
            _ = cursor.next();
            return .promote_current_fence;
        }
    }
    return .{ .promote = .{ .fence = try parseFenceRequest(cursor) } };
}

fn parseOperator(
    alloc: Allocator,
    cursor: *Cursor,
    owned_standbys: *[]operator.StandbySpec,
    owned_sync_names: *[]const []const u8,
) !OperatorPlanCommand {
    const subcommand = cursor.next() orelse return error.OperatorSubcommandMissing;
    if (!std.mem.eql(u8, subcommand, "plan")) return error.UnknownOperatorSubcommand;

    var standbys = std.ArrayListUnmanaged(operator.StandbySpec).empty;
    errdefer standbys.deinit(alloc);
    var command = OperatorPlanCommand{
        .spec = .{ .mode = .hot_standby },
    };
    var sync_builder = SyncPolicyBuilder{};
    defer sync_builder.deinit(alloc);
    var former_identity = standby_mod.Identity{
        .cluster_id = 0,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 0,
        .epoch = 0,
    };
    var former_node_id: ?[]const u8 = null;
    var former_last_lsn: ?u64 = null;
    var has_former_primary = false;
    var has_fence = false;
    var fence_old_primary_id: ?[]const u8 = null;
    var fence_promoted_node_id: ?[]const u8 = null;
    var fence_parent_timeline_id: ?u64 = null;
    var fence_parent_epoch: ?u64 = null;
    var fence_new_timeline_id: ?u64 = null;
    var fence_new_epoch: ?u64 = null;
    var fence_required_lsn: ?u64 = null;
    var fence_observed_lsn: ?u64 = null;
    var fence_generation: ?u64 = null;
    var fence_forced = false;
    var fence_token: ?[]const u8 = null;
    var fence_reason: []const u8 = &.{};

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--primary-admin-url")) {
            _ = cursor.next();
            command.spec.primary_admin_url = try validateHAAdminURL(try cursor.value("--primary-admin-url"));
        } else if (std.mem.eql(u8, arg, "--standby")) {
            _ = cursor.next();
            try standbys.append(alloc, .{ .name = try validateHANodeID(try cursor.value("--standby")) });
        } else if (std.mem.eql(u8, arg, "--standby-admin-url")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            standbys.items[standbys.items.len - 1].admin_url = try validateHAAdminURL(try cursor.value("--standby-admin-url"));
        } else if (std.mem.eql(u8, arg, "--standby-route-selector")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            standbys.items[standbys.items.len - 1].route_selector_configured = true;
        } else if (std.mem.eql(u8, arg, "--standby-initial-lsn")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            standbys.items[standbys.items.len - 1].initial_lsn = try parseU64(try cursor.value("--standby-initial-lsn"));
        } else if (std.mem.eql(u8, arg, "--standby-seed-manifest")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            standbys.items[standbys.items.len - 1].seed_manifest_path = try validateHAPath(try cursor.value("--standby-seed-manifest"), .manifest);
        } else if (std.mem.eql(u8, arg, "--standby-seed-content-root")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            standbys.items[standbys.items.len - 1].seed_content_root = try validateHAPath(try cursor.value("--standby-seed-content-root"), .content_root);
        } else if (std.mem.eql(u8, arg, "--standby-disabled")) {
            _ = cursor.next();
            try standbys.append(alloc, .{
                .name = try validateHANodeID(try cursor.value("--standby-disabled")),
                .desired = false,
            });
        } else if (std.mem.eql(u8, arg, "--standby-drop-slot")) {
            _ = cursor.next();
            if (standbys.items.len == 0) return error.StandbyNameMissing;
            if (standbys.items[standbys.items.len - 1].desired) return error.DropSlotRequiresDisabledStandby;
            standbys.items[standbys.items.len - 1].drop_slot_on_removal = true;
        } else if (std.mem.eql(u8, arg, "--max-lag-lsn")) {
            _ = cursor.next();
            command.spec.retention_policy.max_lag_lsn = try parseU64(try cursor.value("--max-lag-lsn"));
        } else if (std.mem.eql(u8, arg, "--max-retained-bytes")) {
            _ = cursor.next();
            command.spec.retention_policy.max_retained_bytes = try parseU64(try cursor.value("--max-retained-bytes"));
        } else if (std.mem.eql(u8, arg, "--max-retained-age-ns")) {
            _ = cursor.next();
            command.spec.retention_policy.max_retained_age_ns = try parseU64(try cursor.value("--max-retained-age-ns"));
        } else if (std.mem.eql(u8, arg, "--auto-failover")) {
            _ = cursor.next();
            command.spec.auto_failover.enabled = true;
        } else if (std.mem.eql(u8, arg, "--fencing-authority")) {
            _ = cursor.next();
            command.spec.auto_failover.fencing_authority = try parseFencingAuthority(try cursor.value("--fencing-authority"));
        } else if (std.mem.eql(u8, arg, "--auto-max-lag-lsn")) {
            _ = cursor.next();
            command.spec.auto_failover.maximum_lag_lsn = try parseU64(try cursor.value("--auto-max-lag-lsn"));
        } else if (std.mem.eql(u8, arg, "--auto-allow-remote-write")) {
            _ = cursor.next();
            command.spec.auto_failover.require_remote_apply = false;
        } else if (std.mem.eql(u8, arg, "--current-primary-id")) {
            _ = cursor.next();
            command.current_primary_id = try validateHANodeID(try cursor.value("--current-primary-id"));
        } else if (std.mem.eql(u8, arg, "--primary-admin-unavailable")) {
            _ = cursor.next();
            command.primary_admin_unavailable = true;
        } else if (std.mem.eql(u8, arg, "--fence-authority")) {
            _ = cursor.next();
            command.fencing.authority = try parseFencingAuthority(try cursor.value("--fence-authority"));
        } else if (std.mem.eql(u8, arg, "--fence-ready")) {
            _ = cursor.next();
            command.fencing.ready = true;
        } else if (std.mem.eql(u8, arg, "--fence-holder")) {
            _ = cursor.next();
            command.fencing.holder = try validateHANodeID(try cursor.value("--fence-holder"));
        } else if (std.mem.eql(u8, arg, "--fence-generation")) {
            _ = cursor.next();
            command.fencing.generation = try parseU64(try cursor.value("--fence-generation"));
        } else if (std.mem.eql(u8, arg, "--fence-reason")) {
            _ = cursor.next();
            command.fencing.reason = try cursor.value("--fence-reason");
        } else if (std.mem.eql(u8, arg, "--former-primary-id") or std.mem.eql(u8, arg, "--former-node-id")) {
            _ = cursor.next();
            has_former_primary = true;
            former_node_id = try validateHANodeID(try cursor.value(arg));
        } else if (std.mem.eql(u8, arg, "--former-cluster-id")) {
            _ = cursor.next();
            has_former_primary = true;
            former_identity.cluster_id = try parseU64(try cursor.value("--former-cluster-id"));
        } else if (std.mem.eql(u8, arg, "--former-shard-id")) {
            _ = cursor.next();
            has_former_primary = true;
            former_identity.shard_id = try parseU64(try cursor.value("--former-shard-id"));
        } else if (std.mem.eql(u8, arg, "--former-table-id")) {
            _ = cursor.next();
            has_former_primary = true;
            former_identity.table_id = try parseU64(try cursor.value("--former-table-id"));
        } else if (std.mem.eql(u8, arg, "--former-timeline-id")) {
            _ = cursor.next();
            has_former_primary = true;
            former_identity.timeline_id = try parseU64(try cursor.value("--former-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--former-epoch")) {
            _ = cursor.next();
            has_former_primary = true;
            former_identity.epoch = try parseU64(try cursor.value("--former-epoch"));
        } else if (std.mem.eql(u8, arg, "--former-last-lsn")) {
            _ = cursor.next();
            has_former_primary = true;
            former_last_lsn = try parseU64(try cursor.value("--former-last-lsn"));
        } else if (std.mem.eql(u8, arg, "--retained-from-lsn")) {
            _ = cursor.next();
            command.rejoin_policy.retained_from_lsn = try parseU64(try cursor.value("--retained-from-lsn"));
        } else if (std.mem.eql(u8, arg, "--allow-forced-rewind")) {
            _ = cursor.next();
            command.rejoin_policy.allow_rewind_after_forced_promotion = true;
        } else if (std.mem.eql(u8, arg, "--receipt-old-primary-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_old_primary_id = try validateHANodeID(try cursor.value("--receipt-old-primary-id"));
        } else if (std.mem.eql(u8, arg, "--receipt-promoted-node-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_promoted_node_id = try validateHANodeID(try cursor.value("--receipt-promoted-node-id"));
        } else if (std.mem.eql(u8, arg, "--receipt-parent-timeline-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_parent_timeline_id = try parseU64(try cursor.value("--receipt-parent-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--receipt-parent-epoch")) {
            _ = cursor.next();
            has_fence = true;
            fence_parent_epoch = try parseU64(try cursor.value("--receipt-parent-epoch"));
        } else if (std.mem.eql(u8, arg, "--receipt-new-timeline-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_new_timeline_id = try parseU64(try cursor.value("--receipt-new-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--receipt-new-epoch")) {
            _ = cursor.next();
            has_fence = true;
            fence_new_epoch = try parseU64(try cursor.value("--receipt-new-epoch"));
        } else if (std.mem.eql(u8, arg, "--receipt-required-lsn")) {
            _ = cursor.next();
            has_fence = true;
            fence_required_lsn = try parseU64(try cursor.value("--receipt-required-lsn"));
        } else if (std.mem.eql(u8, arg, "--receipt-observed-lsn")) {
            _ = cursor.next();
            has_fence = true;
            fence_observed_lsn = try parseU64(try cursor.value("--receipt-observed-lsn"));
        } else if (std.mem.eql(u8, arg, "--receipt-generation")) {
            _ = cursor.next();
            has_fence = true;
            fence_generation = try parseU64(try cursor.value("--receipt-generation"));
        } else if (std.mem.eql(u8, arg, "--receipt-token")) {
            _ = cursor.next();
            has_fence = true;
            fence_token = try cursor.value("--receipt-token");
        } else if (std.mem.eql(u8, arg, "--receipt-reason")) {
            _ = cursor.next();
            has_fence = true;
            fence_reason = try cursor.value("--receipt-reason");
        } else if (std.mem.eql(u8, arg, "--receipt-forced")) {
            _ = cursor.next();
            has_fence = true;
            fence_forced = true;
        } else if (try sync_builder.parseFlag(alloc, cursor, arg)) {
            continue;
        } else {
            break;
        }
    }

    if (has_former_primary) {
        if (former_identity.cluster_id == 0) return error.FormerClusterIdMissing;
        if (former_identity.timeline_id == 0) return error.FormerTimelineIdMissing;
        if (former_identity.epoch == 0) return error.FormerEpochMissing;
        command.former_primary = .{
            .node_id = former_node_id orelse return error.FormerPrimaryIdMissing,
            .identity = former_identity,
            .last_lsn = former_last_lsn orelse return error.FormerLastLsnMissing,
        };
    }

    if (has_fence) {
        if (!has_former_primary) return error.FormerPrimaryMissing;
        command.promotion_receipt = .{
            .identity = .{
                .cluster_id = former_identity.cluster_id,
                .shard_id = former_identity.shard_id,
                .table_id = former_identity.table_id,
                .timeline_id = fence_new_timeline_id orelse return error.FenceNewTimelineIdMissing,
                .epoch = fence_new_epoch orelse return error.FenceNewEpochMissing,
            },
            .old_primary_id = fence_old_primary_id orelse return error.FenceOldPrimaryIdMissing,
            .promoted_node_id = fence_promoted_node_id orelse return error.FencePromotedNodeIdMissing,
            .parent_timeline_id = fence_parent_timeline_id orelse return error.FenceParentTimelineIdMissing,
            .parent_epoch = fence_parent_epoch orelse return error.FenceParentEpochMissing,
            .new_timeline_id = fence_new_timeline_id.?,
            .new_epoch = fence_new_epoch.?,
            .required_lsn = fence_required_lsn orelse return error.FenceRequiredLsnMissing,
            .observed_lsn = fence_observed_lsn orelse return error.FenceObservedLsnMissing,
            .generation = fence_generation orelse return error.FenceGenerationMissing,
            .forced = fence_forced,
            .token = fence_token orelse return error.FenceTokenMissing,
            .reason = fence_reason,
        };
    }

    const standby_slice = try standbys.toOwnedSlice(alloc);
    standbys = .empty;
    owned_standbys.* = standby_slice;
    command.spec.standbys = standby_slice;
    command.spec.sync_policy = try sync_builder.finish(alloc, owned_sync_names);
    return command;
}

fn parsePromoteAssess(cursor: *Cursor) !PromoteAssessCommand {
    var command = PromoteAssessCommand{ .check = .{} };
    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--required-lsn") or std.mem.eql(u8, arg, "--at-least-lsn")) {
            _ = cursor.next();
            command.check.required_lsn = try parseU64(try cursor.value(arg));
        } else if (std.mem.eql(u8, arg, "--fencing-confirmed")) {
            _ = cursor.next();
            command.check.fencing_confirmed = true;
        } else if (std.mem.eql(u8, arg, "--current-fence") or std.mem.eql(u8, arg, "--use-current-fence")) {
            _ = cursor.next();
            command.use_current_fence = true;
        } else if (std.mem.eql(u8, arg, "--force")) {
            _ = cursor.next();
            command.check.force = true;
        } else {
            break;
        }
    }
    return command;
}

fn parseFenceRequest(cursor: *Cursor) !fencing.FenceRequest {
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
    var generation: ?u64 = null;
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
            old_primary_id = try validateHANodeID(try cursor.value("--old-primary-id"));
        } else if (std.mem.eql(u8, arg, "--promoted-node-id")) {
            _ = cursor.next();
            promoted_node_id = try validateHANodeID(try cursor.value("--promoted-node-id"));
        } else if (std.mem.eql(u8, arg, "--new-timeline-id")) {
            _ = cursor.next();
            new_timeline_id = try parseU64(try cursor.value("--new-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--new-epoch")) {
            _ = cursor.next();
            new_epoch = try parseU64(try cursor.value("--new-epoch"));
        } else if (std.mem.eql(u8, arg, "--generation")) {
            _ = cursor.next();
            generation = try parseU64(try cursor.value("--generation"));
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
    if (identity.timeline_id == 0) return error.TimelineIdMissing;
    if (identity.epoch == 0) return error.EpochMissing;

    return .{
        .identity = identity,
        .old_primary_id = old_primary_id orelse return error.OldPrimaryIdMissing,
        .promoted_node_id = promoted_node_id orelse return error.PromotedNodeIdMissing,
        .new_timeline_id = new_timeline_id orelse return error.NewTimelineIdMissing,
        .new_epoch = new_epoch orelse return error.NewEpochMissing,
        .generation = generation orelse return error.FenceGenerationMissing,
        .required_lsn = required_lsn orelse return error.RequiredLsnMissing,
        .observed_lsn = observed_lsn orelse return error.ObservedLsnMissing,
        .force = force,
        .reason = reason,
    };
}

fn parseRejoin(cursor: *Cursor) !Command {
    const subcommand = cursor.next() orelse return error.RejoinSubcommandMissing;
    const action: enum { assess, rewind, reseed } = if (std.mem.eql(u8, subcommand, "assess"))
        .assess
    else if (std.mem.eql(u8, subcommand, "rewind"))
        .rewind
    else if (std.mem.eql(u8, subcommand, "reseed"))
        .reseed
    else
        return error.UnknownRejoinSubcommand;

    var identity = standby_mod.Identity{
        .cluster_id = 0,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 0,
        .epoch = 0,
    };
    var node_id: ?[]const u8 = null;
    var last_lsn: ?u64 = null;
    var policy = rejoin.RejoinPolicy{ .retained_from_lsn = 0 };

    var has_fence = false;
    var fence_old_primary_id: ?[]const u8 = null;
    var fence_promoted_node_id: ?[]const u8 = null;
    var fence_parent_timeline_id: ?u64 = null;
    var fence_parent_epoch: ?u64 = null;
    var fence_new_timeline_id: ?u64 = null;
    var fence_new_epoch: ?u64 = null;
    var fence_required_lsn: ?u64 = null;
    var fence_observed_lsn: ?u64 = null;
    var fence_generation: ?u64 = null;
    var fence_forced = false;
    var fence_token: ?[]const u8 = null;
    var fence_reason: []const u8 = &.{};

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--node-id")) {
            _ = cursor.next();
            node_id = try validateHANodeID(try cursor.value("--node-id"));
        } else if (std.mem.eql(u8, arg, "--cluster-id")) {
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
        } else if (std.mem.eql(u8, arg, "--last-lsn")) {
            _ = cursor.next();
            last_lsn = try parseU64(try cursor.value("--last-lsn"));
        } else if (std.mem.eql(u8, arg, "--retained-from-lsn")) {
            _ = cursor.next();
            policy.retained_from_lsn = try parseU64(try cursor.value("--retained-from-lsn"));
        } else if (std.mem.eql(u8, arg, "--allow-forced-rewind")) {
            _ = cursor.next();
            policy.allow_rewind_after_forced_promotion = true;
        } else if (std.mem.eql(u8, arg, "--fence-old-primary-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_old_primary_id = try validateHANodeID(try cursor.value("--fence-old-primary-id"));
        } else if (std.mem.eql(u8, arg, "--fence-promoted-node-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_promoted_node_id = try validateHANodeID(try cursor.value("--fence-promoted-node-id"));
        } else if (std.mem.eql(u8, arg, "--fence-parent-timeline-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_parent_timeline_id = try parseU64(try cursor.value("--fence-parent-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--fence-parent-epoch")) {
            _ = cursor.next();
            has_fence = true;
            fence_parent_epoch = try parseU64(try cursor.value("--fence-parent-epoch"));
        } else if (std.mem.eql(u8, arg, "--fence-new-timeline-id")) {
            _ = cursor.next();
            has_fence = true;
            fence_new_timeline_id = try parseU64(try cursor.value("--fence-new-timeline-id"));
        } else if (std.mem.eql(u8, arg, "--fence-new-epoch")) {
            _ = cursor.next();
            has_fence = true;
            fence_new_epoch = try parseU64(try cursor.value("--fence-new-epoch"));
        } else if (std.mem.eql(u8, arg, "--fence-required-lsn")) {
            _ = cursor.next();
            has_fence = true;
            fence_required_lsn = try parseU64(try cursor.value("--fence-required-lsn"));
        } else if (std.mem.eql(u8, arg, "--fence-observed-lsn")) {
            _ = cursor.next();
            has_fence = true;
            fence_observed_lsn = try parseU64(try cursor.value("--fence-observed-lsn"));
        } else if (std.mem.eql(u8, arg, "--fence-generation")) {
            _ = cursor.next();
            has_fence = true;
            fence_generation = try parseU64(try cursor.value("--fence-generation"));
        } else if (std.mem.eql(u8, arg, "--fence-token")) {
            _ = cursor.next();
            has_fence = true;
            fence_token = try cursor.value("--fence-token");
        } else if (std.mem.eql(u8, arg, "--fence-reason")) {
            _ = cursor.next();
            has_fence = true;
            fence_reason = try cursor.value("--fence-reason");
        } else if (std.mem.eql(u8, arg, "--fence-forced")) {
            _ = cursor.next();
            has_fence = true;
            fence_forced = true;
        } else {
            break;
        }
    }

    if (identity.cluster_id == 0) return error.ClusterIdMissing;
    if (identity.timeline_id == 0) return error.TimelineIdMissing;
    if (identity.epoch == 0) return error.EpochMissing;

    const receipt = if (has_fence) fencing.Receipt{
        .identity = .{
            .cluster_id = identity.cluster_id,
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
            .timeline_id = fence_new_timeline_id orelse return error.FenceNewTimelineIdMissing,
            .epoch = fence_new_epoch orelse return error.FenceNewEpochMissing,
        },
        .old_primary_id = fence_old_primary_id orelse return error.FenceOldPrimaryIdMissing,
        .promoted_node_id = fence_promoted_node_id orelse return error.FencePromotedNodeIdMissing,
        .parent_timeline_id = fence_parent_timeline_id orelse return error.FenceParentTimelineIdMissing,
        .parent_epoch = fence_parent_epoch orelse return error.FenceParentEpochMissing,
        .new_timeline_id = fence_new_timeline_id.?,
        .new_epoch = fence_new_epoch.?,
        .required_lsn = fence_required_lsn orelse return error.FenceRequiredLsnMissing,
        .observed_lsn = fence_observed_lsn orelse return error.FenceObservedLsnMissing,
        .generation = fence_generation orelse return error.FenceGenerationMissing,
        .forced = fence_forced,
        .token = fence_token orelse return error.FenceTokenMissing,
        .reason = fence_reason,
    } else null;

    const command = RejoinAssessCommand{
        .former = .{
            .node_id = node_id orelse return error.NodeIdMissing,
            .identity = identity,
            .last_lsn = last_lsn orelse return error.LastLsnMissing,
        },
        .receipt = receipt,
        .policy = policy,
    };

    return switch (action) {
        .assess => .{ .rejoin_assess = command },
        .rewind => .{ .rejoin_rewind = command },
        .reseed => .{ .rejoin_reseed = command },
    };
}

const SyncPolicyBuilder = struct {
    mode: ?primary_mod.DurabilityMode = null,
    selection: primary_mod.StandbySelection = .any,
    required: usize = 1,
    required_set: bool = false,
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
            self.required_set = true;
            return true;
        }
        if (std.mem.eql(u8, arg, "--sync-standby")) {
            _ = cursor.next();
            try self.standby_names.append(alloc, try validateHANodeID(try cursor.value("--sync-standby")));
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
            self.required_set or
            self.failure_policy != .block;
        if (!configured) return null;
        if (self.required == 0) return error.SyncRequiredMustBePositive;
        if (self.selection == .all and (self.required_set or self.standby_names.items.len == 0)) return error.InvalidSyncPolicy;

        const names = try self.standby_names.toOwnedSlice(alloc);
        self.standby_names = .empty;
        owned_standby_names.* = names;
        return primary_mod.SyncPolicy{
            .mode = self.mode orelse .remote_write,
            .selection = self.selection,
            .required = if (self.selection == .all) names.len else self.required,
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

const HAPathField = enum {
    manifest,
    content_root,
};

fn validateHASlotName(raw: []const u8) ![]const u8 {
    switch (validation.classifyHAString(raw)) {
        .ok => {},
        .missing => return error.SlotNameMissing,
        .padded => return error.InvalidSlotName,
    }
    if (!validation.isIdentifier(raw)) return error.InvalidSlotName;
    return raw;
}

fn validateHANodeID(raw: []const u8) ![]const u8 {
    switch (validation.classifyHAString(raw)) {
        .ok => {},
        .missing, .padded => return error.InvalidNodeId,
    }
    if (!validation.isIdentifier(raw)) return error.InvalidNodeId;
    return raw;
}

fn validateHAPath(raw: []const u8, field: HAPathField) ![]const u8 {
    switch (validation.classifyHAString(raw)) {
        .ok => {},
        .missing => return switch (field) {
            .manifest => error.ManifestPathMissing,
            .content_root => error.ContentRootMissing,
        },
        .padded => return haPathInvalidError(field),
    }
    if (!validation.isAbsoluteNormalizedPath(raw)) return haPathInvalidError(field);
    return raw;
}

fn haPathInvalidError(field: HAPathField) anyerror {
    return switch (field) {
        .manifest => error.ManifestPathInvalid,
        .content_root => error.ContentRootInvalid,
    };
}

fn validateHAAdminURL(raw: []const u8) ![]const u8 {
    switch (validation.classifyHAString(raw)) {
        .ok => {},
        .missing, .padded => return error.InvalidHAAdminURL,
    }
    if (!validation.isHTTPURLWithHostNoHiddenWhitespace(raw)) return error.InvalidHAAdminURL;
    return raw;
}

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

fn parseFencingAuthority(raw: []const u8) !operator.FencingAuthority {
    if (std.mem.eql(u8, raw, "none")) return .none;
    if (std.mem.eql(u8, raw, "kubernetes_lease") or std.mem.eql(u8, raw, "kubernetes-lease")) return .kubernetes_lease;
    if (std.mem.eql(u8, raw, "storage_fence") or std.mem.eql(u8, raw, "storage-fence")) return .storage_fence;
    if (std.mem.eql(u8, raw, "metadata_raft") or std.mem.eql(u8, raw, "metadata-raft")) return .metadata_raft;
    if (std.mem.eql(u8, raw, "external")) return .external;
    return error.InvalidFencingAuthority;
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

fn parseGateRole(raw: []const u8) !GateRole {
    if (std.mem.eql(u8, raw, "primary")) return .primary;
    if (std.mem.eql(u8, raw, "standby")) return .standby;
    return error.InvalidGateRole;
}

fn parseOwnerJobKind(raw: []const u8) !owner_job_gate.JobKind {
    if (std.mem.eql(u8, raw, "compaction_publish") or std.mem.eql(u8, raw, "compaction-publish")) return .compaction_publish;
    if (std.mem.eql(u8, raw, "derived_effect_writer") or std.mem.eql(u8, raw, "derived-effect-writer")) return .derived_effect_writer;
    if (std.mem.eql(u8, raw, "enrichment_writer") or std.mem.eql(u8, raw, "enrichment-writer")) return .enrichment_writer;
    if (std.mem.eql(u8, raw, "retention_advance") or std.mem.eql(u8, raw, "retention-advance")) return .retention_advance;
    return error.InvalidOwnerJobKind;
}

fn parseRecordKind(raw: []const u8) !replication_record.RecordKind {
    if (std.mem.eql(u8, raw, "batch_mutation") or std.mem.eql(u8, raw, "batch-mutation")) return .batch_mutation;
    if (std.mem.eql(u8, raw, "metadata_mutation") or std.mem.eql(u8, raw, "metadata-mutation")) return .metadata_mutation;
    if (std.mem.eql(u8, raw, "derived_effect") or std.mem.eql(u8, raw, "derived-effect")) return .derived_effect;
    if (std.mem.eql(u8, raw, "checkpoint")) return .checkpoint;
    if (std.mem.eql(u8, raw, "manifest")) return .manifest;
    if (std.mem.eql(u8, raw, "truncate")) return .truncate;
    return error.InvalidRecordKind;
}

fn parsePayloadCodec(raw: []const u8) !replication_record.PayloadCodec {
    if (std.mem.eql(u8, raw, "raw")) return .raw;
    if (std.mem.eql(u8, raw, "json")) return .json;
    if (std.mem.eql(u8, raw, "binary")) return .binary;
    return error.InvalidPayloadCodec;
}

fn parseU64(raw: []const u8) !u64 {
    return std.fmt.parseInt(u64, raw, 10) catch return error.InvalidInteger;
}

fn parseI64(raw: []const u8) !i64 {
    return std.fmt.parseInt(i64, raw, 10) catch return error.InvalidInteger;
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

    var list = try parse(alloc, &.{ "slot", "list", "--max-lag-lsn", "50", "--max-retained-bytes", "4096", "--max-retained-age-ns", "1000000" });
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 50), list.command.slot_list.retention_policy.max_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4096), list.command.slot_list.retention_policy.max_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1000000), list.command.slot_list.retention_policy.max_retained_age_ns);
}

test "storage.ha admin cli parses standby seed commands" {
    const alloc = std.testing.allocator;

    var begin = try parse(alloc, &.{ "seed", "begin", "--slot", "standby-a", "--manifest-id", "base-0001" });
    defer begin.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", begin.command.seed.begin.slot_name);
    try std.testing.expectEqualStrings("base-0001", begin.command.seed.begin.manifest_id);

    var finish = try parse(alloc, &.{ "seed", "finish", "--manifest", "/tmp/base-0001.afha" });
    defer finish.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/base-0001.afha", finish.command.seed.finish.manifest_path);

    var bootstrap = try parse(alloc, &.{ "seed", "bootstrap", "--manifest", "/tmp/base-0001.afha", "--content-root", "/tmp/base-0001" });
    defer bootstrap.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/base-0001.afha", bootstrap.command.seed.bootstrap.manifest_path);
    try std.testing.expectEqualStrings("/tmp/base-0001", bootstrap.command.seed.bootstrap.content_root.?);
}

test "storage.ha admin cli rejects padded and invalid HA strings" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidSlotName, parse(alloc, &.{ "slot", "create", " standby-a" }));
    try std.testing.expectError(error.InvalidSlotName, parse(alloc, &.{ "stream", "start", "--slot", "standby a", "--from-lsn", "1" }));
    try std.testing.expectError(error.InvalidSlotName, parse(alloc, &.{ "standby", "ack", "--slot", "standby-a\n", "--timeline-id", "1", "--received-lsn", "1", "--applied-lsn", "1" }));
    try std.testing.expectError(error.InvalidNodeId, parse(alloc, &.{ "status", "primary", "--sync-mode", "remote-write", "--sync-standby", " standby-a" }));
    try std.testing.expectError(error.ManifestPathInvalid, parse(alloc, &.{ "seed", "finish", "--manifest", "base.afha" }));
    try std.testing.expectError(error.ManifestPathInvalid, parse(alloc, &.{ "seed", "finish", "--manifest", "/tmp/../base.afha" }));
    try std.testing.expectError(error.ContentRootInvalid, parse(alloc, &.{ "seed", "bootstrap", "--manifest", "/tmp/base.afha", "--content-root", "/tmp//base" }));
    try std.testing.expectError(error.InvalidHAAdminURL, parse(alloc, &.{ "operator", "plan", "--primary-admin-url", " http://primary-ha.default.svc:8081" }));
    try std.testing.expectError(error.InvalidHAAdminURL, parse(alloc, &.{ "operator", "plan", "--primary-admin-url", "http://primary ha.default.svc:8081" }));
    try std.testing.expectError(error.InvalidHAAdminURL, parse(alloc, &.{ "operator", "plan", "--primary-admin-url", "http://primary-ha.default.svc:8081/\tadmin" }));
    try std.testing.expectError(error.InvalidHAAdminURL, parse(alloc, &.{ "operator", "plan", "--primary-admin-url", "not-a-url" }));
    try std.testing.expectError(error.InvalidHAAdminURL, parse(alloc, &.{ "operator", "plan", "--primary-admin-url", "ftp://primary-ha.default.svc:8081" }));
    try std.testing.expectError(error.InvalidNodeId, parse(alloc, &.{ "operator", "plan", "--standby", "standby a" }));
    try std.testing.expectError(error.InvalidNodeId, parse(alloc, &.{ "fence", "acquire", "--cluster-id", "1", "--timeline-id", "1", "--epoch", "1", "--old-primary-id", "primary-a", "--promoted-node-id", " standby-b", "--new-timeline-id", "2", "--new-epoch", "2", "--required-lsn", "3", "--observed-lsn", "3" }));
    try std.testing.expectError(error.InvalidNodeId, parse(alloc, &.{ "rejoin", "assess", "--node-id", "primary a", "--cluster-id", "1", "--timeline-id", "1", "--epoch", "1", "--last-lsn", "3" }));
}

test "storage.ha admin cli parses primary status with sync policy" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "status",                "primary",
        "--view",                "metrics",
        "--max-lag-lsn",         "50",
        "--max-retained-bytes",  "4096",
        "--max-retained-age-ns", "1000000",
        "--sync-mode",           "remote-apply",
        "--sync-selection",      "first",
        "--sync-required",       "2",
        "--sync-standby",        "a",
        "--sync-standby",        "b",
        "--sync-failure",        "fail-closed",
    });
    defer plan.deinit(alloc);

    const command = plan.command.primary_status;
    try std.testing.expectEqual(StatusView.metrics, command.view);
    try std.testing.expectEqual(@as(u64, 50), command.retention_policy.max_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4096), command.retention_policy.max_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1000000), command.retention_policy.max_retained_age_ns);
    const policy = command.sync_policy.?;
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_apply, policy.mode);
    try std.testing.expectEqual(primary_mod.StandbySelection.first, policy.selection);
    try std.testing.expectEqual(@as(usize, 2), policy.required);
    try std.testing.expectEqual(primary_mod.FailurePolicy.fail_closed, policy.failure_policy);
    try std.testing.expectEqual(@as(usize, 2), policy.standby_names.len);
    try std.testing.expectEqualStrings("a", policy.standby_names[0]);
    try std.testing.expectEqualStrings("b", policy.standby_names[1]);
}

test "storage.ha admin cli treats ALL sync policy as all named standbys" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "status",           "primary",
        "--sync-mode",      "remote-apply",
        "--sync-selection", "all",
        "--sync-standby",   "a",
        "--sync-standby",   "b",
    });
    defer plan.deinit(alloc);

    const policy = plan.command.primary_status.sync_policy.?;
    try std.testing.expectEqual(primary_mod.StandbySelection.all, policy.selection);
    try std.testing.expectEqual(@as(usize, 2), policy.required);

    try std.testing.expectError(error.InvalidSyncPolicy, parse(alloc, &.{
        "status",           "primary",
        "--sync-mode",      "remote-apply",
        "--sync-selection", "all",
        "--sync-required",  "1",
        "--sync-standby",   "a",
    }));
    try std.testing.expectError(error.InvalidSyncPolicy, parse(alloc, &.{
        "status",           "primary",
        "--sync-mode",      "remote-apply",
        "--sync-selection", "all",
    }));
}

test "storage.ha admin cli parses operator plan command" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "operator",                             "plan",
        "--primary-admin-url",                  "http://primary-ha.default.svc:8081",
        "--standby",                            "standby-a",
        "--standby-admin-url",                  "http://standby-a-ha.default.svc:8081",
        "--standby-route-selector",             "--standby-initial-lsn",
        "3",                                    "--standby-seed-manifest",
        "/backup/base-standby-a-3.afha",        "--standby-seed-content-root",
        "/backup/base-standby-a-3",             "--standby-disabled",
        "standby-b",                            "--standby-admin-url",
        "http://standby-b-ha.default.svc:8081", "--standby-drop-slot",
        "--max-lag-lsn",                        "50",
        "--max-retained-bytes",                 "4096",
        "--max-retained-age-ns",                "1000000",
        "--sync-mode",                          "remote-apply",
        "--sync-standby",                       "standby-a",
        "--auto-failover",                      "--fencing-authority",
        "kubernetes-lease",                     "--auto-max-lag-lsn",
        "2",                                    "--current-primary-id",
        "primary-a",                            "--primary-admin-unavailable",
        "--fence-authority",                    "kubernetes_lease",
        "--fence-ready",                        "--fence-holder",
        "standby-a",                            "--fence-generation",
        "7",                                    "--fence-reason",
        "LeaseAcquired",                        "--former-primary-id",
        "primary-a",                            "--former-cluster-id",
        "100",                                  "--former-timeline-id",
        "1",                                    "--former-epoch",
        "1",                                    "--former-last-lsn",
        "12",                                   "--retained-from-lsn",
        "8",                                    "--receipt-old-primary-id",
        "primary-a",                            "--receipt-promoted-node-id",
        "standby-a",                            "--receipt-parent-timeline-id",
        "1",                                    "--receipt-parent-epoch",
        "1",                                    "--receipt-new-timeline-id",
        "2",                                    "--receipt-new-epoch",
        "2",                                    "--receipt-required-lsn",
        "10",                                   "--receipt-observed-lsn",
        "10",                                   "--receipt-generation",
        "3",                                    "--receipt-token",
        "token",                                "--receipt-reason",
        "operator-approved",
    });
    defer plan.deinit(alloc);

    const command = plan.command.operator_plan;
    try std.testing.expectEqual(operator.Mode.hot_standby, command.spec.mode);
    try std.testing.expectEqualStrings("http://primary-ha.default.svc:8081", command.spec.primary_admin_url.?);
    try std.testing.expectEqual(@as(usize, 2), command.spec.standbys.len);
    try std.testing.expectEqualStrings("standby-a", command.spec.standbys[0].name);
    try std.testing.expectEqualStrings("http://standby-a-ha.default.svc:8081", command.spec.standbys[0].admin_url.?);
    try std.testing.expect(command.spec.standbys[0].route_selector_configured);
    try std.testing.expectEqual(@as(?u64, 3), command.spec.standbys[0].initial_lsn);
    try std.testing.expectEqualStrings("/backup/base-standby-a-3.afha", command.spec.standbys[0].seed_manifest_path.?);
    try std.testing.expectEqualStrings("/backup/base-standby-a-3", command.spec.standbys[0].seed_content_root.?);
    try std.testing.expectEqualStrings("standby-b", command.spec.standbys[1].name);
    try std.testing.expectEqualStrings("http://standby-b-ha.default.svc:8081", command.spec.standbys[1].admin_url.?);
    try std.testing.expect(!command.spec.standbys[1].desired);
    try std.testing.expect(command.spec.standbys[1].drop_slot_on_removal);
    try std.testing.expectEqual(@as(u64, 50), command.spec.retention_policy.max_lag_lsn);
    try std.testing.expectEqual(@as(u64, 4096), command.spec.retention_policy.max_retained_bytes);
    try std.testing.expectEqual(@as(u64, 1000000), command.spec.retention_policy.max_retained_age_ns);
    try std.testing.expect(command.spec.auto_failover.enabled);
    try std.testing.expectEqual(operator.FencingAuthority.kubernetes_lease, command.spec.auto_failover.fencing_authority);
    try std.testing.expectEqual(@as(u64, 2), command.spec.auto_failover.maximum_lag_lsn);
    try std.testing.expectEqualStrings("primary-a", command.current_primary_id.?);
    try std.testing.expect(command.primary_admin_unavailable);
    try std.testing.expectEqual(operator.FencingAuthority.kubernetes_lease, command.fencing.authority);
    try std.testing.expect(command.fencing.ready);
    try std.testing.expectEqualStrings("standby-a", command.fencing.holder.?);
    try std.testing.expectEqual(@as(?u64, 7), command.fencing.generation);
    try std.testing.expectEqualStrings("LeaseAcquired", command.fencing.reason);
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_apply, command.spec.sync_policy.?.mode);
    try std.testing.expectEqualStrings("standby-a", command.spec.sync_policy.?.standby_names[0]);
    try std.testing.expectEqualStrings("primary-a", command.former_primary.?.node_id);
    try std.testing.expectEqual(@as(u64, 0), command.former_primary.?.identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), command.former_primary.?.identity.table_id);
    try std.testing.expectEqual(@as(u64, 1), command.former_primary.?.identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 12), command.former_primary.?.last_lsn);
    try std.testing.expectEqual(@as(u64, 8), command.rejoin_policy.retained_from_lsn);
    try std.testing.expectEqualStrings("standby-a", command.promotion_receipt.?.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 2), command.promotion_receipt.?.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 10), command.promotion_receipt.?.observed_lsn);
    try std.testing.expectEqual(@as(u64, 3), command.promotion_receipt.?.generation);

    try std.testing.expectError(error.DropSlotRequiresDisabledStandby, parse(alloc, &.{ "operator", "plan", "--standby", "standby-a", "--standby-drop-slot" }));
}

test "storage.ha admin cli renders versioned json command plan" {
    const alloc = std.testing.allocator;
    var plan = try parse(alloc, &.{
        "--prometheus",
        "status",
        "primary",
        "--view",
        "metrics",
        "--max-lag-lsn",
        "50",
        "--max-retained-bytes",
        "4096",
        "--max-retained-age-ns",
        "1000000",
        "--sync-mode",
        "remote-apply",
        "--sync-selection",
        "first",
        "--sync-standby",
        "standby-a",
        "--sync-failure",
        "degrade-to-async",
    });
    defer plan.deinit(alloc);

    const rendered = try renderJsonAlloc(alloc, plan);
    defer alloc.free(rendered);

    try expectContains(rendered, "\"schema_version\":1");
    try expectContains(rendered, "\"output\":\"prometheus\"");
    try expectContains(rendered, "\"primary_status\"");
    try expectContains(rendered, "\"view\":\"metrics\"");
    try expectContains(rendered, "\"max_lag_lsn\":50");
    try expectContains(rendered, "\"max_retained_bytes\":4096");
    try expectContains(rendered, "\"max_retained_age_ns\":1000000");
    try expectContains(rendered, "\"mode\":\"remote_apply\"");
    try expectContains(rendered, "\"selection\":\"first\"");
    try expectContains(rendered, "\"failure_policy\":\"degrade_to_async\"");
    try expectContains(rendered, "\"standby-a\"");
}

test "storage.ha admin cli parses stream ack commit and read checks" {
    const alloc = std.testing.allocator;

    var stream = try parse(alloc, &.{ "stream", "--slot", "standby-a", "--from-lsn", "7", "--max-records", "10" });
    defer stream.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", stream.command.start_replication.slot_name);
    try std.testing.expectEqual(@as(u64, 7), stream.command.start_replication.from_lsn);
    try std.testing.expectEqual(@as(usize, 10), stream.command.start_replication.max_records);

    var stream_start = try parse(alloc, &.{ "stream", "start", "--slot", "standby-a", "--from-lsn", "7" });
    defer stream_start.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", stream_start.command.start_replication.slot_name);

    var stream_once = try parse(alloc, &.{ "stream", "once", "--slot", "standby-a" });
    defer stream_once.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", stream_once.command.stream_once.slot_name);

    var ack = try parse(alloc, &.{ "standby", "ack", "--slot", "standby-a", "--timeline-id", "2", "--received-lsn", "9", "--applied-lsn", "8", "--safe-read-lsn", "7" });
    defer ack.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), ack.command.standby_status_update.timeline_id);
    try std.testing.expectEqual(@as(u64, 9), ack.command.standby_status_update.received_lsn);
    try std.testing.expectEqual(@as(u64, 8), ack.command.standby_status_update.applied_lsn);
    try std.testing.expectEqual(@as(?u64, 7), ack.command.standby_status_update.safe_read_lsn);

    var commit = try parse(alloc, &.{ "commit", "check", "--target-lsn", "9", "--sync-mode", "remote-write", "--sync-standby", "standby-a" });
    defer commit.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), commit.command.commit_check.target_lsn);
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_write, commit.command.commit_check.policy.mode);
    try std.testing.expectEqualStrings("standby-a", commit.command.commit_check.policy.standby_names[0]);

    var append = try parse(alloc, &.{
        "commit",                "append",
        "--payload",             "{\"id\":\"a\"}",
        "--payload-codec",       "json",
        "--kind",                "metadata-mutation",
        "--commit-timestamp-ns", "123",
        "--sync-mode",           "remote-apply",
        "--sync-standby",        "standby-a",
        "--sync-failure",        "fail-closed",
    });
    defer append.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.metadata_mutation, append.command.commit_append.append.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.json, append.command.commit_append.append.payload_codec);
    try std.testing.expectEqual(@as(i64, 123), append.command.commit_append.append.commit_timestamp_ns);
    try std.testing.expectEqual(primary_mod.DurabilityMode.remote_apply, append.command.commit_append.policy.mode);
    try std.testing.expectEqual(primary_mod.FailurePolicy.fail_closed, append.command.commit_append.policy.failure_policy);
    try std.testing.expectEqualStrings("standby-a", append.command.commit_append.policy.standby_names[0]);

    var read = try parse(alloc, &.{
        "read",
        "check",
        "--at-least-lsn",
        "9",
        "--required-metadata-lsn",
        "7",
        "--metadata-applied-lsn",
        "6",
    });
    defer read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Consistency.at_least_lsn, read.command.read_check.consistency);
    try std.testing.expectEqual(@as(?u64, 9), read.command.read_check.required_lsn);
    try std.testing.expectEqual(@as(?u64, 7), read.command.read_check.required_metadata_lsn);
    try std.testing.expectEqual(@as(?u64, 6), read.command.read_check.metadata_applied_lsn);

    var write = try parse(alloc, &.{ "write", "check", "--role", "standby" });
    defer write.deinit(alloc);
    try std.testing.expectEqual(GateRole.standby, write.command.write_check.role);

    var owner_job = try parse(alloc, &.{ "owner-job", "check", "--role", "primary", "--kind", "derived-effect-writer" });
    defer owner_job.deinit(alloc);
    try std.testing.expectEqual(GateRole.primary, owner_job.command.owner_job_check.role);
    try std.testing.expectEqual(owner_job_gate.JobKind.derived_effect_writer, owner_job.command.owner_job_check.request.kind);
}

test "storage.ha admin cli parses fenced promotion request" {
    const alloc = std.testing.allocator;
    var acquired = try parse(alloc, &.{
        "fence",
        "acquire",
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
        "--generation",
        "9",
        "--required-lsn",
        "100",
        "--observed-lsn",
        "99",
        "--force",
        "--reason",
        "operator-approved",
    });
    defer acquired.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), acquired.command.fence_acquire.identity.cluster_id);
    try std.testing.expectEqualStrings("primary-a", acquired.command.fence_acquire.old_primary_id);
    try std.testing.expectEqualStrings("standby-b", acquired.command.fence_acquire.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 6), acquired.command.fence_acquire.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 9), acquired.command.fence_acquire.generation);
    try std.testing.expect(acquired.command.fence_acquire.force);

    var current = try parse(alloc, &.{ "fence", "current" });
    defer current.deinit(alloc);
    switch (current.command) {
        .fence_current => {},
        else => return error.TestExpectedEqual,
    }

    var direct_assess = try parse(alloc, &.{ "promote", "assess", "--required-lsn", "100", "--fencing-confirmed" });
    defer direct_assess.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 100), direct_assess.command.promote_assess.check.required_lsn);
    try std.testing.expect(direct_assess.command.promote_assess.check.fencing_confirmed);
    try std.testing.expect(!direct_assess.command.promote_assess.use_current_fence);

    var fenced_assess = try parse(alloc, &.{ "promote", "assess", "--current-fence" });
    defer fenced_assess.deinit(alloc);
    try std.testing.expect(fenced_assess.command.promote_assess.use_current_fence);

    var current_fence_promote = try parse(alloc, &.{ "promote", "--current-fence" });
    defer current_fence_promote.deinit(alloc);
    switch (current_fence_promote.command) {
        .promote_current_fence => {},
        else => return error.TestExpectedEqual,
    }

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
        "--generation",
        "9",
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

    var whole_instance = try parse(alloc, &.{
        "fence",
        "acquire",
        "--cluster-id",
        "1",
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
        "--generation",
        "9",
        "--required-lsn",
        "100",
        "--observed-lsn",
        "99",
    });
    defer whole_instance.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), whole_instance.command.fence_acquire.identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), whole_instance.command.fence_acquire.identity.table_id);
}

test "storage.ha admin cli parses former primary rejoin assessment" {
    const alloc = std.testing.allocator;

    var no_fence = try parse(alloc, &.{
        "rejoin",              "assess",
        "--node-id",           "primary-a",
        "--cluster-id",        "1",
        "--timeline-id",       "4",
        "--epoch",             "5",
        "--last-lsn",          "12",
        "--retained-from-lsn", "8",
    });
    defer no_fence.deinit(alloc);
    try std.testing.expectEqualStrings("primary-a", no_fence.command.rejoin_assess.former.node_id);
    try std.testing.expectEqual(@as(u64, 0), no_fence.command.rejoin_assess.former.identity.shard_id);
    try std.testing.expectEqual(@as(u64, 0), no_fence.command.rejoin_assess.former.identity.table_id);
    try std.testing.expectEqual(@as(u64, 4), no_fence.command.rejoin_assess.former.identity.timeline_id);
    try std.testing.expectEqual(@as(u64, 12), no_fence.command.rejoin_assess.former.last_lsn);
    try std.testing.expectEqual(@as(u64, 8), no_fence.command.rejoin_assess.policy.retained_from_lsn);
    try std.testing.expect(no_fence.command.rejoin_assess.receipt == null);

    var fenced = try parse(alloc, &.{
        "rejoin",                "assess",
        "--node-id",             "primary-a",
        "--cluster-id",          "1",
        "--shard-id",            "2",
        "--table-id",            "3",
        "--timeline-id",         "4",
        "--epoch",               "5",
        "--last-lsn",            "12",
        "--retained-from-lsn",   "8",
        "--allow-forced-rewind", "--fence-old-primary-id",
        "primary-a",             "--fence-promoted-node-id",
        "standby-b",             "--fence-parent-timeline-id",
        "4",                     "--fence-parent-epoch",
        "5",                     "--fence-new-timeline-id",
        "6",                     "--fence-new-epoch",
        "7",                     "--fence-required-lsn",
        "10",                    "--fence-observed-lsn",
        "10",                    "--fence-generation",
        "2",                     "--fence-token",
        "token",                 "--fence-reason",
        "operator-approved",     "--fence-forced",
    });
    defer fenced.deinit(alloc);
    const receipt = fenced.command.rejoin_assess.receipt.?;
    try std.testing.expect(fenced.command.rejoin_assess.policy.allow_rewind_after_forced_promotion);
    try std.testing.expectEqualStrings("primary-a", receipt.old_primary_id);
    try std.testing.expectEqualStrings("standby-b", receipt.promoted_node_id);
    try std.testing.expectEqual(@as(u64, 4), receipt.parent_timeline_id);
    try std.testing.expectEqual(@as(u64, 6), receipt.new_timeline_id);
    try std.testing.expectEqual(@as(u64, 10), receipt.observed_lsn);
    try std.testing.expectEqual(@as(u64, 2), receipt.generation);
    try std.testing.expect(receipt.forced);
    try std.testing.expectEqualStrings("operator-approved", receipt.reason);

    var rewind = try parse(alloc, &.{
        "rejoin",                     "rewind",
        "--node-id",                  "primary-a",
        "--cluster-id",               "1",
        "--timeline-id",              "4",
        "--epoch",                    "5",
        "--last-lsn",                 "12",
        "--retained-from-lsn",        "8",
        "--fence-old-primary-id",     "primary-a",
        "--fence-promoted-node-id",   "standby-b",
        "--fence-parent-timeline-id", "4",
        "--fence-parent-epoch",       "5",
        "--fence-new-timeline-id",    "6",
        "--fence-new-epoch",          "7",
        "--fence-required-lsn",       "10",
        "--fence-observed-lsn",       "10",
        "--fence-generation",         "2",
        "--fence-token",              "token",
    });
    defer rewind.deinit(alloc);
    try std.testing.expectEqualStrings("primary-a", rewind.command.rejoin_rewind.former.node_id);
    try std.testing.expectEqual(@as(u64, 8), rewind.command.rejoin_rewind.policy.retained_from_lsn);

    var reseed = try parse(alloc, &.{
        "rejoin",                     "reseed",
        "--node-id",                  "primary-a",
        "--cluster-id",               "1",
        "--timeline-id",              "4",
        "--epoch",                    "5",
        "--last-lsn",                 "12",
        "--retained-from-lsn",        "11",
        "--fence-old-primary-id",     "primary-a",
        "--fence-promoted-node-id",   "standby-b",
        "--fence-parent-timeline-id", "4",
        "--fence-parent-epoch",       "5",
        "--fence-new-timeline-id",    "6",
        "--fence-new-epoch",          "7",
        "--fence-required-lsn",       "10",
        "--fence-observed-lsn",       "10",
        "--fence-generation",         "2",
        "--fence-token",              "token",
    });
    defer reseed.deinit(alloc);
    try std.testing.expectEqualStrings("primary-a", reseed.command.rejoin_reseed.former.node_id);
    try std.testing.expectEqual(@as(u64, 11), reseed.command.rejoin_reseed.policy.retained_from_lsn);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
