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

//! HA owner-job decisions.
//!
//! Background jobs that mutate durable table/shard state, such as compaction
//! publication, derived index effects, enrichment writes, and retention
//! advancement, must run only on the current write owner. Standbys can keep
//! read-only observers alive, but owner-only mutation jobs stay disabled until
//! a fenced promotion is handed off to the primary role.

const std = @import("std");
const primary_mod = @import("primary.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const JobKind = enum {
    compaction_publish,
    derived_effect_writer,
    enrichment_writer,
    retention_advance,
};

pub const Role = enum {
    primary,
    standby,
    promoted_standby,
};

pub const Action = enum {
    run,
    disable_on_standby,
    open_promoted_primary,
};

pub const Request = struct {
    kind: JobKind,
    expected_identity: ?standby_mod.Identity = null,
};

pub const Decision = struct {
    kind: JobKind,
    role: Role,
    action: Action,
    identity: standby_mod.Identity,
    durable_lsn: u64,
    next_lsn: u64,
    promotion_handoff: ?standby_mod.PromotionHandoff = null,

    pub fn canRun(self: Decision) bool {
        return self.action == .run;
    }
};

pub fn evaluatePrimary(primary: *const primary_mod.Primary, request: Request) !Decision {
    if (request.expected_identity) |expected| try validateIdentity(primary.identity, expected);
    return .{
        .kind = request.kind,
        .role = .primary,
        .action = .run,
        .identity = primary.identity,
        .durable_lsn = primary.lastLsn(),
        .next_lsn = primary.nextLsn(),
    };
}

pub fn evaluateStandby(standby: *standby_mod.Standby, request: Request) !Decision {
    const handoff = standby.promotedPrimaryHandoff() catch |err| switch (err) {
        error.StandbyNotPromoted => null,
        else => return err,
    };
    if (handoff) |promotion_handoff| {
        if (request.expected_identity) |expected| try validateIdentity(promotion_handoff.identity, expected);
        return .{
            .kind = request.kind,
            .role = .promoted_standby,
            .action = .open_promoted_primary,
            .identity = promotion_handoff.identity,
            .durable_lsn = promotion_handoff.switch_lsn,
            .next_lsn = promotion_handoff.next_lsn,
            .promotion_handoff = promotion_handoff,
        };
    }

    if (request.expected_identity) |expected| try validateIdentity(standby.identity, expected);

    const progress = standby.currentProgress();
    return .{
        .kind = request.kind,
        .role = .standby,
        .action = .disable_on_standby,
        .identity = standby.identity,
        .durable_lsn = progress.received_lsn,
        .next_lsn = progress.received_lsn + 1,
    };
}

fn validateIdentity(actual: standby_mod.Identity, expected: standby_mod.Identity) !void {
    if (actual.cluster_id != expected.cluster_id) return error.WrongCluster;
    if (actual.shard_id != expected.shard_id) return error.WrongShard;
    if (actual.table_id != expected.table_id) return error.WrongTable;
    if (actual.timeline_id != expected.timeline_id) return error.WrongTimeline;
    if (actual.epoch != expected.epoch) return error.WrongEpoch;
}

const TestPaths = struct {
    log: [:0]u8,
    slots: [:0]u8,
    progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.log);
        alloc.free(self.slots);
        alloc.free(self.progress);
    }
};

fn testPaths(alloc: std.mem.Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const log_raw = try allocPath(alloc, name, "log", nonce);
    defer alloc.free(log_raw);
    const slots_raw = try allocPath(alloc, name, "slots", nonce);
    defer alloc.free(slots_raw);
    const progress_raw = try allocPath(alloc, name, "progress", nonce);
    defer alloc.free(progress_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), log_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), slots_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), progress_raw) catch {};

    return .{
        .log = try alloc.dupeZ(u8, log_raw),
        .slots = try alloc.dupeZ(u8, slots_raw),
        .progress = try alloc.dupeZ(u8, progress_raw),
    };
}

fn allocPath(alloc: std.mem.Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-owner-job-gate-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn record(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

fn noOpApply(_: *anyopaque, _: replication_record.RecordView) anyerror!void {}

test "storage.ha owner job gate runs owner jobs only on primary role" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "primary");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .kind = .derived_effect, .payload = "effect" });

    const decision = try evaluatePrimary(&primary, .{
        .kind = .derived_effect_writer,
        .expected_identity = identity,
    });
    try std.testing.expect(decision.canRun());
    try std.testing.expectEqual(JobKind.derived_effect_writer, decision.kind);
    try std.testing.expectEqual(Role.primary, decision.role);
    try std.testing.expectEqual(Action.run, decision.action);
    try std.testing.expectEqual(@as(u64, 1), decision.durable_lsn);
    try std.testing.expectEqual(@as(u64, 2), decision.next_lsn);

    var wrong = identity;
    wrong.epoch = 2;
    try std.testing.expectError(error.WrongEpoch, evaluatePrimary(&primary, .{
        .kind = .retention_advance,
        .expected_identity = wrong,
    }));
}

test "storage.ha owner job gate disables standby jobs until promoted-primary handoff" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "standby");
    defer paths.deinit(alloc);
    const identity = testIdentity();
    const promoted_identity = standby_mod.Identity{
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = 2,
        .epoch = 2,
    };

    var ctx: u8 = 0;
    const handoff = blk: {
        var standby = try standby_mod.Standby.open(alloc, paths.log.ptr, paths.progress.ptr, identity, .{});
        defer standby.close();

        var decision = try evaluateStandby(&standby, .{
            .kind = .compaction_publish,
            .expected_identity = identity,
        });
        try std.testing.expect(!decision.canRun());
        try std.testing.expectEqual(Role.standby, decision.role);
        try std.testing.expectEqual(Action.disable_on_standby, decision.action);
        try std.testing.expect(decision.promotion_handoff == null);

        _ = try standby.receive(record(identity, 1, "before-promotion"));
        try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&ctx, noOpApply));
        _ = try standby.promote(.{
            .new_timeline_id = promoted_identity.timeline_id,
            .new_epoch = promoted_identity.epoch,
            .required_lsn = 1,
            .fencing_confirmed = true,
        });

        decision = try evaluateStandby(&standby, .{
            .kind = .compaction_publish,
            .expected_identity = promoted_identity,
        });
        try std.testing.expect(!decision.canRun());
        try std.testing.expectEqual(Role.promoted_standby, decision.role);
        try std.testing.expectEqual(Action.open_promoted_primary, decision.action);
        break :blk decision.promotion_handoff orelse return error.TestExpectedEqual;
    };

    {
        var recovered = try standby_mod.Standby.open(alloc, paths.log.ptr, paths.progress.ptr, identity, .{});
        defer recovered.close();

        const decision = try evaluateStandby(&recovered, .{
            .kind = .compaction_publish,
            .expected_identity = promoted_identity,
        });
        try std.testing.expect(!decision.canRun());
        try std.testing.expectEqual(Role.promoted_standby, decision.role);
        try std.testing.expectEqual(Action.open_promoted_primary, decision.action);
        try std.testing.expect(decision.promotion_handoff != null);
        try std.testing.expectError(error.WrongTimeline, evaluateStandby(&recovered, .{
            .kind = .compaction_publish,
            .expected_identity = identity,
        }));
    }

    var primary = try primary_mod.Primary.openPromotedFromStandby(alloc, paths.log.ptr, paths.slots.ptr, handoff, .{});
    defer primary.close();
    const primary_decision = try evaluatePrimary(&primary, .{
        .kind = .compaction_publish,
        .expected_identity = promoted_identity,
    });
    try std.testing.expect(primary_decision.canRun());
    try std.testing.expectEqual(@as(u64, 3), primary_decision.next_lsn);
}
