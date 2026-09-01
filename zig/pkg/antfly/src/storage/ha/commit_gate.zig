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

//! HA commit gate decisions.
//!
//! Primary append remains the local durability boundary. This module translates
//! the current standby acknowledgement state plus a sync policy into a concrete
//! caller action: acknowledge, wait, reject, or acknowledge while explicitly
//! degraded to async.

const std = @import("std");
const primary_mod = @import("primary.zig");

var test_path_counter: u64 = 0;

pub const Action = enum {
    acknowledge,
    wait_for_standby,
    reject,
    acknowledge_degraded,
};

pub const GateResult = struct {
    target_lsn: u64,
    action: Action,
    decision: primary_mod.DurabilityDecision,

    pub fn shouldAcknowledge(self: GateResult) bool {
        return self.action == .acknowledge or self.action == .acknowledge_degraded;
    }

    pub fn shouldWait(self: GateResult) bool {
        return self.action == .wait_for_standby;
    }
};

pub const AppendResult = struct {
    lsn: u64,
    gate: GateResult,
};

pub fn evaluate(primary: *const primary_mod.Primary, target_lsn: u64, policy: primary_mod.SyncPolicy) !GateResult {
    const decision = try primary.evaluateDurability(target_lsn, policy);
    return .{
        .target_lsn = target_lsn,
        .action = actionForStatus(decision.status),
        .decision = decision,
    };
}

pub fn appendAndEvaluate(
    primary: *primary_mod.Primary,
    append_options: primary_mod.AppendOptions,
    policy: primary_mod.SyncPolicy,
) !AppendResult {
    if (policy.mode != .async and policy.failure_policy == .fail_closed) {
        const preflight = try primary.evaluateAppendDurability(primary.nextLsn(), policy);
        if (preflight.status == .fail_closed) return error.SyncPolicyUnsatisfied;
    }

    const lsn = try primary.append(append_options);
    return .{
        .lsn = lsn,
        .gate = try evaluate(primary, lsn, policy),
    };
}

fn actionForStatus(status: primary_mod.DurabilityStatus) Action {
    return switch (status) {
        .satisfied => .acknowledge,
        .would_block => .wait_for_standby,
        .fail_closed => .reject,
        .degraded_to_async => .acknowledge_degraded,
    };
}

const TestPaths = struct {
    log: [:0]u8,
    slots: [:0]u8,

    fn deinit(self: TestPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.log);
        alloc.free(self.slots);
    }
};

fn testPaths(alloc: std.mem.Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const log_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-commit-gate-" ++ name ++ "-log-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(log_raw);
    const slots_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-commit-gate-" ++ name ++ "-slots-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(slots_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), log_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), slots_raw) catch {};

    return .{
        .log = try alloc.dupeSentinel(u8, log_raw, 0),
        .slots = try alloc.dupeSentinel(u8, slots_raw, 0),
    };
}

fn testIdentity() primary_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

test "storage.ha commit gate acknowledges async and satisfied remote write" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "ack");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var result = try appendAndEvaluate(&primary, .{ .payload = "one" }, .{ .mode = .async });
    try std.testing.expectEqual(@as(u64, 1), result.lsn);
    try std.testing.expectEqual(Action.acknowledge, result.gate.action);
    try std.testing.expect(result.gate.shouldAcknowledge());

    _ = try appendAndEvaluate(&primary, .{ .payload = "two" }, .{ .mode = .async });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 1);
    const names = [_][]const u8{"standby-a"};
    result.gate = try evaluate(&primary, 2, .{
        .mode = .remote_write,
        .standby_names = &names,
    });
    try std.testing.expectEqual(Action.acknowledge, result.gate.action);
    try std.testing.expectEqual(@as(usize, 1), result.gate.decision.satisfied_count);
}

test "storage.ha commit gate waits rejects or degrades by failure policy" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "policies");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const names = [_][]const u8{"standby-a"};

    const appended = try appendAndEvaluate(&primary, .{ .payload = "one" }, .{
        .mode = .remote_apply,
        .standby_names = &names,
        .failure_policy = .block,
    });
    try std.testing.expectEqual(Action.wait_for_standby, appended.gate.action);
    try std.testing.expect(appended.gate.shouldWait());
    try std.testing.expectEqual(@as(u64, 0), appended.gate.decision.progress_lsn);
    try std.testing.expectEqual(@as(u64, 1), appended.gate.decision.missing_lsn_count);

    var gate = try evaluate(&primary, appended.lsn, .{
        .mode = .remote_apply,
        .standby_names = &names,
        .failure_policy = .fail_closed,
    });
    try std.testing.expectEqual(Action.reject, gate.action);
    try std.testing.expect(!gate.shouldAcknowledge());

    gate = try evaluate(&primary, appended.lsn, .{
        .mode = .remote_apply,
        .standby_names = &names,
        .failure_policy = .degrade_to_async,
    });
    try std.testing.expectEqual(Action.acknowledge_degraded, gate.action);
    try std.testing.expect(gate.shouldAcknowledge());
    try std.testing.expectEqual(@as(u64, 0), gate.decision.progress_lsn);
    try std.testing.expectEqual(@as(u64, 1), gate.decision.missing_lsn_count);

    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);
    gate = try evaluate(&primary, appended.lsn, .{
        .mode = .remote_apply,
        .standby_names = &names,
        .failure_policy = .block,
    });
    try std.testing.expectEqual(Action.acknowledge, gate.action);
    try std.testing.expectEqual(@as(u64, 1), gate.decision.progress_lsn);
    try std.testing.expectEqual(@as(u64, 0), gate.decision.missing_lsn_count);
}

test "storage.ha commit gate fail closed append rejects before local wal" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "fail-closed-append");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const names = [_][]const u8{"standby-a"};

    try std.testing.expectError(error.SyncPolicyUnsatisfied, appendAndEvaluate(&primary, .{ .payload = "one" }, .{
        .mode = .remote_write,
        .standby_names = &names,
        .failure_policy = .fail_closed,
    }));
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
}

test "storage.ha commit gate ignores paused or reseed-required slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "ineligible");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    try primary.createSlot("standby-b", 0);
    const names = [_][]const u8{ "standby-a", "standby-b" };
    _ = try primary.append(.{ .payload = "one" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);
    try primary.standbyStatusUpdate("standby-b", identity.timeline_id, 1, 1);

    try primary.pauseSlot("standby-a");
    try primary.slots.markReseedRequired("standby-b");
    const gate = try evaluate(&primary, 1, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(Action.wait_for_standby, gate.action);
    try std.testing.expectEqual(@as(usize, 0), gate.decision.candidate_count);
}
