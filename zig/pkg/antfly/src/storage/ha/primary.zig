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

//! Primary-side HA replication coordination.
//!
//! This layer ties the durable HA replication log to durable replication slots.
//! It is intentionally transport-agnostic: HTTP/CLI/operator code can create
//! slots, stream records, submit standby status updates, and ask whether a
//! target LSN satisfies the configured async/remote-write/remote-apply policy.

const std = @import("std");
const Allocator = std.mem.Allocator;
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const slot_store = @import("slot_store.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Identity = standby_mod.Identity;

pub const OpenOptions = struct {
    replication_log_options: replication_log.OpenOptions = .{},
    slot_store_options: slot_store.OpenOptions = .{},
};

pub const AppendOptions = struct {
    kind: replication_record.RecordKind = .batch_mutation,
    payload_codec: replication_record.PayloadCodec = .raw,
    flags: u32 = 0,
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    commit_timestamp_ns: i64 = 0,
    payload: []const u8 = &.{},
};

pub const DurabilityMode = enum {
    async,
    remote_write,
    remote_apply,
};

pub const StandbySelection = enum {
    any,
    first,
    all,
};

pub const FailurePolicy = enum {
    block,
    fail_closed,
    degrade_to_async,
};

pub const SyncPolicy = struct {
    mode: DurabilityMode = .async,
    selection: StandbySelection = .any,
    required: usize = 1,
    standby_names: []const []const u8 = &.{},
    failure_policy: FailurePolicy = .block,
};

pub const DurabilityStatus = enum {
    satisfied,
    would_block,
    fail_closed,
    degraded_to_async,
};

pub const DurabilityDecision = struct {
    status: DurabilityStatus,
    mode: DurabilityMode,
    selection: StandbySelection,
    target_lsn: u64,
    satisfied_count: usize,
    required_count: usize,
    candidate_count: usize,
};

pub const Primary = struct {
    alloc: Allocator,
    identity: Identity,
    log: replication_log.ReplicationLog,
    slots: slot_store.SlotStore,

    pub fn open(
        alloc: Allocator,
        log_path: [*:0]const u8,
        slot_store_path: [*:0]const u8,
        identity: Identity,
        options: OpenOptions,
    ) !Primary {
        var log = try replication_log.ReplicationLog.open(log_path, options.replication_log_options);
        const slots = slot_store.SlotStore.open(alloc, slot_store_path, options.slot_store_options) catch |err| {
            log.close();
            return err;
        };
        return .{
            .alloc = alloc,
            .identity = identity,
            .log = log,
            .slots = slots,
        };
    }

    pub fn close(self: *Primary) void {
        self.slots.close();
        self.log.close();
        self.* = undefined;
    }

    pub fn lastLsn(self: *const Primary) u64 {
        return self.log.lastLsn();
    }

    pub fn nextLsn(self: *const Primary) u64 {
        return self.log.nextLsn();
    }

    pub fn append(self: *Primary, options: AppendOptions) !u64 {
        const lsn = self.nextLsn();
        return try self.log.append(self.alloc, .{
            .kind = options.kind,
            .payload_codec = options.payload_codec,
            .flags = options.flags,
            .cluster_id = self.identity.cluster_id,
            .shard_id = options.shard_id orelse self.identity.shard_id,
            .table_id = options.table_id orelse self.identity.table_id,
            .timeline_id = self.identity.timeline_id,
            .epoch = self.identity.epoch,
            .lsn = lsn,
            .previous_lsn = lsn - 1,
            .commit_timestamp_ns = options.commit_timestamp_ns,
            .payload = options.payload,
        });
    }

    pub fn createSlot(self: *Primary, name: []const u8, initial_lsn: u64) !void {
        try self.slots.createOrUpdate(.{
            .name = name,
            .timeline_id = self.identity.timeline_id,
            .restart_lsn = initial_lsn,
            .received_lsn = initial_lsn,
            .applied_lsn = initial_lsn,
        });
    }

    pub fn dropSlot(self: *Primary, name: []const u8) !void {
        try self.slots.drop(name);
    }

    pub fn slot(self: *const Primary, name: []const u8) ?slot_store.SlotState {
        return self.slots.get(name);
    }

    pub fn streamFrom(self: *Primary, alloc: Allocator, slot_name: []const u8, from_lsn: u64) ![]replication_log.Entry {
        const state = self.slots.get(slot_name) orelse return error.SlotNotFound;
        if (!state.active) return error.SlotInactive;
        if (state.reseed_required) return error.SlotRequiresReseed;
        if (state.timeline_id != self.identity.timeline_id) return error.WrongTimeline;
        if (from_lsn < state.restart_lsn) return error.WalNoLongerRetained;
        return try self.log.iterateFrom(alloc, from_lsn);
    }

    pub fn standbyStatusUpdate(
        self: *Primary,
        slot_name: []const u8,
        timeline_id: u64,
        received_lsn: u64,
        applied_lsn: u64,
    ) !void {
        if (timeline_id != self.identity.timeline_id) return error.WrongTimeline;
        if (received_lsn > self.lastLsn()) return error.StandbyAheadOfPrimary;
        try self.slots.updateProgress(slot_name, received_lsn, applied_lsn);
    }

    pub fn retentionSnapshot(
        self: *Primary,
        policy: slot_store.RetentionPolicy,
    ) !slot_store.RetentionSnapshot {
        return try self.slots.retentionSnapshot(self.lastLsn(), policy);
    }

    pub fn evaluateDurability(self: *const Primary, target_lsn: u64, policy: SyncPolicy) !DurabilityDecision {
        if (target_lsn > self.lastLsn()) return error.TargetAheadOfPrimary;
        if (policy.mode == .async) {
            return .{
                .status = .satisfied,
                .mode = .async,
                .selection = policy.selection,
                .target_lsn = target_lsn,
                .satisfied_count = 0,
                .required_count = 0,
                .candidate_count = 0,
            };
        }
        if (policy.required == 0) return error.InvalidSyncPolicy;
        if (policy.standby_names.len == 0) return decisionForUnsatisfied(target_lsn, policy, 0, policy.required, 0);

        const counts = switch (policy.selection) {
            .any => self.evaluateAny(target_lsn, policy),
            .first => self.evaluateFirst(target_lsn, policy),
            .all => self.evaluateAll(target_lsn, policy),
        };

        const satisfied = counts.satisfied_count >= counts.required_count;
        return .{
            .status = if (satisfied) .satisfied else statusForFailurePolicy(policy.failure_policy),
            .mode = policy.mode,
            .selection = policy.selection,
            .target_lsn = target_lsn,
            .satisfied_count = counts.satisfied_count,
            .required_count = counts.required_count,
            .candidate_count = counts.candidate_count,
        };
    }

    fn evaluateAny(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.required,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
        }
        return counts;
    }

    fn evaluateFirst(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.required,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
            if (counts.candidate_count == policy.required) break;
        }
        return counts;
    }

    fn evaluateAll(self: *const Primary, target_lsn: u64, policy: SyncPolicy) Counts {
        var counts = Counts{
            .required_count = policy.standby_names.len,
        };
        for (policy.standby_names) |name| {
            const state = self.eligibleSlot(name) orelse continue;
            counts.candidate_count += 1;
            if (slotSatisfies(state, target_lsn, policy.mode)) counts.satisfied_count += 1;
        }
        return counts;
    }

    fn eligibleSlot(self: *const Primary, name: []const u8) ?slot_store.SlotState {
        const state = self.slots.get(name) orelse return null;
        if (!state.active or state.reseed_required) return null;
        if (state.timeline_id != self.identity.timeline_id) return null;
        return state;
    }
};

const Counts = struct {
    satisfied_count: usize = 0,
    required_count: usize = 0,
    candidate_count: usize = 0,
};

fn slotSatisfies(state: slot_store.SlotState, target_lsn: u64, mode: DurabilityMode) bool {
    return switch (mode) {
        .async => true,
        .remote_write => state.received_lsn >= target_lsn,
        .remote_apply => state.applied_lsn >= target_lsn,
    };
}

fn statusForFailurePolicy(policy: FailurePolicy) DurabilityStatus {
    return switch (policy) {
        .block => .would_block,
        .fail_closed => .fail_closed,
        .degrade_to_async => .degraded_to_async,
    };
}

fn decisionForUnsatisfied(
    target_lsn: u64,
    policy: SyncPolicy,
    satisfied_count: usize,
    required_count: usize,
    candidate_count: usize,
) DurabilityDecision {
    return .{
        .status = statusForFailurePolicy(policy.failure_policy),
        .mode = policy.mode,
        .selection = policy.selection,
        .target_lsn = target_lsn,
        .satisfied_count = satisfied_count,
        .required_count = required_count,
        .candidate_count = candidate_count,
    };
}

const TestPaths = struct {
    log: [:0]u8,
    slots: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.log);
        alloc.free(self.slots);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const log_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-primary-" ++ name ++ "-log-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(log_raw);
    const slots_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-primary-" ++ name ++ "-slots-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(slots_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), log_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), slots_raw) catch {};

    return .{
        .log = try alloc.dupeZ(u8, log_raw),
        .slots = try alloc.dupeZ(u8, slots_raw),
    };
}

fn testIdentity() Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

test "storage.ha primary appends streams and persists standby status" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "stream");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);
        try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
        try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "two" }));

        const entries = try primary.streamFrom(alloc, "standby-a", 1);
        defer replication_log.freeEntries(alloc, entries);
        try std.testing.expectEqual(@as(usize, 2), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].record.payload);
        try std.testing.expectEqualStrings("two", entries[1].record.payload);

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 1);
    }

    {
        var reopened = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(u64, 2), reopened.lastLsn());
        const slot = reopened.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.restart_lsn);
    }
}

test "storage.ha primary evaluates async remote write and remote apply policies" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "durability");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("a", 0);
    try primary.createSlot("b", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    _ = try primary.append(.{ .payload = "three" });
    try primary.standbyStatusUpdate("a", identity.timeline_id, 3, 1);
    try primary.standbyStatusUpdate("b", identity.timeline_id, 3, 3);

    const names = [_][]const u8{ "a", "b" };
    var decision = try primary.evaluateDurability(3, .{ .mode = .async });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);

    decision = try primary.evaluateDurability(3, .{
        .mode = .remote_write,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);
    try std.testing.expectEqual(@as(usize, 2), decision.satisfied_count);

    decision = try primary.evaluateDurability(3, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 2,
        .standby_names = &names,
        .failure_policy = .fail_closed,
    });
    try std.testing.expectEqual(DurabilityStatus.fail_closed, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.satisfied_count);
    try std.testing.expectEqual(@as(usize, 2), decision.required_count);
}

test "storage.ha primary first priority policy waits for the first eligible standby" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "first");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("priority-a", 0);
    try primary.createSlot("priority-b", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.standbyStatusUpdate("priority-a", identity.timeline_id, 2, 1);
    try primary.standbyStatusUpdate("priority-b", identity.timeline_id, 2, 2);

    const names = [_][]const u8{ "priority-a", "priority-b" };
    var decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .first,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.would_block, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.candidate_count);
    try std.testing.expectEqual(@as(usize, 0), decision.satisfied_count);

    try primary.slots.markReseedRequired("priority-a");
    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .first,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(DurabilityStatus.satisfied, decision.status);
    try std.testing.expectEqual(@as(usize, 1), decision.candidate_count);
    try std.testing.expectEqual(@as(usize, 1), decision.satisfied_count);
}

test "storage.ha primary refuses streaming from reseed or expired slots" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "retention");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try Primary.open(alloc, paths.log.ptr, paths.slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 2, 2);
    try std.testing.expectError(error.WalNoLongerRetained, primary.streamFrom(alloc, "standby-a", 1));

    try primary.slots.markReseedRequired("standby-a");
    try std.testing.expectError(error.SlotRequiresReseed, primary.streamFrom(alloc, "standby-a", 2));
}
