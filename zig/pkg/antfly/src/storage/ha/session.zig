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

//! In-process HA replication session.
//!
//! This is the local equivalent of the future streaming transport loop. It
//! pulls records from a primary slot, durably receives them on a standby, applies
//! available records, and reports standby progress back to the primary.

const std = @import("std");
const Allocator = std.mem.Allocator;
const primary_mod = @import("primary.zig");
const replication_log = @import("replication_log.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Result = struct {
    received_count: usize,
    applied_count: usize,
    progress: standby_mod.Progress,
};

pub fn replicateAvailable(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    slot_name: []const u8,
    standby: *standby_mod.Standby,
    apply_ctx: *anyopaque,
    apply_fn: standby_mod.ApplyFn,
) !Result {
    const from_lsn = standby.snapshot().progress.nextReceiveLsn();
    const entries = primary.streamFrom(alloc, slot_name, from_lsn) catch |err| {
        primary.reportReplicationError(slot_name, @errorName(err)) catch {};
        return err;
    };
    defer replication_log.freeEntries(alloc, entries);

    var received_count: usize = 0;
    for (entries) |entry| {
        _ = standby.receive(entry.record) catch |err| {
            primary.reportReplicationError(slot_name, @errorName(err)) catch {};
            return err;
        };
        received_count += 1;
    }

    const applied_count = standby.applyAvailable(apply_ctx, apply_fn) catch |err| {
        reportProgress(primary, slot_name, standby) catch |progress_err| {
            primary.reportReplicationError(slot_name, @errorName(progress_err)) catch {};
            return progress_err;
        };
        primary.reportReplicationError(slot_name, @errorName(err)) catch {};
        return err;
    };

    reportProgress(primary, slot_name, standby) catch |err| {
        primary.reportReplicationError(slot_name, @errorName(err)) catch {};
        return err;
    };
    return .{
        .received_count = received_count,
        .applied_count = applied_count,
        .progress = standby.currentProgress(),
    };
}

fn reportProgress(
    primary: *primary_mod.Primary,
    slot_name: []const u8,
    standby: *const standby_mod.Standby,
) !void {
    const snapshot = standby.snapshot();
    const progress = snapshot.progress;
    try primary.standbyStatusUpdateWithSafeRead(
        slot_name,
        snapshot.identity.timeline_id,
        progress.received_lsn,
        progress.applied_lsn,
        progress.safe_read_lsn,
    );
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
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

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-session-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

const ApplyCapture = struct {
    alloc: Allocator,
    payloads: std.ArrayListUnmanaged([]u8) = .empty,
    fail_at_lsn: u64 = 0,

    fn deinit(self: *ApplyCapture) void {
        for (self.payloads.items) |payload| self.alloc.free(payload);
        self.payloads.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *ApplyCapture = @ptrCast(@alignCast(ctx));
        if (record.lsn == self.fail_at_lsn) return error.IntentionalApplyFailure;
        const owned = try self.alloc.dupe(u8, record.payload);
        errdefer self.alloc.free(owned);
        try self.payloads.append(self.alloc, owned);
    }
};

const SlotTimelineChanger = struct {
    primary: *primary_mod.Primary,
    slot_name: []const u8,

    fn apply(ctx: *anyopaque, record: replication_record.RecordView) !void {
        const self: *SlotTimelineChanger = @ptrCast(@alignCast(ctx));
        try self.primary.slots.createOrUpdate(.{
            .name = self.slot_name,
            .timeline_id = record.timeline_id + 1,
            .restart_lsn = record.lsn,
            .received_lsn = record.lsn,
            .applied_lsn = record.lsn,
            .safe_read_lsn = record.lsn,
        });
    }
};

test "storage.ha session replicates primary records to standby and updates slot progress" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "basic");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var capture = ApplyCapture{ .alloc = alloc };
    defer capture.deinit();
    const result = try replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
    try std.testing.expectEqual(@as(usize, 2), result.received_count);
    try std.testing.expectEqual(@as(usize, 2), result.applied_count);
    try std.testing.expectEqual(@as(u64, 2), result.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.progress.applied_lsn);
    try std.testing.expectEqualStrings("one", capture.payloads.items[0]);
    try std.testing.expectEqualStrings("two", capture.payloads.items[1]);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.restart_lsn);

    const names = [_][]const u8{"standby-a"};
    const decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .selection = .any,
        .required = 1,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
}

test "storage.ha session reports durable receive progress when apply fails" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "apply-fail");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    {
        var capture = ApplyCapture{ .alloc = alloc, .fail_at_lsn = 2 };
        defer capture.deinit();
        try std.testing.expectError(
            error.IntentionalApplyFailure,
            replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply),
        );
    }

    var slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.restart_lsn);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", slot.last_error.?);

    const names = [_][]const u8{"standby-a"};
    var decision = try primary.evaluateDurability(2, .{
        .mode = .remote_write,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.satisfied, decision.status);
    decision = try primary.evaluateDurability(2, .{
        .mode = .remote_apply,
        .standby_names = &names,
    });
    try std.testing.expectEqual(primary_mod.DurabilityStatus.would_block, decision.status);

    {
        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        const result = try replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
        try std.testing.expectEqual(@as(usize, 0), result.received_count);
        try std.testing.expectEqual(@as(usize, 1), result.applied_count);
        try std.testing.expectEqualStrings("two", capture.payloads.items[0]);
    }

    slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
    try std.testing.expect(slot.last_error == null);
}

test "storage.ha session records progress update failures" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "progress-fail");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });

    var changer = SlotTimelineChanger{
        .primary = &primary,
        .slot_name = "standby-a",
    };
    try std.testing.expectError(
        error.WrongTimeline,
        replicateAvailable(alloc, &primary, "standby-a", &standby, &changer, SlotTimelineChanger.apply),
    );

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, identity.timeline_id + 1), slot.timeline_id);
    try std.testing.expectEqualStrings("WrongTimeline", slot.last_error.?);
}

test "storage.ha session resumes after primary and standby reopen" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "reopen");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        try primary.createSlot("standby-a", 0);
        _ = try primary.append(.{ .payload = "one" });

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        const result = try replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
        try std.testing.expectEqual(@as(usize, 1), result.applied_count);
    }

    {
        var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
        defer primary.close();
        var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
        defer standby.close();
        _ = try primary.append(.{ .payload = "two" });
        _ = try primary.append(.{ .payload = "three" });

        var capture = ApplyCapture{ .alloc = alloc };
        defer capture.deinit();
        const result = try replicateAvailable(alloc, &primary, "standby-a", &standby, &capture, ApplyCapture.apply);
        try std.testing.expectEqual(@as(usize, 2), result.received_count);
        try std.testing.expectEqual(@as(usize, 2), result.applied_count);
        try std.testing.expectEqualStrings("two", capture.payloads.items[0]);
        try std.testing.expectEqualStrings("three", capture.payloads.items[1]);

        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 3), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 3), slot.applied_lsn);
    }
}
