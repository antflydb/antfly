// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const primary_mod = @import("storage/hot_standby/primary.zig");
const Cursor = @import("antfly_local_sources").storage_db_durable_outbox.RecoveryCursor;
const read = @import("antfly_local_sources").common_read_admission;
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;

test "primary owned prepared WAL encoding crosses compiled owner and owns payload through publication" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/prepared-log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/prepared-slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    var root = std.testing.FailingAllocator.init(a, .{});
    var primary = try primary_mod.Primary.open(root.allocator(), log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 3, .shard_id = 2 }, .{ .replication_log_options = .{ .wal_options = .{ .no_sync = true, .commit_backend = .sync } } });
    defer primary.close();
    var bytes: [8193]u8 = @splat(37);
    const original = bytes;
    const State = struct {
        alive: bool = true,
        calls: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!self.alive) return error.Canceled;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    var abandoned = try primary.prepareAppend(.{ .payload = &bytes }, read.Port.init(&payload));
    abandoned.deinit();
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    const baseline_bytes = root.allocated_bytes - root.freed_bytes;
    // HA bytes, owner descriptor and WAL bytes are independently fallible.
    for (0..3) |offset| {
        root.fail_index = root.alloc_index + offset;
        try std.testing.expectError(error.OutOfMemory, primary.prepareAppend(.{ .payload = &bytes }, read.Port.init(&payload)));
        root.fail_index = std.math.maxInt(usize);
        try std.testing.expectEqual(baseline_bytes, root.allocated_bytes - root.freed_bytes);
    }
    var prepared = try primary.prepareAppend(.{ .payload = &bytes }, read.Port.init(&payload));
    defer prepared.deinit();
    const checks = state.calls;
    state.alive = false;
    @memset(&bytes, 99);
    // Ordinary interleaving changes the final LSN but never the prepaid tail.
    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "interleaved" }));
    try std.testing.expectEqual(@as(u64, 2), try prepared.publish());
    try std.testing.expectEqual(checks, state.calls);
    try std.testing.expectError(error.InvalidArgument, prepared.publish());
    var entry = (try primary.log.entryAt(a, 2)).?;
    defer entry.deinit(a);
    try std.testing.expectEqualSlices(u8, &original, entry.record.payload);
    try std.testing.expectEqual(@as(u64, 1), entry.record.previous_lsn);
}

test "primary owned recovery compiled client resumes real storage and retires independent roots across close" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    var open_root = std.testing.FailingAllocator.init(a, .{});
    var read_root = std.testing.FailingAllocator.init(a, .{});
    const identity: primary_mod.Identity = .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 3, .shard_id = 2 };
    const options: primary_mod.OpenOptions = .{ .replication_log_options = .{ .wal_options = .{ .no_sync = true, .commit_backend = .sync } }, .slot_store_options = .{ .wal_options = .{ .no_sync = true, .commit_backend = .sync } } };
    var primary = try primary_mod.Primary.open(open_root.allocator(), log_path, slots_path, identity, options);
    var open = true;
    defer if (open) primary.close();
    const bytes = try a.alloc(u8, 256 * 1024 + 19);
    defer a.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @truncate(i *% 67);
    _ = try primary.append(.{ .payload = bytes });
    var cursor = Cursor.init(read_root.allocator());
    defer cursor.reset();
    const State = struct {
        active: bool = true,
        reject: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.active) return error.CompiledPrimaryExpiredScope;
            if (self.reject) return error.CompiledPrimaryOriginalRejection;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    try std.testing.expectEqual(@as(primary_mod.RecoveryMatch, .pending), try primary.findMatchingRecordForRecoveryStep(&cursor, @splat(1), 1, .batch_mutation, bytes, 2, 3, control));
    state.active = false;
    try std.testing.expect(cursor.pending != null);
    primary.close();
    open = false;
    try std.testing.expectEqual(open_root.allocated_bytes, open_root.freed_bytes);
    primary = try primary_mod.Primary.open(open_root.allocator(), log_path, slots_path, identity, options);
    open = true;
    var found = false;
    var attempts: usize = 0;
    for (0..1000) |_| {
        var fresh_state: State = .{};
        const fresh_payload: read.Payload = .{ .ptr = &fresh_state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
        const fresh = try a.dupe(u8, bytes);
        const result = primary.findMatchingRecordForRecoveryStep(&cursor, @splat(1), 1, .batch_mutation, fresh, 2, 3, read.Port.init(&fresh_payload));
        fresh_state.active = false;
        @memset(fresh, 0xDD);
        a.free(fresh);
        attempts += 1;
        switch (try result) {
            .pending => {},
            .found => |lsn| {
                try std.testing.expectEqual(@as(u64, 1), lsn);
                found = true;
                break;
            },
            else => return error.TestUnexpectedResult,
        }
    }
    try std.testing.expect(found and attempts > 4 and cursor.pending == null);
    try std.testing.expectEqual(read_root.allocated_bytes, read_root.freed_bytes);
    state = .{};
    try std.testing.expectEqual(@as(primary_mod.RecoveryMatch, .pending), try primary.findMatchingRecordForRecoveryStep(&cursor, @splat(2), 1, .batch_mutation, bytes, 2, 3, control));
    state.reject = true;
    try std.testing.expectError(error.CompiledPrimaryOriginalRejection, primary.findMatchingRecordForRecoveryStep(&cursor, @splat(2), 1, .batch_mutation, bytes, 2, 3, control));
    try std.testing.expect(cursor.pending == null);
    try std.testing.expectEqual(read_root.allocated_bytes, read_root.freed_bytes);
    state.reject = false;
    try std.testing.expectEqual(@as(primary_mod.RecoveryMatch, .pending), try primary.findMatchingRecordForRecoveryStep(&cursor, @splat(3), 1, .batch_mutation, bytes, 2, 3, control));
    primary.close();
    open = false;
    cursor.reset();
    try std.testing.expectEqual(read_root.allocated_bytes, read_root.freed_bytes);
    try std.testing.expectEqual(open_root.allocated_bytes, open_root.freed_bytes);
}
