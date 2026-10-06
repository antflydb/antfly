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
pub const Debt = struct { config_hash: u64, generation: u64 };
pub const StuckRecord = struct { first_stuck_ns: u64, indexed: u64, expected: u64 };
pub const Snapshot = struct {
    index_name: []u8,
    record: StuckRecord,
    pub fn deinit(self: *Snapshot, alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }
};
/// Volatile handoff and diagnostics. Durable counters, generation validation,
/// rebuild intents and publication authority remain with local storage.
pub const Tracker = struct {
    mutex: std.atomic.Mutex = .unlocked,
    pending: std.StringHashMapUnmanaged(Debt) = .empty,
    stuck: std.StringHashMapUnmanaged(StuckRecord) = .empty,
    warning_last_ns: std.StringHashMapUnmanaged(u64) = .empty,
    fn lock(self: *Tracker) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn deinit(self: *Tracker, alloc: std.mem.Allocator) void {
        inline for (.{ &self.pending, &self.stuck, &self.warning_last_ns }) |map| {
            var keys = map.keyIterator();
            while (keys.next()) |key| alloc.free(@constCast(key.*));
            map.deinit(alloc);
        }
        self.* = .{};
    }
    pub fn shouldLog(self: *Tracker, index_name: []const u8, now_ns: u64, cooldown_ns: u64) bool {
        self.lock();
        defer self.mutex.unlock();
        if (cooldown_ns == 0) return true;
        const last_ns = self.warning_last_ns.get(index_name) orelse return true;
        return now_ns -| last_ns >= cooldown_ns;
    }
    pub fn noteLogged(self: *Tracker, alloc: std.mem.Allocator, index_name: []const u8, now_ns: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        const gop = try self.warning_last_ns.getOrPut(alloc, index_name);
        if (!gop.found_existing) {
            errdefer _ = self.warning_last_ns.remove(index_name);
            gop.key_ptr.* = try alloc.dupe(u8, index_name);
        }
        gop.value_ptr.* = now_ns;
    }
    pub fn recordDebt(
        self: *Tracker,
        alloc: std.mem.Allocator,
        index_name: []const u8,
        debt: Debt,
    ) !void {
        self.lock();
        defer self.mutex.unlock();
        const gop = try self.pending.getOrPut(alloc, index_name);
        if (!gop.found_existing) {
            errdefer _ = self.pending.remove(index_name);
            gop.key_ptr.* = try alloc.dupe(u8, index_name);
        }
        gop.value_ptr.* = debt;
    }
    pub fn debtPending(
        self: *Tracker,
        index_name: []const u8,
        identity: Debt,
    ) bool {
        self.lock();
        defer self.mutex.unlock();
        const debt = self.pending.get(index_name) orelse return false;
        return debt.config_hash == identity.config_hash and debt.generation == identity.generation;
    }
    pub fn clearDebt(self: *Tracker, alloc: std.mem.Allocator, index_name: []const u8) void {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.pending.fetchRemove(index_name) orelse return;
        alloc.free(@constCast(entry.key));
    }
    pub fn noteStuck(self: *Tracker, alloc: std.mem.Allocator, index_name: []const u8, now_ns: u64, indexed: u64, expected: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        const gop = try self.stuck.getOrPut(alloc, index_name);
        if (!gop.found_existing) {
            errdefer _ = self.stuck.remove(index_name);
            gop.key_ptr.* = try alloc.dupe(u8, index_name);
            gop.value_ptr.* = .{ .first_stuck_ns = now_ns, .indexed = indexed, .expected = expected };
            return;
        }
        gop.value_ptr.indexed = indexed;
        gop.value_ptr.expected = expected;
    }
    pub fn clearStuck(self: *Tracker, alloc: std.mem.Allocator, index_name: []const u8) void {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.stuck.fetchRemove(index_name) orelse return;
        alloc.free(@constCast(entry.key));
    }
    pub fn oldestStuck(self: *Tracker, alloc: std.mem.Allocator, timeout_ns: u64, now_ns: u64) !?Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var it = self.stuck.iterator();
        while (it.next()) |stuck_entry| {
            const stuck_ns = now_ns -| stuck_entry.value_ptr.first_stuck_ns;
            if (stuck_ns < timeout_ns) continue;
            return .{
                .index_name = try alloc.dupe(u8, stuck_entry.key_ptr.*),
                .record = stuck_entry.value_ptr.*,
            };
        }
        return null;
    }
};

test "target tracker rolls back every allocation failure and owns snapshots" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var tracker: Tracker = .{};
            defer tracker.deinit(alloc);
            try tracker.recordDebt(alloc, "idx", .{ .config_hash = 11, .generation = 7 });
            try tracker.recordDebt(alloc, "idx", .{ .config_hash = 12, .generation = 8 });
            try std.testing.expect(tracker.debtPending("idx", .{ .config_hash = 12, .generation = 8 }));
            try std.testing.expect(!tracker.debtPending("idx", .{ .config_hash = 11, .generation = 7 }));
            try tracker.noteStuck(alloc, "idx", 10, 3, 9);
            try tracker.noteStuck(alloc, "idx", 20, 5, 9);
            var snapshot = (try tracker.oldestStuck(alloc, 5, 20)).?;
            defer snapshot.deinit(alloc);
            tracker.clearStuck(alloc, "idx");
            try std.testing.expectEqualStrings("idx", snapshot.index_name);
            try std.testing.expectEqual(@as(u64, 10), snapshot.record.first_stuck_ns);
            try std.testing.expectEqual(@as(u64, 5), snapshot.record.indexed);
            try tracker.noteLogged(alloc, "idx", 20);
            try std.testing.expect(!tracker.shouldLog("idx", 21, 10));
            try std.testing.expect(tracker.shouldLog("idx", 30, 10));
            tracker.clearDebt(alloc, "idx");
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
