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
const Allocator = std.mem.Allocator;
const resource_manager_mod = @import("../../resource_manager.zig");

pub const Tracker = struct {
    const Entry = struct {
        sequence: u64,
        bytes: u64,
    };

    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    accounted_bytes: u64 = 0,

    pub fn init(resource_manager: ?*resource_manager_mod.ResourceManager) Tracker {
        return .{ .resource_manager = resource_manager };
    }

    pub fn deinit(self: *Tracker, alloc: Allocator) void {
        self.observe(0);
        self.entries.deinit(alloc);
        self.* = undefined;
    }

    pub fn track(self: *Tracker, alloc: Allocator, sequence: u64, bytes: u64) !void {
        if (self.resource_manager == null or bytes == 0) return;
        try self.entries.append(alloc, .{
            .sequence = sequence,
            .bytes = bytes,
        });
        self.observe(self.accounted_bytes +| bytes);
    }

    pub fn releaseThrough(self: *Tracker, sequence: u64) void {
        if (self.resource_manager == null or self.entries.items.len == 0) return;
        var write_index: usize = 0;
        var released: u64 = 0;
        for (self.entries.items) |entry| {
            if (entry.sequence <= sequence) {
                released +|= entry.bytes;
                continue;
            }
            self.entries.items[write_index] = entry;
            write_index += 1;
        }
        self.entries.items.len = write_index;
        if (released == 0) return;
        self.observe(self.accounted_bytes -| released);
    }

    pub fn shouldThrottleWrites(self: *Tracker) bool {
        const manager = self.resource_manager orelse return false;
        const stats = manager.sliceStats(.derived_backlog);
        return switch (stats.pressure) {
            .normal => false,
            .soft => stats.soft_action == .throttle_writes or stats.soft_action == .reject_work,
            .hard => stats.hard_action == .throttle_writes or stats.hard_action == .reject_work,
        };
    }

    fn observe(self: *Tracker, bytes: u64) void {
        const manager = self.resource_manager orelse return;
        manager.observeUsage(.derived_backlog, &self.accounted_bytes, bytes);
    }
};

test "derived backlog tracker accounts and releases payload bytes" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try tracker.track(std.testing.allocator, 1, 8);
    try tracker.track(std.testing.allocator, 2, 15);
    var stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 23), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].soft_limit_events);
    try std.testing.expectEqual(@as(u64, 1), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].hard_limit_rejections);

    tracker.releaseThrough(1);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 15), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);

    tracker.releaseThrough(2);
    stats = manager.snapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.slices[@intFromEnum(resource_manager_mod.Slice.derived_backlog)].used_bytes);
}

test "derived backlog tracker reports throttle pressure" {
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = .{
        .soft_limit_bytes = 10,
        .hard_limit_bytes = 20,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var tracker = Tracker.init(&manager);
    defer tracker.deinit(std.testing.allocator);

    try std.testing.expect(!tracker.shouldThrottleWrites());
    try tracker.track(std.testing.allocator, 1, 11);
    try std.testing.expect(tracker.shouldThrottleWrites());
    tracker.releaseThrough(1);
    try std.testing.expect(!tracker.shouldThrottleWrites());
}
