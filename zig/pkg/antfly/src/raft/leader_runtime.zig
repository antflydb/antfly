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
const host_mod = @import("host.zig");

pub const LeadershipEventKind = enum {
    gained,
    lost,
};

pub const LeadershipEvent = struct {
    group_id: u64,
    local_node_id: u64,
    leader_id: ?u64,
    kind: LeadershipEventKind,
};

pub const LeaderObserver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        on_event: *const fn (ptr: *anyopaque, event: LeadershipEvent) anyerror!void,
    };

    pub fn onEvent(self: LeaderObserver, event: LeadershipEvent) !void {
        try self.vtable.on_event(self.ptr, event);
    }
};

pub const LeadershipTracker = struct {
    alloc: std.mem.Allocator,
    local_node_id: u64,
    observer: LeaderObserver,
    known_states: std.AutoHashMapUnmanaged(u64, bool) = .empty,

    pub fn init(alloc: std.mem.Allocator, local_node_id: u64, observer: LeaderObserver) LeadershipTracker {
        return .{
            .alloc = alloc,
            .local_node_id = local_node_id,
            .observer = observer,
        };
    }

    pub fn deinit(self: *LeadershipTracker) void {
        self.known_states.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn releaseAll(self: *LeadershipTracker) !usize {
        var emitted: usize = 0;
        var it = self.known_states.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.*) continue;
            try self.observer.onEvent(.{
                .group_id = entry.key_ptr.*,
                .local_node_id = self.local_node_id,
                .leader_id = null,
                .kind = .lost,
            });
            entry.value_ptr.* = false;
            emitted += 1;
        }
        self.known_states.clearRetainingCapacity();
        return emitted;
    }

    pub fn pollHost(self: *LeadershipTracker, host: *host_mod.Host) !usize {
        const groups = try host.listGroupIds(self.alloc);
        defer self.alloc.free(groups);
        return try self.pollGroups(groups, host_mod.Host.leaderId, host_mod.Host.isLocalLeader, host);
    }

    pub fn pollHttpHost(self: *LeadershipTracker, http_host: *host_mod.HttpHost) !usize {
        const groups = try http_host.host.listGroupIds(self.alloc);
        defer self.alloc.free(groups);
        return try self.pollGroups(groups, host_mod.HttpHost.leaderId, host_mod.HttpHost.isLocalLeader, http_host);
    }

    fn pollGroups(
        self: *LeadershipTracker,
        groups: []const u64,
        comptime leaderFn: anytype,
        comptime localLeaderFn: anytype,
        owner: anytype,
    ) !usize {
        var current = std.AutoHashMapUnmanaged(u64, void).empty;
        defer current.deinit(self.alloc);

        var emitted: usize = 0;
        for (groups) |group_id| {
            try current.put(self.alloc, group_id, {});

            const is_local_leader = localLeaderFn(owner, group_id);
            const entry = try self.known_states.getOrPut(self.alloc, group_id);
            if (!entry.found_existing) {
                entry.value_ptr.* = false;
            }

            if (entry.value_ptr.* != is_local_leader) {
                try self.observer.onEvent(.{
                    .group_id = group_id,
                    .local_node_id = self.local_node_id,
                    .leader_id = leaderFn(owner, group_id),
                    .kind = if (is_local_leader) .gained else .lost,
                });
                entry.value_ptr.* = is_local_leader;
                emitted += 1;
            }
        }

        var it = self.known_states.iterator();
        while (it.next()) |entry| {
            if (current.contains(entry.key_ptr.*)) continue;
            if (entry.value_ptr.*) {
                try self.observer.onEvent(.{
                    .group_id = entry.key_ptr.*,
                    .local_node_id = self.local_node_id,
                    .leader_id = null,
                    .kind = .lost,
                });
                emitted += 1;
            }
        }

        var stale = std.ArrayListUnmanaged(u64).empty;
        defer stale.deinit(self.alloc);
        it = self.known_states.iterator();
        while (it.next()) |entry| {
            if (!current.contains(entry.key_ptr.*)) {
                try stale.append(self.alloc, entry.key_ptr.*);
            }
        }
        for (stale.items) |group_id| {
            _ = self.known_states.remove(group_id);
        }

        return emitted;
    }
};

test "leadership tracker emits gained and lost events" {
    const Recorder = struct {
        alloc: std.mem.Allocator,
        events: std.ArrayListUnmanaged(LeadershipEvent) = .empty,

        fn deinit(self: *@This()) void {
            self.events.deinit(self.alloc);
            self.* = undefined;
        }

        fn observer(self: *@This()) LeaderObserver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .on_event = onEvent,
                },
            };
        }

        fn onEvent(ptr: *anyopaque, event: LeadershipEvent) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.events.append(self.alloc, event);
        }
    };

    const FakeHost = struct {
        leader: ?u64 = null,
        local_is_leader: bool = false,

        fn leaderId(self: *@This(), _: u64) ?u64 {
            return self.leader;
        }

        fn isLocalLeader(self: *@This(), _: u64) bool {
            return self.local_is_leader;
        }
    };

    var recorder = Recorder{ .alloc = std.testing.allocator };
    defer recorder.deinit();

    var tracker = LeadershipTracker.init(std.testing.allocator, 1, recorder.observer());
    defer tracker.deinit();

    var fake = FakeHost{};
    try std.testing.expectEqual(@as(usize, 0), try tracker.pollGroups(&.{11}, FakeHost.leaderId, FakeHost.isLocalLeader, &fake));

    fake.leader = 1;
    fake.local_is_leader = true;
    try std.testing.expectEqual(@as(usize, 1), try tracker.pollGroups(&.{11}, FakeHost.leaderId, FakeHost.isLocalLeader, &fake));
    try std.testing.expectEqual(LeadershipEventKind.gained, recorder.events.items[0].kind);

    fake.leader = 2;
    fake.local_is_leader = false;
    try std.testing.expectEqual(@as(usize, 1), try tracker.pollGroups(&.{11}, FakeHost.leaderId, FakeHost.isLocalLeader, &fake));
    try std.testing.expectEqual(LeadershipEventKind.lost, recorder.events.items[1].kind);
}

test "leadership tracker releaseAll emits lost for active groups" {
    const Recorder = struct {
        alloc: std.mem.Allocator,
        events: std.ArrayListUnmanaged(LeadershipEvent) = .empty,

        fn deinit(self: *@This()) void {
            self.events.deinit(self.alloc);
            self.* = undefined;
        }

        fn observer(self: *@This()) LeaderObserver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .on_event = onEvent,
                },
            };
        }

        fn onEvent(ptr: *anyopaque, event: LeadershipEvent) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.events.append(self.alloc, event);
        }
    };

    const FakeHost = struct {
        leader: ?u64 = null,
        local_is_leader: bool = false,

        fn leaderId(self: *@This(), _: u64) ?u64 {
            return self.leader;
        }

        fn isLocalLeader(self: *@This(), _: u64) bool {
            return self.local_is_leader;
        }
    };

    var recorder = Recorder{ .alloc = std.testing.allocator };
    defer recorder.deinit();

    var tracker = LeadershipTracker.init(std.testing.allocator, 7, recorder.observer());
    defer tracker.deinit();

    var fake = FakeHost{ .leader = 7, .local_is_leader = true };
    try std.testing.expectEqual(@as(usize, 1), try tracker.pollGroups(&.{41}, FakeHost.leaderId, FakeHost.isLocalLeader, &fake));
    try std.testing.expectEqual(@as(usize, 1), try tracker.releaseAll());
    try std.testing.expectEqual(@as(usize, 2), recorder.events.items.len);
    try std.testing.expectEqual(LeadershipEventKind.lost, recorder.events.items[1].kind);
    try std.testing.expectEqual(@as(?u64, null), recorder.events.items[1].leader_id);
}
