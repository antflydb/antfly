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

const builtin = @import("builtin");
const std = @import("std");
const leader_runtime = @import("leader_runtime.zig");
const read_state_observer_mod = @import("state_machine/read_state_observer.zig");

pub const ExecutorBackend = enum {
    simulated,
    threaded,
    evented,
};

pub const EnrichmentExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start_group: *const fn (ptr: *anyopaque, group_id: u64) anyerror!void,
        stop_group: *const fn (ptr: *anyopaque, group_id: u64) anyerror!void,
        is_active: *const fn (ptr: *anyopaque, group_id: u64) bool,
        backend: *const fn (ptr: *anyopaque) ExecutorBackend,
    };

    pub fn startGroup(self: EnrichmentExecutor, group_id: u64) !void {
        try self.vtable.start_group(self.ptr, group_id);
    }

    pub fn stopGroup(self: EnrichmentExecutor, group_id: u64) !void {
        try self.vtable.stop_group(self.ptr, group_id);
    }

    pub fn isActive(self: EnrichmentExecutor, group_id: u64) bool {
        return self.vtable.is_active(self.ptr, group_id);
    }

    pub fn backend(self: EnrichmentExecutor) ExecutorBackend {
        return self.vtable.backend(self.ptr);
    }
};

// TODO: Re-enable Linux evented enrichment once std.Io.Evented/std.Io.Uring is
// stable enough for this code path. In Zig 0.16, instantiating std.Io.Uring
// trips stdlib error-set mismatches: std/Io/Uring.zig's dirOpenDir and
// dirRealPathFile propagate openat's error.ReadOnlyFileSystem into std/Io/Dir.zig
// error sets that do not include it.
const supports_evented_executor = false;

pub const Metrics = struct {
    gained_events: u64 = 0,
    lost_events: u64 = 0,
    start_calls: u64 = 0,
    stop_calls: u64 = 0,
};

pub const LeaseReadState = enum {
    follower,
    awaiting_readable,
    active,
};

pub const LeaseMetrics = struct {
    gained_events: u64 = 0,
    lost_events: u64 = 0,
    start_calls: u64 = 0,
    stop_calls: u64 = 0,
    readable_grants: u64 = 0,
    readable_revocations: u64 = 0,
};

fn putActive(map: *std.AutoHashMapUnmanaged(u64, void), alloc: std.mem.Allocator, group_id: u64) !void {
    try map.put(alloc, group_id, {});
}

fn removeActive(map: *std.AutoHashMapUnmanaged(u64, void), group_id: u64) void {
    _ = map.remove(group_id);
}

pub const SimulatedExecutor = struct {
    alloc: std.mem.Allocator,
    active_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,

    pub fn init(alloc: std.mem.Allocator) SimulatedExecutor {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *SimulatedExecutor) void {
        self.active_groups.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn executor(self: *SimulatedExecutor) EnrichmentExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .start_group = startGroup,
                .stop_group = stopGroup,
                .is_active = isActive,
                .backend = backend,
            },
        };
    }

    fn startGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *SimulatedExecutor = @ptrCast(@alignCast(ptr));
        try putActive(&self.active_groups, self.alloc, group_id);
    }

    fn stopGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *SimulatedExecutor = @ptrCast(@alignCast(ptr));
        removeActive(&self.active_groups, group_id);
    }

    fn isActive(ptr: *anyopaque, group_id: u64) bool {
        const self: *SimulatedExecutor = @ptrCast(@alignCast(ptr));
        return self.active_groups.contains(group_id);
    }

    fn backend(_: *anyopaque) ExecutorBackend {
        return .simulated;
    }
};

pub const ThreadedExecutor = struct {
    alloc: std.mem.Allocator,
    threaded: std.Io.Threaded,
    active_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,

    pub fn init(alloc: std.mem.Allocator) ThreadedExecutor {
        return .{
            .alloc = alloc,
            .threaded = std.Io.Threaded.init(alloc, .{}),
        };
    }

    pub fn deinit(self: *ThreadedExecutor) void {
        self.active_groups.deinit(self.alloc);
        self.threaded.deinit();
        self.* = undefined;
    }

    pub fn executor(self: *ThreadedExecutor) EnrichmentExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .start_group = startGroup,
                .stop_group = stopGroup,
                .is_active = isActive,
                .backend = backend,
            },
        };
    }

    fn startGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *ThreadedExecutor = @ptrCast(@alignCast(ptr));
        try putActive(&self.active_groups, self.alloc, group_id);
    }

    fn stopGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *ThreadedExecutor = @ptrCast(@alignCast(ptr));
        removeActive(&self.active_groups, group_id);
    }

    fn isActive(ptr: *anyopaque, group_id: u64) bool {
        const self: *ThreadedExecutor = @ptrCast(@alignCast(ptr));
        return self.active_groups.contains(group_id);
    }

    fn backend(_: *anyopaque) ExecutorBackend {
        return .threaded;
    }
};

pub const EventedExecutor = if (!supports_evented_executor) struct {
    pub fn init(_: std.mem.Allocator) !@This() {
        return error.UnsupportedEventedBackend;
    }
} else struct {
    alloc: std.mem.Allocator,
    evented: std.Io.Evented,
    active_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,

    pub fn init(alloc: std.mem.Allocator) !@This() {
        var evented: std.Io.Evented = undefined;
        try std.Io.Evented.init(&evented, alloc, .{});
        return .{
            .alloc = alloc,
            .evented = evented,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.active_groups.deinit(self.alloc);
        std.Io.Evented.deinit(&self.evented);
        self.* = undefined;
    }

    pub fn executor(self: *@This()) EnrichmentExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .start_group = startGroup,
                .stop_group = stopGroup,
                .is_active = isActive,
                .backend = backend,
            },
        };
    }

    fn startGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try putActive(&self.active_groups, self.alloc, group_id);
    }

    fn stopGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        removeActive(&self.active_groups, group_id);
    }

    fn isActive(ptr: *anyopaque, group_id: u64) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.active_groups.contains(group_id);
    }

    fn backend(_: *anyopaque) ExecutorBackend {
        return .evented;
    }
};

pub const LeaderEnrichmentRuntime = struct {
    executor: EnrichmentExecutor,
    metrics: Metrics = .{},

    pub fn init(executor: EnrichmentExecutor) LeaderEnrichmentRuntime {
        return .{ .executor = executor };
    }

    pub fn observer(self: *LeaderEnrichmentRuntime) leader_runtime.LeaderObserver {
        return .{
            .ptr = self,
            .vtable = &.{
                .on_event = onLeadershipEvent,
            },
        };
    }

    pub fn isActive(self: *LeaderEnrichmentRuntime, group_id: u64) bool {
        return self.executor.isActive(group_id);
    }

    fn onLeadershipEvent(ptr: *anyopaque, event: leader_runtime.LeadershipEvent) !void {
        const self: *LeaderEnrichmentRuntime = @ptrCast(@alignCast(ptr));
        switch (event.kind) {
            .gained => {
                self.metrics.gained_events += 1;
                self.metrics.start_calls += 1;
                try self.executor.startGroup(event.group_id);
            },
            .lost => {
                self.metrics.lost_events += 1;
                self.metrics.stop_calls += 1;
                try self.executor.stopGroup(event.group_id);
            },
        }
    }
};

pub const LeaseGatedLeaderEnrichmentRuntime = struct {
    alloc: std.mem.Allocator,
    executor: EnrichmentExecutor,
    states: std.AutoHashMapUnmanaged(u64, LeaseReadState) = .empty,
    metrics: LeaseMetrics = .{},

    pub fn init(alloc: std.mem.Allocator, executor: EnrichmentExecutor) LeaseGatedLeaderEnrichmentRuntime {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn deinit(self: *LeaseGatedLeaderEnrichmentRuntime) void {
        self.states.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn observer(self: *LeaseGatedLeaderEnrichmentRuntime) leader_runtime.LeaderObserver {
        return .{
            .ptr = self,
            .vtable = &.{
                .on_event = onLeadershipEvent,
            },
        };
    }

    pub fn readStateObserver(self: *LeaseGatedLeaderEnrichmentRuntime) read_state_observer_mod.ReadStateObserver {
        return .{
            .ptr = self,
            .vtable = &.{
                .on_read_states = onReadStates,
            },
        };
    }

    pub fn state(self: *LeaseGatedLeaderEnrichmentRuntime, group_id: u64) LeaseReadState {
        return self.states.get(group_id) orelse .follower;
    }

    pub fn isActive(self: *LeaseGatedLeaderEnrichmentRuntime, group_id: u64) bool {
        return self.executor.isActive(group_id);
    }

    pub fn markReadable(self: *LeaseGatedLeaderEnrichmentRuntime, group_id: u64) !bool {
        const entry = try self.states.getOrPut(self.alloc, group_id);
        if (!entry.found_existing) entry.value_ptr.* = .follower;
        switch (entry.value_ptr.*) {
            .follower => return false,
            .awaiting_readable => {
                self.metrics.readable_grants += 1;
                self.metrics.start_calls += 1;
                try self.executor.startGroup(group_id);
                entry.value_ptr.* = .active;
                return true;
            },
            .active => return false,
        }
    }

    pub fn revokeReadable(self: *LeaseGatedLeaderEnrichmentRuntime, group_id: u64) !bool {
        const entry = self.states.getPtr(group_id) orelse return false;
        switch (entry.*) {
            .follower => return false,
            .awaiting_readable => return false,
            .active => {
                self.metrics.readable_revocations += 1;
                self.metrics.stop_calls += 1;
                try self.executor.stopGroup(group_id);
                entry.* = .awaiting_readable;
                return true;
            },
        }
    }

    fn onLeadershipEvent(ptr: *anyopaque, event: leader_runtime.LeadershipEvent) !void {
        const self: *LeaseGatedLeaderEnrichmentRuntime = @ptrCast(@alignCast(ptr));
        switch (event.kind) {
            .gained => {
                self.metrics.gained_events += 1;
                const entry = try self.states.getOrPut(self.alloc, event.group_id);
                entry.value_ptr.* = .awaiting_readable;
            },
            .lost => {
                self.metrics.lost_events += 1;
                if (self.states.get(event.group_id)) |lease_state| {
                    if (lease_state == .active) {
                        self.metrics.stop_calls += 1;
                        try self.executor.stopGroup(event.group_id);
                    }
                }
                _ = self.states.remove(event.group_id);
            },
        }
    }

    fn onReadStates(
        ptr: *anyopaque,
        group_id: u64,
        read_states: []const @import("raft_engine").core.ReadState,
    ) !void {
        const self: *LeaseGatedLeaderEnrichmentRuntime = @ptrCast(@alignCast(ptr));
        if (read_states.len == 0) return;
        _ = try self.markReadable(group_id);
    }
};

test "leader enrichment runtime starts and stops simulated groups on leadership change" {
    var executor = SimulatedExecutor.init(std.testing.allocator);
    defer executor.deinit();

    var runtime = LeaderEnrichmentRuntime.init(executor.executor());
    const observer = runtime.observer();

    try observer.onEvent(.{
        .group_id = 91,
        .local_node_id = 1,
        .leader_id = 1,
        .kind = .gained,
    });
    try std.testing.expect(runtime.isActive(91));

    try observer.onEvent(.{
        .group_id = 91,
        .local_node_id = 1,
        .leader_id = 2,
        .kind = .lost,
    });
    try std.testing.expect(!runtime.isActive(91));
    try std.testing.expectEqual(@as(u64, 1), runtime.metrics.gained_events);
    try std.testing.expectEqual(@as(u64, 1), runtime.metrics.lost_events);
}

test "lease-gated enrichment runtime waits for readable lease before starting work" {
    var executor = SimulatedExecutor.init(std.testing.allocator);
    defer executor.deinit();

    var runtime = LeaseGatedLeaderEnrichmentRuntime.init(std.testing.allocator, executor.executor());
    defer runtime.deinit();
    const observer = runtime.observer();

    try observer.onEvent(.{
        .group_id = 111,
        .local_node_id = 1,
        .leader_id = 1,
        .kind = .gained,
    });
    try std.testing.expectEqual(LeaseReadState.awaiting_readable, runtime.state(111));
    try std.testing.expect(!runtime.isActive(111));

    try std.testing.expect(try runtime.markReadable(111));
    try std.testing.expectEqual(LeaseReadState.active, runtime.state(111));
    try std.testing.expect(runtime.isActive(111));

    try std.testing.expect(try runtime.revokeReadable(111));
    try std.testing.expectEqual(LeaseReadState.awaiting_readable, runtime.state(111));
    try std.testing.expect(!runtime.isActive(111));

    try observer.onEvent(.{
        .group_id = 111,
        .local_node_id = 1,
        .leader_id = 2,
        .kind = .lost,
    });
    try std.testing.expectEqual(LeaseReadState.follower, runtime.state(111));
    try std.testing.expect(!runtime.isActive(111));
    try std.testing.expectEqual(@as(u64, 1), runtime.metrics.readable_grants);
    try std.testing.expectEqual(@as(u64, 1), runtime.metrics.readable_revocations);
}

test "threaded enrichment executor reports backend and activity" {
    var executor = ThreadedExecutor.init(std.testing.allocator);
    defer executor.deinit();
    const iface = executor.executor();

    try std.testing.expectEqual(ExecutorBackend.threaded, iface.backend());
    try iface.startGroup(55);
    try std.testing.expect(iface.isActive(55));
    try iface.stopGroup(55);
    try std.testing.expect(!iface.isActive(55));
}

test "evented enrichment executor initializes when supported" {
    if (!supports_evented_executor) {
        try std.testing.expectError(error.UnsupportedEventedBackend, EventedExecutor.init(std.testing.allocator));
        return;
    }

    var executor = try EventedExecutor.init(std.testing.allocator);
    defer executor.deinit();
    const iface = executor.executor();
    try std.testing.expectEqual(ExecutorBackend.evented, iface.backend());
}
