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
const builtin = @import("builtin");

// The platform io_uring/Dispatch backends support fibers, timers, cancellation, and file I/O.
// Network operations remain incomplete upstream; production transports use Threaded.
const supports_evented_executor = (builtin.os.tag == .linux or builtin.os.tag == .macos) and std.Io.fiber.supported;
const Evented = @import("antfly_platform").Evented;

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

pub const EventedExecutor = if (!supports_evented_executor) struct {
    pub fn init(_: std.mem.Allocator) !@This() {
        return error.UnsupportedEventedBackend;
    }
} else struct {
    alloc: std.mem.Allocator,
    evented: *Evented,
    active_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,

    pub fn init(alloc: std.mem.Allocator) !@This() {
        // Evented retains pointers into its own fiber and writer buffers.
        // Initialize at its final address, even when this executor is returned.
        const evented = try alloc.create(Evented);
        errdefer alloc.destroy(evented);
        try Evented.init(evented, alloc, .{});
        return .{
            .alloc = alloc,
            .evented = evented,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.active_groups.deinit(self.alloc);
        Evented.deinit(self.evented);
        self.alloc.destroy(self.evented);
        self.* = undefined;
    }

    pub fn io(self: *@This()) std.Io {
        return self.evented.io();
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
        try self.active_groups.put(self.alloc, group_id, {});
    }

    fn stopGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = self.active_groups.remove(group_id);
    }

    fn isActive(ptr: *anyopaque, group_id: u64) bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.active_groups.contains(group_id);
    }

    fn backend(_: *anyopaque) ExecutorBackend {
        return .evented;
    }
};

pub fn testEventedExecutor(require_available: bool) !void {
    if (!supports_evented_executor) {
        try std.testing.expectError(error.UnsupportedEventedBackend, EventedExecutor.init(std.testing.allocator));
        return;
    }

    var executor = EventedExecutor.init(std.testing.allocator) catch |err| switch (@as(anyerror, err)) {
        // Evented is optional in ordinary Raft tests. The dedicated gate must
        // still fail if the kernel or sandbox cannot provide io_uring.
        error.PermissionDenied, error.SystemOutdated => if (require_available) return err else return error.SkipZigTest,
        else => return err,
    };
    defer executor.deinit();
    const iface = executor.executor();
    try std.testing.expectEqual(ExecutorBackend.evented, iface.backend());
    try iface.startGroup(55);
    try std.testing.expect(iface.isActive(55));
    try iface.stopGroup(55);
    try std.testing.expect(!iface.isActive(55));
    const io = executor.io();
    try std.testing.expectEqual(@intFromPtr(executor.evented), @intFromPtr(io.userdata.?));
    const Work = struct {
        fn run(task_io: std.Io) !void {
            try std.Io.sleep(task_io, .fromMilliseconds(1), .awake);
        }
        fn wait(task_io: std.Io) !void {
            try std.Io.sleep(task_io, .fromSeconds(3600), .awake);
        }
        fn groupChild(task_io: std.Io) void {
            task_io.sleep(.fromSeconds(3600), .awake) catch {};
        }
        fn groupParent(task_io: std.Io, ready: *std.Io.Event) std.Io.Cancelable!void {
            var group: std.Io.Group = .init;
            defer group.cancel(task_io);
            group.async(task_io, groupChild, .{task_io});
            ready.set(task_io);
            try group.await(task_io);
        }
        fn immediate(value: u64) u64 {
            return value;
        }
    };
    var completed = try io.concurrent(Work.run, .{io});
    try completed.await(io);
    var canceled = try io.concurrent(Work.wait, .{io});
    try std.testing.expectError(error.Canceled, canceled.cancel(io));
    // Also cancel after the timer has had an opportunity to enter its wait.
    for (0..4) |_| {
        var sleeping = try io.concurrent(Work.wait, .{io});
        try io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expectError(error.Canceled, sleeping.cancel(io));
    }

    // Cancel a parent awaiting actual Io.Group children, rather than only
    // exercising the executor's group bookkeeping.
    for (0..4) |_| {
        var ready: std.Io.Event = .unset;
        var parent = try io.concurrent(Work.groupParent, .{ io, &ready });
        defer parent.cancel(io) catch {};
        ready.waitUncancelable(io);
        try io.sleep(.fromMilliseconds(1), .awake);
        try std.testing.expectError(error.Canceled, parent.cancel(io));
    }

    // Immediate completion must not release a fiber while its stack is active.
    // Repeat concurrent completion to exercise Dispatch's optimized handoff.
    for (0..16) |_| {
        var futures: [8]std.Io.Future(u64) = undefined;
        var launched: usize = 0;
        var joined: usize = 0;
        defer for (futures[joined..launched]) |*future| {
            _ = future.cancel(io);
        };
        for (&futures, 0..) |*future, value| {
            future.* = try io.concurrent(Work.immediate, .{@as(u64, value)});
            launched += 1;
        }
        while (joined < launched) {
            const value = futures[joined].await(io);
            joined += 1;
            try std.testing.expectEqual(@as(u64, joined - 1), value);
        }
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "evented-replay", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, "evented enrichment", 0);
    try file.sync(io);
    var bytes: [32]u8 = undefined;
    const n = try file.readPositionalAll(io, &bytes, 0);
    try std.testing.expectEqualStrings("evented enrichment", bytes[0..n]);
}
