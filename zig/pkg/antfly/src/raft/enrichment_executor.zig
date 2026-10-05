// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

const std = @import("std");
const builtin = @import("builtin");

// Linux io_uring supports fibers, timers, cancellation, and positional file I/O.
// Network operations remain incomplete upstream; production transports use Threaded.
const supports_evented_executor = builtin.os.tag == .linux and std.Io.fiber.supported;
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

pub fn testEventedExecutor() !void {
    if (!supports_evented_executor) {
        try std.testing.expectError(error.UnsupportedEventedBackend, EventedExecutor.init(std.testing.allocator));
        return;
    }

    var executor = try EventedExecutor.init(std.testing.allocator);
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
    };
    var completed = try io.concurrent(Work.run, .{io});
    try completed.await(io);
    var canceled = try io.concurrent(Work.wait, .{io});
    try std.testing.expectError(error.Canceled, canceled.cancel(io));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "evented-replay", .{ .read = true });
    defer file.close(io);
    try file.writePositionalAll(io, "evented enrichment", 0);
    var bytes: [32]u8 = undefined;
    const n = try file.readPositionalAll(io, &bytes, 0);
    try std.testing.expectEqualStrings("evented enrichment", bytes[0..n]);
}
