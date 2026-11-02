// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Concurrent child arenas borrow the caller's charged allocation owner.
//! Keep this owner at a stable address until every task and child arena joins.
const std = @import("std");
const Shared = @import("allocation_budget.zig").Shared;

pub const FanoutMemory = struct {
    child: std.mem.Allocator,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    budget: Shared,

    pub fn init(self: *FanoutMemory, child: std.mem.Allocator, io: std.Io, limit: usize) void {
        self.* = .{
            .child = child,
            .io = io,
            .budget = .{
                .child = .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } },
                .limit = limit,
            },
        };
    }

    pub fn allocator(self: *FanoutMemory) std.mem.Allocator {
        return self.budget.allocator();
    }

    pub fn deinit(self: *FanoutMemory) void {
        std.debug.assert(self.budget.stats().used_bytes == 0);
        self.* = undefined;
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *FanoutMemory = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawAlloc(len, alignment, ra);
    }

    fn resize(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *FanoutMemory = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawResize(bytes, alignment, len, ra);
    }

    fn remap(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *FanoutMemory = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.child.rawRemap(bytes, alignment, len, ra);
    }

    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *FanoutMemory = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.child.rawFree(bytes, alignment, ra);
    }
};

test "workload envelope fanout children share caller charge and retire after joins" {
    var caller: Shared = .{ .child = std.testing.allocator, .limit = 64 << 10 };
    var memory: FanoutMemory = undefined;
    memory.init(caller.allocator(), std.testing.io, 32 << 10);
    defer memory.deinit();
    const Worker = struct {
        fn run(owner: *FanoutMemory) void {
            var arena = std.heap.ArenaAllocator.init(owner.allocator());
            defer arena.deinit();
            const bytes = arena.allocator().alloc(u8, 1024) catch @panic("bounded test allocation failed");
            @memset(bytes, 42);
        }
    };
    var group: std.Io.Group = .init;
    for (0..8) |_| group.async(std.testing.io, Worker.run, .{&memory});
    try group.await(std.testing.io);
    try std.testing.expect(caller.stats().peak_bytes > 0);
    try std.testing.expectEqual(@as(usize, 0), caller.stats().used_bytes);
    const bytes = try memory.allocator().alloc(u8, 32 << 10);
    try std.testing.expectError(error.OutOfMemory, memory.allocator().alloc(u8, 1));
    memory.allocator().free(bytes);
    try std.testing.expectEqual(@as(usize, 0), caller.stats().used_bytes);
}
