//! Request-lifetime allocation scope for decoded input and output buffers.
//! Its address remains stable until the handler, socket writer and cleanup join.
const std = @import("std");
const Shared = @import("body_budget.zig").SharedBodyBudget;

pub const Scope = struct {
    child: std.mem.Allocator,
    shared: ?*Shared,
    live: std.atomic.Value(usize) = .init(0),

    pub fn allocator(self: *Scope) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    pub fn deinit(self: *Scope) void {
        std.debug.assert(self.live.load(.acquire) == 0);
    }
    fn reserve(self: *Scope, bytes: usize) bool {
        return if (self.shared) |shared| shared.tryReserve(bytes) else true;
    }
    fn release(self: *Scope, bytes: usize) void {
        if (self.shared) |shared| shared.release(bytes);
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Scope = @ptrCast(@alignCast(raw));
        if (!self.reserve(len)) return null;
        const result = self.child.rawAlloc(len, alignment, ra) orelse {
            self.release(len);
            return null;
        };
        _ = self.live.fetchAdd(len, .acq_rel);
        return result;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Scope = @ptrCast(@alignCast(raw));
        const growth = len -| bytes.len;
        if (!self.reserve(growth)) return false;
        if (!self.child.rawResize(bytes, alignment, len, ra)) {
            self.release(growth);
            return false;
        }
        _ = self.live.fetchAdd(growth, .acq_rel);
        _ = self.live.fetchSub(bytes.len -| len, .acq_rel);
        self.release(bytes.len -| len);
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Scope = @ptrCast(@alignCast(raw));
        const growth = len -| bytes.len;
        if (!self.reserve(growth)) return null;
        const result = self.child.rawRemap(bytes, alignment, len, ra) orelse {
            self.release(growth);
            return null;
        };
        _ = self.live.fetchAdd(growth, .acq_rel);
        _ = self.live.fetchSub(bytes.len -| len, .acq_rel);
        self.release(bytes.len -| len);
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Scope = @ptrCast(@alignCast(raw));
        self.child.rawFree(bytes, alignment, ra);
        const previous = self.live.fetchSub(bytes.len, .acq_rel);
        std.debug.assert(previous >= bytes.len);
        self.release(bytes.len);
    }
};

test "HTTP application memory keeps response storage charged until output destruction" {
    var shared = Shared.init(32);
    var first: Scope = .{ .child = std.testing.allocator, .shared = &shared };
    defer first.deinit();
    var second: Scope = .{ .child = std.testing.allocator, .shared = &shared };
    defer second.deinit();
    const output = try first.allocator().alloc(u8, 32);
    try std.testing.expectError(error.OutOfMemory, second.allocator().alloc(u8, 1));
    try std.testing.expect(!first.allocator().resize(output, 33));
    // Ending computation cannot release these bytes: the slow socket writer
    // still owns the response. Only actual destruction restores capacity.
    try std.testing.expectEqual(@as(usize, 32), shared.stats().in_use);
    first.allocator().free(output);
    const replacement = try second.allocator().alloc(u8, 32);
    second.allocator().free(replacement);
    try std.testing.expectEqual(@as(usize, 0), shared.stats().in_use);
}

test "HTTP application memory rolls back failed backing allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var shared = Shared.init(32);
    var scope: Scope = .{ .child = failing.allocator(), .shared = &shared };
    defer scope.deinit();
    try std.testing.expectError(error.OutOfMemory, scope.allocator().alloc(u8, 16));
    try std.testing.expectEqual(@as(usize, 0), shared.stats().in_use);
}
