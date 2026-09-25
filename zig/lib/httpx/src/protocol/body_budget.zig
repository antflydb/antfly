//! Shared request-body memory admission.

const std = @import("std");

/// Server-scoped byte budget shared by every inbound HTTP connection on that
/// server. Separate listeners own separate budgets; this is not a process heap
/// limit.
///
/// Charges allocation capacity, including cached buffers and overlapping
/// allocations during growth or materialization. Each parser, stream, or
/// request releases its reservation only after freeing its retained storage.
pub const SharedBodyBudget = struct {
    capacity: usize,
    in_use: std.atomic.Value(usize) = .init(0),
    peak_in_use: std.atomic.Value(usize) = .init(0),
    rejected_total: std.atomic.Value(u64) = .init(0),

    pub fn init(capacity: usize) SharedBodyBudget {
        return .{ .capacity = capacity };
    }

    pub fn tryReserve(self: *@This(), amount: usize) bool {
        if (amount == 0) return true;
        var observed = self.in_use.load(.acquire);
        while (true) {
            const next = std.math.add(usize, observed, amount) catch {
                _ = self.rejected_total.fetchAdd(1, .monotonic);
                return false;
            };
            if (next > self.capacity) {
                _ = self.rejected_total.fetchAdd(1, .monotonic);
                return false;
            }
            if (self.in_use.cmpxchgWeak(observed, next, .acq_rel, .acquire) == null) {
                var peak = self.peak_in_use.load(.acquire);
                while (peak < next) {
                    if (self.peak_in_use.cmpxchgWeak(peak, next, .acq_rel, .acquire) == null) break;
                    peak = self.peak_in_use.load(.acquire);
                }
                return true;
            }
            observed = self.in_use.load(.acquire);
        }
    }

    pub fn release(self: *@This(), amount: usize) void {
        if (amount == 0) return;
        const previous = self.in_use.fetchSub(amount, .acq_rel);
        std.debug.assert(previous >= amount);
    }

    pub const Stats = struct {
        capacity: usize,
        in_use: usize,
        peak_in_use: usize,
        rejected_total: u64,
    };

    pub fn stats(self: *const @This()) Stats {
        return .{
            .capacity = self.capacity,
            .in_use = self.in_use.load(.acquire),
            .peak_in_use = self.peak_in_use.load(.acquire),
            .rejected_total = self.rejected_total.load(.acquire),
        };
    }
};

test "SharedBodyBudget bounds aggregate reservations and records pressure" {
    var budget = SharedBodyBudget.init(5);
    try std.testing.expect(budget.tryReserve(4));
    try std.testing.expect(!budget.tryReserve(2));
    try std.testing.expectEqual(@as(usize, 4), budget.stats().in_use);
    try std.testing.expectEqual(@as(usize, 4), budget.stats().peak_in_use);
    try std.testing.expectEqual(@as(u64, 1), budget.stats().rejected_total);
    budget.release(4);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().in_use);
}

/// Grow a retained body buffer while charging its allocation capacity. The
/// temporary allocator exists only during growth; the owning parser/reader
/// frees the final buffer before releasing `reserved` at retirement.
///
/// ArrayList may fall back from remap to allocate-copy-free. Charging both
/// allocations in that case bounds the transient peak as well as steady state.
pub fn ensureBufferCapacity(
    budget: ?*SharedBodyBudget,
    backing: std.mem.Allocator,
    buffer: *std.ArrayListUnmanaged(u8),
    minimum: usize,
    reserved: *usize,
) !void {
    if (minimum <= buffer.capacity) return;
    const shared = budget orelse return buffer.ensureTotalCapacity(backing, minimum);
    std.debug.assert(reserved.* == buffer.capacity);
    const available = shared.capacity -| shared.stats().in_use;
    const geometric = if (buffer.capacity == 0) minimum else buffer.capacity +| @max(buffer.capacity / 2, 8);
    const target = @max(minimum, @min(geometric, buffer.capacity +| available));
    return ensureBufferCapacityPrecise(shared, backing, buffer, target, reserved);
}

/// Exact growth target for callers that already cap their geometric policy.
pub fn ensureBufferCapacityPrecise(
    budget: ?*SharedBodyBudget,
    backing: std.mem.Allocator,
    buffer: *std.ArrayListUnmanaged(u8),
    minimum: usize,
    reserved: *usize,
) !void {
    if (minimum <= buffer.capacity) return;
    const shared = budget orelse return buffer.ensureTotalCapacityPrecise(backing, minimum);
    std.debug.assert(reserved.* == buffer.capacity);
    var tracker = CapacityAllocator{ .backing = backing, .budget = shared, .reserved = reserved };
    buffer.ensureTotalCapacityPrecise(tracker.allocator(), minimum) catch |err| {
        if (tracker.denied) return error.BodyCapacityExceeded;
        return err;
    };
    std.debug.assert(reserved.* == buffer.capacity);
}

const CapacityAllocator = struct {
    backing: std.mem.Allocator,
    budget: *SharedBodyBudget,
    reserved: *usize,
    denied: bool = false,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn reserve(self: *@This(), bytes: usize) bool {
        if (!self.budget.tryReserve(bytes)) {
            self.denied = true;
            return false;
        }
        self.reserved.* += bytes;
        return true;
    }
    fn release(self: *@This(), bytes: usize) void {
        self.budget.release(bytes);
        self.reserved.* -= bytes;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.reserve(len)) return null;
        return self.backing.rawAlloc(len, alignment, ra) orelse {
            self.release(len);
            return null;
        };
    }
    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const growth = len -| memory.len;
        if (!self.reserve(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, len, ra)) {
            self.release(growth);
            return false;
        }
        self.release(memory.len -| len);
        return true;
    }
    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const growth = len -| memory.len;
        if (!self.reserve(growth)) return null;
        const result = self.backing.rawRemap(memory, alignment, len, ra) orelse {
            self.release(growth);
            return null;
        };
        self.release(memory.len -| len);
        return result;
    }
    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ra);
        self.release(memory.len);
    }
};

test "ingress capacity accounts spare capacity and simultaneous reallocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    const alloc = failing.allocator();
    var budget = SharedBodyBudget.init(32);
    var buffer = std.ArrayListUnmanaged(u8).empty;
    var reserved: usize = 0;
    defer {
        buffer.deinit(alloc);
        budget.release(reserved);
    }
    try ensureBufferCapacity(&budget, alloc, &buffer, 4, &reserved);
    buffer.appendSliceAssumeCapacity("abcd");
    try ensureBufferCapacity(&budget, alloc, &buffer, 5, &reserved);
    try std.testing.expectEqual(@as(usize, 12), buffer.capacity);
    try std.testing.expectEqual(buffer.capacity, reserved);
    try std.testing.expectEqual(reserved, budget.stats().in_use);
    try std.testing.expectEqual(@as(usize, 16), budget.stats().peak_in_use);
    try std.testing.expectEqualStrings("abcd", buffer.items);
    // Final capacity would fit, but both old+new storage would not.
    try std.testing.expectError(error.BodyCapacityExceeded, ensureBufferCapacity(&budget, alloc, &buffer, 25, &reserved));
    try std.testing.expectEqual(@as(usize, 12), reserved);
    try std.testing.expectEqualStrings("abcd", buffer.items);
}

test "ingress capacity rolls back backing OOM without misclassifying pressure" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1, .resize_fail_index = 0 });
    const alloc = failing.allocator();
    var budget = SharedBodyBudget.init(128);
    var buffer = std.ArrayListUnmanaged(u8).empty;
    var reserved: usize = 0;
    defer {
        buffer.deinit(alloc);
        budget.release(reserved);
    }
    try ensureBufferCapacity(&budget, alloc, &buffer, 4, &reserved);
    buffer.appendSliceAssumeCapacity("abcd");
    try std.testing.expectError(error.OutOfMemory, ensureBufferCapacity(&budget, alloc, &buffer, 5, &reserved));
    try std.testing.expectEqual(@as(usize, 4), reserved);
    try std.testing.expectEqual(reserved, budget.stats().in_use);
    try std.testing.expectEqual(@as(u64, 0), budget.stats().rejected_total);
    try std.testing.expectEqualStrings("abcd", buffer.items);
}
