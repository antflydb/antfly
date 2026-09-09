//! Sequential render-lane heap. Reuses freed small slots immediately; all
//! backing slabs are visible to the caller's bounded allocator. No global
//! state or thread-local ownership: a joined lane may move between workers.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Heap = struct {
    backing_allocator: Allocator = undefined,
    slabs: ?*Slab = null,
    available: [13]?*Slab = @splat(null), // 8 through 32768 bytes
    pub const init: Heap = .{};
    const slab_size: usize = 256 * 1024;
    const slab_alignment: std.mem.Alignment = .fromByteUnits(slab_size);
    const Free = struct { next: ?*Free };
    const Slab = struct {
        next: ?*Slab,
        next_available: ?*Slab,
        free: ?*Free,
        used: usize,
        cursor: usize,
        capacity: usize,
        class: usize,
    };

    pub fn allocator(self: *Heap) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = Allocator.noRemap, .free = free } };
    }

    pub fn deinit(self: *Heap) void {
        var slab = self.slabs;
        while (slab) |current| {
            slab = current.next;
            self.backing_allocator.rawFree(@as([*]u8, @ptrCast(current))[0..slab_size], slab_alignment, @returnAddress());
        }
        self.slabs = null;
        self.available = @splat(null);
    }

    fn sizeClass(len: usize, alignment: std.mem.Alignment) ?usize {
        const needed = @max(@max(len, alignment.toByteUnits()), 8);
        if (needed > 32768) return null;
        return std.math.log2_int_ceil(usize, needed) - 3;
    }

    fn trim(self: *Heap) void {
        self.available = @splat(null);
        var link = &self.slabs;
        while (link.*) |slab| {
            if (slab.used == 0) {
                link.* = slab.next;
                self.backing_allocator.rawFree(@as([*]u8, @ptrCast(slab))[0..slab_size], slab_alignment, @returnAddress());
            } else {
                if (slab.used < slab.capacity) {
                    slab.next_available = self.available[slab.class];
                    self.available[slab.class] = slab;
                }
                link = &slab.next;
            }
        }
    }

    fn backingAlloc(self: *Heap, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        if (self.backing_allocator.rawAlloc(len, alignment, ret_addr)) |ptr| return ptr;
        self.trim();
        return self.backing_allocator.rawAlloc(len, alignment, ret_addr);
    }

    fn roundedLarge(len: usize) ?usize {
        const page_size = std.heap.pageSize();
        const upper = std.math.add(usize, len, page_size - 1) catch return null;
        return upper & ~(page_size - 1);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Heap = @ptrCast(@alignCast(ctx));
        const class = sizeClass(len, alignment) orelse
            return self.backingAlloc(roundedLarge(len) orelse return null, @enumFromInt(@max(@intFromEnum(alignment), @intFromEnum(std.mem.Alignment.fromByteUnits(std.heap.pageSize())))), ret_addr);
        const slot_size = @as(usize, 8) << @intCast(class);
        const slab = self.available[class] orelse blk: {
            const memory = self.backingAlloc(slab_size, slab_alignment, ret_addr) orelse return null;
            const created: *Slab = @ptrCast(@alignCast(memory));
            const start = std.mem.alignForward(usize, @sizeOf(Slab), slot_size);
            created.* = .{ .next = self.slabs, .next_available = null, .free = null, .used = 0, .cursor = start, .capacity = (slab_size - start) / slot_size, .class = class };
            self.slabs = created;
            self.available[class] = created;
            break :blk created;
        };
        const ptr: [*]u8 = if (slab.free) |node| blk: {
            slab.free = node.next;
            break :blk @ptrCast(node);
        } else blk: {
            const offset = slab.cursor;
            slab.cursor += slot_size;
            break :blk @as([*]u8, @ptrCast(slab)) + offset;
        };
        slab.used += 1;
        if (slab.used == slab.capacity) self.available[class] = slab.next_available;
        return ptr;
    }

    fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, _: usize) bool {
        const old_class = sizeClass(memory.len, alignment);
        const new_class = sizeClass(new_len, alignment);
        if (old_class != null or new_class != null) return old_class == new_class;
        return roundedLarge(memory.len) == roundedLarge(new_len);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Heap = @ptrCast(@alignCast(ctx));
        const class = sizeClass(memory.len, alignment) orelse {
            self.backing_allocator.rawFree(memory.ptr[0..roundedLarge(memory.len).?], @enumFromInt(@max(@intFromEnum(alignment), @intFromEnum(std.mem.Alignment.fromByteUnits(std.heap.pageSize())))), ret_addr);
            return;
        };
        const slab: *Slab = @ptrFromInt(@intFromPtr(memory.ptr) & ~(slab_size - 1));
        std.debug.assert(slab.class == class and slab.used > 0);
        if (slab.used == slab.capacity) {
            slab.next_available = self.available[class];
            self.available[class] = slab;
        }
        slab.used -= 1;
        const node: *Free = @ptrCast(@alignCast(memory.ptr));
        node.* = .{ .next = slab.free };
        slab.free = node;
    }
};

test "render heap reuses slots while other slots remain live" {
    var heap = Heap{ .backing_allocator = std.testing.allocator };
    defer heap.deinit();
    const alloc = heap.allocator();
    const pinned = try alloc.alloc(u8, 12);
    defer alloc.free(pinned);
    const first = try alloc.alloc(u8, 12);
    alloc.free(first);
    for (0..10000) |_| {
        const reused = try alloc.alloc(u8, 12);
        try std.testing.expectEqual(first.ptr, reused.ptr);
        alloc.free(reused);
    }
}

test "render heap handles alignment large allocations and allocation failures" {
    const Exercise = struct {
        fn run(backing: Allocator) !void {
            var heap = Heap{ .backing_allocator = backing };
            defer heap.deinit();
            const alloc = heap.allocator();
            const small = try alloc.alignedAlloc(u8, .@"64", 17);
            defer alloc.free(small);
            @memset(small, 23);
            const large = try alloc.alignedAlloc(u8, .fromByteUnits(65536), 70000);
            defer alloc.free(large);
            @memset(large, 42);
            try std.testing.expectEqual(@as(u8, 23), small[16]);
            try std.testing.expectEqual(@as(u8, 42), large[69999]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}

test "render heap mixed size reuse preserves live allocations" {
    var heap = Heap{ .backing_allocator = std.testing.allocator };
    defer heap.deinit();
    const alloc = heap.allocator();
    var live: [64]?[]align(64) u8 = @splat(null);
    defer for (live) |memory| if (memory) |bytes| alloc.free(bytes);
    var random = std.Random.DefaultPrng.init(0x504446);
    for (0..4000) |_| {
        const index = random.random().uintLessThan(usize, live.len);
        const marker: u8 = @intCast(index);
        if (live[index]) |bytes| {
            for (bytes) |byte| try std.testing.expectEqual(marker, byte);
            alloc.free(bytes);
            live[index] = null;
        } else {
            const bytes = try alloc.alignedAlloc(u8, .@"64", random.random().uintLessThan(usize, 80000) + 1);
            @memset(bytes, marker);
            live[index] = bytes;
        }
    }
}
