//! Request-local host allocator. Every allocation, including tokenizer scratch
//! and realloc overlap, acquires capacity before touching the backing allocator.
//! The owner must have a stable address and outlive every returned allocation.
const std = @import("std");
const sessions = @import("session.zig");
const memory = @import("../runtime/tier/memory.zig");

pub const AdmittedAllocator = struct {
    backing: std.mem.Allocator,
    session: sessions.Session,
    live_bytes: usize = 0,
    admission_error: ?anyerror = null,

    const Header = struct { lease: ?memory.AdmissionLease, bytes: usize };

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn allocationOverhead(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Header), alignment.toByteUnits());
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const bytes = std.math.add(usize, allocationOverhead(alignment), len) catch return null;
        var permit = self.session.admitHostPreprocess(bytes) catch |err| {
            self.admission_error = err;
            return null;
        };
        const base = self.backing.rawAlloc(bytes, alignment.max(.of(Header)), ra) orelse {
            permit.deinit();
            return null;
        };
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .lease = permit.lease, .bytes = bytes };
        self.live_bytes += bytes;
        return base + allocationOverhead(alignment);
    }

    // Shrink/reuse admitted capacity in place. Actual growth uses
    // allocate/copy/free, accounting for both allocations during the copy.
    fn resize(_: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, new_len: usize, _: usize) bool {
        const header: *Header = @ptrCast(@alignCast(buffer.ptr - allocationOverhead(alignment)));
        return new_len <= header.bytes - allocationOverhead(alignment);
    }
    fn remap(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        return if (resize(raw, buffer, alignment, new_len, ra)) buffer.ptr else null;
    }
    fn free(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const base = buffer.ptr - allocationOverhead(alignment);
        const header: *Header = @ptrCast(@alignCast(base));
        const bytes = header.bytes;
        var lease = header.lease;
        self.backing.rawFree(base[0..bytes], alignment.max(.of(Header)), ra);
        self.live_bytes -= bytes;
        if (lease) |*active| active.release();
    }
};
