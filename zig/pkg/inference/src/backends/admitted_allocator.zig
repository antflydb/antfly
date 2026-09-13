//! Request-local allocation owner with chunked admission and complete rollback.
//! Small allocations reuse capacity without resource-manager/IPC calls.
const std = @import("std");
const sessions = @import("session.zig");
const memory = @import("../runtime/tier/memory.zig");

pub const AdmittedAllocator = struct {
    backing: std.mem.Allocator,
    session: sessions.Session,
    live_bytes: usize = 0,
    reserved_bytes: usize = 0,
    metadata_bytes: usize = 0,
    admission_error: ?anyerror = null,
    blocks: ?*Header = null,
    reservations: ?*Reservation = null,

    const quantum = 64 * 1024;
    const Header = struct { prev: ?*Header, next: ?*Header, bytes: usize, alignment: std.mem.Alignment };
    const Reservation = struct { next: ?*Reservation, lease: ?memory.AdmissionLease, bytes: usize };
    pub const reservation_overhead = @sizeOf(Reservation);

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn allocationOverhead(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Header), alignment.toByteUnits());
    }

    fn ensureCapacity(self: *@This(), extra: usize) !void {
        const needed = try std.math.add(usize, try std.math.add(usize, self.live_bytes, self.metadata_bytes), extra);
        if (needed <= self.reserved_bytes) return;
        const minimum = try std.math.add(usize, needed - self.reserved_bytes, @sizeOf(Reservation));
        var amount = @max(minimum, quantum);
        var permit = self.session.admitHostPreprocess(amount) catch |err| blk: {
            if (amount == minimum or (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable)) return err;
            amount = minimum; // Slack must not reject requests fitting tight limits.
            break :blk try self.session.admitHostPreprocess(amount);
        };
        errdefer permit.deinit();
        const reservation = try self.backing.create(Reservation);
        reservation.* = .{ .next = self.reservations, .lease = permit.lease, .bytes = amount };
        self.reservations = reservation;
        self.metadata_bytes += @sizeOf(Reservation);
        self.reserved_bytes += amount;
    }

    pub fn trim(self: *@This()) !void {
        var spare = self.reserved_bytes - self.live_bytes - self.metadata_bytes;
        var next = self.reservations;
        while (next) |reservation| : (next = reservation.next) {
            const reduction = @min(spare, reservation.bytes);
            if (reduction == 0) continue;
            if (reservation.lease) |*lease| try lease.retain(.{ .host_scratch_bytes = reservation.bytes - reduction });
            reservation.bytes -= reduction;
            self.reserved_bytes -= reduction;
            spare -= reduction;
        }
    }

    /// Includes allocations abandoned by a failing tokenizer.
    pub fn deinit(self: *@This()) void {
        while (self.blocks) |block| {
            self.blocks = block.next;
            self.backing.rawFree(@as([*]u8, @ptrCast(block))[0..block.bytes], block.alignment, @returnAddress());
        }
        while (self.reservations) |reservation| {
            self.reservations = reservation.next;
            var lease = reservation.lease;
            self.backing.destroy(reservation);
            if (lease) |*active| active.release();
        }
        self.live_bytes = 0;
        self.reserved_bytes = 0;
        self.metadata_bytes = 0;
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.admission_error = null;
        const bytes = std.math.add(usize, allocationOverhead(alignment), len) catch return null;
        self.ensureCapacity(bytes) catch |err| {
            self.admission_error = err;
            return null;
        };
        const base = self.backing.rawAlloc(bytes, alignment.max(.of(Header)), ra) orelse return null;
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .prev = null, .next = self.blocks, .bytes = bytes, .alignment = alignment.max(.of(Header)) };
        if (self.blocks) |old| old.prev = header;
        self.blocks = header;
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
        if (header.prev) |prev| prev.next = header.next else self.blocks = header.next;
        if (header.next) |next| next.prev = header.prev;
        self.live_bytes -= header.bytes;
        self.backing.rawFree(base[0..header.bytes], header.alignment, ra);
    }
};
