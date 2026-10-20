// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-local allocation owner with chunked admission and complete rollback.
//! Small allocations reuse capacity without resource-manager/IPC calls.
const std = @import("std");
const sessions = @import("session.zig");
const memory = @import("../runtime/tier/memory.zig");

pub const AdmittedAllocator = struct {
    backing: std.mem.Allocator,
    session: ?sessions.Session = null,
    admission_context: ?*anyopaque = null,
    admit: ?*const fn (*anyopaque, usize) anyerror!memory.AdmissionLease = null,
    trim_on_free: bool = false,
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

    fn reserve(self: *@This(), amount: usize) !?memory.AdmissionLease {
        if (self.admit) |callback| return try callback(self.admission_context.?, amount);
        const session = self.session orelse return error.ResourceLimitExceeded;
        return (try session.admitHostPreprocess(amount)).lease;
    }

    pub fn ensureCapacity(self: *@This(), extra: usize) !void {
        const needed = try std.math.add(usize, try std.math.add(usize, self.live_bytes, self.metadata_bytes), extra);
        if (needed <= self.reserved_bytes) return;
        const minimum = try std.math.add(usize, needed - self.reserved_bytes, @sizeOf(Reservation));
        var amount = @max(minimum, quantum);
        var lease = self.reserve(amount) catch |err| blk: {
            if (amount == minimum or (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable)) return err;
            amount = minimum; // Slack must not reject requests fitting tight limits.
            break :blk try self.reserve(amount);
        };
        errdefer if (lease) |*active| active.release();
        const reservation = try self.backing.create(Reservation);
        reservation.* = .{ .next = self.reservations, .lease = lease, .bytes = amount };
        self.reservations = reservation;
        self.metadata_bytes += @sizeOf(Reservation);
        self.reserved_bytes += amount;
    }

    pub fn trim(self: *@This()) !void {
        // A reservation must not keep its own bookkeeping allocation alive
        // after the last payload is freed. Release both owners together.
        if (self.live_bytes == 0) {
            while (self.reservations) |reservation| {
                self.reservations = reservation.next;
                if (reservation.lease) |*lease| lease.release();
                self.reserved_bytes -= reservation.bytes;
                self.metadata_bytes -= @sizeOf(Reservation);
                self.backing.destroy(reservation);
            }
            return;
        }
        var link = &self.reservations;
        while (link.*) |reservation| {
            const spare = self.reserved_bytes - self.live_bytes - self.metadata_bytes;
            const reduction = @min(spare, reservation.bytes);
            if (reduction != 0) {
                if (reservation.lease) |*lease| try lease.retain(.{ .host_scratch_bytes = reservation.bytes - reduction });
                reservation.bytes -= reduction;
                self.reserved_bytes -= reduction;
            }
            if (reservation.bytes == 0) {
                link.* = reservation.next;
                if (reservation.lease) |*lease| lease.release();
                self.metadata_bytes -= @sizeOf(Reservation);
                self.backing.destroy(reservation);
            } else link = &reservation.next;
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

    // Logical shrink must not hide retained backing bytes from an outer
    // BoundedAllocator. A failed physical resize uses allocate/copy/free.
    fn resize(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const overhead = allocationOverhead(alignment);
        const base = buffer.ptr - overhead;
        const header: *Header = @ptrCast(@alignCast(base));
        if (new_len == buffer.len) return true;
        if (new_len == 0 or new_len > buffer.len) return false;
        const bytes = std.math.add(usize, overhead, new_len) catch return false;
        const old_bytes = header.bytes;
        if (!self.backing.rawResize(base[0..old_bytes], header.alignment, bytes, ra)) return false;
        header.bytes = bytes;
        self.live_bytes -= old_bytes - bytes;
        if (self.trim_on_free) self.trim() catch {};
        return true;
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
        if (self.trim_on_free and (self.live_bytes == 0 or self.reserved_bytes - self.live_bytes - self.metadata_bytes >= quantum)) self.trim() catch {};
    }
};

test "model aware media adaptive admission shares capacity grows and releases idle reservations" {
    const Provider = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 160 * 1024 }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var provider = Provider{};
    var first = AdmittedAllocator{ .backing = std.testing.allocator, .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer first.deinit();
    var second = AdmittedAllocator{ .backing = std.testing.allocator, .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer second.deinit();
    const one = try first.allocator().alloc(u8, 1024);
    const two = try second.allocator().alloc(u8, 1024);
    try std.testing.expect(provider.controller.snapshot().host_scratch_bytes < 160 * 1024);
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 160 * 1024));
    try std.testing.expect(first.admission_error != null);
    first.allocator().free(one);
    try first.trim();
    second.allocator().free(two);
    try second.trim();
    // Repeated small requests must not accumulate zero-byte lease records.
    for (0..100) |_| {
        const bytes = try first.allocator().alloc(u8, 1024);
        first.allocator().free(bytes);
        try first.trim();
        try std.testing.expectEqual(@as(usize, 0), first.metadata_bytes);
        try std.testing.expectEqual(@as(usize, 0), first.reserved_bytes);
    }
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var resized = AdmittedAllocator{ .backing = no_resize.allocator(), .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer resized.deinit();
    var ceiling = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = resized.allocator(), .limit = 2048 };
    var payload = try ceiling.allocator().alloc(u8, 1024);
    @memset(payload, 7);
    payload = try ceiling.allocator().realloc(payload, 512);
    try std.testing.expectEqual(@as(usize, 512), ceiling.live);
    try std.testing.expectEqual(ceiling.live + AdmittedAllocator.allocationOverhead(.of(u8)), resized.live_bytes);
    try std.testing.expectEqual(@as(u8, 7), payload[511]);
    ceiling.allocator().free(payload);
    try std.testing.expectEqual(@as(usize, 0), resized.reserved_bytes);
    first.deinit();
    second.deinit();
    try std.testing.expectEqual(@as(usize, 0), provider.controller.snapshot().host_scratch_bytes);
}
