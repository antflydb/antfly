// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded gathering of partition-only transport packets after category A.
//! Slice identity is validated by Registry; coded-picture boundaries stop reads.
const std = @import("std");
const media = @import("antfly_media");
const avc = @import("avc.zig");
const h264 = @import("h264.zig");
pub const Span = struct {
    allocator: std.mem.Allocator,
    storage: std.ArrayList(u8) = .empty,
    members: std.ArrayList(usize) = .empty,
    reservation: media.admission.Token = .{},
    bytes: []const u8,
    last: usize,
    extra_read_bytes: u64 = 0,
    partitioned: bool = false,
    pub fn init(allocator: std.mem.Allocator, reader: *media.mp4.Reader, first: usize, dependency_start: usize, initial: []const u8, options: h264.Options) !Span {
        var self = Span{ .allocator = allocator, .bytes = initial, .last = first };
        errdefer self.deinit();
        const flags = try kinds(initial, reader.track.nal_length_bytes);
        if (!flags.a) return self;
        self.partitioned = true;
        self.reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = try std.math.add(usize, options.max_packet_bytes, options.max_dependency_packets * @sizeOf(usize)) }) else media.admission.Token{};
        try self.members.append(allocator, first);
        var next = first + 1;
        while (next < reader.packets.len and next - dependency_start < options.max_dependency_packets) : (next += 1) {
            try reader.input.control.check();
            if (reader.packets[next].size > options.max_packet_bytes) return error.ResourceLimitExceeded;
            var lease = try reader.readPacket(next);
            defer lease.deinit();
            self.extra_read_bytes += lease.bytes.len;
            const part = try kinds(lease.bytes, reader.track.nal_length_bytes);
            if (part.a or part.other_picture or part.config) break;
            if (self.storage.items.len == 0) try self.storage.appendSlice(allocator, initial);
            const length = try std.math.add(usize, self.storage.items.len, lease.bytes.len);
            if (length > options.max_packet_bytes) return error.ResourceLimitExceeded;
            // Precise capacity growth avoids uncharged allocator over-allocation.
            try self.storage.ensureTotalCapacityPrecise(allocator, length);
            self.storage.appendSliceAssumeCapacity(lease.bytes);
            if (part.residual) try self.members.append(allocator, next);
            self.last = next;
        }
        if (self.storage.items.len != 0) self.bytes = self.storage.items;
        return self;
    }
    pub fn deinit(self: *Span) void {
        self.storage.deinit(self.allocator);
        self.members.deinit(self.allocator);
        self.reservation.deinit();
    }
};
const Kinds = struct { a: bool = false, residual: bool = false, other_picture: bool = false, config: bool = false };
fn kinds(bytes: []const u8, length: u3) !Kinds {
    try avc.validatePortablePacket(bytes, length);
    var result = Kinds{};
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        var size: usize = 0;
        for (bytes[cursor..][0..length]) |byte| size = (size << 8) | byte;
        cursor += length;
        switch (bytes[cursor] & 31) {
            2 => result.a = true,
            3, 4 => result.residual = true,
            1, 5 => result.other_picture = true,
            7, 8 => result.config = true,
            else => {},
        }
        cursor += size;
    }
    return result;
}
