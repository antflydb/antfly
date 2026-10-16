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
        return initMode(allocator, reader, first, dependency_start, initial, options, false, null);
    }
    pub fn initHeaders(allocator: std.mem.Allocator, reader: *media.mp4.Reader, first: usize, initial: []const u8, options: h264.Options, cached: ?[]const []u8) !Span {
        return initMode(allocator, reader, first, first, initial, options, true, cached);
    }
    fn initMode(allocator: std.mem.Allocator, reader: *media.mp4.Reader, first: usize, dependency_start: usize, initial: []const u8, options: h264.Options, headers: bool, cached: ?[]const []u8) !Span {
        var self = Span{ .allocator = allocator, .bytes = initial, .last = first };
        errdefer self.deinit();
        const flags = try kinds(initial, reader.track.nal_length_bytes);
        if (!flags.a) return self;
        self.partitioned = true;
        self.reservation = if (reader.input.admission_pool) |pool| try pool.acquire(.{ .host_bytes = @sizeOf(usize) }) else media.admission.Token{};
        try self.members.ensureTotalCapacityPrecise(allocator, 1);
        self.members.appendAssumeCapacity(first);
        var coded_bytes: usize = reader.packets[first].size;
        var next = first + 1;
        while (next < reader.packets.len and next - dependency_start < options.max_dependency_packets) : (next += 1) {
            try reader.input.control.check();
            if (reader.packets[next].size > options.max_packet_bytes) return error.ResourceLimitExceeded;
            var owned: ?[]u8 = null;
            defer if (owned) |bytes| allocator.free(bytes);
            const bytes_before = reader.input.total_bytes;
            var lease = if (headers) blk: {
                const bytes = if (cached) |packets| packets[next] else try @import("h264_prefix.zig").packet(allocator, reader, next, options.max_packet_bytes);
                if (cached == null) owned = bytes;
                break :blk media.source.Lease{ .bytes = bytes, .source = null };
            } else try reader.readPacket(next);
            defer lease.deinit();
            self.extra_read_bytes += reader.input.total_bytes - bytes_before;
            const part = try kinds(lease.bytes, reader.track.nal_length_bytes);
            if (part.a or part.other_picture or part.config) break;
            coded_bytes = try std.math.add(usize, coded_bytes, reader.packets[next].size);
            if (coded_bytes > options.max_packet_bytes) return error.ResourceLimitExceeded;
            const previous = if (self.storage.items.len == 0) initial.len else self.storage.items.len;
            const length = try std.math.add(usize, previous, lease.bytes.len);
            if (length > options.max_packet_bytes) return error.ResourceLimitExceeded;
            // Charge coalesced bytes, simultaneous growth and escaped/RBSP copies.
            // Source leases and the core workspace have separate reservations.
            const member_count = self.members.items.len + @intFromBool(part.residual);
            try self.reservation.resize(.{ .host_bytes = try std.math.add(usize, try std.math.mul(usize, length, 3), try std.math.mul(usize, member_count, @sizeOf(usize))) });
            // Precise capacity growth avoids uncharged allocator over-allocation.
            try self.storage.ensureTotalCapacityPrecise(allocator, length);
            if (self.storage.items.len == 0) self.storage.appendSliceAssumeCapacity(initial);
            self.storage.appendSliceAssumeCapacity(lease.bytes);
            if (part.residual) {
                try self.members.ensureTotalCapacityPrecise(allocator, member_count);
                self.members.appendAssumeCapacity(next);
            }
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
