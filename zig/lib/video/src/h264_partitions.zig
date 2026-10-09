// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! B/C association within one access-unit packet; category A stays in slice bits.
const std = @import("std");
const media = @import("antfly_media");
const Bits = @import("h264_bits.zig").Bits;
const Part = struct { id: u32, kind: u8, redundant: u32, bits: Bits, used: bool = false };
pub const Registry = struct {
    allocator: std.mem.Allocator,
    parts: std.ArrayList(Part) = .empty,
    pub const entry_bytes = @sizeOf(Part);
    pub fn init(allocator: std.mem.Allocator, packet: []const u8, length_bytes: u3, redundant: bool, maximum: usize, control: media.source.Control) !Registry {
        if (length_bytes == 0 or length_bytes > 4) return error.MalformedVideoPacket;
        var self = Registry{ .allocator = allocator };
        errdefer self.deinit();
        var cursor: usize = 0;
        while (cursor < packet.len) {
            try control.check();
            if (packet.len - cursor < length_bytes) return error.MalformedVideoPacket;
            var size: usize = 0;
            for (packet[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
            cursor += length_bytes;
            if (size == 0 or size > packet.len - cursor) return error.MalformedVideoPacket;
            const nal = packet[cursor..][0..size];
            cursor += size;
            const kind = nal[0] & 31;
            if (kind != 3 and kind != 4) continue;
            if (maximum > 4096 or self.parts.items.len >= maximum * 2) return error.ResourceLimitExceeded;
            var bits = try Bits.initControlled(allocator, nal, control);
            errdefer bits.deinit();
            const id = try bits.ue();
            const count = if (redundant) try bits.ue() else 0;
            if (count > 127) return error.MalformedVideoPacket;
            for (self.parts.items) |part| if (part.id == id and part.kind == kind and part.redundant == count) return error.DuplicateVideoPartition;
            try self.parts.ensureTotalCapacityPrecise(allocator, self.parts.items.len + 1);
            self.parts.appendAssumeCapacity(.{ .id = id, .kind = kind, .redundant = count, .bits = bits });
        }
        return self;
    }
    pub fn take(self: *Registry, id: u32, kind: u8, redundant: u32) !?*Bits {
        for (self.parts.items) |*part| if (part.id == id and part.kind == kind and part.redundant == redundant) {
            if (part.used) return error.DuplicateVideoPartition;
            part.used = true;
            return &part.bits;
        };
        return null;
    }
    pub fn finish(self: *Registry) !void {
        for (self.parts.items) |*part| {
            if (!part.used) return error.MissingVideoPartition;
            try part.bits.finish();
        }
    }
    pub fn deinit(self: *Registry) void {
        for (self.parts.items) |*part| part.bits.deinit();
        self.parts.deinit(self.allocator);
    }
};

test "partition registry rejects truncation duplicates and orphan payloads" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.MalformedVideoPacket, Registry.init(a, &.{0}, 4, false, 8, .{}));
    try std.testing.expectError(error.MalformedVideoPacket, Registry.init(a, &.{ 0, 0, 0, 3, 0x63 }, 4, false, 8, .{}));
    const part = [_]u8{ 0, 0, 0, 2, 0x63, 0xc0 }; // slice_id=0, rbsp trailing bit
    var duplicate: [part.len * 2]u8 = undefined;
    @memcpy(duplicate[0..part.len], &part);
    @memcpy(duplicate[part.len..], &part);
    try std.testing.expectError(error.DuplicateVideoPartition, Registry.init(a, &duplicate, 4, false, 8, .{}));
    var registry = try Registry.init(a, &part, 4, false, 8, .{});
    defer registry.deinit();
    try std.testing.expectError(error.MissingVideoPartition, registry.finish());
    try std.testing.expect((try registry.take(1, 3, 0)) == null);
    _ = try registry.take(0, 3, 0);
    try std.testing.expectError(error.DuplicateVideoPartition, registry.take(0, 3, 0));
    try registry.finish();
}
