// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
/// Owned RBSP, with the stop bit excluded from syntax reads.
pub const Bits = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    end: usize,
    position: usize = 0,
    pub fn init(allocator: std.mem.Allocator, nal: []const u8) !Bits {
        return initControlled(allocator, nal, .{});
    }
    pub fn initControlled(allocator: std.mem.Allocator, nal: []const u8, control: @import("antfly_media").source.Control) !Bits {
        try control.check();
        if (nal.len < 2 or nal[0] & 128 != 0) return error.MalformedVideoPacket;
        var output: std.ArrayList(u8) = .empty;
        errdefer output.deinit(allocator);
        try output.ensureTotalCapacityPrecise(allocator, nal.len - 1);
        var zeros: usize = 0;
        var cursor: usize = 1;
        while (cursor < nal.len) : (cursor += 1) {
            if (cursor % 4096 == 0) try control.check();
            const byte = nal[cursor];
            if (zeros == 2 and byte == 3) {
                if (cursor + 1 == nal.len or nal[cursor + 1] > 3) return error.MalformedVideoPacket;
                zeros = 0;
                continue;
            }
            if (zeros == 2 and byte <= 2) return error.MalformedVideoPacket;
            output.appendAssumeCapacity(byte);
            zeros = if (byte == 0) zeros + 1 else 0;
        }
        if (output.items.len == 0 or output.items[output.items.len - 1] == 0) return error.MalformedVideoPacket;
        const trailing: usize = @ctz(output.items[output.items.len - 1]);
        const end = output.items.len * 8 - trailing - 1;
        return .{ .allocator = allocator, .bytes = try output.toOwnedSlice(allocator), .end = end };
    }
    pub fn deinit(self: *Bits) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }
    pub fn read(self: *Bits, count: usize) !u32 {
        if (count > 32 or count > self.end -| self.position) return error.MalformedVideoPacket;
        var result: u32 = 0;
        for (0..count) |_| {
            result = (result << 1) | ((self.bytes[self.position / 8] >> @as(u3, @intCast(7 - self.position % 8))) & 1);
            self.position += 1;
        }
        return result;
    }
    pub fn ue(self: *Bits) !u32 {
        var leading: usize = 0;
        while (try self.read(1) == 0) {
            leading += 1;
            if (leading > 30) return error.MalformedVideoPacket;
        }
        return (@as(u32, 1) << @as(u5, @intCast(leading))) - 1 + try self.read(leading);
    }
    pub fn se(self: *Bits) !i32 {
        const code = try self.ue();
        return if (code & 1 != 0) @as(i32, @intCast((code + 1) / 2)) else -@as(i32, @intCast(code / 2));
    }
    pub fn finish(self: *const Bits) !void {
        if (self.position != self.end) return error.MalformedVideoPacket;
    }
};
