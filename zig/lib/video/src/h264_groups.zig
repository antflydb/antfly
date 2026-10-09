// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! H.264 8.2.2 slice-group maps. Storage and work are bounded by picture geometry.
const std = @import("std");
const Bits = @import("h264_bits.zig").Bits;
pub const Groups = struct {
    count: u8 = 1,
    kind: u32 = 0,
    run: [8]usize = @splat(0),
    top: [7]usize = @splat(0),
    bottom: [7]usize = @splat(0),
    direction: bool = false,
    rate: usize = 1,
    explicit: ?[]u8 = null,
    pub fn deinit(self: Groups, allocator: std.mem.Allocator) void {
        if (self.explicit) |map| allocator.free(map);
    }
    pub fn parse(bits: *Bits, allocator: std.mem.Allocator, width: usize, height: usize) !Groups {
        const minus1 = try bits.ue();
        if (minus1 > 7) return error.MalformedVideoConfig;
        var self = Groups{ .count = @intCast(minus1 + 1) };
        errdefer self.deinit(allocator);
        if (self.count == 1) return self;
        self.kind = try bits.ue();
        const size = width * height;
        switch (self.kind) {
            0 => for (self.run[0..self.count]) |*run| {
                run.* = @as(usize, try bits.ue()) + 1;
                if (run.* > size) return error.MalformedVideoConfig;
            },
            1 => {},
            2 => for (0..minus1) |i| {
                self.top[i] = try bits.ue();
                self.bottom[i] = try bits.ue();
                if (self.bottom[i] >= size or self.top[i] / width > self.bottom[i] / width or self.top[i] % width > self.bottom[i] % width) return error.MalformedVideoConfig;
            },
            3...5 => {
                if (self.count != 2) return error.MalformedVideoConfig;
                self.direction = try bits.read(1) != 0;
                self.rate = @as(usize, try bits.ue()) + 1;
                if (self.rate > size) return error.MalformedVideoConfig;
            },
            6 => {
                if (@as(usize, try bits.ue()) + 1 != size) return error.MalformedVideoConfig;
                self.explicit = try allocator.alloc(u8, size);
                const n = std.math.log2_int_ceil(u8, self.count);
                for (self.explicit.?) |*group| {
                    group.* = @intCast(try bits.read(n));
                    if (group.* >= self.count) return error.MalformedVideoConfig;
                }
            },
            else => return error.MalformedVideoConfig,
        }
        return self;
    }
    pub fn cycle(self: Groups, bits: *Bits, size: usize) !usize {
        if (self.count == 1 or self.kind < 3 or self.kind > 5) return 0;
        const maximum = std.math.divCeil(usize, size, self.rate) catch unreachable;
        const value = try bits.read(std.math.log2_int_ceil(usize, maximum + 1));
        if (value > maximum) return error.MalformedVideoPacket;
        return value;
    }
    pub fn build(self: Groups, map: []u8, width: usize, change: usize) void {
        if (self.count == 1) {
            @memset(map, 0);
            return;
        }
        const height = map.len / width;
        switch (self.kind) {
            0 => {
                var i: usize = 0;
                while (i < map.len) for (0..self.count) |group| {
                    const end = @min(map.len, i + self.run[group]);
                    @memset(map[i..end], @intCast(group));
                    i = end;
                };
            },
            1 => for (map, 0..) |*group, i| {
                group.* = @intCast((i % width + (i / width * self.count) / 2) % self.count);
            },
            2 => {
                @memset(map, self.count - 1);
                var group: usize = self.count - 1;
                while (group != 0) {
                    group -= 1;
                    for (self.top[group] / width..self.bottom[group] / width + 1) |y| {
                        @memset(map[y * width + self.top[group] % width .. y * width + self.bottom[group] % width + 1], @intCast(group));
                    }
                }
            },
            3 => {
                @memset(map, 1);
                const direction: i32 = @intFromBool(self.direction);
                const w: i32 = @intCast(width);
                const h: i32 = @intCast(height);
                var x = @divTrunc(w - direction, 2);
                var y = @divTrunc(h - direction, 2);
                var left = x;
                var right = x;
                var top = y;
                var bottom = y;
                var dx = direction - 1;
                var dy = direction;
                var remaining = @min(map.len, change * self.rate);
                while (remaining != 0) {
                    const i: usize = @intCast(y * w + x);
                    if (map[i] == 1) {
                        map[i] = 0;
                        remaining -= 1;
                    }
                    if (dx == -1 and x == left) {
                        left = @max(left - 1, 0);
                        x = left;
                        dx = 0;
                        dy = 2 * direction - 1;
                    } else if (dx == 1 and x == right) {
                        right = @min(right + 1, w - 1);
                        x = right;
                        dx = 0;
                        dy = 1 - 2 * direction;
                    } else if (dy == -1 and y == top) {
                        top = @max(top - 1, 0);
                        y = top;
                        dx = 1 - 2 * direction;
                        dy = 0;
                    } else if (dy == 1 and y == bottom) {
                        bottom = @min(bottom + 1, h - 1);
                        y = bottom;
                        dx = 2 * direction - 1;
                        dy = 0;
                    } else {
                        x += dx;
                        y += dy;
                    }
                }
            },
            4, 5 => {
                const count = @min(map.len, change * self.rate);
                const upper = if (self.direction) map.len - count else count;
                for (map, 0..) |*group, i| {
                    const position = if (self.kind == 4) i else i % width * height + i / width;
                    group.* = @intFromBool(if (position < upper) self.direction else !self.direction);
                }
            },
            6 => @memcpy(map, self.explicit.?),
            else => unreachable,
        }
    }
};
pub fn next(map: []const u8, current: usize, size: usize) usize {
    var i = current + 1;
    if (map.len != 0) while (i < size and map[i] != map[current]) {
        i += 1;
    };
    return i;
}
