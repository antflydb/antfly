// Copyright 2026 Antfly, Inc.
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

//! Bounded, pure-Zig decoder for arithmetic-coded JBIG2 images embedded in
//! PDFs. The currently accepted profile covers symbol dictionaries, text
//! regions, generic refinement, and generic regions. Unsupported coding
//! profiles fail closed instead of returning a partial image.

const std = @import("std");

const Allocator = std.mem.Allocator;

/// Tracks every live allocation made while decoding. The wrapper delegates
/// allocations directly to `backing`, so the final page allocation can be
/// detached and returned to the caller without a copy.
const WorkingSetAllocator = struct {
    backing: Allocator,
    live_bytes: usize = 0,
    max_live_bytes: usize,
    limit_exceeded: bool = false,

    fn allocator(self: *WorkingSetAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn permitsGrowth(self: *WorkingSetAllocator, additional_bytes: usize) bool {
        if (additional_bytes <= self.max_live_bytes -| self.live_bytes) return true;
        self.limit_exceeded = true;
        return false;
    }

    fn disown(self: *WorkingSetAllocator, bytes: usize) void {
        std.debug.assert(bytes <= self.live_bytes);
        self.live_bytes -= bytes;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        if (!self.permitsGrowth(len)) return null;
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live_bytes += len;
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > 0 and !self.permitsGrowth(growth)) return null;
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.live_bytes = self.live_bytes -| memory.len +| new_len;
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *WorkingSetAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.live_bytes -|= memory.len;
    }
};

const DecodeWorkBudget = struct {
    remaining: u64,

    fn charge(self: *DecodeWorkBudget, units: u64) !void {
        if (units > self.remaining) return error.Jbig2WorkLimitExceeded;
        self.remaining -= units;
    }

    fn chargePixels(self: *DecodeWorkBudget, width: u32, height: u32) !void {
        const pixels = std.math.mul(u64, width, height) catch return error.Jbig2WorkLimitExceeded;
        try self.charge(pixels);
    }

    fn chargeBytes(self: *DecodeWorkBudget, bytes: usize) !void {
        try self.charge(std.math.cast(u64, bytes) orelse return error.Jbig2WorkLimitExceeded);
    }

    fn chargeItems(self: *DecodeWorkBudget, count: usize, units_per_item: u64) !void {
        const count_u64 = std.math.cast(u64, count) orelse return error.Jbig2WorkLimitExceeded;
        try self.charge(std.math.mul(u64, count_u64, units_per_item) catch return error.Jbig2WorkLimitExceeded);
    }
};

fn allocZeroedBytes(alloc: Allocator, work: *DecodeWorkBudget, len: usize) ![]u8 {
    try work.chargeBytes(len);
    const result = try alloc.alloc(u8, len);
    @memset(result, 0);
    return result;
}

pub const Decoded = struct {
    width: u32,
    height: u32,
    /// Packed MSB-first pixels: zero is white and one is black.
    pixels: []u8,

    pub fn deinit(self: *Decoded, alloc: Allocator) void {
        alloc.free(self.pixels);
        self.* = undefined;
    }
};

pub const ExpectedDimensions = struct { width: u32, height: u32 };

const Bitmap = struct {
    width: u32,
    height: u32,
    stride: usize,
    data: []u8,

    fn init(alloc: Allocator, width: u32, height: u32, max_bytes: usize) !Bitmap {
        if (width == 0 or height == 0) return error.InvalidJbig2Dimensions;
        const stride = (@as(usize, width) + 7) / 8;
        const len = std.math.mul(usize, stride, height) catch return error.Jbig2ImageTooLarge;
        if (len > max_bytes) return error.Jbig2ImageTooLarge;
        return .{ .width = width, .height = height, .stride = stride, .data = try alloc.alloc(u8, len) };
    }

    fn blank(alloc: Allocator, work: *DecodeWorkBudget, width: u32, height: u32, max_bytes: usize) !Bitmap {
        var result = try init(alloc, width, height, max_bytes);
        errdefer result.deinit(alloc);
        try work.chargeBytes(result.data.len);
        @memset(result.data, 0);
        return result;
    }

    fn clone(self: Bitmap, alloc: Allocator, work: *DecodeWorkBudget, max_bytes: usize) !Bitmap {
        var result = try init(alloc, self.width, self.height, max_bytes);
        errdefer result.deinit(alloc);
        try work.chargeBytes(result.data.len);
        @memcpy(result.data, self.data);
        return result;
    }

    fn deinit(self: *Bitmap, alloc: Allocator) void {
        alloc.free(self.data);
        self.* = undefined;
    }

    fn get(self: Bitmap, x: i64, y: i64) u1 {
        if (x < 0 or y < 0 or x >= self.width or y >= self.height) return 0;
        const ux: usize = @intCast(x);
        const index = @as(usize, @intCast(y)) * self.stride + ux / 8;
        return @truncate((self.data[index] >> @intCast(7 - (ux & 7))) & 1);
    }

    fn set(self: Bitmap, x: u32, y: u32, value: u1) void {
        const index = @as(usize, y) * self.stride + @as(usize, x) / 8;
        const shift: u3 = @intCast(7 - (x & 7));
        const mask: u8 = @as(u8, 1) << shift;
        if (value == 1) self.data[index] |= mask else self.data[index] &= ~mask;
    }

    fn compose(self: Bitmap, source: Bitmap, dx: i64, dy: i64, op: u3, work: *DecodeWorkBudget) !void {
        if (op > 4) return error.UnsupportedJbig2CombinationOperator;
        const x_clip = clippedRange(dx, source.width, self.width) orelse return;
        const y_clip = clippedRange(dy, source.height, self.height) orelse return;
        try work.chargePixels(x_clip.len, y_clip.len);
        var row: u32 = 0;
        while (row < y_clip.len) : (row += 1) {
            const sy = y_clip.source_start + row;
            const ty = y_clip.target_start + row;
            var column: u32 = 0;
            while (column < x_clip.len) : (column += 1) {
                const sx = x_clip.source_start + column;
                const tx = x_clip.target_start + column;
                const a = self.get(tx, ty);
                const b = source.get(sx, sy);
                const value: u1 = switch (op) {
                    0 => a | b,
                    1 => a & b,
                    2 => a ^ b,
                    3 => @truncate(~(a ^ b)),
                    4 => b,
                    else => unreachable,
                };
                self.set(tx, ty, value);
            }
        }
    }
};

const ClipRange = struct { source_start: u32, target_start: u32, len: u32 };

fn clippedRange(offset: i64, source_len: u32, target_len: u32) ?ClipRange {
    var source_start: u32 = 0;
    var target_start: u32 = 0;
    if (offset < 0) {
        if (offset <= -@as(i64, source_len)) return null;
        source_start = @intCast(-offset);
    } else {
        if (offset >= target_len) return null;
        target_start = @intCast(offset);
    }
    const len = @min(source_len - source_start, target_len - target_start);
    if (len == 0) return null;
    return .{ .source_start = source_start, .target_start = target_start, .len = len };
}

const Cursor = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn take(self: *Cursor, len: usize) ![]const u8 {
        if (len > self.bytes.len -| self.pos) return error.TruncatedJbig2Stream;
        defer self.pos += len;
        return self.bytes[self.pos .. self.pos + len];
    }
    fn byte(self: *Cursor) !u8 {
        return (try self.take(1))[0];
    }
    fn u16be(self: *Cursor) !u16 {
        const bytes = try self.take(2);
        return std.mem.readInt(u16, bytes[0..2], .big);
    }
    fn u32be(self: *Cursor) !u32 {
        const bytes = try self.take(4);
        return std.mem.readInt(u32, bytes[0..4], .big);
    }
    fn i32be(self: *Cursor) !i32 {
        return @bitCast(try self.u32be());
    }
};

const Qe = struct { qe: u16, nmps: u8, nlps: u8, switch_mps: bool };
const qe_table = [_]Qe{
    .{ .qe = 0x5601, .nmps = 1, .nlps = 1, .switch_mps = true },    .{ .qe = 0x3401, .nmps = 2, .nlps = 6, .switch_mps = false },
    .{ .qe = 0x1801, .nmps = 3, .nlps = 9, .switch_mps = false },   .{ .qe = 0x0ac1, .nmps = 4, .nlps = 12, .switch_mps = false },
    .{ .qe = 0x0521, .nmps = 5, .nlps = 29, .switch_mps = false },  .{ .qe = 0x0221, .nmps = 38, .nlps = 33, .switch_mps = false },
    .{ .qe = 0x5601, .nmps = 7, .nlps = 6, .switch_mps = true },    .{ .qe = 0x5401, .nmps = 8, .nlps = 14, .switch_mps = false },
    .{ .qe = 0x4801, .nmps = 9, .nlps = 14, .switch_mps = false },  .{ .qe = 0x3801, .nmps = 10, .nlps = 14, .switch_mps = false },
    .{ .qe = 0x3001, .nmps = 11, .nlps = 17, .switch_mps = false }, .{ .qe = 0x2401, .nmps = 12, .nlps = 18, .switch_mps = false },
    .{ .qe = 0x1c01, .nmps = 13, .nlps = 20, .switch_mps = false }, .{ .qe = 0x1601, .nmps = 29, .nlps = 21, .switch_mps = false },
    .{ .qe = 0x5601, .nmps = 15, .nlps = 14, .switch_mps = true },  .{ .qe = 0x5401, .nmps = 16, .nlps = 14, .switch_mps = false },
    .{ .qe = 0x5101, .nmps = 17, .nlps = 15, .switch_mps = false }, .{ .qe = 0x4801, .nmps = 18, .nlps = 16, .switch_mps = false },
    .{ .qe = 0x3801, .nmps = 19, .nlps = 17, .switch_mps = false }, .{ .qe = 0x3401, .nmps = 20, .nlps = 18, .switch_mps = false },
    .{ .qe = 0x3001, .nmps = 21, .nlps = 19, .switch_mps = false }, .{ .qe = 0x2801, .nmps = 22, .nlps = 19, .switch_mps = false },
    .{ .qe = 0x2401, .nmps = 23, .nlps = 20, .switch_mps = false }, .{ .qe = 0x2201, .nmps = 24, .nlps = 21, .switch_mps = false },
    .{ .qe = 0x1c01, .nmps = 25, .nlps = 22, .switch_mps = false }, .{ .qe = 0x1801, .nmps = 26, .nlps = 23, .switch_mps = false },
    .{ .qe = 0x1601, .nmps = 27, .nlps = 24, .switch_mps = false }, .{ .qe = 0x1401, .nmps = 28, .nlps = 25, .switch_mps = false },
    .{ .qe = 0x1201, .nmps = 29, .nlps = 26, .switch_mps = false }, .{ .qe = 0x1101, .nmps = 30, .nlps = 27, .switch_mps = false },
    .{ .qe = 0x0ac1, .nmps = 31, .nlps = 28, .switch_mps = false }, .{ .qe = 0x09c1, .nmps = 32, .nlps = 29, .switch_mps = false },
    .{ .qe = 0x08a1, .nmps = 33, .nlps = 30, .switch_mps = false }, .{ .qe = 0x0521, .nmps = 34, .nlps = 31, .switch_mps = false },
    .{ .qe = 0x0441, .nmps = 35, .nlps = 32, .switch_mps = false }, .{ .qe = 0x02a1, .nmps = 36, .nlps = 33, .switch_mps = false },
    .{ .qe = 0x0221, .nmps = 37, .nlps = 34, .switch_mps = false }, .{ .qe = 0x0141, .nmps = 38, .nlps = 35, .switch_mps = false },
    .{ .qe = 0x0111, .nmps = 39, .nlps = 36, .switch_mps = false }, .{ .qe = 0x0085, .nmps = 40, .nlps = 37, .switch_mps = false },
    .{ .qe = 0x0049, .nmps = 41, .nlps = 38, .switch_mps = false }, .{ .qe = 0x0025, .nmps = 42, .nlps = 39, .switch_mps = false },
    .{ .qe = 0x0015, .nmps = 43, .nlps = 40, .switch_mps = false }, .{ .qe = 0x0009, .nmps = 44, .nlps = 41, .switch_mps = false },
    .{ .qe = 0x0005, .nmps = 45, .nlps = 42, .switch_mps = false }, .{ .qe = 0x0001, .nmps = 45, .nlps = 43, .switch_mps = false },
    .{ .qe = 0x5601, .nmps = 46, .nlps = 46, .switch_mps = false },
};

const ArithmeticDecoder = struct {
    bytes: []const u8,
    pos: usize = 0,
    start: usize = 0,
    a: u32 = 0x8000,
    c: u64 = 0,
    ct: u8 = 0,

    fn init(bytes: []const u8) !ArithmeticDecoder {
        if (bytes.len < 2) return error.TruncatedJbig2Stream;
        var self = ArithmeticDecoder{ .bytes = bytes };
        const b = try self.read();
        self.c = @as(u64, b) << 16;
        try self.byteIn();
        self.c = (self.c << 7) & 0xffffffff;
        self.ct -= 7;
        return self;
    }

    fn read(self: *ArithmeticDecoder) !u8 {
        if (self.pos >= self.bytes.len) return error.TruncatedJbig2Stream;
        defer self.pos += 1;
        return self.bytes[self.pos];
    }

    fn byteIn(self: *ArithmeticDecoder) !void {
        if (self.pos > self.start) self.pos -= 1;
        const b = try self.read();
        if (b == 0xff) {
            const b1 = try self.read();
            if (b1 > 0x8f) {
                self.c += 0xff00;
                self.ct = 8;
                self.pos -= 2;
            } else {
                self.c += @as(u64, b1) << 9;
                self.ct = 7;
            }
        } else {
            const b1 = try self.read();
            self.c += @as(u64, b1) << 8;
            self.ct = 8;
        }
        self.c &= 0xffffffff;
    }

    fn decode(self: *ArithmeticDecoder, contexts: []u8, index: usize) !u1 {
        if (index >= contexts.len) return error.InvalidJbig2Context;
        const entry = contexts[index];
        const state: usize = entry & 0x7f;
        const mps: u1 = @truncate(entry >> 7);
        const q = qe_table[state];
        self.a -= q.qe;
        var value: u1 = undefined;
        if ((self.c >> 16) < q.qe) {
            if (self.a < q.qe) {
                contexts[index] = (@as(u8, mps) << 7) | q.nmps;
                self.a = q.qe;
                value = mps;
            } else {
                const next_mps: u1 = if (q.switch_mps) 1 - mps else mps;
                contexts[index] = (@as(u8, next_mps) << 7) | q.nlps;
                self.a = q.qe;
                value = 1 - mps;
            }
            try self.renormalize();
        } else {
            self.c -= @as(u64, q.qe) << 16;
            if ((self.a & 0x8000) != 0) return mps;
            if (self.a < q.qe) {
                const next_mps: u1 = if (q.switch_mps) 1 - mps else mps;
                contexts[index] = (@as(u8, next_mps) << 7) | q.nlps;
                value = 1 - mps;
            } else {
                contexts[index] = (@as(u8, mps) << 7) | q.nmps;
                value = mps;
            }
            try self.renormalize();
        }
        return value;
    }

    fn renormalize(self: *ArithmeticDecoder) !void {
        while ((self.a & 0x8000) == 0) {
            if (self.ct == 0) try self.byteIn();
            self.a <<= 1;
            self.c = (self.c << 1) & 0xffffffff;
            self.ct -= 1;
        }
    }
};

fn decodeInteger(arith: *ArithmeticDecoder, contexts: []u8) !?i64 {
    var prev: usize = 1;
    const sign = try arith.decode(contexts, prev & 0x1ff);
    prev = updatePrev(prev, sign);
    var d = try arith.decode(contexts, prev & 0x1ff);
    prev = updatePrev(prev, d);
    var bits: u6 = 2;
    var offset: u64 = 0;
    if (d == 1) {
        d = try arith.decode(contexts, prev & 0x1ff);
        prev = updatePrev(prev, d);
        if (d == 1) {
            d = try arith.decode(contexts, prev & 0x1ff);
            prev = updatePrev(prev, d);
            if (d == 1) {
                d = try arith.decode(contexts, prev & 0x1ff);
                prev = updatePrev(prev, d);
                if (d == 1) {
                    d = try arith.decode(contexts, prev & 0x1ff);
                    prev = updatePrev(prev, d);
                    if (d == 1) {
                        bits = 32;
                        offset = 4436;
                    } else {
                        bits = 12;
                        offset = 340;
                    }
                } else {
                    bits = 8;
                    offset = 84;
                }
            } else {
                bits = 6;
                offset = 20;
            }
        } else {
            bits = 4;
            offset = 4;
        }
    }
    var value: u64 = 0;
    var i: u6 = 0;
    while (i < bits) : (i += 1) {
        d = try arith.decode(contexts, prev & 0x1ff);
        prev = updatePrev(prev, d);
        value = (value << 1) | d;
    }
    value += offset;
    if (value > std.math.maxInt(i32)) return error.InvalidJbig2Integer;
    if (sign == 0) return @intCast(value);
    if (value == 0) return null;
    return -@as(i64, @intCast(value));
}

fn updatePrev(prev: usize, bit: u1) usize {
    if (prev < 256) return ((prev << 1) | bit) & 0x1ff;
    return (((prev << 1) | bit) & 0x1ff) | 0x100;
}

fn decodeIaid(arith: *ArithmeticDecoder, contexts: []u8, code_len: u6) !u32 {
    var prev: usize = 1;
    const mask = (@as(usize, 1) << code_len) - 1;
    var i: u6 = 0;
    while (i < code_len) : (i += 1) prev = (prev << 1) | try arith.decode(contexts, prev & mask);
    return @intCast(prev - (@as(usize, 1) << code_len));
}

fn genericContextCount(template: u2) usize {
    return switch (template) {
        0 => 1 << 16,
        1 => 1 << 13,
        2, 3 => 1 << 10,
    };
}

fn genericAdaptivePointCount(template: u2) usize {
    return if (template == 0) 4 else 1;
}

fn validateGenericAdaptivePoints(at: [4][2]i8, count: usize) !void {
    for (at[0..count]) |point| {
        // Adaptive pixels must be inside the 128-row causal field defined by
        // T.88: earlier rows, or an already-decoded pixel on the current row.
        if (point[1] > 0 or (point[1] == 0 and point[0] >= 0))
            return error.InvalidJbig2AdaptiveTemplate;
    }
}

fn decodeGenericBitmap(alloc: Allocator, work: *DecodeWorkBudget, arith: *ArithmeticDecoder, contexts: []u8, width: u32, height: u32, template: u2, at: [4][2]i8, max_bytes: usize) !Bitmap {
    if (contexts.len < genericContextCount(template)) return error.InvalidJbig2Context;
    try validateGenericAdaptivePoints(at, genericAdaptivePointCount(template));
    try work.chargePixels(width, height);
    var bitmap = try Bitmap.blank(alloc, work, width, height, max_bytes);
    errdefer bitmap.deinit(alloc);
    var y: u32 = 0;
    while (y < height) : (y += 1) {
        var x: u32 = 0;
        while (x < width) : (x += 1) {
            const ix: i64 = x;
            const iy: i64 = y;
            const cx: usize = switch (template) {
                0 => blk: {
                    var value: usize = 0;
                    value |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
                    value |= @as(usize, bitmap.get(ix - 2, iy)) << 1;
                    value |= @as(usize, bitmap.get(ix - 3, iy)) << 2;
                    value |= @as(usize, bitmap.get(ix - 4, iy)) << 3;
                    value |= @as(usize, bitmap.get(ix + at[0][0], iy + at[0][1])) << 4;
                    value |= @as(usize, bitmap.get(ix - 2, iy - 1)) << 5;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 6;
                    value |= @as(usize, bitmap.get(ix, iy - 1)) << 7;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 8;
                    value |= @as(usize, bitmap.get(ix + 2, iy - 1)) << 9;
                    value |= @as(usize, bitmap.get(ix + at[1][0], iy + at[1][1])) << 10;
                    value |= @as(usize, bitmap.get(ix + at[2][0], iy + at[2][1])) << 11;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 2)) << 12;
                    value |= @as(usize, bitmap.get(ix, iy - 2)) << 13;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 2)) << 14;
                    value |= @as(usize, bitmap.get(ix + at[3][0], iy + at[3][1])) << 15;
                    break :blk value;
                },
                1 => blk: {
                    var value: usize = 0;
                    value |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
                    value |= @as(usize, bitmap.get(ix - 2, iy)) << 1;
                    value |= @as(usize, bitmap.get(ix - 3, iy)) << 2;
                    value |= @as(usize, bitmap.get(ix + at[0][0], iy + at[0][1])) << 3;
                    value |= @as(usize, bitmap.get(ix - 2, iy - 1)) << 4;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 5;
                    value |= @as(usize, bitmap.get(ix, iy - 1)) << 6;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 7;
                    value |= @as(usize, bitmap.get(ix + 2, iy - 1)) << 8;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 2)) << 9;
                    value |= @as(usize, bitmap.get(ix, iy - 2)) << 10;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 2)) << 11;
                    value |= @as(usize, bitmap.get(ix + 2, iy - 2)) << 12;
                    break :blk value;
                },
                2 => blk: {
                    var value: usize = 0;
                    value |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
                    value |= @as(usize, bitmap.get(ix - 2, iy)) << 1;
                    value |= @as(usize, bitmap.get(ix + at[0][0], iy + at[0][1])) << 2;
                    value |= @as(usize, bitmap.get(ix - 2, iy - 1)) << 3;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 4;
                    value |= @as(usize, bitmap.get(ix, iy - 1)) << 5;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 6;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 2)) << 7;
                    value |= @as(usize, bitmap.get(ix, iy - 2)) << 8;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 2)) << 9;
                    break :blk value;
                },
                3 => blk: {
                    var value: usize = 0;
                    value |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
                    value |= @as(usize, bitmap.get(ix - 2, iy)) << 1;
                    value |= @as(usize, bitmap.get(ix - 3, iy)) << 2;
                    value |= @as(usize, bitmap.get(ix - 4, iy)) << 3;
                    value |= @as(usize, bitmap.get(ix + at[0][0], iy + at[0][1])) << 4;
                    value |= @as(usize, bitmap.get(ix - 3, iy - 1)) << 5;
                    value |= @as(usize, bitmap.get(ix - 2, iy - 1)) << 6;
                    value |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 7;
                    value |= @as(usize, bitmap.get(ix, iy - 1)) << 8;
                    value |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 9;
                    break :blk value;
                },
            };
            bitmap.set(x, y, try arith.decode(contexts, cx));
        }
    }
    return bitmap;
}

fn decodeRefinement0(alloc: Allocator, work: *DecodeWorkBudget, arith: *ArithmeticDecoder, contexts: []u8, width: u32, height: u32, reference: Bitmap, dx: i64, dy: i64, at: [2][2]i8, max_bytes: usize) !Bitmap {
    const defaults = [2][2]i8{ .{ -1, -1 }, .{ -1, -1 } };
    if (!std.mem.eql([2]i8, &at, &defaults)) return error.UnsupportedJbig2AdaptiveTemplate;
    try work.chargePixels(width, height);
    var bitmap = try Bitmap.blank(alloc, work, width, height, max_bytes);
    errdefer bitmap.deinit(alloc);
    var y: u32 = 0;
    while (y < height) : (y += 1) {
        var x: u32 = 0;
        while (x < width) : (x += 1) {
            const ix: i64 = x;
            const iy: i64 = y;
            var cx: usize = 0;
            cx |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
            cx |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 1;
            cx |= @as(usize, bitmap.get(ix, iy - 1)) << 2;
            cx |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 3;
            cx |= @as(usize, reference.get(ix + 1 - dx, iy + 1 - dy)) << 4;
            cx |= @as(usize, reference.get(ix - dx, iy + 1 - dy)) << 5;
            cx |= @as(usize, reference.get(ix - 1 - dx, iy + 1 - dy)) << 6;
            cx |= @as(usize, reference.get(ix + 1 - dx, iy - dy)) << 7;
            cx |= @as(usize, reference.get(ix - dx, iy - dy)) << 8;
            cx |= @as(usize, reference.get(ix - 1 - dx, iy - dy)) << 9;
            cx |= @as(usize, reference.get(ix + 1 - dx, iy - 1 - dy)) << 10;
            cx |= @as(usize, reference.get(ix - dx, iy - 1 - dy)) << 11;
            cx |= @as(usize, reference.get(ix - 1 - dx, iy - 1 - dy)) << 12;
            bitmap.set(x, y, try arith.decode(contexts, cx));
        }
    }
    return bitmap;
}

const IntContexts = struct {
    dt: []u8,
    fs: []u8,
    ds: []u8,
    it: []u8,
    ri: []u8,
    rdw: []u8,
    rdh: []u8,
    rdx: []u8,
    rdy: []u8,

    fn init(alloc: Allocator, work: *DecodeWorkBudget) !IntContexts {
        var all: [9][]u8 = undefined;
        var count: usize = 0;
        errdefer for (all[0..count]) |item| alloc.free(item);
        for (&all) |*item| {
            item.* = try allocZeroedBytes(alloc, work, 512);
            count += 1;
        }
        return .{ .dt = all[0], .fs = all[1], .ds = all[2], .it = all[3], .ri = all[4], .rdw = all[5], .rdh = all[6], .rdx = all[7], .rdy = all[8] };
    }
    fn deinit(self: *IntContexts, alloc: Allocator) void {
        inline for (.{ self.dt, self.fs, self.ds, self.it, self.ri, self.rdw, self.rdh, self.rdx, self.rdy }) |item| alloc.free(item);
        self.* = undefined;
    }
};

const TextParams = struct {
    width: u32,
    height: u32,
    instances: u32,
    strips_log: u2,
    reference_corner: u2,
    transposed: bool,
    op: u2,
    default_pixel: u1,
    ds_offset: i6,
    refine: bool,
    refine_at: [2][2]i8,
    symbol_code_len: ?u6 = null,
};

fn ceilLog2(value: usize) !u6 {
    if (value == 0) return error.InvalidJbig2SymbolCount;
    return @intCast(std.math.log2_int_ceil(usize, value));
}

fn decodeTextRegion(alloc: Allocator, work: *DecodeWorkBudget, arith: *ArithmeticDecoder, symbols: []const Bitmap, params: TextParams, shared_generic_contexts: ?[]u8, shared_iaid: ?[]u8, shared_int_contexts: ?*IntContexts, max_bytes: usize) !Bitmap {
    // Symbols may legally overlap, so instance count is not bounded by region
    // pixels. Reserve the full control-loop cost up front instead; this accepts
    // dense valid regions while rejecting impossible workloads before decode.
    try work.chargeItems(params.instances, 32);
    const code_len = params.symbol_code_len orelse try ceilLog2(symbols.len);
    var owned_int_ctx: ?IntContexts = null;
    if (shared_int_contexts == null) owned_int_ctx = try IntContexts.init(alloc, work);
    defer if (owned_int_ctx) |*value| value.deinit(alloc);
    const int_ctx = shared_int_contexts orelse &owned_int_ctx.?;
    const iaid = if (shared_iaid) |value| value else blk: {
        const value = try allocZeroedBytes(alloc, work, @as(usize, 1) << code_len);
        break :blk value;
    };
    defer if (shared_iaid == null) alloc.free(iaid);
    const generic_ctx = if (shared_generic_contexts) |value| value else blk: {
        const value = try allocZeroedBytes(alloc, work, 65536);
        break :blk value;
    };
    defer if (shared_generic_contexts == null) alloc.free(generic_ctx);

    var region = try Bitmap.blank(alloc, work, params.width, params.height, max_bytes);
    errdefer region.deinit(alloc);
    if (params.default_pixel == 1) {
        try work.chargeBytes(region.data.len);
        @memset(region.data, 0xff);
    }
    const strips: i64 = @as(i64, 1) << params.strips_log;
    const initial_t = try decodeInteger(arith, int_ctx.dt) orelse return error.InvalidJbig2Integer;
    var strip_t = -initial_t * strips;
    var first_s: i64 = 0;
    var count: u32 = 0;
    while (count < params.instances) {
        const count_before_strip = count;
        const dt = try decodeInteger(arith, int_ctx.dt) orelse return error.InvalidJbig2Integer;
        strip_t += dt * strips;
        var first = true;
        var current_s: i64 = 0;
        while (true) {
            if (first) {
                const dfs = try decodeInteger(arith, int_ctx.fs) orelse return error.InvalidJbig2Integer;
                first_s += dfs;
                current_s = first_s;
                first = false;
            } else {
                const ids = try decodeInteger(arith, int_ctx.ds) orelse break;
                if (count >= params.instances) return error.InvalidJbig2SymbolCount;
                current_s += ids + params.ds_offset;
            }
            const current_t = if (strips == 1) 0 else (try decodeInteger(arith, int_ctx.it) orelse return error.InvalidJbig2Integer);
            var t = strip_t + current_t;
            const id = try decodeIaid(arith, iaid, code_len);
            if (id >= symbols.len) return error.InvalidJbig2SymbolId;
            const ri = if (params.refine) (try decodeInteger(arith, int_ctx.ri) orelse return error.InvalidJbig2Integer) else 0;
            var refined: ?Bitmap = null;
            defer if (refined) |*value| value.deinit(alloc);
            const symbol = if (ri == 0) symbols[id] else blk: {
                const rdw = try decodeInteger(arith, int_ctx.rdw) orelse return error.InvalidJbig2Integer;
                const rdh = try decodeInteger(arith, int_ctx.rdh) orelse return error.InvalidJbig2Integer;
                const rdx = try decodeInteger(arith, int_ctx.rdx) orelse return error.InvalidJbig2Integer;
                const rdy = try decodeInteger(arith, int_ctx.rdy) orelse return error.InvalidJbig2Integer;
                const new_width = @as(i64, symbols[id].width) + rdw;
                const new_height = @as(i64, symbols[id].height) + rdh;
                if (new_width <= 0 or new_height <= 0 or new_width > std.math.maxInt(u32) or new_height > std.math.maxInt(u32)) return error.InvalidJbig2Dimensions;
                refined = try decodeRefinement0(alloc, work, arith, generic_ctx, @intCast(new_width), @intCast(new_height), symbols[id], @divFloor(rdw, 2) + rdx, @divFloor(rdh, 2) + rdy, params.refine_at, max_bytes);
                break :blk refined.?;
            };
            if (!params.transposed and (params.reference_corner == 2 or params.reference_corner == 3)) current_s += symbol.width - 1;
            if (params.transposed and (params.reference_corner == 0 or params.reference_corner == 2)) current_s += symbol.height - 1;
            var s = current_s;
            if (params.transposed) std.mem.swap(i64, &s, &t);
            switch (params.reference_corner) {
                0 => t -= symbol.height - 1,
                2 => {
                    t -= symbol.height - 1;
                    s -= symbol.width - 1;
                },
                3 => s -= symbol.width - 1,
                else => {},
            }
            try region.compose(symbol, s, t, params.op, work);
            if (!params.transposed and (params.reference_corner == 0 or params.reference_corner == 1)) current_s += symbol.width - 1;
            if (params.transposed and (params.reference_corner == 1 or params.reference_corner == 3)) current_s += symbol.height - 1;
            count += 1;
        }
        if (count == count_before_strip) return error.InvalidJbig2SymbolCount;
    }
    return region;
}

const Dictionary = struct {
    symbols: []Bitmap,
    fn deinit(self: *Dictionary, alloc: Allocator) void {
        for (self.symbols) |*symbol| symbol.deinit(alloc);
        alloc.free(self.symbols);
        self.* = undefined;
    }
};

const StoredSegment = struct { number: u32, dictionary: ?Dictionary = null };

fn validateExtension(payload: []const u8) !void {
    if (payload.len < @sizeOf(u32)) return error.TruncatedJbig2Extension;
    const extension_type = std.mem.readInt(u32, payload[0..4], .big);
    // T.88 defines these as non-necessary comment extensions. Their contents
    // do not affect the decoded page bitmap, so the image decoder may ignore
    // them without allocating or parsing their descriptive payload.
    if (extension_type == 0x20000000 or extension_type == 0x20000002) return;
    // Bit 31 marks an extension as necessary to reproduce the intended page.
    // Unknown non-necessary metadata remains safely ignorable, but returning a
    // partial bitmap for an unknown necessary extension would violate the
    // decoder's fail-closed contract.
    if ((extension_type & 0x80000000) != 0)
        return error.UnsupportedJbig2NecessaryExtension;
}

const Decoder = struct {
    alloc: Allocator,
    work: *DecodeWorkBudget,
    max_bytes: usize,
    segments: std.ArrayList(StoredSegment) = .empty,
    page: ?Bitmap = null,
    page_default: u1 = 0,
    page_op: u2 = 0,
    page_allows_op_override: bool = false,
    expected_dimensions: ?ExpectedDimensions = null,

    fn deinit(self: *Decoder) void {
        for (self.segments.items) |*segment| if (segment.dictionary) |*dict| dict.deinit(self.alloc);
        self.segments.deinit(self.alloc);
        if (self.page) |*page| page.deinit(self.alloc);
    }

    fn dictionaryFor(self: *Decoder, number: u32) !?*const Dictionary {
        // References normally point to a recent segment. Search backwards for
        // that fast path and charge adversarial long scans to the CPU budget.
        var index = self.segments.items.len;
        while (index > 0) {
            index -= 1;
            try self.work.charge(1);
            const segment = &self.segments.items[index];
            if (segment.number == number and segment.dictionary != null) return &segment.dictionary.?;
        }
        return null;
    }

    fn decodeStream(self: *Decoder, bytes: []const u8) !void {
        var cursor = Cursor{ .bytes = bytes };
        while (cursor.pos < bytes.len) {
            try self.work.charge(64);
            if (self.segments.items.len >= 100_000) return error.TooManyJbig2Segments;
            const number = try cursor.u32be();
            const flags = try cursor.byte();
            const segment_type = flags & 0x3f;
            const ref_flags = try cursor.byte();
            const ref_count = ref_flags >> 5;
            if (ref_count == 7) return error.UnsupportedJbig2LongReferences;
            var refs: [4]u32 = undefined;
            if (ref_count > refs.len) return error.InvalidJbig2Segment;
            var i: usize = 0;
            while (i < ref_count) : (i += 1) refs[i] = if (number <= 256) try cursor.byte() else if (number <= 65536) try cursor.u16be() else try cursor.u32be();
            _ = if ((flags & 0x40) != 0) try cursor.u32be() else try cursor.byte();
            const length = try cursor.u32be();
            var unknown_generic_rows: ?u32 = null;
            const payload = if (length == 0xffffffff) blk: {
                // T.88 7.2.7 terminates an unknown-length arithmetic-coded
                // immediate generic region with FF AC followed by the decoded
                // row count. Locate that bounded framing marker instead of
                // consuming subsequent segment headers as image data.
                if (segment_type != 38 and segment_type != 39)
                    return error.UnsupportedJbig2UnknownLength;
                const framed = try self.takeUnknownLengthGeneric(&cursor);
                unknown_generic_rows = framed.rows;
                break :blk framed.payload;
            } else try cursor.take(length);
            // Reserve list capacity before a segment decoder creates owned
            // data, so an append OOM cannot orphan a decoded dictionary.
            try self.segments.ensureUnusedCapacity(self.alloc, 1);
            var stored = StoredSegment{ .number = number };
            switch (segment_type) {
                0 => stored.dictionary = try self.decodeDictionary(payload, refs[0..ref_count]),
                6, 7 => try self.decodeText(payload, refs[0..ref_count]),
                38, 39 => try self.decodeGeneric(payload, unknown_generic_rows),
                48 => try self.decodePageInformation(payload),
                49, 50, 51, 52 => {},
                62 => try validateExtension(payload),
                else => return error.UnsupportedJbig2Segment,
            }
            self.segments.appendAssumeCapacity(stored);
        }
    }

    const UnknownLengthGeneric = struct {
        payload: []const u8,
        rows: u32,
    };

    fn takeUnknownLengthGeneric(self: *Decoder, cursor: *Cursor) !UnknownLengthGeneric {
        const payload_start = cursor.pos;
        var header = Cursor{ .bytes = cursor.bytes[payload_start..] };
        _ = try header.u32be();
        _ = try header.u32be();
        _ = try header.u32be();
        _ = try header.u32be();
        _ = try header.byte();
        const flags = try header.byte();
        if ((flags & 1) != 0) return error.UnsupportedJbig2GenericProfile;
        const template: u2 = @truncate((flags >> 1) & 3);
        _ = try header.take(2 * genericAdaptivePointCount(template));

        var marker = header.pos;
        while (marker + 6 <= header.bytes.len) : (marker += 1) {
            try self.work.charge(1);
            if (header.bytes[marker] != 0xff or header.bytes[marker + 1] != 0xac) continue;
            const rows = std.mem.readInt(u32, header.bytes[marker + 2 ..][0..4], .big);
            if (rows == 0) return error.InvalidJbig2UnknownRowCount;
            const payload_len = marker + 6;
            return .{
                .payload = try cursor.take(payload_len),
                .rows = rows,
            };
        }
        return error.TruncatedJbig2UnknownLength;
    }

    fn decodePageInformation(self: *Decoder, payload: []const u8) !void {
        var cursor = Cursor{ .bytes = payload };
        const width = try cursor.u32be();
        const height = try cursor.u32be();
        _ = try cursor.u32be();
        _ = try cursor.u32be();
        const flags = try cursor.byte();
        _ = try cursor.u16be();
        if (self.page != null) return error.InvalidJbig2Segment;
        if (self.expected_dimensions) |expected| {
            const width_delta = @max(width, expected.width) - @min(width, expected.width);
            const height_delta = @max(height, expected.height) - @min(height, expected.height);
            // Some producer pipelines round the XObject and JBIG2 page width
            // on opposite sides of a pixel boundary. Bound compatibility to
            // that known one-pixel discrepancy; larger mismatches still fail.
            if (width_delta > 1 or height_delta > 1) return error.Jbig2DimensionMismatch;
        }
        self.page_default = @truncate((flags >> 2) & 1);
        self.page_op = @truncate((flags >> 3) & 3);
        self.page_allows_op_override = (flags & 0x40) != 0;
        var page = try Bitmap.blank(self.alloc, self.work, width, height, self.max_bytes);
        errdefer page.deinit(self.alloc);
        if (self.page_default == 1) {
            try self.work.chargeBytes(page.data.len);
            @memset(page.data, 0xff);
        }
        self.page = page;
    }

    fn decodeDictionary(self: *Decoder, payload: []const u8, refs: []const u32) !Dictionary {
        var cursor = Cursor{ .bytes = payload };
        const flags = try cursor.u16be();
        const huffman = (flags & 1) != 0;
        const refine = (flags & 2) != 0;
        const template = (flags >> 10) & 3;
        const refine_template = (flags >> 12) & 1;
        if (huffman or (refine and refine_template != 0) or (flags & 0x0300) != 0) return error.UnsupportedJbig2DictionaryProfile;
        var at: [4][2]i8 = @splat(.{ 0, 0 });
        for (at[0..genericAdaptivePointCount(@intCast(template))]) |*point| {
            point[0] = @bitCast(try cursor.byte());
            point[1] = @bitCast(try cursor.byte());
        }
        var refine_at = [2][2]i8{ .{ -1, -1 }, .{ -1, -1 } };
        if (refine) for (&refine_at) |*point| {
            point[0] = @bitCast(try cursor.byte());
            point[1] = @bitCast(try cursor.byte());
        };
        const export_count = try cursor.u32be();
        const new_count = try cursor.u32be();
        if (new_count > 100_000 or export_count > 100_000) return error.InvalidJbig2SymbolCount;

        var symbols = std.ArrayList(Bitmap).empty;
        defer {
            for (symbols.items) |*item| item.deinit(self.alloc);
            symbols.deinit(self.alloc);
        }
        var symbol_bytes: usize = 0;
        for (refs) |ref| {
            const dict = (try self.dictionaryFor(ref)) orelse return error.MissingJbig2SymbolDictionary;
            for (dict.symbols) |symbol| {
                try self.work.charge(16);
                try symbols.ensureUnusedCapacity(self.alloc, 1);
                var copy = try symbol.clone(self.alloc, self.work, self.max_bytes);
                if (copy.data.len > self.max_bytes -| symbol_bytes) {
                    copy.deinit(self.alloc);
                    return error.Jbig2ImageTooLarge;
                }
                symbol_bytes += copy.data.len;
                symbols.appendAssumeCapacity(copy);
            }
        }
        const imported_count = symbols.items.len;
        const total_count = std.math.add(usize, imported_count, new_count) catch return error.InvalidJbig2SymbolCount;
        // Empty dictionaries are valid no-ops and appear in production scans.
        // They have no symbol identifiers or export runs to arithmetic-decode,
        // so avoid both ceilLog2(0) and the otherwise unnecessary 65 KiB
        // arithmetic context. A nonzero export count is still inconsistent.
        if (total_count == 0) {
            if (export_count != 0) return error.InvalidJbig2SymbolCount;
            return .{ .symbols = try self.alloc.alloc(Bitmap, 0) };
        }
        const code_len = if (refine) try ceilLog2(total_count) else 0;
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        const generic_context_count = if (refine) @as(usize, 1 << 13) else genericContextCount(@intCast(template));
        const generic_ctx = try allocZeroedBytes(self.alloc, self.work, generic_context_count);
        defer self.alloc.free(generic_ctx);
        const dh = try allocZeroedBytes(self.alloc, self.work, 512);
        defer self.alloc.free(dh);
        const dw = try allocZeroedBytes(self.alloc, self.work, 512);
        defer self.alloc.free(dw);
        const iaai = try allocZeroedBytes(self.alloc, self.work, 512);
        defer self.alloc.free(iaai);
        const iaex = try allocZeroedBytes(self.alloc, self.work, 512);
        defer self.alloc.free(iaex);
        var text_int_ctx: ?IntContexts = null;
        if (refine) text_int_ctx = try IntContexts.init(self.alloc, self.work);
        defer if (text_int_ctx) |*value| value.deinit(self.alloc);
        var iaid: []u8 = &.{};
        if (refine) {
            iaid = try allocZeroedBytes(self.alloc, self.work, @as(usize, 1) << code_len);
        }
        defer if (iaid.len > 0) self.alloc.free(iaid);
        var height: i64 = 0;
        var decoded: u32 = 0;
        while (decoded < new_count) {
            const decoded_before_height_class = decoded;
            height += try decodeInteger(&arith, dh) orelse return error.InvalidJbig2Integer;
            if (height <= 0 or height > std.math.maxInt(u32)) return error.InvalidJbig2Dimensions;
            var width: i64 = 0;
            while (true) {
                const delta_width = try decodeInteger(&arith, dw) orelse break;
                if (decoded >= new_count) break;
                try self.work.charge(64);
                width += delta_width;
                if (width <= 0 or width > std.math.maxInt(u32)) return error.InvalidJbig2Dimensions;
                try symbols.ensureUnusedCapacity(self.alloc, 1);
                var symbol: Bitmap = undefined;
                if (!refine) {
                    symbol = try decodeGenericBitmap(self.alloc, self.work, &arith, generic_ctx, @intCast(width), @intCast(height), @intCast(template), at, self.max_bytes);
                } else {
                    const instances = try decodeInteger(&arith, iaai) orelse return error.InvalidJbig2Integer;
                    if (instances == 1) {
                        const id = try decodeIaid(&arith, iaid, code_len);
                        if (id >= symbols.items.len) return error.InvalidJbig2SymbolId;
                        const rdx = try decodeInteger(&arith, text_int_ctx.?.rdx) orelse return error.InvalidJbig2Integer;
                        const rdy = try decodeInteger(&arith, text_int_ctx.?.rdy) orelse return error.InvalidJbig2Integer;
                        symbol = try decodeRefinement0(self.alloc, self.work, &arith, generic_ctx, @intCast(width), @intCast(height), symbols.items[id], rdx, rdy, refine_at, self.max_bytes);
                    } else if (instances > 1 and instances <= std.math.maxInt(u32)) {
                        symbol = try decodeTextRegion(self.alloc, self.work, &arith, symbols.items, .{
                            .width = @intCast(width),
                            .height = @intCast(height),
                            .instances = @intCast(instances),
                            .strips_log = 0,
                            .reference_corner = 1,
                            .transposed = false,
                            .op = 0,
                            .default_pixel = 0,
                            .ds_offset = 0,
                            .refine = true,
                            .refine_at = refine_at,
                            .symbol_code_len = code_len,
                        }, generic_ctx, iaid, &text_int_ctx.?, self.max_bytes);
                    } else return error.InvalidJbig2SymbolCount;
                }
                if (symbol.data.len > self.max_bytes -| symbol_bytes) {
                    symbol.deinit(self.alloc);
                    return error.Jbig2ImageTooLarge;
                }
                symbol_bytes += symbol.data.len;
                symbols.appendAssumeCapacity(symbol);
                decoded += 1;
            }
            // A height class without a symbol cannot contribute to the
            // declared total. Reject it instead of repeatedly consuming the
            // arithmetic marker padding forever.
            if (decoded == decoded_before_height_class) return error.InvalidJbig2SymbolCount;
        }
        const total = symbols.items.len;
        var exported = std.ArrayList(Bitmap).empty;
        errdefer {
            for (exported.items) |*item| item.deinit(self.alloc);
            exported.deinit(self.alloc);
        }
        var index: usize = 0;
        var flag: u1 = 0;
        var run_count: usize = 0;
        var previous_run_was_zero = false;
        while (index < total) {
            try self.work.charge(16);
            if (run_count > std.math.mul(usize, total, 2) catch return error.InvalidJbig2ExportRun) return error.InvalidJbig2ExportRun;
            run_count += 1;
            const run = try decodeInteger(&arith, iaex) orelse return error.InvalidJbig2ExportRun;
            if (run < 0 or run > total - index) return error.InvalidJbig2ExportRun;
            if (run == 0 and previous_run_was_zero) return error.InvalidJbig2ExportRun;
            previous_run_was_zero = run == 0;
            if (flag == 1) for (symbols.items[index .. index + @as(usize, @intCast(run))]) |symbol| {
                try self.work.charge(16);
                try exported.ensureUnusedCapacity(self.alloc, 1);
                var copy = try symbol.clone(self.alloc, self.work, self.max_bytes);
                if (copy.data.len > self.max_bytes -| symbol_bytes) {
                    copy.deinit(self.alloc);
                    return error.Jbig2ImageTooLarge;
                }
                symbol_bytes += copy.data.len;
                exported.appendAssumeCapacity(copy);
            };
            index += @intCast(run);
            flag = 1 - flag;
        }
        if (exported.items.len != export_count or symbols.items.len != total_count) return error.InvalidJbig2SymbolCount;
        try self.work.chargeItems(exported.items.len, 16);
        return .{ .symbols = try exported.toOwnedSlice(self.alloc) };
    }

    fn decodeText(self: *Decoder, payload: []const u8, refs: []const u32) !void {
        if (self.page == null) return error.MissingJbig2PageInformation;
        var cursor = Cursor{ .bytes = payload };
        const width = try cursor.u32be();
        const height = try cursor.u32be();
        const x = try cursor.i32be();
        const y = try cursor.i32be();
        const region_flags = try cursor.byte();
        const flags = try cursor.u16be();
        if ((flags & 1) != 0 or ((flags >> 15) & 1) != 0) return error.UnsupportedJbig2TextProfile;
        const refine = (flags & 2) != 0;
        var refine_at = [2][2]i8{ .{ -1, -1 }, .{ -1, -1 } };
        if (refine) for (&refine_at) |*point| {
            point[0] = @bitCast(try cursor.byte());
            point[1] = @bitCast(try cursor.byte());
        };
        const instances = try cursor.u32be();
        var symbols = std.ArrayList(Bitmap).empty;
        defer symbols.deinit(self.alloc);
        for (refs) |ref| {
            const dict = (try self.dictionaryFor(ref)) orelse return error.MissingJbig2SymbolDictionary;
            try self.work.chargeItems(dict.symbols.len, 16);
            try symbols.appendSlice(self.alloc, dict.symbols);
        }
        if (symbols.items.len == 0) return error.MissingJbig2SymbolDictionary;
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        var region = try decodeTextRegion(self.alloc, self.work, &arith, symbols.items, .{
            .width = width,
            .height = height,
            .instances = instances,
            .strips_log = @truncate((flags >> 2) & 3),
            .reference_corner = @truncate((flags >> 4) & 3),
            .transposed = ((flags >> 6) & 1) != 0,
            .op = @truncate((flags >> 7) & 3),
            .default_pixel = @truncate((flags >> 9) & 1),
            .ds_offset = decodeSignedFive(@truncate((flags >> 10) & 0x1f)),
            .refine = refine,
            .refine_at = refine_at,
        }, null, null, null, self.max_bytes);
        defer region.deinit(self.alloc);
        try self.page.?.compose(region, x, y, if (self.page_allows_op_override) @truncate(region_flags & 7) else self.page_op, self.work);
    }

    fn decodeGeneric(self: *Decoder, payload: []const u8, unknown_rows: ?u32) !void {
        if (self.page == null) return error.MissingJbig2PageInformation;
        var cursor = Cursor{ .bytes = payload };
        const width = try cursor.u32be();
        const declared_height = try cursor.u32be();
        const height = if (unknown_rows) |rows| blk: {
            if (declared_height != std.math.maxInt(u32) and declared_height != rows)
                return error.InvalidJbig2UnknownRowCount;
            break :blk rows;
        } else declared_height;
        const x = try cursor.i32be();
        const y = try cursor.i32be();
        const region_flags = try cursor.byte();
        const flags = try cursor.byte();
        const template: u2 = @truncate((flags >> 1) & 3);
        if ((flags & 1) != 0 or ((flags >> 3) & 1) != 0 or ((flags >> 4) & 1) != 0) return error.UnsupportedJbig2GenericProfile;
        var at: [4][2]i8 = @splat(.{ 0, 0 });
        for (at[0..genericAdaptivePointCount(template)]) |*point| {
            point[0] = @bitCast(try cursor.byte());
            point[1] = @bitCast(try cursor.byte());
        }
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        const contexts = try allocZeroedBytes(self.alloc, self.work, genericContextCount(template));
        defer self.alloc.free(contexts);
        var region = try decodeGenericBitmap(self.alloc, self.work, &arith, contexts, width, height, template, at, self.max_bytes);
        defer region.deinit(self.alloc);
        try self.page.?.compose(region, x, y, if (self.page_allows_op_override) @truncate(region_flags & 7) else self.page_op, self.work);
    }
};

fn decodeSignedFive(value: u5) i6 {
    return if (value <= 15) @intCast(value) else @intCast(@as(i8, @intCast(value)) - 32);
}

pub fn decodeAlloc(
    alloc: Allocator,
    globals: ?[]const u8,
    bytes: []const u8,
    max_output_bytes: usize,
    max_working_set_bytes: usize,
    max_work_units: u64,
    expected_dimensions: ?ExpectedDimensions,
) !Decoded {
    if (max_output_bytes == 0) return error.Jbig2ImageTooLarge;
    if (max_working_set_bytes == 0) return error.Jbig2WorkingSetTooLarge;
    if (max_work_units == 0) return error.Jbig2WorkLimitExceeded;
    const input_bytes = std.math.add(usize, bytes.len, if (globals) |value| value.len else 0) catch return error.Jbig2WorkingSetTooLarge;
    if (input_bytes >= max_working_set_bytes) return error.Jbig2WorkingSetTooLarge;
    var budget = WorkingSetAllocator{ .backing = alloc, .live_bytes = input_bytes, .max_live_bytes = max_working_set_bytes };
    var work = DecodeWorkBudget{ .remaining = max_work_units };
    var decoder = Decoder{ .alloc = budget.allocator(), .work = &work, .max_bytes = max_output_bytes, .expected_dimensions = expected_dimensions };
    defer decoder.deinit();
    if (globals) |global_bytes| decoder.decodeStream(global_bytes) catch |err| {
        if (err == error.OutOfMemory and budget.limit_exceeded) return error.Jbig2WorkingSetTooLarge;
        return err;
    };
    decoder.decodeStream(bytes) catch |err| {
        if (err == error.OutOfMemory and budget.limit_exceeded) return error.Jbig2WorkingSetTooLarge;
        return err;
    };
    const page = decoder.page orelse return error.MissingJbig2PageInformation;
    decoder.page = null;
    // WorkingSetAllocator is a transparent wrapper over `alloc`; detach the
    // page from its accounting so callers can release it with `alloc`.
    budget.disown(page.data.len);
    return .{ .width = page.width, .height = page.height, .pixels = page.data };
}

test "arithmetic decoder matches Annex E context transitions" {
    // The first bytes of the standard arithmetic test sequence exercise both
    // MPS and LPS exchange paths without relying on a PDF container.
    const encoded = [_]u8{ 0x84, 0xc7, 0x3b, 0xfc, 0xe1, 0xa1, 0x43, 0x04, 0x02, 0x20, 0x00, 0x00 };
    var decoder = try ArithmeticDecoder.init(&encoded);
    var contexts = [_]u8{0} ** 2;
    var bits: u16 = 0;
    for (0..16) |_| bits = (bits << 1) | try decoder.decode(&contexts, 0);
    try std.testing.expectEqual(@as(u16, 2), bits);
    try std.testing.expect(contexts[0] != 0);
}

test "bitmap composition clips work to visible pixels" {
    var work = DecodeWorkBudget{ .remaining = 100 };
    var target = try Bitmap.blank(std.testing.allocator, &work, 2, 2, 1024);
    defer target.deinit(std.testing.allocator);
    var source = try Bitmap.blank(std.testing.allocator, &work, 4, 2, 1024);
    defer source.deinit(std.testing.allocator);
    @memset(source.data, 0xff);

    // Only the rightmost source column intersects the target.
    try target.compose(source, -3, 0, 4, &work);
    try std.testing.expectEqual(@as(u64, 94), work.remaining);
    try std.testing.expectEqual(@as(u1, 1), target.get(0, 0));
    try std.testing.expectEqual(@as(u1, 0), target.get(1, 0));

    // A fully off-page region does no pixel work.
    try target.compose(source, 2, 0, 4, &work);
    try std.testing.expectEqual(@as(u64, 94), work.remaining);

    // Composition reserves its complete visible cost before touching output.
    var exhausted = DecodeWorkBudget{ .remaining = 1 };
    try std.testing.expectError(error.Jbig2WorkLimitExceeded, target.compose(source, 0, 0, 4, &exhausted));
    try std.testing.expectEqual(@as(u64, 1), exhausted.remaining);
}

test "embedded generic region decodes through segment and page composition" {
    const page = [_]u8{
        0, 0, 0, 0, 48, 0, 1, 0, 0, 0, 19,
        0, 0, 0, 1, 0,  0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0,
    };
    const region = [_]u8{
        0,    0,    0,    1,    38,   0,    1,    0,    0,    0,    38,
        0,    0,    0,    1,    0,    0,    0,    1,    0,    0,    0,
        0,    0,    0,    0,    0,    0,    0,    3,    0xff, 0xfd, 0xff,
        2,    0xfe, 0xfe, 0xfe, 0x84, 0xc7, 0x3b, 0xfc, 0xe1, 0xa1, 0x43,
        0x04, 0x02, 0x20, 0,    0,
    };
    var stream: [page.len + region.len]u8 = undefined;
    @memcpy(stream[0..page.len], &page);
    @memcpy(stream[page.len..], &region);
    var decoded = try decodeAlloc(std.testing.allocator, null, &stream, 1024, 128 * 1024, 1_000_000, null);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqual(@as(u8, 0), decoded.pixels[0]);
    try std.testing.expectError(error.Jbig2WorkLimitExceeded, decodeAlloc(std.testing.allocator, null, &stream, 1024, 128 * 1024, 130, null));

    var unknown_length_stream: [stream.len + 6]u8 = undefined;
    @memcpy(unknown_length_stream[0..stream.len], &stream);
    @memset(unknown_length_stream[page.len + 7 .. page.len + 11], 0xff);
    const end_marker = [_]u8{ 0xff, 0xac, 0, 0, 0, 1 };
    @memcpy(unknown_length_stream[stream.len..], &end_marker);
    var unknown_length = try decodeAlloc(std.testing.allocator, null, &unknown_length_stream, 1024, 128 * 1024, 1_000_000, null);
    defer unknown_length.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, decoded.pixels, unknown_length.pixels);

    var unknown_height = unknown_length_stream;
    @memset(unknown_height[page.len + 15 .. page.len + 19], 0xff);
    var row_terminated = try decodeAlloc(std.testing.allocator, null, &unknown_height, 1024, 128 * 1024, 1_000_000, null);
    defer row_terminated.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, decoded.pixels, row_terminated.pixels);

    const necessary_extension = [_]u8{
        0,    0, 0, 2, 62, 0, 0, 0, 0, 0, 4,
        0xa0, 0, 0, 1,
    };
    var followed: [unknown_length_stream.len + necessary_extension.len]u8 = undefined;
    @memcpy(followed[0..unknown_length_stream.len], &unknown_length_stream);
    @memcpy(followed[unknown_length_stream.len..], &necessary_extension);
    try std.testing.expectError(
        error.UnsupportedJbig2NecessaryExtension,
        decodeAlloc(std.testing.allocator, null, &followed, 1024, 128 * 1024, 1_000_000, null),
    );

    var truncated: [stream.len + 2]u8 = undefined;
    @memcpy(truncated[0..stream.len], &stream);
    @memset(truncated[page.len + 7 .. page.len + 11], 0xff);
    const truncated_marker = [_]u8{ 0xff, 0xac };
    @memcpy(truncated[stream.len..], &truncated_marker);
    try std.testing.expectError(
        error.TruncatedJbig2UnknownLength,
        decodeAlloc(std.testing.allocator, null, &truncated, 1024, 128 * 1024, 1_000_000, null),
    );

    var mismatched = unknown_length_stream;
    mismatched[mismatched.len - 1] = 2;
    try std.testing.expectError(
        error.InvalidJbig2UnknownRowCount,
        decodeAlloc(std.testing.allocator, null, &mismatched, 1024, 128 * 1024, 1_000_000, null),
    );
}

test "PDF 32000 JBIG2 example decodes global dictionary and text region" {
    // PDF 32000-1:2008, 7.4.7 Examples 1 and 2. Keeping this small published
    // vector in-tree protects the segment integration paths, not just the
    // arithmetic primitive.
    const globals_hex =
        "0000000000010000000032" ++
        "000003fffdff02fefefe0000000100000001" ++
        "2ae225aea9a5a538b4d9999c5c8e56ef0f8727f2b53d4e37ef795cc5506dffac";
    const page_hex =
        "0000000130000100000013" ++
        "00000034000000420000000000000000400000" ++
        "00000002062000010000001e" ++
        "000000340000004200000000000000000000100000000231db51ce51ffac" ++
        "0000000331000100000000" ++
        "0000000433010000000000";
    var globals: [globals_hex.len / 2]u8 = undefined;
    var page: [page_hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&globals, globals_hex);
    _ = try std.fmt.hexToBytes(&page, page_hex);

    var decoded = try decodeAlloc(std.testing.allocator, &globals, &page, 1024, 256 * 1024, 1_000_000, null);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 52), decoded.width);
    try std.testing.expectEqual(@as(u32, 66), decoded.height);
    var black_pixels: usize = 0;
    for (decoded.pixels) |byte| black_pixels += @popCount(byte);
    try std.testing.expectEqual(@as(usize, 234), black_pixels);
}

test "decoder enforces cumulative working set" {
    const page = [_]u8{
        0, 0, 0, 0, 48, 0, 1, 0, 0, 0, 19,
        0, 0, 0, 1, 0,  0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0,  0, 0, 0,
    };
    try std.testing.expectError(error.Jbig2WorkingSetTooLarge, decodeAlloc(std.testing.allocator, null, &page, 1024, 1, 1_000_000, null));
    var rounded = try decodeAlloc(std.testing.allocator, null, &page, 1024, 128 * 1024, 1_000_000, .{ .width = 2, .height = 1 });
    defer rounded.deinit(std.testing.allocator);
    try std.testing.expectError(error.Jbig2DimensionMismatch, decodeAlloc(std.testing.allocator, null, &page, 1024, 128 * 1024, 1_000_000, .{ .width = 3, .height = 1 }));
}

test "extension segments ignore metadata and reject necessary content" {
    const comment = [_]u8{
        0, 0, 0, 1, // Segment number.
        62, // Extension segment, one-byte page association.
        0, // No referred-to segments.
        0, // Global page association.
        0, 0, 0, 4, // Payload length.
        0x20, 0, 0, 0, // Single-byte coded comment extension.
    };
    const unknown_metadata = [_]u8{
        0,    0, 0, 2, 62, 0, 0, 0, 0, 0, 4,
        0x00, 0, 0, 1,
    };
    const necessary = [_]u8{
        0,    0, 0, 3, 62, 0, 0, 0, 0, 0, 4,
        0xa0, 0, 0, 1,
    };
    const truncated = [_]u8{
        0,    0, 0, 4, 62, 0, 0, 0, 0, 0, 3,
        0x20, 0, 0,
    };

    var work = DecodeWorkBudget{ .remaining = 10_000 };
    var decoder = Decoder{
        .alloc = std.testing.allocator,
        .work = &work,
        .max_bytes = 1024,
    };
    defer decoder.deinit();
    try decoder.decodeStream(&comment);
    try decoder.decodeStream(&unknown_metadata);
    try std.testing.expectError(error.UnsupportedJbig2NecessaryExtension, decoder.decodeStream(&necessary));
    try std.testing.expectError(error.TruncatedJbig2Extension, decoder.decodeStream(&truncated));
}

test "Treasury mask decodes refinement aggregation and text region" {
    const encoded = @embedFile("testdata/treasury-page25-symbols.b64");
    const base64_decoder = std.base64.standard.decoderWithIgnore("\r\n");
    const stream_buffer = try std.testing.allocator.alloc(u8, base64_decoder.calcSizeUpperBound(encoded.len));
    defer std.testing.allocator.free(stream_buffer);
    const stream_len = try base64_decoder.decode(stream_buffer, encoded);
    const stream = stream_buffer[0..stream_len];

    // Match the Reader's production budget for this 2,393 x 3,201 page.
    const work_limit = 4_000_000 + 8 * 2_393 * 3_201;
    var decoded = try decodeAlloc(std.testing.allocator, null, stream, 64 * 1024 * 1024, 128 * 1024 * 1024, work_limit, null);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2393), decoded.width);
    try std.testing.expectEqual(@as(u32, 3201), decoded.height);
    var black_pixels: usize = 0;
    for (decoded.pixels) |byte| black_pixels += @popCount(byte);
    try std.testing.expectEqual(@as(usize, 230_542), black_pixels);
}

test "Treasury empty refinement dictionary is a valid no-op" {
    // Extracted from page 2 of the March 1985 Treasury Bulletin. The segment
    // declares refinement aggregation but imports, defines, and exports zero
    // symbols. Its trailing bytes are the original arithmetic terminator.
    const encoded = @embedFile("testdata/treasury-1985-page2-empty-refinement-dictionary.hex");
    const payload_len = std.mem.trim(u8, encoded, "\r\n").len / 2;
    const payload = try std.testing.allocator.alloc(u8, payload_len);
    defer std.testing.allocator.free(payload);
    _ = try std.fmt.hexToBytes(payload, std.mem.trim(u8, encoded, "\r\n"));

    var work = DecodeWorkBudget{ .remaining = 1_000_000 };
    var decoder = Decoder{
        .alloc = std.testing.allocator,
        .work = &work,
        .max_bytes = 1024,
    };
    defer decoder.deinit();
    var dictionary = try decoder.decodeDictionary(payload, &.{});
    defer dictionary.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), dictionary.symbols.len);

    // The same empty dictionary cannot claim an exported symbol.
    payload[17] = 1;
    try std.testing.expectError(error.InvalidJbig2SymbolCount, decoder.decodeDictionary(payload, &.{}));
}
