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

    fn blank(alloc: Allocator, width: u32, height: u32, max_bytes: usize) !Bitmap {
        const result = try init(alloc, width, height, max_bytes);
        @memset(result.data, 0);
        return result;
    }

    fn clone(self: Bitmap, alloc: Allocator, max_bytes: usize) !Bitmap {
        const result = try init(alloc, self.width, self.height, max_bytes);
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

    fn compose(self: Bitmap, source: Bitmap, dx: i64, dy: i64, op: u3) !void {
        if (op > 4) return error.UnsupportedJbig2CombinationOperator;
        var sy: u32 = 0;
        while (sy < source.height) : (sy += 1) {
            const ty = dy + sy;
            if (ty < 0 or ty >= self.height) continue;
            var sx: u32 = 0;
            while (sx < source.width) : (sx += 1) {
                const tx = dx + sx;
                if (tx < 0 or tx >= self.width) continue;
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
                self.set(@intCast(tx), @intCast(ty), value);
            }
        }
    }
};

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

fn decodeGeneric0(alloc: Allocator, arith: *ArithmeticDecoder, contexts: []u8, width: u32, height: u32, at: [4][2]i8, max_bytes: usize) !Bitmap {
    const defaults = [4][2]i8{ .{ 3, -1 }, .{ -3, -1 }, .{ 2, -2 }, .{ -2, -2 } };
    if (!std.mem.eql([2]i8, &at, &defaults)) return error.UnsupportedJbig2AdaptiveTemplate;
    var bitmap = try Bitmap.blank(alloc, width, height, max_bytes);
    errdefer bitmap.deinit(alloc);
    var y: u32 = 0;
    while (y < height) : (y += 1) {
        var x: u32 = 0;
        while (x < width) : (x += 1) {
            const ix: i64 = x;
            const iy: i64 = y;
            var cx: usize = 0;
            cx |= @as(usize, bitmap.get(ix - 1, iy)) << 0;
            cx |= @as(usize, bitmap.get(ix - 2, iy)) << 1;
            cx |= @as(usize, bitmap.get(ix - 3, iy)) << 2;
            cx |= @as(usize, bitmap.get(ix - 4, iy)) << 3;
            cx |= @as(usize, bitmap.get(ix + 3, iy - 1)) << 4;
            cx |= @as(usize, bitmap.get(ix - 2, iy - 1)) << 5;
            cx |= @as(usize, bitmap.get(ix - 1, iy - 1)) << 6;
            cx |= @as(usize, bitmap.get(ix, iy - 1)) << 7;
            cx |= @as(usize, bitmap.get(ix + 1, iy - 1)) << 8;
            cx |= @as(usize, bitmap.get(ix + 2, iy - 1)) << 9;
            cx |= @as(usize, bitmap.get(ix - 3, iy - 1)) << 10;
            cx |= @as(usize, bitmap.get(ix + 2, iy - 2)) << 11;
            cx |= @as(usize, bitmap.get(ix - 1, iy - 2)) << 12;
            cx |= @as(usize, bitmap.get(ix, iy - 2)) << 13;
            cx |= @as(usize, bitmap.get(ix + 1, iy - 2)) << 14;
            cx |= @as(usize, bitmap.get(ix - 2, iy - 2)) << 15;
            bitmap.set(x, y, try arith.decode(contexts, cx));
        }
    }
    return bitmap;
}

fn decodeRefinement0(alloc: Allocator, arith: *ArithmeticDecoder, contexts: []u8, width: u32, height: u32, reference: Bitmap, dx: i64, dy: i64, at: [2][2]i8, max_bytes: usize) !Bitmap {
    const defaults = [2][2]i8{ .{ -1, -1 }, .{ -1, -1 } };
    if (!std.mem.eql([2]i8, &at, &defaults)) return error.UnsupportedJbig2AdaptiveTemplate;
    var bitmap = try Bitmap.blank(alloc, width, height, max_bytes);
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

    fn init(alloc: Allocator) !IntContexts {
        var all: [9][]u8 = undefined;
        var count: usize = 0;
        errdefer for (all[0..count]) |item| alloc.free(item);
        for (&all) |*item| {
            item.* = try alloc.alloc(u8, 512);
            @memset(item.*, 0);
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
};

fn ceilLog2(value: usize) !u6 {
    if (value == 0) return error.InvalidJbig2SymbolCount;
    return @intCast(std.math.log2_int_ceil(usize, value));
}

fn decodeTextRegion(alloc: Allocator, arith: *ArithmeticDecoder, symbols: []const Bitmap, params: TextParams, shared_generic_contexts: ?[]u8, shared_iaid: ?[]u8, max_bytes: usize) !Bitmap {
    const pixels = std.math.mul(u64, params.width, params.height) catch return error.Jbig2ImageTooLarge;
    if (params.instances > pixels) return error.InvalidJbig2SymbolCount;
    const code_len = try ceilLog2(symbols.len);
    var int_ctx = try IntContexts.init(alloc);
    defer int_ctx.deinit(alloc);
    const iaid = if (shared_iaid) |value| value else blk: {
        const value = try alloc.alloc(u8, @as(usize, 1) << code_len);
        @memset(value, 0);
        break :blk value;
    };
    defer if (shared_iaid == null) alloc.free(iaid);
    const generic_ctx = if (shared_generic_contexts) |value| value else blk: {
        const value = try alloc.alloc(u8, 65536);
        @memset(value, 0);
        break :blk value;
    };
    defer if (shared_generic_contexts == null) alloc.free(generic_ctx);

    var region = try Bitmap.blank(alloc, params.width, params.height, max_bytes);
    errdefer region.deinit(alloc);
    if (params.default_pixel == 1) @memset(region.data, 0xff);
    const strips: i64 = @as(i64, 1) << params.strips_log;
    const initial_t = try decodeInteger(arith, int_ctx.dt) orelse return error.InvalidJbig2Integer;
    var strip_t = -initial_t * strips;
    var first_s: i64 = 0;
    var count: u32 = 0;
    while (count < params.instances) {
        const dt = try decodeInteger(arith, int_ctx.dt) orelse return error.InvalidJbig2Integer;
        strip_t += dt * strips;
        var first = true;
        var current_s: i64 = 0;
        while (count < params.instances) {
            if (first) {
                const dfs = try decodeInteger(arith, int_ctx.fs) orelse return error.InvalidJbig2Integer;
                first_s += dfs;
                current_s = first_s;
                first = false;
            } else {
                const ids = try decodeInteger(arith, int_ctx.ds) orelse break;
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
                refined = try decodeRefinement0(alloc, arith, generic_ctx, @intCast(new_width), @intCast(new_height), symbols[id], @divFloor(rdw, 2) + rdx, @divFloor(rdh, 2) + rdy, params.refine_at, max_bytes);
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
            try region.compose(symbol, s, t, params.op);
            if (!params.transposed and (params.reference_corner == 0 or params.reference_corner == 1)) current_s += symbol.width - 1;
            if (params.transposed and (params.reference_corner == 1 or params.reference_corner == 3)) current_s += symbol.height - 1;
            count += 1;
        }
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

const Decoder = struct {
    alloc: Allocator,
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

    fn dictionaryFor(self: *Decoder, number: u32) ?*const Dictionary {
        for (self.segments.items) |*segment| if (segment.number == number and segment.dictionary != null) return &segment.dictionary.?;
        return null;
    }

    fn decodeStream(self: *Decoder, bytes: []const u8) !void {
        var cursor = Cursor{ .bytes = bytes };
        while (cursor.pos < bytes.len) {
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
            if (length == 0xffffffff) return error.UnsupportedJbig2UnknownLength;
            const payload = try cursor.take(length);
            // Reserve list capacity before a segment decoder creates owned
            // data, so an append OOM cannot orphan a decoded dictionary.
            try self.segments.ensureUnusedCapacity(self.alloc, 1);
            var stored = StoredSegment{ .number = number };
            switch (segment_type) {
                0 => stored.dictionary = try self.decodeDictionary(payload, refs[0..ref_count]),
                6, 7 => try self.decodeText(payload, refs[0..ref_count]),
                38, 39 => try self.decodeGeneric(payload),
                48 => try self.decodePageInformation(payload),
                49, 50, 51, 52, 62 => {},
                else => return error.UnsupportedJbig2Segment,
            }
            self.segments.appendAssumeCapacity(stored);
        }
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
            if (width != expected.width or height != expected.height) return error.Jbig2DimensionMismatch;
        }
        self.page_default = @truncate((flags >> 2) & 1);
        self.page_op = @truncate((flags >> 3) & 3);
        self.page_allows_op_override = (flags & 0x40) != 0;
        const page = try Bitmap.blank(self.alloc, width, height, self.max_bytes);
        if (self.page_default == 1) @memset(page.data, 0xff);
        self.page = page;
    }

    fn decodeDictionary(self: *Decoder, payload: []const u8, refs: []const u32) !Dictionary {
        var cursor = Cursor{ .bytes = payload };
        const flags = try cursor.u16be();
        const huffman = (flags & 1) != 0;
        const refine = (flags & 2) != 0;
        const template = (flags >> 10) & 3;
        const refine_template = (flags >> 12) & 1;
        if (huffman or template != 0 or (refine and refine_template != 0) or (flags & 0x0300) != 0) return error.UnsupportedJbig2DictionaryProfile;
        var at: [4][2]i8 = undefined;
        for (&at) |*point| {
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
            const dict = self.dictionaryFor(ref) orelse return error.MissingJbig2SymbolDictionary;
            for (dict.symbols) |symbol| {
                try symbols.ensureUnusedCapacity(self.alloc, 1);
                var copy = try symbol.clone(self.alloc, self.max_bytes);
                if (copy.data.len > self.max_bytes -| symbol_bytes) {
                    copy.deinit(self.alloc);
                    return error.Jbig2ImageTooLarge;
                }
                symbol_bytes += copy.data.len;
                symbols.appendAssumeCapacity(copy);
            }
        }
        const imported_count = symbols.items.len;
        const code_len = if (refine) try ceilLog2(imported_count + new_count) else 0;
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        const generic_ctx = try self.alloc.alloc(u8, 65536);
        defer self.alloc.free(generic_ctx);
        @memset(generic_ctx, 0);
        const dh = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(dh);
        @memset(dh, 0);
        const dw = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(dw);
        @memset(dw, 0);
        const iaai = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(iaai);
        @memset(iaai, 0);
        const iaex = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(iaex);
        @memset(iaex, 0);
        const iardx = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(iardx);
        @memset(iardx, 0);
        const iardy = try self.alloc.alloc(u8, 512);
        defer self.alloc.free(iardy);
        @memset(iardy, 0);
        var iaid: []u8 = &.{};
        if (refine) {
            iaid = try self.alloc.alloc(u8, @as(usize, 1) << code_len);
            @memset(iaid, 0);
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
                width += delta_width;
                if (width <= 0 or width > std.math.maxInt(u32)) return error.InvalidJbig2Dimensions;
                try symbols.ensureUnusedCapacity(self.alloc, 1);
                var symbol: Bitmap = undefined;
                if (!refine) {
                    symbol = try decodeGeneric0(self.alloc, &arith, generic_ctx, @intCast(width), @intCast(height), at, self.max_bytes);
                } else {
                    const instances = try decodeInteger(&arith, iaai) orelse return error.InvalidJbig2Integer;
                    if (instances == 1) {
                        const id = try decodeIaid(&arith, iaid, code_len);
                        if (id >= symbols.items.len) return error.InvalidJbig2SymbolId;
                        const rdx = try decodeInteger(&arith, iardx) orelse return error.InvalidJbig2Integer;
                        const rdy = try decodeInteger(&arith, iardy) orelse return error.InvalidJbig2Integer;
                        symbol = try decodeRefinement0(self.alloc, &arith, generic_ctx, @intCast(width), @intCast(height), symbols.items[id], rdx, rdy, refine_at, self.max_bytes);
                    } else if (instances > 1 and instances <= std.math.maxInt(u32)) {
                        symbol = try decodeTextRegion(self.alloc, &arith, symbols.items, .{
                            .width = @intCast(width),
                            .height = @intCast(height),
                            .instances = @intCast(instances),
                            .strips_log = 0,
                            .reference_corner = 0,
                            .transposed = false,
                            .op = 0,
                            .default_pixel = 0,
                            .ds_offset = 0,
                            .refine = true,
                            .refine_at = refine_at,
                        }, generic_ctx, iaid, self.max_bytes);
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
            if (run_count > std.math.mul(usize, total, 2) catch return error.InvalidJbig2ExportRun) return error.InvalidJbig2ExportRun;
            run_count += 1;
            const run = try decodeInteger(&arith, iaex) orelse return error.InvalidJbig2ExportRun;
            if (run < 0 or run > total - index) return error.InvalidJbig2ExportRun;
            if (run == 0 and previous_run_was_zero) return error.InvalidJbig2ExportRun;
            previous_run_was_zero = run == 0;
            if (flag == 1) for (symbols.items[index .. index + @as(usize, @intCast(run))]) |symbol| {
                try exported.ensureUnusedCapacity(self.alloc, 1);
                var copy = try symbol.clone(self.alloc, self.max_bytes);
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
        if (exported.items.len != export_count or symbols.items.len != imported_count + new_count) return error.InvalidJbig2SymbolCount;
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
            const dict = self.dictionaryFor(ref) orelse return error.MissingJbig2SymbolDictionary;
            try symbols.appendSlice(self.alloc, dict.symbols);
        }
        if (symbols.items.len == 0) return error.MissingJbig2SymbolDictionary;
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        var region = try decodeTextRegion(self.alloc, &arith, symbols.items, .{
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
        }, null, null, self.max_bytes);
        defer region.deinit(self.alloc);
        try self.page.?.compose(region, x, y, if (self.page_allows_op_override) @truncate(region_flags & 7) else self.page_op);
    }

    fn decodeGeneric(self: *Decoder, payload: []const u8) !void {
        if (self.page == null) return error.MissingJbig2PageInformation;
        var cursor = Cursor{ .bytes = payload };
        const width = try cursor.u32be();
        const height = try cursor.u32be();
        const x = try cursor.i32be();
        const y = try cursor.i32be();
        const region_flags = try cursor.byte();
        const flags = try cursor.byte();
        if ((flags & 1) != 0 or ((flags >> 1) & 3) != 0 or ((flags >> 3) & 1) != 0 or ((flags >> 4) & 1) != 0) return error.UnsupportedJbig2GenericProfile;
        var at: [4][2]i8 = undefined;
        for (&at) |*point| {
            point[0] = @bitCast(try cursor.byte());
            point[1] = @bitCast(try cursor.byte());
        }
        var arith = try ArithmeticDecoder.init(payload[cursor.pos..]);
        const contexts = try self.alloc.alloc(u8, 65536);
        defer self.alloc.free(contexts);
        @memset(contexts, 0);
        var region = try decodeGeneric0(self.alloc, &arith, contexts, width, height, at, self.max_bytes);
        defer region.deinit(self.alloc);
        try self.page.?.compose(region, x, y, if (self.page_allows_op_override) @truncate(region_flags & 7) else self.page_op);
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
    expected_dimensions: ?ExpectedDimensions,
) !Decoded {
    if (max_output_bytes == 0) return error.Jbig2ImageTooLarge;
    if (max_working_set_bytes == 0) return error.Jbig2WorkingSetTooLarge;
    const input_bytes = std.math.add(usize, bytes.len, if (globals) |value| value.len else 0) catch return error.Jbig2WorkingSetTooLarge;
    if (input_bytes >= max_working_set_bytes) return error.Jbig2WorkingSetTooLarge;
    var budget = WorkingSetAllocator{ .backing = alloc, .live_bytes = input_bytes, .max_live_bytes = max_working_set_bytes };
    var decoder = Decoder{ .alloc = budget.allocator(), .max_bytes = max_output_bytes, .expected_dimensions = expected_dimensions };
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
    var decoded = try decodeAlloc(std.testing.allocator, null, &stream, 1024, 128 * 1024, null);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqual(@as(u8, 0), decoded.pixels[0]);
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

    var decoded = try decodeAlloc(std.testing.allocator, &globals, &page, 1024, 256 * 1024, null);
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
    try std.testing.expectError(error.Jbig2WorkingSetTooLarge, decodeAlloc(std.testing.allocator, null, &page, 1024, 1, null));
    try std.testing.expectError(error.Jbig2DimensionMismatch, decodeAlloc(std.testing.allocator, null, &page, 1024, 128 * 1024, .{ .width = 2, .height = 1 }));
}

test "Treasury mask decodes refinement aggregation and text region" {
    const encoded = @embedFile("testdata/treasury-page25-symbols.b64");
    const base64_decoder = std.base64.standard.decoderWithIgnore("\r\n");
    const stream_buffer = try std.testing.allocator.alloc(u8, base64_decoder.calcSizeUpperBound(encoded.len));
    defer std.testing.allocator.free(stream_buffer);
    const stream_len = try base64_decoder.decode(stream_buffer, encoded);
    const stream = stream_buffer[0..stream_len];

    var decoded = try decodeAlloc(std.testing.allocator, null, stream, 64 * 1024 * 1024, 128 * 1024 * 1024, null);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u32, 2393), decoded.width);
    try std.testing.expectEqual(@as(u32, 3201), decoded.height);
    var black_pixels: usize = 0;
    for (decoded.pixels) |byte| black_pixels += @popCount(byte);
    try std.testing.expectEqual(@as(usize, 230_542), black_pixels);
}
