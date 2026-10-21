// SPDX-License-Identifier: 0BSD
//
// Copyright (C) The XZ Embedded authors and contributors
//
// Permission to use, copy, modify, and/or distribute this
// software for any purpose with or without fee is hereby granted.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
// WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
// THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
// CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
// LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
// NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
// CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
//
// Native Zig adaptation of XZ Embedded's branch converters by Lasse Collin
// and Igor Pavlov. Reference revision d4a9bc83c72d8087fe36ff388e89599626da7873:
// https://github.com/tukaani-project/xz-embedded/blob/d4a9bc83c72d8087fe36ff388e89599626da7873/linux/lib/xz/xz_dec_bcj.c

//! Bounded streaming BCJ inversion. Unprocessed instruction tails stay in the
//! filter between writes; positions and x86 overlap masks persist across chunks.
const std = @import("std");
const Writer = std.Io.Writer;
pub const Filter = struct {
    target: *Writer,
    writer: Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    id: u8,
    position: u32,
    x86_mask: u32 = 0,
    buffer: [4096]u8 = undefined,
    len: usize = 0,
    pub fn alignment(id: u64) ?u32 {
        return switch (id) {
            4 => 1,
            5, 7, 9, 10 => 4,
            6 => 16,
            8, 11 => 2,
            else => null,
        };
    }
    pub fn finish(self: *Filter) Writer.Error!void {
        try self.target.writeAll(self.buffer[0..self.len]);
        self.len = 0;
    }
    fn send(self: *Filter, bytes: []const u8) Writer.Error!void {
        var at: usize = 0;
        while (at < bytes.len) {
            const n = @min(bytes.len - at, self.buffer.len - self.len);
            @memcpy(self.buffer[self.len..][0..n], bytes[at..][0..n]);
            self.len += n;
            at += n;
            const processed = self.apply(self.buffer[0..self.len]);
            try self.target.writeAll(self.buffer[0..processed]);
            self.position +%= @truncate(processed);
            std.mem.copyForwards(u8, &self.buffer, self.buffer[processed..self.len]);
            self.len -= processed;
        }
    }
    fn drain(w: *Writer, slices: []const []const u8, splat: usize) Writer.Error!usize {
        const self: *Filter = @alignCast(@fieldParentPtr("writer", w));
        var n: usize = 0;
        for (slices[0 .. slices.len - 1]) |bytes| {
            try self.send(bytes);
            n += bytes.len;
        }
        for (0..splat) |_| {
            const bytes = slices[slices.len - 1];
            try self.send(bytes);
            n += bytes.len;
        }
        return n;
    }
    fn apply(self: *Filter, b: []u8) usize {
        return switch (self.id) {
            4 => self.x86(b),
            6 => self.ia64(b),
            8 => self.thumb(b),
            11 => self.riscv(b),
            else => self.fixed(b),
        };
    }
    fn fixed(self: *Filter, b: []u8) usize {
        const end = b.len & ~@as(usize, 3);
        var i: usize = 0;
        while (i < end) : (i += 4) {
            const pc = self.position +% @as(u32, @truncate(i));
            const endian: std.builtin.Endian = if (self.id == 5 or self.id == 9) .big else .little;
            var v = std.mem.readInt(u32, b[i..][0..4], endian);
            switch (self.id) {
                5 => if (v & 0xfc000003 == 0x48000001) {
                    v = ((v -% pc) & 0x03fffffc) | 0x48000001;
                },
                7 => if (v >> 24 == 0xeb) {
                    v = 0xeb000000 | (((v << 2) -% (pc +% 8)) >> 2 & 0x00ffffff);
                },
                9 => if (v >> 22 == 0x100 or v >> 22 == 0x1ff) {
                    v = ((v << 2) -% pc) >> 2;
                    v = (0x40000000 -% (v & 0x400000)) | 0x40000000 | (v & 0x3fffff);
                },
                10 => {
                    if (v >> 26 == 0x25) {
                        v = 0x94000000 | ((v -% (pc >> 2)) & 0x03ffffff);
                    } else if (v & 0x9f000000 == 0x90000000) {
                        var addr = ((v >> 29) & 3) | ((v >> 3) & 0x1ffffc);
                        if ((addr +% 0x020000) & 0x1c0000 != 0) continue;
                        addr -%= pc >> 12;
                        v = (v & 0x9000001f) | ((addr & 3) << 29) | ((addr & 0x03fffc) << 3);
                        v |= (0 -% (addr & 0x020000)) & 0xe00000;
                    }
                },
                else => unreachable,
            }
            std.mem.writeInt(u32, b[i..][0..4], v, endian);
        }
        return end;
    }
    fn thumb(self: *Filter, b: []u8) usize {
        var i: usize = 0;
        while (i + 4 <= b.len) : (i += 2) {
            if (b[i + 1] & 0xf8 != 0xf0 or b[i + 3] & 0xf8 != 0xf8) continue;
            var addr = (@as(u32, b[i + 1] & 7) << 19) | (@as(u32, b[i]) << 11) | (@as(u32, b[i + 3] & 7) << 8) | b[i + 2];
            addr = ((addr << 1) -% (self.position +% @as(u32, @truncate(i)) +% 4)) >> 1;
            b[i + 1] = 0xf0 | @as(u8, @truncate((addr >> 19) & 7));
            b[i] = @truncate(addr >> 11);
            b[i + 3] = 0xf8 | @as(u8, @truncate((addr >> 8) & 7));
            b[i + 2] = @truncate(addr);
            i += 2;
        }
        return i;
    }
    fn msbyte(b: u8) bool {
        return b == 0 or b == 255;
    }
    fn x86(self: *Filter, b: []u8) usize {
        const allowed = [_]bool{ true, true, true, false, true, false, false, false };
        const bits = [_]u5{ 0, 1, 2, 2, 3, 3, 3, 3 };
        if (b.len <= 4) return 0;
        var prev: usize = std.math.maxInt(usize);
        var mask = self.x86_mask;
        var i: usize = 0;
        while (i < b.len - 4) : (i += 1) {
            if (b[i] & 0xfe != 0xe8) continue;
            const distance = i -% prev;
            if (distance > 3) {
                mask = 0;
            } else {
                mask = (mask << @as(u5, @intCast(distance - 1))) & 7;
                if (mask != 0 and (!allowed[mask] or msbyte(b[i + 4 - bits[mask]]))) {
                    prev = i;
                    mask = (mask << 1) | 1;
                    continue;
                }
            }
            prev = i;
            if (msbyte(b[i + 4])) {
                var src = std.mem.readInt(u32, b[i + 1 ..][0..4], .little);
                var dest: u32 = undefined;
                while (true) {
                    dest = src -% (self.position +% @as(u32, @truncate(i)) +% 5);
                    if (mask == 0) break;
                    const shift = bits[mask] * 8;
                    if (!msbyte(@truncate(dest >> (24 - shift)))) break;
                    src = dest ^ ((@as(u32, 1) << @as(u5, @intCast(32 - @as(u6, shift)))) - 1);
                }
                dest &= 0x01ffffff;
                dest |= 0 -% (dest & 0x01000000);
                std.mem.writeInt(u32, b[i + 1 ..][0..4], dest, .little);
                i += 4;
            } else {
                mask = (mask << 1) | 1;
            }
        }
        const distance = i -% prev;
        self.x86_mask = if (distance > 3) 0 else mask << @as(u5, @intCast(distance - 1));
        return i;
    }
    fn ia64(self: *Filter, b: []u8) usize {
        const branches = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 6, 6, 0, 0, 7, 7, 4, 4, 0, 0, 4, 4, 0, 0 };
        const end = b.len & ~@as(usize, 15);
        var i: usize = 0;
        while (i < end) : (i += 16) {
            const mask = branches[b[i] & 31];
            for (0..3) |slot| {
                if ((mask >> @as(u3, @intCast(slot))) & 1 == 0) continue;
                const bit = 5 + 41 * slot;
                const byte = bit >> 3;
                const rem: u6 = @intCast(bit & 7);
                var instr: u64 = 0;
                for (0..6) |j| instr |= @as(u64, b[i + byte + j]) << @as(u6, @intCast(8 * j));
                var norm = instr >> rem;
                if ((norm >> 37) & 15 != 5 or (norm >> 9) & 7 != 0) continue;
                var addr: u32 = @truncate((norm >> 13) & 0xfffff);
                addr |= @as(u32, @truncate((norm >> 36) & 1)) << 20;
                addr = ((addr << 4) -% (self.position +% @as(u32, @truncate(i)))) >> 4;
                norm &= ~(@as(u64, 0x8fffff) << 13);
                norm |= @as(u64, addr & 0xfffff) << 13;
                norm |= @as(u64, addr & 0x100000) << 16;
                instr = (instr & ((@as(u64, 1) << rem) - 1)) | (norm << rem);
                for (0..6) |j| b[i + byte + j] = @truncate(instr >> @as(u6, @intCast(8 * j)));
            }
        }
        return end;
    }
    fn riscv(self: *Filter, b: []u8) usize {
        var i: usize = 0;
        while (i + 8 <= b.len) : (i += 2) {
            var instr: u32 = b[i];
            const pc = self.position +% @as(u32, @truncate(i));
            if (instr == 0xef) {
                const b1: u32 = b[i + 1];
                if (b1 & 0x0d != 0) continue;
                var addr = ((b1 & 0xf0) << 13) | (@as(u32, b[i + 2]) << 9) | (@as(u32, b[i + 3]) << 1);
                addr -%= pc;
                b[i + 1] = @truncate((b1 & 15) | ((addr >> 8) & 0xf0));
                b[i + 2] = @truncate(((addr >> 16) & 15) | ((addr >> 7) & 0x10) | ((addr << 4) & 0xe0));
                b[i + 3] = @truncate(((addr >> 4) & 0x7f) | ((addr >> 13) & 0x80));
                i += 2;
            } else if (instr & 0x7f == 0x17) {
                instr = std.mem.readInt(u32, b[i..][0..4], .little);
                var instr2: u32 = undefined;
                var addr: u32 = undefined;
                if (instr & 0xe80 != 0) {
                    instr2 = std.mem.readInt(u32, b[i + 4 ..][0..4], .little);
                    if (((instr << 8) ^ (instr2 -% 3)) & 0xf8003 != 0) {
                        i += 4;
                        continue;
                    }
                    addr = (instr & 0xfffff000) +% (instr2 >> 20);
                    instr = 0x17 | (2 << 7) | (instr2 << 12);
                    instr2 = addr;
                } else {
                    const rs1 = instr >> 27;
                    if ((instr -% 0x3117) << 18 >= rs1 & 0x1d) {
                        i += 2;
                        continue;
                    }
                    addr = std.mem.readInt(u32, b[i + 4 ..][0..4], .big) -% pc;
                    instr2 = (instr >> 12) | (addr << 20);
                    instr = 0x17 | (rs1 << 7) | ((addr +% 0x800) & 0xfffff000);
                }
                std.mem.writeInt(u32, b[i..][0..4], instr, .little);
                std.mem.writeInt(u32, b[i + 4 ..][0..4], instr2, .little);
                i += 6;
            }
        }
        return i;
    }
};
