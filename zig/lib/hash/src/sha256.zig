// The MIT License (Expat)
//
// Copyright (c) Zig contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//! SHA-256 adapted from Zig 0.16 std/crypto/sha2.zig. Runtime dispatch retains
//! exact wire semantics in baseline binaries. No AVX/XGETBV dependency.
const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const math = std.math;
const cpu = @import("cpu.zig");
// Keep the existing compiler-specialized kernel where std already enables it.
// Baseline binaries use the identical digest with runtime feature discovery.
pub const RuntimeSha256 = Sha2x32(iv256, 256);
pub const Sha256 = if (asm_supported and switch (builtin.cpu.arch) {
    .aarch64 => builtin.cpu.has(.aarch64, .sha2),
    .x86_64 => builtin.cpu.hasAll(.x86, &.{ .sha, .avx2 }),
    else => false,
}) std.crypto.hash.sha2.Sha256 else RuntimeSha256;
const iv256 = Iv32{
    0x6A09E667,
    0xBB67AE85,
    0x3C6EF372,
    0xA54FF53A,
    0x510E527F,
    0x9B05688C,
    0x1F83D9AB,
    0x5BE0CD19,
};

const Iv32 = [8]u32;
fn Sha2x32(comptime iv: Iv32, digest_bits: comptime_int) type {
    return struct {
        const Self = @This();
        pub const block_length = 64;
        pub const digest_length = digest_bits / 8;
        pub const Options = struct { portable: bool = false };

        accelerated: bool,
        s: [8]u32 align(16),
        // Streaming Cache
        buf: [64]u8 = undefined,
        buf_len: u8 = 0,
        total_len: u64 = 0,

        pub fn init(options: Options) Self {
            return Self{ .s = iv, .accelerated = !options.portable and !@inComptime() and hardwareAvailable() };
        }

        pub fn hash(b: []const u8, out: *[digest_length]u8, options: Options) void {
            var d = Self.init(options);
            d.update(b);
            d.final(out);
        }

        pub fn update(d: *Self, b: []const u8) void {
            var off: usize = 0;

            // Partial buffer exists from previous update. Copy into buffer then hash.
            if (d.buf_len != 0 and d.buf_len + b.len >= 64) {
                off += 64 - d.buf_len;
                @memcpy(d.buf[d.buf_len..][0..off], b[0..off]);

                d.round(&d.buf);
                d.buf_len = 0;
            }

            // Full middle blocks.
            while (off + 64 <= b.len) : (off += 64) {
                d.round(b[off..][0..64]);
            }

            // Copy any remainder for next pass.
            const b_slice = b[off..];
            @memcpy(d.buf[d.buf_len..][0..b_slice.len], b_slice);
            d.buf_len += @as(u8, @intCast(b[off..].len));

            d.total_len += b.len;
        }

        pub fn peek(d: Self) [digest_length]u8 {
            var copy = d;
            return copy.finalResult();
        }

        pub fn final(d: *Self, out: *[digest_length]u8) void {
            // The buffer here will never be completely full.
            @memset(d.buf[d.buf_len..], 0);

            // Append padding bits.
            d.buf[d.buf_len] = 0x80;
            d.buf_len += 1;

            // > 448 mod 512 so need to add an extra round to wrap around.
            if (64 - d.buf_len < 8) {
                d.round(&d.buf);
                @memset(d.buf[0..], 0);
            }

            // Append message length.
            var i: usize = 1;
            var len = d.total_len >> 5;
            d.buf[63] = @as(u8, @intCast(d.total_len & 0x1f)) << 3;
            while (i < 8) : (i += 1) {
                d.buf[63 - i] = @as(u8, @intCast(len & 0xff));
                len >>= 8;
            }

            d.round(&d.buf);

            // May truncate for possible 224 or 192 output
            const rr = d.s[0 .. digest_length / 4];

            for (rr, 0..) |s, j| {
                mem.writeInt(u32, out[4 * j ..][0..4], s, .big);
            }
        }

        pub fn finalResult(d: *Self) [digest_length]u8 {
            var result: [digest_length]u8 = undefined;
            d.final(&result);
            return result;
        }

        const W = [64]u32{
            0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
            0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3, 0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
            0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC, 0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
            0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7, 0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
            0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13, 0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
            0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
            0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
            0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208, 0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
        };

        fn round(d: *Self, b: *const [64]u8) void {
            var s: [64]u32 align(16) = undefined;
            for (@as(*align(1) const [16]u32, @ptrCast(b)), 0..) |*elem, i| {
                s[i] = mem.readInt(u32, mem.asBytes(elem), .big);
            }

            if (!@inComptime() and d.accelerated) {
                const V4u32 = @Vector(4, u32);
                switch (builtin.cpu.arch) {
                    .aarch64 => if (comptime asm_supported) {
                        var x: V4u32 = d.s[0..4].*;
                        var y: V4u32 = d.s[4..8].*;
                        const s_v = @as(*[16]V4u32, @ptrCast(&s));

                        comptime var k: u8 = 0;
                        inline while (k < 16) : (k += 1) {
                            if (k > 3) {
                                s_v[k] = asm (
                                    \\.arch_extension sha2
                                    \\sha256su0.4s %[w0_3], %[w4_7]
                                    \\sha256su1.4s %[w0_3], %[w8_11], %[w12_15]
                                    : [w0_3] "=&w" (-> V4u32),
                                    : [_] "0" (s_v[k - 4]),
                                      [w4_7] "w" (s_v[k - 3]),
                                      [w8_11] "w" (s_v[k - 2]),
                                      [w12_15] "w" (s_v[k - 1]),
                                );
                            }

                            const w: V4u32 = s_v[k] +% @as(V4u32, W[4 * k ..][0..4].*);
                            asm volatile (
                                \\.arch_extension sha2
                                \\mov.4s v0, %[x]
                                \\sha256h.4s %[x], %[y], %[w]
                                \\sha256h2.4s %[y], v0, %[w]
                                : [x] "=w" (x),
                                  [y] "=w" (y),
                                : [_] "0" (x),
                                  [_] "1" (y),
                                  [w] "w" (w),
                                : .{ .v0 = true });
                        }

                        d.s[0..4].* = x +% @as(V4u32, d.s[0..4].*);
                        d.s[4..8].* = y +% @as(V4u32, d.s[4..8].*);
                        return;
                    },
                    // C backend doesn't currently support passing vectors to inline asm.
                    .x86_64 => if (comptime asm_supported) {
                        var x: V4u32 = [_]u32{ d.s[5], d.s[4], d.s[1], d.s[0] };
                        var y: V4u32 = [_]u32{ d.s[7], d.s[6], d.s[3], d.s[2] };
                        const s_v = @as(*[16]V4u32, @ptrCast(&s));

                        comptime var k: u8 = 0;
                        inline while (k < 16) : (k += 1) {
                            if (k < 12) {
                                const tmp = asm ("sha256msg1 %[next], %[current]"
                                    : [current] "=x" (-> V4u32),
                                    : [_] "0" (s_v[k]),
                                      [next] "x" (s_v[k + 1]),
                                );
                                const shifted: V4u32 = .{ s_v[k + 2][1], s_v[k + 2][2], s_v[k + 2][3], s_v[k + 3][0] };
                                s_v[k + 4] = asm ("sha256msg2 %[last], %[current]"
                                    : [current] "=x" (-> V4u32),
                                    : [_] "0" (tmp +% shifted),
                                      [last] "x" (s_v[k + 3]),
                                );
                            }

                            const w: V4u32 = s_v[k] +% @as(V4u32, W[4 * k ..][0..4].*);
                            y = asm ("sha256rnds2 %[x], %[y]"
                                : [y] "=x" (-> V4u32),
                                : [_] "0" (y),
                                  [x] "x" (x),
                                  [_] "{xmm0}" (w),
                            );

                            x = asm ("sha256rnds2 %[y], %[x]"
                                : [x] "=x" (-> V4u32),
                                : [_] "0" (x),
                                  [y] "x" (y),
                                  [_] "{xmm0}" (@as(V4u32, @bitCast(@as(u128, @bitCast(w)) >> 64))),
                            );
                        }

                        d.s[0] +%= x[3];
                        d.s[1] +%= x[2];
                        d.s[4] +%= x[1];
                        d.s[5] +%= x[0];
                        d.s[2] +%= y[3];
                        d.s[3] +%= y[2];
                        d.s[6] +%= y[1];
                        d.s[7] +%= y[0];
                        return;
                    },
                    else => {},
                }
            }

            var i: usize = 16;
            while (i < 64) : (i += 1) {
                s[i] = s[i - 16] +% s[i - 7] +% (math.rotr(u32, s[i - 15], @as(u32, 7)) ^ math.rotr(u32, s[i - 15], @as(u32, 18)) ^ (s[i - 15] >> 3)) +% (math.rotr(u32, s[i - 2], @as(u32, 17)) ^ math.rotr(u32, s[i - 2], @as(u32, 19)) ^ (s[i - 2] >> 10));
            }

            var v: [8]u32 = d.s;

            const round0 = comptime [_]RoundParam256{
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 0),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 1),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 2),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 3),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 4),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 5),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 6),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 7),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 8),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 9),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 10),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 11),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 12),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 13),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 14),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 15),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 16),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 17),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 18),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 19),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 20),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 21),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 22),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 23),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 24),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 25),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 26),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 27),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 28),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 29),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 30),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 31),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 32),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 33),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 34),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 35),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 36),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 37),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 38),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 39),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 40),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 41),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 42),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 43),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 44),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 45),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 46),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 47),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 48),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 49),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 50),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 51),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 52),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 53),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 54),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 55),
                roundParam256(0, 1, 2, 3, 4, 5, 6, 7, 56),
                roundParam256(7, 0, 1, 2, 3, 4, 5, 6, 57),
                roundParam256(6, 7, 0, 1, 2, 3, 4, 5, 58),
                roundParam256(5, 6, 7, 0, 1, 2, 3, 4, 59),
                roundParam256(4, 5, 6, 7, 0, 1, 2, 3, 60),
                roundParam256(3, 4, 5, 6, 7, 0, 1, 2, 61),
                roundParam256(2, 3, 4, 5, 6, 7, 0, 1, 62),
                roundParam256(1, 2, 3, 4, 5, 6, 7, 0, 63),
            };
            inline for (round0) |r| {
                v[r.h] = v[r.h] +% (math.rotr(u32, v[r.e], @as(u32, 6)) ^ math.rotr(u32, v[r.e], @as(u32, 11)) ^ math.rotr(u32, v[r.e], @as(u32, 25))) +% (v[r.g] ^ (v[r.e] & (v[r.f] ^ v[r.g]))) +% W[r.i] +% s[r.i];

                v[r.d] = v[r.d] +% v[r.h];

                v[r.h] = v[r.h] +% (math.rotr(u32, v[r.a], @as(u32, 2)) ^ math.rotr(u32, v[r.a], @as(u32, 13)) ^ math.rotr(u32, v[r.a], @as(u32, 22))) +% ((v[r.a] & (v[r.b] | v[r.c])) | (v[r.b] & v[r.c]));
            }

            for (&d.s, v) |*dv, vv| dv.* +%= vv;
        }
    };
}

const RoundParam256 = struct {
    a: usize,
    b: usize,
    c: usize,
    d: usize,
    e: usize,
    f: usize,
    g: usize,
    h: usize,
    i: usize,
};

fn roundParam256(a: usize, b: usize, c: usize, d: usize, e: usize, f: usize, g: usize, h: usize, i: usize) RoundParam256 {
    return RoundParam256{
        .a = a,
        .b = b,
        .c = c,
        .d = d,
        .e = e,
        .f = f,
        .g = g,
        .h = h,
        .i = i,
    };
}

const asm_supported = builtin.zig_backend != .stage2_c and builtin.zig_backend != .stage2_x86_64;
fn hardwareAvailable() bool {
    if (comptime !asm_supported) return false;
    if (comptime cpu.guaranteed().sha256) return true;
    return cpu.features().sha256;
}

test "SHA256 dispatch matches std across streaming padding and unaligned payloads" {
    var data: [12353]u8 = undefined;
    for (&data, 0..) |*v, i| v.* = @truncate(i *% 137 +% 17);
    for ([_]usize{ 0, 1, 7, 31, 55, 56, 63, 64, 65, 119, 120, 127, 128, 3072, 12288 }) |len| {
        const bytes = data[1..][0..len];
        var expected: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &expected, .{});
        for ([_]bool{ false, true }) |portable| for ([_]usize{ 1, 7, 64, 777, 12288 }) |step| {
            var hash = RuntimeSha256.init(.{ .portable = portable });
            var offset: usize = 0;
            while (offset < len) {
                const end = @min(len, offset + step);
                hash.update(bytes[offset..end]);
                offset = end;
            }
            try std.testing.expectEqualSlices(u8, &expected, &hash.finalResult());
        };
    }
}

test "SHA256 optional instruction path is exercised when present" {
    if (!hardwareAvailable()) return error.SkipZigTest;
    var expected: [32]u8 = undefined;
    var actual: [32]u8 = undefined;
    const bytes = [_]u8{0x71} ** 8193;
    RuntimeSha256.hash(&bytes, &expected, .{ .portable = true });
    RuntimeSha256.hash(&bytes, &actual, .{});
    try std.testing.expectEqualSlices(u8, &expected, &actual);
}
