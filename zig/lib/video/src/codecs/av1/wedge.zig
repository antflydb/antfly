// Copyright © 2018-2019, VideoLAN and dav1d authors
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-2-Clause
// /*
//  * Copyright © 2018, VideoLAN and dav1d authors
//  * Copyright © 2018, Two Orioles, LLC
//  * All rights reserved.
//  *
//  * Redistribution and use in source and binary forms, with or without
//  * modification, are permitted provided that the following conditions are met:
//  *
//  * 1. Redistributions of source code must retain the above copyright notice, this
//  *    list of conditions and the following disclaimer.
//  *
//  * 2. Redistributions in binary form must reproduce the above copyright notice,
//  *    this list of conditions and the following disclaimer in the documentation
//  *    and/or other materials provided with the distribution.
//  *
//  * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub fn imax(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a > b) a else b;
}
pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub const II_DC_PRED = common.II_DC_PRED;
pub const BS_32x32 = common.BS_32x32;
pub const BS_32x16 = common.BS_32x16;
pub const BS_32x8 = common.BS_32x8;
pub const BS_16x32 = common.BS_16x32;
pub const BS_16x16 = common.BS_16x16;
pub const BS_16x8 = common.BS_16x8;
pub const BS_8x32 = common.BS_8x32;
pub const BS_8x16 = common.BS_8x16;
pub const BS_8x8 = common.BS_8x8;
pub const enum_BlockSize = common.enum_BlockSize;
const struct_unnamed_24 = extern struct {
    wedge: [2][16]u16 = @import("std").mem.zeroes([2][16]u16),
    ii: [4]u16 = @import("std").mem.zeroes([4]u16),
};
pub const Dav1dMasks = extern struct {
    offsets: [3][11]struct_unnamed_24 align(64) = @import("std").mem.zeroes([3][11]struct_unnamed_24),
    wedge_444_32x32: [16384]u8 align(64) = @import("std").mem.zeroes([16384]u8),
    wedge_444_32x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_444_32x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_16x32: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_444_16x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_16x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_444_8x32: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_8x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_444_8x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_422_16x32: [16384]u8 = @import("std").mem.zeroes([16384]u8),
    wedge_422_16x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_422_16x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_8x32: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_422_8x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_8x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_422_4x32: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_4x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_422_4x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_16x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_420_16x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_420_16x4: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_8x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_420_8x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_8x4: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_4x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_4x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_4x4: [512]u8 = @import("std").mem.zeroes([512]u8),
    ii_dc: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    ii_nondc_32x32: [3072]u8 = @import("std").mem.zeroes([3072]u8),
    ii_nondc_16x32: [1536]u8 = @import("std").mem.zeroes([1536]u8),
    ii_nondc_16x16: [768]u8 = @import("std").mem.zeroes([768]u8),
    ii_nondc_8x32: [768]u8 = @import("std").mem.zeroes([768]u8),
    ii_nondc_8x16: [384]u8 = @import("std").mem.zeroes([384]u8),
    ii_nondc_8x8: [192]u8 = @import("std").mem.zeroes([192]u8),
    ii_nondc_4x16: [192]u8 = @import("std").mem.zeroes([192]u8),
    ii_nondc_4x8: [96]u8 = @import("std").mem.zeroes([96]u8),
    ii_nondc_4x4: [48]u8 = @import("std").mem.zeroes([48]u8),
};
pub const WEDGE_HORIZONTAL: c_int = 0;
pub const WEDGE_VERTICAL: c_int = 1;
pub const WEDGE_OBLIQUE27: c_int = 2;
pub const WEDGE_OBLIQUE63: c_int = 3;
pub const WEDGE_OBLIQUE117: c_int = 4;
pub const WEDGE_OBLIQUE153: c_int = 5;
pub const wedge_code_type = extern struct {
    direction: u8 = 0,
    x_offset: u8 = 0,
    y_offset: u8 = 0,
};
pub const wedge_codebook_16_hgtw: [16]wedge_code_type = [16]wedge_code_type{
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 6,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 6,
        .y_offset = 4,
    },
};
pub const wedge_codebook_16_hltw: [16]wedge_code_type = [16]wedge_code_type{
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 6,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 6,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 6,
        .y_offset = 4,
    },
};
pub const wedge_codebook_16_heqw: [16]wedge_code_type = [16]wedge_code_type{
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_HORIZONTAL,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_VERTICAL,
        .x_offset = 6,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE27,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 2,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE153,
        .x_offset = 4,
        .y_offset = 6,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE63,
        .x_offset = 6,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 2,
        .y_offset = 4,
    },
    wedge_code_type{
        .direction = WEDGE_OBLIQUE117,
        .x_offset = 6,
        .y_offset = 4,
    },
};
pub export var antfly_av1_dav1d_masks: Dav1dMasks = @import("std").mem.zeroes(Dav1dMasks);
pub fn insert_border(dst: [*c]u8, src: [*c]const u8, ctr: c_int) callconv(.c) void {
    _ = &dst;
    _ = &src;
    _ = &ctr;
    if (ctr > @as(c_int, 4)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), 0, @bitCast(@as(c_long, ctr - @as(c_int, 4))));
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(imax(ctr, 4)))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))))), @ptrCast(@alignCast(src + @as(usize, @bitCast(@as(isize, @intCast(imax(@as(c_int, 4) - ctr, 0))))))), @bitCast(@as(c_long, imin(@as(c_int, 64) - ctr, 8))));
    if (ctr < (@as(c_int, 64) - @as(c_int, 4))) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(ctr))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))))), 64, @bitCast(@as(c_long, (@as(c_int, 64) - @as(c_int, 4)) - ctr)));
    }
}
pub fn transpose(dst: [*c]u8, src: [*c]const u8) callconv(.c) void {
    _ = &dst;
    _ = &src;
    {
        var y: c_int = 0;
        _ = &y;
        var y_off: c_int = 0;
        _ = &y_off;
        while (y < @as(c_int, 64)) : ({
            y += 1;
            y_off += 64;
        }) {
            var x: c_int = 0;
            _ = &x;
            var x_off: c_int = 0;
            _ = &x_off;
            while (x < @as(c_int, 64)) : ({
                x += 1;
                x_off += 64;
            }) {
                dst[@bitCast(@as(isize, @intCast(x_off + y)))] = src[@bitCast(@as(isize, @intCast(y_off + x)))];
            }
        }
    }
}
pub fn hflip(dst: [*c]u8, src: [*c]const u8) callconv(.c) void {
    _ = &dst;
    _ = &src;
    {
        var y: c_int = 0;
        _ = &y;
        var y_off: c_int = 0;
        _ = &y_off;
        while (y < @as(c_int, 64)) : ({
            y += 1;
            y_off += 64;
        }) {
            var x: c_int = 0;
            _ = &x;
            while (x < @as(c_int, 64)) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(((y_off + @as(c_int, 64)) - @as(c_int, 1)) - x)))] = src[@bitCast(@as(isize, @intCast(y_off + x)))];
            }
        }
    }
}
pub fn copy2d(arg_dst: [*c]u8, arg_src: [*c]const u8, arg_sign: c_int, w: c_int, h: c_int, x_off: c_int, y_off: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var src = arg_src;
    _ = &src;
    var sign = arg_sign;
    _ = &sign;
    _ = &w;
    _ = &h;
    _ = &x_off;
    _ = &y_off;
    src += @as(usize, @bitCast(@as(isize, @intCast((y_off * @as(c_int, 64)) + x_off))));
    if (sign != 0) {
        {
            var y: c_int = 0;
            _ = &y;
            while (y < h) : (y += 1) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(@as(c_int, 64) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]))));
                    }
                }
                src += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 64)))));
                dst += @as(usize, @bitCast(@as(isize, @intCast(w))));
            }
        }
    } else {
        {
            var y: c_int = 0;
            _ = &y;
            while (y < h) : (y += 1) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @bitCast(@as(c_long, w)));
                src += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 64)))));
                dst += @as(usize, @bitCast(@as(isize, @intCast(w))));
            }
        }
    }
}
pub fn init_chroma(arg_chroma: [*c]u8, arg_luma: [*c]const u8, sign: c_int, w: c_int, h: c_int, ss_ver: c_int) callconv(.c) u16 {
    var chroma = arg_chroma;
    _ = &chroma;
    var luma = arg_luma;
    _ = &luma;
    _ = &sign;
    _ = &w;
    _ = &h;
    _ = &ss_ver;
    const offset: u16 = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(chroma))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
    _ = &offset;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += @as(c_int, 1) + ss_ver) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 2) {
                    var sum: c_int = (@as(c_int, luma[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, luma[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))])) + @as(c_int, 1);
                    _ = &sum;
                    if (ss_ver != 0) {
                        sum += (@as(c_int, luma[@bitCast(@as(isize, @intCast(w + x)))]) + @as(c_int, luma[@bitCast(@as(isize, @intCast((w + x) + @as(c_int, 1))))])) + @as(c_int, 1);
                    }
                    chroma[@bitCast(@as(isize, @intCast(x >> @intCast(@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate((sum - sign) >> @intCast(@as(c_int, 1) + ss_ver))));
                }
            }
            luma += @as(usize, @bitCast(@as(isize, @intCast(w << @intCast(ss_ver)))));
            chroma += @as(usize, @bitCast(@as(isize, @intCast(w >> @intCast(@as(c_int, 1))))));
        }
    }
    return offset;
}
pub fn fill2d_16x2(w: c_int, h: c_int, bs: enum_BlockSize, master: [*c][4096]u8, cb: [*c]const wedge_code_type, arg_masks_444: [*c]u8, arg_masks_422: [*c]u8, arg_masks_420: [*c]u8, arg_signs: c_uint) callconv(.c) void {
    _ = &w;
    _ = &h;
    _ = &bs;
    _ = &master;
    _ = &cb;
    var masks_444 = arg_masks_444;
    _ = &masks_444;
    var masks_422 = arg_masks_422;
    _ = &masks_422;
    var masks_420 = arg_masks_420;
    _ = &masks_420;
    var signs = arg_signs;
    _ = &signs;
    const n_stride_444: c_int = w * h;
    _ = &n_stride_444;
    const n_stride_422: c_int = n_stride_444 >> @intCast(@as(c_int, 1));
    _ = &n_stride_422;
    const n_stride_420: c_int = n_stride_444 >> @intCast(@as(c_int, 2));
    _ = &n_stride_420;
    const sign_stride_422: c_int = @as(c_int, 16) * n_stride_422;
    _ = &sign_stride_422;
    const sign_stride_420: c_int = @as(c_int, 16) * n_stride_420;
    _ = &sign_stride_420;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 16)) : (n += 1) {
            const sign: c_int = @bitCast(@as(c_uint, @truncate(signs & @as(c_uint, 1))));
            _ = &sign;
            copy2d(masks_444, @ptrCast(@alignCast(&master[cb[@bitCast(@as(isize, @intCast(n)))].direction])), sign, w, h, @as(c_int, 32) - ((w * @as(c_int, cb[@bitCast(@as(isize, @intCast(n)))].x_offset)) >> @intCast(@as(c_int, 3))), @as(c_int, 32) - ((h * @as(c_int, cb[@bitCast(@as(isize, @intCast(n)))].y_offset)) >> @intCast(@as(c_int, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][bs].wedge[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))] = blk: {
                const tmp = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(masks_444))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
                antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][bs].wedge[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))] = tmp;
                break :blk tmp;
            };
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][bs].wedge[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))] = init_chroma(&masks_422[@bitCast(@as(isize, @intCast(sign * sign_stride_422)))], masks_444, 0, w, h, 0);
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][bs].wedge[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))] = init_chroma(&masks_422[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(sign != 0))) * sign_stride_422)))], masks_444, 1, w, h, 0);
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][bs].wedge[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))] = init_chroma(&masks_420[@bitCast(@as(isize, @intCast(sign * sign_stride_420)))], masks_444, 0, w, h, 1);
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][bs].wedge[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))] = init_chroma(&masks_420[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(sign != 0))) * sign_stride_420)))], masks_444, 1, w, h, 1);
            signs >>= @intCast(@as(c_uint, 1));
            masks_444 += @as(usize, @bitCast(@as(isize, @intCast(n_stride_444))));
            masks_422 += @as(usize, @bitCast(@as(isize, @intCast(n_stride_422))));
            masks_420 += @as(usize, @bitCast(@as(isize, @intCast(n_stride_420))));
        }
    }
}
pub fn build_nondc_ii_masks(mask_v: [*c]u8, w: c_int, h: c_int, step: c_int) callconv(.c) void {
    _ = &mask_v;
    _ = &w;
    _ = &h;
    _ = &step;
    const static_local_ii_weights_1d = struct {
        const ii_weights_1d: [32]u8 = [32]u8{
            60,
            52,
            45,
            39,
            34,
            30,
            26,
            22,
            19,
            17,
            15,
            13,
            11,
            10,
            8,
            7,
            6,
            6,
            5,
            4,
            4,
            3,
            3,
            2,
            2,
            2,
            2,
            1,
            1,
            1,
            1,
            1,
        };
    };
    _ = &static_local_ii_weights_1d;
    const mask_h: [*c]u8 = &mask_v[@bitCast(@as(isize, @intCast(w * h)))];
    _ = &mask_h;
    const mask_sm: [*c]u8 = &mask_h[@bitCast(@as(isize, @intCast(w * h)))];
    _ = &mask_sm;
    {
        var y: c_int = 0;
        _ = &y;
        var off: c_int = 0;
        _ = &off;
        while (y < h) : ({
            y += 1;
            off += w;
        }) {
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&mask_v[@bitCast(@as(isize, @intCast(off)))])), static_local_ii_weights_1d.ii_weights_1d[@bitCast(@as(isize, @intCast(y * step)))], @bitCast(@as(c_long, w)));
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    mask_sm[@bitCast(@as(isize, @intCast(off + x)))] = static_local_ii_weights_1d.ii_weights_1d[@bitCast(@as(isize, @intCast(imin(x, y) * step)))];
                    mask_h[@bitCast(@as(isize, @intCast(off + x)))] = static_local_ii_weights_1d.ii_weights_1d[@bitCast(@as(isize, @intCast(x * step)))];
                }
            }
        }
    }
}
pub export fn antfly_av1_dav1d_init_ii_wedge_masks() void {
    const WEDGE_MASTER_LINE_ODD: c_int = 0;
    _ = &WEDGE_MASTER_LINE_ODD;
    const WEDGE_MASTER_LINE_EVEN: c_int = 1;
    _ = &WEDGE_MASTER_LINE_EVEN;
    const WEDGE_MASTER_LINE_VERT: c_int = 2;
    _ = &WEDGE_MASTER_LINE_VERT;
    const N_WEDGE_MASTER_LINES: c_int = 3;
    _ = &N_WEDGE_MASTER_LINES;
    const enum_WedgeMasterLineType = c_uint;
    _ = &enum_WedgeMasterLineType;
    const static_local_wedge_master_border = struct {
        var wedge_master_border: [3][8]u8 = [3][8]u8{
            [8]u8{
                1,
                2,
                6,
                18,
                37,
                53,
                60,
                63,
            },
            [8]u8{
                1,
                4,
                11,
                27,
                46,
                58,
                62,
                63,
            },
            [8]u8{
                0,
                2,
                7,
                21,
                43,
                57,
                62,
                64,
            },
        };
    };
    _ = &static_local_wedge_master_border;
    var master: [6][4096]u8 = undefined;
    _ = &master;
    {
        var y: c_int = 0;
        _ = &y;
        var off: c_int = 0;
        _ = &off;
        while (y < @as(c_int, 64)) : ({
            y += 1;
            off += 64;
        }) {
            insert_border(&master[WEDGE_VERTICAL][@bitCast(@as(isize, @intCast(off)))], @ptrCast(@alignCast(&static_local_wedge_master_border.wedge_master_border[WEDGE_MASTER_LINE_VERT])), 32);
        }
    }
    {
        var y: c_int = 0;
        _ = &y;
        var off: c_int = 0;
        _ = &off;
        var ctr: c_int = 48;
        _ = &ctr;
        while (y < @as(c_int, 64)) : ({
            y += 2;
            off += 128;
            ctr -= 1;
        }) {
            insert_border(&master[WEDGE_OBLIQUE63][@bitCast(@as(isize, @intCast(off)))], @ptrCast(@alignCast(&static_local_wedge_master_border.wedge_master_border[WEDGE_MASTER_LINE_EVEN])), ctr);
            insert_border(&master[WEDGE_OBLIQUE63][@bitCast(@as(isize, @intCast(off + @as(c_int, 64))))], @ptrCast(@alignCast(&static_local_wedge_master_border.wedge_master_border[WEDGE_MASTER_LINE_ODD])), ctr - @as(c_int, 1));
        }
    }
    transpose(@ptrCast(@alignCast(&master[WEDGE_OBLIQUE27])), @ptrCast(@alignCast(&master[WEDGE_OBLIQUE63])));
    transpose(@ptrCast(@alignCast(&master[WEDGE_HORIZONTAL])), @ptrCast(@alignCast(&master[WEDGE_VERTICAL])));
    hflip(@ptrCast(@alignCast(&master[WEDGE_OBLIQUE117])), @ptrCast(@alignCast(&master[WEDGE_OBLIQUE63])));
    hflip(@ptrCast(@alignCast(&master[WEDGE_OBLIQUE153])), @ptrCast(@alignCast(&master[WEDGE_OBLIQUE27])));
    fill2d_16x2(32, 32, @bitCast(BS_32x32 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_heqw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_32x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_16x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_16x16)), 31739);
    fill2d_16x2(32, 16, @bitCast(BS_32x16 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hltw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_32x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_16x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_16x8)), 31723);
    fill2d_16x2(32, 8, @bitCast(BS_32x8 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hltw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_32x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_16x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_16x4)), 27627);
    fill2d_16x2(16, 32, @bitCast(BS_16x32 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hgtw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_16x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_8x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_8x16)), 31723);
    fill2d_16x2(16, 16, @bitCast(BS_16x16 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_heqw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_16x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_8x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_8x8)), 31739);
    fill2d_16x2(16, 8, @bitCast(BS_16x8 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hltw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_16x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_8x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_8x4)), 31723);
    fill2d_16x2(8, 32, @bitCast(BS_8x32 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hgtw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_8x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_4x32)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_4x16)), 31467);
    fill2d_16x2(8, 16, @bitCast(BS_8x16 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_hgtw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_8x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_4x16)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_4x8)), 31723);
    fill2d_16x2(8, 8, @bitCast(BS_8x8 - BS_32x32), @ptrCast(@alignCast(&master)), @ptrCast(@alignCast(&wedge_codebook_16_heqw)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_444_8x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_422_4x8)), @ptrCast(@alignCast(&antfly_av1_dav1d_masks.wedge_420_4x4)), 31739);
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_dc))))), 32, @bitCast(@as(c_long, @as(c_int, 32) * @as(c_int, 32))));
    {
        var c: c_int = 0;
        _ = &c;
        while (c < @as(c_int, 3)) : (c += 1) {
            antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_32x32 - BS_32x32].ii[II_DC_PRED] = blk: {
                const tmp = blk_1: {
                    const tmp_2 = blk_2: {
                        const tmp_3 = blk_3: {
                            const tmp_4 = blk_4: {
                                const tmp_5 = blk_5: {
                                    const tmp_6 = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(@as([*c]u8, @ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_dc)))))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
                                    antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_8x8 - BS_32x32].ii[II_DC_PRED] = tmp_6;
                                    break :blk_5 tmp_6;
                                };
                                antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_8x16 - BS_32x32].ii[II_DC_PRED] = tmp_5;
                                break :blk_4 tmp_5;
                            };
                            antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_16x8 - BS_32x32].ii[II_DC_PRED] = tmp_4;
                            break :blk_3 tmp_4;
                        };
                        antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_16x16 - BS_32x32].ii[II_DC_PRED] = tmp_3;
                        break :blk_2 tmp_3;
                    };
                    antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_16x32 - BS_32x32].ii[II_DC_PRED] = tmp_2;
                    break :blk_1 tmp_2;
                };
                antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(c)))][BS_32x16 - BS_32x32].ii[II_DC_PRED] = tmp;
                break :blk tmp;
            };
        }
    }
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_32x32)), 32, 32, 1);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_16x32)), 16, 32, 1);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_16x16)), 16, 16, 2);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_8x32)), 8, 32, 1);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_8x16)), 8, 16, 2);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_8x8)), 8, 8, 4);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_4x16)), 4, 16, 2);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_4x8)), 4, 8, 4);
    build_nondc_ii_masks(@ptrCast(@alignCast(&antfly_av1_dav1d_masks.ii_nondc_4x4)), 4, 4, 8);
    {
        var p: c_int = 0;
        _ = &p;
        while (p < @as(c_int, 3)) : (p += 1) {
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_32x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_32x32[@bitCast(@as(isize, @intCast((p * @as(c_int, 32)) * @as(c_int, 32))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_32x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x32[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 32))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_32x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_32x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_32x32[@bitCast(@as(isize, @intCast((p * @as(c_int, 32)) * @as(c_int, 32))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_32x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_32x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_16x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x32[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 32))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_16x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x32[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 32))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_16x32 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_16x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_16x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_16x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_16x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_16x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 16)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_16x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_16x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_8x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_8x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_4x16[@bitCast(@as(isize, @intCast((p * @as(c_int, 4)) * @as(c_int, 16))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_8x16 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_4x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 4)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][BS_8x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_8x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 8)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 1)][BS_8x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_4x8[@bitCast(@as(isize, @intCast((p * @as(c_int, 4)) * @as(c_int, 8))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
            antfly_av1_dav1d_masks.offsets[@as(c_int, 2)][BS_8x8 - BS_32x32].ii[@bitCast(@as(isize, @intCast(p + @as(c_int, 1))))] = @as(u16, @truncate((@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks.ii_nondc_4x4[@bitCast(@as(isize, @intCast((p * @as(c_int, 4)) * @as(c_int, 4))))]))) -% @as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks)))) >> @intCast(@as(usize, 3))));
        }
    }
}
