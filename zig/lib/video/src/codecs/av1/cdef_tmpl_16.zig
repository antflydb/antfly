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
pub const __builtin = common.__builtin;
pub extern fn antfly_codec_abs(c_int) c_int;
pub const ptrdiff_t = common.ptrdiff_t;
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

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
pub fn umin(a: c_uint, b: c_uint) callconv(.c) c_uint {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub fn iclip(v: c_int, min: c_int, max: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &min;
    _ = &max;
    return if (v < min) min else if (v > max) max else v;
}
pub fn apply_sign(v: c_int, s: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(c_int, 0)) -v else v;
}
pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
}
pub const pixel = u16;
pub fn PXSTRIDE(x: ptrdiff_t) callconv(.c) ptrdiff_t {
    _ = &x;
    while (true) {
        if (!!((x & @as(ptrdiff_t, 1)) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    return x >> @intCast(@as(ptrdiff_t, 1));
}
pub const CDEF_HAVE_LEFT = common.CDEF_HAVE_LEFT;
pub const CDEF_HAVE_RIGHT = common.CDEF_HAVE_RIGHT;
pub const CDEF_HAVE_TOP = common.CDEF_HAVE_TOP;
pub const CDEF_HAVE_BOTTOM = common.CDEF_HAVE_BOTTOM;
pub const enum_CdefEdgeFlags = common.enum_CdefEdgeFlags;
pub const const_left_pixel_row_2px = [*c][2]pixel;
pub const cdef_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, left: const_left_pixel_row_2px, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void;
pub const cdef_dir_fn = ?*const fn (dst: [*c]const pixel, dst_stride: ptrdiff_t, @"var": [*c]c_uint, bitdepth_max: c_int) callconv(.c) c_int;
pub const struct_Dav1dCdefDSPContext = extern struct {
    dir: cdef_dir_fn = null,
    fb: [3]cdef_fn = @import("std").mem.zeroes([3]cdef_fn),
};
pub const Dav1dCdefDSPContext = struct_Dav1dCdefDSPContext;
pub extern var antfly_av1_dav1d_cdef_directions: [12][2]i8;
pub fn constrain(diff: c_int, threshold: c_int, shift: c_int) callconv(.c) c_int {
    _ = &diff;
    _ = &threshold;
    _ = &shift;
    const adiff: c_int = antfly_codec_abs(diff);
    _ = &adiff;
    return apply_sign(imin(adiff, imax(0, threshold - (adiff >> @intCast(shift)))), diff);
}
pub fn fill(arg_tmp: [*c]i16, stride: ptrdiff_t, w: c_int, h: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &stride;
    _ = &w;
    _ = &h;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(-@as(c_int, 32768));
                }
            }
            tmp += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        }
    }
}
pub fn padding(arg_tmp: [*c]i16, tmp_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, arg_left: [*c][2]pixel, arg_top: [*c]const pixel, arg_bottom: [*c]const pixel, w: c_int, h: c_int, edges: enum_CdefEdgeFlags) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &tmp_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    var left = arg_left;
    _ = &left;
    var top = arg_top;
    _ = &top;
    var bottom = arg_bottom;
    _ = &bottom;
    _ = &w;
    _ = &h;
    _ = &edges;
    var x_start: c_int = -@as(c_int, 2);
    _ = &x_start;
    var x_end: c_int = w + @as(c_int, 2);
    _ = &x_end;
    var y_start: c_int = -@as(c_int, 2);
    _ = &y_start;
    var y_end: c_int = h + @as(c_int, 2);
    _ = &y_end;
    if (!((edges & @as(enum_CdefEdgeFlags, CDEF_HAVE_TOP)) != 0)) {
        fill((tmp - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2)))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * tmp_stride)))), tmp_stride, w + @as(c_int, 4), 2);
        y_start = 0;
    }
    if (!((edges & @as(enum_CdefEdgeFlags, CDEF_HAVE_BOTTOM)) != 0)) {
        fill((tmp + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, h) * tmp_stride))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), tmp_stride, w + @as(c_int, 4), 2);
        y_end -= 2;
    }
    if (!((edges & @as(enum_CdefEdgeFlags, CDEF_HAVE_LEFT)) != 0)) {
        fill((tmp + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_start) * tmp_stride))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), tmp_stride, 2, y_end - y_start);
        x_start = 0;
    }
    if (!((edges & @as(enum_CdefEdgeFlags, CDEF_HAVE_RIGHT)) != 0)) {
        fill((tmp + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_start) * tmp_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(w)))), tmp_stride, 2, y_end - y_start);
        x_end -= 2;
    }
    {
        var y: c_int = y_start;
        _ = &y;
        while (y < @as(c_int, 0)) : (y += 1) {
            {
                var x: c_int = x_start;
                _ = &x;
                while (x < x_end) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, y) * tmp_stride))))] = @bitCast(@as(c_ushort, @truncate(top[@bitCast(@as(isize, @intCast(x)))])));
                }
            }
            top += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
        }
    }
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            var x: c_int = x_start;
            _ = &x;
            while (x < @as(c_int, 0)) : (x += 1) {
                tmp[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, y) * tmp_stride))))] = @bitCast(@as(c_ushort, @truncate(left[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(@as(c_int, 2) + x)))])));
            }
        }
    }
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            {
                var x: c_int = if (y < h) @as(c_int, 0) else x_start;
                _ = &x;
                while (x < x_end) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_ushort, @truncate(src[@bitCast(@as(isize, @intCast(x)))])));
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
        }
    }
    {
        var y: c_int = h;
        _ = &y;
        while (y < y_end) : (y += 1) {
            {
                var x: c_int = x_start;
                _ = &x;
                while (x < x_end) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_ushort, @truncate(bottom[@bitCast(@as(isize, @intCast(x)))])));
                }
            }
            bottom += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
        }
    }
}
pub fn cdef_filter_block_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_left: [*c][2]pixel, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, w: c_int, arg_h: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var left = arg_left;
    _ = &left;
    _ = &top;
    _ = &bottom;
    _ = &pri_strength;
    _ = &sec_strength;
    _ = &dir;
    _ = &damping;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &edges;
    _ = &bitdepth_max;
    const tmp_stride: ptrdiff_t = 12;
    _ = &tmp_stride;
    while (true) {
        if (!(((w == @as(c_int, 4)) or (w == @as(c_int, 8))) and ((h == @as(c_int, 4)) or (h == @as(c_int, 8))))) {
            unreachable;
        }
        if (!false) break;
    }
    var tmp_buf: [144]i16 = undefined;
    _ = &tmp_buf;
    var tmp: [*c]i16 = (@as([*c]i16, @ptrCast(@alignCast(&tmp_buf))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * tmp_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2)))));
    _ = &tmp;
    padding(tmp, tmp_stride, dst, dst_stride, left, top, bottom, w, h, edges);
    if (pri_strength != 0) {
        const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
        _ = &bitdepth_min_8;
        const pri_tap: c_int = @as(c_int, 4) - ((pri_strength >> @intCast(bitdepth_min_8)) & @as(c_int, 1));
        _ = &pri_tap;
        const pri_shift: c_int = imax(0, damping - ulog2(@bitCast(@as(c_int, pri_strength))));
        _ = &pri_shift;
        if (sec_strength != 0) {
            const sec_shift: c_int = damping - ulog2(@bitCast(@as(c_int, sec_strength)));
            _ = &sec_shift;
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        const px: c_int = dst[@bitCast(@as(isize, @intCast(x)))];
                        _ = &px;
                        var sum: c_int = 0;
                        _ = &sum;
                        var max: c_int = px;
                        _ = &max;
                        var min: c_int = px;
                        _ = &min;
                        var pri_tap_k: c_int = pri_tap;
                        _ = &pri_tap_k;
                        {
                            var k: c_int = 0;
                            _ = &k;
                            while (k < @as(c_int, 2)) : (k += 1) {
                                const off1: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 2))))][@bitCast(@as(isize, @intCast(k)))];
                                _ = &off1;
                                const p0: c_int = tmp[@bitCast(@as(isize, @intCast(x + off1)))];
                                _ = &p0;
                                const p1: c_int = tmp[@bitCast(@as(isize, @intCast(x - off1)))];
                                _ = &p1;
                                sum += pri_tap_k * constrain(p0 - px, pri_strength, pri_shift);
                                sum += pri_tap_k * constrain(p1 - px, pri_strength, pri_shift);
                                pri_tap_k = (pri_tap_k & @as(c_int, 3)) | @as(c_int, 2);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, p0)), @bitCast(@as(c_int, min))))));
                                max = imax(p0, max);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, p1)), @bitCast(@as(c_int, min))))));
                                max = imax(p1, max);
                                const off2: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 4))))][@bitCast(@as(isize, @intCast(k)))];
                                _ = &off2;
                                const off3: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 0))))][@bitCast(@as(isize, @intCast(k)))];
                                _ = &off3;
                                const s0: c_int = tmp[@bitCast(@as(isize, @intCast(x + off2)))];
                                _ = &s0;
                                const s1: c_int = tmp[@bitCast(@as(isize, @intCast(x - off2)))];
                                _ = &s1;
                                const s2: c_int = tmp[@bitCast(@as(isize, @intCast(x + off3)))];
                                _ = &s2;
                                const s3: c_int = tmp[@bitCast(@as(isize, @intCast(x - off3)))];
                                _ = &s3;
                                const sec_tap: c_int = @as(c_int, 2) - k;
                                _ = &sec_tap;
                                sum += sec_tap * constrain(s0 - px, sec_strength, sec_shift);
                                sum += sec_tap * constrain(s1 - px, sec_strength, sec_shift);
                                sum += sec_tap * constrain(s2 - px, sec_strength, sec_shift);
                                sum += sec_tap * constrain(s3 - px, sec_strength, sec_shift);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, s0)), @bitCast(@as(c_int, min))))));
                                max = imax(s0, max);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, s1)), @bitCast(@as(c_int, min))))));
                                max = imax(s1, max);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, s2)), @bitCast(@as(c_int, min))))));
                                max = imax(s2, max);
                                min = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, s3)), @bitCast(@as(c_int, min))))));
                                max = imax(s3, max);
                            }
                        }
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip(px + (((sum - @as(c_int, @intFromBool(sum < @as(c_int, 0)))) + @as(c_int, 8)) >> @intCast(@as(c_int, 4))), min, max))));
                    }
                }
                dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
                tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        } else {
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        const px: c_int = dst[@bitCast(@as(isize, @intCast(x)))];
                        _ = &px;
                        var sum: c_int = 0;
                        _ = &sum;
                        var pri_tap_k: c_int = pri_tap;
                        _ = &pri_tap_k;
                        {
                            var k: c_int = 0;
                            _ = &k;
                            while (k < @as(c_int, 2)) : (k += 1) {
                                const off: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 2))))][@bitCast(@as(isize, @intCast(k)))];
                                _ = &off;
                                const p0: c_int = tmp[@bitCast(@as(isize, @intCast(x + off)))];
                                _ = &p0;
                                const p1: c_int = tmp[@bitCast(@as(isize, @intCast(x - off)))];
                                _ = &p1;
                                sum += pri_tap_k * constrain(p0 - px, pri_strength, pri_shift);
                                sum += pri_tap_k * constrain(p1 - px, pri_strength, pri_shift);
                                pri_tap_k = (pri_tap_k & @as(c_int, 3)) | @as(c_int, 2);
                            }
                        }
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(px + (((sum - @as(c_int, @intFromBool(sum < @as(c_int, 0)))) + @as(c_int, 8)) >> @intCast(@as(c_int, 4))))));
                    }
                }
                dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
                tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        }
    } else {
        while (true) {
            if (!(sec_strength != 0)) {
                unreachable;
            }
            if (!false) break;
        }
        const sec_shift: c_int = damping - ulog2(@bitCast(@as(c_int, sec_strength)));
        _ = &sec_shift;
        while (true) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    const px: c_int = dst[@bitCast(@as(isize, @intCast(x)))];
                    _ = &px;
                    var sum: c_int = 0;
                    _ = &sum;
                    {
                        var k: c_int = 0;
                        _ = &k;
                        while (k < @as(c_int, 2)) : (k += 1) {
                            const off1: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 4))))][@bitCast(@as(isize, @intCast(k)))];
                            _ = &off1;
                            const off2: c_int = antfly_av1_dav1d_cdef_directions[@bitCast(@as(isize, @intCast(dir + @as(c_int, 0))))][@bitCast(@as(isize, @intCast(k)))];
                            _ = &off2;
                            const s0: c_int = tmp[@bitCast(@as(isize, @intCast(x + off1)))];
                            _ = &s0;
                            const s1: c_int = tmp[@bitCast(@as(isize, @intCast(x - off1)))];
                            _ = &s1;
                            const s2: c_int = tmp[@bitCast(@as(isize, @intCast(x + off2)))];
                            _ = &s2;
                            const s3: c_int = tmp[@bitCast(@as(isize, @intCast(x - off2)))];
                            _ = &s3;
                            const sec_tap: c_int = @as(c_int, 2) - k;
                            _ = &sec_tap;
                            sum += sec_tap * constrain(s0 - px, sec_strength, sec_shift);
                            sum += sec_tap * constrain(s1 - px, sec_strength, sec_shift);
                            sum += sec_tap * constrain(s2 - px, sec_strength, sec_shift);
                            sum += sec_tap * constrain(s3 - px, sec_strength, sec_shift);
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(px + (((sum - @as(c_int, @intFromBool(sum < @as(c_int, 0)))) + @as(c_int, 8)) >> @intCast(@as(c_int, 4))))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
            tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
            if (!((blk: {
                const ref = &h;
                ref.* -= 1;
                break :blk ref.*;
            }) != 0)) break;
        }
    }
}
pub fn cdef_filter_block_4x4_c(dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][2]pixel, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    _ = &top;
    _ = &bottom;
    _ = &pri_strength;
    _ = &sec_strength;
    _ = &dir;
    _ = &damping;
    _ = &edges;
    _ = &bitdepth_max;
    cdef_filter_block_c(dst, stride, left, top, bottom, pri_strength, sec_strength, dir, damping, 4, 4, edges, bitdepth_max);
}
pub fn cdef_filter_block_4x8_c(dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][2]pixel, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    _ = &top;
    _ = &bottom;
    _ = &pri_strength;
    _ = &sec_strength;
    _ = &dir;
    _ = &damping;
    _ = &edges;
    _ = &bitdepth_max;
    cdef_filter_block_c(dst, stride, left, top, bottom, pri_strength, sec_strength, dir, damping, 4, 8, edges, bitdepth_max);
}
pub fn cdef_filter_block_8x8_c(dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][2]pixel, top: [*c]const pixel, bottom: [*c]const pixel, pri_strength: c_int, sec_strength: c_int, dir: c_int, damping: c_int, edges: enum_CdefEdgeFlags, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    _ = &top;
    _ = &bottom;
    _ = &pri_strength;
    _ = &sec_strength;
    _ = &dir;
    _ = &damping;
    _ = &edges;
    _ = &bitdepth_max;
    cdef_filter_block_c(dst, stride, left, top, bottom, pri_strength, sec_strength, dir, damping, 8, 8, edges, bitdepth_max);
}
pub fn cdef_find_dir_c(arg_img: [*c]const pixel, stride: ptrdiff_t, @"var": [*c]c_uint, bitdepth_max: c_int) callconv(.c) c_int {
    var img = arg_img;
    _ = &img;
    _ = &stride;
    _ = &@"var";
    _ = &bitdepth_max;
    const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    var partial_sum_hv: [2][8]c_int = [1][8]c_int{
        [1]c_int{
            0,
        } ++ @as([7]c_int, @splat(0)),
    } ++ @as([1][8]c_int, @splat(@import("std").mem.zeroes([8]c_int)));
    _ = &partial_sum_hv;
    var partial_sum_diag: [2][15]c_int = [1][15]c_int{
        [1]c_int{
            0,
        } ++ @as([14]c_int, @splat(0)),
    } ++ @as([1][15]c_int, @splat(@import("std").mem.zeroes([15]c_int)));
    _ = &partial_sum_diag;
    var partial_sum_alt: [4][11]c_int = [1][11]c_int{
        [1]c_int{
            0,
        } ++ @as([10]c_int, @splat(0)),
    } ++ @as([3][11]c_int, @splat(@import("std").mem.zeroes([11]c_int)));
    _ = &partial_sum_alt;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 8)) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < @as(c_int, 8)) : (x += 1) {
                    const px: c_int = (@as(c_int, img[@bitCast(@as(isize, @intCast(x)))]) >> @intCast(bitdepth_min_8)) - @as(c_int, 128);
                    _ = &px;
                    partial_sum_diag[@as(c_int, 0)][@bitCast(@as(isize, @intCast(y + x)))] += px;
                    partial_sum_alt[@as(c_int, 0)][@bitCast(@as(isize, @intCast(y + (x >> @intCast(@as(c_int, 1))))))] += px;
                    partial_sum_hv[@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))] += px;
                    partial_sum_alt[@as(c_int, 1)][@bitCast(@as(isize, @intCast((@as(c_int, 3) + y) - (x >> @intCast(@as(c_int, 1))))))] += px;
                    partial_sum_diag[@as(c_int, 1)][@bitCast(@as(isize, @intCast((@as(c_int, 7) + y) - x)))] += px;
                    partial_sum_alt[@as(c_int, 2)][@bitCast(@as(isize, @intCast((@as(c_int, 3) - (y >> @intCast(@as(c_int, 1)))) + x)))] += px;
                    partial_sum_hv[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))] += px;
                    partial_sum_alt[@as(c_int, 3)][@bitCast(@as(isize, @intCast((y >> @intCast(@as(c_int, 1))) + x)))] += px;
                }
            }
            img += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
    var cost: [8]c_uint = [1]c_uint{
        0,
    } ++ @as([7]c_uint, @splat(0));
    _ = &cost;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 8)) : (n += 1) {
            cost[@as(c_int, 2)] +%= @bitCast(@as(c_int, partial_sum_hv[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))] * partial_sum_hv[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))]));
            cost[@as(c_int, 6)] +%= @bitCast(@as(c_int, partial_sum_hv[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))] * partial_sum_hv[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))]));
        }
    }
    cost[@as(c_int, 2)] *%= 105;
    cost[@as(c_int, 6)] *%= 105;
    const static_local_div_table = struct {
        const div_table: [7]u16 = [7]u16{
            840,
            420,
            280,
            210,
            168,
            140,
            120,
        };
    };
    _ = &static_local_div_table;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 7)) : (n += 1) {
            const d: c_int = static_local_div_table.div_table[@bitCast(@as(isize, @intCast(n)))];
            _ = &d;
            cost[@as(c_int, 0)] +%= @bitCast(@as(c_int, ((partial_sum_diag[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))] * partial_sum_diag[@as(c_int, 0)][@bitCast(@as(isize, @intCast(n)))]) + (partial_sum_diag[@as(c_int, 0)][@bitCast(@as(isize, @intCast(@as(c_int, 14) - n)))] * partial_sum_diag[@as(c_int, 0)][@bitCast(@as(isize, @intCast(@as(c_int, 14) - n)))])) * d));
            cost[@as(c_int, 4)] +%= @bitCast(@as(c_int, ((partial_sum_diag[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))] * partial_sum_diag[@as(c_int, 1)][@bitCast(@as(isize, @intCast(n)))]) + (partial_sum_diag[@as(c_int, 1)][@bitCast(@as(isize, @intCast(@as(c_int, 14) - n)))] * partial_sum_diag[@as(c_int, 1)][@bitCast(@as(isize, @intCast(@as(c_int, 14) - n)))])) * d));
        }
    }
    cost[@as(c_int, 0)] +%= @bitCast(@as(c_int, (partial_sum_diag[@as(c_int, 0)][@as(c_int, 7)] * partial_sum_diag[@as(c_int, 0)][@as(c_int, 7)]) * @as(c_int, 105)));
    cost[@as(c_int, 4)] +%= @bitCast(@as(c_int, (partial_sum_diag[@as(c_int, 1)][@as(c_int, 7)] * partial_sum_diag[@as(c_int, 1)][@as(c_int, 7)]) * @as(c_int, 105)));
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 4)) : (n += 1) {
            const cost_ptr: [*c]c_uint = &cost[@bitCast(@as(isize, @intCast((n * @as(c_int, 2)) + @as(c_int, 1))))];
            _ = &cost_ptr;
            {
                var m: c_int = 0;
                _ = &m;
                while (m < @as(c_int, 5)) : (m += 1) {
                    cost_ptr.* +%= @bitCast(@as(c_int, partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(@as(c_int, 3) + m)))] * partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(@as(c_int, 3) + m)))]));
                }
            }
            cost_ptr.* *%= 105;
            {
                var m: c_int = 0;
                _ = &m;
                while (m < @as(c_int, 3)) : (m += 1) {
                    const d: c_int = static_local_div_table.div_table[@bitCast(@as(isize, @intCast((@as(c_int, 2) * m) + @as(c_int, 1))))];
                    _ = &d;
                    cost_ptr.* +%= @bitCast(@as(c_int, ((partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(m)))] * partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(m)))]) + (partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(@as(c_int, 10) - m)))] * partial_sum_alt[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(@as(c_int, 10) - m)))])) * d));
                }
            }
        }
    }
    var best_dir: c_int = 0;
    _ = &best_dir;
    var best_cost: c_uint = cost[@as(c_int, 0)];
    _ = &best_cost;
    {
        var n: c_int = 1;
        _ = &n;
        while (n < @as(c_int, 8)) : (n += 1) {
            if (cost[@bitCast(@as(isize, @intCast(n)))] > best_cost) {
                best_cost = cost[@bitCast(@as(isize, @intCast(n)))];
                best_dir = n;
            }
        }
    }
    @"var".* = (best_cost -% cost[@bitCast(@as(isize, @intCast(best_dir ^ @as(c_int, 4))))]) >> @intCast(@as(c_uint, 10));
    return best_dir;
}
pub export fn antfly_av1_dav1d_cdef_dsp_init_16bpc(c: [*c]Dav1dCdefDSPContext) void {
    _ = &c;
    c.*.dir = cdef_find_dir_c;
    c.*.fb[@as(c_int, 0)] = cdef_filter_block_8x8_c;
    c.*.fb[@as(c_int, 1)] = cdef_filter_block_4x8_c;
    c.*.fb[@as(c_int, 2)] = cdef_filter_block_4x4_c;
}
