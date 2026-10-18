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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const ptrdiff_t = common.ptrdiff_t;
pub fn ctz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.ctz(mask);
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
pub const pixel = u16;
pub fn pixel_set(dst: [*c]pixel, val: c_int, num: c_int) callconv(.c) void {
    _ = &dst;
    _ = &val;
    _ = &num;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < num) : (n += 1) {
            dst[@bitCast(@as(isize, @intCast(n)))] = @bitCast(@as(c_short, @truncate(val)));
        }
    }
}
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
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I422 = common.DAV1D_PIXEL_LAYOUT_I422;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const DC_PRED = common.DC_PRED;
pub const VERT_PRED = common.VERT_PRED;
pub const HOR_PRED = common.HOR_PRED;
pub const SMOOTH_PRED = common.SMOOTH_PRED;
pub const SMOOTH_V_PRED = common.SMOOTH_V_PRED;
pub const SMOOTH_H_PRED = common.SMOOTH_H_PRED;
pub const PAETH_PRED = common.PAETH_PRED;
pub const LEFT_DC_PRED = common.LEFT_DC_PRED;
pub const TOP_DC_PRED = common.TOP_DC_PRED;
pub const DC_128_PRED = common.DC_128_PRED;
pub const Z1_PRED = common.Z1_PRED;
pub const Z2_PRED = common.Z2_PRED;
pub const Z3_PRED = common.Z3_PRED;
pub const FILTER_PRED = common.FILTER_PRED;
pub const angular_ipred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, angle: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const cfl_ac_fn = ?*const fn (ac: [*c]i16, y: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void;
pub const cfl_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const pal_pred_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, pal: [*c]const pixel, idx: [*c]const u8, w: c_int, h: c_int) callconv(.c) void;
pub const struct_Dav1dIntraPredDSPContext = extern struct {
    intra_pred: [14]angular_ipred_fn = @import("std").mem.zeroes([14]angular_ipred_fn),
    cfl_ac: [3]cfl_ac_fn = @import("std").mem.zeroes([3]cfl_ac_fn),
    cfl_pred: [6]cfl_pred_fn = @import("std").mem.zeroes([6]cfl_pred_fn),
    pal_pred: pal_pred_fn = null,
};
pub const Dav1dIntraPredDSPContext = struct_Dav1dIntraPredDSPContext;
pub extern const antfly_av1_dav1d_sm_weights: [128]u8;
pub extern const antfly_av1_dav1d_dr_intra_derivative: [44]u16;
pub extern var antfly_av1_dav1d_filter_intra_taps: [5][64]i8;
pub fn splat_dc(arg_dst: [*c]pixel, stride: ptrdiff_t, width: c_int, height: c_int, dc: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &width;
    _ = &height;
    _ = &dc;
    _ = &bitdepth_max;
    while (true) {
        if (!(dc <= bitdepth_max)) {
            unreachable;
        }
        if (!false) break;
    }
    const dcN: u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, dc))) *% @as(c_ulonglong, 281479271743489);
    _ = &dcN;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : ({
                    const ref = &x;
                    ref.* = @bitCast(@as(c_uint, @truncate(@as(c_ulong, @bitCast(@as(c_long, ref.*))) +% (@sizeOf(@TypeOf(dcN)) >> @intCast(@as(c_ulong, 1))))));
                }) {
                    @as([*c]u64, @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(x)))]))).* = dcN;
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn cfl_pred(arg_dst: [*c]pixel, stride: ptrdiff_t, width: c_int, height: c_int, dc: c_int, arg_ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &width;
    _ = &height;
    _ = &dc;
    var ac = arg_ac;
    _ = &ac;
    _ = &alpha;
    _ = &bitdepth_max;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 1) {
                    const diff: c_int = alpha * @as(c_int, ac[@bitCast(@as(isize, @intCast(x)))]);
                    _ = &diff;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip(dc + apply_sign((antfly_codec_abs(diff) + @as(c_int, 32)) >> @intCast(@as(c_int, 6)), diff), 0, bitdepth_max))));
                }
            }
            ac += @as(usize, @bitCast(@as(isize, @intCast(width))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn dc_gen_top(topleft: [*c]const pixel, width: c_int) callconv(.c) c_uint {
    _ = &topleft;
    _ = &width;
    var dc: c_uint = @bitCast(@as(c_int, width >> @intCast(@as(c_int, 1))));
    _ = &dc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < width) : (i += 1) {
            dc +%= @bitCast(@as(c_int, @as(c_int, topleft[@bitCast(@as(isize, @intCast(@as(c_int, 1) + i)))])));
        }
    }
    return dc >> @intCast(@as(c_uint, @bitCast(@as(c_int, ctz(@bitCast(@as(c_int, width)))))));
}
pub fn ipred_dc_top_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    splat_dc(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc_gen_top(topleft, width)))), bitdepth_max);
}
pub fn ipred_cfl_top_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, arg_ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    var ac = arg_ac;
    _ = &ac;
    _ = &alpha;
    _ = &bitdepth_max;
    cfl_pred(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc_gen_top(topleft, width)))), ac, alpha, bitdepth_max);
}
pub fn dc_gen_left(topleft: [*c]const pixel, height: c_int) callconv(.c) c_uint {
    _ = &topleft;
    _ = &height;
    var dc: c_uint = @bitCast(@as(c_int, height >> @intCast(@as(c_int, 1))));
    _ = &dc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < height) : (i += 1) {
            dc +%= @bitCast(@as(c_int, @as(c_int, topleft[@bitCast(@as(isize, @intCast(-(@as(c_int, 1) + i))))])));
        }
    }
    return dc >> @intCast(@as(c_uint, @bitCast(@as(c_int, ctz(@bitCast(@as(c_int, height)))))));
}
pub fn ipred_dc_left_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    splat_dc(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc_gen_left(topleft, height)))), bitdepth_max);
}
pub fn ipred_cfl_left_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, arg_ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    var ac = arg_ac;
    _ = &ac;
    _ = &alpha;
    _ = &bitdepth_max;
    const dc: c_uint = dc_gen_left(topleft, height);
    _ = &dc;
    cfl_pred(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc))), ac, alpha, bitdepth_max);
}
pub fn dc_gen(topleft: [*c]const pixel, width: c_int, height: c_int) callconv(.c) c_uint {
    _ = &topleft;
    _ = &width;
    _ = &height;
    var dc: c_uint = @bitCast(@as(c_int, (width + height) >> @intCast(@as(c_int, 1))));
    _ = &dc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < width) : (i += 1) {
            dc +%= @bitCast(@as(c_int, @as(c_int, topleft[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))])));
        }
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < height) : (i += 1) {
            dc +%= @bitCast(@as(c_int, @as(c_int, topleft[@bitCast(@as(isize, @intCast(-(i + @as(c_int, 1)))))])));
        }
    }
    dc >>= @intCast(@as(c_uint, @bitCast(@as(c_int, ctz(@bitCast(@as(c_int, width + height)))))));
    if (width != height) {
        dc *%= @bitCast(@as(c_int, if ((width > (height * @as(c_int, 2))) or (height > (width * @as(c_int, 2)))) @as(c_int, 26215) else @as(c_int, 43691)));
        dc >>= @intCast(@as(c_uint, 17));
    }
    return dc;
}
pub fn ipred_dc_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    splat_dc(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc_gen(topleft, width, height)))), bitdepth_max);
}
pub fn ipred_cfl_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, arg_ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    var ac = arg_ac;
    _ = &ac;
    _ = &alpha;
    _ = &bitdepth_max;
    var dc: c_uint = dc_gen(topleft, width, height);
    _ = &dc;
    cfl_pred(dst, stride, width, height, @bitCast(@as(c_uint, @truncate(dc))), ac, alpha, bitdepth_max);
}
pub fn ipred_dc_128_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const dc: c_int = (bitdepth_max + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
    _ = &dc;
    splat_dc(dst, stride, width, height, dc, bitdepth_max);
}
pub fn ipred_cfl_128_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, arg_ac: [*c]const i16, alpha: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    var ac = arg_ac;
    _ = &ac;
    _ = &alpha;
    _ = &bitdepth_max;
    const dc: c_int = (bitdepth_max + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
    _ = &dc;
    cfl_pred(dst, stride, width, height, dc, ac, alpha, bitdepth_max);
}
pub fn ipred_v_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(topleft + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))))), @bitCast(@as(c_long, width << @intCast(@as(c_int, 1)))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn ipred_h_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            pixel_set(dst, topleft[@bitCast(@as(isize, @intCast(-(@as(c_int, 1) + y))))], width);
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn ipred_paeth_c(arg_dst: [*c]pixel, stride: ptrdiff_t, tl_ptr: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &tl_ptr;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const topleft: c_int = tl_ptr[@as(c_int, 0)];
    _ = &topleft;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            const left: c_int = tl_ptr[@bitCast(@as(isize, @intCast(-(y + @as(c_int, 1)))))];
            _ = &left;
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 1) {
                    const top: c_int = tl_ptr[@bitCast(@as(isize, @intCast(@as(c_int, 1) + x)))];
                    _ = &top;
                    const base: c_int = (left + top) - topleft;
                    _ = &base;
                    const ldiff: c_int = antfly_codec_abs(left - base);
                    _ = &ldiff;
                    const tdiff: c_int = antfly_codec_abs(top - base);
                    _ = &tdiff;
                    const tldiff: c_int = antfly_codec_abs(topleft - base);
                    _ = &tldiff;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(if ((ldiff <= tdiff) and (ldiff <= tldiff)) left else if (tdiff <= tldiff) top else topleft)));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn ipred_smooth_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const weights_hor: [*c]const u8 = &antfly_av1_dav1d_sm_weights[@bitCast(@as(isize, @intCast(width)))];
    _ = &weights_hor;
    const weights_ver: [*c]const u8 = &antfly_av1_dav1d_sm_weights[@bitCast(@as(isize, @intCast(height)))];
    _ = &weights_ver;
    const right: c_int = topleft[@bitCast(@as(isize, @intCast(width)))];
    _ = &right;
    const bottom: c_int = topleft[@bitCast(@as(isize, @intCast(-height)))];
    _ = &bottom;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 1) {
                    const pred: c_int = (((@as(c_int, weights_ver[@bitCast(@as(isize, @intCast(y)))]) * @as(c_int, topleft[@bitCast(@as(isize, @intCast(@as(c_int, 1) + x)))])) + ((@as(c_int, 256) - @as(c_int, weights_ver[@bitCast(@as(isize, @intCast(y)))])) * bottom)) + (@as(c_int, weights_hor[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, topleft[@bitCast(@as(isize, @intCast(-(@as(c_int, 1) + y))))]))) + ((@as(c_int, 256) - @as(c_int, weights_hor[@bitCast(@as(isize, @intCast(x)))])) * right);
                    _ = &pred;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((pred + @as(c_int, 256)) >> @intCast(@as(c_int, 9)))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn ipred_smooth_v_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const weights_ver: [*c]const u8 = &antfly_av1_dav1d_sm_weights[@bitCast(@as(isize, @intCast(height)))];
    _ = &weights_ver;
    const bottom: c_int = topleft[@bitCast(@as(isize, @intCast(-height)))];
    _ = &bottom;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 1) {
                    const pred: c_int = (@as(c_int, weights_ver[@bitCast(@as(isize, @intCast(y)))]) * @as(c_int, topleft[@bitCast(@as(isize, @intCast(@as(c_int, 1) + x)))])) + ((@as(c_int, 256) - @as(c_int, weights_ver[@bitCast(@as(isize, @intCast(y)))])) * bottom);
                    _ = &pred;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((pred + @as(c_int, 128)) >> @intCast(@as(c_int, 8)))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn ipred_smooth_h_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft: [*c]const pixel, width: c_int, height: c_int, a: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft;
    _ = &width;
    _ = &height;
    _ = &a;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const weights_hor: [*c]const u8 = &antfly_av1_dav1d_sm_weights[@bitCast(@as(isize, @intCast(width)))];
    _ = &weights_hor;
    const right: c_int = topleft[@bitCast(@as(isize, @intCast(width)))];
    _ = &right;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 1) {
                    const pred: c_int = (@as(c_int, weights_hor[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, topleft[@bitCast(@as(isize, @intCast(-(y + @as(c_int, 1)))))])) + ((@as(c_int, 256) - @as(c_int, weights_hor[@bitCast(@as(isize, @intCast(x)))])) * right);
                    _ = &pred;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((pred + @as(c_int, 128)) >> @intCast(@as(c_int, 8)))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub fn get_filter_strength(wh: c_int, angle: c_int, is_sm: c_int) callconv(.c) c_int {
    _ = &wh;
    _ = &angle;
    _ = &is_sm;
    if (is_sm != 0) {
        if (wh <= @as(c_int, 8)) {
            if (angle >= @as(c_int, 64)) return 2;
            if (angle >= @as(c_int, 40)) return 1;
        } else if (wh <= @as(c_int, 16)) {
            if (angle >= @as(c_int, 48)) return 2;
            if (angle >= @as(c_int, 20)) return 1;
        } else if (wh <= @as(c_int, 24)) {
            if (angle >= @as(c_int, 4)) return 3;
        } else {
            return 3;
        }
    } else {
        if (wh <= @as(c_int, 8)) {
            if (angle >= @as(c_int, 56)) return 1;
        } else if (wh <= @as(c_int, 16)) {
            if (angle >= @as(c_int, 40)) return 1;
        } else if (wh <= @as(c_int, 24)) {
            if (angle >= @as(c_int, 32)) return 3;
            if (angle >= @as(c_int, 16)) return 2;
            if (angle >= @as(c_int, 8)) return 1;
        } else if (wh <= @as(c_int, 32)) {
            if (angle >= @as(c_int, 32)) return 3;
            if (angle >= @as(c_int, 4)) return 2;
            return 1;
        } else {
            return 3;
        }
    }
    return 0;
}
pub fn filter_edge(out: [*c]pixel, sz: c_int, lim_from: c_int, lim_to: c_int, in: [*c]const pixel, from: c_int, to: c_int, strength: c_int) callconv(.c) void {
    _ = &out;
    _ = &sz;
    _ = &lim_from;
    _ = &lim_to;
    _ = &in;
    _ = &from;
    _ = &to;
    _ = &strength;
    const static_local_kernel = struct {
        var kernel: [3][5]u8 = [3][5]u8{
            [5]u8{
                0,
                4,
                8,
                4,
                0,
            },
            [5]u8{
                0,
                5,
                6,
                5,
                0,
            },
            [5]u8{
                2,
                4,
                4,
                4,
                2,
            },
        };
    };
    _ = &static_local_kernel;
    while (true) {
        if (!(strength > @as(c_int, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    var i: c_int = 0;
    _ = &i;
    while (i < imin(sz, lim_from)) : (i += 1) {
        out[@bitCast(@as(isize, @intCast(i)))] = in[@bitCast(@as(isize, @intCast(iclip(i, from, to - @as(c_int, 1)))))];
    }
    while (i < imin(lim_to, sz)) : (i += 1) {
        var s: c_int = 0;
        _ = &s;
        {
            var j: c_int = 0;
            _ = &j;
            while (j < @as(c_int, 5)) : (j += 1) {
                s += @as(c_int, in[@bitCast(@as(isize, @intCast(iclip((i - @as(c_int, 2)) + j, from, to - @as(c_int, 1)))))]) * @as(c_int, static_local_kernel.kernel[@bitCast(@as(isize, @intCast(strength - @as(c_int, 1))))][@bitCast(@as(isize, @intCast(j)))]);
            }
        }
        out[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(c_short, @truncate((s + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
    }
    while (i < sz) : (i += 1) {
        out[@bitCast(@as(isize, @intCast(i)))] = in[@bitCast(@as(isize, @intCast(iclip(i, from, to - @as(c_int, 1)))))];
    }
}
pub fn get_upsample(wh: c_int, angle: c_int, is_sm: c_int) callconv(.c) c_int {
    _ = &wh;
    _ = &angle;
    _ = &is_sm;
    return @as(c_int, @intFromBool((angle < @as(c_int, 40)) and (wh <= (@as(c_int, 16) >> @intCast(is_sm)))));
}
pub fn upsample_edge(out: [*c]pixel, hsz: c_int, in: [*c]const pixel, from: c_int, to: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &out;
    _ = &hsz;
    _ = &in;
    _ = &from;
    _ = &to;
    _ = &bitdepth_max;
    const static_local_kernel = struct {
        const kernel: [4]i8 = [4]i8{
            @truncate(-@as(c_int, 1)),
            9,
            9,
            @truncate(-@as(c_int, 1)),
        };
    };
    _ = &static_local_kernel;
    var i: c_int = undefined;
    _ = &i;
    {
        i = 0;
        while (i < (hsz - @as(c_int, 1))) : (i += 1) {
            out[@bitCast(@as(isize, @intCast(i * @as(c_int, 2))))] = in[@bitCast(@as(isize, @intCast(iclip(i, from, to - @as(c_int, 1)))))];
            var s: c_int = 0;
            _ = &s;
            {
                var j: c_int = 0;
                _ = &j;
                while (j < @as(c_int, 4)) : (j += 1) {
                    s += @as(c_int, in[@bitCast(@as(isize, @intCast(iclip((i + j) - @as(c_int, 1), from, to - @as(c_int, 1)))))]) * @as(c_int, static_local_kernel.kernel[@bitCast(@as(isize, @intCast(j)))]);
                }
            }
            out[@bitCast(@as(isize, @intCast((i * @as(c_int, 2)) + @as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(iclip((s + @as(c_int, 8)) >> @intCast(@as(c_int, 4)), 0, bitdepth_max))));
        }
    }
    out[@bitCast(@as(isize, @intCast(i * @as(c_int, 2))))] = in[@bitCast(@as(isize, @intCast(iclip(i, from, to - @as(c_int, 1)))))];
}
pub fn ipred_z1_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft_in: [*c]const pixel, width: c_int, height: c_int, arg_angle: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft_in;
    _ = &width;
    _ = &height;
    var angle = arg_angle;
    _ = &angle;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const is_sm: c_int = (angle >> @intCast(@as(c_int, 9))) & @as(c_int, 1);
    _ = &is_sm;
    const enable_intra_edge_filter: c_int = angle >> @intCast(@as(c_int, 10));
    _ = &enable_intra_edge_filter;
    angle &= 511;
    while (true) {
        if (!(angle < @as(c_int, 90))) {
            unreachable;
        }
        if (!false) break;
    }
    var dx: c_int = antfly_av1_dav1d_dr_intra_derivative[@bitCast(@as(isize, @intCast(angle >> @intCast(@as(c_int, 1)))))];
    _ = &dx;
    var top_out: [128]pixel = undefined;
    _ = &top_out;
    var top: [*c]const pixel = undefined;
    _ = &top;
    var max_base_x: c_int = undefined;
    _ = &max_base_x;
    const upsample_above: c_int = if (enable_intra_edge_filter != 0) get_upsample(width + height, @as(c_int, 90) - angle, is_sm) else @as(c_int, 0);
    _ = &upsample_above;
    if (upsample_above != 0) {
        upsample_edge(@ptrCast(@alignCast(&top_out)), width + height, &topleft_in[@as(c_int, 1)], -@as(c_int, 1), width + imin(width, height), bitdepth_max);
        top = @ptrCast(@alignCast(&top_out));
        max_base_x = (@as(c_int, 2) * (width + height)) - @as(c_int, 2);
        dx <<= @intCast(@as(c_int, 1));
    } else {
        const filter_strength: c_int = if (enable_intra_edge_filter != 0) get_filter_strength(width + height, @as(c_int, 90) - angle, is_sm) else @as(c_int, 0);
        _ = &filter_strength;
        if (filter_strength != 0) {
            filter_edge(@ptrCast(@alignCast(&top_out)), width + height, 0, width + height, &topleft_in[@as(c_int, 1)], -@as(c_int, 1), width + imin(width, height), filter_strength);
            top = @ptrCast(@alignCast(&top_out));
            max_base_x = (width + height) - @as(c_int, 1);
        } else {
            top = &topleft_in[@as(c_int, 1)];
            max_base_x = (width + imin(width, height)) - @as(c_int, 1);
        }
    }
    const base_inc: c_int = @as(c_int, 1) + upsample_above;
    _ = &base_inc;
    {
        var y: c_int = 0;
        _ = &y;
        var xpos: c_int = dx;
        _ = &xpos;
        while (y < height) : ({
            y += 1;
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
            xpos += dx;
        }) {
            const frac: c_int = xpos & @as(c_int, 62);
            _ = &frac;
            {
                var x: c_int = 0;
                _ = &x;
                var base: c_int = xpos >> @intCast(@as(c_int, 6));
                _ = &base;
                while (x < width) : ({
                    x += 1;
                    base += base_inc;
                }) {
                    if (base < max_base_x) {
                        const v: c_int = (@as(c_int, top[@bitCast(@as(isize, @intCast(base)))]) * (@as(c_int, 64) - frac)) + (@as(c_int, top[@bitCast(@as(isize, @intCast(base + @as(c_int, 1))))]) * frac);
                        _ = &v;
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((v + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
                    } else {
                        pixel_set(&dst[@bitCast(@as(isize, @intCast(x)))], top[@bitCast(@as(isize, @intCast(max_base_x)))], width - x);
                        break;
                    }
                }
            }
        }
    }
}
pub fn ipred_z2_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft_in: [*c]const pixel, width: c_int, height: c_int, arg_angle: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft_in;
    _ = &width;
    _ = &height;
    var angle = arg_angle;
    _ = &angle;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const is_sm: c_int = (angle >> @intCast(@as(c_int, 9))) & @as(c_int, 1);
    _ = &is_sm;
    const enable_intra_edge_filter: c_int = angle >> @intCast(@as(c_int, 10));
    _ = &enable_intra_edge_filter;
    angle &= 511;
    while (true) {
        if (!((angle > @as(c_int, 90)) and (angle < @as(c_int, 180)))) {
            unreachable;
        }
        if (!false) break;
    }
    var dy: c_int = antfly_av1_dav1d_dr_intra_derivative[@bitCast(@as(isize, @intCast((angle - @as(c_int, 90)) >> @intCast(@as(c_int, 1)))))];
    _ = &dy;
    var dx: c_int = antfly_av1_dav1d_dr_intra_derivative[@bitCast(@as(isize, @intCast((@as(c_int, 180) - angle) >> @intCast(@as(c_int, 1)))))];
    _ = &dx;
    const upsample_left: c_int = if (enable_intra_edge_filter != 0) get_upsample(width + height, @as(c_int, 180) - angle, is_sm) else @as(c_int, 0);
    _ = &upsample_left;
    const upsample_above: c_int = if (enable_intra_edge_filter != 0) get_upsample(width + height, angle - @as(c_int, 90), is_sm) else @as(c_int, 0);
    _ = &upsample_above;
    var edge: [129]pixel = undefined;
    _ = &edge;
    const topleft: [*c]pixel = &edge[@as(c_int, 64)];
    _ = &topleft;
    if (upsample_above != 0) {
        upsample_edge(topleft, width + @as(c_int, 1), topleft_in, 0, width + @as(c_int, 1), bitdepth_max);
        dx <<= @intCast(@as(c_int, 1));
    } else {
        const filter_strength: c_int = if (enable_intra_edge_filter != 0) get_filter_strength(width + height, angle - @as(c_int, 90), is_sm) else @as(c_int, 0);
        _ = &filter_strength;
        if (filter_strength != 0) {
            filter_edge(&topleft[@as(c_int, 1)], width, 0, max_width, &topleft_in[@as(c_int, 1)], -@as(c_int, 1), width, filter_strength);
        } else {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&topleft[@as(c_int, 1)])), @ptrCast(@alignCast(&topleft_in[@as(c_int, 1)])), @bitCast(@as(c_long, width << @intCast(@as(c_int, 1)))));
        }
    }
    if (upsample_left != 0) {
        upsample_edge(&topleft[@bitCast(@as(isize, @intCast(-height * @as(c_int, 2))))], height + @as(c_int, 1), &topleft_in[@bitCast(@as(isize, @intCast(-height)))], 0, height + @as(c_int, 1), bitdepth_max);
        dy <<= @intCast(@as(c_int, 1));
    } else {
        const filter_strength: c_int = if (enable_intra_edge_filter != 0) get_filter_strength(width + height, @as(c_int, 180) - angle, is_sm) else @as(c_int, 0);
        _ = &filter_strength;
        if (filter_strength != 0) {
            filter_edge(&topleft[@bitCast(@as(isize, @intCast(-height)))], height, height - max_height, height, &topleft_in[@bitCast(@as(isize, @intCast(-height)))], 0, height + @as(c_int, 1), filter_strength);
        } else {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&topleft[@bitCast(@as(isize, @intCast(-height)))])), @ptrCast(@alignCast(&topleft_in[@bitCast(@as(isize, @intCast(-height)))])), @bitCast(@as(c_long, height << @intCast(@as(c_int, 1)))));
        }
    }
    topleft.* = topleft_in.*;
    const base_inc_x: c_int = @as(c_int, 1) + upsample_above;
    _ = &base_inc_x;
    const left: [*c]const pixel = &topleft[@bitCast(@as(isize, @intCast(-(@as(c_int, 1) + upsample_left))))];
    _ = &left;
    {
        var y: c_int = 0;
        _ = &y;
        var xpos: c_int = ((@as(c_int, 1) + upsample_above) << @intCast(@as(c_int, 6))) - dx;
        _ = &xpos;
        while (y < height) : ({
            y += 1;
            xpos -= dx;
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }) {
            var base_x: c_int = xpos >> @intCast(@as(c_int, 6));
            _ = &base_x;
            const frac_x: c_int = xpos & @as(c_int, 62);
            _ = &frac_x;
            {
                var x: c_int = 0;
                _ = &x;
                var ypos: c_int = (y << @intCast(@as(c_int, 6) + upsample_left)) - dy;
                _ = &ypos;
                while (x < width) : ({
                    x += 1;
                    base_x += base_inc_x;
                    ypos -= dy;
                }) {
                    var v: c_int = undefined;
                    _ = &v;
                    if (base_x >= @as(c_int, 0)) {
                        v = (@as(c_int, topleft[@bitCast(@as(isize, @intCast(base_x)))]) * (@as(c_int, 64) - frac_x)) + (@as(c_int, topleft[@bitCast(@as(isize, @intCast(base_x + @as(c_int, 1))))]) * frac_x);
                    } else {
                        const base_y: c_int = ypos >> @intCast(@as(c_int, 6));
                        _ = &base_y;
                        while (true) {
                            if (!(base_y >= -(@as(c_int, 1) + upsample_left))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        const frac_y: c_int = ypos & @as(c_int, 62);
                        _ = &frac_y;
                        v = (@as(c_int, left[@bitCast(@as(isize, @intCast(-base_y)))]) * (@as(c_int, 64) - frac_y)) + (@as(c_int, left[@bitCast(@as(isize, @intCast(-(base_y + @as(c_int, 1)))))]) * frac_y);
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((v + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
                }
            }
        }
    }
}
pub fn ipred_z3_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft_in: [*c]const pixel, width: c_int, height: c_int, arg_angle: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft_in;
    _ = &width;
    _ = &height;
    var angle = arg_angle;
    _ = &angle;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    const is_sm: c_int = (angle >> @intCast(@as(c_int, 9))) & @as(c_int, 1);
    _ = &is_sm;
    const enable_intra_edge_filter: c_int = angle >> @intCast(@as(c_int, 10));
    _ = &enable_intra_edge_filter;
    angle &= 511;
    while (true) {
        if (!(angle > @as(c_int, 180))) {
            unreachable;
        }
        if (!false) break;
    }
    var dy: c_int = antfly_av1_dav1d_dr_intra_derivative[@bitCast(@as(isize, @intCast((@as(c_int, 270) - angle) >> @intCast(@as(c_int, 1)))))];
    _ = &dy;
    var left_out: [128]pixel = undefined;
    _ = &left_out;
    var left: [*c]const pixel = undefined;
    _ = &left;
    var max_base_y: c_int = undefined;
    _ = &max_base_y;
    const upsample_left: c_int = if (enable_intra_edge_filter != 0) get_upsample(width + height, angle - @as(c_int, 180), is_sm) else @as(c_int, 0);
    _ = &upsample_left;
    if (upsample_left != 0) {
        upsample_edge(@ptrCast(@alignCast(&left_out)), width + height, &topleft_in[@bitCast(@as(isize, @intCast(-(width + height))))], imax(width - height, 0), (width + height) + @as(c_int, 1), bitdepth_max);
        left = &left_out[@bitCast(@as(isize, @intCast((@as(c_int, 2) * (width + height)) - @as(c_int, 2))))];
        max_base_y = (@as(c_int, 2) * (width + height)) - @as(c_int, 2);
        dy <<= @intCast(@as(c_int, 1));
    } else {
        const filter_strength: c_int = if (enable_intra_edge_filter != 0) get_filter_strength(width + height, angle - @as(c_int, 180), is_sm) else @as(c_int, 0);
        _ = &filter_strength;
        if (filter_strength != 0) {
            filter_edge(@ptrCast(@alignCast(&left_out)), width + height, 0, width + height, &topleft_in[@bitCast(@as(isize, @intCast(-(width + height))))], imax(width - height, 0), (width + height) + @as(c_int, 1), filter_strength);
            left = &left_out[@bitCast(@as(isize, @intCast((width + height) - @as(c_int, 1))))];
            max_base_y = (width + height) - @as(c_int, 1);
        } else {
            left = &topleft_in[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            max_base_y = (height + imin(width, height)) - @as(c_int, 1);
        }
    }
    const base_inc: c_int = @as(c_int, 1) + upsample_left;
    _ = &base_inc;
    {
        var x: c_int = 0;
        _ = &x;
        var ypos: c_int = dy;
        _ = &ypos;
        while (x < width) : ({
            x += 1;
            ypos += dy;
        }) {
            const frac: c_int = ypos & @as(c_int, 62);
            _ = &frac;
            {
                var y: c_int = 0;
                _ = &y;
                var base: c_int = ypos >> @intCast(@as(c_int, 6));
                _ = &base;
                while (y < height) : ({
                    y += 1;
                    base += base_inc;
                }) {
                    if (base < max_base_y) {
                        const v: c_int = (@as(c_int, left[@bitCast(@as(isize, @intCast(-base)))]) * (@as(c_int, 64) - frac)) + (@as(c_int, left[@bitCast(@as(isize, @intCast(-(base + @as(c_int, 1)))))]) * frac);
                        _ = &v;
                        dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(stride)) + @as(ptrdiff_t, x))))] = @bitCast(@as(c_short, @truncate((v + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
                    } else {
                        while (true) {
                            dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(stride)) + @as(ptrdiff_t, x))))] = left[@bitCast(@as(isize, @intCast(-max_base_y)))];
                            if (!((blk: {
                                const ref = &y;
                                ref.* += 1;
                                break :blk ref.*;
                            }) < height)) break;
                        }
                        break;
                    }
                }
            }
        }
    }
}
pub fn ipred_filter_c(arg_dst: [*c]pixel, stride: ptrdiff_t, topleft_in: [*c]const pixel, width: c_int, height: c_int, arg_filt_idx: c_int, max_width: c_int, max_height: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &topleft_in;
    _ = &width;
    _ = &height;
    var filt_idx = arg_filt_idx;
    _ = &filt_idx;
    _ = &max_width;
    _ = &max_height;
    _ = &bitdepth_max;
    filt_idx &= 511;
    while (true) {
        if (!(filt_idx < @as(c_int, 5))) {
            unreachable;
        }
        if (!false) break;
    }
    const filter: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_filter_intra_taps[@bitCast(@as(isize, @intCast(filt_idx)))]));
    _ = &filter;
    var top: [*c]const pixel = &topleft_in[@as(c_int, 1)];
    _ = &top;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < height) : (y += 2) {
            var topleft: [*c]const pixel = &topleft_in[@bitCast(@as(isize, @intCast(-y)))];
            _ = &topleft;
            var left: [*c]const pixel = &topleft[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &left;
            var left_stride: ptrdiff_t = -@as(c_int, 1);
            _ = &left_stride;
            {
                var x: c_int = 0;
                _ = &x;
                while (x < width) : (x += 4) {
                    const p0: c_int = topleft.*;
                    _ = &p0;
                    const p1: c_int = top[@as(c_int, 0)];
                    _ = &p1;
                    const p2: c_int = top[@as(c_int, 1)];
                    _ = &p2;
                    const p3: c_int = top[@as(c_int, 2)];
                    _ = &p3;
                    const p4: c_int = top[@as(c_int, 3)];
                    _ = &p4;
                    const p5: c_int = left[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * left_stride)))];
                    _ = &p5;
                    const p6: c_int = left[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * left_stride)))];
                    _ = &p6;
                    var ptr: [*c]pixel = &dst[@bitCast(@as(isize, @intCast(x)))];
                    _ = &ptr;
                    var flt_ptr: [*c]const i8 = filter;
                    _ = &flt_ptr;
                    {
                        var yy: c_int = 0;
                        _ = &yy;
                        while (yy < @as(c_int, 2)) : (yy += 1) {
                            {
                                var xx: c_int = 0;
                                _ = &xx;
                                while (xx < @as(c_int, 4)) : ({
                                    xx += 1;
                                    flt_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
                                }) {
                                    const acc: c_int = ((((((@as(c_int, flt_ptr[@as(c_int, 0)]) * p0) + (@as(c_int, flt_ptr[@as(c_int, 8)]) * p1)) + (@as(c_int, flt_ptr[@as(c_int, 16)]) * p2)) + (@as(c_int, flt_ptr[@as(c_int, 24)]) * p3)) + (@as(c_int, flt_ptr[@as(c_int, 32)]) * p4)) + (@as(c_int, flt_ptr[@as(c_int, 40)]) * p5)) + (@as(c_int, flt_ptr[@as(c_int, 48)]) * p6);
                                    _ = &acc;
                                    ptr[@bitCast(@as(isize, @intCast(xx)))] = @bitCast(@as(c_short, @truncate(iclip((acc + @as(c_int, 8)) >> @intCast(@as(c_int, 4)), 0, bitdepth_max))));
                                }
                            }
                            ptr += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
                        }
                    }
                    left = &dst[@bitCast(@as(isize, @intCast((x + @as(c_int, 4)) - @as(c_int, 1))))];
                    left_stride = PXSTRIDE(stride);
                    top += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
                    topleft = &top[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
                }
            }
            top = &dst[@bitCast(@as(isize, @intCast(PXSTRIDE(stride))))];
            dst = &dst[@bitCast(@as(isize, @intCast(PXSTRIDE(stride) * @as(ptrdiff_t, 2))))];
        }
    }
}
pub fn cfl_ac_c(arg_ac: [*c]i16, arg_ypx: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, width: c_int, height: c_int, ss_hor: c_int, ss_ver: c_int) callconv(.c) void {
    var ac = arg_ac;
    _ = &ac;
    var ypx = arg_ypx;
    _ = &ypx;
    _ = &stride;
    _ = &w_pad;
    _ = &h_pad;
    _ = &width;
    _ = &height;
    _ = &ss_hor;
    _ = &ss_ver;
    var y: c_int = undefined;
    _ = &y;
    var x: c_int = undefined;
    _ = &x;
    const ac_orig: [*c]i16 = ac;
    _ = &ac_orig;
    while (true) {
        if (!((w_pad >= @as(c_int, 0)) and ((w_pad * @as(c_int, 4)) < width))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!((h_pad >= @as(c_int, 0)) and ((h_pad * @as(c_int, 4)) < height))) {
            unreachable;
        }
        if (!false) break;
    }
    {
        y = 0;
        while (y < (height - (@as(c_int, 4) * h_pad))) : (y += 1) {
            {
                x = 0;
                while (x < (width - (@as(c_int, 4) * w_pad))) : (x += 1) {
                    var ac_sum: c_int = ypx[@bitCast(@as(isize, @intCast(x << @intCast(ss_hor))))];
                    _ = &ac_sum;
                    if (ss_hor != 0) {
                        ac_sum += ypx[@bitCast(@as(isize, @intCast((x * @as(c_int, 2)) + @as(c_int, 1))))];
                    }
                    if (ss_ver != 0) {
                        ac_sum += ypx[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x << @intCast(ss_hor)) + PXSTRIDE(stride))))];
                        if (ss_hor != 0) {
                            ac_sum += ypx[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, (x * @as(c_int, 2)) + @as(c_int, 1)) + PXSTRIDE(stride))))];
                        }
                    }
                    ac[@bitCast(@as(isize, @intCast(x)))] = @truncate(ac_sum << @intCast((@as(c_int, 1) + @as(c_int, @intFromBool(!(ss_ver != 0)))) + @as(c_int, @intFromBool(!(ss_hor != 0)))));
                }
            }
            while (x < width) : (x += 1) {
                ac[@bitCast(@as(isize, @intCast(x)))] = ac[@bitCast(@as(isize, @intCast(x - @as(c_int, 1))))];
            }
            ac += @as(usize, @bitCast(@as(isize, @intCast(width))));
            ypx += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride) << @intCast(@as(ptrdiff_t, ss_ver))))));
        }
    }
    while (y < height) : (y += 1) {
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(ac)), @ptrCast(@alignCast(&ac[@bitCast(@as(isize, @intCast(-width)))])), @as(c_ulong, @bitCast(@as(c_long, width))) *% @sizeOf(@TypeOf(ac.*)));
        ac += @as(usize, @bitCast(@as(isize, @intCast(width))));
    }
    const log2sz: c_int = ctz(@bitCast(@as(c_int, width))) + ctz(@bitCast(@as(c_int, height)));
    _ = &log2sz;
    var sum: c_int = (@as(c_int, 1) << @intCast(log2sz)) >> @intCast(@as(c_int, 1));
    _ = &sum;
    {
        ac = ac_orig;
        y = 0;
        while (y < height) : (y += 1) {
            {
                x = 0;
                while (x < width) : (x += 1) {
                    sum += ac[@bitCast(@as(isize, @intCast(x)))];
                }
            }
            ac += @as(usize, @bitCast(@as(isize, @intCast(width))));
        }
    }
    sum >>= @intCast(log2sz);
    {
        ac = ac_orig;
        y = 0;
        while (y < height) : (y += 1) {
            {
                x = 0;
                while (x < width) : (x += 1) {
                    {
                        const ref = &ac[@bitCast(@as(isize, @intCast(x)))];
                        ref.* = @truncate(@as(c_int, ref.*) - sum);
                    }
                }
            }
            ac += @as(usize, @bitCast(@as(isize, @intCast(width))));
        }
    }
}
pub fn cfl_ac_420_c(ac: [*c]i16, ypx: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void {
    _ = &ac;
    _ = &ypx;
    _ = &stride;
    _ = &w_pad;
    _ = &h_pad;
    _ = &cw;
    _ = &ch;
    cfl_ac_c(ac, ypx, stride, w_pad, h_pad, cw, ch, 1, 1);
}
pub fn cfl_ac_422_c(ac: [*c]i16, ypx: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void {
    _ = &ac;
    _ = &ypx;
    _ = &stride;
    _ = &w_pad;
    _ = &h_pad;
    _ = &cw;
    _ = &ch;
    cfl_ac_c(ac, ypx, stride, w_pad, h_pad, cw, ch, 1, 0);
}
pub fn cfl_ac_444_c(ac: [*c]i16, ypx: [*c]const pixel, stride: ptrdiff_t, w_pad: c_int, h_pad: c_int, cw: c_int, ch: c_int) callconv(.c) void {
    _ = &ac;
    _ = &ypx;
    _ = &stride;
    _ = &w_pad;
    _ = &h_pad;
    _ = &cw;
    _ = &ch;
    cfl_ac_c(ac, ypx, stride, w_pad, h_pad, cw, ch, 0, 0);
}
pub fn pal_pred_c(arg_dst: [*c]pixel, stride: ptrdiff_t, pal: [*c]const pixel, arg_idx: [*c]const u8, w: c_int, h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &pal;
    var idx = arg_idx;
    _ = &idx;
    _ = &w;
    _ = &h;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 2) {
                    const i: c_int = (blk: {
                        const ref = &idx;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    }).*;
                    _ = &i;
                    while (true) {
                        if (!!((i & @as(c_int, 136)) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    dst[@bitCast(@as(isize, @intCast(x + @as(c_int, 0))))] = pal[@bitCast(@as(isize, @intCast(i & @as(c_int, 7))))];
                    dst[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))] = pal[@bitCast(@as(isize, @intCast(i >> @intCast(@as(c_int, 4)))))];
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(stride)))));
        }
    }
}
pub export fn antfly_av1_dav1d_intra_pred_dsp_init_16bpc(c: [*c]Dav1dIntraPredDSPContext) void {
    _ = &c;
    c.*.intra_pred[DC_PRED] = ipred_dc_c;
    c.*.intra_pred[DC_128_PRED] = ipred_dc_128_c;
    c.*.intra_pred[TOP_DC_PRED] = ipred_dc_top_c;
    c.*.intra_pred[LEFT_DC_PRED] = ipred_dc_left_c;
    c.*.intra_pred[HOR_PRED] = ipred_h_c;
    c.*.intra_pred[VERT_PRED] = ipred_v_c;
    c.*.intra_pred[PAETH_PRED] = ipred_paeth_c;
    c.*.intra_pred[SMOOTH_PRED] = ipred_smooth_c;
    c.*.intra_pred[SMOOTH_V_PRED] = ipred_smooth_v_c;
    c.*.intra_pred[SMOOTH_H_PRED] = ipred_smooth_h_c;
    c.*.intra_pred[Z1_PRED] = ipred_z1_c;
    c.*.intra_pred[Z2_PRED] = ipred_z2_c;
    c.*.intra_pred[Z3_PRED] = ipred_z3_c;
    c.*.intra_pred[FILTER_PRED] = ipred_filter_c;
    c.*.cfl_ac[DAV1D_PIXEL_LAYOUT_I420 - @as(c_int, 1)] = cfl_ac_420_c;
    c.*.cfl_ac[DAV1D_PIXEL_LAYOUT_I422 - @as(c_int, 1)] = cfl_ac_422_c;
    c.*.cfl_ac[DAV1D_PIXEL_LAYOUT_I444 - @as(c_int, 1)] = cfl_ac_444_c;
    c.*.cfl_pred[DC_PRED] = ipred_cfl_c;
    c.*.cfl_pred[DC_128_PRED] = ipred_cfl_128_c;
    c.*.cfl_pred[TOP_DC_PRED] = ipred_cfl_top_c;
    c.*.cfl_pred[LEFT_DC_PRED] = ipred_cfl_left_c;
    c.*.pal_pred = pal_pred_c;
}
