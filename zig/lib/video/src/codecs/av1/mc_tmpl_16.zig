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
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
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
pub const DAV1D_FILTER_8TAP_REGULAR = common.DAV1D_FILTER_8TAP_REGULAR;
pub const DAV1D_FILTER_8TAP_SMOOTH = common.DAV1D_FILTER_8TAP_SMOOTH;
pub const DAV1D_FILTER_8TAP_SHARP = common.DAV1D_FILTER_8TAP_SHARP;
pub const FILTER_2D_8TAP_REGULAR = common.FILTER_2D_8TAP_REGULAR;
pub const FILTER_2D_8TAP_REGULAR_SMOOTH = common.FILTER_2D_8TAP_REGULAR_SMOOTH;
pub const FILTER_2D_8TAP_REGULAR_SHARP = common.FILTER_2D_8TAP_REGULAR_SHARP;
pub const FILTER_2D_8TAP_SHARP_REGULAR = common.FILTER_2D_8TAP_SHARP_REGULAR;
pub const FILTER_2D_8TAP_SHARP_SMOOTH = common.FILTER_2D_8TAP_SHARP_SMOOTH;
pub const FILTER_2D_8TAP_SHARP = common.FILTER_2D_8TAP_SHARP;
pub const FILTER_2D_8TAP_SMOOTH_REGULAR = common.FILTER_2D_8TAP_SMOOTH_REGULAR;
pub const FILTER_2D_8TAP_SMOOTH = common.FILTER_2D_8TAP_SMOOTH;
pub const FILTER_2D_8TAP_SMOOTH_SHARP = common.FILTER_2D_8TAP_SMOOTH_SHARP;
pub const FILTER_2D_BILINEAR = common.FILTER_2D_BILINEAR;
pub const mc_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mc_scaled_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const warp8x8_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mct_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mct_scaled_fn = ?*const fn (tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const warp8x8t_fn = ?*const fn (tmp: [*c]i16, tmp_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const w_avg_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, weight: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]const u8, bitdepth_max: c_int) callconv(.c) void;
pub const w_mask_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, mask: [*c]u8, sign: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const blend_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int, mask: [*c]const u8) callconv(.c) void;
pub const blend_dir_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, tmp: [*c]const pixel, w: c_int, h: c_int) callconv(.c) void;
pub const emu_edge_fn = ?*const fn (bw: isize, bh: isize, iw: isize, ih: isize, x: isize, y: isize, dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t) callconv(.c) void;
pub const resize_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, dst_w: c_int, h: c_int, src_w: c_int, dx: c_int, mx: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dMCDSPContext = extern struct {
    mc: [10]mc_fn = @import("std").mem.zeroes([10]mc_fn),
    mc_scaled: [10]mc_scaled_fn = @import("std").mem.zeroes([10]mc_scaled_fn),
    mct: [10]mct_fn = @import("std").mem.zeroes([10]mct_fn),
    mct_scaled: [10]mct_scaled_fn = @import("std").mem.zeroes([10]mct_scaled_fn),
    avg: avg_fn = null,
    w_avg: w_avg_fn = null,
    mask: mask_fn = null,
    w_mask: [3]w_mask_fn = @import("std").mem.zeroes([3]w_mask_fn),
    blend: blend_fn = null,
    blend_v: blend_dir_fn = null,
    blend_h: blend_dir_fn = null,
    warp8x8: warp8x8_fn = null,
    warp8x8t: warp8x8t_fn = null,
    emu_edge: emu_edge_fn = null,
    resize: resize_fn = null,
};
pub const Dav1dMCDSPContext = struct_Dav1dMCDSPContext;
pub extern var antfly_av1_dav1d_mc_subpel_filters: [6][15][8]i8;
pub extern var antfly_av1_dav1d_mc_warp_filter: [193][8]i8;
pub extern var antfly_av1_dav1d_resize_filter: [64][8]i8;
pub extern const antfly_av1_dav1d_obmc_masks: [64]u8;
pub fn put_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, arg_h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    while (true) {
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @bitCast(@as(c_long, w << @intCast(@as(c_int, 1)))));
        dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
        src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn prep_c(arg_tmp: [*c]i16, arg_src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, arg_h: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate((@as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) << @intCast(intermediate_bits)) - @as(c_int, 8192));
            }
        }
        tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn put_8tap_c(arg_dst: [*c]pixel, arg_dst_stride: ptrdiff_t, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, my: c_int, filter_type: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &filter_type;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const intermediate_rnd: c_int = @as(c_int, 32) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)));
    _ = &intermediate_rnd;
    const fh: [*c]const i8 = @ptrCast(@alignCast(if (!(mx != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (w > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type & @as(c_int, 3))))][@bitCast(@as(isize, @intCast(mx - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + (filter_type & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(mx - @as(c_int, 1))))]))))))));
    _ = &fh;
    const fv: [*c]const i8 = @ptrCast(@alignCast(if (!(my != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (h > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type >> @intCast(@as(c_int, 2)))))][@bitCast(@as(isize, @intCast(my - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + ((filter_type >> @intCast(@as(c_int, 2))) & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(my - @as(c_int, 1))))]))))))));
    _ = &fv;
    dst_stride = PXSTRIDE(dst_stride);
    src_stride = PXSTRIDE(src_stride);
    if (fh != null) {
        if (fv != null) {
            var tmp_h: c_int = h + @as(c_int, 7);
            _ = &tmp_h;
            var mid: [17280]i16 = undefined;
            _ = &mid;
            var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
            _ = &mid_ptr;
            src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, 3)))));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &tmp_h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
            mid_ptr = @as([*c]i16, @ptrCast(@alignCast(&mid))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128) * @as(c_int, 3)))));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 128)))))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 128)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) + intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) + intermediate_bits), 0, bitdepth_max))));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
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
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + intermediate_rnd) >> @intCast(@as(c_int, 6)), 0, bitdepth_max))));
                    }
                }
                dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        }
    } else if (fv != null) {
        while (true) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 3)) * src_stride))))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 2)) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 1)) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 0) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 1) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 2) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 3) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 4) * src_stride))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6)), 0, bitdepth_max))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (!((blk: {
                const ref = &h;
                ref.* -= 1;
                break :blk ref.*;
            }) != 0)) break;
        }
    } else {
        put_c(dst, dst_stride, src, src_stride, w, h);
    }
}
pub fn put_8tap_scaled_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, arg_my: c_int, dx: c_int, dy: c_int, filter_type: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &filter_type;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const intermediate_rnd: c_int = (@as(c_int, 1) << @intCast(intermediate_bits)) >> @intCast(@as(c_int, 1));
    _ = &intermediate_rnd;
    var mid: [1024]i16 = undefined;
    _ = &mid;
    var mid_ptrs: [8][*c]i16 = undefined;
    _ = &mid_ptrs;
    var in_y: c_int = -@as(c_int, 8);
    _ = &in_y;
    src_stride = PXSTRIDE(src_stride);
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 8)) : (i += 1) {
            mid_ptrs[@bitCast(@as(isize, @intCast(i)))] = &mid[@bitCast(@as(isize, @intCast(@as(c_int, 128) * i)))];
        }
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, 3)))));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            var x: c_int = undefined;
            _ = &x;
            var src_y: c_int = my >> @intCast(@as(c_int, 10));
            _ = &src_y;
            const fv: [*c]const i8 = @ptrCast(@alignCast(if (!(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (h > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type >> @intCast(@as(c_int, 2)))))][@bitCast(@as(isize, @intCast(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + ((filter_type >> @intCast(@as(c_int, 2))) & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))))))));
            _ = &fv;
            while (in_y < src_y) {
                var imx: c_int = mx;
                _ = &imx;
                var ioff: c_int = 0;
                _ = &ioff;
                var mid_ptr: [*c]i16 = mid_ptrs[@as(c_int, 0)];
                _ = &mid_ptr;
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < @as(c_int, 7)) : (i += 1) {
                        mid_ptrs[@bitCast(@as(isize, @intCast(i)))] = mid_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
                    }
                }
                mid_ptrs[@as(c_int, 7)] = mid_ptr;
                {
                    x = 0;
                    while (x < w) : (x += 1) {
                        const fh: [*c]const i8 = @ptrCast(@alignCast(if (!((imx >> @intCast(@as(c_int, 6))) != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (w > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type & @as(c_int, 3))))][@bitCast(@as(isize, @intCast((imx >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + (filter_type & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast((imx >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))))))));
                        _ = &fh;
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate(if (fh != null) (((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits) else @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))]) << @intCast(intermediate_bits));
                        imx += dx;
                        ioff += imx >> @intCast(@as(c_int, 10));
                        imx &= 1023;
                    }
                }
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                in_y += 1;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(if (fv != null) iclip((((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, mid_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, mid_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, mid_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, mid_ptrs[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, mid_ptrs[@as(c_int, 4)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, mid_ptrs[@as(c_int, 5)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, mid_ptrs[@as(c_int, 6)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, mid_ptrs[@as(c_int, 7)][@bitCast(@as(isize, @intCast(x)))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) + intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) + intermediate_bits), 0, bitdepth_max) else iclip((@as(c_int, mid_ptrs[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))]) + intermediate_rnd) >> @intCast(intermediate_bits), 0, bitdepth_max))));
                }
            }
            my += dy;
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        }
    }
}
pub fn prep_8tap_c(arg_tmp: [*c]i16, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, my: c_int, filter_type: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &filter_type;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const fh: [*c]const i8 = @ptrCast(@alignCast(if (!(mx != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (w > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type & @as(c_int, 3))))][@bitCast(@as(isize, @intCast(mx - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + (filter_type & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(mx - @as(c_int, 1))))]))))))));
    _ = &fh;
    const fv: [*c]const i8 = @ptrCast(@alignCast(if (!(my != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (h > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type >> @intCast(@as(c_int, 2)))))][@bitCast(@as(isize, @intCast(my - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + ((filter_type >> @intCast(@as(c_int, 2))) & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(my - @as(c_int, 1))))]))))))));
    _ = &fv;
    src_stride = PXSTRIDE(src_stride);
    if (fh != null) {
        if (fv != null) {
            var tmp_h: c_int = h + @as(c_int, 7);
            _ = &tmp_h;
            var mid: [17280]i16 = undefined;
            _ = &mid;
            var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
            _ = &mid_ptr;
            src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, 3)))));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &tmp_h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
            mid_ptr = @as([*c]i16, @ptrCast(@alignCast(&mid))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128) * @as(c_int, 3)))));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        var t: c_int = ((((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 128)))))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 128)))))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 128)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6))) - @as(c_int, 8192);
                        _ = &t;
                        while (true) {
                            if (!((t >= -@as(c_int, 32768)) and (t <= @as(c_int, 32767)))) {
                                unreachable;
                            }
                            if (!false) break;
                        }
                        tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(t);
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
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
                        tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits)) - @as(c_int, 8192));
                    }
                }
                tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        }
    } else if (fv != null) {
        while (true) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 3)) * src_stride))))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 2)) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, -@as(c_int, 1)) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 0) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 1) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 2) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 3) * src_stride))))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + (@as(ptrdiff_t, 4) * src_stride))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits)) - @as(c_int, 8192));
                }
            }
            tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (!((blk: {
                const ref = &h;
                ref.* -= 1;
                break :blk ref.*;
            }) != 0)) break;
        }
    } else {
        prep_c(tmp, src, src_stride, w, h, bitdepth_max);
    }
}
pub fn prep_8tap_scaled_c(arg_tmp: [*c]i16, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, arg_my: c_int, dx: c_int, dy: c_int, filter_type: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &filter_type;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    var mid: [1024]i16 = undefined;
    _ = &mid;
    var mid_ptrs: [8][*c]i16 = undefined;
    _ = &mid_ptrs;
    var in_y: c_int = -@as(c_int, 8);
    _ = &in_y;
    src_stride = PXSTRIDE(src_stride);
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 8)) : (i += 1) {
            mid_ptrs[@bitCast(@as(isize, @intCast(i)))] = &mid[@bitCast(@as(isize, @intCast(@as(c_int, 128) * i)))];
        }
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, 3)))));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : (y += 1) {
            var x: c_int = undefined;
            _ = &x;
            var src_y: c_int = my >> @intCast(@as(c_int, 10));
            _ = &src_y;
            const fv: [*c]const i8 = @ptrCast(@alignCast(if (!(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (h > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type >> @intCast(@as(c_int, 2)))))][@bitCast(@as(isize, @intCast(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + ((filter_type >> @intCast(@as(c_int, 2))) & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast(((my & @as(c_int, 1023)) >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))))))));
            _ = &fv;
            while (in_y < src_y) {
                var imx: c_int = mx;
                _ = &imx;
                var ioff: c_int = 0;
                _ = &ioff;
                var mid_ptr: [*c]i16 = mid_ptrs[@as(c_int, 0)];
                _ = &mid_ptr;
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < @as(c_int, 7)) : (i += 1) {
                        mid_ptrs[@bitCast(@as(isize, @intCast(i)))] = mid_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
                    }
                }
                mid_ptrs[@as(c_int, 7)] = mid_ptr;
                {
                    x = 0;
                    while (x < w) : (x += 1) {
                        const fh: [*c]const i8 = @ptrCast(@alignCast(if (!((imx >> @intCast(@as(c_int, 6))) != 0)) @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))) else @as(?*const anyopaque, @ptrCast(@alignCast(if (w > @as(c_int, 4)) @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(filter_type & @as(c_int, 3))))][@bitCast(@as(isize, @intCast((imx >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))) else @as([*c]const i8, @ptrCast(@alignCast(&antfly_av1_dav1d_mc_subpel_filters[@bitCast(@as(isize, @intCast(@as(c_int, 3) + (filter_type & @as(c_int, 1)))))][@bitCast(@as(isize, @intCast((imx >> @intCast(@as(c_int, 6))) - @as(c_int, 1))))]))))))));
                        _ = &fh;
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate(if (fh != null) (((((((((@as(c_int, fh[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, fh[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (-@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, fh[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6) - intermediate_bits) else @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))]) << @intCast(intermediate_bits));
                        imx += dx;
                        ioff += imx >> @intCast(@as(c_int, 10));
                        imx &= 1023;
                    }
                }
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                in_y += 1;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate((if (fv != null) (((((((((@as(c_int, fv[@as(c_int, 0)]) * @as(c_int, mid_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))])) + (@as(c_int, fv[@as(c_int, 1)]) * @as(c_int, mid_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 2)]) * @as(c_int, mid_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 3)]) * @as(c_int, mid_ptrs[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 4)]) * @as(c_int, mid_ptrs[@as(c_int, 4)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 5)]) * @as(c_int, mid_ptrs[@as(c_int, 5)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 6)]) * @as(c_int, mid_ptrs[@as(c_int, 6)][@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, fv[@as(c_int, 7)]) * @as(c_int, mid_ptrs[@as(c_int, 7)][@bitCast(@as(isize, @intCast(x)))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 6))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 6)) else @as(c_int, mid_ptrs[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))])) - @as(c_int, 8192));
                }
            }
            my += dy;
            tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        }
    }
}
pub fn put_8tap_regular_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_regular_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_regular_sharp_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_regular_sharp_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_sharp_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_sharp_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_regular_smooth_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_regular_smooth_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_smooth_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_regular_smooth_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_REGULAR | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_regular_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_regular_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_regular_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_regular_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_sharp_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_smooth_sharp_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_sharp_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_smooth_sharp_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SMOOTH | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SHARP << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_regular_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_regular_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_regular_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_regular_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_REGULAR << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_smooth_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    put_8tap_c(dst, dst_stride, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_8tap_sharp_smooth_scaled_c(dst: [*c]pixel, dst_stride: ptrdiff_t, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    put_8tap_scaled_c(dst, dst_stride, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_smooth_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    prep_8tap_c(tmp, src, src_stride, w, h, mx, my, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn prep_8tap_sharp_smooth_scaled_c(tmp: [*c]i16, src: [*c]const pixel, src_stride: ptrdiff_t, w: c_int, h: c_int, mx: c_int, my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &tmp;
    _ = &src;
    _ = &src_stride;
    _ = &w;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    prep_8tap_scaled_c(tmp, src, src_stride, w, h, mx, my, dx, dy, DAV1D_FILTER_8TAP_SHARP | (DAV1D_FILTER_8TAP_SMOOTH << @intCast(@as(c_int, 2))), bitdepth_max);
}
pub fn put_bilin_c(arg_dst: [*c]pixel, arg_dst_stride: ptrdiff_t, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const intermediate_rnd: c_int = (@as(c_int, 1) << @intCast(intermediate_bits)) >> @intCast(@as(c_int, 1));
    _ = &intermediate_rnd;
    dst_stride = PXSTRIDE(dst_stride);
    src_stride = PXSTRIDE(src_stride);
    if (mx != 0) {
        if (my != 0) {
            var mid: [16512]i16 = undefined;
            _ = &mid;
            var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
            _ = &mid_ptr;
            var tmp_h: c_int = h + @as(c_int, 1);
            _ = &tmp_h;
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (mx * (@as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &tmp_h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
            mid_ptr = @ptrCast(@alignCast(&mid));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, 16) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x)))])) + (my * (@as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + @as(c_int, 128))))]) - @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) + intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) + intermediate_bits), 0, bitdepth_max))));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
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
                        const px: c_int = (((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (mx * (@as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits);
                        _ = &px;
                        dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((px + intermediate_rnd) >> @intCast(intermediate_bits), 0, bitdepth_max))));
                    }
                }
                dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        }
    } else if (my != 0) {
        while (true) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (my * (@as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + src_stride)))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4)), 0, bitdepth_max))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (!((blk: {
                const ref = &h;
                ref.* -= 1;
                break :blk ref.*;
            }) != 0)) break;
        }
    } else {
        put_c(dst, dst_stride, src, src_stride, w, h);
    }
}
pub fn put_bilin_scaled_c(arg_dst: [*c]pixel, arg_dst_stride: ptrdiff_t, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, arg_my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    var mid: [256]i16 = undefined;
    _ = &mid;
    var in_y: c_int = -@as(c_int, 2);
    _ = &in_y;
    while (true) {
        var x: c_int = undefined;
        _ = &x;
        var y: c_int = my >> @intCast(@as(c_int, 10));
        _ = &y;
        var mid1: [*c]i16 = &mid[@bitCast(@as(isize, @intCast((y & @as(c_int, 1)) * @as(c_int, 128))))];
        _ = &mid1;
        var mid2: [*c]i16 = &mid[@bitCast(@as(isize, @intCast(((y + @as(c_int, 1)) & @as(c_int, 1)) * @as(c_int, 128))))];
        _ = &mid2;
        var dmy: c_int = my & @as(c_int, 1023);
        _ = &dmy;
        while (in_y < y) {
            var imx: c_int = mx;
            _ = &imx;
            var ioff: c_int = 0;
            _ = &ioff;
            var mid_ptr: [*c]i16 = &mid[@bitCast(@as(isize, @intCast((in_y & @as(c_int, 1)) * @as(c_int, 128))))];
            _ = &mid_ptr;
            {
                x = 0;
                while (x < w) : (x += 1) {
                    mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))])) + ((imx >> @intCast(@as(c_int, 6))) * (@as(c_int, src[@bitCast(@as(isize, @intCast(ioff + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits));
                    imx += dx;
                    ioff += imx >> @intCast(@as(c_int, 10));
                    imx &= 1023;
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            in_y += 1;
        }
        {
            x = 0;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, 16) * @as(c_int, mid1[@bitCast(@as(isize, @intCast(x)))])) + ((dmy >> @intCast(@as(c_int, 6))) * (@as(c_int, mid2[@bitCast(@as(isize, @intCast(x)))]) - @as(c_int, mid1[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) + intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) + intermediate_bits), 0, bitdepth_max))));
            }
        }
        my += dy;
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn prep_bilin_c(arg_tmp: [*c]i16, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, my: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    _ = &my;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    src_stride = PXSTRIDE(src_stride);
    if (mx != 0) {
        if (my != 0) {
            var mid: [16512]i16 = undefined;
            _ = &mid;
            var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
            _ = &mid_ptr;
            var tmp_h: c_int = h + @as(c_int, 1);
            _ = &tmp_h;
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (mx * (@as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &tmp_h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
            mid_ptr = @ptrCast(@alignCast(&mid));
            while (true) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w) : (x += 1) {
                        tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((@as(c_int, 16) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x)))])) + (my * (@as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + @as(c_int, 128))))]) - @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4))) - @as(c_int, 8192));
                    }
                }
                mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
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
                        tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (mx * (@as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits)) - @as(c_int, 8192));
                    }
                }
                tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
                src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
                if (!((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) != 0)) break;
            }
        }
    } else if (my != 0) {
        while (true) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (my * (@as(c_int, src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, x) + src_stride)))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits)) - @as(c_int, 8192));
                }
            }
            tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (!((blk: {
                const ref = &h;
                ref.* -= 1;
                break :blk ref.*;
            }) != 0)) break;
        }
    } else {
        prep_c(tmp, src, src_stride, w, h, bitdepth_max);
    }
}
pub fn prep_bilin_scaled_c(arg_tmp: [*c]i16, arg_src: [*c]const pixel, arg_src_stride: ptrdiff_t, w: c_int, arg_h: c_int, mx: c_int, arg_my: c_int, dx: c_int, dy: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &dx;
    _ = &dy;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    var mid: [256]i16 = undefined;
    _ = &mid;
    var in_y: c_int = -@as(c_int, 2);
    _ = &in_y;
    while (true) {
        var x: c_int = undefined;
        _ = &x;
        var y: c_int = my >> @intCast(@as(c_int, 10));
        _ = &y;
        var mid1: [*c]i16 = &mid[@bitCast(@as(isize, @intCast((y & @as(c_int, 1)) * @as(c_int, 128))))];
        _ = &mid1;
        var mid2: [*c]i16 = &mid[@bitCast(@as(isize, @intCast(((y + @as(c_int, 1)) & @as(c_int, 1)) * @as(c_int, 128))))];
        _ = &mid2;
        var dmy: c_int = my & @as(c_int, 1023);
        _ = &dmy;
        while (in_y < y) {
            var imx: c_int = mx;
            _ = &imx;
            var ioff: c_int = 0;
            _ = &ioff;
            var mid_ptr: [*c]i16 = &mid[@bitCast(@as(isize, @intCast((in_y & @as(c_int, 1)) * @as(c_int, 128))))];
            _ = &mid_ptr;
            {
                x = 0;
                while (x < w) : (x += 1) {
                    mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((@as(c_int, 16) * @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))])) + ((imx >> @intCast(@as(c_int, 6))) * (@as(c_int, src[@bitCast(@as(isize, @intCast(ioff + @as(c_int, 1))))]) - @as(c_int, src[@bitCast(@as(isize, @intCast(ioff)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4) - intermediate_bits));
                    imx += dx;
                    ioff += imx >> @intCast(@as(c_int, 10));
                    imx &= 1023;
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            in_y += 1;
        }
        {
            x = 0;
            while (x < w) : (x += 1) {
                tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((@as(c_int, 16) * @as(c_int, mid1[@bitCast(@as(isize, @intCast(x)))])) + ((dmy >> @intCast(@as(c_int, 6))) * (@as(c_int, mid2[@bitCast(@as(isize, @intCast(x)))]) - @as(c_int, mid1[@bitCast(@as(isize, @intCast(x)))])))) + ((@as(c_int, 1) << @intCast(@as(c_int, 4))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 4))) - @as(c_int, 8192));
            }
        }
        my += dy;
        tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn avg_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp1: [*c]const i16, arg_tmp2: [*c]const i16, w: c_int, arg_h: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp1 = arg_tmp1;
    _ = &tmp1;
    var tmp2 = arg_tmp2;
    _ = &tmp2;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const sh: c_int = intermediate_bits + @as(c_int, 1);
    _ = &sh;
    const rnd: c_int = (@as(c_int, 1) << @intCast(intermediate_bits)) + (@as(c_int, 8192) * @as(c_int, 2));
    _ = &rnd;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip(((@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))])) + rnd) >> @intCast(sh), 0, bitdepth_max))));
            }
        }
        tmp1 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        tmp2 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn w_avg_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp1: [*c]const i16, arg_tmp2: [*c]const i16, w: c_int, arg_h: c_int, weight: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp1 = arg_tmp1;
    _ = &tmp1;
    var tmp2 = arg_tmp2;
    _ = &tmp2;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &weight;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const sh: c_int = intermediate_bits + @as(c_int, 4);
    _ = &sh;
    const rnd: c_int = (@as(c_int, 8) << @intCast(intermediate_bits)) + (@as(c_int, 8192) * @as(c_int, 16));
    _ = &rnd;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) * weight) + (@as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 16) - weight))) + rnd) >> @intCast(sh), 0, bitdepth_max))));
            }
        }
        tmp1 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        tmp2 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn mask_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp1: [*c]const i16, arg_tmp2: [*c]const i16, w: c_int, arg_h: c_int, arg_mask: [*c]const u8, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp1 = arg_tmp1;
    _ = &tmp1;
    var tmp2 = arg_tmp2;
    _ = &tmp2;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const sh: c_int = intermediate_bits + @as(c_int, 6);
    _ = &sh;
    const rnd: c_int = (@as(c_int, 32) << @intCast(intermediate_bits)) + (@as(c_int, 8192) * @as(c_int, 64));
    _ = &rnd;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))])) + (@as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))])))) + rnd) >> @intCast(sh), 0, bitdepth_max))));
            }
        }
        tmp1 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        tmp2 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        mask += @as(usize, @bitCast(@as(isize, @intCast(w))));
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn blend_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp: [*c]const pixel, w: c_int, arg_h: c_int, arg_mask: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, tmp[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))]))) + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        mask += @as(usize, @bitCast(@as(isize, @intCast(w))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn blend_v_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp: [*c]const pixel, w: c_int, arg_h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &w;
    var h = arg_h;
    _ = &h;
    const mask: [*c]const u8 = &antfly_av1_dav1d_obmc_masks[@bitCast(@as(isize, @intCast(w)))];
    _ = &mask;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < ((w * @as(c_int, 3)) >> @intCast(@as(c_int, 2)))) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))]))) + (@as(c_int, tmp[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, mask[@bitCast(@as(isize, @intCast(x)))]))) + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn blend_h_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp: [*c]const pixel, w: c_int, arg_h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var mask: [*c]const u8 = &antfly_av1_dav1d_obmc_masks[@bitCast(@as(isize, @intCast(h)))];
    _ = &mask;
    h = (h * @as(c_int, 3)) >> @intCast(@as(c_int, 2));
    while (true) {
        const m: c_int = (blk: {
            const ref = &mask;
            const tmp_1 = ref.*;
            ref.* += 1;
            break :blk tmp_1;
        }).*;
        _ = &m;
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - m)) + (@as(c_int, tmp[@bitCast(@as(isize, @intCast(x)))]) * m)) + @as(c_int, 32)) >> @intCast(@as(c_int, 6)))));
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        tmp += @as(usize, @bitCast(@as(isize, @intCast(w))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn w_mask_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_tmp1: [*c]const i16, arg_tmp2: [*c]const i16, w: c_int, arg_h: c_int, arg_mask: [*c]u8, sign: c_int, ss_hor: c_int, ss_ver: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var tmp1 = arg_tmp1;
    _ = &tmp1;
    var tmp2 = arg_tmp2;
    _ = &tmp2;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    _ = &sign;
    _ = &ss_hor;
    _ = &ss_ver;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    const bitdepth: c_int = @as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)));
    _ = &bitdepth;
    const sh: c_int = intermediate_bits + @as(c_int, 6);
    _ = &sh;
    const rnd: c_int = (@as(c_int, 32) << @intCast(intermediate_bits)) + (@as(c_int, 8192) * @as(c_int, 64));
    _ = &rnd;
    const mask_sh: c_int = (bitdepth + intermediate_bits) - @as(c_int, 4);
    _ = &mask_sh;
    const mask_rnd: c_int = @as(c_int, 1) << @intCast(mask_sh - @as(c_int, 5));
    _ = &mask_rnd;
    while (true) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                const m: c_int = imin(@as(c_int, 38) + ((antfly_codec_abs(@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) - @as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))])) + mask_rnd) >> @intCast(mask_sh)), 64);
                _ = &m;
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) * m) + (@as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - m))) + rnd) >> @intCast(sh), 0, bitdepth_max))));
                if (ss_hor != 0) {
                    x += 1;
                    const n: c_int = imin(@as(c_int, 38) + ((antfly_codec_abs(@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) - @as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))])) + mask_rnd) >> @intCast(mask_sh)), 64);
                    _ = &n;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((@as(c_int, tmp1[@bitCast(@as(isize, @intCast(x)))]) * n) + (@as(c_int, tmp2[@bitCast(@as(isize, @intCast(x)))]) * (@as(c_int, 64) - n))) + rnd) >> @intCast(sh), 0, bitdepth_max))));
                    if ((h & ss_ver) != 0) {
                        mask[@bitCast(@as(isize, @intCast(x >> @intCast(@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate(((((m + n) + @as(c_int, mask[@bitCast(@as(isize, @intCast(x >> @intCast(@as(c_int, 1)))))])) + @as(c_int, 2)) - sign) >> @intCast(@as(c_int, 2)))));
                    } else if (ss_ver != 0) {
                        mask[@bitCast(@as(isize, @intCast(x >> @intCast(@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate(m + n)));
                    } else {
                        mask[@bitCast(@as(isize, @intCast(x >> @intCast(@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate((((m + n) + @as(c_int, 1)) - sign) >> @intCast(@as(c_int, 1)))));
                    }
                } else {
                    mask[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(m)));
                }
            }
        }
        tmp1 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        tmp2 += @as(usize, @bitCast(@as(isize, @intCast(w))));
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        if (!(ss_ver != 0) or ((h & @as(c_int, 1)) != 0)) {
            mask += @as(usize, @bitCast(@as(isize, @intCast(w >> @intCast(ss_hor)))));
        }
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub fn w_mask_444_c(dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, arg_mask: [*c]u8, sign: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &tmp1;
    _ = &tmp2;
    _ = &w;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    _ = &sign;
    _ = &bitdepth_max;
    w_mask_c(dst, dst_stride, tmp1, tmp2, w, h, mask, sign, 0, 0, bitdepth_max);
}
pub fn w_mask_422_c(dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, arg_mask: [*c]u8, sign: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &tmp1;
    _ = &tmp2;
    _ = &w;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    _ = &sign;
    _ = &bitdepth_max;
    w_mask_c(dst, dst_stride, tmp1, tmp2, w, h, mask, sign, 1, 0, bitdepth_max);
}
pub fn w_mask_420_c(dst: [*c]pixel, dst_stride: ptrdiff_t, tmp1: [*c]const i16, tmp2: [*c]const i16, w: c_int, h: c_int, arg_mask: [*c]u8, sign: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst;
    _ = &dst_stride;
    _ = &tmp1;
    _ = &tmp2;
    _ = &w;
    _ = &h;
    var mask = arg_mask;
    _ = &mask;
    _ = &sign;
    _ = &bitdepth_max;
    w_mask_c(dst, dst_stride, tmp1, tmp2, w, h, mask, sign, 1, 1, bitdepth_max);
}
pub fn warp_affine_8x8_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, arg_mx: c_int, arg_my: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &abcd;
    var mx = arg_mx;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    var mid: [120]i16 = undefined;
    _ = &mid;
    var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
    _ = &mid_ptr;
    src -= @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * PXSTRIDE(src_stride)))));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 15)) : ({
            y += 1;
            mx += abcd[@as(c_int, 1)];
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                var tmx: c_int = mx;
                _ = &tmx;
                while (x < @as(c_int, 8)) : ({
                    x += 1;
                    tmx += abcd[@as(c_int, 0)];
                }) {
                    const filter: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_mc_warp_filter[@bitCast(@as(isize, @intCast(@as(c_int, 64) + ((tmx + @as(c_int, 512)) >> @intCast(@as(c_int, 10))))))]));
                    _ = &filter;
                    mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((((((((@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 7) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 7) - intermediate_bits));
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
        }
    }
    mid_ptr = &mid[@as(c_int, 3) * @as(c_int, 8)];
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 8)) : ({
            y += 1;
            my += abcd[@as(c_int, 3)];
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                var tmy: c_int = my;
                _ = &tmy;
                while (x < @as(c_int, 8)) : ({
                    x += 1;
                    tmy += abcd[@as(c_int, 2)];
                }) {
                    const filter: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_mc_warp_filter[@bitCast(@as(isize, @intCast(@as(c_int, 64) + ((tmy + @as(c_int, 512)) >> @intCast(@as(c_int, 10))))))]));
                    _ = &filter;
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((((((((((@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 3) * @as(c_int, 8)))))])) + (@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 2) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 2)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 1) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 3)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 4)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 5)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 6)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 7)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 8)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 7) + intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 7) + intermediate_bits), 0, bitdepth_max))));
                }
            }
            mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        }
    }
}
pub fn warp_affine_8x8t_c(arg_tmp: [*c]i16, tmp_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, abcd: [*c]const i16, arg_mx: c_int, arg_my: c_int, bitdepth_max: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    _ = &tmp_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &abcd;
    var mx = arg_mx;
    _ = &mx;
    var my = arg_my;
    _ = &my;
    _ = &bitdepth_max;
    const intermediate_bits: c_int = @as(c_int, 14) - (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max))));
    _ = &intermediate_bits;
    var mid: [120]i16 = undefined;
    _ = &mid;
    var mid_ptr: [*c]i16 = @ptrCast(@alignCast(&mid));
    _ = &mid_ptr;
    src -= @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * PXSTRIDE(src_stride)))));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 15)) : ({
            y += 1;
            mx += abcd[@as(c_int, 1)];
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                var tmx: c_int = mx;
                _ = &tmx;
                while (x < @as(c_int, 8)) : ({
                    x += 1;
                    tmx += abcd[@as(c_int, 0)];
                }) {
                    const filter: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_mc_warp_filter[@bitCast(@as(isize, @intCast(@as(c_int, 64) + ((tmx + @as(c_int, 512)) >> @intCast(@as(c_int, 10))))))]));
                    _ = &filter;
                    mid_ptr[@bitCast(@as(isize, @intCast(x)))] = @truncate((((((((((@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 3) * @as(c_int, 1)))))])) + (@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x - (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 1)))))]))) + (@as(c_int, filter[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 1)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 7) - intermediate_bits)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 7) - intermediate_bits));
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
            mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
        }
    }
    mid_ptr = &mid[@as(c_int, 3) * @as(c_int, 8)];
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 8)) : ({
            y += 1;
            my += abcd[@as(c_int, 3)];
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                var tmy: c_int = my;
                _ = &tmy;
                while (x < @as(c_int, 8)) : ({
                    x += 1;
                    tmy += abcd[@as(c_int, 2)];
                }) {
                    const filter: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_mc_warp_filter[@bitCast(@as(isize, @intCast(@as(c_int, 64) + ((tmy + @as(c_int, 512)) >> @intCast(@as(c_int, 10))))))]));
                    _ = &filter;
                    tmp[@bitCast(@as(isize, @intCast(x)))] = @truncate(((((((((((@as(c_int, filter[@as(c_int, 0)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 3) * @as(c_int, 8)))))])) + (@as(c_int, filter[@as(c_int, 1)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 2) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 2)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x - (@as(c_int, 1) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 3)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 0) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 4)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 1) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 5)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 2) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 6)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 3) * @as(c_int, 8)))))]))) + (@as(c_int, filter[@as(c_int, 7)]) * @as(c_int, mid_ptr[@bitCast(@as(isize, @intCast(x + (@as(c_int, 4) * @as(c_int, 8)))))]))) + ((@as(c_int, 1) << @intCast(@as(c_int, 7))) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 7))) - @as(c_int, 8192));
                }
            }
            mid_ptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            tmp += @as(usize, @bitCast(@as(isize, @intCast(tmp_stride))));
        }
    }
}
pub fn emu_edge_c(bw: isize, bh: isize, iw: isize, ih: isize, x: isize, y: isize, arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_ref: [*c]const pixel, ref_stride: ptrdiff_t) callconv(.c) void {
    _ = &bw;
    _ = &bh;
    _ = &iw;
    _ = &ih;
    _ = &x;
    _ = &y;
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var ref = arg_ref;
    _ = &ref;
    _ = &ref_stride;
    ref += @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, iclip(@truncate(y), 0, @as(c_int, @truncate(ih)) - @as(c_int, 1))) * PXSTRIDE(ref_stride)) + @as(ptrdiff_t, iclip(@truncate(x), 0, @as(c_int, @truncate(iw)) - @as(c_int, 1)))))));
    const left_ext: c_int = iclip(@truncate(-x), 0, @as(c_int, @truncate(bw)) - @as(c_int, 1));
    _ = &left_ext;
    const right_ext: c_int = iclip(@truncate((x + bw) - iw), 0, @as(c_int, @truncate(bw)) - @as(c_int, 1));
    _ = &right_ext;
    while (true) {
        if (!(@as(isize, left_ext + right_ext) < bw)) {
            unreachable;
        }
        if (!false) break;
    }
    const top_ext: c_int = iclip(@truncate(-y), 0, @as(c_int, @truncate(bh)) - @as(c_int, 1));
    _ = &top_ext;
    const bottom_ext: c_int = iclip(@truncate((y + bh) - ih), 0, @as(c_int, @truncate(bh)) - @as(c_int, 1));
    _ = &bottom_ext;
    while (true) {
        if (!(@as(isize, top_ext + bottom_ext) < bh)) {
            unreachable;
        }
        if (!false) break;
    }
    var blk: [*c]pixel = dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, top_ext) * PXSTRIDE(dst_stride)))));
    _ = &blk;
    const center_w: c_int = @truncate((bw - @as(isize, left_ext)) - @as(isize, right_ext));
    _ = &center_w;
    const center_h: c_int = @truncate((bh - @as(isize, top_ext)) - @as(isize, bottom_ext));
    _ = &center_h;
    {
        var y_1: c_int = 0;
        _ = &y_1;
        while (y_1 < center_h) : (y_1 += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(blk + @as(usize, @bitCast(@as(isize, @intCast(left_ext)))))), @ptrCast(@alignCast(ref)), @bitCast(@as(c_long, center_w << @intCast(@as(c_int, 1)))));
            if (left_ext != 0) {
                pixel_set(blk, blk[@bitCast(@as(isize, @intCast(left_ext)))], left_ext);
            }
            if (right_ext != 0) {
                pixel_set((blk + @as(usize, @bitCast(@as(isize, @intCast(left_ext))))) + @as(usize, @bitCast(@as(isize, @intCast(center_w)))), blk[@bitCast(@as(isize, @intCast((left_ext + center_w) - @as(c_int, 1))))], right_ext);
            }
            ref += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(ref_stride)))));
            blk += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        }
    }
    blk = dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, top_ext) * PXSTRIDE(dst_stride)))));
    {
        var y_1: c_int = 0;
        _ = &y_1;
        while (y_1 < top_ext) : (y_1 += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(blk)), @bitCast(@as(c_long, bw << @intCast(@as(isize, 1)))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, center_h) * PXSTRIDE(dst_stride)))));
    {
        var y_1: c_int = 0;
        _ = &y_1;
        while (y_1 < bottom_ext) : (y_1 += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(-PXSTRIDE(dst_stride))))])), @bitCast(@as(c_long, bw << @intCast(@as(isize, 1)))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        }
    }
}
pub fn resize_c(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_src: [*c]const pixel, src_stride: ptrdiff_t, dst_w: c_int, arg_h: c_int, src_w: c_int, dx: c_int, mx0: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    _ = &dst_w;
    var h = arg_h;
    _ = &h;
    _ = &src_w;
    _ = &dx;
    _ = &mx0;
    _ = &bitdepth_max;
    while (true) {
        var mx: c_int = mx0;
        _ = &mx;
        var src_x: c_int = -@as(c_int, 1);
        _ = &src_x;
        {
            var x: c_int = 0;
            _ = &x;
            while (x < dst_w) : (x += 1) {
                const F: [*c]const i8 = @ptrCast(@alignCast(&antfly_av1_dav1d_resize_filter[@bitCast(@as(isize, @intCast(mx >> @intCast(@as(c_int, 8)))))]));
                _ = &F;
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(iclip((-((((((((@as(c_int, F[@as(c_int, 0)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x - @as(c_int, 3), 0, src_w - @as(c_int, 1)))))])) + (@as(c_int, F[@as(c_int, 1)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x - @as(c_int, 2), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 2)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x - @as(c_int, 1), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 3)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x + @as(c_int, 0), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 4)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x + @as(c_int, 1), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 5)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x + @as(c_int, 2), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 6)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x + @as(c_int, 3), 0, src_w - @as(c_int, 1)))))]))) + (@as(c_int, F[@as(c_int, 7)]) * @as(c_int, src[@bitCast(@as(isize, @intCast(iclip(src_x + @as(c_int, 4), 0, src_w - @as(c_int, 1)))))]))) + @as(c_int, 64)) >> @intCast(@as(c_int, 7)), 0, bitdepth_max))));
                mx += dx;
                src_x += mx >> @intCast(@as(c_int, 14));
                mx &= 16383;
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(dst_stride)))));
        src += @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(src_stride)))));
        if (!((blk: {
            const ref = &h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub export fn antfly_av1_dav1d_mc_dsp_init_16bpc(c: [*c]Dav1dMCDSPContext) void {
    _ = &c;
    while (true) {
        c.*.mc[FILTER_2D_8TAP_REGULAR] = put_8tap_regular_c;
        c.*.mc_scaled[FILTER_2D_8TAP_REGULAR] = put_8tap_regular_scaled_c;
        c.*.mct[FILTER_2D_8TAP_REGULAR] = prep_8tap_regular_c;
        c.*.mct_scaled[FILTER_2D_8TAP_REGULAR] = prep_8tap_regular_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_REGULAR_SMOOTH] = put_8tap_regular_smooth_c;
        c.*.mc_scaled[FILTER_2D_8TAP_REGULAR_SMOOTH] = put_8tap_regular_smooth_scaled_c;
        c.*.mct[FILTER_2D_8TAP_REGULAR_SMOOTH] = prep_8tap_regular_smooth_c;
        c.*.mct_scaled[FILTER_2D_8TAP_REGULAR_SMOOTH] = prep_8tap_regular_smooth_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_REGULAR_SHARP] = put_8tap_regular_sharp_c;
        c.*.mc_scaled[FILTER_2D_8TAP_REGULAR_SHARP] = put_8tap_regular_sharp_scaled_c;
        c.*.mct[FILTER_2D_8TAP_REGULAR_SHARP] = prep_8tap_regular_sharp_c;
        c.*.mct_scaled[FILTER_2D_8TAP_REGULAR_SHARP] = prep_8tap_regular_sharp_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SHARP_REGULAR] = put_8tap_sharp_regular_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SHARP_REGULAR] = put_8tap_sharp_regular_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SHARP_REGULAR] = prep_8tap_sharp_regular_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SHARP_REGULAR] = prep_8tap_sharp_regular_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SHARP_SMOOTH] = put_8tap_sharp_smooth_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SHARP_SMOOTH] = put_8tap_sharp_smooth_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SHARP_SMOOTH] = prep_8tap_sharp_smooth_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SHARP_SMOOTH] = prep_8tap_sharp_smooth_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SHARP] = put_8tap_sharp_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SHARP] = put_8tap_sharp_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SHARP] = prep_8tap_sharp_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SHARP] = prep_8tap_sharp_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SMOOTH_REGULAR] = put_8tap_smooth_regular_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SMOOTH_REGULAR] = put_8tap_smooth_regular_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SMOOTH_REGULAR] = prep_8tap_smooth_regular_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SMOOTH_REGULAR] = prep_8tap_smooth_regular_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SMOOTH] = put_8tap_smooth_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SMOOTH] = put_8tap_smooth_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SMOOTH] = prep_8tap_smooth_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SMOOTH] = prep_8tap_smooth_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_8TAP_SMOOTH_SHARP] = put_8tap_smooth_sharp_c;
        c.*.mc_scaled[FILTER_2D_8TAP_SMOOTH_SHARP] = put_8tap_smooth_sharp_scaled_c;
        c.*.mct[FILTER_2D_8TAP_SMOOTH_SHARP] = prep_8tap_smooth_sharp_c;
        c.*.mct_scaled[FILTER_2D_8TAP_SMOOTH_SHARP] = prep_8tap_smooth_sharp_scaled_c;
        if (!false) break;
    }
    while (true) {
        c.*.mc[FILTER_2D_BILINEAR] = put_bilin_c;
        c.*.mc_scaled[FILTER_2D_BILINEAR] = put_bilin_scaled_c;
        c.*.mct[FILTER_2D_BILINEAR] = prep_bilin_c;
        c.*.mct_scaled[FILTER_2D_BILINEAR] = prep_bilin_scaled_c;
        if (!false) break;
    }
    c.*.avg = avg_c;
    c.*.w_avg = w_avg_c;
    c.*.mask = mask_c;
    c.*.blend = blend_c;
    c.*.blend_v = blend_v_c;
    c.*.blend_h = blend_h_c;
    c.*.w_mask[@as(c_int, 0)] = w_mask_444_c;
    c.*.w_mask[@as(c_int, 1)] = w_mask_422_c;
    c.*.w_mask[@as(c_int, 2)] = w_mask_420_c;
    c.*.warp8x8 = warp_affine_8x8_c;
    c.*.warp8x8t = warp_affine_8x8t_c;
    c.*.emu_edge = emu_edge_c;
    c.*.resize = resize_c;
}
